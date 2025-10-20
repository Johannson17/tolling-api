# app.py
import os, json
from typing import Any, Dict, List, Optional

from flask import Flask, request, jsonify, abort, redirect
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import select, insert, update, delete, and_, text
from sqlalchemy.engine import RowMapping
from sqlalchemy.pool import QueuePool
from sqlalchemy import types as satypes

# ---------- Config ----------
def load_config() -> Dict[str, Any]:
    path = os.environ.get("APP_CONFIG", "config.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

cfg = load_config()

# ---------- App ----------
app = Flask(__name__)
CORS(app, resources={r"*": {"origins": cfg["server"].get("cors_allow_origins", ["*"])}})

# JSON error output
@app.errorhandler(HTTPException)
def handle_http_error(e: HTTPException):
    resp = {"error": e.name, "message": e.description, "status": e.code}
    return jsonify(resp), e.code

@app.errorhandler(Exception)
def handle_any_error(e: Exception):
    resp = {"error": "Internal Server Error", "message": str(e)}
    return jsonify(resp), 500

# ---------- DB ----------
engine = create_engine(
    cfg["database"]["url"],
    echo=bool(cfg["api"].get("log_sql")),
    poolclass=QueuePool, pool_size=5, max_overflow=10, future=True
)
meta = MetaData()
meta.reflect(bind=engine, only=cfg["api"].get("expose_tables"))

BASE = cfg["api"].get("base_prefix", "/api").rstrip("/")
DEFAULT_LIMIT = int(cfg["api"].get("default_limit", 100))
MAX_LIMIT = int(cfg["api"].get("max_limit", 1000))
READ_ONLY = set(cfg["api"].get("read_only_tables", []))

# ---------- Helpers ----------
def get_table_or_404(name: str) -> Table:
    if name not in meta.tables:
        abort(404, description=f"Unknown table '{name}'")
    return meta.tables[name]

def pk_columns(tbl: Table) -> List[Column]:
    return list(tbl.primary_key.columns)

def cast_limit(limit: Optional[str], default: int, max_limit: int) -> int:
    try:
        v = int(limit) if limit is not None else default
        return max(1, min(v, max_limit))
    except Exception:
        return default

def build_filters(tbl: Table, args: Dict[str, str]):
    """
    Filtros por querystring:
      col=val        → igualdad
      col_gte=val    → >=
      col_lte=val    → <=
      col_like=pat   → ILIKE
    Combinación por AND. Ignora limit/offset/order_* y claves PK si están para lectura por PK.
    """
    conds = []
    skip = {"limit", "offset", "order_by", "order_dir"}
    # si vienen todas las PK, filtramos solo por PK en GET individual
    if all(c.name in args for c in pk_columns(tbl)):
        for c in pk_columns(tbl):
            conds.append(tbl.c[c.name] == args.get(c.name))
        return and_(*conds)
    # filtros generales
    for key, val in args.items():
        if key in skip or key in [c.name for c in pk_columns(tbl)]:
            continue
        if key.endswith("_gte"):
            col = key[:-4]
            if col in tbl.c: conds.append(tbl.c[col] >= val)
            continue
        if key.endswith("_lte"):
            col = key[:-4]
            if col in tbl.c: conds.append(tbl.c[col] <= val)
            continue
        if key.endswith("_like"):
            col = key[:-5]
            if col in tbl.c: conds.append(tbl.c[col].cast(str).ilike(val))
            continue
        if key in tbl.c:
            conds.append(tbl.c[key] == val)
    return and_(*conds) if conds else None

def parse_order(tbl: Table, args: Dict[str, str]):
    ob = args.get("order_by")
    if not ob or ob not in tbl.c:
        return None
    direction = args.get("order_dir", "desc").lower()
    col = tbl.c[ob]
    return col.desc() if direction == "desc" else col.asc()

def payload_known_columns(tbl: Table, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (payload or {}).items() if k in tbl.c}

def require_pk_params(tbl: Table, args: Dict[str, str]) -> Dict[str, Any]:
    data = {}
    for col in pk_columns(tbl):
        v = args.get(col.name)
        if v is None:
            abort(400, description=f"Missing primary key parameter '{col.name}'")
        data[col.name] = v
    return data

# ---------- Routes factory ----------
def register_table_endpoint(name: str):
    table = get_table_or_404(name)
    path = f"{BASE}/{name}"

    def handler():
        method = request.method

        # GET: si vienen TODAS las PK → registro único; si no → listado
        if method == "GET":
            if all(c.name in request.args for c in pk_columns(table)):
                where = and_(*[table.c[c.name] == request.args.get(c.name) for c in pk_columns(table)])
                stmt = select(table).where(where).limit(1)
                with engine.connect() as conn:
                    row = conn.execute(stmt).mappings().first()
                if not row:
                    abort(404, description="Not found")
                return jsonify(dict(row))
            # listado con filtros
            limit = cast_limit(request.args.get("limit"), DEFAULT_LIMIT, MAX_LIMIT)
            offset = int(request.args.get("offset", 0))
            where = build_filters(table, request.args)
            order = parse_order(table, request.args)
            stmt = select(table)
            if where is not None: stmt = stmt.where(where)
            if order is not None: stmt = stmt.order_by(order)
            stmt = stmt.limit(limit).offset(offset)
            with engine.connect() as conn:
                rows = conn.execute(stmt).mappings().all()
            return jsonify({"data": [dict(r) for r in rows], "limit": limit, "offset": offset})

        # POST: alta
        if method == "POST":
            if name in READ_ONLY:
                abort(405, description=f"Table '{name}' is read-only")
            payload = payload_known_columns(table, request.get_json(silent=True) or {})
            if not payload:
                abort(400, description="Empty payload or unknown fields")
            stmt = insert(table).values(**payload).returning(table)
            with engine.begin() as conn:
                row = conn.execute(stmt).mappings().first()
            return jsonify(dict(row)), 201

        # PUT/PATCH: requiere PK por querystring
        if method in ("PUT", "PATCH"):
            if name in READ_ONLY:
                abort(405, description=f"Table '{name}' is read-only")
            pkd = require_pk_params(table, request.args)
            payload = payload_known_columns(table, request.get_json(silent=True) or {})
            if not payload:
                abort(400, description="Empty payload or unknown fields")
            where = and_(*[table.c[k] == v for k, v in pkd.items()])
            stmt = update(table).where(where).values(**payload).returning(table)
            with engine.begin() as conn:
                row = conn.execute(stmt).mappings().first()
            if not row:
                abort(404, description="Not found")
            return jsonify(dict(row))

        # DELETE: requiere PK por querystring
        if method == "DELETE":
            if name in READ_ONLY:
                abort(405, description=f"Table '{name}' is read-only")
            pkd = require_pk_params(table, request.args)
            where = and_(*[table.c[k] == v for k, v in pkd.items()])
            stmt = delete(table).where(where).returning(*table.primary_key.columns)
            with engine.begin() as conn:
                row = conn.execute(stmt).first()
            if not row:
                abort(404, description="Not found")
            return jsonify({"deleted": True})

        abort(405, description="Method not allowed")

    # Registrar con endpoint único por tabla
    app.add_url_rule(
        path,
        endpoint=f"{name}_endpoint",
        view_func=handler,
        methods=["GET", "POST", "PUT", "PATCH", "DELETE"]
    )

# registra un endpoint único por cada tabla expuesta
for t in cfg["api"]["expose_tables"]:
    register_table_endpoint(t)

# ---------- Health ----------
@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("select 1"))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------- Root redirect & favicon ----------
@app.get("/")
def root():
    docs_path = cfg["api"].get("docs", {}).get("docs_path", "/docs")
    return redirect(docs_path, code=302)

@app.get("/favicon.ico")
def favicon():
    return ("", 204)

# ---------- OpenAPI / Swagger ----------
try:
    from sqlalchemy.dialects.postgresql import UUID as PGUUID, JSONB as PGJSONB
except Exception:
    PGUUID = None; PGJSONB = None

def _oa_schema_for_col(col):
    t = col.type
    if isinstance(t, (satypes.Integer, satypes.BigInteger)): return {"type": "integer"}
    if isinstance(t, (satypes.Numeric, satypes.Float, satypes.DECIMAL)): return {"type": "number"}
    if isinstance(t, satypes.Boolean): return {"type": "boolean"}
    if isinstance(t, satypes.DateTime): return {"type": "string", "format": "date-time"}
    if isinstance(t, satypes.Date): return {"type": "string", "format": "date"}
    if (PGUUID and isinstance(t, PGUUID)): return {"type": "string", "format": "uuid"}
    if (PGJSONB and isinstance(t, PGJSONB)) or isinstance(t, satypes.JSON): return {"type": "object"}
    return {"type": "string"}

def _schema_for_table(tbl: Table):
    props = {c.name: _oa_schema_for_col(c) for c in tbl.columns}
    return {"type": "object", "properties": props}

def build_openapi_spec():
    info = cfg["api"].get("docs", {})
    title = info.get("title", "API")
    version = info.get("version", "1.0.0")
    spec = {"openapi": "3.0.3", "info": {"title": title, "version": version}, "paths": {}, "components": {"schemas": {}}}

    for name, tbl in meta.tables.items():
        if name not in cfg["api"]["expose_tables"]:
            continue

        # Register schema
        spec["components"]["schemas"][name] = _schema_for_table(tbl)

        # Common path (single endpoint per table)
        base_path = f"{BASE}/{name}"
        pk_cols = pk_columns(tbl)

        # Query params (filters + PKs)
        filter_params = [
            {"name":"limit","in":"query","schema":{"type":"integer"}},
            {"name":"offset","in":"query","schema":{"type":"integer"}},
            {"name":"order_by","in":"query","schema":{"type":"string"}},
            {"name":"order_dir","in":"query","schema":{"type":"string","enum":["asc","desc"]}}
        ]

        # equality / gte / lte / like for each column
        for c in tbl.columns:
            filter_params.append({"name": c.name, "in": "query", "required": False, "schema": _oa_schema_for_col(c)})
            filter_params.append({"name": f"{c.name}_gte", "in": "query", "required": False, "schema": {"type":"string"}})
            filter_params.append({"name": f"{c.name}_lte", "in": "query", "required": False, "schema": {"type":"string"}})
            filter_params.append({"name": f"{c.name}_like", "in": "query", "required": False, "schema": {"type":"string"}})

        # Endpoint único por tabla: GET/POST/PUT/PATCH/DELETE
        spec["paths"][base_path] = {
            "get": {
                "summary": f"List or get {name}",
                "description": "If all primary key params are provided → returns a single record; otherwise returns a filtered list.",
                "parameters": filter_params,
                "responses": {
                    "200": {"description": "OK"},
                    "404": {"description": "Not found (when PK provided and not exists)"}
                }
            },
            "post": {
                "summary": f"Create {name}",
                "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{name}"}}}},
                "responses": {"201": {"description": "Created"}, "400": {"description": "Bad request"}}
            },
            "put": {
                "summary": f"Update {name} (requires PK params)",
                "parameters": [{"name": c.name, "in": "query", "required": True, "schema": _oa_schema_for_col(c)} for c in pk_cols],
                "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{name}"}}}},
                "responses": {"200": {"description": "Updated"}, "400": {"description": "Bad request"}, "404": {"description": "Not found"}}
            },
            "patch": {
                "summary": f"Partial update {name} (requires PK params)",
                "parameters": [{"name": c.name, "in": "query", "required": True, "schema": _oa_schema_for_col(c)} for c in pk_cols],
                "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{name}"}}}},
                "responses": {"200": {"description": "Updated"}, "400": {"description": "Bad request"}, "404": {"description": "Not found"}}
            },
            "delete": {
                "summary": f"Delete {name} (requires PK params)",
                "parameters": [{"name": c.name, "in": "query", "required": True, "schema": _oa_schema_for_col(c)} for c in pk_cols],
                "responses": {"200": {"description": "Deleted"}, "404": {"description": "Not found"}}
            }
        }

    return spec

@app.get("/openapi.json")
def openapi_json():
    docs_cfg = cfg["api"].get("docs", {})
    if not docs_cfg.get("enabled", False):
        return jsonify({"error": "docs disabled in config"}), 404
    return jsonify(build_openapi_spec())

@app.get(cfg["api"].get("docs", {}).get("docs_path", "/docs"))
def swagger_ui():
    docs_cfg = cfg["api"].get("docs", {})
    if not docs_cfg.get("enabled", False):
        return jsonify({"error": "docs disabled in config"}), 404
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{docs_cfg.get('title','API')} — Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({{
      url: '/openapi.json',
      dom_id: '#swagger-ui'
    }});
  </script>
</body>
</html>"""

# ---------- Run ----------
if __name__ == "__main__":
    app.run(
        host=cfg["server"]["host"],
        port=int(cfg["server"]["port"]),
        debug=bool(cfg["server"]["debug"])
    )
