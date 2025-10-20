# db.py
from typing import Dict, Any, Optional, List
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import Engine, RowMapping
from sqlalchemy.pool import QueuePool

class Database:
    def __init__(self, url: str, echo: bool = False):
        self.engine: Engine = create_engine(
            url, echo=echo, poolclass=QueuePool, pool_size=5, max_overflow=10, future=True
        )
        self.meta = MetaData()

    def reflect(self, only: Optional[List[str]] = None):
        self.meta.clear()
        self.meta.reflect(bind=self.engine, only=only)

    def table(self, name: str):
        return self.meta.tables[name]

def rows_to_dict(rows: List[RowMapping]) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]

def cast_limit(limit: Optional[int], default: int, max_limit: int) -> int:
    try:
        v = int(limit) if limit is not None else default
        return max(1, min(v, max_limit))
    except Exception:
        return default
