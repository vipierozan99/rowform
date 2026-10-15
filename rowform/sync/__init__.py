"""The sync twin of `rowform`: `rowform.sync.Engine` over a SQLAlchemy `Engine`.

Generated from the async modules by `scripts/unasync.py`; models, queries and
hydration are shared with the async API.
"""

from .connection import Connection, active_connection
from .engine import Engine

__all__ = ["Connection", "Engine", "active_connection"]
