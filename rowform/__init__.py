"""rowform: SQLAlchemy Core statements and schema, typed dataclass rows, no ORM session.

    import sqlalchemy as sa
    from sqlalchemy.orm import Mapped
    import rowform as rf

    class User(rf.Base):
        __tablename__ = "users"
        id: Mapped[int] = rf.mapped_column(primary_key=True)
        name: Mapped[str]

    db = rf.Engine(create_async_engine("postgresql+asyncpg://localhost/app"))
    users = await db.fetch_all(sa.select(User).where(User.name == "ada"))  # list[User]

`fetch_*` returns hydrated objects; `execute()` returns SQLAlchemy's own `Result`.
`db.connect(bind=session)` reads inside an existing session's transaction.
"""

from .compile import compile_hydrator, result_processor
from .connection import Connection, active_connection
from .drivers import Driver, driver_for
from .engine import Engine, Observer
from .errors import (
    ConfigurationError,
    DeclarationError,
    EngineStateError,
    PlanError,
    RowformError,
    StatementError,
    UnsupportedError,
)
from .model import DEFAULT_TYPE_MAP, Base, ModelMeta, alias, mapped_column, model_for
from .planner import Plan, plan
from .query import CoreQuery
from .view import CreateView, DropView, View, view, views

#: Read by [tool.hatch.version] in pyproject.toml.
__version__ = "0.1.0"

__all__ = [
    "DEFAULT_TYPE_MAP",
    "Base",
    "ConfigurationError",
    "Connection",
    "CoreQuery",
    "CreateView",
    "DeclarationError",
    "Driver",
    "DropView",
    "Engine",
    "EngineStateError",
    "ModelMeta",
    "Observer",
    "Plan",
    "PlanError",
    "RowformError",
    "StatementError",
    "UnsupportedError",
    "View",
    "__version__",
    "active_connection",
    "alias",
    "compile_hydrator",
    "driver_for",
    "mapped_column",
    "model_for",
    "plan",
    "result_processor",
    "view",
    "views",
]
