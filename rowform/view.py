"""Database views a model can sit on: `__table__ = rf.view(name, metadata, select)`.

A view is a `sa.Table`, as SQLAlchemy's reflection presents one, so it caches
like any table and `Model.__table__` stays a `Table`. It lives in a private
`MetaData` so `create_all()` never emits `CREATE TABLE` for it; the caller's
metadata gets one create/drop listener that handles all of its views, in
declaration order and in reverse.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import ExecutableDDLElement
from sqlalchemy.sql.base import SchemaEventTarget

from .errors import DeclarationError

# Where the views of one `MetaData` are kept, in declaration order.
VIEWS_KEY = "rowform.views"


class View(sa.Table):
    """A view: its columns are the defining SELECT's, under the same keys."""

    inherit_cache = True

    #: The SELECT the view is defined as; `CreateView` renders it.
    definition: Any


class CreateView(ExecutableDDLElement):
    """`CREATE VIEW name AS <select>`, literals inlined: a view takes no parameters."""

    def __init__(self, view: View):
        self.view = view


class DropView(ExecutableDDLElement):
    def __init__(self, view: View):
        self.view = view


@compiles(CreateView)
def _create_view(element: CreateView, compiler: Any, **kw: Any) -> str:
    name = compiler.preparer.format_table(element.view)
    select = compiler.sql_compiler.process(element.view.definition, literal_binds=True)
    return f"CREATE VIEW {name} AS {select}"


@compiles(DropView)
def _drop_view(element: DropView, compiler: Any, **kw: Any) -> str:
    return f"DROP VIEW IF EXISTS {compiler.preparer.format_table(element.view)}"


def view(name: str, metadata: sa.MetaData, select: Any, *, schema: str | None = None) -> View:
    """Declare view `name` as `select` on `metadata`: `create_all(metadata)` creates
    it after the tables, `drop_all(metadata)` drops it before them.
    """
    declared_views = views(metadata)
    if name in declared_views:
        raise DeclarationError(f"view {name!r} is already declared on this MetaData")
    columns = [
        sa.Column(column.name, _own_type(column.type), key=column.key)
        for column in select.selected_columns
    ]
    declared = View(name, sa.MetaData(), *columns, schema=schema)
    declared.definition = select
    if not declared_views:
        event.listen(metadata, "after_create", _create_all)
        event.listen(metadata, "before_drop", _drop_all)
    declared_views[name] = declared
    return declared


def _own_type(type_: sa.types.TypeEngine[Any]) -> sa.types.TypeEngine[Any]:
    """A type that attaches itself to its column (an `Enum`) is copied, as
    `Column._copy()` does, so the view never re-parents the source table's.
    """
    return type_.copy() if isinstance(type_, SchemaEventTarget) else type_


def views(metadata: sa.MetaData) -> dict[str, View]:
    """The views declared on `metadata`, by name, in declaration order."""
    return metadata.info.setdefault(VIEWS_KEY, {})


def _create_all(metadata: sa.MetaData, connection: Any, **kw: Any) -> None:
    inspector = sa.inspect(connection)
    for declared in views(metadata).values():
        if declared.name not in inspector.get_view_names(schema=declared.schema):
            connection.execute(CreateView(declared))


def _drop_all(metadata: sa.MetaData, connection: Any, **kw: Any) -> None:
    for declared in reversed(views(metadata).values()):
        connection.execute(DropView(declared))
