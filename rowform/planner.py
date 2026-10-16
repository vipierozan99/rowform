"""Decide what a statement's rows mean, from the statement rather than the model.

Columns are matched **by identity** against `selected_columns` — `Column.__eq__`
builds SQL — so `select(User.name, User.id)` can never mis-assign fields. A
model entity is planned only where a whole `FromClause` was selected
(`_raw_columns`, private); `select(User.id, User.name, ...)` listing every
column by hand stays a tuple of scalars, as it does in SQLAlchemy.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import FromClause
from sqlalchemy.sql.expression import Join

from .errors import PlanError
from .model import model_for

# One slot of the output tuple, in select order:
#   ("model", model_cls, [(attr_name, ColumnElement), ...], nullable)
#   ("column", ColumnElement)
Entity = tuple[Any, ...]


class Plan:
    """What one statement's rows hydrate into."""

    __slots__ = ("columns", "entities", "wrap")

    def __init__(self, entities: list[Entity], columns: list[Any]):
        self.entities = entities
        self.columns = columns
        # One entity yields that entity, two or more a tuple. Arity alone decides
        # because arity is all a checker can see: `select(User)` and
        # `select(User.name)` are both `Select[tuple[X]]`.
        self.wrap = len(entities) != 1

    def __repr__(self) -> str:
        parts = [
            e[1].__name__ + ("|None" if e[3] else "") if e[0] == "model" else str(e[1])
            for e in self.entities
        ]
        return f"<Plan {', '.join(parts)}{'' if self.wrap else ' (unwrapped)'}>"


def plan(stmt: Any) -> Plan:
    """Build the entity plan for a `Select`, or for a write with RETURNING."""
    columns = list(
        stmt.selected_columns if getattr(stmt, "is_select", False) else stmt.exported_columns
    )
    nullable_froms = _nullable_froms(stmt)
    entity_starts = _entity_starts(stmt)

    entities: list[Entity] = []
    index = 0
    while index < len(columns):
        matched = _match_model(columns, index, nullable_froms) if index in entity_starts else None
        if matched is None:
            entities.append(("column", columns[index]))
            index += 1
        else:
            entity, width = matched
            entities.append(entity)
            index += width

    if not entities:
        raise PlanError("a statement must select at least one column")
    return Plan(entities, columns)


def _match_model(
    columns: list[Any], index: int, nullable_froms: set[int]
) -> tuple[Entity, int] | None:
    """Does the from clause selected whole at `index` yield a model, and how wide is it?"""
    from_clause = getattr(columns[index], "table", None)
    if from_clause is None:
        return None
    model = model_for(from_clause)
    if model is None:
        return None

    declared = type.__getattribute__(model, "__columns__")
    width = len(declared)
    if index + width > len(columns):
        return None

    # By key, in the selected order: an alias proxies the table's columns under the
    # same keys, and a model over a view may declare its fields in another order.
    attr_for = {column.key: attr for attr, column in declared.items()}
    pairs = []
    for column in columns[index : index + width]:
        attr = attr_for.pop(column.key, None)
        if attr is None or from_clause.columns.get(column.key) is not column:
            return None
        pairs.append((attr, column))

    return ("model", model, pairs, id(from_clause) in nullable_froms), width


def _entity_starts(stmt: Any) -> set[int]:
    """Selected-column indices where a whole from clause was selected. Only the raw
    select list (`_raw_columns`, or `_returning` for a write) can tell a model
    from a hand-listed full column set.
    """
    raw = getattr(stmt, "_raw_columns", None)
    if raw is None:
        raw = getattr(stmt, "_returning", ()) or ()

    starts: set[int] = set()
    index = 0
    for element in raw:
        if isinstance(element, FromClause):
            starts.add(index)
            index += len(list(element.exported_columns))
        else:
            index += 1
    return starts


def _nullable_froms(stmt: Any) -> set[int]:
    """`id()` of every from clause reachable only through an OUTER join; an all-NULL
    run of its columns hydrates to `None` rather than an object full of `None`s.
    """
    marked: set[int] = set()
    # A write with RETURNING has no joins and no `get_final_froms`.
    if not hasattr(stmt, "get_final_froms"):
        return marked

    def leaves(clause: Any, into: set[int]) -> None:
        if isinstance(clause, Join):
            leaves(clause.left, into)
            leaves(clause.right, into)
        else:
            into.add(id(clause))

    def walk(clause: Any) -> None:
        if not isinstance(clause, Join):
            return
        walk(clause.left)
        walk(clause.right)
        if clause.isouter or clause.full:
            leaves(clause.right, marked)
        if clause.full:
            leaves(clause.left, marked)

    for from_clause in stmt.get_final_froms():
        walk(from_clause)
    return marked
