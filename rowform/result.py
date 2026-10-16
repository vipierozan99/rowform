"""The compatibility track: rowform's hydrated rows inside SQLAlchemy's own `Result`.

Rows are handed to `IteratorResult` as they are, with `_source_supports_scalars`
saying whether each item is a whole row or a scalar to wrap only if asked — so
`.scalars()` over a single model builds no `Row` at all. Both that flag and `SimpleResultMetaData`
are SQLAlchemy-private; the flag is spelled `source_supports_scalars` on
`ChunkedIteratorResult`, upstream's inconsistency.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

from sqlalchemy.engine.result import (
    ChunkedIteratorResult,
    IteratorResult,
    SimpleResultMetaData,
)
from sqlalchemy.util import await_only

from .planner import Plan


def keys_for(plan: Plan, compiled: Any) -> list[str]:
    """Column labels for one statement's rows: a model by its class name, as the ORM
    does; a scalar by the name Core gives it, the compiler's result column (`id_1`
    for a repeated `id`, `anon_1` for an unnamed expression). `_result_columns` is
    SQLAlchemy-private. A bare `text()` has no compiled name; it is keyed by its text.
    """
    names = [column.keyname for column in compiled._result_columns]
    if len(names) != len(plan.columns):
        names = [None] * len(plan.columns)
    keys: list[str] = []
    offset = 0
    for entity in plan.entities:
        if entity[0] == "model":
            keys.append(entity[1].__name__)
            offset += len(entity[2])
            continue
        name = names[offset] or str(entity[1])
        # The compiler deduplicates against a model's columns too, which the row
        # does not show: `select(Author, Book.id)` keeps `id`, as the ORM does.
        own = getattr(entity[1], "key", None)
        if own and name != own and name.startswith(f"{own}_") and own not in keys:
            name = own
        keys.append(name)
        offset += 1
    return keys


def rowcount_of(report: Any) -> int:
    """The driver's report of a write as an int; asyncpg's status tag is parsed as
    SQLAlchemy's own dialect parses it, and `-1` means "not known".
    """
    if isinstance(report, int):
        return report
    if isinstance(report, str):
        tail = report.rsplit(" ", 1)[-1]
        if tail.isdigit():
            return int(tail)
    return -1


class _Result(IteratorResult):
    """`IteratorResult` plus `rowcount` and `returns_rows`, which a `CursorResult` carries."""

    def __init__(
        self,
        metadata: Any,
        iterator: Iterator[Any],
        rowcount: int = -1,
        *,
        _source_supports_scalars: bool = False,
    ):
        super().__init__(metadata, iterator, _source_supports_scalars=_source_supports_scalars)
        self._rowform_rowcount = rowcount

    @property
    def rowcount(self) -> int:
        return self._rowform_rowcount

    @property
    def returns_rows(self) -> bool:
        return True


class _NoRows(_Result):
    """A statement with no result set: closed on construction so row accessors raise
    `ResourceClosedError`, as SQLAlchemy does, rather than returning `[]`.
    """

    def __init__(self, rowcount: int = -1):
        super().__init__(SimpleResultMetaData([]), iter(()), rowcount)
        self.close()

    @property
    def returns_rows(self) -> bool:
        return False


def _scalar_source(plan: Plan) -> bool:
    """Whether each hydrated item can always stand in for its row. `IteratorResult`
    reads a bare `None` from a scalar source as "no more rows", so a lone slot that
    can hydrate to `None` (a column, or an outer-joined model) may not.
    """
    if plan.wrap:
        return False
    entity = plan.entities[0]
    return entity[0] == "model" and not entity[3]


def _as_rows(plan: Plan, hydrated: list[Any]) -> list[Any]:
    if plan.wrap or _scalar_source(plan):
        return hydrated
    return [(item,) for item in hydrated]


def result_for(plan: Plan, metadata: Any, hydrated: list[Any]) -> _Result:
    # A lone slot that could be `None` but is not in this batch keeps the scalar
    # source, so `.scalars()` builds no `Row`; `None in` is a C-level scan.
    scalars = not plan.wrap and (_scalar_source(plan) or None not in hydrated)
    return _Result(
        metadata,
        iter(hydrated if plan.wrap or scalars else [(item,) for item in hydrated]),
        len(hydrated),
        _source_supports_scalars=scalars,
    )


def no_rows(report: Any) -> _NoRows:
    return _NoRows(rowcount_of(report))


def chunked_result(
    plan: Plan,
    metadata: Any,
    make_chunks: Callable[[int | None], AsyncIterator[list[Any]]],
) -> ChunkedIteratorResult:
    """A streaming `Result` fed by an async generator. `AsyncResult` runs the sync
    `Result` inside `greenlet_spawn`, which is what makes `await_only` legal here.
    """

    def sync_chunks(size: int | None) -> Iterator[list[Any]]:
        iterator = make_chunks(size)
        while True:
            try:
                yield _as_rows(plan, await_only(iterator.__anext__()))
            except StopAsyncIteration:
                return

    return ChunkedIteratorResult(
        metadata, sync_chunks, source_supports_scalars=_scalar_source(plan)
    )
