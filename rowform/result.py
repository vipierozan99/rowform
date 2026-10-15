"""The compatibility track: rowform's hydrated rows inside SQLAlchemy's own `Result`.

Rows are handed to `IteratorResult` as they are, with `_source_supports_scalars`
saying whether each item is a whole row or a scalar to wrap only if asked — so
`.scalars()` builds no `Row` at all. Both that flag and `SimpleResultMetaData`
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


def keys_for(plan: Plan) -> list[str]:
    """Column labels for one statement's rows: a model by its class name, a scalar by
    its column key, as the ORM does. Duplicates are left to SQLAlchemy to report.
    """
    keys: list[str] = []
    for entity in plan.entities:
        if entity[0] == "model":
            keys.append(entity[1].__name__)
        else:
            column = entity[1]
            keys.append(getattr(column, "key", None) or getattr(column, "name", None) or str(column))
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


def result_for(plan: Plan, metadata: Any, hydrated: list[Any]) -> _Result:
    return _Result(
        metadata, iter(hydrated), len(hydrated), _source_supports_scalars=not plan.wrap
    )


def no_rows(report: Any) -> _NoRows:
    return _NoRows(rowcount_of(report))


def chunked_result(
    metadata: Any,
    make_chunks: Callable[[int | None], AsyncIterator[list[Any]]],
    *,
    scalars: bool = False,
) -> ChunkedIteratorResult:
    """A streaming `Result` fed by an async generator. `AsyncResult` runs the sync
    `Result` inside `greenlet_spawn`, which is what makes `await_only` legal here.
    """

    def sync_chunks(size: int | None) -> Iterator[list[Any]]:
        iterator = make_chunks(size)
        while True:
            try:
                yield await_only(iterator.__anext__())
            except StopAsyncIteration:
                return

    return ChunkedIteratorResult(metadata, sync_chunks, source_supports_scalars=scalars)


def sync_chunked_result(
    metadata: Any,
    make_chunks: Callable[[int | None], Iterator[list[Any]]],
    *,
    scalars: bool = False,
) -> ChunkedIteratorResult:
    """`chunked_result` for a plain generator: no greenlet to await in."""
    return ChunkedIteratorResult(metadata, make_chunks, source_supports_scalars=scalars)
