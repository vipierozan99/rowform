"""One checked-out connection, two ways to read it.

Methods spelled as SQLAlchemy spells them (`execute`, `scalars`, `stream`,
`begin`, `commit`) behave as SQLAlchemy's do: `execute()` hands rowform's
hydrated rows to SQLAlchemy's own `Result` (`result.py`). Methods spelled
`fetch_*` are rowform's and return plain hydrated objects with no `Row`.

Transactions are SQLAlchemy's: `begin()`/`begin_nested()` return its
`AsyncTransaction` unwrapped, and the first statement autobegins as it does on
an `AsyncConnection`.
"""

from __future__ import annotations

import contextvars
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from time import perf_counter
from typing import TYPE_CHECKING, Any, TypeVar, overload

from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncResult, AsyncScalarResult

from . import result as _result
from .errors import EngineStateError, StatementError
from .query import CoreQuery, _one_row

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.engine import Result, ScalarResult
    from sqlalchemy.ext.asyncio import AsyncConnection

# One type variable per selected entity — the same per-arity overloads the engine
# carries, because the hot track's exact typing is half of why it exists.
R = TypeVar("R")
R2 = TypeVar("R2")
R3 = TypeVar("R3")
R4 = TypeVar("R4")

# The innermost active Connection for the current task.
_ACTIVE: contextvars.ContextVar[Connection | None] = contextvars.ContextVar(
    "rowform_active_connection", default=None
)


def active_connection() -> Connection | None:
    """The innermost `Connection` scope open in this task, or None."""
    return _ACTIVE.get()


class Connection:
    """A checked-out connection. See the module docstring for the two tracks."""

    __slots__ = (
        "_defers_txn",
        "_engine",
        "_outer",
        "_owns",
        "_token",
        "connection",
        "sa_connection",
    )

    def __init__(
        self,
        engine: Any,
        sa_connection: AsyncConnection,
        connection: Any,
        *,
        owns: bool = True,
    ):
        self._engine = engine
        self._defers_txn = engine.driver.defers_transaction
        #: SQLAlchemy's connection — what owns the transaction.
        self.sa_connection = sa_connection
        #: The driver connection under it — what statements actually run on.
        self.connection = connection
        # False when bound to somebody else's connection: rowform neither opens nor ends one.
        self._owns = owns
        self._token: Any = None
        #: The scope this one was opened inside, for `_reject_if_in_transaction`.
        self._outer: Connection | None = None

    # --- scope bookkeeping ---------------------------------------------------

    def _enter(self) -> None:
        self._outer = _ACTIVE.get()
        self._token = _ACTIVE.set(self)

    def _exit(self) -> None:
        if self._token is not None:
            _ACTIVE.reset(self._token)
            self._token = None

    async def _autobegin(self) -> None:
        conn = self.sa_connection
        if self._owns and not conn.in_transaction():
            await conn.begin()
        # Not `elif`: a transaction the caller opened (`conn.begin()`, `bind=`) is not
        # on the asyncpg driver connection until this runs.
        if self._defers_txn and conn.in_transaction():
            await self._engine.driver.enter_transaction(conn)

    # --- transactions, unwrapped ---------------------------------------------

    def begin(self) -> Any:
        """SQLAlchemy's `AsyncTransaction`, not a wrapper around one."""
        return self.sa_connection.begin()

    def begin_nested(self) -> Any:
        """A SAVEPOINT, as `AsyncConnection.begin_nested()`."""
        return self.sa_connection.begin_nested()

    def _refuse_if_bound(self, method: str) -> None:
        """A bound scope does not own its transaction, so it must not end it — closing
        it here closed it under the caller's session.
        """
        if not self._owns:
            raise EngineStateError(
                f"conn.{method}() on a connection rowform did not open; that "
                f"transaction is the caller's and ending it here would end it "
                f"under them. Call {method}() on the connection or session you "
                f"passed to bind=."
            )

    async def commit(self) -> None:
        self._refuse_if_bound("commit")
        await self.sa_connection.commit()

    async def rollback(self) -> None:
        self._refuse_if_bound("rollback")
        await self.sa_connection.rollback()

    async def close(self) -> None:
        self._refuse_if_bound("close")
        await self.sa_connection.close()

    async def execution_options(self, **options: Any) -> Connection:
        await self.sa_connection.execution_options(**options)
        return self

    def in_transaction(self) -> bool:
        return self.sa_connection.in_transaction()

    def in_nested_transaction(self) -> bool:
        return self.sa_connection.in_nested_transaction()

    @property
    def closed(self) -> bool:
        return self.sa_connection.closed

    # --- compatibility track -------------------------------------------------

    async def execute(
        self, statement: Any, parameters: Any = None, **params: Any
    ) -> Result[Any]:
        """Run `statement` and return a SQLAlchemy `Result`. `parameters` is a dict, or
        a list of dicts for an executemany; `**params` is rowform's extension and
        merges into it. A statement with no result set returns a closed `Result`.
        """
        return await self._execute_any(statement, parameters, params, None)

    async def _execute_any(
        self, statement: Any, parameters: Any, params: dict[str, Any], resolved: Any
    ) -> Result[Any]:
        """`execute()`, with the compiled query optionally already in hand so
        `Engine.execute` does not compute the structural cache key twice.
        """
        engine = self._engine
        await self._autobegin()
        many = isinstance(parameters, (list, tuple))
        if many and params:
            raise StatementError(
                "**params cannot be combined with a sequence of parameter sets; "
                "each set is its own, so there is nothing for them to merge into. "
                f"Put {', '.join(sorted(params))} in every dict instead."
            )
        query, extracted = (
            resolved
            if resolved is not None
            else engine._query_for(
                statement, parameters if many else {**(parameters or {}), **params}
            )
        )
        if many:
            return _result.no_rows(await self._execute_many(query, extracted, parameters))
        bound = {**(parameters or {}), **params}
        if not query.returns_rows:
            return _result.no_rows(await self._execute(query, bound, extracted))
        rows, hydrate = await engine._run(query, bound, self._pinned, extracted)
        plan = query.entities
        assert plan is not None  # returns_rows guarantees it
        return _result.result_for(plan, query.result_metadata, hydrate(rows))

    async def scalar(self, statement: Any, parameters: Any = None, **params: Any) -> Any:
        return (await self.execute(statement, parameters, **params)).scalar()

    async def scalars(
        self, statement: Any, parameters: Any = None, **params: Any
    ) -> ScalarResult[Any]:
        return (await self.execute(statement, parameters, **params)).scalars()

    async def stream(
        self, statement: Any, parameters: Any = None, *, chunk: int = 1000, **params: Any
    ) -> AsyncResult[Any]:
        """`AsyncResult` over a server-side cursor — `conn.stream()`, with rowform
        hydrating each chunk. `chunk` is rowform's spelling of `yield_per`.
        """
        engine = self._engine
        await self._autobegin()
        bound = {**(parameters or {}), **params}
        query, extracted = engine._require_rows(statement, bound)
        plan = query.entities
        assert plan is not None  # _require_rows guarantees it
        chunks = engine._chunks(query, bound, extracted, chunk, self._pinned)
        return AsyncResult(_result.chunked_result(plan, query.result_metadata, chunks))

    async def stream_scalars(
        self, statement: Any, parameters: Any = None, *, chunk: int = 1000, **params: Any
    ) -> AsyncScalarResult[Any]:
        return (await self.stream(statement, parameters, chunk=chunk, **params)).scalars()

    async def exec_driver_sql(self, sql: str, parameters: Any = None) -> Result[Any]:
        """A literal string on the driver: no compilation, so no rows to hydrate."""
        engine = self._engine
        await self._autobegin()
        observer = engine.observer
        start = perf_counter() if observer is not None else 0.0
        try:
            report = await engine.driver.execute(self.connection, sql, parameters)
        except engine.driver.errors as err:
            raise engine._wrap(err, sql, parameters)
        engine._observe(observer, sql, start, None)
        return _result.no_rows(report)

    # --- hot track -----------------------------------------------------------

    @overload
    async def fetch_all(self, statement: CoreQuery[R], **params: Any) -> list[R]: ...

    @overload
    async def fetch_all(self, statement: Select[tuple[R]], **params: Any) -> list[R]: ...

    @overload
    async def fetch_all(
        self, statement: Select[tuple[R, R2]], **params: Any
    ) -> list[tuple[R, R2]]: ...

    @overload
    async def fetch_all(
        self, statement: Select[tuple[R, R2, R3]], **params: Any
    ) -> list[tuple[R, R2, R3]]: ...

    @overload
    async def fetch_all(
        self, statement: Select[tuple[R, R2, R3, R4]], **params: Any
    ) -> list[tuple[R, R2, R3, R4]]: ...

    @overload
    async def fetch_all(self, statement: Any, **params: Any) -> list[Any]: ...

    async def fetch_all(self, statement: Any, **params: Any) -> Any:
        """Hydrated rows, with no `Result` and no `Row` between them and you. One
        selected entity yields that entity; two or more yield a tuple (`planner.py`).
        """
        engine = self._engine
        await self._autobegin()
        query, extracted = engine._require_rows(statement, params)
        rows, hydrate = await engine._run(query, params, self._pinned, extracted)
        return hydrate(rows)

    @overload
    async def fetch_one(self, statement: CoreQuery[R], **params: Any) -> R | None: ...

    @overload
    async def fetch_one(self, statement: Select[tuple[R]], **params: Any) -> R | None: ...

    @overload
    async def fetch_one(
        self, statement: Select[tuple[R, R2]], **params: Any
    ) -> tuple[R, R2] | None: ...

    @overload
    async def fetch_one(
        self, statement: Select[tuple[R, R2, R3]], **params: Any
    ) -> tuple[R, R2, R3] | None: ...

    @overload
    async def fetch_one(
        self, statement: Select[tuple[R, R2, R3, R4]], **params: Any
    ) -> tuple[R, R2, R3, R4] | None: ...

    @overload
    async def fetch_one(self, statement: Any, **params: Any) -> Any: ...

    async def fetch_one(self, statement: Any, **params: Any) -> Any:
        """The first row, or None — narrowed to `LIMIT 1` where that is safe."""
        rows = await self.fetch_all(_one_row(statement), **params)
        return rows[0] if rows else None

    @overload
    def fetch_iter(
        self, statement: CoreQuery[R], *, chunk: int = ..., **params: Any
    ) -> AsyncIterator[R]: ...

    @overload
    def fetch_iter(
        self, statement: Select[tuple[R]], *, chunk: int = ..., **params: Any
    ) -> AsyncIterator[R]: ...

    @overload
    def fetch_iter(
        self, statement: Select[tuple[R, R2]], *, chunk: int = ..., **params: Any
    ) -> AsyncIterator[tuple[R, R2]]: ...

    @overload
    def fetch_iter(
        self, statement: Select[tuple[R, R2, R3]], *, chunk: int = ..., **params: Any
    ) -> AsyncIterator[tuple[R, R2, R3]]: ...

    @overload
    def fetch_iter(
        self, statement: Select[tuple[R, R2, R3, R4]], *, chunk: int = ..., **params: Any
    ) -> AsyncIterator[tuple[R, R2, R3, R4]]: ...

    @overload
    def fetch_iter(
        self, statement: Any, *, chunk: int = ..., **params: Any
    ) -> AsyncIterator[Any]: ...

    def fetch_iter(self, statement: Any, *, chunk: int = 1000, **params: Any) -> Any:
        """`Engine.fetch_iter` on this connection."""
        return self._fetch_iter(statement, chunk, params)

    async def _fetch_iter(
        self, statement: Any, chunk: int, params: dict[str, Any]
    ) -> AsyncIterator[Any]:
        """Autobegin, then delegate — so a scope that opens with a stream is in a
        transaction and the `commit()` after it ends something.
        """
        await self._autobegin()
        async for row in self._engine._iterate(statement, chunk, params, self._pinned):
            yield row

    async def execute_many(self, statement: Any, params: Sequence[dict[str, Any]]) -> Any:
        """One compiled statement, many parameter sets, one round trip; the driver's own report."""
        await self._autobegin()
        query, extracted = self._engine._query_for(statement, params)
        return await self._execute_many(query, extracted, params)

    async def _execute_many(self, query: Any, extracted: Any, params: Sequence[dict[str, Any]]) -> Any:
        """`execute_many` with the query already resolved; both entrances pass through here."""
        if not params:
            return None
        engine = self._engine
        if query._expanding:
            # A post-compile bind rewrites the SQL per set; psycopg would silently
            # write the wrong rows.
            raise StatementError(
                "execute_many cannot run a statement whose SQL is rewritten per "
                "parameter set by a post-compile bind — an expanding bind (an IN "
                "over a list) or a literal-execute bind: each set would need its "
                "own SQL, and executemany sends one statement for all of them. Run "
                "each set with its own execute(), under one transaction if they "
                "must stay atomic."
            )
        shaped = [query.bind(each, extracted) for each in params]
        sql = shaped[0][0]
        observer = engine.observer
        start = perf_counter() if observer is not None else 0.0
        bound_sets = [bound for _, bound in shaped]
        try:
            report = await engine.driver.execute_many(self.connection, sql, bound_sets)
        except engine.driver.errors as err:
            raise engine._wrap(err, sql, bound_sets, multi=True)
        engine._observe(observer, sql, start, None)
        return report

    # --- extensions ----------------------------------------------------------

    async def copy_in(
        self,
        table: Any,
        rows: Sequence[dict[str, Any]],
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Bulk-load through the server's COPY path, on this connection."""
        await self._autobegin()
        return await self._engine._copy_in(self.connection, table, rows, columns)

    def pipeline(self) -> AbstractAsyncContextManager[Any]:
        """psycopg's pipeline mode: statements go out without waiting for each result.
        Worth it only when the round trip is the cost. A statement's result is not
        available until the pipeline synchronises, and an error raises there rather
        than at the statement; it is wrapped like any other, with no `statement` since
        the driver cannot say which one failed. Other drivers raise `UnsupportedError`.
        """
        return self._wrapped(self._engine.driver.pipeline(self.connection))

    @asynccontextmanager
    async def _wrapped(self, pipeline: Any) -> AsyncIterator[Any]:
        engine = self._engine
        try:
            async with pipeline as entered:
                yield entered
        except engine.driver.errors as err:
            raise engine._wrap(err, None, None)

    # --- plumbing ------------------------------------------------------------

    async def _execute(self, query: Any, params: dict[str, Any], extracted: Any) -> Any:
        engine = self._engine
        sql, bound = query.bind(params, extracted)
        observer = engine.observer
        start = perf_counter() if observer is not None else 0.0
        try:
            report = await engine.driver.execute(self.connection, sql, bound)
        except engine.driver.errors as err:
            raise engine._wrap(err, sql, bound)
        engine._observe(observer, sql, start, None)
        return report

    def _pinned(self) -> AbstractAsyncContextManager[Any]:
        """Stands in for the engine's pool checkout, handing back this scope's connection."""
        return _Held(self.connection)

    def __repr__(self) -> str:
        state = "bound" if not self._owns else "open"
        return f"<{type(self).__name__} {state}>"


class _Held:
    __slots__ = ("connection",)

    def __init__(self, connection: Any):
        self.connection = connection

    async def __aenter__(self) -> Any:
        return self.connection

    async def __aexit__(self, *exc: object) -> None:
        return None
