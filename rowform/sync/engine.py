# GENERATED from rowform/engine.py by scripts/unasync.py; edit that file.
"""`rf.Engine`: rowform's read/write layer over a SQLAlchemy `Engine`.

SQLAlchemy owns the pool, the transactions and the schema; rowform compiles
statements once (`query.py`) and runs them on the driver connection beneath
SQLAlchemy's, which is what lets a read run inside a caller's own session
transaction (`connect(bind=...)`). rowform never opens or disposes the engine.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Iterator, Callable, Sequence
from contextlib import contextmanager
from time import perf_counter
from typing import Any, TypeVar, overload

import sqlalchemy as sa
from sqlalchemy import Select
from sqlalchemy.exc import DBAPIError
from sqlalchemy import Connection as SAConnection
from sqlalchemy import Engine as SAEngine

from .connection import _ACTIVE, Connection
from .drivers import Driver, driver_for
from ..errors import (
    ConfigurationError,
    EngineStateError,
    StatementError,
)
from ..query import CoreQuery, _one_row

_LOG = logging.getLogger("rowform")


#: Called after every statement with the SQL, the round-trip seconds, and the row
#: count (`None` for a statement that returns none).
Observer = Callable[[str, float, "int | None"], None]


#: Compiled statements kept (LRU). SQLAlchemy's `compiled_cache` default, for the
#: same reason: statements built per request would otherwise accumulate forever.
DEFAULT_CACHE_SIZE = 500

# One type variable per selected entity: `Select` is parameterised by a tuple.
R = TypeVar("R")
R2 = TypeVar("R2")
R3 = TypeVar("R3")
R4 = TypeVar("R4")


class Engine:
    """rowform's row layer over a SQLAlchemy `Engine`."""

    def __init__(
        self,
        engine: SAEngine,
        *,
        observer: Observer | None = None,
        cache_size: int | None = DEFAULT_CACHE_SIZE,
    ):
        if cache_size is not None and cache_size < 1:
            raise ConfigurationError(
                f"cache_size must be at least 1, or None for no limit; got {cache_size}"
            )
        if not isinstance(engine, SAEngine):
            raise ConfigurationError(
                f"rf.Engine wraps a SQLAlchemy SAEngine, got {type(engine).__name__}. "
                f"Build one with create_engine(url) and hand it here; rowform "
                f"does not open connections of its own."
            )
        #: The wrapped engine; never opened or disposed here.
        self.sa_engine = engine
        self.driver: Driver = driver_for(engine.dialect)
        self.driver.configure(engine)
        #: Reassignable at any time; `None` disables it. Exceptions it raises are
        #: not caught — it runs on the caller's path.
        self.observer = observer
        self._cache_size = cache_size
        self._queries: OrderedDict[Any, CoreQuery[Any]] = OrderedDict()

    @property
    def dialect(self) -> Any:
        """The engine's own dialect — one SQLAlchemy has run `initialize()` against, so
        it knows the server version where a freshly constructed dialect does not.
        """
        return self.sa_engine.dialect

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.sa_engine.url!r}>"

    # --- statements ---------------------------------------------------------

    @overload
    def prepare(self, statement: Select[tuple[R]]) -> CoreQuery[R]: ...

    @overload
    def prepare(self, statement: Select[tuple[R, R2]]) -> CoreQuery[tuple[R, R2]]: ...

    @overload
    def prepare(self, statement: Select[tuple[R, R2, R3]]) -> CoreQuery[tuple[R, R2, R3]]: ...

    @overload
    def prepare(
        self, statement: Select[tuple[R, R2, R3, R4]]
    ) -> CoreQuery[tuple[R, R2, R3, R4]]: ...

    @overload
    def prepare(self, statement: Any) -> CoreQuery[Any]: ...

    def prepare(self, statement: Any) -> Any:
        """Compile a statement for this engine's dialect, once.

        `fetch_all` does this for you and caches the result under SQLAlchemy's
        structural cache key; hoisting a `CoreQuery` only saves that lookup.
        """
        return CoreQuery(statement, self.dialect)

    def _query_for(self, statement: Any) -> tuple[CoreQuery[Any], Any]:
        """The compiled query, plus this statement's own literal values.

        The structural cache key ignores literals, so the cached compiled object holds
        the *first* statement's; the caller's travel separately as `CacheKey.bindparams`
        (private, like `_generate_cache_key`). `.key` because `CacheKey` is unhashable.
        Bounded LRU so statements built per request do not accumulate forever.
        """
        if isinstance(statement, CoreQuery):
            # Another driver's paramstyle would fail as a cryptic driver error.
            if statement.dialect.driver != self.dialect.driver:
                raise ConfigurationError(
                    f"this CoreQuery was compiled for {statement.dialect.name}+"
                    f"{statement.dialect.driver} and this engine is {self.dialect.name}+"
                    f"{self.dialect.driver}; the compiled SQL would use the wrong "
                    f"paramstyle. prepare() the statement on this engine."
                )
            return statement, None
        cache_key = statement._generate_cache_key()
        if cache_key is None:
            # Uncacheable construct (e.g. `postgresql.insert()`, `inherit_cache=False`):
            # compile fresh; the compiled object then holds this statement's literals.
            return self.prepare(statement), None
        queries = self._queries
        query = queries.get(cache_key.key)
        if query is None:
            query = queries[cache_key.key] = self.prepare(statement)
            if self._cache_size is not None and len(queries) > self._cache_size:
                queries.popitem(last=False)
        else:
            queries.move_to_end(cache_key.key)
        return query, cache_key.bindparams

    @property
    def cached_statements(self) -> int:
        """How many compiled statements are held."""
        return len(self._queries)

    # --- reads --------------------------------------------------------------

    @overload
    def fetch_all(self, statement: CoreQuery[R], **params: Any) -> list[R]: ...

    @overload
    def fetch_all(self, statement: Select[tuple[R]], **params: Any) -> list[R]: ...

    @overload
    def fetch_all(
        self, statement: Select[tuple[R, R2]], **params: Any
    ) -> list[tuple[R, R2]]: ...

    @overload
    def fetch_all(
        self, statement: Select[tuple[R, R2, R3]], **params: Any
    ) -> list[tuple[R, R2, R3]]: ...

    @overload
    def fetch_all(
        self, statement: Select[tuple[R, R2, R3, R4]], **params: Any
    ) -> list[tuple[R, R2, R3, R4]]: ...

    @overload
    def fetch_all(self, statement: Any, **params: Any) -> list[Any]: ...

    def fetch_all(self, statement: Any, **params: Any) -> Any:
        """Hydrated rows. `**params` supplies the statement's `bindparam()` values.

        The statement decides the row: one selected entity yields that entity
        (`select(User)` -> `User`, `select(User.name)` -> `str`); two or more yield a
        tuple. The overloads say the same by arity, which is the most a checker can
        tell; past four entities the row is `Any`.

        A one-shot: a SELECT runs outside any transaction, straight from the pool
        (`_direct_connection`), so no isolation level applies to it — a read that
        needs one belongs in `begin()`. A write with RETURNING commits, or the pool's
        rollback on release would discard it.
        """
        self._reject_if_in_transaction("fetch_all")
        query, extracted = self._require_rows(statement)
        rows, hydrate = self._run(query, params, self._acquire_for(query), extracted)
        return hydrate(rows)

    @overload
    def fetch_iter(
        self, statement: CoreQuery[R], *, chunk: int = ..., **params: Any
    ) -> Iterator[R]: ...

    @overload
    def fetch_iter(
        self, statement: Select[tuple[R]], *, chunk: int = ..., **params: Any
    ) -> Iterator[R]: ...

    @overload
    def fetch_iter(
        self, statement: Select[tuple[R, R2]], *, chunk: int = ..., **params: Any
    ) -> Iterator[tuple[R, R2]]: ...

    @overload
    def fetch_iter(
        self, statement: Select[tuple[R, R2, R3]], *, chunk: int = ..., **params: Any
    ) -> Iterator[tuple[R, R2, R3]]: ...

    @overload
    def fetch_iter(
        self, statement: Select[tuple[R, R2, R3, R4]], *, chunk: int = ..., **params: Any
    ) -> Iterator[tuple[R, R2, R3, R4]]: ...

    @overload
    def fetch_iter(
        self, statement: Any, *, chunk: int = ..., **params: Any
    ) -> Iterator[Any]: ...

    def fetch_iter(self, statement: Any, *, chunk: int = 1000, **params: Any) -> Any:
        """The same rows as `fetch_all`, `chunk` at a time, through a server-side cursor.

        The connection is held for the whole iteration; leaving the `for` early
        closes the cursor. psycopg cannot stream `INSERT ... RETURNING` (postgres will
        not DECLARE a cursor for it) and raises `UnsupportedError`; asyncpg and sqlite can.
        """
        self._reject_if_in_transaction("fetch_iter")
        return self._iterate(statement, chunk, params, None)

    def _iterate(
        self, statement: Any, chunk: int, params: dict[str, Any], acquire: Any
    ) -> Iterator[Any]:
        """Shared by `Engine.fetch_iter` and `Connection.fetch_iter`; `acquire=None`
        means take a pooled connection, resolved after compiling (a RETURNING write
        needs the committing checkout).
        """
        if chunk < 1:
            raise ConfigurationError(f"chunk must be at least 1, got {chunk}")
        query, extracted = self._require_rows(statement)
        if acquire is None:
            acquire = self._acquire_for(query, stream=True)
        sql, bound = query.bind(params, extracted)
        observer = self.observer
        start = perf_counter() if observer is not None else 0.0
        total = 0
        try:
            with acquire() as conn:
                try:
                    for rows, description in self.driver.stream(
                        conn, sql, bound, chunk, query
                    ):
                        hydrate = query._hydrate
                        if hydrate is None:
                            hydrate = query.hydrator(self.dialect, description)
                        total += len(rows)
                        for row in hydrate(rows):
                            yield row
                except self.driver.errors as err:
                    raise self._wrap(err, sql, bound)
        finally:
            # One call per stream, rows actually delivered, consumer time included;
            # in `finally` so an abandoned iteration is still reported.
            self._observe(observer, sql, start, total)

    @overload
    def fetch_one(self, statement: CoreQuery[R], **params: Any) -> R | None: ...

    @overload
    def fetch_one(self, statement: Select[tuple[R]], **params: Any) -> R | None: ...

    @overload
    def fetch_one(
        self, statement: Select[tuple[R, R2]], **params: Any
    ) -> tuple[R, R2] | None: ...

    @overload
    def fetch_one(
        self, statement: Select[tuple[R, R2, R3]], **params: Any
    ) -> tuple[R, R2, R3] | None: ...

    @overload
    def fetch_one(
        self, statement: Select[tuple[R, R2, R3, R4]], **params: Any
    ) -> tuple[R, R2, R3, R4] | None: ...

    @overload
    def fetch_one(self, statement: Any, **params: Any) -> Any: ...

    def fetch_one(self, statement: Any, **params: Any) -> Any:
        """The first row, or None, shaped as `fetch_all` shapes it.

        Narrowed to `LIMIT 1` where that is safe (`_one_row`). For one column of the
        row, narrow the statement with `with_only_columns` instead.
        """
        self._reject_if_in_transaction("fetch_one")
        query, extracted = self._require_rows(_one_row(statement))
        rows, hydrate = self._run(query, params, self._acquire_for(query), extracted)
        hydrated = hydrate(rows)
        return hydrated[0] if hydrated else None

    # --- writes -------------------------------------------------------------

    def execute(self, statement: Any, parameters: Any = None, **params: Any) -> Any:
        """Run a statement in a scope of its own and return a SQLAlchemy `Result`.

        Anything but a SELECT is committed, because the pool's rollback on release
        would otherwise discard it. Keyed on `is_select`, not "returns rows": a write
        with RETURNING returns rows and is still a write. An executemany commits too.
        """
        self._reject_if_in_transaction("execute")
        return self._execute_scoped(statement, parameters, params)

    def _execute_scoped(self, statement: Any, parameters: Any, params: dict[str, Any]) -> Any:
        """`execute()` past the in-transaction guard, so `scalar()`/`scalars()` share it."""
        resolved = self._query_for(statement)
        many = isinstance(parameters, (list, tuple))
        with self._scope(commit=many or not resolved[0].is_select) as conn:
            return conn._execute_any(statement, parameters, params, resolved)

    def scalar(self, statement: Any, parameters: Any = None, **params: Any) -> Any:
        """`execute(...).scalar()`, in a scope of its own."""
        self._reject_if_in_transaction("scalar")
        return (self._execute_scoped(statement, parameters, params)).scalar()

    def scalars(self, statement: Any, parameters: Any = None, **params: Any) -> Any:
        """`execute(...).scalars()`, in a scope of its own; rows are already buffered."""
        self._reject_if_in_transaction("scalars")
        return (self._execute_scoped(statement, parameters, params)).scalars()

    def execute_many(self, statement: Any, params: Sequence[dict[str, Any]]) -> Any:
        """One compiled statement, many parameter sets, one driver round trip.
        Returns the driver's report; `execute(stmt, [...])` wraps the same in a `Result`.
        """
        self._reject_if_in_transaction("execute_many")
        with self._scope(commit=True) as conn:
            return conn.execute_many(statement, params)

    def copy_in(
        self,
        table: sa.Table,
        rows: Sequence[dict[str, Any]],
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Bulk-load rows through the server's COPY path. Returns how many.

        A load path, not a write path: no RETURNING, no ON CONFLICT. `columns` defaults
        to every column of the table. Values go through the same bind processors a
        parameterised INSERT uses, since COPY bypasses the statement path where those
        run. Refused inside a scope, as the one-shot reads are.
        """
        self._reject_if_in_transaction("copy_in")
        with self._checkout(commit=True) as (_, conn):
            return self._copy_in(conn, table, rows, columns)

    def _copy_in(
        self,
        conn: Any,
        table: sa.Table,
        rows: Sequence[dict[str, Any]],
        columns: Sequence[str] | None,
    ) -> int:
        if not rows:
            return 0
        names = list(columns) if columns is not None else [c.key for c in table.columns]
        selected = [table.columns[name] for name in names]
        processors = [
            column.type._cached_bind_processor(self.dialect) for column in selected
        ]
        records = [
            tuple(
                processor(row[column.key]) if processor is not None else row[column.key]
                for column, processor in zip(selected, processors, strict=True)
            )
            for row in rows
        ]
        observer = self.observer
        start = perf_counter() if observer is not None else 0.0
        label = f"COPY {table.name} ({', '.join(names)})"
        try:
            copied = self.driver.copy_in(conn, table, [c.name for c in selected], records)
        except self.driver.errors as err:
            raise self._wrap(err, label, None)
        self._observe(observer, label, start, copied)
        return copied

    # --- schema -------------------------------------------------------------

    def create_all(self, metadata: sa.MetaData) -> None:
        """Create every table in `metadata`, through SQLAlchemy's own `SchemaGenerator`.
        `checkfirst=False`: this is bootstrap; point Alembic at the same `metadata` otherwise.
        """
        with self.sa_engine.begin() as conn:
            metadata.create_all(conn, checkfirst=False)

    def drop_all(self, metadata: sa.MetaData, *, ignore_missing: bool = True) -> None:
        """Drop every table in `metadata`; `ignore_missing` is SQLAlchemy's `checkfirst`."""
        with self.sa_engine.begin() as conn:
            metadata.drop_all(conn, checkfirst=ignore_missing)

    # --- connections and transactions ---------------------------------------

    @contextmanager
    def _checkout(self, *, commit: bool = False) -> Iterator[tuple[Any, Any]]:
        """One pooled checkout, as `(sqlalchemy_connection, driver_connection)`.

        `commit=True` is load-bearing: a one-shot write run on the driver connection
        sits in the driver's own implicit transaction (pysqlite, psycopg) and the pool's
        rollback on release silently discards it. Also handles cancellation and
        disconnect (`_is_disconnect`).
        """
        cm = self.sa_engine.begin() if commit else self.sa_engine.connect()
        with cm as conn:
            fairy = conn.connection
            driver_conn = fairy.driver_connection
            try:
                yield conn, driver_conn
            except Exception as err:
                if self._is_disconnect(err, fairy.dbapi_connection):
                    conn.invalidate()
                raise

    def _is_disconnect(self, err: Exception, dbapi_connection: Any) -> bool:
        """Ask the dialect whether `err` means the connection is dead.

        rowform runs statements on the driver connection, so SQLAlchemy never sees the
        exception and never runs this itself; without it a dead connection goes back
        into the pool. A wrapped error is asked about by its `orig`, and marked
        `connection_invalidated` as SQLAlchemy marks it.
        """
        orig = getattr(err, "orig", err)
        dead = bool(self.dialect.is_disconnect(orig, dbapi_connection, None))
        if dead and isinstance(err, DBAPIError):
            err.connection_invalidated = True
        return dead

    def _wrap(
        self, err: Exception, sql: str | None, params: Any, *, multi: bool = False
    ) -> Exception:
        """`err`, a driver error, as the `sa.exc.DBAPIError` subclass SQLAlchemy's
        `Connection` would have raised, so `except sa.exc.IntegrityError` keeps working.
        Its `orig` is the DBAPI exception and its cause the same, as in SQLAlchemy.
        """
        dbapi = self.dialect.loaded_dbapi
        translated = self.driver.translate(err)
        wrapped = DBAPIError.instance(
            sql,
            params,
            translated,
            dbapi.Error,
            hide_parameters=self.sa_engine.hide_parameters,
            dialect=self.dialect,
            ismulti=multi,
        )
        wrapped.__cause__ = translated
        return wrapped

    @contextmanager
    def _direct_connection(self) -> Iterator[Any]:
        """A pooled driver connection with no `Connection` around it, for one-shot reads.

        `Pool.connect()` directly keeps the pool (pre-ping, recycle, pool events, the
        sqlite `connect` listener) and skips `Connection`/`SAConnection` and two
        greenlet crossings. What it skips is exactly the `engine_connect` event, so an
        engine with any listener there (`execution_options(isolation_level=...)`,
        a caller's own) takes the ordinary checkout instead (`_acquire_for`). The
        driver puts the connection in autocommit for the block (`Driver.autocommit`).
        """
        pool = self.sa_engine.pool
        fairy = pool.connect()
        dbapi_conn = fairy.dbapi_connection
        assert dbapi_conn is not None  # a fresh checkout is never invalidated
        driver_conn = fairy.driver_connection
        try:
            with self.driver.autocommit(driver_conn):
                yield driver_conn
        except Exception as err:
            if self._is_disconnect(err, dbapi_conn):
                # The adapter's close needs a greenlet to in.
                fairy.invalidate()
            raise
        finally:
            fairy.close()

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        """The read seam: a checked-out driver connection, nothing committed. A mock
        engine overrides this and `_direct_connection`.
        """
        with self._checkout() as (_, driver_conn):
            yield driver_conn

    @contextmanager
    def _write_connection(self) -> Iterator[Any]:
        """`_connection()`, committed on the way out."""
        with self._checkout(commit=True) as (_, driver_conn):
            yield driver_conn

    def _acquire_for(self, query: CoreQuery[Any], *, stream: bool = False) -> Any:
        """Which checkout a one-shot takes: committing for a write, direct for a SELECT
        unless something listens on `engine_connect`, ordinary for a stream (psycopg's
        server-side cursor needs a transaction).
        """
        if not query.is_select:
            return self._write_connection
        if stream or len(self.sa_engine.dispatch.engine_connect):
            return self._connection
        return self._direct_connection

    @contextmanager
    def acquire(self) -> Iterator[Any]:
        """Raw driver connection, for anything this engine does not model."""
        with self._connection() as conn:
            yield conn

    @contextmanager
    def _scope(self, *, commit: bool) -> Iterator[Connection]:
        """One checkout as a `Connection` that does not autobegin, for the engine's own
        `execute`-track one-shots.
        """
        with self._checkout(commit=commit) as (sa_conn, driver_conn):
            yield Connection(self, sa_conn, driver_conn, owns=False)

    @contextmanager
    def connect(self, bind: Any = None, **execution_options: Any) -> Iterator[Connection]:
        """A connection scope — `SAEngine.connect()`: commit-as-you-go, the first
        statement autobegins, leaving without `commit()` rolls back.

        `bind=` runs on an `SAConnection` or `Session` somebody else owns:
        statements see that transaction's uncommitted writes and roll back with it,
        and rowform neither begins nor ends anything. Flush the session first: rowform
        reads the connection under it, so a pending `add()` is not yet in the database
        and nothing here autoflushes it. `execution_options` reach
        `SAConnection.execution_options()`.
        """
        if bind is not None:
            if execution_options:
                raise ConfigurationError(
                    "execution_options cannot be set on a connection rowform did not "
                    "open; configure them where the connection was opened"
                )
            sa_conn = self._resolve(bind)
            conn = Connection(self, sa_conn, self._driver_connection(sa_conn), owns=False)
            # Registered even though bound, so `engine.fetch_*` inside is refused.
            conn._enter()
            try:
                yield conn
            finally:
                conn._exit()
            return
        with self._checkout() as (sa_conn, driver_conn):
            if execution_options:
                sa_conn.execution_options(**execution_options)
            conn = Connection(self, sa_conn, driver_conn)
            conn._enter()
            try:
                yield conn
            finally:
                conn._exit()

    @contextmanager
    def begin(self, **execution_options: Any) -> Iterator[Connection]:
        """A connection scope with a transaction open — `SAEngine.begin()`: commits on
        clean exit, rolls back on exception, `conn.begin_nested()` for savepoints.
        """
        with self._checkout() as (sa_conn, driver_conn):
            if execution_options:
                sa_conn.execution_options(**execution_options)
            with sa_conn.begin():
                conn = Connection(self, sa_conn, driver_conn)
                conn._enter()
                try:
                    yield conn
                finally:
                    conn._exit()

    def _resolve(self, target: Any) -> SAConnection:
        """The `SAConnection` behind an `SAConnection` or an `Session`."""
        if isinstance(target, SAConnection):
            conn = target
        else:
            connection = getattr(target, "connection", None)
            if connection is None:
                raise ConfigurationError(
                    f"bind= takes a Connection or a Session, got "
                    f"{type(target).__name__}"
                )
            conn = connection()
        driver = conn.engine.dialect.driver
        if driver != self.dialect.driver:
            raise ConfigurationError(
                f"this engine compiles for {self.dialect.driver} and that connection "
                f"is {driver}; the compiled SQL would use the wrong paramstyle. Use "
                f"an rf.Engine wrapping that connection's own engine."
            )
        return conn

    @staticmethod
    def _driver_connection(conn: SAConnection) -> Any:
        return conn.connection.driver_connection

    def _reject_if_in_transaction(self, method: str) -> None:
        """Inside `connect()`/`begin()` a one-shot would take a *different* pooled
        connection and miss the scope's uncommitted state; fail loudly instead.
        """
        # Walk the whole stack: another engine's scope may be innermost.
        active = _ACTIVE.get()
        while active is not None and active._engine is not self:
            active = active._outer
        if active is not None:
            raise EngineStateError(
                f"engine.{method}() was called inside connect()/begin(); it would run "
                f"on a different pooled connection and miss the scope's uncommitted "
                f"state. Use conn.{method}() instead."
            )

    # --- shared plumbing ----------------------------------------------------

    def _require_rows(self, statement: Any) -> tuple[CoreQuery[Any], Any]:
        query, extracted = self._query_for(statement)
        if not query.returns_rows:
            raise StatementError(
                "this statement produces no rows; hydrating it would return [] and "
                "look like 'nothing matched'. Use execute() for it, or add "
                "returning(...)."
            )
        return query, extracted

    def _run(
        self, query: CoreQuery[Any], params: dict[str, Any], acquire: Any, extracted: Any = None
    ) -> Any:
        """Execute and return `(rows, hydrator)`. The driver describes its result only
        while the hydrator is unbuilt: postgres `Numeric` needs the DBAPI type codes.
        """
        sql, bound = query.bind(params, extracted)
        hydrate = query._hydrate
        observer = self.observer
        with acquire() as conn:
            # Timed from here: the observer's contract is the driver round trip.
            start = perf_counter() if observer is not None else 0.0
            try:
                rows, description = self.driver.fetch(conn, sql, bound, hydrate is None)
            except self.driver.errors as err:
                raise self._wrap(err, sql, bound)
        if hydrate is None:
            hydrate = query.hydrator(self.dialect, description)
        self._observe(observer, sql, start, len(rows))
        return rows, hydrate

    def _chunks(self, query: CoreQuery[Any], params: dict[str, Any], extracted: Any,
                default_chunk: int, acquire: Any) -> Any:
        """A factory of async chunk iterators for `Connection.stream()`, sized by what
        SQLAlchemy asks for (`partitions(50)`) or the caller's `chunk=`.
        """

        def chunks(size: int | None) -> Iterator[list[Any]]:
            # Not `size or default`: an explicit 0 must reach the guard below.
            wanted = size if size is not None else default_chunk
            if wanted < 1:
                raise ConfigurationError(f"chunk must be at least 1, got {wanted}")
            sql, bound = query.bind(params, extracted)
            observer = self.observer
            start = perf_counter() if observer is not None else 0.0
            total = 0
            with acquire() as conn:
                try:
                    for rows, description in self.driver.stream(
                        conn, sql, bound, wanted, query
                    ):
                        hydrate = query._hydrate
                        if hydrate is None:
                            hydrate = query.hydrator(self.dialect, description)
                        total += len(rows)
                        yield hydrate(rows)
                except self.driver.errors as err:
                    raise self._wrap(err, sql, bound)
            self._observe(observer, sql, start, total)

        return chunks

    def _observe(self, observer: Observer | None, sql: str, start: float, rows: int | None) -> None:
        """Hand one completed statement to `observer`. Captured by the caller at start,
        not re-read here, so an observer attached mid-statement cannot see a 0.0 start.
        Timing is the driver round trip, not hydration.
        """
        if observer is not None:
            observer(sql, perf_counter() - start, rows)
