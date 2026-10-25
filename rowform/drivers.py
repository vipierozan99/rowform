"""What differs between drivers once SQLAlchemy owns the pool and the transaction:
running a string for rows and a description, streaming, COPY, pipeline mode, and
recovering from a statement prepared before the schema changed.

Nothing here opens a connection; each method takes `native()` of the connection
SQLAlchemy checked out, and the dialect is the wrapping engine's own.
"""

from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from contextlib import asynccontextmanager
from functools import cached_property
from typing import Any

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.util import await_only

from .errors import ConfigurationError, UnsupportedError
from .query import CoreQuery

# Cursor names are per session: nested `fetch_iter`s on one connection need distinct ones.
_STREAM_NAMES = itertools.count()


class Driver(ABC):
    """One driver's execution primitives; `driver_for()` picks one from the dialect."""

    #: True when the driver connection is still outside a transaction SQLAlchemy has
    #: opened, and needs `enter_transaction()` first (asyncpg only).
    defers_transaction = False

    def __init__(self, dialect: Any):
        self.dialect = dialect

    def native(self, dbapi_connection: Any) -> Any:
        """What this driver's methods take, given SQLAlchemy's adapted DBAPI connection:
        the driver connection under it, except where the adapter is needed (asyncpg).
        """
        return dbapi_connection.driver_connection

    def is_stale(self, err: Exception) -> bool:
        """Whether `err` is postgres refusing a statement prepared before the schema
        changed under it, the errors Core's asyncpg dialect invalidates its caches on.
        Default: never; sqlite prepares again by itself.
        """
        return False

    def invalidate(self, conn: Any) -> None:
        """Forget what was prepared against the old schema, after `is_stale`."""

    def in_transaction(self, conn: Any) -> bool:
        """Whether a statement that failed on `conn` failed inside a transaction, which
        it aborted, so that it cannot simply run again.
        """
        return True

    @staticmethod
    def type_codes(description: Any) -> list[Any]:
        """The DBAPI type code of each column `fetch`/`stream` described."""
        return [column[1] for column in description]

    async def enter_transaction(self, sa_conn: Any) -> None:
        """Put the driver connection inside the transaction SQLAlchemy opened on
        `sa_conn`. Default: nothing — it already is, except on asyncpg.
        """

    def configure(self, engine: Any) -> None:
        """Per-driver setup on the `AsyncEngine` being wrapped. Default: none."""

    @asynccontextmanager
    async def autocommit(self, conn: Any) -> AsyncIterator[None]:
        """Run the block outside any transaction, for a one-shot read. Default: nothing;
        sqlite is already in autocommit and asyncpg has no implicit transaction.
        """
        yield

    async def on_cancelled(self, conn: Any) -> None:
        """Called while unwinding a `CancelledError`, before the connection goes back
        to the pool. Default: nothing; asyncpg and psycopg cancel server-side.
        """

    @cached_property
    def errors(self) -> tuple[type[Exception], ...]:
        """What `fetch`/`execute`/... raise for a server or connection error; `translate()`
        turns each into the DBAPI exception SQLAlchemy's adapter would have raised.
        """
        return (self.dialect.loaded_dbapi.Error,)

    def translate(self, err: Exception) -> Exception:
        """The DBAPI exception SQLAlchemy's own adapter would have raised for `err`.
        Default: `err` itself — aiosqlite and psycopg already raise DBAPI errors.
        """
        return err

    @abstractmethod
    async def fetch(
        self, conn: Any, sql: str, params: Any, describe: bool
    ) -> tuple[Any, Any]:
        """Run `sql` and return `(rows, description)`, for `type_codes`. `description`
        may be None unless `describe` is true; a driver whose server can change a
        column's type under a cached statement returns one every time.
        """

    @abstractmethod
    def stream(
        self, conn: Any, sql: str, params: Any, chunk: int, query: CoreQuery[Any]
    ) -> AsyncGenerator[tuple[Any, Any], None]:
        """Yield `(rows, description)` per chunk, incrementally from the server."""

    @abstractmethod
    async def execute(self, conn: Any, sql: str, params: Any) -> Any: ...

    @abstractmethod
    async def execute_many(self, conn: Any, sql: str, params: Sequence[Any]) -> Any: ...

    async def copy_in(
        self, conn: Any, table: sa.Table, columns: Sequence[str], records: Sequence[tuple]
    ) -> int:
        """Per-driver COPY. Default: refused; only postgres has one."""
        raise UnsupportedError(
            f"{self.dialect.driver} has no COPY path — that is a postgres feature. "
            f"Use execute_many() instead."
        )

    def pipeline(self, conn: Any) -> Any:
        """Per-driver pipeline mode. Default: refused; only psycopg has one."""
        raise UnsupportedError(
            f"{self.dialect.driver} has no pipeline mode. psycopg3 is the only "
            f"driver here that implements one; asyncpg has no such API, and "
            f"sqlite is a local file with no round trip to hide."
        )


class SqliteDriver(Driver):
    """aiosqlite. sqlite returns temporal types as strings and booleans as ints;
    `compile.py` runs sqlite's own `result_processor`s, as `Row` would.
    """

    def configure(self, engine: Any) -> None:
        """SQLAlchemy's documented pysqlite recipe: `isolation_level=None` and an explicit
        `BEGIN` on the `begin` event. Without it `begin_nested()`'s savepoint lands
        outside the transaction (pysqlite begins before DML, not before SAVEPOINT) and
        silently survives the outer rollback.
        """
        sync = engine.sync_engine
        if getattr(sync, "_rowform_sqlite_configured", False):
            return
        sync._rowform_sqlite_configured = True

        @event.listens_for(sync, "connect")
        def _no_implicit_begin(dbapi_connection: Any, _record: Any) -> None:
            dbapi_connection.isolation_level = None

        @event.listens_for(sync, "begin")
        def _explicit_begin(conn: Any) -> None:
            # Driver connection: one worker-thread hop, not the cursor adapter's three.
            # `await_only` is safe: rowform is async-only, so this runs in a greenlet.
            await_only(conn.connection.driver_connection.execute("BEGIN"))

    async def on_cancelled(self, conn: Any) -> None:
        """`interrupt()` the statement still running in aiosqlite's worker thread."""
        await conn.interrupt()

    async def fetch(self, conn, sql, params, describe):
        # Cursor left unclosed here and in execute/execute_many: a sqlite3 cursor holds
        # no server resource, and `aiosqlite.Cursor.close` is a worker-thread hop.
        cursor = await conn.execute(sql, params)
        rows = await cursor.fetchall()
        return rows, cursor.description if describe else None

    async def stream(self, conn, sql, params, chunk, query):
        """`fetchmany` on sqlite's own incremental cursor; it streams anything, RETURNING included."""
        cursor = await conn.execute(sql, params)
        try:
            description = cursor.description
            while True:
                rows = await cursor.fetchmany(chunk)
                if not rows:
                    return
                yield rows, description
        finally:
            await cursor.close()

    async def execute(self, conn, sql, params):
        cursor = await conn.execute(sql, params or ())  # unclosed — see fetch()
        return cursor.rowcount

    async def execute_many(self, conn, sql, params):
        cursor = await conn.executemany(sql, params)  # unclosed — see fetch()
        return cursor.rowcount


class AsyncpgDriver(Driver):
    """asyncpg. JSON codecs are registered by SQLAlchemy's dialect `on_connect`.

    Statements are prepared by SQLAlchemy's adapter, through the per-connection cache
    Core's cursor uses (`_prepare`, SQLAlchemy-private): `prepared_statement_cache_size`
    and `prepared_statement_name_func` apply to them, and so does the dialect-wide
    invalidation Core starts after DDL or a stale statement. asyncpg's own cache,
    sized by `statement_cache_size`, serves only `executemany` and introspection, as
    under Core.
    """

    defers_transaction = True

    def native(self, dbapi_connection):
        return dbapi_connection

    async def _prepare(self, conn: Any, sql: str) -> tuple[Any, Any]:
        """`(prepared statement, attributes)`, as Core's cursor gets them."""
        return await conn._prepare(sql, self.dialect._invalidate_schema_cache_asof)

    def _cached(self, conn: Any, sql: str) -> tuple[Any, Any] | None:
        """What `_prepare` would answer without preparing, or None: its cache entry is
        `(statement, attributes, prepared at)`, current if prepared after the last
        invalidation. Also SQLAlchemy-private.
        """
        cache = conn._prepared_statement_cache
        entry = cache.get(sql) if cache is not None else None
        if entry is not None and entry[2] > self.dialect._invalidate_schema_cache_asof:
            return entry[0], entry[1]
        return None

    @cached_property
    def _stale(self) -> tuple[type[Exception], ...]:
        exceptions = self.dialect.loaded_dbapi.asyncpg.exceptions
        return (exceptions.InvalidCachedStatementError, exceptions.InternalServerError)

    def is_stale(self, err):
        return isinstance(err, self._stale)

    def invalidate(self, conn):
        """What Core's execution context does on the same errors: every connection's
        cached statements and asyncpg type caches are rebuilt before their next use.
        """
        self.dialect._invalidate_schema_cache()

    def in_transaction(self, conn):
        return conn.driver_connection.is_in_transaction()

    @staticmethod
    def type_codes(description):
        return [attribute.type.oid for attribute in description]

    async def enter_transaction(self, sa_conn):
        """Start the asyncpg transaction SQLAlchemy's `begin()` only promised.

        SQLAlchemy's adapter starts it lazily on the first statement through *its own*
        cursor, which rowform never uses — so without this a scope had no atomicity.
        Driving the adapter's private `_started`/`_start_transaction` keeps its
        `commit()`/`rollback()` in charge of ending it.
        """
        adapted = sa_conn.sync_connection.connection.dbapi_connection
        if not adapted._started:
            await adapted._start_transaction()

    @cached_property
    def errors(self):
        return tuple(self.dialect.loaded_dbapi._asyncpg_error_translate)

    def translate(self, err):
        """SQLAlchemy's asyncpg adapter keeps this map; asyncpg's own errors are not DBAPI
        ones. Every class in `errors` is a key, so the walk always finds one.
        """
        mapping = self.dialect.loaded_dbapi._asyncpg_error_translate
        for cls in type(err).__mro__:
            if cls in mapping:
                translated = mapping[cls](f"{type(err)}: {err}")
                translated.pgcode = translated.sqlstate = getattr(err, "sqlstate", None)
                translated.__cause__ = err
                return translated
        raise AssertionError(f"{type(err)} is not in the asyncpg error map") from err

    async def fetch(self, conn, sql, params, describe):
        """The description is the attributes cached with the statement, so it is free.

        A statement that has to be prepared outside a transaction is prepared and run
        inside one, as Core always does: behind a transaction pooler, a type lookup
        asyncpg makes between the two hands the client a different server, which never
        saw the statement. A cached one runs as it is, in one round trip.
        """
        cached = self._cached(conn, sql)
        if cached is None and not conn.driver_connection.is_in_transaction():
            async with conn.driver_connection.transaction():
                prepared, attributes = await self._prepare(conn, sql)
                return await prepared.fetch(*params), attributes
        prepared, attributes = cached or await self._prepare(conn, sql)
        return await prepared.fetch(*params), attributes

    async def stream(self, conn, sql, params, chunk, query):
        """A portal over the prepared statement; asyncpg needs a transaction for one."""
        async with conn.driver_connection.transaction():
            prepared, attributes = await self._prepare(conn, sql)
            cursor = await prepared.cursor(*params)
            while True:
                rows = await cursor.fetch(chunk)
                if not rows:
                    return
                yield rows, attributes

    async def copy_in(self, conn, table, columns, records):
        """asyncpg's binary COPY; it encodes values with the same codecs a query would."""
        # `schema=None` stays unqualified so `search_path` resolves it, as psycopg's does.
        await conn.driver_connection.copy_records_to_table(
            table.name,
            records=records,
            columns=list(columns),
            schema_name=table.schema,
        )
        return len(records)

    async def execute(self, conn, sql, params):
        """Returns asyncpg's status tag (`"INSERT 0 3"`), the driver's own report. With no
        parameters it is one simple-protocol round trip and nothing is prepared.
        """
        if not params:
            return await conn.driver_connection.execute(sql)
        prepared, _ = await self._prepare(conn, sql)
        await prepared.fetch(*params)
        return prepared.get_statusmsg()

    async def execute_many(self, conn, sql, params):
        """asyncpg's `executemany` reports nothing, so `Result.rowcount` is `-1` here.
        Its statement is asyncpg's to cache, as under Core, after the same invalidation check.
        """
        await conn._check_type_cache_invalidation(self.dialect._invalidate_schema_cache_asof)
        return await conn.driver_connection.executemany(sql, params)


class PsycopgDriver(Driver):
    """psycopg3 — the one driver whose paramstyle is not positional; `CoreQuery.bind()`
    hands it a dict, decided by the dialect.
    """

    @asynccontextmanager
    async def autocommit(self, conn):
        """psycopg's connection is transactional by default, so a bare SELECT costs a
        `BEGIN` and the pool's reset a `ROLLBACK`. `autocommit` is a local flag while
        idle. Left alone when the caller's engine already runs in autocommit, or when
        a pool `checkout` listener already opened a transaction (the flag cannot
        change inside one, and that transaction is the caller's to keep).
        """
        from psycopg.pq import TransactionStatus

        if conn.autocommit or conn.info.transaction_status != TransactionStatus.IDLE:
            yield
            return
        await conn.set_autocommit(True)
        try:
            yield
        finally:
            if conn.info.transaction_status == TransactionStatus.IDLE:
                await conn.set_autocommit(False)
            else:
                # A cancelled statement can leave the connection ACTIVE, where the flag
                # cannot be restored. Closing it makes the pool's reset fail and retire
                # it, and lets the CancelledError through rather than a ProgrammingError.
                await conn.close()

    def is_stale(self, err: Any) -> bool:
        """The two errors asyncpg classifies as `InvalidCachedStatementError` and
        `InternalServerError`, by SQLSTATE.
        """
        state = getattr(err, "sqlstate", None)
        return state == "XX000" or (
            state == "0A000"
            and err.diag.message_primary == "cached plan must not change result type"
        )

    def invalidate(self, conn):
        """What the `ROLLBACK` ending a transaction does: psycopg forgets its prepared
        statements and deallocates them with its next one. A one-shot read in
        autocommit never rolls back, so without this a statement prepared before a
        type change fails on that connection for good. `_prepared` is psycopg-private.
        """
        conn._prepared.clear()

    def in_transaction(self, conn):
        from psycopg.pq import TransactionStatus

        return conn.info.transaction_status != TransactionStatus.IDLE

    @staticmethod
    def type_codes(description):
        return description

    async def fetch(self, conn, sql, params, describe):
        """The description is the result's type OIDs: `cursor.description` builds a
        `Column` per field, which costs far more and holds nothing else needed.
        """
        cursor = await conn.execute(sql, params)
        rows = await cursor.fetchall()
        return rows, _type_oids(cursor)

    async def stream(self, conn, sql, params, chunk, query):
        """A named (server-side) cursor; the unnamed one reads every row into the client
        first. postgres will not DECLARE one for a write with RETURNING.
        """
        if not query.is_select:
            raise UnsupportedError(
                "psycopg streams through a server-side cursor, and postgres will "
                "only DECLARE one for a SELECT — not for a write with RETURNING. "
                "Use fetch_all() for this statement, or the asyncpg driver, which "
                "streams it through a portal."
            )
        async with conn.cursor(name=f"rowform_stream_{next(_STREAM_NAMES)}") as cursor:
            await cursor.execute(sql, params)
            while True:
                rows = await cursor.fetchmany(chunk)
                if not rows:
                    return
                yield rows, _type_oids(cursor)

    async def copy_in(self, conn, table, columns, records):
        """`COPY ... FROM STDIN`; identifiers quoted by SQLAlchemy's own preparer."""
        preparer = self.dialect.identifier_preparer
        target = preparer.format_table(table)
        names = ", ".join(preparer.quote(name) for name in columns)
        async with conn.cursor() as cursor, cursor.copy(f"COPY {target} ({names}) FROM STDIN") as copy:
            for record in records:
                await copy.write_row(record)
        return len(records)

    async def execute(self, conn, sql, params):
        # None, not an empty mapping: that would force the extended protocol.
        cursor = await conn.execute(sql, params or None)
        return cursor.rowcount

    async def execute_many(self, conn, sql, params):
        async with conn.cursor() as cursor:
            await cursor.executemany(sql, params)
            return cursor.rowcount

    def pipeline(self, conn: Any) -> Any:
        """psycopg's pipeline mode; needs libpq 14+, so it is checked rather than assumed."""
        import psycopg

        supported = getattr(getattr(psycopg, "capabilities", None), "has_pipeline", None)
        available = supported() if supported is not None else psycopg.Pipeline.is_supported()
        if not available:
            raise UnsupportedError(
                "this psycopg build has no pipeline mode; it needs libpq 14 or newer"
            )
        return conn.pipeline()


def _type_oids(cursor: Any) -> list[int]:
    result = cursor.pgresult
    return [result.ftype(i) for i in range(result.nfields)]


#: Keyed by `dialect.driver`, the name after the `+` in the URL.
DRIVERS: dict[str, type[Driver]] = {
    "aiosqlite": SqliteDriver,
    "asyncpg": AsyncpgDriver,
    "psycopg": PsycopgDriver,
}


def driver_for(dialect: Any) -> Driver:
    """The execution primitives for the dialect. Refuses a sync driver: rowform
    awaits the driver connection directly, so there has to be one.
    """
    try:
        return DRIVERS[dialect.driver](dialect)
    except KeyError:
        raise ConfigurationError(
            f"no rowform driver for {dialect.name}+{dialect.driver}; "
            f"supported: {', '.join(sorted(DRIVERS))}. rowform runs statements on "
            f"the driver connection itself, so the engine must be an async one "
            f"(create_async_engine, not create_engine)."
        ) from None
