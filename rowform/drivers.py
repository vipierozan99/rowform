"""What differs between drivers once SQLAlchemy owns the pool and the transaction:
running a string for rows and a description, streaming, COPY, pipeline mode.

Nothing here opens a connection; each method takes the driver connection
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
        """Run `sql` and return `(rows, description)`; `description` is
        `cursor.description`-shaped and only consulted when `describe` is true.
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
    """asyncpg. JSON codecs are registered by SQLAlchemy's dialect `on_connect`."""

    defers_transaction = True

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
        if not describe:
            return await conn.fetch(sql, *params), None
        # No cursor, so no `description`: the type OIDs live on the prepared statement.
        prepared = await conn.prepare(sql)
        rows = await prepared.fetch(*params)
        description = [(a.name, a.type.oid) for a in prepared.get_attributes()]
        return rows, description

    async def stream(self, conn, sql, params, chunk, query):
        """A portal over the prepared statement; asyncpg needs a transaction for one."""
        async with conn.transaction():
            prepared = await conn.prepare(sql)
            description = [(a.name, a.type.oid) for a in prepared.get_attributes()]
            cursor = await prepared.cursor(*params)
            while True:
                rows = await cursor.fetch(chunk)
                if not rows:
                    return
                yield rows, description

    async def copy_in(self, conn, table, columns, records):
        """asyncpg's binary COPY; it encodes values with the same codecs a query would."""
        # `schema=None` stays unqualified so `search_path` resolves it, as psycopg's does.
        await conn.copy_records_to_table(
            table.name,
            records=records,
            columns=list(columns),
            schema_name=table.schema,
        )
        return len(records)

    async def execute(self, conn, sql, params):
        """Returns asyncpg's status tag (`"INSERT 0 3"`), the driver's own report."""
        return await conn.execute(sql, *(params or ()))

    async def execute_many(self, conn, sql, params):
        """asyncpg's `executemany` reports nothing, so `Result.rowcount` is `-1` here."""
        return await conn.executemany(sql, params)


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

    async def fetch(self, conn, sql, params, describe):
        cursor = await conn.execute(sql, params)
        rows = await cursor.fetchall()
        return rows, cursor.description if describe else None

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
            description = cursor.description
            while True:
                rows = await cursor.fetchmany(chunk)
                if not rows:
                    return
                yield rows, description

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
