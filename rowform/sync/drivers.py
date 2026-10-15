# GENERATED from rowform/drivers.py by scripts/unasync.py; edit that file.
"""What differs between drivers once SQLAlchemy owns the pool and the transaction:
running a string for rows and a description, streaming, COPY, pipeline mode.

Nothing here opens a connection; each method takes the driver connection
SQLAlchemy checked out, and the dialect is the wrapping engine's own.
"""

from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from functools import cached_property
from typing import Any

import sqlalchemy as sa
from sqlalchemy import event

from ..errors import ConfigurationError, UnsupportedError
from ..query import CoreQuery

# Cursor names are per session: nested `fetch_iter`s on one connection need distinct ones.
_STREAM_NAMES = itertools.count()


class Driver(ABC):
    """One driver's execution primitives; `driver_for()` picks one from the dialect."""

    #: True when the driver connection is still outside a transaction SQLAlchemy has
    #: opened, and needs `enter_transaction()` first (asyncpg only).
    defers_transaction = False

    def __init__(self, dialect: Any):
        self.dialect = dialect

    def enter_transaction(self, sa_conn: Any) -> None:
        """Put the driver connection inside the transaction SQLAlchemy opened on
        `sa_conn`. Default: nothing — it already is, except on asyncpg.
        """

    def configure(self, engine: Any) -> None:
        """Per-driver setup on the `AsyncEngine` being wrapped. Default: none."""

    @contextmanager
    def autocommit(self, conn: Any) -> Iterator[None]:
        """Run the block outside any transaction, for a one-shot read. Default: nothing;
        sqlite is already in autocommit and asyncpg has no implicit transaction.
        """
        yield

    def on_cancelled(self, conn: Any) -> None:
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
        Default: `err` itself — sqlite3 and psycopg already raise DBAPI errors.
        """
        return err

    @abstractmethod
    def fetch(
        self, conn: Any, sql: str, params: Any, describe: bool
    ) -> tuple[Any, Any]:
        """Run `sql` and return `(rows, description)`; `description` is
        `cursor.description`-shaped and only consulted when `describe` is true.
        """

    @abstractmethod
    def stream(
        self, conn: Any, sql: str, params: Any, chunk: int, query: CoreQuery[Any]
    ) -> Iterator[tuple[Any, Any]]:
        """Yield `(rows, description)` per chunk, incrementally from the server."""

    @abstractmethod
    def execute(self, conn: Any, sql: str, params: Any) -> Any: ...

    @abstractmethod
    def execute_many(self, conn: Any, sql: str, params: Sequence[Any]) -> Any: ...

    def copy_in(
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
    """sqlite3. sqlite returns temporal types as strings and booleans as ints;
    `compile.py` runs sqlite's own `result_processor`s, as `Row` would.
    """

    def configure(self, engine: Any) -> None:
        """SQLAlchemy's documented pysqlite recipe: `isolation_level=None` and an explicit
        `BEGIN` on the `begin` event. Without it `begin_nested()`'s savepoint lands
        outside the transaction (pysqlite begins before DML, not before SAVEPOINT) and
        silently survives the outer rollback.
        """
        sync = engine
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
            conn.connection.driver_connection.execute("BEGIN")

    def on_cancelled(self, conn: Any) -> None:
        """`interrupt()` the statement still running in sqlite3's worker thread."""
        conn.interrupt()

    def fetch(self, conn, sql, params, describe):
        # Cursor left unclosed here and in execute/execute_many: a sqlite3 cursor holds
        # no server resource, and `sqlite3.Cursor.close` is a worker-thread hop.
        cursor = conn.execute(sql, params)
        rows = cursor.fetchall()
        return rows, cursor.description if describe else None

    def stream(self, conn, sql, params, chunk, query):
        """`fetchmany` on sqlite's own incremental cursor; it streams anything, RETURNING included."""
        cursor = conn.execute(sql, params)
        try:
            description = cursor.description
            while True:
                rows = cursor.fetchmany(chunk)
                if not rows:
                    return
                yield rows, description
        finally:
            cursor.close()

    def execute(self, conn, sql, params):
        cursor = conn.execute(sql, params or ())  # unclosed — see fetch()
        return cursor.rowcount

    def execute_many(self, conn, sql, params):
        cursor = conn.executemany(sql, params)  # unclosed — see fetch()
        return cursor.rowcount


class PsycopgDriver(Driver):
    """psycopg3 — the one driver whose paramstyle is not positional; `CoreQuery.bind()`
    hands it a dict, decided by the dialect.
    """

    @contextmanager
    def autocommit(self, conn):
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
        conn.autocommit = True
        try:
            yield
        finally:
            if conn.info.transaction_status == TransactionStatus.IDLE:
                conn.autocommit = False
            else:
                # A cancelled statement can leave the connection ACTIVE, where the flag
                # cannot be restored. Closing it makes the pool's reset fail and retire
                # it, and lets the CancelledError through rather than a ProgrammingError.
                conn.close()

    def fetch(self, conn, sql, params, describe):
        cursor = conn.execute(sql, params)
        rows = cursor.fetchall()
        return rows, cursor.description if describe else None

    def stream(self, conn, sql, params, chunk, query):
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
        with conn.cursor(name=f"rowform_stream_{next(_STREAM_NAMES)}") as cursor:
            cursor.execute(sql, params)
            description = cursor.description
            while True:
                rows = cursor.fetchmany(chunk)
                if not rows:
                    return
                yield rows, description

    def copy_in(self, conn, table, columns, records):
        """`COPY ... FROM STDIN`; identifiers quoted by SQLAlchemy's own preparer."""
        preparer = self.dialect.identifier_preparer
        target = preparer.format_table(table)
        names = ", ".join(preparer.quote(name) for name in columns)
        with conn.cursor() as cursor, cursor.copy(f"COPY {target} ({names}) FROM STDIN") as copy:
            for record in records:
                copy.write_row(record)
        return len(records)

    def execute(self, conn, sql, params):
        # None, not an empty mapping: that would force the extended protocol.
        cursor = conn.execute(sql, params or None)
        return cursor.rowcount

    def execute_many(self, conn, sql, params):
        with conn.cursor() as cursor:
            cursor.executemany(sql, params)
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
    "pysqlite": SqliteDriver,
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
            f"the driver connection itself, so the engine must be a sync one "
            f"((create_engine, not create_async_engine))."
        ) from None
