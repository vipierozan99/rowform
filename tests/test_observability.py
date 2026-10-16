"""The two ways to see what the library is doing: an `observer` per statement,
and `logging.getLogger("rowform")` at DEBUG.

Both exist because a service needs to answer "which query was slow" without
reaching into a `CoreQuery`, and because there was previously no way to answer it
at all — the library imported `logging` nowhere.

The hook is on the caller's path, so the cost of *not* using it is part of the
contract: `observer=None` is one attribute load and a branch per statement, and
nothing per row.
"""

from __future__ import annotations

import contextvars
import logging
from contextlib import aclosing, contextmanager

import pytest
import sqlalchemy as sa
from conftest import Author, seed, sqlite_db


class TestObserver:
    async def test_a_read_reports_sql_duration_and_row_count(self, engine):
        seen: list[tuple[str, float, int | None]] = []
        engine.observer = lambda *call: seen.append(call)

        rows = await engine.fetch_all(sa.select(Author).order_by(Author.id))

        assert len(seen) == 1
        sql, seconds, count = seen[0]
        assert "t_authors" in sql
        assert 0 <= seconds < 10
        assert count == len(rows)

    async def test_a_write_reports_none_for_the_row_count(self, engine):
        """`execute()` already returns the driver's own report, which is a rowcount
        on some drivers and a status tag on others; the observer does not
        normalise it."""
        seen: list[tuple[str, float, int | None]] = []
        engine.observer = lambda *call: seen.append(call)

        await engine.execute(
            sa.insert(Author.__table__).values(id=7001, name="edsger", active=True)
        )

        assert len(seen) == 1
        assert seen[0][2] is None

    async def test_execute_many_is_reported_once(self, engine):
        seen: list[tuple[str, float, int | None]] = []
        engine.observer = lambda *call: seen.append(call)

        await engine.execute_many(
            sa.insert(Author.__table__),
            [
                {"id": 7101, "name": "alan", "active": True},
                {"id": 7102, "name": "grace", "active": True},
            ],
        )

        assert len(seen) == 1

    async def test_statements_inside_a_transaction_are_reported(self, engine):
        """Transaction writes go straight to the driver hooks rather than through
        `Engine.execute`, so they need their own call — the case a hook wired only
        into the engine would silently miss."""
        seen: list[tuple[str, float, int | None]] = []
        engine.observer = lambda *call: seen.append(call)

        async with engine.begin() as conn:
            await conn.execute(
                sa.insert(Author.__table__).values(id=7201, name="ken", active=True)
            )
            await conn.fetch_all(sa.select(Author))

        assert len(seen) == 2

    async def test_an_abandoned_stream_is_still_reported(self, engine):
        """`fetch_iter` reports once, at the end, with the total row count — and
        the line that did it sat after the loop, so a consumer that gave up half
        way never reached it. An abandoned export is exactly the stream an
        observer is for. It now reports from a `finally`.

        `aclosing` because that is what makes an abandoned async generator finish
        *here* rather than whenever it is collected — the same reason it is the
        right way to leave `fetch_iter` early at all.
        """
        seen: list[tuple[str, float, int | None]] = []
        engine.observer = lambda *call: seen.append(call)

        rows = engine.fetch_iter(sa.select(Author).order_by(Author.id), chunk=1)
        async with aclosing(rows):
            async for _ in rows:
                break

        assert len(seen) == 1
        sql, duration, delivered = seen[0]
        assert "SELECT" in sql.upper()
        assert duration >= 0
        # What was delivered, not what the statement would have produced.
        assert delivered == 1

    async def test_it_can_be_passed_to_the_constructor(self, sqlite_path):
        seen: list[tuple[str, float, int | None]] = []
        async with sqlite_db(sqlite_path, observer=lambda *c: seen.append(c)) as db:
            await seed(db)
            await db.fetch_all(sa.select(Author))
        assert seen

    async def test_none_is_the_default_and_nothing_breaks(self, engine):
        assert engine.observer is None
        assert await engine.fetch_all(sa.select(Author))

    async def test_an_observer_attached_mid_statement_reports_no_uptime(self, sqlite_engine):
        """The observer is captured once, at the start of the operation. If it was
        None then, attaching one before the statement finishes must not make
        `_observe` fire with a `start` of 0.0 and report the whole process uptime
        as the duration — the statement began before any observer existed, so it
        is simply not reported (F4)."""
        seen: list[tuple[str, float, int | None]] = []
        original = sqlite_engine.driver.fetch

        async def attach_then_fetch(*args, **kwargs):
            sqlite_engine.observer = lambda *call: seen.append(call)
            return await original(*args, **kwargs)

        sqlite_engine.driver.fetch = attach_then_fetch
        assert sqlite_engine.observer is None

        await sqlite_engine.fetch_all(sa.select(Author))
        assert seen == [], "the in-flight statement predates the observer"

        # The next statement is wholly inside the observer's lifetime, so it is
        # reported — with a real duration, not the uptime the bug produced.
        await sqlite_engine.fetch_all(sa.select(Author))
        assert len(seen) == 1
        assert 0 <= seen[0][1] < 10

    async def test_a_slow_query_log_is_the_shape_this_is_for(self, engine, caplog):
        """The motivating use case, wired end to end."""
        log = logging.getLogger("myapp.sql")
        threshold = 0.0  # every statement, so the assertion is not timing-dependent

        def warn_if_slow(sql: str, seconds: float, rows: int | None) -> None:
            if seconds >= threshold:
                log.warning("slow query %.6fs rows=%s: %s", seconds, rows, sql)

        engine.observer = warn_if_slow
        with caplog.at_level(logging.WARNING, logger="myapp.sql"):
            await engine.fetch_all(sa.select(Author))

        assert any("slow query" in record.message for record in caplog.records)


_CURRENT: contextvars.ContextVar[str | None] = contextvars.ContextVar("span", default=None)


class Spans:
    """A stand-in tracer: what OpenTelemetry's `start_as_current_span` does to the
    context, plus a record of what each span saw."""

    def __init__(self):
        self.spans: list[dict] = []

    @contextmanager
    def __call__(self, sql: str):
        span = {"sql": sql, "error": None}
        self.spans.append(span)
        token = _CURRENT.set(sql)
        try:
            yield span
        except BaseException as err:
            span["error"] = err
            raise
        finally:
            _CURRENT.reset(token)


class TestAround:
    async def test_every_round_trip_gets_a_span(self, engine):
        spans = Spans()
        engine.around = spans
        await engine.fetch_all(sa.select(Author))
        await engine.execute(sa.insert(Author.__table__).values(id=7301, name="a", active=True))
        await engine.execute_many(
            sa.insert(Author.__table__),
            [{"id": 7302, "name": "b", "active": True}, {"id": 7303, "name": "c", "active": True}],
        )
        async with engine.begin() as conn:
            await conn.execute(sa.delete(Author.__table__).where(Author.id > 7300))
        assert [s["sql"].split()[0] for s in spans.spans] == ["SELECT", "INSERT", "INSERT", "DELETE"]
        assert _CURRENT.get() is None

    async def test_a_failed_statement_is_seen_by_the_span(self, engine):
        spans = Spans()
        engine.around = spans
        with pytest.raises(sa.exc.IntegrityError):
            await engine.execute(sa.insert(Author.__table__).values(id=1, name="dup", active=True))
        assert isinstance(spans.spans[-1]["error"], sa.exc.IntegrityError)

    async def test_a_stream_is_never_in_a_span_while_the_consumer_runs(self, engine):
        """Held across a yield, the span would be current for the caller's own work."""
        spans = Spans()
        engine.around = spans
        outside: list[str | None] = []
        statement = sa.select(Author).order_by(Author.id)
        async with engine.connect() as conn, aclosing(conn.fetch_iter(statement, chunk=2)) as rows:
            async for _ in rows:
                outside.append(_CURRENT.get())
        assert outside == [None] * 4
        assert len(spans.spans) >= 2, "one span per chunk round trip"

    async def test_the_execute_stream_is_spanned_per_chunk_too(self, engine):
        spans = Spans()
        engine.around = spans
        async with engine.connect() as conn:
            result = await conn.stream(sa.select(Author.name).order_by(Author.id), chunk=2)
            names = [row[0] async for row in result]
        assert names == ["ada", "brian", "carol", "dan"]
        assert len(spans.spans) >= 2

    async def test_it_can_be_passed_to_the_constructor(self, sqlite_path):
        spans = Spans()
        async with sqlite_db(sqlite_path, around=spans) as db:
            await seed(db)
            await db.fetch_all(sa.select(Author))
        assert any(s["sql"].startswith("SELECT") for s in spans.spans)


class TestLogging:
    async def test_compiling_and_hydrating_are_logged_at_debug(self, sqlite_path, caplog):
        async with sqlite_db(sqlite_path) as db:
            await seed(db)
            with caplog.at_level(logging.DEBUG, logger="rowform"):
                await db.fetch_all(sa.select(Author).limit(1))

        messages = [r.message for r in caplog.records if r.name == "rowform"]
        assert any("compiled:" in m for m in messages)
        assert any("hydrator built:" in m for m in messages)

    async def test_the_logged_hydrator_is_the_generated_source(self, sqlite_path, caplog):
        """The codegen is inspectable from a log, not only from `__source__`."""
        async with sqlite_db(sqlite_path) as db:
            await seed(db)
            with caplog.at_level(logging.DEBUG, logger="rowform"):
                await db.fetch_all(sa.select(Author).limit(1))

        built = [r.message for r in caplog.records if "hydrator built:" in r.message]
        assert built
        assert "def _hydrate(rows):" in built[0]

    async def test_nothing_is_logged_above_debug(self, engine, caplog):
        """A library that chatters at INFO is a library people disable."""
        with caplog.at_level(logging.INFO, logger="rowform"):
            await engine.fetch_all(sa.select(Author))
        assert [r for r in caplog.records if r.name == "rowform"] == []
