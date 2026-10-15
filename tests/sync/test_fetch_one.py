"""`fetch_one` asks the server for one row.

It used to read the whole result and discard everything after the first, so
"get me this user" transferred and hydrated the entire table. The observer is the
instrument here: it reports how many rows the driver actually returned, which is
the only way to tell a `LIMIT 1` from a full scan that was thrown away.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sync.conftest import Author
from sqlalchemy.dialects.sqlite import pysqlite

import rowform


@pytest.fixture
def rows_returned(engine):
    """Row counts the driver reported, per statement."""
    seen: list[int | None] = []
    engine.observer = lambda sql, seconds, rows: seen.append(rows)
    return seen


class TestItLimits:
    def test_fetch_one_reads_one_row(self, engine, rows_returned):
        assert engine.fetch_one(sa.select(Author)) is not None
        assert rows_returned == [1]

    def test_a_scalar_select_reads_one_row_too(self, engine, rows_returned):
        """The narrowing is the statement's, not the model's, so it applies to a
        select that hydrates no model at all."""
        assert engine.fetch_one(sa.select(Author.name)) is not None
        assert rows_returned == [1]

    def test_the_limit_reaches_the_sql(self, engine):
        seen: list[str] = []
        engine.observer = lambda sql, seconds, rows: seen.append(sql)
        engine.fetch_one(sa.select(Author))
        assert "LIMIT" in seen[0].upper()

    def test_inside_a_transaction_too(self, engine, rows_returned):
        with engine.begin() as conn:
            conn.fetch_one(sa.select(Author))
        assert rows_returned == [1]

    def test_an_offset_is_still_narrowed(self, engine, rows_returned):
        """The first row of *that* statement is what was asked for."""
        ordered = sa.select(Author).order_by(Author.id)
        second = engine.fetch_one(ordered.offset(1))
        assert rows_returned == [1]
        assert second is not None
        assert second.id == (engine.fetch_all(ordered))[1].id


class TestWhatItLeavesAlone:
    def test_a_caller_s_own_limit_is_not_replaced(self, engine, rows_returned):
        """Replacing it would be harmless here but not in general — see the
        bindparam case below — so the rule is "narrow only when unlimited"."""
        engine.fetch_one(sa.select(Author).limit(3))
        assert rows_returned == [3]

    def test_a_bindparam_limit_still_binds(self, engine):
        """The case that makes an unconditional `.limit(1)` wrong: the caller's
        value would have nothing left to bind to."""
        statement = sa.select(Author).order_by(Author.id).limit(sa.bindparam("n"))
        assert engine.fetch_one(statement, n=2) is not None

    def test_a_compiled_query_is_untouched(self, engine):
        """A `CoreQuery` has no statement left to narrow; it must still work."""
        hoisted = engine.prepare(sa.select(Author).order_by(Author.id))
        assert (engine.fetch_one(hoisted)) is not None

    def test_a_hoisted_query_can_carry_its_own_limit(self, engine, rows_returned):
        """What to do instead, for the hoisted case."""
        hoisted = engine.prepare(sa.select(Author).order_by(Author.id).limit(1))
        assert engine.fetch_one(hoisted) is not None
        assert rows_returned == [1]

    def test_no_match_is_still_none(self, engine):
        assert engine.fetch_one(sa.select(Author).where(Author.name == "no")) is None
        assert engine.fetch_one(sa.select(Author.name).where(Author.id < 0)) is None


class TestUnit:
    @pytest.mark.parametrize(
        "statement",
        [
            sa.select(Author).limit(5),
            sa.select(Author).limit(sa.bindparam("n")),
        ],
    )
    def test_limited_statements_are_returned_unchanged(self, statement):
        from rowform.engine import _one_row

        assert _one_row(statement) is statement

    def test_an_unlimited_select_gains_a_limit(self):
        from rowform.engine import _one_row

        statement = sa.select(Author)
        narrowed = _one_row(statement)
        assert narrowed is not statement, "the caller's statement was mutated"
        assert "LIMIT" in str(narrowed).upper()
        assert "LIMIT" not in str(statement).upper()

    def test_a_core_query_passes_through(self):
        query = rowform.CoreQuery(sa.select(Author), pysqlite.dialect())
        from rowform.engine import _one_row

        assert _one_row(query) is query
