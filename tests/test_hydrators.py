"""Codegen: what the generated hydrator does, and what it costs.

These tests read the generated source. That is on purpose — the source is
attached to the function as `__source__` precisely so the codegen is inspectable
rather than magic, and the properties that make it fast (one UNPACK_SEQUENCE per
row, `append` bound once, plain attribute stores rather than `setattr`) are only
observable there.
"""

from __future__ import annotations

import datetime as dt
import decimal

import pytest
import sqlalchemy as sa
from conftest import Author, Book, Wide
from sqlalchemy.dialects.sqlite import aiosqlite
from sqlalchemy.orm import Mapped

import rowform as rf

SQLITE = aiosqlite.dialect()


def build(statement, dialect=SQLITE):
    p = rf.plan(statement)
    return p, rf.compile_hydrator(p, dialect, [None] * len(p.columns))


class TestShape:
    def test_a_whole_model_is_not_wrapped(self):
        _, hydrate = build(sa.select(Author))
        [author] = hydrate([(1, "ada", 1)])
        assert isinstance(author, Author)
        assert (author.id, author.name, author.active) == (1, "ada", True)

    def test_two_models_become_a_tuple_each(self):
        _, hydrate = build(sa.select(Author, Book).join(Book))
        [(author, book)] = hydrate([(1, "ada", 1, 10, 1, "structures")])
        assert isinstance(author, Author)
        assert isinstance(book, Book)
        assert book.title == "structures"

    def test_model_plus_scalar(self):
        _, hydrate = build(sa.select(Author, Book.title).join(Book))
        [(author, title)] = hydrate([(1, "ada", 1, "structures")])
        assert author.name == "ada"
        assert title == "structures"

    def test_a_single_scalar_is_unwrapped_like_a_single_model(self):
        _, hydrate = build(sa.select(sa.func.count()).select_from(Author))
        assert hydrate([(5,)]) == [5]

    def test_a_single_scalar_row_is_not_nested(self):
        """The trailing comma in the generated `for f0, in rows:` is what makes
        this work; without it f0 would bind the whole row tuple and every value
        would come back as a 1-tuple."""
        _, hydrate = build(sa.select(Author.name))
        assert hydrate([("ada",), ("brian",)]) == ["ada", "brian"]

    def test_empty_input(self):
        _, hydrate = build(sa.select(Author))
        assert hydrate([]) == []


class TestOuterJoins:
    def test_an_all_null_entity_becomes_none(self):
        _, hydrate = build(sa.select(Author, Book).outerjoin(Book))
        [(author, book)] = hydrate([(4, "dan", 1, None, None, None)])
        assert author.name == "dan"
        assert book is None, "a missing match is None, not an object of Nones"

    def test_a_partially_null_entity_still_builds(self):
        _, hydrate = build(sa.select(Author, Book).outerjoin(Book))
        [(_, book)] = hydrate([(1, "ada", 1, 10, None, None)])
        assert isinstance(book, Book)
        assert book.id == 10

    def test_an_inner_joined_entity_is_not_null_checked(self):
        _, hydrate = build(sa.select(Author, Book).join(Book))
        assert " is None and " not in hydrate.__source__

    def test_every_column_of_an_outer_entity_is_checked(self):
        _, hydrate = build(sa.select(Author, Book).outerjoin(Book))
        test = [line for line in hydrate.__source__.splitlines() if " is None and " in line]
        assert len(test) == 1
        assert test[0].count(" is None") == 3  # Book has three columns


class TestProcessors:
    def test_a_column_needing_no_conversion_compiles_to_a_bare_store(self):
        _, hydrate = build(sa.select(Author.id, Author.name))
        assert "_p0(" not in hydrate.__source__
        assert "_p1(" not in hydrate.__source__

    def test_sqlite_boolean_gets_a_processor(self):
        _, hydrate = build(sa.select(Author))
        assert "active = _p2(f2)" in hydrate.__source__
        [author] = hydrate([(1, "ada", 1)])
        assert author.active is True

    def test_the_processors_come_from_sqlalchemy_per_column(self):
        """Not a hand-written type table: the same `result_processor` `Row` runs.
        On sqlite that is 8 of the 13 wide columns; on asyncpg it would be
        almost none, and each of those compiles to a bare store."""
        _, hydrate = build(sa.select(Wide))
        converted = {
            line.split(".")[1].split(" ")[0]
            for line in hydrate.__source__.splitlines()
            if "= _p" in line
        }
        assert converted == {"flag", "when", "day", "clock", "amount", "colour", "uid", "payload"}

    def test_result_processor_is_none_where_the_driver_already_decodes(self):
        assert rf.result_processor(Author.id, SQLITE, None) is None
        assert rf.result_processor(Author.active, SQLITE, None) is not None

    def test_a_datetime_round_trips_through_sqlite_s_string_storage(self):
        _, hydrate = build(sa.select(Wide.when))
        [when] = hydrate([("2024-03-01 12:30:45.123456",)])
        assert when == dt.datetime(2024, 3, 1, 12, 30, 45, 123456)


class TestGeneratedSource:
    def test_source_is_attached_for_inspection(self):
        _, hydrate = build(sa.select(Author))
        assert hydrate.__source__.startswith("def _hydrate(rows):")

    def test_the_row_is_unpacked_by_the_for_statement(self):
        _, hydrate = build(sa.select(Author))
        assert "for f0, f1, f2, in rows:" in hydrate.__source__

    def test_append_is_bound_once_outside_the_loop(self):
        _, hydrate = build(sa.select(Author))
        lines = hydrate.__source__.splitlines()
        assert lines[2].strip() == "append = out.append"

    def test_fields_are_plain_attribute_stores(self):
        """PEP 659 quickens `obj.x = v` into a cached STORE_ATTR; routing through
        `setattr` or a descriptor defeats that inline cache."""
        _, hydrate = build(sa.select(Author))
        assert "setattr" not in hydrate.__source__
        assert "o0.name = f1" in hydrate.__source__


def test_a_slotted_model_hydrates():
    """The hydrator fills instances with `object.__new__` plus plain attribute
    stores; a slotted model has no `__dict__` slot for those, so this checks the
    stores land in the slots rather than raising."""

    class Base(rf.Base):
        metadata = sa.MetaData()

    class Slotted(Base, slots=True):
        __tablename__ = "slotted_hydrate"
        id: Mapped[int] = rf.mapped_column(primary_key=True)
        name: Mapped[str]
        active: Mapped[bool]

    _, hydrate = build(sa.select(Slotted))
    [row] = hydrate([(1, "ada", 1)])
    assert isinstance(row, Slotted)
    assert (row.id, row.name, row.active) == (1, "ada", True)


def test_a_description_of_the_wrong_width_is_refused():
    """Rather than mis-assign fields: the driver's account of the result is the
    authority, and if it disagrees with the rf.plan something is wrong upstream."""
    p = rf.plan(sa.select(Author))
    with pytest.raises(ValueError, match="refusing to hydrate"):
        rf.compile_hydrator(p, SQLITE, [None, None])


class TestTypeCodes:
    """A hydrator is keyed by the DBAPI type codes it was built from: postgres
    `Numeric` decodes by them, and a migration can change them under a statement.
    """

    @pytest.fixture
    def query(self):
        from sqlalchemy.dialects.postgresql import psycopg

        dialect = psycopg.dialect()
        return dialect, rf.CoreQuery(sa.select(Wide.amount), dialect)

    def test_new_type_codes_build_a_new_hydrator(self, query):
        """`numeric(12, 3)` altered to `double precision`: still a `Decimal`, as from a fresh engine."""
        dialect, query = query
        numeric = query.hydrator(dialect, [("amount", 1700)], rf.Driver.type_codes)
        double = query.hydrator(dialect, [("amount", 701)], rf.Driver.type_codes)
        assert double is not numeric
        assert double([(1.5,)]) == [decimal.Decimal("1.500")]

    def test_equal_type_codes_keep_the_hydrator(self, query):
        dialect, query = query
        first = query.hydrator(dialect, [("amount", 1700)], rf.Driver.type_codes)
        assert query.hydrator(dialect, [("amount", 1700)], rf.Driver.type_codes) is first

    def test_the_same_description_is_not_read_again(self, query):
        """asyncpg hands back the description cached with the statement, so the common
        case costs an identity check rather than a pass over the columns.
        """
        dialect, query = query
        description, reads = [("amount", 1700)], []

        def type_codes(described):
            reads.append(described)
            return rf.Driver.type_codes(described)

        first = query.hydrator(dialect, description, type_codes)
        assert query.hydrator(dialect, description, type_codes) is first
        assert reads == [description]
