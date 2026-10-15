"""The declaration layer: one class, three jobs (table, expressions, storage)."""

from __future__ import annotations

import dataclasses
import datetime as dt
import decimal
import enum
import typing
import uuid
from abc import ABC

import pytest
import sqlalchemy as sa
from conftest import Author, Base, Book, Colour, Wide
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.sqlite import aiosqlite
from sqlalchemy.orm import Mapped

import rowform as rf

# Module level: string annotations resolve against module globals.
UserId = typing.NewType("UserId", uuid.UUID)
PatientId = typing.NewType("PatientId", UserId)
Slug = typing.NewType("Slug", str)


def make_base():
    """A throwaway Base, so a test that declares tables cannot collide with
    another test's names in a shared MetaData."""

    class Scratch(rf.Base):
        metadata = sa.MetaData()

    return Scratch


class TestTableConstruction:
    def test_the_model_is_the_table(self):
        assert isinstance(Author.__table__, sa.Table)
        assert Author.__table__.name == "t_authors"
        assert Author.__table__ in Base.metadata.sorted_tables

    def test_class_access_yields_the_column(self):
        assert Author.id is Author.__table__.c.id
        assert isinstance(Author.id, sa.Column)

    def test_instance_access_yields_the_value(self):
        author = Author(id=1, name="ada", active=True)
        assert author.id == 1
        assert author.name == "ada"

    def test_class_access_is_unaffected_by_instances(self):
        Author(id=1, name="ada", active=True)
        assert Author.id is Author.__table__.c.id

    def test_columns_carry_the_annotation_derived_type(self):
        types = {c.name: type(c.type) for c in Wide.__table__.columns}
        assert types["id"] is sa.Integer
        assert types["text"] is sa.String
        assert types["flag"] is sa.Boolean
        assert types["when"] is sa.DateTime
        assert types["day"] is sa.Date
        assert types["clock"] is sa.Time
        assert types["ratio"] is sa.Float
        assert types["uid"] is sa.Uuid
        assert types["payload"] is sa.JSON
        assert types["blob"] is sa.LargeBinary

    def test_optional_annotation_makes_the_column_nullable(self):
        assert Wide.__table__.c.note.nullable is True
        assert Wide.__table__.c.text.nullable is False

    def test_primary_key_is_never_nullable(self):
        assert Wide.__table__.c.id.primary_key is True
        assert Wide.__table__.c.id.nullable is False

    def test_enum_becomes_a_sa_enum_over_the_same_class(self):
        assert isinstance(Wide.__table__.c.colour.type, sa.Enum)
        assert Wide.__table__.c.colour.type.enum_class is Colour

    def test_explicit_type_overrides_the_annotation(self):
        assert Wide.__table__.c.amount.type.precision == 12
        assert Wide.__table__.c.amount.type.scale == 3

    def test_foreign_keys_pass_through(self):
        fks = list(Book.__table__.c.author_id.foreign_keys)
        assert len(fks) == 1
        assert fks[0].target_fullname == "t_authors.id"

    def test_a_leading_string_renames_the_column(self):
        Scratch = make_base()

        class Renamed(Scratch):
            __tablename__ = "renamed"
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            title: Mapped[str] = rf.mapped_column("headline", index=True)

        assert Renamed.__table__.c.keys() == ["id", "headline"]
        assert Renamed.title.name == "headline"
        # The attribute keeps the Python name; only the DB column is renamed.
        assert Renamed(id=1, title="t").title == "t"

    def test_table_args_pass_through(self):
        Scratch = make_base()

        class Constrained(Scratch):
            __tablename__ = "constrained"
            __table_args__ = (sa.UniqueConstraint("a", "b"),)
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            a: Mapped[int]
            b: Mapped[int]

        uniques = [
            c for c in Constrained.__table__.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        assert len(uniques) == 1

    def test_type_annotation_map_extends_the_defaults(self):
        class Scratch(rf.Base):
            metadata = sa.MetaData()
            type_annotation_map = {str: sa.Text()}

        class Doc(Scratch):
            __tablename__ = "doc"
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            body: Mapped[str]

        assert isinstance(Doc.__table__.c.body.type, sa.Text)

    def test_a_newtype_resolves_through_its_supertype(self):
        """As SQLAlchemy does: the NewType itself first, then its flattened supertype."""

        class Scratch(rf.Base):
            metadata = sa.MetaData()
            type_annotation_map = {Slug: sa.Text()}

        class Person(Scratch):
            __tablename__ = "person"
            id: Mapped[PatientId] = rf.mapped_column(primary_key=True)
            owner: Mapped[UserId | None]
            slug: Mapped[Slug]

        columns = Person.__table__.c
        assert isinstance(columns.id.type, sa.Uuid)
        assert isinstance(columns.owner.type, sa.Uuid)
        assert columns.owner.nullable
        assert isinstance(columns.slug.type, sa.Text)

    def test_an_unmapped_python_type_is_refused_with_a_way_out(self):
        Scratch = make_base()

        with pytest.raises(TypeError, match="no SQLAlchemy type registered"):

            class Odd(Scratch):
                __tablename__ = "odd"
                id: Mapped[int] = rf.mapped_column(primary_key=True)
                thing: Mapped[complex]


class TestExpressions:
    def test_comparison_builds_real_sql(self):
        clause = Author.id > 100
        assert isinstance(clause, sa.sql.elements.BinaryExpression)
        assert "t_authors.id > " in str(clause)

    def test_select_treats_the_class_as_its_table(self):
        compiled = str(sa.select(Author))
        assert compiled.startswith("SELECT t_authors.id, t_authors.name, t_authors.active")

    def test_join_and_select_from_accept_the_class(self):
        compiled = str(sa.select(Author, Book).join(Book))
        assert "FROM t_authors JOIN t_books ON t_authors.id = t_books.author_id" in compiled
        assert "FROM t_authors" in str(sa.select(sa.func.count()).select_from(Author))

    def test_an_abstract_model_has_no_table_to_select_from(self):
        Scratch = make_base()

        class Mixin(Scratch):
            created: Mapped[int]

        with pytest.raises(TypeError, match="abstract"):
            sa.select(Mixin)


class TestInstances:
    def test_no_orm_instance_state(self):
        author = Author(id=1, name="ada", active=True)
        assert not hasattr(author, "_sa_instance_state")

    def test_is_a_real_dataclass(self):
        assert dataclasses.is_dataclass(Author)
        assert [f.name for f in dataclasses.fields(Author)] == ["id", "name", "active"]

    def test_repr_and_eq_come_from_dataclasses(self):
        one = Author(id=1, name="ada", active=True)
        two = Author(id=1, name="ada", active=True)
        assert one == two
        assert repr(one) == "Author(id=1, name='ada', active=True)"

    def test_positional_construction(self):
        assert Author(1, "ada", True).name == "ada"

    def test_instances_have_a_dict_so_orjson_can_read_them(self):
        # README "What it costs": a slotted model forces
        # orjson onto a much slower fallback, so non-slotted is the default.
        assert Author(id=1, name="ada", active=True).__dict__ == {
            "id": 1,
            "name": "ada",
            "active": True,
        }

    def test_class_keywords_reach_dataclasses(self):
        Scratch = make_base()

        class Frozen(Scratch, frozen=True):
            __tablename__ = "frozen"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        with pytest.raises(dataclasses.FrozenInstanceError):
            Frozen(id=1).id = 2

    def test_slots_true_still_builds_a_usable_model(self):
        """`slots=True` reaches `dataclasses.dataclass`, which rebuilds the class
        through this same metaclass — so the class-level Column interception has
        to survive that rebuild and compose with `__slots__`. `ModelMeta.__new__`
        is the mechanism; this is the guarantee.
        """
        Scratch = make_base()

        class Slotted(Scratch, slots=True):
            __tablename__ = "slotted"
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            name: Mapped[str]
            active: Mapped[bool]

        assert Slotted.__slots__ == ("id", "name", "active")
        # Class access is still the Column (metaclass wins over the slot
        # descriptor); instance access is still the plain value.
        assert Slotted.id is Slotted.__table__.c.id
        instance = Slotted(id=1, name="ada", active=True)
        assert (instance.id, instance.name, instance.active) == (1, "ada", True)
        assert instance == Slotted(id=1, name="ada", active=True)
        assert repr(instance).endswith("Slotted(id=1, name='ada', active=True)")
        # Fully slotted: the base chain carries `__slots__ = ()`, so a slots=True
        # model has no per-instance __dict__ at all — the layout that actually
        # saves memory and GC-traversal cost (a slotted class under a dict-
        # carrying base keeps the managed-dict overhead and saves neither).
        assert not hasattr(instance, "__dict__")
        with pytest.raises(AttributeError):
            instance.stray = 1

    def test_the_base_chain_is_slotted_so_slots_true_actually_pays_off(self):
        """The memory/GC win of `slots=True` exists only if *no* class in the MRO
        carries a `__dict__`; otherwise the object keeps the managed-dict overhead
        and slots save nothing. `rf.Base` and the abstract user `Base` both
        declare `__slots__ = ()` to guarantee that. The other half of the contract
        is that a *default* model still re-acquires its own `__dict__`, which is
        what keeps orjson on its fast native-dict path — base-slotting must not
        break it.
        """
        Scratch = make_base()
        assert rf.Base.__slots__ == ()
        assert Scratch.__slots__ == ()

        class Default(Scratch):
            __tablename__ = "plain_default"
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            name: Mapped[str]

        assert Default(id=1, name="x").__dict__ == {"id": 1, "name": "x"}

    def test_slots_composes_with_other_class_keywords(self):
        Scratch = make_base()

        class FrozenSlotted(Scratch, frozen=True, slots=True):
            __tablename__ = "frozen_slotted"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        assert FrozenSlotted.__slots__ == ("id",)
        with pytest.raises(dataclasses.FrozenInstanceError):
            FrozenSlotted(id=1).id = 2

    def test_defaults_make_a_field_optional(self):
        Scratch = make_base()

        class Defaulted(Scratch):
            __tablename__ = "defaulted"
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            active: Mapped[bool] = rf.mapped_column(default=True)

        assert Defaulted(id=1).active is True
        assert Defaulted.__table__.c.active.default.arg is True

    def test_init_false_keeps_a_field_out_of_the_constructor(self):
        Scratch = make_base()

        class Generated(Scratch):
            __tablename__ = "generated"
            id: Mapped[int] = rf.mapped_column(primary_key=True, init=False)
            name: Mapped[str]

        instance = Generated(name="x")
        assert not hasattr(instance, "id")


class TestClassBodyReferences:
    """A field declared with `rf.mapped_column()` is a column expression inside the
    class body, so `__table_args__` can name it, as it can with the ORM's.
    """

    def test_constraints_take_the_field_itself(self):
        Scratch = make_base()

        class Account(Scratch):
            __tablename__ = "account"
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            username: Mapped[str] = rf.mapped_column()
            org: Mapped[int] = rf.mapped_column()

            __table_args__ = (
                sa.UniqueConstraint(username, name="account_username_key"),
                sa.Index("ix_account_org_username", org, username.desc()),
            )

        table = Account.__table__
        unique = next(c for c in table.constraints if isinstance(c, sa.UniqueConstraint))
        assert list(unique.columns) == [table.c.username]
        index = next(iter(table.indexes))
        assert index.expressions[0] is table.c.org

    def test_expressions_render_with_the_resolved_types(self):
        Scratch = make_base()

        class Delivery(Scratch):
            __tablename__ = "delivery"
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            # Typed explicitly, as with the ORM: the body binds `Colour.RED` before
            # the annotation is resolved, and an untyped literal cannot render.
            colour: Mapped[Colour] = rf.mapped_column(sa.Enum(Colour))
            sent_at: Mapped[dt.datetime | None] = rf.mapped_column()
            deleted_at: Mapped[dt.datetime | None] = rf.mapped_column()

            __table_args__ = (
                sa.CheckConstraint(
                    sa.or_(colour != Colour.RED, sent_at.is_not(None)), name="delivery_chk"
                ),
                sa.Index(
                    "uq_delivery_live", id, unique=True, postgresql_where=deleted_at.is_(None)
                ),
            )

        ddl = str(sa.schema.CreateTable(Delivery.__table__).compile(dialect=postgresql.dialect()))
        assert "CHECK (colour != 'RED' OR sent_at IS NOT NULL)" in ddl
        index = str(
            sa.schema.CreateIndex(next(iter(Delivery.__table__.indexes))).compile(
                dialect=postgresql.dialect()
            )
        )
        assert "WHERE deleted_at IS NULL" in index
        assert Delivery.__table__.c.sent_at.nullable
        assert not Delivery.__table__.c.colour.nullable

    def test_a_referenced_column_binds_with_its_resolved_type(self):
        """The type is resolved after the class body ran; a comparison built later
        must not keep the untyped comparator the body used.
        """
        Scratch = make_base()

        class Ticket(Scratch):
            __tablename__ = "ticket"
            id: Mapped[uuid.UUID] = rf.mapped_column(primary_key=True)
            __table_args__ = (sa.Index("ix_ticket_id", id.desc()),)

        assert isinstance((Ticket.id == uuid.uuid4()).right.type, sa.Uuid)

    def test_a_mixin_foreign_key_serves_every_model_below_it(self):
        Scratch = make_base()

        class Org(Scratch):
            __tablename__ = "org"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        class Owned(Scratch):
            org_id: Mapped[int] = rf.mapped_column(sa.ForeignKey("org.id"))

        class Project(Owned):
            __tablename__ = "project"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        class Team(Owned):
            __tablename__ = "team"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        for model in (Project, Team):
            (fk,) = model.__table__.c.org_id.foreign_keys
            assert fk.column is Org.__table__.c.id


class TestMixinsAndOrder:
    def test_mixin_fields_are_inherited_as_real_columns(self):
        Scratch = make_base()

        class Timestamped(Scratch):
            created: Mapped[dt.datetime]

        class Note(Timestamped):
            __tablename__ = "note"
            id: Mapped[int] = rf.mapped_column(primary_key=True)
            body: Mapped[str]

        assert Note.__table__.c.keys() == ["created", "id", "body"]
        assert [f.name for f in dataclasses.fields(Note)] == ["created", "id", "body"]

    def test_a_mixin_builds_no_table_of_its_own(self):
        Scratch = make_base()

        class Timestamped(Scratch):
            created: Mapped[dt.datetime]

        assert "created" in Timestamped.__columns__
        assert not hasattr(Timestamped, "__table__")
        assert Scratch.metadata.tables == {}

    def test_each_model_gets_its_own_column_objects(self):
        """A Column belongs to exactly one Table, so an inherited declaration
        cannot reuse its base's."""
        Scratch = make_base()

        class Timestamped(Scratch):
            created: Mapped[dt.datetime]

        class A(Timestamped):
            __tablename__ = "a"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        class B(Timestamped):
            __tablename__ = "b"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        assert A.created is not B.created
        assert A.created.table is A.__table__

    def test_inherited_fields_come_first_and_are_recorded(self):
        """Adding a mixin moves its columns to the front of CREATE TABLE,
        and Alembic autogenerate does not diff column order — so the order is at
        least deterministic and readable off the class."""
        Scratch = make_base()

        class Timestamped(Scratch):
            created: Mapped[dt.datetime]

        class Note(Timestamped):
            __tablename__ = "note"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        assert Note.__column_order__ == ("created", "id")

    def test_column_order_can_be_pinned(self):
        Scratch = make_base()

        class Timestamped(Scratch):
            created: Mapped[dt.datetime]

        class Note(Timestamped):
            __tablename__ = "note"
            __column_order__ = ("id", "created")
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        assert Note.__table__.c.keys() == ["id", "created"]
        # The pin governs the *column* order — CREATE TABLE, and what a plain
        # `select(Note)` returns. The constructor's parameter order stays
        # stdlib dataclass semantics, where inherited fields always come first.
        assert [f.name for f in dataclasses.fields(Note)] == ["created", "id"]

    def test_an_incomplete_pin_is_refused(self):
        Scratch = make_base()

        with pytest.raises(TypeError, match=r"every Mapped\[\] field exactly once"):

            class Note(Scratch):
                __tablename__ = "note"
                __column_order__ = ("id",)
                id: Mapped[int] = rf.mapped_column(primary_key=True)
                body: Mapped[str]

    def test_a_reordering_default_explains_itself(self):
        """The stdlib message names two fields in an order the author never
        wrote, because the reordering is inherited rather than declared."""
        Scratch = make_base()

        class Timestamped(Scratch):
            created: Mapped[int] = rf.mapped_column(default=0)

        with pytest.raises(TypeError, match="inherited from a base or mixin"):

            class Note(Timestamped):
                __tablename__ = "note"
                id: Mapped[int] = rf.mapped_column(primary_key=True)

    def test_kw_only_is_the_documented_way_out(self):
        Scratch = make_base()

        class Timestamped(Scratch):
            created: Mapped[int] = rf.mapped_column(default=0)

        class Note(Timestamped, kw_only=True):
            __tablename__ = "note"
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        assert Note(id=1).created == 0


class TestGuards:
    def test_a_reserved_name_is_refused(self):
        Scratch = make_base()

        with pytest.raises(TypeError, match="collides with a reserved name"):

            class Bad(Scratch):
                __tablename__ = "bad"
                metadata: Mapped[str]

    def test_a_multi_type_union_is_refused(self):
        Scratch = make_base()

        with pytest.raises(TypeError, match="union of more than one non-None type"):

            class Bad(Scratch):
                __tablename__ = "bad"
                id: Mapped[int] = rf.mapped_column(primary_key=True)
                thing: Mapped[int | str]

    def test_metaclass_conflict_is_real_and_documented(self):
        """Every model carries ModelMeta, so combining with ABC or Protocol
        raises. Accepted rather than worked around — a decorator would compose
        freely, but the decorator route erases field types."""
        Scratch = make_base()

        with pytest.raises(TypeError, match="metaclass conflict"):

            class Bad(Scratch, ABC):
                __tablename__ = "bad"
                id: Mapped[int] = rf.mapped_column(primary_key=True)


class TestModelLookup:
    def test_a_table_knows_the_model_it_was_built_from(self):
        assert rf.model_for(Author.__table__) is Author

    def test_an_alias_resolves_through_to_the_model(self):
        assert rf.model_for(sa.alias(Author.__table__, "a2")) is Author

    def test_a_foreign_table_has_no_model(self):
        assert rf.model_for(sa.table("elsewhere", sa.column("id"))) is None


class TestAlias:
    """`sa.orm.aliased()` needs a `Mapper` and there is none, so `alias()` is
    rowform's own — a `FromClause` that answers to field names."""

    def test_sqlalchemy_orm_aliased_is_not_available(self):
        with pytest.raises(sa.exc.NoInspectionAvailable):
            sa.orm.aliased(Author)

    def test_it_coerces_to_an_alias_of_the_table(self):
        other = rf.alias(Author, "a2")
        element = other.__clause_element__()
        assert isinstance(element, sa.Alias)
        assert element.name == "a2"
        assert element.element is Author.__table__

    def test_attributes_are_the_alias_columns(self):
        other = rf.alias(Author, "a2")
        assert other.name is not Author.name
        assert other.name.key == "name"
        assert str(other.name) == "a2.name"

    def test_it_selects_and_joins_like_the_model(self):
        other = rf.alias(Author, "a2")
        rendered = str(sa.select(other).where(other.id > 1))
        assert "FROM t_authors AS a2" in rendered
        assert "a2.id >" in rendered

    def test_a_renamed_column_is_reached_by_its_field_name(self):
        class Renamed(make_base()):
            __tablename__ = "renamed"

            id: Mapped[int] = rf.mapped_column(primary_key=True)
            slug: Mapped[str] = rf.mapped_column("url_slug")

        other = rf.alias(Renamed, "r2")
        assert str(other.slug) == "r2.url_slug"

    def test_an_unknown_field_raises(self):
        other = rf.alias(Author, "a2")
        with pytest.raises(AttributeError, match="no column 'missing'"):
            _ = other.missing

    def test_an_abstract_model_cannot_be_aliased(self):
        class Abstract(make_base()):
            id: Mapped[int] = rf.mapped_column(primary_key=True)

        with pytest.raises(TypeError, match="abstract"):
            rf.alias(Abstract)

    def test_repr_names_the_model_and_the_alias(self):
        assert repr(rf.alias(Author, "a2")) == "<alias Author AS a2>"


class TestAliasOf:
    """`of=` says "these rows are Authors" about a subquery or CTE, which is the
    only way one can hydrate: a CTE has no `.info` and its `.element` is a
    `Select`, so `model_for` has nothing to walk to."""

    def test_a_subquery_is_scalars_until_it_is_declared(self):
        sub = sa.select(Author).subquery()
        assert rf.model_for(sub) is None

        rf.alias(Author, of=sub)
        assert rf.model_for(sub) is Author

    def test_the_mark_lands_on_the_given_from_clause(self):
        """Not on a wrapper of it: `select(top, newest.c.id)` has to stay one
        from clause, or it is a cartesian product."""
        newest = sa.select(Author).limit(5).subquery()
        top = rf.alias(Author, of=newest)
        assert top.id is newest.c.id

    def test_a_cte_hydrates_as_the_model(self):
        cte = sa.select(Author).cte("recent")
        recent = rf.alias(Author, of=cte)
        rendered = str(sa.select(recent).where(recent.active))
        assert "WITH recent AS" in rendered
        assert "FROM recent" in rendered

    def test_a_union_hydrates_as_the_model(self):
        both = sa.union_all(sa.select(Author), sa.select(Author)).subquery()
        assert rf.model_for(both) is None
        rf.alias(Author, of=both)
        assert rf.model_for(both) is Author

    def test_an_extra_column_is_refused(self):
        """`select()` expands every column of a from clause, so an extra one
        would hydrate as `(Author, int)` while still typed `Select[tuple[Author]]`."""
        ranked = sa.select(
            Author, sa.func.row_number().over(order_by=Author.id).label("rk")
        ).subquery()
        with pytest.raises(TypeError, match="exactly that model's columns"):
            rf.alias(Author, of=ranked)

    def test_reordered_columns_are_refused(self):
        reordered = sa.select(Author.name, Author.id, Author.active).subquery()
        with pytest.raises(TypeError, match="exactly that model's columns"):
            rf.alias(Author, of=reordered)

    def test_narrowing_past_the_extra_column_is_what_the_error_asks_for(self):
        inner = sa.select(
            Author, sa.func.row_number().over(order_by=Author.id).label("rk")
        ).subquery()
        narrowed = (
            sa.select(*[inner.c[c.key] for c in Author.__table__.c])
            .where(inner.c.rk == 1)
            .subquery()
        )

        first = rf.alias(Author, of=narrowed)
        assert rf.model_for(narrowed) is Author
        assert "rk" in str(sa.select(first))

    def test_a_select_is_refused(self):
        with pytest.raises(TypeError, match="A Select becomes one with"):
            rf.alias(Author, of=sa.select(Author))

    def test_a_name_belongs_to_the_subquery_not_the_alias(self):
        with pytest.raises(TypeError, match="not to alias"):
            rf.alias(Author, "a2", of=sa.select(Author).subquery())


class TestDdl:
    def test_create_table_renders_from_the_declaration(self):
        ddl = str(sa.schema.CreateTable(Wide.__table__))
        assert "CREATE TABLE t_wide" in ddl
        assert "PRIMARY KEY (id)" in ddl
        assert "note VARCHAR" in ddl

    def test_sqlite_renders_the_types_it_has_no_native_form_for(self):
        ddl = str(
            sa.schema.CreateTable(Wide.__table__).compile(
                dialect=aiosqlite.dialect()
            )
        )
        assert "CHAR(32)" in ddl  # sqlite has no native uuid
        assert "JSON" in ddl


def test_default_type_map_covers_the_types_the_wide_shape_uses():
    assert isinstance(rf.DEFAULT_TYPE_MAP[uuid.UUID], sa.Uuid)
    assert isinstance(rf.DEFAULT_TYPE_MAP[decimal.Decimal], sa.Numeric)
    assert isinstance(rf.DEFAULT_TYPE_MAP[dt.datetime], sa.DateTime)


def test_enum_members_survive_declaration():
    assert list(Colour) == [Colour.RED, Colour.BLUE]
    assert issubclass(Colour, enum.Enum)
