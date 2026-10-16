# Guide

Recipes. [API.md](API.md) is the reference. `rf` is `import rowform as rf`.

## Install and declare

```bash
uv add "rowform @ git+https://github.com/vipierozan99/rowform" asyncpg   # or psycopg[binary], aiosqlite
```

```python
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Mapped
import rowform as rf

class Base(rf.Base):
    metadata = sa.MetaData()

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    name: Mapped[str]
    email: Mapped[str | None]                       # nullable
    role: Mapped[Role]                              # an Enum class -> sa.Enum
    org_id: Mapped[int] = rf.mapped_column(sa.ForeignKey("orgs.id"))
    slug: Mapped[str] = rf.mapped_column("url_slug", unique=True)

    __table_args__ = (sa.Index("ix_users_org_name", "org_id", "name"),)

sa_engine = create_async_engine("postgresql+asyncpg://localhost/app")
db = rf.Engine(sa_engine)          # yours to dispose: await sa_engine.dispose()
```

Instances are plain dataclasses; `frozen=True`, `kw_only=True`, `slots=True`
pass through. Caveat: a slotted dataclass falls off orjson's fast path and
serializes about 2x slower — leave `slots` off for JSON endpoints.

## One-shot or scope

A one-shot runs one statement on a pooled connection with **no transaction**;
a scope holds one connection, and `begin()` wraps it in a transaction. One-shot
when one statement is the whole job; scope when two statements must agree.

```python
users = await db.fetch_all(sa.select(User).limit(100))      # one-shot, no BEGIN

async with db.begin(isolation_level="REPEATABLE READ") as conn:  # postgres: one snapshot
    users = await conn.fetch_all(sa.select(User))
    posts = await conn.fetch_all(sa.select(Post))             # same snapshot
```

Caveat: inside a scope call `conn.*`; `db.*` raises there, and so does a task
spawned with `asyncio.create_task` inside the block.

## Reading

The statement decides the row shape: one selected entity is that entity, two or
more are a tuple in select order, an outer join with no match is `None`.

```python
await db.fetch_all(sa.select(User))                     # list[User]
await db.fetch_all(sa.select(User.name, User.id))       # list[tuple[str, int]]
await db.fetch_all(sa.select(User, Post).join(Post))    # list[tuple[User, Post]]
await db.fetch_one(sa.select(User).where(User.id == 1)) # User | None
await db.fetch_one(sa.select(sa.func.count()).select_from(User))  # int | None
```

The other way to read is SQLAlchemy's own `Result`, for code being ported:

```python
async with db.connect() as conn:
    users = (await conn.execute(sa.select(User))).scalars().all()   # list[User]
    rows = (await conn.execute(sa.select(User.name, User.id))).all() # list[Row]
```

## Inside an existing AsyncSession

Reads run on the session's connection, see its uncommitted writes, and roll back
with it — one query at a time is the migration unit.

```python
async with Session() as session, session.begin():
    session.add(AuditRow(...))
    await session.flush()                             # rowform will not
    async with db.connect(bind=session) as conn:
        hot = await conn.fetch_all(sa.select(User))
```

Caveat: an unflushed `add()` is not in the database, and rowform never triggers
autoflush.

A session that routes tables to several engines (`binds=`) works the same way:
wrap each engine once, and each `rf.Engine` joins the session on its own
engine's connection.

## Tracing

`around=` wraps each driver round trip, which is where a span belongs:

```python
from opentelemetry import trace

tracer = trace.get_tracer("rowform")

db = rf.Engine(sa_engine, around=lambda sql: tracer.start_as_current_span(
    "db.query", attributes={"db.statement": sql}
))
```

A streamed read opens one span per chunk, so the consumer's own spans never nest
inside a database span.

## FastAPI

One engine per process; return bytes, not a pydantic `response_model`, which
would re-validate every row.

```python
from contextlib import asynccontextmanager
from typing import Annotated
import orjson
from fastapi import Depends, FastAPI, Request, Response

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.sa_engine = create_async_engine(DSN, pool_size=8, max_overflow=0)
    app.state.db = rf.Engine(app.state.sa_engine)
    try:
        yield
    finally:
        await app.state.sa_engine.dispose()

app = FastAPI(lifespan=lifespan)

def get_db(request: Request) -> rf.Engine:
    return request.app.state.db

Db = Annotated[rf.Engine, Depends(get_db)]
LIST_USERS = sa.select(User).where(User.id > sa.bindparam("after")).order_by(User.id).limit(50)

@app.get("/users")
async def list_users(db: Db, after: int = 0) -> Response:
    rows = await db.fetch_all(LIST_USERS, after=after)
    return Response(orjson.dumps(rows), media_type="application/json")
```

Caveat: prefer `max_overflow=0` and the `pool_size` you want — SQLAlchemy closes
overflow connections on return, so `1+7` reconnects where `8+0` reuses.

## Pagination

Keyset, not `OFFSET`: constant cost per page and no skipped rows under writes.

```python
PAGE = sa.select(User).where(User.id > sa.bindparam("after")).order_by(User.id).limit(sa.bindparam("size"))

first = await db.fetch_all(PAGE, after=0, size=50)
rest = await db.fetch_all(PAGE, after=first[-1].id, size=50) if first else []
```

Caveat: order by something unique, or carry `(column, id)` in the cursor.

## Streaming an export

`fetch_iter` hydrates a chunk at a time through a cursor and holds the
connection for the whole loop.

```python
from contextlib import aclosing

async with aclosing(db.fetch_iter(sa.select(User), chunk=500)) as stream:
    async for user in stream:
        await sink.write(user)
```

Caveat: psycopg cannot stream `INSERT ... RETURNING` (server cursors are
SELECT-only); asyncpg and sqlite can.

## Aliases, self-joins, CTEs

`rf.alias()` keeps the per-field types that `sa.alias()` and `.c` lose;
`sqlalchemy.orm.aliased()` does not work here at all.

```python
mgr = rf.alias(User, "mgr")
pairs = await db.fetch_all(
    sa.select(User, mgr).join(mgr, User.manager_id == mgr.id)
)   # list[tuple[User, User]]

active = rf.alias(User, of=sa.select(User).where(User.active).cte("active"))
users = await db.fetch_all(sa.select(active).order_by(active.id))   # list[User]
```

Caveat: `of=` demands exactly the model's columns, in order. To filter on a
window function, compute it in an inner subquery and select the model's columns
back out:

```python
inner = sa.select(User, sa.func.row_number().over(order_by=User.id).label("rk")).subquery()
first = rf.alias(User, of=(
    sa.select(*[inner.c[c.key] for c in User.__table__.c]).where(inner.c.rk == 1).subquery()
))
```

## Views

A model can sit on a view: declare the view with `rf.view()` and assign it to
`__table__`. Every column of the select needs a field; types come from the select.

```python
ACTIVE = sa.select(User.id, User.name).where(User.active)

class ActiveUser(Base):
    __table__ = rf.view("active_users", Base.metadata, ACTIVE)
    id: Mapped[int]
    name: Mapped[str]

await db.create_all(Base.metadata)      # tables, then views
rows = await db.fetch_all(sa.select(ActiveUser))    # list[ActiveUser]
```

In a migration, emit the same DDL: `op.execute(rf.CreateView(ActiveUser.__table__))`
and `op.execute(rf.DropView(ActiveUser.__table__))`. Caveat: `drop_all` drops views
without `CASCADE`, so a database object rowform does not know about that depends on
one makes the drop fail rather than vanish.

## Writing

The model stands in for its table; `execute()` returns SQLAlchemy's `Result`.

```python
await db.execute(sa.insert(User).values(name="ada"))
await db.execute(sa.update(User).where(User.id == 1).values(hits=User.hits + 1))
await db.execute_many(sa.insert(User), [{"name": "a"}, {"name": "b"}])
created = await db.fetch_all(sa.insert(User).values(name="ada").returning(User))

await db.copy_in(User.__table__, rows)              # postgres COPY; rows: list[dict]

async with db.begin() as conn:
    await conn.execute(sa.update(Account).where(...).values(...))
    async with conn.begin_nested():                 # a savepoint
        await conn.execute(...)
```

A unique violation raises `sa.exc.IntegrityError`, as it does through SQLAlchemy;
the driver's own exception is `.orig` (asyncpg's, one `__cause__` below).

## Alembic

`Base.metadata` is an ordinary `MetaData`, so autogenerate works unchanged.

```python
# alembic/env.py
from myapp.models import Base
target_metadata = Base.metadata
```

Views declared with `rf.view()` are not in `metadata.tables`, so autogenerate
leaves them alone; create and drop them with `rf.CreateView`/`rf.DropView`.

Caveat: column order is inherited-first and Alembic does not diff order, so
adding a mixin to an existing table silently reorders `CREATE TABLE`. Pin it:

```python
class User(Timestamped):
    __tablename__ = "users"
    __column_order__ = ("id", "name", "created")
    ...
```

## Testing with sqlite

The declaration is the DDL, so fixtures never hand-write `CREATE TABLE`.

```python
@pytest.fixture
async def db(tmp_path):
    sa_engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 't.sqlite3'}")
    db = rf.Engine(sa_engine)
    try:
        await db.drop_all(Base.metadata)
        await db.create_all(Base.metadata)
        yield db
    finally:
        await sa_engine.dispose()

async def test_no_n_plus_one(db):
    seen = []
    db.observer = lambda sql, *_: seen.append(sql)
    await load_dashboard(db)
    assert len(seen) == 2
```

Caveat: sqlite's raw driver returns strings for temporal types and ints for
booleans; rowform applies SQLAlchemy's result processors. Still run type-sensitive
tests against the database you deploy on.

## The metaclass conflict

`class User(Base, abc.ABC)` raises `TypeError: metaclass conflict`. Type against
a `Protocol` instead, `register` the model with an existing ABC, and share columns
through a mixin under the same `Base`.

```python
class HasName(Protocol):
    name: str

class Nameable(abc.ABC): ...
Nameable.register(User)             # isinstance(user, Nameable) is True

class Timestamped(Base):            # no __tablename__: a mixin
    created: Mapped[dt.datetime]

class Review(Timestamped, kw_only=True):
    __tablename__ = "reviews"
    id: Mapped[int] = rf.mapped_column(primary_key=True)
```
