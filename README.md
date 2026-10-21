# rowform

**SQLAlchemy's SQL, schema and transactions. Typed dataclass rows. No ORM session.**

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
    email: Mapped[str | None]

db = rf.Engine(create_async_engine("postgresql+asyncpg://localhost/app"))

users = await db.fetch_all(sa.select(User).where(User.name.like("a%")))   # list[User]
```

`User` is one declaration doing three jobs: `User.__table__` is a real `sa.Table`
(for `create_all()`, `Inspector`, Alembic); `sa.select(User)` and `User.id > 100` are
real Core expressions; `user.id` is an `int` on a plain dataclass. SQLAlchemy Core
compiles every statement, and SQLAlchemy's engine owns the pool and the
transactions. rowform takes the driver's rows and fills dataclasses with a
generated function — no `Row`, no `Session`, no instance state.

> **Status: early.** Tested against sqlite, asyncpg and psycopg; benchmarked;
> not on PyPI; never run in production. Requires `sqlalchemy>=2.0.18,<2.1`.

## Why

**Nothing is implicit.** The ORM makes `user.posts` a `SELECT` when the relationship
is not loaded, expires every object on `commit()`, and flushes pending writes before
a read. Under asyncio the lazy load does not work at all. rowform has no
instrumented attributes, so there is nothing to switch off: every round trip is a
statement you wrote, and a test can assert how many ran.

```python
db.observer = lambda sql, seconds, rows: seen.append(sql)
await load_dashboard(db)
assert len(seen) == 2
```

**Adopt one query at a time.** Because the engine is SQLAlchemy's, a rowform read
can run inside a transaction you already have — an `AsyncConnection` or an
`AsyncSession` — seeing its uncommitted writes and rolling back with it:

```python
async with Session() as session, session.begin():
    session.add(AuditRow(...))
    await session.flush()                            # rowform will not flush for you
    async with db.connect(bind=session) as conn:
        users = await conn.fetch_all(sa.select(User))
```

**Values are SQLAlchemy's.** Each selected column decodes through its own dialect
`result_processor`, so a `DateTime` on sqlite or a `Numeric` on postgres comes back
exactly as it would through `Row`. The suite checks this against Core as an oracle
over generated statements.

## Reading

The statement decides the row shape. One selected entity yields that entity; two or
more yield a tuple. Every read is overloaded on arity, so these infer without casts:

```python
await db.fetch_all(sa.select(User))                    # list[User]
await db.fetch_all(sa.select(User.name))               # list[str]
await db.fetch_all(sa.select(User, Post).join(Post))   # list[tuple[User, Post]]
await db.fetch_one(sa.select(User).where(User.id == 1))                 # User | None
await db.fetch_one(sa.select(sa.func.count()).select_from(User))        # int | None

async for user in db.fetch_iter(sa.select(User), chunk=500):           # a server cursor
    ...
```

An `outerjoin` with no match yields `None` for that slot. `rf.alias(User, "mgr")`
is the self-join alias; `rf.alias(User, of=subquery)` says a subquery's rows are
`User`s.

**Two ways to read, told apart by name.** `fetch_*` returns hydrated objects.
`execute()` returns SQLAlchemy's own `Result` — rowform hands its rows to the
upstream `IteratorResult`, so `.scalars()`, `.mappings()`, `.partitions()`,
`row.name` and `NoResultFound` are the real implementations. That is what lets
existing code move over a query at a time.

## Scopes

```python
users = await db.fetch_all(stmt)          # one-shot: pooled connection, no transaction

async with db.begin() as conn:            # begin-once: commit on exit, rollback on error
    await conn.execute(sa.update(Account).where(...))
    rows = await conn.fetch_all(sa.select(Account))
    async with conn.begin_nested():       # a savepoint
        ...

async with db.connect() as conn:          # commit-as-you-go, like AsyncConnection
    await conn.execute(sa.insert(User).values(name="ada"))
    await conn.commit()
```

A one-shot `fetch_all`/`fetch_one` on a `SELECT` takes a pooled connection
directly and runs outside any transaction: one snapshot, no `BEGIN`/`COMMIT` (on
asyncpg, except the run that first prepares the statement on that connection), no
isolation level. A read that needs either belongs in a scope. Calling `db.fetch_*`
*inside* a scope raises rather than silently reading from another connection.

The `BEGIN`, `COMMIT` and `SAVEPOINT` are SQLAlchemy's. On aiosqlite, wrapping an
engine registers SQLAlchemy's documented pysqlite recipe on it (`isolation_level=None`
plus an explicit `BEGIN`), without which savepoints land outside their transaction;
this applies to every user of that engine.

## Writing and schema

```python
await db.execute(sa.insert(User).values(name="ada"))
await db.execute_many(sa.insert(User), [{...}, {...}])
rows = await db.fetch_all(sa.insert(User).values(name="ada").returning(User))
await db.copy_in(User.__table__, rows)          # postgres COPY

await db.create_all(Base.metadata)              # bootstrap; Alembic: target_metadata = Base.metadata
```

Driver errors are SQLAlchemy's: a unique violation raises `sa.exc.IntegrityError`, with
the driver's exception as `.orig`.

## Performance

Medians, ms per read of 1000 rows (`@1` is one row), one contender per process, GC
off, pinned cores, CPU boost off, byte-identical output enforced before timing.
Every contender runs the same Core-compiled SQL inside `BEGIN`…`COMMIT`, except
`one-shot`. `rowform` does the same per-row work as its rivals; `idiomatic` is what
an application writes (prepared once, dataclasses straight to orjson).

postgres 16 (asyncpg, loopback):

| contender | flat | join | wide | | vs Core |
|---|---|---|---|---|---|
| raw driver → dicts *(floor)* | 0.990 | 1.946 | — | | 0.77x / 0.89x / — |
| **rowform** | 1.374 | 2.708 | 4.367 | | 1.08x / 1.24x / 1.02x |
| rowform (idiomatic) | 1.135 | 2.132 | 3.774 | | 0.89x / 0.97x / 0.88x |
| SQLAlchemy Core | 1.278 | 2.189 | 4.273 | | 1.00x |
| SQLAlchemy ORM | 6.780 | 11.412 | 12.144 | | 5.31x / 5.21x / 2.84x |

**rowform costs about what Core costs and returns typed dataclasses where Core
returns tuples; the ORM costs 2–6x.** It is not faster than Core's result layer:
SQLAlchemy 2.0 builds `Row` in C, and a plain dataclass constructor matches the
generated hydrator. The idiomatic margin is skipping SQLAlchemy's execution path and
a dict pass before serialization. All of this is CPU on loopback; over a real network
the round trips dominate and every row above converges. The sqlite table, the
recipe, the gates and the lessons are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## What it costs

- **SQLAlchemy is a hard dependency**, and rowform reads its private compiler and
  result internals (listed in `pyproject.toml`), hence the `<2.1` pin.
- **Every model carries a metaclass**, so `class User(Base, ABC)` and `Protocol`
  mixing raise `TypeError`. A decorator would compose but erases field types.
- **No relationships, no unit of work.** You write every join; insert ordering and
  batching are yours. Instances are not tracked; mutating one does nothing.
- **Column order is inherited-first**; pin it with `__column_order__` on a table that
  already exists, since Alembic does not diff column order.
- **`slots=True` models serialize ~2x slower** through orjson, which falls off its
  fast dataclass path for them. The default is non-slotted.

## Documentation

- [docs/GUIDE.md](docs/GUIDE.md) — recipes: FastAPI, pagination, streaming, `bind=`, Alembic and live migrations, pgbouncer, testing
- [docs/API.md](docs/API.md) — every public name and what it returns
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — the numbers, how they are taken, what went wrong before
- [CONTRIBUTING.md](CONTRIBUTING.md) · [SECURITY.md](SECURITY.md)

```bash
git clone https://github.com/vipierozan99/rowform && cd rowform && uv sync --all-groups
just test && just lint && just typecheck
```
