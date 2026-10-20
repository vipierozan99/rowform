# Contributing

```bash
git clone https://github.com/vipierozan99/rowform && cd rowform
uv sync --all-groups
just test            # sqlite always; PostgreSQL when 127.0.0.1:5432 answers
just lint --fix
just typecheck
```

## Both backends, always

Engine and transaction tests run against sqlite, asyncpg and psycopg from one
parametrised fixture (`tests/conftest.py`). The three disagree exactly where this
library is exposed — sqlite returns strings and ints where postgres returns
`datetime` and `bool`; psycopg opens transactions implicitly where asyncpg does
not — so a behaviour asserted on one backend has not been tested. PostgreSQL
tests skip when no server is reachable; run `just test . --pg-required` before
opening a PR, as CI does. `ROWFORM_TEST_DSN` overrides the default DSN.

`tests/test_pgbouncer.py` and `tests/test_migrations.py` also run through three
pgbouncers in front of that server (1.20 and 1.22 in transaction pooling, 1.22 in
session pooling); `just pgbouncer up` starts them with Docker, as CI runs them.

Types are tested, not declared: `tests/typing/positive.py` uses `assert_type`,
`tests/typing/negative.py` carries a `# pyright: ignore` on every line that must
fail. Change both when you change a signature.

## SQLAlchemy's private surface

rowform reads SQLAlchemy internals on purpose; the list lives in `pyproject.toml`
next to the `<2.1` pin. Each coupling must be pinned by a test that fails when it
moves, and a weekly canary runs the suite against SQLAlchemy `main`. Adding a
coupling means adding it to that list and that test.

## Docs are written once, at PR time

Code and tests land per commit. Prose — `README.md`, `docs/*.md`, module
docstrings describing the public surface — is updated in one pass when the PR
opens. Keep a list of what your change made stale as you go.

## Benchmarks

`benchmarks/README.md` has the recipe; `docs/BENCHMARKS.md` has the rules a
number must meet before it is quoted. The PR performance gate compares the micro
benchmarks against the merge base; if it fails and the regression is justified,
say so with the numbers.

## Scope

A read/write path over SQLAlchemy Core, not an ORM. Relationships, lazy loading,
an identity map and a unit of work are deliberately absent — open an issue
before a PR that adds one.
