"""Spike: derive sync tests from the async ones, into tests/sync/ (git-ignored)."""
import re, sys
from pathlib import Path

T = Path(__file__).resolve().parent.parent / "tests"
OUT = T / "sync"
OUT.mkdir(exist_ok=True)
(OUT / "__init__.py").write_text("")

GENERIC = [
    (r"\brf\.active_connection\b", "rfs.active_connection"),
    (r"\.aclose\(\)", ".close()"),
    (r'"aiosqlite":', '"pysqlite":'),
    (r"\baiosqlite\.dialect\(\)", "pysqlite.dialect()"),
    (r'\["asyncpg", "psycopg"\]', '["psycopg"]'),
    (r"\baclosing\b", "closing"),
    (r"^from conftest import", "from sync.conftest import"),
    (r"^def pytest_addoption\(parser\):\n(?:    .*\n|\n)+?(?=\n\n)", ""),
    (r"from sqlalchemy\.dialects\.sqlite import aiosqlite", "from sqlalchemy.dialects.sqlite import pysqlite"),

    (r"\basync def\b", "def"), (r"\basync with\b", "with"), (r"\basync for\b", "for"),
    (r"\bawait\s+", ""), (r"\basynccontextmanager\b", "contextmanager"),
    (r"\bAsyncIterator\b", "Iterator"),
    (r"\brf\.Engine\b", "rfs.Engine"), (r"\brf\.Connection\b", "rfs.Connection"),
    (r"^import rowform as rf\n", "import rowform as rf\nimport rowform.sync as rfs\n"),
    (r"sqlite\+aiosqlite", "sqlite+pysqlite"),
    (r"create_async_engine", "create_engine"),
    (r"from sqlalchemy\.ext\.asyncio import create_engine", "from sqlalchemy import create_engine"),
    (r'params=\["sqlite", "asyncpg", "psycopg"\]', 'params=["sqlite", "psycopg"]'),
    (r'params=\["sqlite", "asyncpg"\]', 'params=["sqlite"]'),
    (r'def pg_url\(dsn: str, driver: str = "asyncpg"\)', 'def pg_url(dsn: str, driver: str = "psycopg")'),
    (r'if driver == "asyncpg" else url', 'if driver == "asyncpg" else url'),
]
# Tests that are inherently async, or touch async-only surface.
SKIP = ("asyncio", "AsyncSession", "AsyncConnection", "AsyncEngine", "pytest_asyncio", "gather")

KEEP = {"conftest.py", "test_fetch_one.py", "test_transactions.py", "test_streaming.py", "test_copy_in.py",
        "test_driver_errors.py", "test_result.py", "test_observability.py", "test_types.py",
        "test_sqla_equivalence.py", "test_pipeline.py", "test_engines.py",
        "test_errors.py", "test_oneshot_checkout.py", "test_statement_cache.py"}
for stale in OUT.glob("*.py"):
    if stale.name not in KEEP | {"__init__.py"}:
        stale.unlink()
for src in sorted(T.glob("test_*.py")) + [T / "conftest.py"]:
    if src.name not in KEEP:
        continue
    text = src.read_text()
    skipped = [w for w in SKIP if w in text] if src.name != "conftest.py" else []
    if skipped and "--all" not in sys.argv:
        print("skip", src.name, skipped)
        continue
    for p, r in GENERIC:
        text = re.sub(p, r, text, flags=re.M)
    (OUT / src.name).write_text(text)
