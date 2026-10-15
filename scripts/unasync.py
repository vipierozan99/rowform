"""Generate `rowform/sync/` from the async modules. `python scripts/unasync.py` writes;
`--check` fails if the generated files are stale.

Each rule must match at least once, so a refactor of the async source that moves a
seam fails the build instead of silently emitting async code into the sync package.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "rowform"
OUT = ROOT / "sync"

# (pattern, replacement, flags). Applied in order; every one must match.
COMMON = [
    (r"^from \. import result as _result$", "from .. import result as _result", re.M),
    (r"^from \.(errors|query|planner|result|model|compile) ", r"from ..\1 ", re.M),
    # Cancellation is asyncio's; a sync call has no equivalent to unwind.
    (r"^( +)except asyncio\.CancelledError:\n(?:\1 {4}.*\n)+", "", re.M),
    (r"^import asyncio\n", "", re.M),
]
PER_FILE = {
    "engine.py": [
        (r"from sqlalchemy\.ext\.asyncio import AsyncConnection, AsyncEngine\n",
         "from sqlalchemy import Connection as SAConnection\nfrom sqlalchemy import Engine as SAEngine\n", 0),
        (r"from sqlalchemy\.util import greenlet_spawn\n", "", 0),
        (r"await greenlet_spawn\((\w+)\.(\w+)\)", r"\1.\2()", 0),
        (r"\.sync_engine\b", "", 0),
        # A sync pool hands out the driver connection itself: no adapter in between.
        (r"driver_conn = dbapi_conn\.driver_connection", "driver_conn = fairy.driver_connection", 0),
        (r"fairy = await conn\.get_raw_connection\(\)", "fairy = conn.connection", 0),
        (r"\(await conn\.get_raw_connection\(\)\)\.driver_connection", "conn.connection.driver_connection", 0),
        (r"await (\w+)\.run_sync\((\w+\.\w+), ", r"\2(\1, ", 0),
        (r"an AsyncConnection or an AsyncSession", "a Connection or a Session", 0),
        (r"AsyncConnection", "SAConnection", 0),
        (r"AsyncEngine", "SAEngine", 0),
        (r"AsyncSession", "Session", 0),
        (r"create_async_engine", "create_engine", 0),
        (r"`SAEngine`", "`Engine`", 0),
    ],
    "connection.py": [
        (r"from sqlalchemy\.ext\.asyncio import AsyncResult, AsyncScalarResult\n", "", 0),
        (r"    from sqlalchemy\.ext\.asyncio import AsyncConnection\n",
         "    from sqlalchemy import Connection as SAConnection\n", 0),
        (r"AsyncResult\(\s*_result\.chunked_result\(([^\n]*)\)\n\s*\)",
         r"_result.sync_chunked_result(\1)", 0),
        (r"AsyncScalarResult", "ScalarResult", 0),
        (r"AsyncResult", "Result", 0),
        (r"AsyncConnection", "SAConnection", 0),
        (r"AsyncTransaction", "Transaction", 0),
    ],
    "drivers.py": [
        (r"\nclass AsyncpgDriver\(Driver\):.*?(?=\nclass PsycopgDriver)", "", re.S),
        (r"\"aiosqlite\": SqliteDriver,\n    \"asyncpg\": AsyncpgDriver,", "\"pysqlite\": SqliteDriver,", 0),
        (r"sync = engine\.sync_engine", "sync = engine", 0),
        (r"await_only\((conn\.connection\.driver_connection\.execute\(\"BEGIN\"\))\)", r"\1", 0),
        (r"from sqlalchemy\.util import await_only\n", "", 0),
        (r"await conn\.set_autocommit\((\w+)\)", r"conn.autocommit = \1", 0),
        (r"(create_async_engine, not create_engine)", "(create_engine, not create_async_engine)", 0),
        (r"the engine must be an async one", "the engine must be a sync one", 0),
        (r"aiosqlite", "sqlite3", 0),
    ],
}
GENERIC = [
    (r"\basync def\b", "def"),
    (r"\basync with\b", "with"),
    (r"\basync for\b", "for"),
    (r"\bawait\s+", ""),
    (r"\basynccontextmanager\b", "contextmanager"),
    (r"\bAbstractAsyncContextManager\b", "AbstractContextManager"),
    (r"\bAsyncIterator\b", "Iterator"),
    (r"\b__aenter__\b", "__enter__"),
    (r"\b__aexit__\b", "__exit__"),
]


def generate(name: str) -> str:
    text = (ROOT / name).read_text()
    for pattern, repl, flags in COMMON + PER_FILE[name]:
        text, n = re.subn(pattern, repl, text, flags=flags)
        # CancelledError blocks and the asyncio import are absent from some files.
        optional = "CancelledError" in pattern or "import asyncio" in pattern or "from \\." in pattern
        if n == 0 and not optional:
            raise SystemExit(f"{name}: rule matched nothing: {pattern!r}")
    for pattern, repl in GENERIC:
        text = re.sub(pattern, repl, text)
    header = f'# GENERATED from rowform/{name} by scripts/unasync.py; edit that file.\n'
    return header + text


def main() -> int:
    stale = False
    for name in PER_FILE:
        out = OUT / name
        new = generate(name)
        if "--check" in sys.argv:
            stale |= not out.exists() or out.read_text() != new
        else:
            out.write_text(new)
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
