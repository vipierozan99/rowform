# Benchmarks

What the numbers are, how they were taken, and the mistakes that shaped the harness.
`benchmarks/README.md` is the operator's guide; this is the record.

## The claim

**rowform's read costs about what SQLAlchemy Core's costs and returns typed
dataclasses; the ORM costs 2–6x.** Not: rowform is faster than Core. At equal work
Core's C-built `Row` is a few percent ahead or tied; the `idiomatic` row's margin is
skipping SQLAlchemy's execution path and a per-row dict pass before serialization.

Three shapes, because one shape is an extreme without saying so: `flat`
(`int/str/str/bool`, where bypassing `Row` looks free), `join` (two entities per row),
`wide` (`DateTime/Date/Numeric/Enum/Uuid/nullable`, where per-column processors
dominate and both sides run the same ones).

## Results

Medians of per-trial medians, ms per read, lower is better. 1000 rows per read except
`@1`. 1500 timed iterations after 200 warmup (20000/2000 for `@1`), 3 trials (5 for
`@1`), one contender per process, GC off, pinned to two physical cores, CPU boost off.
Every contender runs identical Core-compiled SQL inside `BEGIN`…`COMMIT`, and builds
its payload with the same per-shape builder, except `one-shot`, which is `Engine.fetch_all`
off the engine and opens no transaction. `raw driver` is the driver alone, no SQLAlchemy,
sending the same transaction. Absolute times are not comparable to runs taken with
boost on; ratios are.

### postgres 16 (asyncpg, loopback) — `bench/2026-08-27-postgres-attached`, `e28c2e0`

| contender | flat @1000 | join @1000 | wide @1000 | flat @1 | | vs Core flat | vs Core join | vs Core wide | vs Core flat @1 |
|---|---|---|---|---|---|---|---|---|---|
| raw driver → dicts (floor) | 0.990 | 1.946 | — | 0.338 | | 0.77x | 0.89x | — | 0.91x |
| **rowform** | 1.374 | 2.708 | 4.367 | 0.338 | | 1.08x | 1.24x | 1.02x | 0.91x |
| rowform (idiomatic) | 1.135 | 2.132 | 3.774 | 0.332 | | 0.89x | 0.97x | 0.88x | 0.90x |
| rowform (one-shot) | 1.324 | — | — | 0.194 | | 1.04x | — | — | 0.52x |
| SQLAlchemy Core | 1.278 | 2.189 | 4.273 | 0.370 | | 1.00x | 1.00x | 1.00x | 1.00x |
| SQLAlchemy ORM | 6.780 | 11.412 | 12.144 | 0.589 | | 5.31x | 5.21x | 2.84x | 1.59x |

On postgres the transaction is two real round trips: the `@1` column puts it at 43% of
a single-row read (`one-shot` against `rowform`), where on sqlite it is Python.
Recorded against a different postgres 16 than the `033812a` table it replaces, so
absolutes do not compare across the two; ratios do.

### sqlite (aiosqlite, 200k-row file) — `bench/2026-08-26-sqlite-begin`, `17e867e`

| contender | flat @1000 | join @1000 | wide @1000 | flat @1 | | vs Core flat | vs Core join | vs Core wide | vs Core flat @1 |
|---|---|---|---|---|---|---|---|---|---|
| raw driver → dicts (floor) | 1.961 | 3.493 | 6.558 | 0.187 | | 0.79x | 0.85x | 0.98x | 0.49x |
| **rowform** | 2.555 | 4.524 | 6.923 | 0.347 | | 1.03x | 1.10x | 1.04x | 0.91x |
| rowform (idiomatic) | 2.344 | 3.941 | 6.358 | 0.352 | | 0.95x | 0.96x | 0.95x | 0.92x |
| rowform (one-shot) | 2.399 | — | — | 0.226 | | 0.97x | — | — | 0.59x |
| SQLAlchemy Core | 2.478 | 4.098 | 6.672 | 0.383 | | 1.00x | 1.00x | 1.00x | 1.00x |
| SQLAlchemy ORM | 7.931 | 13.556 | 14.593 | 0.541 | | 3.20x | 3.31x | 2.19x | 1.41x |

On sqlite, rowform's read is in a real transaction and stock Core's is not: pysqlite
sends no `BEGIN` before a `SELECT`, and rowform applies SQLAlchemy's pysqlite recipe
so that savepoints work. That round trip is a real difference in what the two provide,
and it is most of the `@1` gap.

Trial-to-trial spread: under 4.2% on every `@1000` cell quoted (4.0% on postgres); the `@1` cell is
looser (up to 20% on the one-shot row) and its medians reproduced across two runs to
within 4%. The sqlite `join`/`wide` cells do not reproduce reliably on the recording
box — later trials come back 15–30% slower than the first with no diagnosed cause —
so they are quoted from the sweep that reproduces and flagged here.

### Provenance and what is stale

Rows were recorded under their previous names (`SQLAlchemy Core (positional)`,
`rowform (no transaction)`); the contenders are the same code. Three things have
changed since: the one-shot read now takes a direct pool checkout (`ce8ac4f`), which
neither table's `one-shot` row has been re-recorded for; the contender set was
cut from 66 to the rows above; and asyncpg statements are prepared through
SQLAlchemy's adapter cache, with each hydrator checked against the result's type
codes, which a postgres row has not been re-recorded for either (an interleaved
before/after run on an unpinned box is in the pull request that made the change). A fresh sweep supersedes this section when it lands; until then these
are the numbers.

Raw `run.json` artifacts live on the `bench/<date>-<topic>` branches named above.

## Reading the numbers

- A 1000-row read is ~92% per-row work, so a fixed per-request cost (checkout,
  `BEGIN`/`COMMIT`) is invisible there and legible only in `@1`.
- Everything here is CPU on loopback. Over a network of 0.5 ms RTT the two round
  trips a transaction costs (~1 ms) exceed the differences between rowform, Core and
  the raw-driver floor, though not the ORM gap. The
  one-shot read exists for that reason.
- The row layer itself is ~0.07 ms per 1000 rows of 4 columns, for dicts, for the
  generated hydrator, for a dataclass constructor and for SQLAlchemy's C `Row` alike.
  The only path materially below it is one that builds no Python object per row.
- In bytes, a 1000-row `flat` read peaks at ~0.52 MB for rowform — within 1% of
  raw-driver dicts on sqlite, 10% on postgres — against +13–18% for Core and 3.3–3.6x
  for the ORM (`just bench micro memory`).
- GC on/off made no difference to the row layer in isolation; it is off for the
  published runs because it collapses trial spread on `join`.

## Recipe

```bash
sudo scripts/bench_cpu_boost.sh off    # the gate refuses to call a run quotable otherwise
just bench env check                   # boost, gevent patch, dirty tree, load average
for shape in flat join wide; do
  just bench micro run --shape $shape --iterations 1500 --warmup 200 --trials 3 --isolate --record
done
just bench micro run --shape flat --limit 1 --iterations 20000 --warmup 2000 --trials 5 --isolate --record
just bench db up && just bench micro run --backend postgres --isolate --trials 3 --record --pg-dsn "$(just bench db dsn)"
sudo scripts/bench_cpu_boost.sh on     # put boost back
```

Gates, all enforced by the harness rather than by review: byte-identical payload across
every contender in a cell before timing, re-run three times for self-consistency and
hash-verified in the child process; one contender per process; boost off at start *and*
end; no gevent monkey-patch in the timing process; no thermal throttle events; clean
tree. A run that fails any of them is recorded `quotable=False`.

Not a gate, and the audit to repeat when a floor is added: what each contender *sends*.
`log_statement=all` on postgres, a hop count on aiosqlite's worker thread
(`tests/test_transactions.py::TestSqliteBeginCost`, `tests/test_oneshot_checkout.py`).
Byte equality cannot see a missing `BEGIN`.

## Lessons

Sixteen published claims were wrong before this table. Each became a rule or a gate;
the narrative is in git history (`docs/METHODOLOGY.md` before `ce8ac4f`).

1. Different bytes is different work → the equivalence gate.
2. Contenders in one process contaminate each other → one process per contender.
3. A single asyncio loop uses one core; pinning two measures migration → `--pin`.
4. Numbers from two harnesses never share a table.
5. One run is an anecdote → trials, spread, and ties flagged instead of ranked.
6. What is inside the timed region must be the same for everyone (connection setup,
   session construction) — and a per-request `Session` on a hoisted connection, or the
   identity map skips the work being measured.
7. Never divide a bottom-up sum into a top-down measurement.
8. Price every workaround the comparison needs; find the other side's best idiom
   (`.mappings()` cost Core 2.6x against positional rows).
9. The obvious idiom for the rival (`for row in result`) was slower than its tuned one
   (`.all()`) — assume there is another.
10. A floor must do strictly less work, per backend: `User(**kw)` and `zip(fields, row)`
    floors came out above what they bounded.
11. One type shape generalises to nothing; `wide` exists because a hand-written converter
    table was wrong on 7 of 8 columns while passing every test.
12. Different transaction semantics under similar names must be equalised before timing
    (every contender in `BEGIN`…`COMMIT`; the exception is a named row).
13. The harness's own imports are inside the experiment: locust's gevent patch moved
    every number 30% → `assert_unpatched_threading`.
14. The headline rows did less work than their rivals (prepared once, serialized in C).
    Split into equal-work and idiomatic rows; parity enforced by shared builders.
15. A floor that marked a transaction in Python and sent nothing to the wire put the
    pool cost at ~0 → read the wire, not the code.
16. The same bug one backend over: sqlite floors sent no `BEGIN` while rowform did; and
    rowform's `BEGIN` itself took three worker-thread hops where one would do.
17. (After the cut.) Reset-on-return was priced at 0.11 ms per checkout; on current
    SQLAlchemy it is skipped for autocommit connections and costs nothing.

## Open

- A concurrent-throughput number (`bench load`, GC on, 50-row reads) has never been
  published; it is the number a service actually buys.
- All runs are loopback. An injected RTT (`tc qdisc add dev lo root netem delay 250us`)
  would make round trips the dominant term, as in production.
- The sqlite `join`/`wide` dispersion is undiagnosed.
