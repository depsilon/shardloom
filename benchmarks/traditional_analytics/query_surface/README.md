# Broad native query corpus

This is the measurement foundation for [queue item 20](../../../docs/architecture/native-broad-query-program-2026-10-26.md).
It declares each workload once and executes it through real CLI SQL, embedded
workflow, Python SQL, DataFrame and typed native relation APIs. The DataFrame and
typed adapters construct public expressions and relations; they do not disguise
SQL submission as another interface. All candidate computation remains native
ShardLoom execution over Vortex. Independent reference calculations and the
optional Polars comparison are benchmark tools only.

The current full catalog contains **366 cases: 347 executable cases and 19
pending requests**, across 79 archetypes and 21 named families. Sixty archetypes
are executable. The default pairwise catalog contains 147 cases, of which 128
are executable; selecting its development partition executes 109. The aggregate
factor space has 240 feasible cells and its deterministic pairwise subset has 21.
Counts are not a completeness claim. A pending adapter can represent an already
implemented engine capability; a proposed request is neither an executed rejection
nor a correctness pass.

## Responsibilities

| Module | Contract |
| --- | --- |
| `datasets.py` | Independent named seeds, declared correlation, observed null/cardinality/selectivity/fanout, logical identity, five physical layouts and bounded Arrow IPC fixture transport |
| `specification.py`, `corpus.py`, `coverage.py` | Typed query intent, stable semantic identity, reviewed archetypes, factor coverage and neighboring compositions |
| `reference.py`, `semantics.py` | Independent bounded expectations and complete typed comparison, including duplicate multiplicity and explicit floating policies |
| `adapters.py` | Thin declarations and complete result consumption through five native public interfaces |
| `run.py`, `worker.py`, `evidence.py` | Existing storage/process/RSS/deadline guards, native preparation and complete readback, immutable requests/receipts/source snapshots, before/after identities and classified failures |
| `diagnostics.py`, `report.py` | Available provider/work evidence and family-balanced outcomes; unavailable physical counters stay unavailable |
| `polars_adapter.py`, `polars_run.py` | Separately authorized, one-observation external comparison against the original Arrow IPC source |
| `replay.py` | Offline content-addressed packet verification and complete-result recomparison without running an engine |

The 38-column fact fixture includes signed/unsigned integers, exact decimals,
floats, validity patterns, text, dates, naive microsecond timestamps and selected
nested values. At the main scale it has 1,000,000 rows, with 100,000 entities,
10,000 products, 200,000 relationships and 731 calendar rows. `sorted`, `shuffled`,
`clustered`, `partitioned` and `fragmented` layouts preserve the same logical
records and multiplicities. Exact observed cardinalities use bounded sets;
larger estimates are explicitly labelled approximate. Unit tests verify every
layout on small fixtures; the initial 1M observation uses the sorted layout only.

## Run a new observation

Use Python 3.10 or newer with PyArrow 25.0.1 and a feature-enabled CLI/native
library built from the intended revision. Resolve Cargo outputs with
`cargo metadata --offline --no-deps --format-version 1`; do not assume a checkout
`target` path. Put datasets, preparation and logs in local storage outside synced
directories. Each run ID must be new. The runner refuses an existing identity.

From the repository root, substitute the absolute paths for your environment:

```sh
python -B benchmarks/traditional_analytics/query_surface/run.py \
  --root /local/shardloom/query-observations --run-id tiny_semantics_r1 \
  --binary /local/runtime/shardloom --library /local/runtime/_native.abi3.so \
  --rows 127 --interactions all --partition all \
  --cases predicate_eq,numeric_mod,join_semi,join_anti
```

With no `--surfaces` option all five native interfaces run. The public interface
identifiers are `cli_sql,embedded_workflow,python_sql,dataframe,typed_relation`.
The first run creates Arrow IPC source fixtures, prepares ordinary Vortex inputs
and compares **all** persisted values and logical schemas with the source.
Preparation and readback precede query observations.

An authorized full initial 1M observation uses one query execution per case:

```sh
python -B benchmarks/traditional_analytics/query_surface/run.py \
  --root /local/shardloom/query-observations --run-id million_full_r1 \
  --binary /local/runtime/shardloom --library /local/runtime/_native.abi3.so \
  --rows 1000000 --interactions all --partition all --surfaces python_sql
```

To reuse validated inputs, provide both `--dataset /local/.../dataset` and
`--prepared /local/.../prepared/prepared.json`. The manifest, exact source
mapping, preparation receipts, complete readback, file identities and hashes must
still agree. Changing a dataset option does not regenerate a supplied dataset;
its stored specification is authoritative. Full hashing occurs before/after
queries, with lightweight generation checks between calls. Keep other task-owned
work idle while timed queries run. Existing guards and ceilings are mandatory.

Reports retain every selected outcome. A timed-out, incorrect, unsupported,
adapter-gap, crashed or incomplete request is not removed from the denominator.
Independent-reference failure cannot yield an engine pass. Passing implies
complete values and declared schema/order, not complete physical instrumentation.
Unknown bytes, memory and provider fields are not replaced with zero.

The `held_out` partition is a deterministic selection aid. The October 26 request
released every existing executable case for discovery. Those 347 cases are no
longer an unseen acceptance cohort: future generalization claims require new
seeds, physical layouts and previously unused combinations.

## Optional Polars comparison

Install `requirements-polars.txt` in a separate environment only for an authorized
comparison. It is not a ShardLoom package dependency. The comparison reuses an
intact completed native run without rerunning native queries:

```sh
POLARS_MAX_THREADS=2 python -B benchmarks/traditional_analytics/query_surface/polars_run.py \
  --native-run /local/shardloom/query-observations/million_full_r1 \
  --run-id polars_million_full_r1
```

Both comparison clocks span lazy declaration through complete Python-row delivery,
excluding explicit pre-clock imports and source preparation. Initialization or
imports triggered inside those APIs are included; Polars delivery includes its
Arrow conversion boundary. These are first-request observations, not warmed
steady-state API costs. Native **engine** time is retained
separately; Polars exposes no equivalent engine clock here, so the two are never
divided. ShardLoom reads prepared Vortex while Polars reads the logically identical
original Arrow IPC. Both use two threads in the initial observation; only
ShardLoom has the internal 2-GiB resource grant. Both have the external RSS guard.
Polars nullability metadata is unavailable; observable logical types, names/order
and every value still undergo strict comparison. SQL null sums, count widths and
signed remainder are declared before execution, not repaired in result rows.

One native cohort followed by one Polars cohort does not establish a noise floor,
balanced execution order, cold caches, statistical benefit or superiority. See
the [dated report](../../../docs/benchmarks/native-broad-query-2026-10-26.md) for
the exact observations, limitations and next investigations.

## Verification and offline replay

```sh
PYTHONPATH=python/src python -B -m unittest discover -s python/tests -p 'test_query_surface_*.py' -v
python -B benchmarks/traditional_analytics/query_surface/replay.py \
  docs/benchmarks/evidence/native-broad-query-2026-10-26
```

Pure semantic/coverage/evidence tests require no native runtime. PyArrow dataset
tests, native execution and Polars adapter tests use their explicit dependencies;
skips are visible. Set `SHARDLOOM_REQUIRE_NATIVE_PYTHON=1`,
`SHARDLOOM_NATIVE_TEST_LIBRARY` and `SHARDLOOM_SURFACE_TEST_BINARY` to require
native five-interface acceptance. The independent native surfaces are tested
with subprocess creation forbidden. New tests are discovered automatically by
the existing Python CI shard. Installed-wheel proof also requires the complete
five-interface native regression suite.

Replay reads regular content-addressed objects in memory and checks retained
requests, status counts, source/reference/result hashes, complete typed values,
no-fallback evidence and matching clocks. It neither extracts archived code nor
executes a query. Original missing job hashes and pre-summary failures stay visible.
Two early cohorts also lack an original Python source snapshot inventory; replay
reports that limitation explicitly. Hashes prove consistency rather than signed
execution provenance. The file inventory is itself a content-addressed archive
object so the checked-in manifest remains small.
Large source/native payloads and executables are represented by their identities
and regeneration/build instructions, not copied into the compact packet.
