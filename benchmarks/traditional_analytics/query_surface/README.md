# Broad native query corpus

This is the measurement foundation for [queue item 20](../../../docs/architecture/native-broad-query-program-2026-10-26.md).
It declares each workload once and executes it through real CLI SQL, embedded
workflow, Python SQL, DataFrame and typed native relation APIs. The DataFrame and
typed adapters construct public expressions and relations; they do not disguise
SQL submission as another interface. All candidate computation remains native
ShardLoom execution over Vortex. Independent reference calculations and the
optional Polars comparison are benchmark tools only.

The original full catalog contains **366 cases: 347 executable cases and 19
pending requests**, across 79 archetypes and 21 named families. Sixty archetypes
are executable. The default pairwise catalog contains 147 cases, of which 128
are executable; selecting its development partition executes 109. The aggregate
factor space has 240 feasible cells and its deterministic pairwise subset has 21.
Counts are not a completeness claim. A pending adapter can represent an already
implemented engine capability; a proposed request is neither an executed rejection
nor a correctness pass.

## Expanded analytical cohort

The separately versioned `analytical-10000-v2` campaign preserves the original
347 executable identities and their execution order. It adds 9,653 distinct
requests. Each request has a unique rendered SQL statement with symbolic source
names. The 19 historical pending declarations remain visible outside that
denominator. The 104 archetypes include population and multiway joins, nullable
and distinct reductions, selective predicates, ordered payloads, windows, as-of
matches, three-valued subqueries, list expansion, shared materialization and
bounded recursion.

The November 3 maintainer decision admits one Polars observation for each of
the 10,000 cases before the native pass. Earlier 347-case results keep their
original comparison contract. The new inputs comprise 8,924 cases on the
original million-row population, 132 small semantic cases, 880 cases on a
separately seeded shuffled holdout with 100,003 fact rows, and 64 cases at
five million rows. Twelve named scale cases cover families from the maintainer's
5M summary. Their SQL and input data were independently designed for this
campaign.

`expanded_catalog.py` selects feasible factor cells, covers every declared
pair plus the specified three-, four- or five-factor intersections, then fills
the remaining family budgets deterministically. Each case has one primary
native interface and input route. The recursive and explicitly materialized
requests use the SQL interfaces. Polars expresses the finite recursive frontier
as a bounded sequence of lazy relations. The twelve scale cases also receive
one alternate native execution on Vortex explicitly prepared from their Parquet
source. Those twelve observations have their own ledger.

### Reproduce the expanded run

Use an empty nonsynced directory and the pinned Polars/PyArrow environment.
Set `EXPANSION` to that directory, `ORIGINAL` to the retained million-row
Arrow dataset directory, `PREPARED` to its validated `prepared.json`,
and `LEGACY` to the frozen 347-case `inputs.json` with its independent
reference files and Parquet sources available at their recorded paths.
`BINARY` and `LIBRARY` select the matching ShardLoom CLI and native extension.
Allocation includes source files, three physical formats, reference data,
complete observation ledgers and the existing free-space reserve.

```sh
SUITE=benchmarks/traditional_analytics/query_surface
python -B "$SUITE/expanded_inputs.py" --root "$EXPANSION" --original "$ORIGINAL"
python -B "$SUITE/expanded_freeze.py" design --root "$EXPANSION" --legacy "$LEGACY"
python -B "$SUITE/expanded_freeze.py" inputs --root "$EXPANSION" \
  --original "$ORIGINAL" --prepared "$PREPARED" --binary "$BINARY" --library "$LIBRARY"
for profile in smoke holdout scale primary; do
  python -B "$SUITE/expanded_freeze.py" expectations --root "$EXPANSION" --profile "$profile" || exit 1
done
python -B "$SUITE/expanded_freeze.py" seal --root "$EXPANSION"
python -B "$SUITE/expanded_run.py" polars --root "$EXPANSION" &&
python -B "$SUITE/expanded_run.py" prepare --root "$EXPANSION" &&
python -B "$SUITE/expanded_run.py" native --root "$EXPANSION" &&
python -B "$SUITE/expanded_run.py" alternate --root "$EXPANSION" &&
python -B "$SUITE/expanded_report.py" --root "$EXPANSION" --output "$EXPANSION/report"
```

The freeze checks the retained expectations, complete source readbacks, unique
case and source identities, comparator environment and selected runtime. Pass
`--runtime-proof` to `seal` when a release staging receipt is available. New
expected values come from an independent SQLite evaluator over the original
Arrow values. Its SQL compiler and explicit type, NULL and ordering rules live
in the benchmark harness. The native and Polars requests each construct their
own plans and compare their complete results with those expectations.

The native grant is 2 GiB and two threads. Both engines use a fresh supervised
process per request. Each scored stage checks input and code identities before
and after execution, journals each attempt before dispatch and retains the
request, complete result or failure, clocks and process evidence in a compressed
SQLite ledger. `--resume-unattempted` continues a recovered stage once its
previous supervisor and process group have exited and its lock is released.
An interrupted attempt stays visible. A completed stage refuses a second run.

Replay produces per-case receipts, family metrics, a separate comparison for
the twelve 5M cases and the preparation measurements. The native engine clock
comes from the runtime report. Cross-engine observations use declaration through
complete Python-row delivery and report common-source and prepared-Vortex cases
separately. Every selected case contributes to its family denominator.

The [November 3 observations](../../../docs/benchmarks/native-expanded-query-2026-11-03.md)
include the case table, all independent expectations, complete observed
worker files and preparation receipts. To check the published results from a
checkout, run the offline replay below with Python 3.10 or later. Each
archived record binds the original SQLite blob and its exact decompressed
JSON. Replay validates that JSON on the current platform without reopening the
original data.

```sh
PACKET=docs/benchmarks/evidence/native-expanded-query-2026-11-03
python -B benchmarks/traditional_analytics/query_surface/expanded_replay.py \
  --packet "$PACKET/packet" --reports "$PACKET"
python -B benchmarks/traditional_analytics/query_surface/expanded_membership_replay.py \
  --baseline "$PACKET/packet" --candidate "$PACKET/membership"
python -B -S benchmarks/traditional_analytics/query_surface/expanded_operator_replay.py \
  --repo . --archive "$PACKET/operator-investigations"
python -B -S benchmarks/traditional_analytics/query_surface/expanded_batch_replay.py \
  --archive "$PACKET/batch-investigations"
python -B -S benchmarks/traditional_analytics/query_surface/expanded_updated_replay.py \
  --baseline "$PACKET/packet" --archive "$PACKET/updated-native"
```

The operator replay checks the 55 matched case-route pairs, the separate
prepared-group challenge, fresh declaration evidence and the unscored
reservation capture against the original requests. It uses Python's standard
library and the saved worker packets.

The batch replay verifies 33 pairs of public declarations on one runtime and
four separate sampled requests. It reads archived worker packets, checks
complete results against the independent expectations and reconstructs the
main-thread call-path counts from the saved samples.

The updated replay checks the later maintainer-authorized complete native
pass. It binds all 10,000 original declarations and independent expectations
to the accepted investigation runtime, verifies every complete result, and
recomputes the full updated engine total and the 9,995-case common-correct
comparison. Its source, resource and isolation proofs are portable. It also
retains the five recovered requests and all individual timing increases.
[Updated complete-cohort results](../../../docs/benchmarks/native-expanded-updated-cohort-2026-11-03.md).

To package a new completed local run, first write its offline report to
`$EXPANSION/report`, then run `expanded_packet.py --root "$EXPANSION" --output NEW_DIRECTORY`.
The index binds every compressed part and the original ledgers. The reader
reconstructs each case, checks its full result and recomputes the published metrics.

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

The maintainer's October 26 decision is to reuse existing competitor
observations while their implementation and comparison contract are unchanged.
Do not rerun Polars or another unchanged competitor for a ShardLoom iteration.
Retain the original versions, inputs, resources and timing boundary. A changed
input format or workload must be labelled as a different comparison contract;
it does not authorize another competitor run. A new competitor observation
requires a separate explicit request.

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
