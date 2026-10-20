<!-- SPDX-License-Identifier: Apache-2.0 -->

# Pre-writer overlap attribution — October 20, 2026

Status: **inconclusive; eligible measurement is still missing**. Two observer
implementations complete the corrected frozen comparison and pass exact output,
activation, source identity and cleanup checks. Both fail the mandatory observer
overhead gate. No scheduling candidate is designed, implemented or retained.
Temporary instrumentation is removed from the runtime and test sources.

This is a prerequisite record for conditional row 10, `PERF-09.overlap`, rather
than a no-signal result or a rejected scheduling mechanism. The
[conditional sequence](native-conditional-investigations-2026-10-19.md) has nine
completed finite decisions, this inconclusive prerequisite, and eleven rows
awaiting their first result. Broader owners and CG-1 through CG-23 stay open.
Continue with codec profiles and revisit this measurement prerequisite before
resuming the 37 required substeps.

## Boundary and immutable gates

The [design](native-ingest-overlap-attribution-design-2026-10-19.md) wraps the
existing Vortex 0.85.0 current-thread executor in a test-only observer. Source,
conversion and writer tasks retain their shared runtime and admitted driver
count. The production writer already combines one active subtree with one
following input using its existing shared slot. This experiment does not repeat
the previously retained shared-slot mechanism or implement two concurrent
writer subtrees.

The corrected protocol uses prefixes of row groups 0, 113 and 225 from the
resident official Parquet source. Each contains at most three 32,768-row batches
and all 105 source columns plus seven existing derived columns, with current
dictionary preparation and the
existing large-source writer profile. Three four-driver targets, three serial
controls and one single-batch control retain the same 2-GiB native grant. These
prefixes do not represent default full-file batch geometry or a full ingestion.

Fourteen preflight workflows establish geometry and real conversion-prefetch
activation before each comparison. Two frozen cohorts each contain three
ordinary/observed pairs for all seven cells: 84 measured workflows in total.
Role order alternates and the second cohort reverses it. Both cohorts finish
before comparison results are read. Every output is independently reopened and
checked against complete source values, schema, row count and native layout/
statistics requirements. No external engine executes the workload.

Every cell in both cohorts must keep median observed complete-ingest cost within
10% of its ordinary control. Only an eligible observer can supply opportunity
evidence. The ready-successor, CPU-queue-empty unoccupied-driver integral must
then reach 5% of admitted driver capacity in the same two targets across both
cohorts before a scheduling design is admitted. Occupied-driver wall intervals
are not thread CPU measurements; async runnable state remains unknown. Even an
eligible integral would not establish achievable speedup.

## Attempts and result

| Attempt | Completion | Disposition |
| --- | --- | --- |
| `794-a1` | Four preflight workflows; no measured cohort | Footer-backed DType debug output exceeds diagnostic admission. Compact schema reporting replaces that diagnostic before a fresh attempt. |
| `794-a2` | 14 preflight and 84 measured workflows | Invalid setup: the original 131,072-row geometry admits zero conversion-prefetch slots. Its zero opportunity cannot answer the ready-successor question. |
| `797-a1` | 14 preflight and 55 of 84 measured workflows | Diagnostic storage admission refuses the next workflow before its native process starts. The incomplete cohort receives no comparison credit. |
| `800-a1` | 14 preflight and all 84 measured workflows | Corrected activation and exact checks pass; cohort B's `rg0-p4` observer cost exceeds the 10% bound. Inconclusive. |
| `801-a1` / source revision `802` | 14 preflight and all 84 measured workflows | Bounded append-only event recording replaces synchronized aggregation. The same cell again exceeds the bound. Inconclusive. |

The [final decision](../benchmarks/evidence/ingest-overlap-attribution-decision-803.json)
records cohort-B `rg0-p4`'s observed median exceeding its ordinary
median by **13.0683%**. All other thirteen cells satisfy the overhead bound.
The raw opportunity calculation reaches the nominal threshold in `rg113-p4`
and `rg225-p4` in both cohorts, but the failed eligibility gate prevents using
those values to admit a scheduling candidate. No machine-load cause, general
observer penalty or production regression is inferred from this result.

The revised observer passes 78 native lifecycle, source, shared-slot, ownership,
pressure and statistics checks plus 35 independent verifier/retention tests,
formatting and feature-specific static analysis before measurement. Independent
verification replays the event streams, checks activation and all complete
records, and recomputes the frozen decision. Correctness does not override the
observer eligibility failure.

## Evidence and restoration

The [compact packet index](../benchmarks/evidence/ingest-attribution-results-803.json)
binds 2,151 exact result, protocol, helper, failure and changed-source members in
the [4.4-MB result packet](../benchmarks/evidence/ingest-attribution-results-803.json.xz).
The [packet verification](../benchmarks/evidence/ingest-attribution-packet-verification-803.json)
reconstructs all 1,907 Rust/Cargo inputs for each of four source versions from
accepted Git commit `ce9428fc1239ce9b6a65d55d857e05d46c878f97`, its patch and
added files. It also recomputes all 84 final measurement records and checks all
14 preflight records. The packet preserves every attempt's result and binding;
it contains no executable, dataset, generated Vortex payload or unchanged full
source bundle.

The [restoration receipt](../benchmarks/evidence/ingest-overlap-restoration-804.json)
checks all 1,903 remaining Rust/Cargo inputs. All runtime and dependency sources
equal the accepted version. The sole Rust difference is an independent test fix:
credit-window tests conditionally use optional observation instrumentation, so
they also compile without that feature. CI runs those tests with observation
enabled as well. The [restored-source check index](../benchmarks/evidence/ingest-restored-checks-805.json)
and [exact check packet](../benchmarks/evidence/ingest-restored-checks-805.json.xz)
record formatting, strict default-workspace Clippy, 3,235 default-workspace test
passes, and all nine credit-window tests both with and without observation.
Eight unfiltered workspace targets contain no tests; neither focused selector
has zero matches. All five supervised checks exit successfully and drain their
owned processes. Their 1,903 source identities are rechecked after completion.
These are restoration checks, separate from the measurement builds. A new full
ingest or query benchmark is unnecessary for exact restoration of unchanged
runtime/dependency sources and is not claimed here.

Event traces are indexed by exact identity and were independently replayed
before retirement. The [retirement receipt](../benchmarks/evidence/ingest-attribution-retirement-804.json)
records removal of all 184 detailed event dumps and the obsolete observer
executable after identity and closed-handle checks. No backup archive was made.
The compact packet supports result and source
reconstruction; it does not contain the raw event streams. Reproducing a new
event trace requires rebuilding the recorded source and executing a new guarded
attempt in fresh paths, with the original source and pinned dependencies.
The [portable reopening check](../benchmarks/evidence/ingest-attribution-portable-reopening-804.json)
repeats source reconstruction and decision recomputation without the original
working tree, raw event streams or Vortex outputs. From the repository root:

```sh
python3 docs/benchmarks/evidence/verify_ingest_attribution_result_packet_804.py \
  --packet docs/benchmarks/evidence/ingest-attribution-results-803.json.xz \
  --repository . --scratch "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT" \
  --output "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/ingest-result-reopening.json"
```

Use an existing local-only scratch directory and a new output filename. The
recorded Git base must be available. This checks retained results and sources;
it does not rerun the ingestion or independently reopen retired physical data.

Further work needs a materially different measurement mechanism that satisfies
the same activation, lifecycle and overhead requirements. Repeating unchanged
cohorts, ignoring one cell or relaxing the threshold would not supply that
prerequisite. No full ingestion, Full43 speedup, package, release or broader
capability claim follows from this investigation.
