# Native completion and ownership continuation

Status: focused checks pass; combined feature validation and public acceptance
are in progress. This packet does not promote an unmeasured query speedup or
close the entire performance plan. It accompanies the September 12
[plan reconciliation](../architecture/performance-plan-exhaustion-2026-09-12.md).

## Production changes

The explicitly admitted non-null UTF8 weighted COUNT spill route now uses the
existing aggregate job queue and exact string partitions. It reserves spill
scratch and output state before worker admission. When state fits, complete
partitions supply the bounded final selection without writing runs. Under
pressure, it drains workers, transfers committed state once, and consumes each
untouched suffix once through the existing native run store. Compound keys keep
their existing serial spill route. This is not general join or distinct spill.

Projected native dictionaries use the same structural field resolver as the
existing encoded consumers. The worker grows its existing task reservation when
the referenced dictionary domain is larger than the projected row estimate.
Dictionary execution evidence records actual consumers; selection of a contract
alone is not execution evidence.

Prepared integer aggregates additionally admit identity MIN, MAX and AVG through
the existing lowering and consumers. Every call executes with fresh state.
Integer extrema retain exact comparison, and AVG retains the ordinary engine's
ordered floating accumulation. This change adds source/lowering reuse, not
answer caching or different aggregate mathematics.

The already admitted non-null integer grouped COUNT DISTINCT family can finalize
bounded results directly into owned native columns. It preserves original
integer key widths and complete count-descending/key-ascending ranking, applying
offset and limit once. Vortex, Arrow IPC and Parquet output use the existing
writers and publication contracts. Owned results retain their session and memory
credits and can be written after the input file is removed; writing them does
not reopen the source or execute the query again. Unsupported nullable, measure,
ordering and unbounded result shapes fail explicitly before query execution.

Unix Parquet ingestion now retains the source generation through metadata reads,
batch pulls, EOF and final prepublication validation. Independent reader opens
avoid shared seek cursors. See the
[stream-source API migration and generation limits](../reference/resident-native-results.md)
for the required public struct field and platform scope. These checks are not an
atomic filesystem snapshot or a new durability guarantee.

Metadata proof rejects approximate, unknown or internally inconsistent facts.
Native file pruning is existing production behavior; the new fixtures verify
complete results with and without physical statistics instead of claiming a new
pruning mechanism. Registry reports now distinguish selected contracts from
observed dispatch. A production registry dispatch bridge remains open.

## Focused evidence

The retained logs live under the machine-local evidence directory
`/Users/dylan/LocalData/shardloom/perf-all-20260906/`. Counts below overlap where a
test matches multiple filters and must not be added together.

| Check | Result | Log |
|---|---|---|
| Canonical numeric probe preservation | 6 passed | `plan-exhaustion-numeric-focused-r2.log` |
| Streaming lifecycle, pressure and generation | 21 passed | `plan-exhaustion-streaming-focused.log` |
| Prepared aggregate native acceptance | 15 passed | `plan-exhaustion-prepared-focused.log` |
| Public resident aggregate worker | 6 passed | `plan-exhaustion-resident-worker-focused.log` |
| Owned aggregate columns, including committed worker handoff | 9 passed | `plan-exhaustion-owned-focused-r2.log` |
| Existing native and compatibility sinks | 40 passed, 1 pre-existing ignored | `plan-exhaustion-sinks-focused.log` |
| File-backed shared-session serving | 3 passed | `plan-exhaustion-serving-focused.log` |
| Native file pruning differential fixture | 1 passed | `plan-exhaustion-pruning-focused.log` |
| Core library, including conservative statistics proof | 687 passed | `plan-exhaustion-core-focused.log` |
| Query primitive contracts | 13 passed | `plan-exhaustion-query-contracts.log` |
| Registry selection reporting | 12 passed | `plan-exhaustion-registry-focused.log` |
| Weighted COUNT workers and native spill | 25 passed | `plan-exhaustion-count-spill-focused-r4.log` |

The serving fixture proves serialized progress, cooperative cancellation between
reads, pressure recovery and ownership release. It does not establish FIFO
fairness, preemptive queued/blocked-I/O cancellation, or ingest-plus-short-query
latency distributions. Owned-output pressure checks separately cover admission
rejection and a real partially committed worker handoff, with complete independent
values at multiple worker counts and offsets. These fixtures are not throughput
benchmarks or process RSS ceilings.

Failed compile, lint and fixture attempts remain in their original logs. The
combined validation checks rebuild local crate artifacts after the shared-cache
check exposed a stale dependency across isolated worktrees. Frozen benchmark
binaries and source/reference artifacts are unaffected.

The faster ingest change is retained under the maintainer's instruction, with
its two full-size observations and unchanged 95.447305458-second baseline in the
[numeric-probe packet](ingest-numeric-probe-2026-09-12.md). Storage changes remain
isolated until their changed physical bytes receive complete validation.
