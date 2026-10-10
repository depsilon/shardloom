<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Batch Input and Incremental Results — October 6, 2026

Status: accepted local source at `3ec56331241f32c11046d575e967e5a874f5226f`,
with regression acceptance repeated on combined commit
`99e0a4b304c506a2b24f605eb5156859b356486a`. These APIs merged in
[PR #1526](https://github.com/depsilon/shardloom/pull/1526) after all 39 checks
passed. Published v0.4.0 packages predate these additions.
They extend universal workflow step 2 under the existing PERF and CG-20/21
owners through one native Vortex plan.

The [October 9 input growth contract](native-input-growth-2026-10-09.md)
supersedes this initial contract's fixed input field/batch counts and result
field count in source builds after the 0.5.1 release. Growing metadata consumes
the shared query grant. Per-frame row/byte limits and complete collection's
row/byte bounds remain. Published 0.5.1 artifacts retain their release-time limits.

## Public usage

`shardloom.from_batches(...)` and `ShardLoomContext.from_batches(...)` declare native
resident input. Each supplied batch is a sequence of row mappings. Declaration
does not call the producer. Supply an iterable for one execution or a factory
that returns a fresh iterable for repeated executions.

```python
import shardloom as sl

def input_batches():
    yield [{"order_id": 1, "amount": 12.5}, {"order_id": 2, "amount": None}]
    yield [{"order_id": 3, "amount": 7.0}]

frame = sl.from_batches(
    input_batches,
    schema={"order_id": "int64", "amount": "float64"},
)
with frame.iter_batches(batch_rows=1024) as batches:
    for batch in batches:
        print(batch.result_rows)
    report = batches.report
    assert report is not None
```

`LazyFrame.iter_batches(...)` and `SqlWorkflow.iter_batches(...)` consume
admitted results incrementally. They accept the existing `memory_gb`,
`max_parallelism` and optional `spill` policy. `batch_rows` must be 1–2,048.
Each `ResultBatch` exposes `index`, `result_rows`, `result_schema`,
`result_columns`, `python_objects`, `to_pandas()`, `to_arrow()` and `to_numpy()`.
Conversions consume the delivered values; they do not execute another query.

## Admission and lifecycle

The table below records the original October 6 admission snapshot. Use the
linked growth contract for current source field and cumulative-input admission.

| Boundary | Contract |
| --- | --- |
| Input schema | 1–128 distinct fields, each name 1–256 UTF8 bytes; nullable Int64, finite Float64, Boolean and UTF8 |
| Input batch | At most 2,048 row mappings and 8 MiB per input frame |
| Input lifetime | At most 4,096 batches per declared source; accumulated native input must fit the shared resident grant |
| Result batch | At most 2,048 rows and 8 MiB per result payload frame; oversized individual rows fail |
| Transport envelope | A separate 16 MiB wire-frame ceiling includes framing and metadata |
| Small complete collection | Existing 65,536-row, 128-field and 8 MiB limits remain explicit |

This October 6 table records resident input. A subsequent opt-in
`from_batches(..., streaming=True)` mode has its own
[contract](native-input-completion-2026-10-07.md) and
[local acceptance](../benchmarks/native-input-completion-2026-10-07.md).
It preserves the finite field, row/frame and total batch-count bounds while
retaining at most one native input batch. That mode permits one source used once
through pure Scan/Filter/Project, with incremental results, bounded small
collection or one native Vortex destination. Cumulative input may exceed the
grant. That initial acceptance does not authorize input or general state spill.
Later [ordering, aggregation, join and analytic-window contracts](../reference/native-query-spill.md)
extend operator composition and explicit state spill, while preserving these
intake limits and the absence of a process-RSS bound.
Resident mode stays the default, and published v0.4.0 is unchanged.

Resident batch intake is demand-driven, but it does not provide input spill or guarantee
that arbitrary total input fits in memory. Result delivery supports the
already-admitted typed and static nested schemas; that wider result contract
does not widen the four-domain input schema above. General Variant/extension
support and broader operator state spill remain separate.

The iterator acknowledges the preceding output batch only when the consumer
requests another. Native production waits for that acknowledgement. All earlier
batches are provisional until final source-generation validation, complete
acknowledgement and successful execution. `batches.report` remains `None`
until then. A consumer must not publish a successful prefix after a failed or
cancelled operation.

Use a `with` block or call `close()` when stopping early. Close, consumer error,
bad frames and native failure cancel/drain the owned operation and release its
native owners. A consumed one-shot input rejects reuse; it is not silently
replayed. Factories are called anew for subsequent explicit executions.
Consumer-retained Python objects and conversion libraries have their own
allocations outside the native grant. The Python wait timeout applies to each
iterator operation; it cannot preempt arbitrary synchronous producer code or
bound time spent by the consumer.

The reviewed transport uses Unix facilities and has runtime acceptance on
macOS arm64. This is not cross-platform parity. Prepare compatibility file
inputs explicitly to Vortex before `iter_batches()`: automatic preparation
does not share this transaction's external cancellation owner. Native file,
memory and batch sources then use the same plan, resource policy, source
validation and no-fallback checks as ordinary collection and writers.

## Writers and evidence

Batch sources pass complete output through native Vortex, CSV, JSON, JSONL,
Parquet, Arrow IPC, Avro and ORC writers, subject to their existing feature/type
contracts. Existing destinations remain rejected by the atomic-publication
route. Output typing and metadata loss follow the existing native/compatibility
contracts; batching does not add hidden Python or external-engine execution.

The final combined acceptance includes 48 batch checks and 19 format-fidelity
checks, plus source tests, complete public/native regressions and all 129
Full43 calls. The preceding paired adapter screen includes 324 complete calls
across 54 cells, with no predeclared time or RSS investigation threshold
crossed. Retention is for API availability and lifecycle correctness, not a
speedup claim. See the [acceptance report](../benchmarks/native-engine-acceptance-2026-10-06.md).

The original batch run compared complete streamed values but did not retain
every streamed frame. Finalization reconstructed expected payload digests and
reopened the final reports. Independent format readers verify the compatibility
outputs; Vortex readback uses ShardLoom. These evidence boundaries do not imply
an independent Vortex decoder, general recovery, total RSS control or production
certification.
