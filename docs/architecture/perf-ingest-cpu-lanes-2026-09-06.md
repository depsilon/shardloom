# Ingest CPU ownership and bounded overlap candidate

This PERF-03/PERF-08/PERF-09 candidate follows RFC 0044 and the supplied
review's instruction to measure matched 1/2/4/8-worker controls before adding
cohort concurrency. The immutable `6b0890d3` control is measured separately while
the working-tree candidate integrates `ingest_cpu_lanes.rs` into the existing
source, conversion and writer owners. No candidate performance result or
validation pass is established by this document.

The first public scaling control used frozen runtime `75fc09a0` (unchanged
ingest composition from `6b0890d3`). Requested parallelism one was raised to two
by the public CLI's legacy floor: 95.539879 seconds, 3,127,197,696-byte OS peak
RSS and the unchanged 18,643,482,956-byte artifact. It is an effective-two
observation, not evidence of one-lane throughput. Requested and effective grants
must remain separate in every scaling comparison.

The candidate preserves every explicit positive `--max-parallelism` ceiling,
including one; automatic defaults retain their existing minimum of two. Existing
floor evidence fields remain compatible and report false for an explicit ceiling.
Zero is still invalid. Native tests must prove progress at one, and public tests
must prove the same resource value reaches execution and its evidence. This
corrects override handling; it is not itself an algorithmic speedup claim.

## Current source and provider contract

The frozen control Parquet source assigns `requested - 1` background source threads in
`universal_format_io.rs::parquet_row_group_source_parallelism_budget`, then
starts independent task readers in `ParquetRowGroupParallelRecordBatchReader`.
`vortex_ingest.rs::StreamingColumnarVortexArrayIterator::new` separately starts a
ComputePool with up to four conversion workers. The native writer separately
starts `requested - 1` provider runtime drivers through
`LocalVortexWriteContext::apply_runtime_policy`. For sufficient source tasks the
configured execution-owner maxima are therefore:

| Requested setting | Caller | Source | Conversion | Provider drivers | Sum |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 0 | 0 | 0 | 1 |
| 2 | 1 | 1 | 1 | 1 | 4 |
| 4 | 1 | 3 | 3 | 3 | 10 |
| 8 | 1 | 7 | 4 | 7 | 19 |

This is a source-derived count of configured owners, not a measurement of
simultaneously busy CPU cores. Source-task availability can lower it. Blocking
I/O threads, hidden provider workers and process-wide threads are separate.

Pinned Vortex 0.85 provides the required caller progress path:

- `vortex-io/src/runtime/current.rs:103`: `CurrentThreadRuntime::block_on` runs
  the shared Smol executor on its caller. Its default requires no background
  provider driver.
- `vortex-file/src/writer.rs:583`: `BlockingWrite::write` drives the complete
  native writer through that `block_on` call.
- `vortex-io/src/runtime/smol.rs:45`: CPU work is enqueued onto that executor;
  blocking I/O uses its separate instance-owned blocking pool.
- `vortex-file/src/segments/writer.rs:51`: the native sink waits on
  `SequenceId::collapse` before assigning ordered segment IDs and streaming
  buffers through its bounded channel. This is also the ordering boundary for
  a later overlapping-cohort experiment.
- `vortex-io/src/runtime/pool.rs:46`: reducing `set_workers` signals detached
  threads without joining them. It cannot immediately release a CPU grant for
  another owner. Use ShardLoom's owned/joined driver pattern when integrating.

## Allocation and progress

The helper reserves one caller lane, grants independently executable source
tasks first, then grants conversion workers up to their explicit prefetch-slot
cap. Remaining lanes drive native writer CPU work. Source/conversion targets
are explicit policy inputs. The first runtime candidate selects one source
driver and at most one converter, leaving the rest to native writer CPU work:

| Grant | Caller | Source | Conversion | Provider drivers |
|---:|---:|---:|---:|---:|
| 1 | 1 | 0 | 0 | 0 |
| 2 | 1 | 1 | 0 | 0 |
| 4 | 1 | 1 | 1 | 1 |
| 8 | 1 | 1 | 1 | 5 |

This is a measured-candidate recipe, not a claim that these ratios are optimal. At grant one,
source pull and conversion are synchronous on the caller. At grant two, one
source thread supplies a bounded channel, while the caller converts and drives
the writer. No zero-worker ComputePool or producerless source queue may be
constructed. If the source has no independent task support but conversion has
a lane, that worker pulls the synchronous source itself. This last route is
admitted only for synchronous source providers that do not await CPU work on
the caller's currently blocked native runtime.

The plan remains immutable while owned threads or work are live. Reconfiguration
validation requires zero live source/conversion/provider threads, no active or
queued tasks and no retained unpublished batches. A no-op does not require
teardown. These are checks on actual caller-supplied ownership observations,
not a substitute for joining or a certificate for unobserved threads. Ordinary
cohort/WIP adjustment can occur at completed task boundaries without moving CPU
lanes; changing lane ownership requires the stronger drained/joined boundary.
Never resize an upstream detached worker pool and assume its previous threads
have stopped. Blocking I/O progress remains available and separately reported.

## Runtime candidate integration

1. Preserve public options and source-request types. The native source helpers
   use the common recipe before starting any threads. The streaming writer then
   reconciles the admitted source's existing executor status/count into a typed
   private plan before creating conversion or provider drivers. A source count
   exceeding the request fails explicitly; no requested value is fabricated.
2. Apply the source grant inside
   `universal_format_io.rs::stream_flat_parquet_columnar_source_with_batch_budget`
   and the columnar prefetch constructor. The latter reports its actual one
   thread, rather than reporting channel capacity as thread count. Row-group
   task order, source bytes and complete-source admission are unchanged.
3. Use the reconciled plan in `StreamingColumnarVortexArrayIterator::new`: conversion
   zero selects the existing inline path; otherwise construct exactly the
   admitted ComputePool and retain the current queued-plus-completed byte bound.
   The prefetch window remains at most `min(requested - 1, 4)`; no queue expands.
4. Use exactly `provider_drivers` in `LocalVortexWriteContext` with the existing
   shared `resident_worker_group.rs` owned/joined implementation. Each write owns
   its driver guard; the reusable thread-local context retains no idle driver
   or detached pool into the next source launch. Source readers and conversion
   pools retain their existing cancellation/drain/join boundaries.
   Keep the artifact session allocator and native
   source-buffer leases. Codec/stat task concurrency remains unchanged: these
   native tasks run on the admitted provider executor and create no new driver
   grant. Requested task concurrency is not measured active CPU parallelism.
5. Preserve `stream_write_options_for_decision`, codec policy, batching,
   bounded per-batch EOF, checksum/readback and publication for the first
   integration. The existing physical-design topology evidence records requested
   total, configured total, caller, source/conversion/provider driver counts,
   joined lifetime and the explicit exclusion of blocking I/O/source-library
   internal threads. These are constructed owner grants, not active CPU time.
   Only after matched controls establish headroom, test two then
   four concurrent child futures using the same CPU plan and locally scoped
   sequence descendants. Charge active input and completed-but-unpublished
   encoded buffers before increasing cohort admission; preserve root-reference
   credits through the footer. No extra producer threads or widened array queue.

The eventual task controller should consume actual source starvation,
conversion/codec busy work, sink sequence waits and retained output pressure.
`shardloom-exec/src/pulseweave.rs::plan_endopulse_adjustment` currently returns a
one-window recommendation. The next integration must apply bounded cohort/WIP
decisions at completion boundaries, with hysteresis and an explicit no-op when
evidence is insufficient. It must not introduce per-row adaptation or claim
that an unchanged schedule was adapted because a report field changed.

## Verification and measurement

Isolated tests cover allocations at 1/2/4/8 and extreme integer grants, limited
source tasks, bounded conversion slots, inline paths, invalid zero grants,
and refusal to replace still-owned plans. A native proof writes two batches,
reopens every exact Int64 value above 2^53, and completes blocking I/O at grants
one/two without a background native driver. The integrated regression writes
and completely reopens renamed nullable text plus exact identifiers for native
Parquet task readers and the single-prefetch adapter at grants 1/2/4/8; it checks
the actual plan and result report together. Repeated 8/1/4/2/8/1 driver changes
exercise per-artifact owned teardown, supported by the existing exit-guard and
partial-spawn-failure join tests. These native tests and the public CLI propagation
tests pass. The public resident worker also executes a real filtered count at one
lane, proves zero background workers, and resets its retained operation on a
change to two. Matched large-source measurements remain pending.

Measure frozen control and candidate at each identical requested setting with
the same 24 GiB memory setting, source generation and writer profile. Report
the actual lane allocation as well as native user/system CPU, wall time and RSS.
The inner `write_micros` is sampled before the artifact-scoped driver guard drops;
joining those drivers is included in the outer artifact call/process wall, not
that inner writer span. Do not substitute the inner span for the full lifecycle.
Alternate control/candidate order and preserve at least three timed samples
when storage permits; no cold-cache claim. Keep one fresh output at a time
under the unchanged 100 GiB workspace / 256 MiB log / 12 GiB free-space guards,
with a 24 GiB per-artifact cap and full publication/verification. Record segment
geometry, complete physical encoding inventory, all default file statistics and
all 129 exact query results before interpreting full-suite scores. Run the
native memory example separately; it is not an ingest scaling control.

An unconditional text-zoning switch, copied numeric-accessor restoration,
larger disconnected prefetch queue, repeated metadata inventory, and another
prepared-count microbenchmark duplicate completed or rejected work. Joint
codec/consumer selection remains a later distinct measurement: current numeric
compression and original-width consumers are controls; bounded categorical
dictionaries/FSST versus Zstd need complete ingest, first-query and repeated
query costs, dictionary epochs, nulls and actual consumer evidence. Do not move
preprocessing to the first query or select policies by ClickBench field names.
