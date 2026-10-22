<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Python output-window attribution

Status: finite pre-experiment design for `PERF-07.output_windows`. No queue
candidate is admitted by this document. The October 19 conditional sequence,
RFC 0044 and the existing demand/lifetime contracts remain authoritative.
The expert comparator is a streaming execution maintainer separating native
work, representation conversion, client work and demand wait without treating
an artificial delay or an enqueue as completed execution.

## Current boundary and provider check

The native Python `Session` / `Plan.iter_batches` route prepares a real shared
native operation. Its worker sends one converted batch through a one-event
channel and waits for the caller's next demand. `with_consumer_wait` marks an
admission state and keeps the original allocation; it is not a duration counter
or a release of resources. Final reports follow successful source validation,
all deliveries and final demand. Early close, failure and cancellation leave
completion absent; escaped Arrow/native owners remain valid and credited.

The streaming Arrow path currently has no engine clock. Python `next()` latency
includes conversion, transport and wrapper work and cannot replace that clock.
The process JSON transport also waits for acknowledgments, but its serializer,
framing and process costs are a different boundary. This finite experiment uses
the native Python columnar path only; process transport is inventoried, not
silently treated as equivalent evidence.

Vortex-first classification: `use_vortex_native_provider`. Keep pinned Vortex
0.85.0 `ArrayRef` ownership, the existing prepared relational batch callback and
the explicit Arrow export adapter. Pinned `ArrayStream` carries DType and array
chunks; its `read_all` collects chunks. `ScanBuilder` already buffers provider
scan tasks under its configured concurrency. Neither defines the Python demand
handshake or a ShardLoom output-credit window. No provider, dependency, execution
kernel, source prefetch or persistence behavior changes in attribution.

## Observation contract

Add a temporary, opt-in `output-window-observation` build feature, absent from
normal/default/release feature sets. The feature connects bounded thread-local
observation at the native prepared execution callback to the Python stream's
actual send/acknowledgment boundaries. It changes neither queue capacity nor
demand, cancellation, allocation, validation or completion behavior. No observer
is active unless the experiment explicitly requests `clock` or `trace` mode.

Both modes measure the complete `for_each_batch_with_input` call and its
synchronous callback spans. `QueryTiming::from_elapsed` subtracts callbacks,
which include Arrow conversion and all consumer waiting. The remainder includes
native source checks, operators, admission and execution certification, and is
the experiment's engine clock. Arrow export/report finalization is outside
that native call and remains in complete-workflow cost. Missing, overlapping,
nested, truncated or inconsistent observations invalidate a workflow.

Trace mode additionally records native callback entry/exit, converted-batch
readiness, send completion and demand acknowledgment, in one monotonic worker
clock. Each record retains row count and Arrow referenced-memory size, explicitly
not a unique allocation count. At most 128 records are allowed; overflow is an
invalid observation, never silently dropped evidence. Observation buffers and
caller fixtures/IPC witnesses are bounded test infrastructure outside the
native grant, not evidence of an RSS limit. Clock mode keeps aggregate counts
and timings without detailed records. Both modes use the same executable.

The experimental observation is available separately from completion. A failed
call may expose diagnostic spans but never a successful query report. Tests
check that source/demand effects, deadlines, closed-consumer drain and final
owner release remain unchanged. The original accepted runtime can be restored
exactly after the finite decision; no new public timing capability is claimed.

## Workload and consumers

Use an immutable native memory source imported explicitly through `from_arrow`
before result demand. Fields are nullable Int64 `k`, nonnullable Int64 `id`,
nullable Int64 `x` and `y`; source generation is specified in the frozen JSON
protocol. Ordinary outputs are `k`, `id` and `d_j = x * (j + 1) + y`.
No external query engine executes this plan. Source import is an explicit
compatibility-input boundary, and Arrow IPC is an explicit client-output
consumer; Vortex remains the execution substrate.

Four targets combine 32,768/65,536 rows with eight/32 derived columns and a
real Arrow IPC consumer using Zstd level 3 without codec worker threads. Each
batch is fully serialized once and every emitted IPC message is retained and
reopened independently for complete schema, values, nulls and order. Client
compression is not ShardLoom engine execution. The fast consumer uses the same
IPC path without compression. A blocking stable sort, alias-only projection and
17-row input are controls. Two additional controls add fixed steady/bursty
client delays; they can validate wait accounting but cannot qualify opportunity.

Every workflow owns a fresh session, one immutable source and prepared plan,
one complete stream and one consumer. It records first provisional batch and
final completion separately. Native memory is 64 MiB with one CPU lane, zero
provider background workers and 2,048-row native output batches. No native spill
or file output is requested. Every successful workflow releases stream, plan,
source and output owners and checks zero final native reservations.

Debug smoke runs every cell at zero/17 rows in both modes, plus one full target
in both modes: 42 workflows. Release preflight runs all ten cells at zero, 17
and declared row counts in both modes: 60 workflows. Two frozen cohorts then
run three samples per cell/mode and four excluded warmups: 124 workflows. A runs
clock then trace; B reverses modes and cell order. Both cohorts finish before
comparison. Independent validation precedes every decision.

## Opportunity gate and limits

Detailed tracing must avoid an engine or complete-workflow median regression
against clock mode exceeding both 5% and 1 ms for every cell in each cohort.
Semantic/source/ownership/failure checks are mandatory independently of costs.
No sample, workload or threshold is selected after results are observed.

A target qualifies in both cohorts only if its nonterminal acknowledgment waits
sum to at least 20% and 5 ms of its native-call elapsed time, and the paired
overlap upper bound is at least 10% and 5 ms of complete-workflow time. For batch
`i`, pair its acknowledgment wait with native work plus conversion producing
batch `i+1`; sum the smaller duration for each adjacent pair. Exclude terminal
wait, initial preparation, work before the first output and final validation
from assumed recoverable time. Deliberate delay controls never qualify.

This bound is an admission estimate, not a measured speedup: CPU/cache
contention, scheduling, allocation and handoff can reduce or eliminate it.
Require each observed batch's referenced size to fit a prospective 4-MiB window,
and the current native peak plus that full allowance to fit the original
64-MiB grant. These are prerequisites, not proof of future queue ownership.
The source/plan is effect-free and immutable, but current demand semantics are
unchanged; any candidate needs an explicit bounded read-ahead contract.

If at least one real target passes the entire gate in both cohorts, freeze a
separate candidate design and retain/drop protocol before implementation.
Otherwise record the finite no-signal or observer-ineligible decision, preserve
all original evidence and restore accepted source. No artificial wait, static
source inspection or incomplete workflow establishes overlap benefit. Broader
output windows, transport, publication and CG-1 through CG-23 remain open.

## Verification and source anchors

Before release measurement, cover observer nesting/overflow/thread isolation;
empty and mixed-null complete outputs; no producer effects before demand;
one-shot producer progression; late native errors; consumer exception/close;
pre-cancellation and cancellation during wait; deadlines during caller idle;
retained batch/slice validity; grant denial; and subsequent fresh-operation
success. Run existing incremental public binding tests with zero skips and
new focused observation checks. Reject false completion and missing clocks.

Source anchors: `shardloom-python/src/stream.rs`, `plan.rs`, `session.rs`;
`local_primitive_prepared_relational.rs`, `local_primitive_relational_arrow.rs`,
`resident_result_arrow.rs`, `resident_operation_resources.rs` inside
`shardloom-vortex`; `native_runtime_operation.rs`, `public_relational_batches.rs`
and `python_batch_protocol.rs` inside `shardloom-cli`; pinned
`vendor/vortex-array/src/stream/` and `vendor/vortex-layout/src/scan/scan_builder.rs`.
Original and instrumented source hashes, fresh build and loaded library
identities, Python/PyArrow/codec versions, guards and every output are frozen.
