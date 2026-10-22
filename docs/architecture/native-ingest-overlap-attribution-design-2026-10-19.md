<!-- SPDX-License-Identifier: Apache-2.0 -->

# Current pre-writer overlap attribution

Status: the [completed attribution attempts](native-ingest-overlap-attribution-2026-10-20.md)
are inconclusive because the corrected observer and its bounded-event revision
both fail the frozen overhead gate. Temporary observer code is removed; eligible
recoverable-idle attribution remains a missing prerequisite. The original complete
observation is also retained as an invalid zero-prefetch setup. No new scheduling
candidate is admitted. This is conditional row 10, `PERF-09.overlap`,
under the existing RFC 0044 program and RFCs 0013/0014/0017. The
[conditional-first sequence](native-conditional-investigations-2026-10-19.md)
continues after nine finite decisions; all 58 substeps and CG-1 through CG-23
retain their gates.

## Question and current boundary

Does the current bounded writer leave material unused driver capacity while a
fully prepared successor is already available? The retained writer overlaps
source/read/derived/conversion and one next-input preparation with one active
source-batch subtree. Columns, zones, statistics and compression already run
concurrently within that subtree. A further subtree is not justified by summed
codec spans, source-pull time, total process CPU or an idle-looking host.

The current source is `ce9428fc1239ce9b6a65d55d857e05d46c878f97`.
`IngestRuntime` owns a Vortex current-thread executor and at most the admitted
caller plus background drivers. `BoundedIngestLayout` preserves local EOF and
one live child while its shared-slot handshake prepares one following item.
The complete public streaming writer owns conversion credits, file statistics,
staging, source-generation validation, commit and cleanup. The separate legacy
conversion pool cannot describe activity on this shared executor.

The [September 26 screen](writer-subtree-occupancy-screen-2026-09-26.md) retains
its historical measurements and accepted shared-slot correction. Its prepared
writer-only input, two-driver observation and earlier input reuse are not a
measurement of today's streaming pre-writer path. The current investigation
does not rerun the unchanged lookahead candidate or change the accepted policy.

## Vortex-first decision

Classification: `use_vortex_native_provider`. Keep pinned Vortex 0.85.0
`CurrentThreadRuntime`, `Executor`, `Handle`, `Task`, session allocator,
`LayoutStrategy`, locally sequenced children and file writer. Do not replace the
executor or add an engine, dependency, thread pool, codec or persistence format.
The observer wraps public task dispatch and existing ShardLoom boundaries only
in test builds. Normal packages and public diagnostics gain no observation API.

Source checks include the locked Vortex runtime's `mod.rs`, `handle.rs`,
`current.rs` and `smol.rs`; vendored layout compression/zoned/struct/chunked
writers and the file writer; ShardLoom `ingest_runtime`, `ingest_source`,
`vortex_ingest_shared_conversion`, `vortex_ingest_shared_stream`,
`ingest_bounded_layout` and the controlled streaming writer. The runtime's
public handle exposes no runnable-queue snapshot. Its private executor remains
unchanged. Provider/Native-I/O certificates and `fallback_attempted=false` remain
required; no external engine executes any residual work.

## Observer contract

Record monotonic event ordering under one bounded observer owner. Count a driver
thread once across nested synchronous future polls and CPU closures. Record
queued CPU tasks, running CPU tasks and separately dispatched blocking I/O.
Distinguish complete bounded ingest, native writer, source-batch child, input
poll and ready-successor intervals. Use actual readiness of a nonempty successful
following item at the existing writer boundary; a declared batch hint, empty
batch, error or EOF is not a ready successor.

For each child, partition elapsed wall intervals by the number of occupied
admitted drivers. Preserve the CPU-queue-empty subset and its subset with a
ready successor. Record child completion independently of waiting for the next
input. Keep final-child and EOF intervals separate. Also retain actual prefetch
window, completed/pending input bounds, source/conversion stage records, native
credit peaks, reservation denials, output bytes and publication outcome.

These are occupied-driver wall regions. A poll can include synchronous I/O,
preemption or blocking work; it is not thread CPU time. Async runnable state is
unknown. The eligible unoccupied-driver integral is opportunity evidence, not an
achievable speedup, complete-ingest upper bound or promise of recoverability.
Only a later independently frozen candidate comparison can demonstrate recovery.

Dispatch observation must preserve these lifecycle contracts:

- Dropping a raw `AbortHandle` continues work; explicitly aborting it may cancel
  work that has not started. Started CPU/blocking work is not preempted.
- Dropping a `Task` retains the upstream cancellation behavior; completion,
  panic and abort remain distinct. Retain the strong runtime owner through drain.
- A delegation adapter must not detach a `Handle::spawn_cpu` or
  `Handle::spawn_blocking` task by dropping its result receiver: the pinned
  implementation checks that receiver before invoking the closure. If a raw
  abort handle is detached, retain and join its delegated task on the same
  underlying runtime until it terminates. Do not add a driver for this join.
- Cancellation, panic and a never-polled future release observation tickets and
  credits exactly once. An observer overflow, invalid event or incomplete drain
  fails the observation. It must not silently truncate a successful record.

Installation is explicit and scoped to a test invocation. All source, conversion
and writer handles for the observed operation must use the same observer; root
polls are included. There is no environment switch in the production library.
The ordinary role in the same optimized test binary leaves observation disabled.
Observer metadata has a separate fixed diagnostic allowance, reported apart from
unchanged native grants. Instrumentation must not read payloads or warm statistics.

## Frozen finite workload

Use the resident official 99,997,497-row Parquet source under the existing local
storage/source-generation guards. Preserve its complete pre/post identity.
For row groups 0, 113 and 225, retain the first three 32,768-row batches,
including a shorter final batch inside that cap. Select row groups and caps
before reading values. Use the current production dictionary schema and existing
lean embedded-derived-column preparation. Record exact emitted rows, schema and
field names during deterministic preflight; a mismatch invalidates setup rather
than silently changing the workload.

The original 131,072-row geometry completed both cohorts but admitted zero
conversion prefetch slots in every P4 workflow. Its numerical zero opportunity
does not answer the intended ready-successor question. Preserve that complete
attempt, original calculation, source versions and exact helpers with an explicit
setup invalidation. The corrected fixed geometry keeps all 112 columns and the
same native grant; it changes only this diagnostic source batch size and prefix.
It does not describe the default full-file batch geometry or its performance.

Before workflow preflight, record the first independent source batch's reported
backing size, exact native-copy charge and the unchanged production memory
admission calculation. Every corrected prefix must admit a positive P4 window.
Then require positive actual prefetch admission in every P4 workflow and exactly
`batch_count - 1` converted and handed-off items in each observed P4 target.
Serial controls must retain zero prefetch; the one-batch P4 control admits the
path but converts/hands off no successor. All final and discarded counts must
be zero. Failure stops the experiment before cost comparison. Actual readiness
may still occur only after the preceding child finishes; that is a valid zero
opportunity observation once path activation is proved. Do not add pacing to
force a ready successor or require positive opportunity as an eligibility gate.

The bounded reader keeps production schema/source identity but consumes only
the named prefix. Its row/batch hints describe that prefix accurately. It enters
the current shared source/conversion runtime and controlled streaming writer,
including staged publication and source validation. Apply the existing large
source writer profile explicitly for this diagnostic, with 262,144-row blocks,
8-MiB target bytes and the profile's current admitted codecs/statistics. This is
a bounded current-profile workflow, not a full-file ingestion or a claim about
the default profile chosen for a small file. Keep profile admission and actual
settings in every record; do not introduce a new profile.

Use a 2-GiB native grant and a 256-MiB per-artifact cap. Keep source-provider
buffers and process RSS separate from managed credits. Execute one workflow at
a time under the existing workload guard. Prepare a fresh reader, native arrays,
session and output target for each role; no retained statistics cross samples.
No artificial pacing, battery/thermal gate or uncontrolled resource increase is
allowed. Source pre/post hashing, full reference readback and archive checks are
outside the recorded bounded-ingest interval.

| Cell | Source prefix | CPU grant | Purpose |
| --- | --- | --- | --- |
| `rg0-p4` | Row group 0, at most three batches | 4 | Opportunity target |
| `rg113-p4` | Row group 113, at most three batches | 4 | Opportunity target |
| `rg225-p4` | Row group 225, at most three batches | 4 | Opportunity target |
| `rg0-p1` | Same group 0 prefix | 1 | Serial control |
| `rg113-p1` | Same group 113 prefix | 1 | Serial control |
| `rg225-p1` | Same group 225 prefix | 1 | Serial control |
| `rg0-one-p4` | Row group 0, first batch only | 4 | No-successor control |

Freeze both cohorts A and B before running either. Each has three paired repeats
of every cell, alternating ordinary/observed order and reversing the first role
in B. Complete both cohorts before inspecting cost results. Keep all 84 workflows
and every failure. A failed workflow invalidates its cohort; do not combine
partial cohorts or substitute a later faster sample.

The complete bounded-ingest interval starts before source opening and ends after
the controlled writer returns, with source/conversion tasks and drivers drained.
It includes source metadata, bounded reading, derived columns, conversion,
statistics, encoding, output digest, validation and publication. Keep native
writer and stage intervals separate. Routine query timing communication still
uses engine time only; these diagnostic clocks are not Full43 engine timings.

## Correctness, instrumentation and decision gates

Before measured cohorts, prove deterministic histogram partitioning, nested
driver counting, CPU-ticket cancellation, child/ready/EOF classification, queue
and record bounds, raw-handle detach/abort, task panic/abort, started blocking
work, root polling and complete drain. Run the existing shared-slot, owned
writer lifecycle and text-zone reopen selectors on the observer-enabled build.
Use tiny fixtures for empty input, nullable/partial batches, source/conversion
failure, blocked cancellation and a competing destination. No measured timing
from these checks enters the attribution result.

Every measured artifact must pass complete ordered values, logical schema,
row-count and current statistics/layout checks against independently reopened
source batches. Record full artifact identity/bytes and ordinary/observed parity.
Check final source identity, no-fallback certificates, exact owned cleanup and
zero remaining observation/native reservations. A successful prefix cannot
certify a failed writer or publish a partial result.

Instrumentation is eligible only if, in each cohort and every cell, the observed
median complete-ingest time is no more than 10% above the ordinary median.
Require exact output parity and all lifecycle/accounting checks independently
of that threshold. Failure of the observer gate is inconclusive, not no signal;
repair or redesign the observer with preserved prior evidence before new cohorts.

For an eligible observation, calculate each P4 target's median of:

`ready-successor, CPU-queue-empty unoccupied driver nanoseconds /
(admitted driver count × complete bounded-ingest nanoseconds)`.

At least two of the three fixed P4 targets must reach 5% in both cohorts to
justify designing a bounded two-subtree candidate. The single-batch control must
have exactly zero ready-successor opportunity. Report each cell and cohort;
aggregate positives do not override a failed correctness or observer control.
Below that threshold, record no material signal in this finite scope and remove
the temporary observer. This does not establish general ingest optimality.

A positive screen authorizes only a new design and frozen comparison protocol.
It does not retain scheduling changes or close broader PERF-09. Any candidate
must preserve the original grant, bounded completed blocks, ordered feed, local
EOF, cancellation, source identity, exact staging ownership and publication
races, then pass complete local acceptance including guarded full ingest and
downstream queries. Release/publication authorization remains separate.
