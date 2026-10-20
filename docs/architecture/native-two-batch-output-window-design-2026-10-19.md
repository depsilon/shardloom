<!-- SPDX-License-Identifier: Apache-2.0 -->

# Two-batch native Python output window

Status: finite candidate design for `PERF-07.output_windows`, frozen after the
separate attribution and before candidate implementation. This is an experiment,
not an admitted package capability or closure of the broader owner.

## Evidence and scope

The unchanged demand path passed all 42 debug and 184 release attribution
workflows. Every tracing control passed. Only `project-65536-8-zstd` met the
frozen real-consumer wait, paired-overlap and space prerequisites in both
cohorts. The complete original sources, failed attempts, exact values and
decision are preserved in the independently reopened attribution packet with
SHA-256 `296335feab2c08fb87c44bdc6d3ec6e8f278dbfb9348564db6712d3ac47c3840`.
That observation permits a candidate test; it is not a measured overlap gain.

The expert comparator is a streaming-engine maintainer who keeps source demand,
allocation lifetime, provisional delivery and final completion distinct.
Vortex-first classification remains `use_vortex_native_provider`: pinned
Vortex 0.85.0 arrays and the existing native-to-Arrow delivery adapter retain
payload ownership. Provider scan buffering does not define this Python
acknowledgment contract. No new dependency, execution provider, codec, worker,
engine lane, file prefetch or output representation is introduced.

## Explicit admission

The temporary `output-window-candidate` feature is absent from ordinary feature
groups. The experimental native Python `Plan.iter_batches` accepts an explicit
`output_window_bytes=4194304` request; omission/zero keeps the existing demand
policy. Booleans, negative values and other sizes are rejected. A build without
the feature rejects a nonzero request before starting a worker. This option is
not advertised on other public facades until a candidate passes acceptance.

Admission inspects the actual bound native plan. It accepts a direct immutable
memory scan or one pure projection over that scan, with no filter/residual,
dynamic schema, parameterized input, file/preparation source, streaming batch
input, spill or other operator. The source has 0–65,536 rows; the output has
1–64 Int64 fields. Its exact output row count and existing 2,048-row delivery
partition are known. Other plans keep per-batch acknowledgment and report the
requested window as ineligible. This is an explicit native policy selection;
there is no external executor or hidden read-ahead on an ineligible source.

The declaration remains lazy. Only first demand starts preparation/execution.
The request permits bounded read-ahead from the already immutable native memory
source; it does not authorize advancing an effectful producer, changing source
semantics or consuming another file ahead of demand.

## Window and lifetime

Keep the current one-event channel. The worker may produce the successor of a
delivered batch before its acknowledgment. At most two sent, unacknowledged
batches exist: one may be with the consumer and one may be in the channel.
Before advancing beyond the second, the worker waits for the oldest acknowledgment.
The final callback drains every outstanding acknowledgment before returning to
native source validation and execution certification. An enqueue never means
query completion.

The fixed ledger holds two byte counts and ordinals. It reserves 1,024 bytes of
metadata from the original native memory pool before creating the ledger. Every
batch's Arrow referenced-memory size must be at most 2 MiB, and the sum of sent,
unacknowledged referenced sizes must be at most 4 MiB. Oversized batches fail
explicitly before being queued. Referenced size is conservative and can count
aliases repeatedly; it is not a unique allocation or RSS measurement. One
currently producing/converting batch uses the existing producer reservations;
it receives no uncharged payload allowance from the queue policy.

Acknowledgment frees only the logical window slot. It never releases an Arrow
or Vortex payload reservation. Existing leases remain with every escaped batch,
clone, slice and imported consumer array until its last owner disappears. New
output allocation, source ownership and all retained buffers compete for the
same original 64-MiB grant. Admission is not a promise that arbitrary retained
client results will fit.

All waits remain cancellable. Early close drops the event receiver before
joining the worker and drains queued owners. A cancellation is checked before
delivering a queued batch. Late source/kernel/conversion failure can follow
provisional values, but never produces a final report. A failed operation must
leave a subsequent fresh operation usable. Reentrant same-session work must
fail explicitly at consumer waits instead of deadlocking.

## Measurement and frozen decision

Use one fresh compiled library with two explicit roles: ordinary acknowledgment
and the requested window. Keep all ten original workload cells, source formulas,
native resources, real IPC consumers, empty/small overrides and complete-output
oracles. Only the single target admitted by attribution can earn retention;
the other nine cells are controls. Artificial slow/bursty delays are controls.

Both roles use the same revised bounded observer. Native engine time is the
complete prepared call minus its synchronous conversion/send/wait callback
spans, exactly as in attribution. Acknowledgment timestamps are indexed by batch
and may follow an earlier callback's exit under read-ahead. They are actual
worker receipt times, not invented completion marks. Preserve every callback,
send, acknowledgment, queue byte/count high-water mark, final zero ledger,
native allocation, source, certificate, provisional response and complete result.

Before release comparison, run focused ledger, admission, cancellation, retained
owner and complete-value checks plus the existing native binding suite. Debug
smoke covers every cell at 0/17 rows and the admitted full target in both roles
(42 workflows). Release preflight covers every cell at 0/17/declared rows in
both roles (60). Two cohorts then run three samples per cell/role and one
excluded target warmup per role/cohort (124 workflows). A runs acknowledgment
then window in declared cell order; B reverses role and cell order. Both finish
before inspection. Do not tune the candidate, select samples or add cohorts
after comparative results are visible.

Retention requires the admitted target's complete-workflow median to improve
by at least 10% and 5 ms in **each** cohort. Every cell in both cohorts must avoid
an engine, complete-workflow or first-provisional-response regression exceeding
both 5% and 1 ms. Native peak growth relative to acknowledgment must stay within
4 MiB plus the 1,024-byte ledger charge, and every sample must stay under the
original grant. Correctness, actual admission/activation, final acknowledgment,
queue bounds and final owner release are unconditional gates. Small/empty
workflows may have no overlap but must preserve exact behavior.

Passing the finite comparison still requires full native/workspace, installed
Python/public, Full43 and modular acceptance for the final integrated source.
Failure preserves the attribution and every candidate attempt, removes the
experimental code after archive verification and proves exact accepted-source
restoration. No result closes broader output windows, transport, publication,
required roadmap work or CG-1 through CG-23.

## Required focused proof

Cover empty, one-row, partial/final batch, nullable/all-null, alias and arithmetic
outputs; 65,536/65,537 rows and 64/65 output fields; direct scan/projection versus
sort/filter/limit/dynamic/file/one-shot/parameterized controls; precise two-slot
and 4-MiB limits; oversize denial and ledger metadata pressure; out-of-order or
duplicate acknowledgment; no completion while the final consumer is paused;
late arithmetic error; early consumer exception/close; pre-cancel, cancellation
during a full queue, idle deadline and cross-thread close; retained batches,
slices and Arrow owners after stream/plan/source/session retirement; and clean
subsequent execution. Use deterministic synchronization for concurrent tests.
