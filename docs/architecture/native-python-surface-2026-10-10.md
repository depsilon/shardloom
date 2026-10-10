# Native Python data and operation ownership

Status: maintainer-directed follow-on to required explicit resources, October 10.
This implementation contract follows the accepted local typed-input work. It does
not ship the old experiment, declare a binding ready, or establish a performance gain.
Owners remain PERF-02/03/07/11/12 and the existing capability gates. Version 0.5.1
stays fixed; all remaining six areas/eight investigations remain in the queue.

The engine already computes in Rust. The priority is removing compulsory
native-columnar to Python-row to columnar conversions and per-value JSON input
transport, while retaining one shared native planner and execution implementation.

## Requested implementation order

1. Native Python result/batch owners over current ShardLoom-owned arrays, with
   continued native operations, existing writers, direct columnar export and
   explicit opt-in row conversion.
2. Direct columnar intake with exact shared schema semantics and real producer
   ownership/accounting; ordinary Python object input retains explicit conversion.
3. In-process session and complete-operation bindings using the same shared
   allocation, source identity, cancellation, diagnostics and certificates.
4. Native expression/prepared-plan handles feeding the same representation as
   actual SQL parsing. Extract callable core interfaces from CLI handlers where
   necessary; do not duplicate SQL or operator semantics inside Python bindings.

The first three share an ownership boundary and may form one implementation
unit. Preserve the subprocess option for isolation, without capability divergence
or an implicit transport fallback. Cross-process columnar transfer requires a
real IPC/shared-memory contract; process-local C pointers cannot cross a pipe.

## Grounding and provider checks

The old `experiments/python-native` proposal is preserved on local branch
`codex/perf-remaining-work` at `be2a69f3`, outside shipping manifests. Its September
6 record says dependency resolution, build, load, tests and measurement are
pending. It did not fail a measured performance gate. Reuse sound ownership ideas
against current relational/streaming APIs; do not import its narrow operation set,
16-MiB source limit, 64-handle limit or other experimental caps into the product.

Current native owners include `ResidentVortexSession`,
`OwnedVortexResultBatch`, `ResidentMemorySource`, prepared relational execution
and per-batch native consumers. Current owned-column intake intentionally rejects
foreign/unbudgeted arbitrary arrays. Columnar import/export must resolve that
ownership gap, not bypass it. Vortex remains the execution substrate and native
persistence; Arrow is an optional compatibility boundary.

Official primary references checked October 10:

- [Arrow C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html):
  same-process exchange and producer release callbacks; not cross-process storage.
- [Arrow C Stream interface](https://arrow.apache.org/docs/format/CStreamInterface.html):
  streamed columnar batches, end/error/release lifecycle.
- [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html):
  Python capsule ownership and transfer protocol.
- [PyO3 parallelism guidance](https://pyo3.rs/main/parallelism): detach during
  substantial Rust-only work. Validate the exact chosen version and complete
  dependency/license graph before intake; the old proposal is not that proof.

Pinned Arrow Rust 58.3.0 and Vortex 0.85.0 offer relevant conversion/FFI surfaces.
Verify enabled features and safe provider APIs. Keep workspace unsafe-code policy
unless a separately documented, narrowly reviewed provider boundary is necessary.
No external engine, Arrow evaluator, new semantic engine or hidden row bridge.

## Optional binding dependencies

The native binding crate is feature-gated; the lean workspace does not link
Python. The October 10 lockfile intake adds `arrow-pyarrow` 58.3.0, matching the
existing Arrow graph, PyO3 and its four support crates at 0.28.3, and
`target-lexicon` 0.13.5. Existing locked packages are neither removed nor upgraded.
Arrow's adapter declares Apache-2.0 and Rust 1.85; the PyO3 family declares
MIT OR Apache-2.0 and Rust 1.83. The workspace's Rust 1.96 floor remains unchanged.
`target-lexicon` has no declared Rust floor; its actual compilation is part of
the workspace MSRV check.

The target parser declares Apache-2.0 WITH LLVM-exception. Its cached license
contains Apache-2.0 and the [LLVM exception](https://spdx.org/licenses/LLVM-exception.html),
which grants additional permissions. The dependency allowlist records that exact
expression. This intake adds Python/columnar interoperability and build-target
support, with no external execution engine.

The `arrow-pyarrow` crate archive omits root license/notice files while its source
headers refer to the Arrow project notice. The exact-version upstream
[license](https://github.com/apache/arrow-rs/blob/58.3.0/LICENSE.txt) and
[notice](https://github.com/apache/arrow-rs/blob/58.3.0/NOTICE.txt) match the files
in the already cached Arrow 58.3.0 sibling crates. Binary packaging must carry
these notices plus the selected dependency license texts; a component-only SBOM
does not supply those texts. Wheel staging, load proof, license bundle verification
and CI acceptance remain required before declaring this binding distributable.

## Per-call resources with retained results

The embedded adapter uses an opt-in serialized operation controller over one
resident Vortex session. Its memory pool can admit a different explicit allocation
between complete calls without replacing the pool, allocator, runtime or retained
array owners. Fixed-budget and concurrent-serving sessions keep their existing
behavior. Configurable sessions have no persistent background CPU drivers;
native operations use the current admitted execution-lane ceiling.

The controller intersects each call with the session's administrator ceilings,
keeps per-field origins, and holds configuration across preparation, execution and
the requested output boundary. Increasing an authorized grant does not grant an
independent pool. Lowering it below live retained ownership fails before input
demand. A call override never replaces the configured inheritance source for later
calls. The last admitted allocation remains with retained owners until the next
explicit operation; lifetime peak reservations may exceed a later reduced limit.
Native Python plan, input, result and stream operations must all use this controller.
Admission waiting checks cancellation. External caller memory and whole-process
RSS remain outside the reservation counter.
Recursive calls on the same controller fail explicitly instead of waiting on
their own admission lock. Importing a result owned by that controller reuses its
native result owner directly. A generic producer callback must not recursively
enter another operation on the same controller.
Nested callbacks may enter an available different controller, but cannot wait
for a busy one while holding their original grant. This prevents crossed imports
from deadlocking two sessions. The thread-local nesting counter controls lock
ordering only; it carries no data or resource allocation.

## Incremental delivery and explicit Python objects

The binding's incremental operation keeps one shared prepared plan and runs the
existing native batch consumer. Its worker starts on first demand, queues at most
one converted Arrow batch, and waits for the next demand before advancing. The
execution report becomes available only after final source validation and worker
completion. Closing or dropping the stream cancels its child token and joins the
worker; session cancellation also reaches the child without a stream close
cancelling unrelated session work. Retained batches remain provisional and valid
after close, but do not establish a completed transaction.

The allocation remains held during consumer waits. An attempted same-session
operation receives an actionable refusal instead of waiting for the consumer
that initiated it. A completed batch's direct Arrow export needs no new native
operation. Plan-cache release also refuses while the plan is executing.

Completed result exports retain a separate one-shot stream/report owner. Report
fields distinguish native shared bytes, representation capacity, conversion-work
reservations, canonicalization calls, demand counts and completion. These are
referenced-region observations, not unique physical bytes, measured decoding or
process RSS. Requested-schema casts are refused before capsule ownership moves.

Python row construction is a separate explicit endpoint. It reads admitted
columnar batches directly, preserves exact integers/binary/decimal/nested values,
and follows the existing temporal rule: represent calendar-compatible values as
Python date/datetime objects, retaining exact epoch integers beyond Python's
calendar range. Caller-owned Python containers are outside native reservations.
Neither Arrow export nor continued native execution invokes this endpoint.
Compilation, integration tests and packaging remain required before acceptance.

## Lifetime, conversion and evidence

Results must retain arrays, runtime and credits after session/plan handles drop.
Explicit close revokes new work without invalidating independently retained
results. Cancellation, interrupted iteration and downstream exceptions must drain
admitted work, stop input demand and release the exact owners. Sharing external
buffers must retain the producer and account honestly; copy mutable/incompatible
input when required. Never borrow merely because an object exposes a capsule.

Encoded Vortex layouts may require decoding/materialization for Arrow. Share
compatible buffers when sound and convert required representations once at the
chosen boundary. Record copies, decoding and retained memory separately. Ordinary
Python dictionaries require conversion. One boundary crossing per operation or
batch is the target; avoid per-row native calls and Python callbacks in Rust work.

Acceptance must cover current shared operator/type capabilities, empty/nullable/
nested and endpoint values, slicing/aliasing, released/malformed producers, retained
results, session close, mutation, cancellation, resource refusal and all writer
fidelity contracts. Measure complete Python workflows separately from native
Full43: large columnar intake/output, retained chained results, many short calls,
wide plans and native-file queries with small output. Keep setup, native execution,
conversion and full-return timing distinct and retain paired raw measurements.
Do not transfer historical tiny-worker latency or current Full43 timings into an
unmeasured binding speedup claim. No publication/version bump is authorized by
this implementation request.

## Explicit adjacent follow-up

Track full-domain microsecond timestamp compression and safe statistics through
the pinned writer/provider boundary under existing ingest/pruning owners. Preserve
the complete Int64 domain; do not trade correctness for statistics. Current typed
input omits global file min/max when timestamps are present and preserves timestamp
storage uncompressed because the pinned provider's calendar validation is narrower.
Restore useful compression and field-appropriate statistics only with endpoint,
pruning, complete-value, size/CPU and end-to-end evidence. This item remains
separate from Python boundary performance.
