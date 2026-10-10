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
