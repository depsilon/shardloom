<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native workflow result streaming

Status: implementation in progress under PERF-03/06/07/11/12. The
[phase plan](phased-execution-plan.md) owns sequencing; this document records
the design and finite acceptance contract for its first workflow completion unit.

## Contract

Complete already executable flat-scalar aggregate and ordered results through
one native array boundary. Local writes must consume bounded batches without
constructing a complete JSON/scalar result table or running the query again.
Small in-memory collection keeps its independent row and byte bounds. Source
generation validation, schema, validity, complete values, ordering, cancellation,
spill ownership and atomic publication must survive the producer/consumer handoff.

The reference comparator is an encoded-columnar engine maintainer reviewing
buffer lifetimes, global aggregate selection, precise null/type semantics and
failure before publication. A larger fixture passing is insufficient if retained
output still grows without reservation or a sink silently publishes a prefix.

## Provider decision

Vortex-first provider check: `use_vortex_native_provider`. Reuse pinned Vortex
0.85 `ArrayRef`, `StructArray`, primitive/Boolean/UTF8 arrays, native buffer
ownership, native scan, and the existing sequential Vortex writer. Compatibility
conversion remains at the existing output adapter. No dependency changes or
external execution providers are required.

Inspection of Vortex 0.85 `builders/mod.rs:451-457` shows that
`builder_with_capacity_in` currently discards its allocator argument. Consequently
completed scalar columns use the existing ShardLoom reserved host allocator and
the native array constructors directly. Payload and validity leases attach to
each buffer, so clones and slices retain credit after the producing result and
session are dropped. This is explicit final-result construction, not a new array
representation or a claim that arbitrary provider allocations are accounted.

Retain the existing aggregate state machines and their exact global selection:
finalized integer/UTF8 DISTINCT, numeric COUNT, numeric-pair compact/late measures,
numeric-minute-string COUNT, numeric-UTF8 COUNT, UTF8 COUNT/DISTINCT refinement,
transformed dictionary measures, generic ordered groups and source-order groups.
Do not replace complete-key reduction with local top-K selection. Preserve the
existing floating accumulation/evaluation order and signed/unsigned key identity.

Shared result construction accepts complete typed values directly from those
states. Report rendering is a terminal consumer, rather than an intermediate
execution format. Bounded synchronous handoff supplies backpressure: a producer
cannot advance while its consumer retains the active operation. Later async
queues would require their own admitted capacity and cancellation contract.

Completed DISTINCT and weighted COUNT owners already reserve their exact global
selection. Their final handoff borrows at most 2,048 selected keys/counts at once,
with a separate reservation for that reference window. It does not duplicate the
whole selected result before producing batches. Generic aggregate ordering still
reserves its complete candidate selection separately; this does not add spill
support or complete resource accounting to those state machines.

## Acceptance contract

- Complete values, dtype, validity and order for scalar/grouped/ordered empty and
  nonempty results; repeated calls and independent source generations.
- More than 65,536 output rows, more than 8 MiB overall output, multiple batches,
  and batch/resource boundary failures. Collection limits remain independently
  tested. A single oversized value must fail deterministically before publication.
- Native Vortex and admitted Parquet, Arrow IPC, Avro, ORC, JSON, JSONL and CSV
  sinks, including format-specific UInt64/loss checks and existing-file safety.
- Native aggregate spill and numeric-sort spill output, cancellation, corrupt
  run/pressure failure and cleanup, with no query replay or leaked reservations.
- Downstream native consumption, drop/clone/slice lifetimes, slow-consumer and
  consumer-error behavior. Buffer accounting exclusions remain explicit.
- Public CLI, SQL and Python/DataFrame calls share the same native handlers;
  full source/transform/write/reopen checks use renamed non-ClickBench schemas.

Focused tests precede the required format, strict Clippy and workspace/native
gates. Full43 remains regression evidence. No timing improvement, complete
relational breadth, total RSS ceiling, competitive gate completion, production
certification or package publication follows from this unit alone.

Every accepted execution retains `fallback_attempted=false` and
`external_engine_invoked=false`. Real native payload verification is required;
placeholder artifact paths do not satisfy native output acceptance.
