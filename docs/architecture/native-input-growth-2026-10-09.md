# Native input growth under shared admission

Status: complete local engine, documentation and independent packet acceptance;
hosted integration is pending. This work belongs to the
existing PERF-03/06/07/10/11/12 checklists. The version remains 0.5.1; this unit
does not close the remaining local-workflow scope or authorize a release.

The contract replaces cumulative input, top-level field-count and generated
range limits with admission of the actual native owners. Per-frame row/byte
limits and bounded small-result collection remain explicit boundaries.
These are source changes after the 0.5.1 release. Published package artifacts
retain their release-time limits.

## Admission contract

| Boundary | Current source contract |
| --- | --- |
| Copied, owned and batch input fields | Nonempty distinct names; growing native schema metadata uses the shared grant |
| Resident batch composition | Retained payloads and growing containers use the shared grant; no fixed cumulative batch count |
| Finite streaming input | One finite producer used once; release each native input before requesting the next; checked counters and explicit end-of-input |
| Python input frames | At most 2,048 rows and 8 MiB; schema declarations retain their separate 8 MiB envelope |
| Generated Int64 range | Compact source metadata plus admitted scan intervals; checked endpoints, total length and logical bytes |
| Native relational binding | Each operator, expression and field acquires metadata credit; cumulative node counts have no separate fixed cap |
| Results | Top-level field metadata uses the shared grant; complete JSON collection retains its 65,536-row and 8 MiB bounds |
| Nested value schemas | Existing depth 24, 4,096-node, 1,024-field nested-struct and 8 MiB metadata checks apply within each value below the top-level record |

Metadata uses conservative allowances, while owned buffers retain their actual
capacity credits. These are admission reservations, not measurements of every
process allocation. Recursive plan/expression depth, SQL frontend limits,
operator-specific pivot/key/argument limits, four-domain public intake, repeated
or multiple producers, and streaming destination composition remain separate
implementation obligations.

## Native provider and ownership decisions

- Resident batch composition uses the pinned Vortex 0.85.0 `ChunkedArray`.
  Growable containers reserve replacement overlap before allocation; empty
  batches validate schema without retaining an ever-growing list of empty arrays.
  Payload and composition metadata retain their leases through native aliases.
- Generated Int64 input uses Vortex `Sequence` for private compact metadata.
  The scanner constructs only the requested bounded interval in the session's
  host allocator. It does not call the uncredited general sequence decoder.
  Signed endpoints, intermediate multiplication and byte counts remain checked.
- Top-level schemas reserve field containers, name copies and uniqueness indexes
  before construction. Owned intake wraps only its reviewed primitive, Boolean,
  UTF8 and slice encodings with safe typed Vortex reconstruction, preserving
  payload pointers and their original capacity owners.
  Copy boundaries distinguish a whole row record from an individual nested
  value column. Retaining a one-row struct value uses its bounded nested
  metadata allowance; it does not repeat table-schema container admission.
  Both paths keep the schema lease attached to every copied child buffer.
  Native footer admission includes schema serialization workspace before writer
  creation, and incremental leaf credit preserves that width-dependent base.
- Incoming frame buffers, JSON workspace, decoded cells and typed conversion
  hold leases from the same query pool. Their overlap observer serializes pool
  transitions with counter changes. Control declarations retain their separate
  bounded envelope; a query grant is not a process-RSS ceiling.
- Native binding already reserves each operator and expression before constructing
  it. Total node counts can therefore grow under that existing metadata lease.
  The separate recursive depth guards remain until the recursive parser, binder,
  execution, clone and destruction paths have an explicit traversal contract.

These decisions use Vortex native array providers and ShardLoom's existing
resource policy and execution certificates. No query-engine integration,
external residual evaluator or fallback engine is introduced.

## Acceptance contract

Verify exact types, field order and all values through ordinary Python/CLI
declarations, execution, collection, bounded iteration, writing and reopening.
Cover resident and completion-aware input beyond the former batch count;
wide schemas across native and compatibility destinations; compact ranges beyond
the former total-length limit; and native plans beyond former total-node counts.
Verify resource denial, retained aliases, typed empty input, late producer or
expression failure, slow consumption, cancellation and owned output cleanup.
Run applicable existing operator/public/regression gates before integration.

The first public growth packet passed 20 cases, including 1,025 fields, 4,099
nonempty and 8,193 empty batches, and every value of a 1,000,017-row range.
The existing input/protocol packet passed 37 cases. Those were intermediate
checks on the working implementation, not final broad acceptance or performance
claims. Richer intake, multiple/repeated producers, dynamic input, compatibility
streaming/fanout, deeper traversal and the other existing scope owners remain
implementation work.

Subsequent native checks exercise 4,097 flat projected fields and all eight
destinations. That case exposed a footer reservation which did not grow with
the schema; its failed observation is retained, and the corrected writer now
admits schema serialization workspace before construction. Wide source aliases
and primitive-root Vortex inputs also passed their focused writer checks. At that
intermediate checkpoint, broad regression and final public acceptance were pending.

The first broad public regression then exposed table-schema admission being
charged for each retained nested pivot value. The 65,537-row nested pivot
exhausted its unchanged 1 GiB grant before reaching the collection row limit.
That failed candidate is retained. The correction separates native record and
value copy admission and requires fresh nested-state, wide-record and complete
public acceptance; the grant and expected collection denial stay unchanged.

The native regression suite then exposed a related spill estimate: sorting
counted retained payload and keys but omitted each compact record's admitted
schema metadata. Many small records could exhaust the grant before reaching
the spill threshold. Sorting now includes that metadata when deciding to spill.
A 1,025-batch reproducer fails before the correction and passes afterward under
the same 8 MiB grant, with exact ordered values, producer release and cleanup.
The original 133,137-row DISTINCT/grouped aggregation pressure test also passes
with its unchanged 16 MiB grant, complete result comparison and spill cleanup.
Both failed observations remain retained. All fifteen corrected source gates
pass, including 2,513 native library tests with 24 ignored tests.

Final corrected acceptance uses clean runtime `61a813b7` and 1,020 frozen assets.
All eleven workflow stages run freshly: 20 growth cases, 442 retained streaming
cases, five input-pressure controls, 32,497 ordinary public cases with
18,595,284 complete row comparisons, direct/adaptor/semantic regressions and
all 129 Full43 results pass. The wide public fixture has 4,097 fields; cumulative
controls exceed 4,096 batches; every value of the 1,000,017-row range is checked.
Streaming completes 4,851,019,008 logical input bytes under a 1-GiB grant, while
resident intake denies that grant and completes under 6 GiB. Every producer closes.
The [acceptance report](../benchmarks/native-input-growth-2026-10-09.md) binds
the independently inspected packet, unchanged oracles and grants, all retained
failures, and exact source/executable identities. Hosted integration remains
pending. This finite unit does not close the broader implementation obligations
above or authorize another version bump.
