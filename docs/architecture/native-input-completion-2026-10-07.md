# Completion-aware native batch input

Status: local engine acceptance and independent packet inspection pass for
corrected runtime `b7de216fe421012d3f9b632ad19e95f19b88dbe5`.
The FSST integration correction preserves the input contract and passes all 39
hosted engine checks. Refreshed documentation, native examples and browser checks
pass. [PR #1530](https://github.com/depsilon/shardloom/pull/1530) merged at
`c16f8da7` after all 39 checks passed; the
[hosted receipt](../benchmarks/evidence/native-fsst-hosted-2026-10-07.json) records
preview/production verification and unchanged accepted runtime assets. This is the
`NATIVE-INPUT-COMPLETION` unit under PERF-03/07/11/12 and CG-5/19/20/21.
It follows the accepted builder runtime `53cd1582` and the two dropped
conditional-work experiments recorded at `0117ab9f`. Published v0.4.0 is unchanged.

The [corrected acceptance report](../benchmarks/native-fsst-admission-2026-10-07.md)
records complete output from 4.5 GiB of UTF8 input under a 1 GiB native grant,
all 40 new public cases, ten native ownership tests, five pressure/control cases,
the existing complete regression portfolio and all 129 Full43 results. This
removes a finite resident-input barrier; it is not a speedup or process-RSS claim.
The [original report](../benchmarks/native-input-completion-2026-10-07.md) remains
historical evidence for `f14460ef`; its packet is not rewritten by the correction.

## Decision

Explicit streaming input uses the existing native relational execution and
batch transport. Python `from_batches(..., streaming=True)` requests it;
`streaming=False` preserves the documented resident mode. The first admitted
plan is one declared source used once, with only pure row-local scans, filters
and projections. It may deliver incremental results, collect an already bounded
small result, or write one native Vortex destination. It cannot silently retain
the whole input when that plan shape is unsupported.

Keep the existing finite intake bounds: 1–128 declared nullable scalar fields,
2,048 rows per batch, an 8-MiB control frame and at most 4,096 payload batches.
The native typed intake retains its 32-MiB logical-byte bound per batch. Total
input may exceed the query grant, but this does not authorize an unbounded
producer, unrestricted output, new spill, or a whole-process memory ceiling.

The external values are copied at the existing typed compatibility boundary.
After intake, filtering, projection, bounded output ownership and persistence
remain Vortex-native. SQL and DataFrame declarations lower through the existing
binder; there is no Python query loop, second query invocation or external engine.

## Source-grounded boundaries

These are the pre-implementation boundaries that motivated the design. The
accepted implementation preserves resident intake and adds the separately
admitted single-batch path described below.

- `resident_memory_batches.rs` currently accumulates `ArrayRef` values until
  `finish`. Preserve that implementation for resident input.
- `python_batch_protocol.rs::Transport::build_source` currently pulls the whole
  producer during source registration. Streaming registration must instead build
  an empty typed schema owner and make no transport demand.
- `local_primitive_relational_bind.rs::Binder` already owns schema, metadata and
  exact bound expressions. Register a distinct schema-only batch source there;
  classify the complete lowered plan before binding any data-dependent operator.
- `local_primitive_relational_scan.rs` and `run_transform` already pass native
  batches synchronously through the same filter/project implementations. Supply
  one current batch to that scan inside the existing admitted execution.
- `select_batch` can create lazy native takes that retain original buffers.
  Merely dropping a source handle does not prove input release. Streaming output
  must become independently owned before crossing the consumer boundary.
- `local_primitive_relational_writer.rs` already supplies one producer to the
  native sink. `NativeSinkPlan::write_produced` accounts for growing output
  metadata, drains a healthy writer after producer failure, validates the complete
  file and only then commits. Reuse this lifecycle.

## Vortex-first provider decision

Decision: `wrap_vortex_concept`. The pinned Vortex 0.85.0 `ArrayIterator` and
`ArrayStream` represent dtype-bearing native batches. `ArrayIterator::read_all`
collects all chunks, and adapter dtype assertions do not establish release-build
validation or ShardLoom resource ownership. Neither is a query planner or a
source-completion certificate.

Use native `DType`, arrays, bound expressions, the existing native allocator and
native sink. The input callback additionally requires a session-owned typed
batch and propagates ShardLoom errors without losing denial/cancellation identity.
It is a local ownership/completion wrapper around native array delivery, not a
second array representation. No dependency, query-engine integration or global
source registry is added. `fallback_attempted=false` remains explicit.

## Binding and public execution

`VortexRelationalPreparation::register_batch_source` receives a native empty
schema owner built through the new bounded single-batch intake constructor.
It rejects nonempty schema owners, foreign sessions, duplicate URIs and invalid
schemas. It retains only that small schema owner; no producer is opened.

The binder recognizes a batch scan independently from resident-memory scans.
Before any producer call, require exactly one batch source, no additional source,
and a lowered chain containing only Scan/Filter/Project. Reject joins, repeated
source references, sets, sorting, limits/offsets, aggregates, windows, unary
stateful operators, correlated work and data-dependent schema binding. Existing
expression binding still decides which pure scalar operations and types are
supported. Source pruning must not erase the obligation to observe end-of-input.
The SQL parser's internal unlimited token retains its synthetic origin; a user
written `LIMIT`, including the largest representable limit, remains a prefix
operation and is rejected. The schema owner's zero rows are not an execution
estimate: the native plan exposes the finite 4,096 × 2,048 input-row bound.

`PreparedVortexRelational::with_batch_input` creates one borrowed execution
adapter around the prepared plan and a callback returning
`Result<Option<ResidentMemorySource>>` from the same resident session. The adapter
is consumed by its execution method; it owns neither a reusable input cache nor
a hidden replay factory. It shares native/JSON batch consumption, small JSONL
collection and the single native writer with ordinary prepared execution.
Calling an ordinary prepared execution method without the required provider
fails explicitly. Dynamic preparation rejects batch declarations before payload.

The callback may construct a private typed native batch under the supplied
session. It must not reacquire execution admission or execute another query.
`None` is its explicit end-of-input event. Python keeps the existing `Rows`,
`End`, result `batch`, `Ack` and `Cancel` framing; matching source/sequence and
bounded frame checks remain mandatory. A factory may supply a fresh producer
for a later operation; a one-shot iterable is still opened only once.

## Lifetime and completion proof

The execution moves through declared, admitted, consuming and source-complete
states. Only observed `None` or the matching transport `End` establishes source
completion. An empty batch, empty filtered output, a full batch, cancellation or
a temporary absence of records cannot establish it.

For each admitted batch:

1. Reserve transport conversion scratch before demanding payload. Construct
   native buffers under the existing session allocator, with one structural
   metadata lease shared by every input buffer and the source owner.
2. Validate exact dtype, session ownership, per-batch bounds and cumulative
   batch/row/byte counters in release builds before evaluating any row.
3. Run the already-bound row-local native chain once on that current batch.
   Preserve batch and row order and all existing NULL/value/error rules.
4. Compact emitted native output through the existing native payload owner before
   handing it to an external consumer or queued writer. This explicit copy is
   charged and reported; it prevents lazy output selections from pinning input.
5. Release the current source and all operator temporaries. Check a weak reference
   to the input's shared metadata lease before requesting another batch. A live
   input alias is a deterministic error, not permission to retain more batches.

The final lease check turns the single-current-input claim into an ownership
invariant, including buffer clones and slices. A caller-provided native batch
must be private to this transfer; retaining its input aliases is unsupported.
Retained output has its own reservations and may still exhaust the shared grant.
The empty-schema owner is constant plan metadata, outside the current-input count.
The CLI uses `ResidentVortexSession::reserve_input_scratch` to reserve its existing
64-MiB conversion envelope before sending demand. This narrow reservation method
does not start another operation or expose query-admission internals.

One native operation and one final report cover all input batches. A source with
no rows still has a declared schema and produces the existing typed empty result.
Success requires observed input completion, finished native consumers, source
validation, cancellation checks and all result acknowledgements. Earlier results
remain provisional. A late malformed value, wrong dtype, producer exception or
missing `End` prevents a successful report.

## Output and failure boundaries

First admit one native Vortex destination for streamed input. Refuse multiple
destinations and compatibility file exports before pulling the producer; their
replay and buffer contracts require separate proof. Normal resident output
capabilities remain explicit and unchanged.

The writer may create only its owned staging file before source completion.
Its producer must complete exactly once, observe end-of-input and deliver all
rows before finish/validation/commit. Late failure drains healthy accepted work
without publishing it; cancellation, sink failure and quota denial preserve the
existing cleanup contract and original diagnostic. The published-but-unlink-failed
case remains distinct from pre-publication failure.

Native output metadata grows with actual chunks and stays separately reserved.
Input release does not remove footer or result-buffer costs. Output compaction
can raise CPU/copy cost and peak overlap; measure these costs instead of claiming
zero-copy or speedup. The existing strict acknowledgement remains the result
backpressure policy. The byte-credit output-window candidate is separate.

## Evidence and diagnostics

Add a successful-execution input report containing payload batch/row counts,
cumulative logical bytes and intake payload copies, largest batch rows,
maximum retained input batches and logical bytes, and observed end-of-input.
State that logical bytes are not allocated capacity or RSS. Report detached
output ownership and the shared grant separately from sink metadata/output
reservations. Derive CLI fields from the completed native report, not counters
sampled before execution. Certificates must identify the declared native batch
source and observed completion rather than calling it a fully resident source.

Use the existing native-batch diagnostic family for unsupported streaming plans,
missing/foreign input providers, sequence/schema drift, live input aliases and
finite intake limits. Give the unsupported operator and actionable explicit
resident-mode alternative where appropriate. Explain, estimate and discovery
must never open the Python producer or send an input demand.

## Acceptance and remaining work

The following local acceptance requirements now pass, with raw values, failure
traces, resource reports and separate packet inspection linked from the report.
Documentation, browser checks and hosted integration also pass. The
[remaining-scope contract](native-local-completion-scope-2026-10-07.md) preserves
the separate stateful, streaming-adapter and operational obligations.

Focused tests must cover complete exact values/types/order, empty and all-filtered
inputs, empty batches, NULLs, Unicode, signed limits, native output reopening,
retained output clones, private-input release, foreign/shared input refusal,
late input errors, consumer failures, cancellation and writer cleanup. Assert
that unsupported joins/repeated sources, limits, aggregates, dynamic schemas,
multiple destinations and unsupported file formats consume zero producer items.

Freeze a public workflow with cumulative input at least four times its native
grant, complete independent output, at most one retained native input batch,
successful final completion and complete owner release. Include a native write,
incremental results, slow/failed consumers and late failure after provisional
output. Keep matched resident and ample-memory controls, report time to first
provisional result separately from complete delivery, and retain every failure.
This is a capability/resource acceptance gate, not a speedup experiment.

Focused Rust/Python checks, the repository's required workspace gates, the
existing public/native/format regression families, Full43 correctness and
adversarial ownership/publication review pass. Hosted integration remains
required before closing this unit. Research selective rematerialization only
after finding a genuinely retained derived owner; current predicate truth words
remain transient. Other state/structure and conditional-work candidates retain
their independent evidence prerequisites and all CG owners remain visible.
