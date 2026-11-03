<!-- SPDX-License-Identifier: Apache-2.0 -->

# Preparation and query investigations before the documentation refresh

Status: November 2 pre-measurement investigation and cleanup complete. A/B/C are
implemented and locally validated; their release-build creation/publication/first
Full43 acceptance remains pending. D/E have explicit no-change dispositions.
Complete workflow acceptance after items 18/23 integration and applicable
local-format acceptance, before the README/documentation/field-guide/website
refresh. Complete that refresh before the 1.1.0 version bump. The
[phase plan](phased-execution-plan.md#planned) owns sequencing. These packages
extend the existing preparation, source, representation and metadata owners;
they introduce no new phase ID or competitive gate.

The maintainer supplied the opportunities below from a review of main and PR
#1643 at `70faa310`. Reground each mechanism before implementation. The accepted
[fresh preparation and prepared-query evidence](../benchmarks/evidence/native-adaptive-prepared-full43-2026-11-02.json)
uses runtime `f0bf6d46`: creation 36.945, publication 6.695 and Full43
40.592222712 engine seconds, totaling 84.232222712. These stage totals do not
attribute time to checksum reads, indexing, helper evaluation or code-frequency
reconstruction. No speedup is forecast from them.

## Priority and work packages

### A. Artifact finalization: reuse committed checksum proof

- [ ] Trace the existing workspace-safe streaming SHA-256, byte count, completed
  writer validation and commit through `vortex_ingest.rs` publication and
  `LocalReuseFileFingerprint::from_path` / `sha256_file_digest`.
- [ ] Design a private committed-artifact receipt carrying digest, byte count,
  committed identity, writer/source validation and the ownership/lifecycle proof
  needed to establish the same artifact at immediate publication. A caller's
  digest string or unchecked constructor is insufficient.
- [ ] Admit reuse only for a proven fresh same-transaction handoff. Preserve
  content fingerprinting for independently supplied artifacts, later publication,
  changed/replaced files and missing proof. Preserve cancellation, mutation,
  commit and publication-race controls.
- [ ] Measure payload bytes reread solely to reconstruct the writer's checksum;
  the admitted fresh handoff should reach zero. Keep separately required
  independent storage readback. A writer checksum proves accepted write bytes,
  not an independent readback of persisted storage.
- [ ] Include shared admitted footer/dtype/embedded-binding/generation ownership
  where it fits the same lifecycle. Account for already deferred large-file
  inventory work and identify later identity work outside the publication clock.

The accepted artifact is 15,732,915,012 bytes. A removable full checksum read is
a work-removal hypothesis with a concrete counter. The 6.695-second publication
span includes other work and is not a checksum-time measurement.

### B. Text compression preparation: sparse frame indexing

- [ ] Ground `collect_valid_vbv` and its `value_byte_indices` consumer in
  `vendor/vortex-zstd/src/array.rs`, the effective frame-width policy and the
  native writer's 8,192-value frame configuration.
- [ ] Determine effective frame width before packing. Return the unchanged
  length-prefixed valid-value payload, valid count and frame-start offsets only,
  eliminating the per-valid-string offset vector and second index construction.
- [ ] Preserve codec, compression level, dictionary policy, exact payload and
  frame boundaries, validity, logical types, empty/null behavior, overflow/error
  ordering and dependency provenance. Prove identical frames and metadata where
  the existing representation makes byte identity the correct contract.
- [ ] Count scratch bytes and offset entries, then measure complete creation,
  publication and queries against the newly written artifact under unchanged
  resource limits. Cumulative index writes are distinct from peak live memory.
- [ ] Evaluate frame-local payload packing only as a subsequent separately
  admitted extension. It changes allocation lifetimes and compression feeding;
  preserve error/cancellation behavior and acknowledge remaining payload copies.

### C. Derived-expression contract continuity

- [ ] Trace `embedded_derived_column_schema` and typed helper construction in
  `universal_format_io.rs` through persistence, embedded rewrite admission and
  `triple_count_workers::preserve_raw_minute_input`.
- [ ] Define producer/consumer proof for input nullability, null introduction,
  output type, overflow/error behavior, expression semantics and source identity.
  Begin with the demonstrated typed minute/minute-bucket helper contract.
- [ ] Give newly written helpers sound non-null declarations when admitted
  non-null inputs and transform semantics prove them. Do not infer this for
  dictionary values merely from non-null row codes.
- [ ] Consider existing artifacts only with exact generation-bound zero-null
  coverage over the accessed domain; absence or sampling cannot supply proof.
  Such admission must preserve the artifact's logical schema.
- [ ] Prove actual helper selection, lower raw timestamp reads/decodes and minute
  extraction, and continued specialized-worker admission. Verify complete values,
  temporal/null semantics, fallible expressions and source invalidation.

Q19 is an affected grouped-count target, not a measurement of removable minute
work. Broaden controls to other helpers and shared consumers without introducing
query-ID dispatch or weakening existing worker guards.

### D. Conditional shared-allocation ownership transfer

- [ ] Attribute current residual copied bytes by reason before selecting a
  mechanism: safe internal sharing, small slices pinning large allocations,
  alignment/conversion needs, and unknown/external ownership.
- [ ] Extend the existing controlled transfer boundary only when complete backing
  allocation identity/capacity, immutable aliasing and lifetime can be proved.
  Charge each allocation once through its complete retained lifetime; prove
  source-drop safety, cancellation/failure refunds and independent column release.
- [ ] Keep copying when a small slice would retain excessive unrelated storage,
  required alignment needs conversion or allocation provenance is unavailable.
  Retaining an entire wide batch for one column is not an acceptable substitute
  for per-allocation ownership.
- [ ] Treat `universal_format_io/owned_batch.rs` as the existing lifetime envelope,
  not proof of complete allocation capacity. Historical transfer gains do not
  quantify this runtime's remaining eligible copies.

Proceed only if observed eligible shared allocations justify the ownership
extension and complete-workflow controls pass.

### E. Conditional exact dictionary metadata

- [ ] Identify the physical dictionary domains and existing weighted consumers
  that reconstruct frequencies from codes/row IDs. Ground native count
  preparation, transformed dictionary partials and dictionary-preserving writes.
- [ ] Evaluate bounded exact frequency and active-value facts tied to the exact
  dictionary epoch and row domain. Charge creation, persistence, publication,
  reading, validation and ownership costs before claiming a benefit.
- [ ] Preserve exact global distinct merging across domains. Reject unfiltered
  frequencies for unrelated row predicates; single-column vectors do not prove
  compound-key distributions. Include first-occurrence facts where order matters.
  High cardinality may justify declining metadata construction or use.
- [ ] Prove activation and avoided code walks in real eligible consumers with
  complete results. Q34/Q35 are attribution targets; their total query time does
  not establish dictionary coverage or removable population work.

This is a conditional representation experiment under existing metadata-summary
and consumer owners. It requires evidence of first-use economics before retention.

## Shared acceptance and sequence

### November 2 implementation contract

The next coherent batch is A/B/C on the locally accepted table-reader source.
The expert comparator is a native storage/codec maintainer: proof must survive
ownership and persistence boundaries, and reduced work cannot change values,
frame bytes, observable errors or required independent readback.

Vortex-first provider check: B uses the existing Apache-2.0 Vortex 0.90.0 Zstd
provider and its admitted host allocator (`use_vortex_native_provider`), with a
local scratch-representation patch. C carries ShardLoom's existing typed helper
semantics through Vortex DType and embedded fields (`implement_shardloom_kernel`);
Vortex cannot infer the frontend transform contract. A belongs to ShardLoom's
workspace-safe commit and prepared-state publication boundary
(`implement_shardloom_kernel`), which Vortex's writer does not own. All three keep
the existing native writer/array consumers and explicit compatibility-input
boundary; no query-engine integration or external residual execution is added.

For B, resolve valid count and effective frame width from the existing validity
mask before packing. Validate every valid source view and total payload size in
the existing order, then allocate the unchanged payload plus exactly
`ceil(valid_count / effective_width)` native `usize` offsets. Emit one offset at
each frame start directly; do not build a per-value index or a second frame index.
Zero valid values retain empty buffers and effective width one. Preserve valid
value order, length prefixes, codec/dictionary settings, exact compressed frames
and metadata. Keep frame-local payload packing outside this patch. Controls must
compare against an independently constructed original representation and exercise
all-null/empty/sliced/binary inputs, width zero/one/oversized, partial final frames,
malformed buffers, allocation failure/refunds and retained ownership.

The existing writer stage report carries successful selected-text packing counts
from the provider's exact emitted frame metadata: valid values, length-prefixed
payload bytes, frame-start entries and their native `usize` byte extent. The
allocator controls independently verify that those are the requested payload and
index allocations. These are cumulative extents for completed calls, excluding
failed calls, other codec scratch, alignment overhead and peak live memory. No
per-value index savings are fabricated from array memory estimates. The report
survives stream/writer evidence combination without another row traversal.

For C, begin with direct nonnullable integer/timestamp fields handled by the
existing typed minute/minute-bucket builders. Successful evaluation cannot add
nulls; fallible bucket conversion remains an error. Nullable inputs, dictionary
domains and text-derived transforms remain conservatively nullable. Preserve
existing artifact schemas and the nullable-helper worker guard. Acceptance must
show the newly persisted helper's exact dtype, complete temporal results and
actual helper/worker selection, with generation/refusal controls. No sampled or
absent null statistic is promoted to proof.

A uses an opt-in companion write outcome, leaving existing cloneable reports
unchanged. Its opaque, noncloneable receipt retains the actual flushed staging
descriptor through the existing same-directory commit. On Unix, admission checks
the descriptor's device/inode, size and modification time across commit, then
binds its committed generation (including change time and link count) to the
published path. Publication consumes the receipt and checks that generation
before and after deriving its report. A newly opened pathname, caller-supplied
digest or report clone cannot construct the receipt. Changed, replaced, linked
or symlinked artifacts cannot use it. Existing independent/later publication
continues content fingerprinting; platforms without admitted generation identity
also retain content fingerprinting. This is the existing optimistic filesystem
generation contract, not exclusion of hostile concurrent filesystem writers or
an independent storage readback. Required writer reopen validation remains.

The pre-measurement cleanup makes that opt-in apply at the actual workspace-safe
write boundary. Report-only scalar, buffered-columnar and streaming APIs must
not construct a receipt and discard it after reopening. Immediate-publication
APIs retain the same descriptor proof; other writers use the existing validated
commit without the additional receipt lifecycle. Both modes share producer,
source, cancellation, resource and reopen checks. Controls compare complete
artifact bytes and exercise both receipt presence and absence; no elapsed-time
benefit is presumed from this cleanup.

The CLI's private immediate-write outcome carries the receipt separately from
its public report and consumes it exactly once during the existing publication
span. The report records the checksum source and actual payload bytes read for
fingerprinting. Shared footer/embedded-binding ownership is deferred: its
consumers have separate source-admission lifetimes, and the current inventory
already defers large-file work. The D/E source inventory finds
aggregate copy counters and transient dictionary counts, but no current per-reason
copy attribution or persisted domain-bound frequency proof; their conditional
dispositions are recorded below. No new ingest or Full43 run is started by this contract.

### D/E source and preserved-run findings

The current controlled Arrow handoff can prove complete allocation capacity only
after `Buffer::into_mutable` establishes a unique standard allocation. It reserves
that capacity before transfer and retains the reservation with the buffer owner.
The shared `OwnedBatch` envelope proves lifetime, not complete backing capacity.
The existing copy branch does not distinguish shared standard allocations from
offset, alignment, oversized-capacity or opaque-owner refusals.

The preserved November 2 preparation report records 30,974,099,552 copied region
bytes and 72,299,123,989 transferred region bytes. These are cumulative observed
work across admitted buffers, not peak memory or a measurement of avoidable
shared copies. The complete copy allocation charge is 30,985,042,016 bytes. No
per-reason breakdown in that report establishes which residual copies qualify
for the proposed shared-owner extension. The zero repeated-region counter has
the narrower scope of exact held pointer-and-length matches within one batch.

**D disposition: defer shared-allocation transfer.** The existing proof boundary
and validated copies remain. This investigation found a material aggregate but
did not establish eligible shared-copy volume or a complete-capacity proof for
shared owners. A future candidate needs both before changing ownership or credit
lifetimes; this is not a claim that all remaining copies are necessary.

The writer reports 992 dictionary-preservation calls across 197,135,986 rows,
but those are writer operations, not persisted per-value frequencies. Source
inspection confirms exact counts and active-value information are reconstructed
from row codes inside existing weighted consumers. The saved Q34/Q35 reports
both record zero native-dictionary worker chunks/values and no dictionary
accessor columns. Their physical-layout status is `not_inspected`; it does not
prove that the artifact lacks dictionaries. Dedicated frequency code-walk,
active-value and first-occurrence counters are absent, rather than measured zero.

**E disposition: defer persisted frequency metadata.** The observed target
reports do not establish eligible physical domains, consumer activation or
first-use economics. No frequency payload is added on the basis of total query
time. Further work must bind exact facts to dictionary epochs and row domains,
prove predicate and ordering compatibility, and include construction, publication
and first-query costs before retention.

These findings use the existing preparation and Full43 JSON observations. The
ingest report SHA-256 is
`3883977729d12b67f1e2db833ef32d6bd2e6562ec188ee40367d9f1058fde960`;
the Full43 summary SHA-256 is
`e46b4b61836ee58e1a73175e8d3b1b00a3383da65a97060793c801df3c949511`.
All 35 selected field values and source identities were checked against the
original JSON, without opening data payloads or executing another query. These
conditional no-change outcomes do not certify A/B/C performance.

### Pre-measurement local closeout

The final source passes workspace formatting, strict default and enabled
workspace Clippy, 3,318 default Rust tests and 6,875 enabled Rust tests. The
36 existing opt-in native ignores remain unchanged. The patched Zstd provider
passes its separate formatting/Clippy checks and all 96 library tests. The
generated API and public-status checks, bundled-license verification, seven
notice regression tests and fifteen API-reference regression tests also pass.
Original compilation, assertion and storage-guard failures remain recorded;
an interrupted earlier native run is not counted as a complete pass.

The source-strategy investigation completes its declared fourteen parent-runtime
executions with full results and explicit no-change dispositions. Before freezing
the final measurement build, completed default and native test caches are retired
with repeated file identities, closed-handle checks and regeneration evidence.
This includes the small default-profile cache recreated by the final native
staged-write smoke test. Current inputs, accepted output/runtime/wheel and all
compact observations remain; no payload backup or storage-limit increase occurs.

The remaining acceptance is the clean release build, installed-wheel proof and
one frozen fresh preparation/publication followed by one isolated Full43 pass.
The controls above prove the bounded contracts, not their complete-workflow
performance. No further source-strategy mechanism is selected for this batch.

The implemented batch is A/B/C, with source-grounded contracts and targeted
controls establishing each mechanism. D/E follow only after their
attribution and admission gates. Record a justified decline or no-change outcome
when evidence does not support implementation; do not presume all proposals ship.

Use existing resource, PulseWeave and capillary-work owners. Reduced scratch and
proved allocation ownership feed the existing adaptive decisions. Preserve caller
CPU/memory ceilings, native Vortex input/state/output, exact validation, first
observable errors, lazy domains, cancellation, cleanup and no external fallback.
Observe mechanism-specific counters in production evidence without per-row traces
or fabricated avoided work. Keep unknown attribution explicit.

Freeze a complete-workflow protocol before execution and evaluate
`native creation + publication + first Full43` while retaining each stage's engine
total. Query regressions on newly created artifacts are part of the tradeoff.
Preserve full result checks, matched resources and the isolated single-pass
procedure; do not add repeats, cache manipulation or benchmark-specific grants.
The queue entry itself does not start another ingestion/Full43 run. The active
decision-evidence 347-case test has separate maintainer authorization.

Pass applicable provider/ownership/semantic controls, default/native workspace
gates, public and installed interfaces and hosted review before accepting a
change. Update the subsequent documentation refresh from the final supported
results. Versioning and publication retain their existing approval boundaries;
CG-1 through CG-23 remain independent. The subsequently brought-forward
[source/preparation-strategy investigation](native-source-strategy-investigation-2026-11-02.md)
now joins this work before the next Full43 measurement. Complete its dispositions
and any justified code/generated-artifact cleanup before freezing the final
measurement build; current correctness checks continue. The combined documentation
refresh and version bump follow the final accepted evidence.
