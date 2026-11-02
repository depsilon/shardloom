<!-- SPDX-License-Identifier: Apache-2.0 -->

# Preparation and query investigations before the documentation refresh

Status: requested November 2. Complete the investigations and evidence-backed
dispositions after current items 18/23 integration and applicable local-format
acceptance, before the README/documentation/field-guide/website refresh. Complete
that refresh before the 1.1.0 version bump. The
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

The first proposed implementation batch is A/B/C, after source-grounded contracts
and targeted controls establish each mechanism. D/E follow only after their
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
CG-1 through CG-23 remain independent. The separately requested
[source/preparation-strategy investigation](native-source-strategy-investigation-2026-11-02.md)
remains after the version bump and before item 19.
