<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact count from an admitted direct source

Status: implementation contract frozen on October 31 against `a1d66dbb`.
The later October 31 maintainer instruction folds this implementation into the
remaining item 18 batch, with one combined build and acceptance pass after
implementation. Existing focused and broad-check attempts remain evidence;
the standalone count build/package/timing sequence below is superseded before
any count performance observation. Freeze the combined I/O acceptance protocol
before measuring that batch, retaining the original 347-case declarations and
accepted baseline history.
The preceding B6/B9 composition remains in PR #1633 with its own integration
and retirement gates. This unit starts the source-capability audit required by
the [universal-I/O queue](native-universal-io-performance-coverage-2026-10-24.md#contract-and-intake),
within the existing RFC 0044 program and RFCs 0012/0014/0015/0031.
It does not close item 20, item 18, adaptive-execution item 23 or CG-1–CG-23.

## Existing providers and the missing connection

The fully bound relational tree already selects specialized native-file
providers when it has one `PreparedVortexSource` and no memory/batch sources.
Ordinary fresh Parquet queries instead use the admitted demand reader and the
same native relational operators. That distinction is a source-capability
boundary, not a separate execution engine.

| Strategy | Required proof | Direct-source disposition in this unit |
| --- | --- | --- |
| `COUNT(*)` | Exact source cardinality, one held generation, complete inventory, checked accumulation and metadata validation | Extend the existing input lifecycle with an optional exact count capability; Parquet is the first direct adapter. |
| Filtered count | Predicate semantics, validity, selected rows and complete required predicate evaluation | Keep the current native batch evaluation. Footer cardinality does not prove a filtered count. |
| Aggregate | Group/measure semantics; exact null/extrema facts or native update/merge/finalize inputs | Preserve existing shared batch kernels. Native-file footer/encoded strategies retain their stronger prerequisites. |
| Projection | Requested/hidden columns, predicate dependencies, source order, validation and buffer ownership | Existing Parquet projection, selected chunks, prefixes and native ownership remain. |
| Ordering | Exact typed comparisons/ties, bounded demand, generation-bound positions and selected payload ownership | Existing shared ordering and admitted two-phase reads remain. A file-provider guard is not removed. |

Source anchors are `local_primitive_relational_provider{,_plan}.rs`,
`local_primitive_prepared_relational.rs`, `local_primitive_footer_aggregate.rs`,
`local_primitive_relational_batch_input.rs`, `public_relational_sources.rs` and
`universal_format_io/demanded{,/source}.rs`. The combined item-18 batch extends
direct demand to the other admitted local adapters through the same native
batch-input contract. Only the admitted Parquet footer path supplies this exact
metadata-count capability; the other adapters still consume their required
source data. Explicit full preparation retains complete-source validation.

## Admission and semantics

- Use an already bound scalar aggregate containing only unfiltered `COUNT(*)`
  measures, including aliases/repeated measures. Require an unfiltered direct
  scan, allowing only intervening direct-column projections. Computed
  projections, filters, ranges, joins, grouping sets and other operators retain
  their existing evaluation, including errors and effects.
- The owned source must have one static use, no replay/prefill requirement,
  no nested-owner retention and no deferred full-validation obligation.
  External callbacks cannot advertise metadata by supplying a schema or row hint.
- The source descriptor must represent all admitted rows with no prefix or
  excluded groups. For each held Parquet file, require nonnegative footer and
  row-group counts, checked group accumulation, and equality of both totals.
  Sum files with checked `u64` arithmetic. No floating conversion or narrowing
  through a platform-sized row count is permitted.
- Validate held file generations and directory membership before completion
  and after the final consumer through the existing owner. Mutation poisons
  reuse. Cancellation, resource denial, malformed metadata and publication
  failure terminate the query; no preparation retry or external execution occurs.
- A metadata-only count does not validate data pages, values or null statistics.
  Corrupt undemanded payload is outside that query's validation scope. Explicit
  full-source preparation/validation and `COUNT(column)` retain their contracts.
  Missing/nullable column counts are not inferred from `COUNT(*)` evidence.
- Build the scalar through the existing admitted native aggregate state/output
  owner. It must remain independent of input ownership and writable to admitted
  native/compatibility destinations, including when query spill is configured.

## Evidence and acceptance

Report actual metadata-count source/row totals separately from decoded rows,
payload batches and scan work. Count completion reports zero data reads/decode
only because the source reader is never opened; metadata/fingerprint admission
reads remain separate nonzero facts where applicable. Add the selected strategy
and exact validation scope to the existing native I/O certificate/public fields.

Required controls cover empty/nonempty/null data, multiple files and measures,
aliases/direct-column wrappers, large exact cardinality and overflow, malformed
footer totals, corrupt undemanded pages, generation/membership changes before
and after output, reuse, cancellation, memory denial/refund, spill configuration,
and negative admissions for filters, column counts, mixed reducers, computed
expressions, repeated sources and caller producers. Public proof uses the same
five interfaces and independent complete write/readback. Preserve existing
native/default/installed-package requirements at the coherent unit boundary.

Work-removal counters are not a timing claim. Before timing, freeze the focused
direct-source/neighbor controls and compare one candidate execution of every
unchanged 347 case with the saved accepted `11.978234339`-second engine result.
Use the maintainer's complete-cohort gain/loss rule and preserve every outcome.
No Full43 or new competitor observation is authorized by this unit; the existing
Full43 hold remains until I/O acceptance.

## Vortex-first provider check

Decision: `implement_shardloom_kernel`, extending existing native aggregate and
input-lifecycle owners with adapter-provided metadata, rather than defining a
new source abstraction or delegating execution. Vortex 0.90.0 already exposes
native-file row counts and native scalar/array output; its file metadata does
not describe a direct Parquet inventory. The pinned Parquet 59.3.1 metadata
provider exposes signed file and row-group cardinalities in
`vendor/parquet/src/file/metadata/mod.rs`. The format deliberately separates
metadata from column data ([Apache Parquet file format](https://parquet.apache.org/docs/file-format/)).
Reuse these provider facts with ShardLoom identity, resource and certificate
policy; no upstream API/dependency change or query-engine integration is needed.
No row pages are decoded for admitted counts. Native Vortex output remains the
highest-fidelity persistence boundary. `fallback_attempted=false` and
`external_engine_invoked=false` remain mandatory.
