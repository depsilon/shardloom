<!-- SPDX-License-Identifier: Apache-2.0 -->

# Variant payload preservation through native workflows

Status: implementation, finite native-runtime and installed-package verification
complete, November 9, 2026. Hosted integration and package publication are
separate. This is the Variant fidelity obligation within
[item 19](native-rich-data-lifecycle-2026-10-24.md). Deep nested traversal,
standalone UUID/spatial support and Variant field expressions remain separate
required work; this unit does not close item 19 or CG-1 through CG-23.

## Provider and ownership contract

Classification: `use_vortex_native_provider`. Vortex 0.93.0 already supplies
`DType::Variant`, the canonical `VariantArray`, the `vortex.parquet.variant`
encoding, native selection kernels, file serialization and Arrow import/export.
Use those types behind the existing `upstream-vortex` feature gate. The default
build remains free of these optional dependencies. Parquet Variant 59.3.2 is a
format parser/validator, not a query executor. These Apache-2.0 providers are
isolated in `shardloom-vortex`; no query-engine integration is admitted.

Variant persistence uses the existing frozen `core/2026/08/3` edition. The
provider initializer also registers the separate `json/2026/08/0` dtype edition;
this does not admit JSON extension payloads or JSON field expressions through
ShardLoom. Shared payload-schema checks remain authoritative, and the writer
continues to reject the opt-in draft buffer encoding.

The logical dtype and outer validity must survive selection, joins, bounded
delivery, native spill, native write and fresh-process reopen. SQL NULL and a
non-null Variant whose value is null are distinct. Preserve raw metadata/value
bytes and supplied shredded children for selected values through native ownership,
including dictionaries, nonzero slices, duplicate matches and missing outer-join
rows. Empty outputs retain the logical Variant dtype and may use the provider's
default empty physical representation. Do not turn carried values into JSON
strings or decoded Arrow rows inside relational execution. Copy only selected
physical payloads into credited native owners when detachment is required.

Variant's physical schema is value-dependent. Its metadata, value and optional
typed tree need their own checked schema/buffer admission in addition to the
one-node logical dtype. Malformed bytes, conflicting shredded values, denied
growth and cancellation must fail before unsafe provider access or publication,
and release partially constructed owners. An unsupported physical subtype must
produce an explicit diagnostic, never silent type erasure. The existing depth
boundary remains until provider serialization, traversal and destruction are
covered together by the separate deep-nesting unit.

Arrow and Parquet use the standard `arrow.parquet.variant` field metadata.
Explicit export may reconstruct an unshredded representation when the requested
output schema requires it; that conversion must be accounted and preserve logical
values. Raw native persistence must retain the native representation. JSON is a
terminal presentation boundary and cannot substitute for typed round-trip proof.
Variant comparison, grouping, ordering and field expressions are not enabled by
payload admission. Existing execution and I/O evidence must continue to report
`fallback_attempted=false` and `external_engine_invoked=false`.

## Current user boundary

Use the native Python transport and `context.from_arrow_table` with the canonical
`arrow.parquet.variant` field metadata, or read an admitted native Vortex,
Arrow IPC or Parquet file. `Result.result_schema` names the logical type
`variant`. `Result.to_arrow()` preserves the extension identity and returns its
standard physical `metadata`/`value` binary children. Python row objects expose
those encoded children, with SQL NULL represented as `None`; they do not turn
Variant objects into ordinary Python dictionaries. An explicitly converted
shredded value is reconstructed into the standard encoded representation.
`context.from_result` reuses a completed native owner. Ordinary query writers
and native `Plan.write_many` use the shared native write/replay boundary.

This unit admits raw Parquet Variant values (including nested arrays/objects,
UUID values, floating representations and binary), supplied shredded scalar,
object and list trees, and supported canonical Variant storage. Physical
shredding has a narrower contract than raw values: unsigned integer, UUID,
fixed-size-list, empty-struct and nested canonical-Variant shredded leaves are
not admitted. Raw/typed object names must be present in the metadata dictionary;
overlapping fields and multiple levels of canonical shredding fail explicitly.
Non-null native Constant Variant values require encoded Parquet Variant storage;
ordinary file compression/reopen of repeated admitted values remains covered.

The existing logical and shredded depth limit is 24; storage wrappers are
bounded at 64. This is payload transport through filter/join/selection/order on
other keys, bounded delivery, spill and typed persistence. Variant keys, field
expressions, row-literal schema hints, general JSON conversion and other
compatibility writers are outside this acceptance. Existing mixed-MELT diagnostic
rows retain their rendered Python scalar values. This presentation does not
reconstruct Variant encodings: structured or encoded Variant values, and non-null
Variant Arrow conversion, require native Arrow export.

## Finite verification contract

Use small independent fixtures containing SQL null, Variant null, booleans,
signed integer limits, distinct floating representations, binary and text,
empty/nonempty arrays and objects, and mixed row types. Include unshredded,
fully shredded and partially shredded object/list storage; compare raw bytes
where the representation is preserved and complete typed values at explicit
conversion boundaries. Test dictionary import with extension metadata and
nested Variant fields. Invalid metadata, invalid value bytes, overlapping raw
and typed object fields, invalid child shapes, nullability violations and
unsupported operations are negative controls.

Ordinary SQL and DataFrame paths must cover filter, duplicate and outer join,
streaming delivery, typed empty output, all native write interfaces, separate-
process reopen and Arrow IPC/Parquet export. A pressure case must show positive
native spill activation and complete values after merge and cleanup. Small
resource grants, cancellation, context drop, independently retained results and
final exported-child release require explicit ownership assertions.

Acceptance also requires focused tests, default/native workspace formatting,
strict lint and tests, relevant provider/feature/MSRV checks, a fresh installed-
wheel proof and portable source-bound evidence. Original failures remain
observations. No Full43 or performance comparison is authorized by this unit.

## Local verification and evidence

Default and native workspace validation pass 3,322 and 7,045 tests respectively;
the native run retains 37 existing opt-in ignores. Formatting, strict lint,
seven feature configurations and both Rust 1.96 checks pass. The changed Arrow
provider passes 408 tests and strict lint. All 19 required native Variant,
legacy-MELT/pivot, Arrow-ownership and writer-edition regressions pass.

The fresh runtime passes all 15 selected Python tests with zero skips. Its
isolated installed wheel passes all 420 integration tests with zero failures,
errors or skips, followed by isolated uninstall verification. The wheel contains
the final Python source and exactly the frozen native library and CLI.
The [portable packet](../release/evidence/native-variant-lifecycle-2026-11-09/README.md)
records final Python core, documentation and contract checks, complete source
inventories, exact commands, runtime identities, original failures and saved
corruption-control outcomes. Replay does not repeat native execution.

The native retry used four test workers with the complete unfiltered suite,
unchanged fixtures, deadlines and storage limits after an earlier free-space
refusal. This is a test-supervision change, not an engine-performance result.
All stopped or failed observations remain distinct from completed acceptance.
The negative self-join fixture retains its original parsing failure and exact
two-`AS`-token correction; values, grants and refusal assertions are unchanged.
Generated API references retain their original stale-source failures and exact
canonical-format/source-identity refresh. Final consumers validate those bytes.

This completes the scoped Variant payload-preservation obligation. It does not
admit Variant comparison or field expressions, replace the depth boundary,
complete standalone UUID/spatial support, close item 19 or change CG-1 through
CG-23. No benchmark or package publication is included.

## Source grounding

- Pinned `vortex-parquet-variant` 0.93.0: `lib.rs`, `array.rs`, `vtable.rs`,
  `arrow.rs` and `kernel.rs` define native registration, physical slots,
  shredding, persistence and conversion.
- Pinned `parquet-variant` 59.3.2 `Variant::try_new` validates metadata and the
  complete encoded value. `parquet-variant-compute::VariantArray::try_value`
  explicitly does not deeply validate and can panic on conflicting typed/raw
  non-object values; it is not an intake validator.
- Existing ShardLoom `native_payload`, `native_payload_schema`,
  `native_arrow_conversion`, `native_json` and input/result schema owners are
  the integration points. No second semi-structured store is introduced.
