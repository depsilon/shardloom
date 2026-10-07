# Native Builder Resource Ownership

Status: implementation and local/hosted acceptance complete at `53cd1582`, with
independent packet inspection passed. PR #1529 merged at `0f7609da` after all
39 hosted checks passed, preserving all 941 accepted runtime assets. This is
`NATIVE-BUILDER-RESOURCES`, under PERF-03/06/07 and CG-5/19/20/21. The
[acceptance report](../benchmarks/native-builder-resources-2026-10-07.md) records
the measured overhead, failures and complete regression proof. Published v0.4.0
is unchanged. This finite unit does not complete those broader owners.

## Decision and reuse

Extend the existing `native_provider_memory` session boundary to admit the
output buffers of native `Chunked` execution for primitive numbers, Boolean
values and decimals. Use the pinned Vortex builder and each child's existing
`append_to_builder` implementation. Do not pre-decode every child or substitute
a gather loop: native BitPacked append, constant runs and selected decoder
strategies must retain their existing execution behavior.

The same review corrects a small lifetime gap in the accepted string builder:
`finish` replaces its transferred value/view buffer with an empty `BufferMut`.
That replacement still requests the preferred alignment capacity. Its temporary
credit must survive until the builder drops, independently of the returned
buffer's credit. Historical acceptance packets remain unchanged; they did not
identify this finalization allocation.

The executable numeric-Zstd concatenation test also exposes a pinned provider
gap: `Zstd::append_to_builder` only dispatches variable-binary builders, although
Zstd canonical execution supports primitive numbers. Extend that existing
vendored provider method to use its native primitive decoder and then native
primitive append. Its already-admitted codec payload/workspaces overlap the
builder's output, then release when the leaf append completes. Preserve typed
denial, selected frame decoding and raw codec errors. This adds no second codec
or consumer-specific execution path.

Vortex-first provider check: `use_vortex_native_provider`. The providers are
Vortex 0.85.0 `Chunked`, `ArrayBuilder`, `PrimitiveBuilder`, `BoolBuilder`,
`DecimalBuilder`, `VarBinViewBuilder`, `Validity` and `BufferMut`. The existing
`vortex-local-primitives` feature, session `ProviderMemory` marker, PulseWeave
pool and retained-buffer hooks remain the ownership boundary. Pinned
`builder_with_capacity_in` ignores its allocator, so passing an allocator alone
does not enforce this contract. No new allocator, dependency, unsafe code or
query-engine integration is needed.

This is shared provider behavior below aggregate, join, ordering, nested-value
and result consumers. SQL, Python, DataFrame and CLI wrappers receive the same
native execution. Their existing no-fallback certificates and materialization
boundaries are unchanged; no new front-door execution route is introduced.

## Capacity and lifetime contract

- Reserve before constructing the builder or executing any child. The data
  capacity is row count times primitive width, ceiling(row count / 8) for Bool,
  or row count times the pinned decimal builder's precision-selected physical
  width. Include preferred alignment slack and checked size arithmetic.
- Reserve a possible nullable bitmap at ceiling(row count / 8), with its own
  alignment slack. Release that credit if finishing produces all-valid or
  all-invalid validity without a retained bitmap.
- Reserve the empty replacement buffer used by `finish`, then drop the builder
  before releasing that temporary credit. Preserve this overlap for numeric,
  Boolean, decimal and the existing string concatenation provider.
- Nested Chunked inputs append into the same builder through Vortex's native
  append implementation. Preserve source order, exact dtype/scale/nullability,
  selected values and the native decimal storage-width decision.
- Attach value and validity credits to their respective native byte buffers.
  A surviving value buffer, validity child, clone or slice keeps its own full
  allocation credit after the result and session are dropped. Wrapping retained
  ownership must not copy the produced bytes.
- Preserve the original typed allocation-denial and decoder error. On an error,
  all partially built output and temporary reservations must drop. Do not retry
  through a different provider or external engine.

## Deliberate boundary

This unit owns the concatenation builder's value/view byte buffers, nullable
bitmap and empty value-buffer replacement during finalization.
Child codecs, encoded validity, selection-mask caches, source storage and other
provider scratch retain their existing ownership contracts and exclusions.
Calling a child's native append implementation does not certify all of its
allocations. In particular, general numeric/Bool/decimal decoder scratch is a
separate review boundary: sliced extents, copy-on-write and selective BitPacked
decoding cannot be replaced with a row-count-only reservation or unconditional
child canonicalization.

Canonical arrays that bypass Chunked execution retain their existing owners.
Single-step `execute::<ArrayRef>` reduces zero/one chunks before dispatch;
typed/canonical execution can still use a builder for one chunk. An empty
Chunked array has no append operation and keeps its existing native empty path.
Other dtypes and sessions without the installed resource marker retain their
existing behavior. This changes neither their support nor their resource claim.
Array/builder headers and VarBinView's per-source-buffer handle collections
(`Vec`/`Arc` metadata) are not covered by these byte-buffer reservations. Their
capacity is a separate structural-metadata boundary. Allocator metadata, stack
storage, whole-process RSS, general operator spill
and resumable recovery remain outside this finite contract.

## Acceptance

The following contract was fixed before timing. Local acceptance now passes
14 new ownership tests, all 17 source gates, 27,373 public cases, 202 direct
cases, 48 batch checks, 19 format checks and all 129 Full43 calls. The
840,960-call lifecycle screen passes its predefined revision rule while retaining
five repeated relative UTF-8 regressions, including a confirmation only 0.0703
microseconds below the five-microsecond threshold. See the report for exact
scope and all samples; this is a resource correction, not a speedup.

Before implementation, reproduce denied-grant bypass for fixed-width builders
and the uncharged string finalization overlap. Prove all primitive widths, Bool
bit offsets, decimal storage widths/scales, nested chunks, empty/all-null input,
and exact encoded values against independent fixtures. Prove value/validity
lifetimes separately, absence of an extra buffer copy, typed denial before child
execution, late decoder failure cleanup and overlapping live outputs.

A frozen native lifecycle cost screen compares unchanged native concatenation
with the admitted session for complete outputs, including encoded and nullable
inputs. A cell exceeding a 10% median time ratio triggers a fresh reversed-order
repeat. A repeated regression exceeding both 10% and five microseconds per
complete call requires revising the implementation. The absolute floor separates
fixed ownership overhead on tiny concatenations from material provider cost;
retain and report the smaller differences as well. Freeze all cohort sizes and
both thresholds before timing. Retain every sample and source/binary identity.
This is a resource-correction screen, not a speedup claim. Run focused ownership
tests, required workspace gates, complete
public/native workflow checks and Full43 regression acceptance after the
cohesive implementation is stable. Keep local acceptance, hosted integration
and package publication distinct.

PulseWeave owns actual overlapping buffer lifetimes. Native append strategies
preserve work avoidance; no new capillary scheduling or adaptive strategy is
introduced. Report provider measurements separately from whole-query timing,
and keep evidence limited to this allocation class.
