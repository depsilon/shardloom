<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Variant field expressions

Status: source and installed-runtime verification passed; final acceptance pending,
November 10, 2026. This is the required
field-expression work within [item 19](native-rich-data-lifecycle-2026-10-24.md).
Useful spatial support and resource-admitted deep traversal remain separate
required work. Item 19, item 20 and CG-1 through CG-23 remain open.

The accepted base is UUID merge `8df722749e3993bb27a9cdd573de8a2a3860bb34`.
Its 58 PR checks and 55 exact-main checks passed with review clear, followed by
verified retirement and source-bound replay. The preparation head and merge
have identical source trees; adoption preserved the implementation, working
diff and index. Final documentation checks and the portable evidence packet
must bind this closeout to the unchanged tested runtime.

## User contract

The shared native binder admits three pure functions through SQL, typed plans
and DataFrame declarations:

- `variant_get(value, path)` returns a nullable Variant, retaining the selected
  value's type, including arrays, objects, binary and UUID payloads. Missing
  paths and traversal through the wrong value kind return SQL NULL. A present
  Variant null remains distinct from SQL NULL.
- `try_variant_get(value, path, type)` returns the explicitly requested nullable
  type. Missing paths, Variant null and failed conversions to concrete targets return SQL NULL.
  The type must be an admitted literal primitive, decimal, temporal, UUID or
  Variant declaration. A Variant target has the same value/null semantics as
  `variant_get`. This does not enable ordinary casts from Variant or UUID.
- `is_variant_null(value)` returns nonnullable Boolean: true only for a present
  Variant null, false for SQL NULL and other present values.

Paths are literal strings beginning with `$`, followed by `.identifier`,
nonnegative zero-based `[index]`, or a JSON-quoted `["field name"]`. Quoted names
preserve case, Unicode, punctuation and JSON escapes. A path has at most 24
steps and 16,384 UTF-8 bytes. Wildcards, recursive descent, filters, negative
indexes, malformed syntax, dynamic controls and unsupported target declarations
fail during binding, before a source producer runs. Existing payload depth 24
and storage-wrapper depth 64 limits remain in force.

## Vortex-first provider check

Decision: `use_vortex_native_provider`. The pinned Vortex 0.93.0
`VariantGet`, `VariantGetOptions`, `VariantPath` and `ParquetVariant` provider
already own extraction and format conversion. Use them under the existing
`vortex-local-primitives` / `upstream-vortex` boundary in `shardloom-vortex`.
No dependency update or external query engine is needed. The primitive/reference
row evaluator stays explicitly unsupported for this native Variant family.

Source review found two representation constraints. Core Variant scalar
extraction produces temporary constant Variant scalars that the current payload
and persistence owners do not admit. Also, the pinned Parquet provider's scalar
accessor renders an embedded UUID as text. Consequently extraction uses its
encoded Parquet Variant path, preserving raw UUID domains. Existing unshredded
native Parquet Variant storage goes directly to that provider. Shredded storage,
including canonical shredded arrays, is normalized through the provider's format
bridge before extraction. The pinned provider otherwise uses different casts
for its shredded and raw paths; normalization makes the raw Variant conversion
rules apply independently of physical storage. For example, text `"6"` remains
text and returns NULL for an integer target. String-to-decimal conversion is
explicitly admitted by the pinned provider; numeric width conversion can round
floating values, and overflow or incompatible domains return NULL. Concrete
floating outputs retain the session's strict/extended observation policy.
Temporal conversions preserve the target's unit and timezone metadata. The
pinned conversion pairs are narrower than general temporal casts: nanosecond
Variant timestamps return NULL for a microsecond target even when aligned;
microsecond Variant timestamps can produce nanoseconds. Second/millisecond
targets reject nonzero discarded precision. Duration targets are not admitted.
This is an explicit bounded conversion of Variant storage, not a JSON or
row-map execution substrate.

The pinned provider internally exposes its encoded metadata/value/shredded
children through Arrow and uses `parquet-variant-compute` 59.3.2 to select and
convert the requested field. Arrow here is a provider-internal format boundary;
it is not a separate query executor. Materialization is admitted and reported,
and no zero-decode, zero-copy or performance claim is made. The native execution
certificate records actual provider rows/batches and normalization boundaries,
with decoder-byte volume explicitly unmeasured. Residual operators use the
existing ShardLoom-native kernels. `fallback_attempted=false` and
`external_engine_invoked=false` remain required.

## Ownership and verification

Selected input chunks pass the existing complete Variant validation and credited
payload-copy boundary before provider access. A checked workspace reservation
covers provider metadata, buffers, conversion and overlap. Its physical-tree
walk charges every selected buffer and each schema/row occurrence. A separate
borrowed walk charges the expanded values of raw object aliases: eight offsets
to one large binary value must cover eight materialized copies. The selected
input's byte length alone is not an allocation bound. The reservation uses
128 bytes per selected/expanded content byte, 8,192 bytes per dynamic node,
schema-per-row allowances and explicit buffer-handle/path scratch before the
pinned format provider runs. These conservative allowances cover the finite
normalization/extraction/detachment pipeline; allocator bookkeeping and total
process RSS are not measured or bounded by this claim. Bounded chunks give
cancellation checkpoints. Every returned output is detached into the existing
native owner before temporary provider storage and workspace credits are
released. Root Variant extraction preserves the original native value. Empty
results retain their bound type. All-SQL-NULL chunks are answered from validated
native validity, avoiding the pinned provider's untyped Arrow NullArray result.
Failed admission or cancellation publishes
no completed result and releases partial owners.

The expert comparator is a columnar-engine maintainer checking format fidelity,
SQL null semantics, source laziness, grants and surviving output lifetimes.
Frozen independent fixtures must cover raw and shredded objects/lists, quoted
paths, exact integer/float/UUID/binary values, empty/all-null inputs, selections,
dictionary/slice wrappers, malformed bytes, denied growth and cancellation.
Verify SQL/DataFrame parity, predicates, typed joins/aggregates/order, retained
results, actual spill, native/IPC/Parquet writes and separate-process reopening.
Default/native workspace gates, relevant feature/MSRV checks, generated docs,
fresh installed-wheel tests and portable source-bound evidence are required.
Preserve every failed observation. No benchmark or publication is included.

The current release runtime passes all 25 selected public/catalog/schema tests,
including ten new public integrations, and all 441 isolated installed-wheel
tests with zero skips. The new cases include SQL/DataFrame and typed/embedded/CLI write routes,
fresh-process reopen after source removal, real spill/cancellation, computed
joins/grouping/ranking, retained children, raw/shredded parity, numeric widths,
temporal conversion pairs and memory denial. Complete default/native workspace
checks pass 3,323/7,065 tests respectively, with 37 native opt-in ignores;
formatting, strict lint, seven feature configurations and two supported-Rust
configurations pass. The broader Python check reports 868 runs from 1,301
declared cases, with 63 optional/environment skip records and no failures/errors.
Final documentation, portable source binding and hosted acceptance remain
required. The original parser, fixture,
all-null and free-space failures remain recorded. The free-space stop executed
no tests; verified retirement preserved accepted/comparison runtimes, current
inputs and release compiler output, without changing storage limits.

## Semantic references

The literal path/type and tolerant conversion interface is grounded in
[Databricks try_variant_get](https://docs.databricks.com/aws/en/sql/language-manual/functions/try_variant_get).
The distinction between SQL NULL and an embedded null follows
[Spark's documented is_variant_null semantics](https://spark.apache.org/docs/4.2.1/api/sql/variant-functions/).
These are interface references only; neither system executes ShardLoom work.
The pinned local Vortex source and finite fixtures determine actual admitted
conversion behavior. General JSON parsing, Variant keys and mutation functions
remain outside this unit.
