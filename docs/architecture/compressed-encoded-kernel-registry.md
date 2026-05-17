# Compressed Encoded Kernel Registry

## Purpose

This document is the architecture reference for the scoped `GAR-PERF-2D` compressed/encoded kernel
registry evidence now emitted by selective-filter prepared/native rows. It defines the current
encoding/operator registry surface over Vortex array facts and ShardLoom-owned encoded kernel
admission paths.

The purpose is to turn scoped encoded-predicate evidence into an explicit registry of admitted,
blocked, and unsupported encoding/operator pairs. The registry is not itself an encoded-native
operator claim.

## Current State

Selective-filter prepared/native rows now emit a `compressed_kernel_registry_*` aggregate contract
beside the `encoded_predicate_provider_*` fields. When the scoped filter-column probe observes real
reader chunks such as `flag:fastlanes.bitpacked` and `value:vortex.sequence`, ShardLoom lowers those
filter-column inputs, intersects their selection vectors, and records the bitpacked and sequence
pairs as admitted/executed reader-generated filter inputs. Selected metric aggregation remains
residual-native and does not become an encoded-native claim.

The registry is intentionally narrow. Encoded-native operator coverage is not broad. Dictionary
group-by, constant array count/filter, sorted min/max pruning, and FSST/dictionary string equality
are visible deterministic blockers or not-available rows until future slices add correctness,
materialization/decode, and certificate evidence.

## Initial Registry Rows

`GAR-PERF-2D` classifies these initial encoding/operator pairs:

```text
bitpacked boolean/integer filter
sequence equality/range predicate
dictionary equality/group-by
constant array count/filter
sorted min/max range pruning
FSST/dictionary string equality if available
```

Each pair should be classified independently. Support for one encoding/operator pair must not imply
support for another pair, another DType, another nullability shape, another layout, or another
scenario family.

Current selective-filter non-empty fixtures classify the first two rows as scoped
reader-generated encoded filter inputs:

```text
bitpacked_boolean_integer_filter -> executed_selection_vector_filter_input
sequence_equality_range_predicate -> executed_selection_vector_range_input
```

Empty-selection fixtures still emit the registry, but admitted/executed counts remain zero because
no selected row path consumes an encoded kernel input for metric evidence.

## Vortex-First Provider Check

- Subject area: compressed/encoded kernel registry for Vortex array encodings.
- Upstream Vortex concepts checked: DType, Array, Encoding, Layout, Statistics, Validity,
  dictionary/constant/sorted/sequence/FSST-like encodings, and compressed-array execution/provider
  surfaces.
- Decision: wrap Vortex encoding/layout facts in ShardLoom registry evidence and admit scoped
  reader-generated inputs only when ShardLoom can provide materialization/decode, no-fallback, and
  claim-gate evidence. Broader encoded-native promotion still requires future correctness and
  certificate evidence.
- ShardLoom surface: benchmark rows, capability matrix rows, compute-flow docs, and future
  execution certificate refs.
- Residual handling: unsupported encodings are deterministic blockers or residual-native paths, not
  external-engine fallback.
- Materialization/decode boundary: every registry row must state whether canonicalization, decode,
  or materialization is required.

## Required Evidence Contract

Benchmark rows expose an aggregate registry contract:

```text
compressed_kernel_registry_schema_version
compressed_kernel_registry_report_id
compressed_kernel_registry_scope
compressed_kernel_registry_current_surface
compressed_kernel_registry_vortex_first_decision
compressed_kernel_registry_initial_pair_count
compressed_kernel_registry_pairs_classified
compressed_kernel_registry_pair_ids
compressed_kernel_registry_pair_statuses
compressed_kernel_registry_encoding_ids
compressed_kernel_registry_logical_dtypes
compressed_kernel_registry_physical_encodings
compressed_kernel_registry_operator_families
compressed_kernel_registry_kernel_admitted
compressed_kernel_registry_kernel_executed
compressed_kernel_registry_canonicalization_required
compressed_kernel_registry_decoded
compressed_kernel_registry_materialized
compressed_kernel_registry_selection_vector_emitted
compressed_kernel_registry_validity_semantics
compressed_kernel_registry_unsupported_kernel_reasons
compressed_kernel_registry_encoded_native_claim_allowed
compressed_kernel_registry_admitted_pair_count
compressed_kernel_registry_executed_pair_count
compressed_kernel_registry_blocked_pair_count
compressed_kernel_registry_not_available_pair_count
compressed_kernel_registry_claim_gate_status
compressed_kernel_registry_claim_boundary
compressed_kernel_registry_fallback_attempted
compressed_kernel_registry_external_engine_invoked
```

The row-level vocabulary inside those aggregate fields includes:

```text
encoding_id
logical_dtype
physical_encoding
operator_family
kernel_admitted
kernel_executed
canonicalization_required
decoded
materialized
selection_vector_emitted
validity_semantics
unsupported_kernel_reason
encoded_native_claim_allowed
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

If the row is blocked, it should still be visible with:

```text
kernel_admitted=false
kernel_executed=false
unsupported_kernel_reason=<stable reason>
encoded_native_claim_allowed=false
claim_gate_status=not_claim_grade
```

## Claim Boundary

The registry may show that a kernel was admitted or executed for a scoped local fixture. It does not
permit an encoded-native operator claim until the full path proves:

- supported DType and encoding.
- null/validity semantics.
- selection-vector behavior.
- no unwanted canonicalization.
- no decode or materialization beyond the declared boundary.
- correctness against a decoded reference.
- benchmark/evidence rows.
- execution and Native I/O certificate refs where applicable.
- `fallback_attempted=false`.
- `external_engine_invoked=false`.

Current rows keep:

```text
compressed_kernel_registry_encoded_native_claim_allowed=false
compressed_kernel_registry_claim_gate_status=not_claim_grade
compressed_kernel_registry_fallback_attempted=false
compressed_kernel_registry_external_engine_invoked=false
```

## Non-Goals

- No broad SQL/DataFrame runtime.
- No broad encoded-native operator coverage claim.
- No external query-engine fallback.
- No object-store/lakehouse runtime.
- No production or performance/superiority claim.
- No automatic promotion from registry admission to claim-grade evidence.

## Acceptance

- Initial encoding/operator pairs are represented as admitted/executed, blocked, unsupported, or not
  available with deterministic reasons.
- Unsupported encodings block deterministically.
- `encoded_native_claim_allowed=false` remains the default until end-to-end evidence passes.
- Rows distinguish canonicalization, decode, materialization, and selection-vector behavior.
- Benchmark evidence and capability matrix rows can render registry posture without prose.

## Verification Plan

Current verification includes:

```text
cargo test -p shardloom-vortex selective_filter_lowers_observed_bitpacked_and_sequence_filter_columns --features vortex-traditional-analytics-benchmark
cargo test -p shardloom-vortex selective_filter_selection_vector_metric_aggregation_handles_empty_selection --features vortex-traditional-analytics-benchmark
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
python -m compileall -q benchmarks/traditional_analytics
cargo fmt --all -- --check
```

Broader follow-up verification for future kernel expansion should include:

```text
unit tests per new encoding/operator pair
null/all-null/high-cardinality cases where relevant
decoded-reference correctness comparison before encoded-native promotion
selective filter and group-by benchmark smoke
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python -m compileall -q benchmarks/traditional_analytics
python scripts/check_website_readiness.py
git diff --check
```

Planning-only updates should run release-readiness metadata and website readiness checks.
