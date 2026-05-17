# Compressed Encoded Kernel Registry

## Purpose

This document is the report-only architecture reference for `GAR-PERF-2D`. It defines the planned
compressed/encoded kernel registry for encoding-specific operator support over Vortex arrays and
ShardLoom-owned encoded kernel paths.

The purpose is to turn scoped encoded-predicate evidence into an explicit registry of admitted,
blocked, and unsupported encoding/operator pairs. The registry is not itself an encoded-native
operator claim.

## Current State

Selective-filter encoded-predicate provider evidence exists for scoped local paths. Current evidence
can identify real reader chunks such as `fastlanes.bitpacked` and `vortex.sequence`, lower admitted
filter-column inputs, intersect selection vectors, and keep the selected metric aggregation
residual-native.

Encoded-native operator coverage is not broad. Existing constant, dictionary, run-end, selective
predicate, and kernel-registry history provides useful foundations, but the repo still needs a
single planned registry that classifies encoding/operator pairs with stable evidence fields and
claim boundaries.

## Initial Registry Rows

`GAR-PERF-2D` should start with these encoding/operator pairs:

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

## Vortex-First Provider Check

- Subject area: compressed/encoded kernel registry for Vortex array encodings.
- Upstream Vortex concepts checked: DType, Array, Encoding, Layout, Statistics, Validity,
  dictionary/constant/sorted/sequence/FSST-like encodings, and compressed-array execution/provider
  surfaces.
- Decision: wrap Vortex encoding/layout facts in a ShardLoom registry and admit kernels only when
  ShardLoom can provide correctness, materialization/decode, no-fallback, and claim-gate evidence.
- ShardLoom surface: benchmark rows, capability matrix rows, compute-flow docs, and future
  execution certificate refs.
- Residual handling: unsupported encodings are deterministic blockers or residual-native paths, not
  external-engine fallback.
- Materialization/decode boundary: every registry row must state whether canonicalization, decode,
  or materialization is required.

## Required Evidence Contract

Every registry row should expose:

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

## Non-Goals

- No broad SQL/DataFrame runtime.
- No broad encoded-native operator coverage claim.
- No external query-engine fallback.
- No object-store/lakehouse runtime.
- No production or performance/superiority claim.
- No automatic promotion from registry admission to claim-grade evidence.

## Acceptance

- Initial encoding/operator pairs are represented as admitted, blocked, unsupported, or not
  available with deterministic reasons.
- Unsupported encodings block deterministically.
- `encoded_native_claim_allowed=false` remains the default until end-to-end evidence passes.
- Rows distinguish canonicalization, decode, materialization, and selection-vector behavior.
- Benchmark evidence and capability matrix rows can render registry posture without prose.

## Verification Plan

Future implementation should include:

```text
unit tests per encoding/operator pair
null/empty/all-null/high-cardinality cases where relevant
decoded-reference correctness comparison
selective filter benchmark smoke
group-by benchmark smoke
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python -m compileall -q benchmarks/traditional_analytics
python scripts/check_website_readiness.py
git diff --check
```

Planning-only updates should run release-readiness metadata and website readiness checks.
