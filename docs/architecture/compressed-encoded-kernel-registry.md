# Compressed Encoded Kernel Registry

## Purpose

This document is the architecture reference for the scoped `GAR-PERF-2D` compressed/encoded kernel
registry evidence now emitted by prepared/native rows. It defines the current
encoding/operator registry surface over Vortex array facts and ShardLoom-owned encoded kernel
admission paths.

The purpose is to turn scoped encoded-predicate evidence into an explicit registry of admitted,
blocked, and unsupported encoding/operator pairs. The registry is not itself an encoded-native
operator claim.

## Current State

Prepared/native rows now emit a `compressed_kernel_registry_*` aggregate contract beside the
`encoded_predicate_provider_*` fields. When the scoped filter-column probe observes real reader
chunks such as `flag:fastlanes.bitpacked`, `value:vortex.sequence`, or `flag:vortex.constant`,
ShardLoom lowers those inputs through the reader-generated encoded-kernel path, executes the
selection-vector kernel, and records decoded-reference correctness digest evidence. Selected metric
aggregation remains residual-native and does not become an encoded-native claim.

The scoped group-by fixture also exercises a real prepared Vortex reader chunk whose `group_key`
column is stored as `vortex.dict`. ShardLoom lowers the reader-generated dictionary codes/values,
compares encoded group counts with the decoded reference group counts, and records the dictionary
group-by pair as executed without adding a standalone side lane.

The registry is intentionally narrow. Encoded-native operator coverage is not broad. Sorted
min/max pruning, FSST/dictionary string equality, sparse traversal, TurboQuant/vector kernels, and
generalized operator/function coverage remain deterministic blockers or future candidates until
correctness, materialization/decode, certificate, and claim-gate evidence land.

`runtime.5g-f1` now projects the registry into the current runtime plan and capability surfaces
instead of treating encoded-kernel work as a side lane. The current physical plan marks scan,
filter, project, limit, count aggregate, aggregate, join, top-k, sort, and window as supported
runtime families, while repartition and write remain explicit blockers. Operator and function
coverage reports separate encoded-capable, native-decoded/residual, planned-native, partial, and
unsupported families so the CLI, compute matrix, benchmark suite, and website status can surface the
same support boundary.

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

Current fixtures classify these rows as scoped reader-generated encoded inputs:

```text
bitpacked_boolean_integer_filter -> executed_selection_vector_filter_input
sequence_equality_range_predicate -> executed_selection_vector_range_input
constant_array_count_filter -> executed_constant_array_count_filter_input when Vortex emits a constant filter input
dictionary_equality_group_by -> executed_dictionary_equality_group_by_input when Vortex emits a vortex.dict group key
```

Empty-selection fixtures still emit the registry, but admitted/executed counts remain zero because
no selected row path consumes a bitpacked filter input for metric evidence. They may still execute
sequence or constant filter pairs when those real reader-generated inputs are present.

## Vortex-First Provider Check

- Subject area: compressed/encoded kernel registry for Vortex array encodings.
- Upstream Vortex concepts checked: DType, Array, Encoding, Layout, Statistics, Validity,
  dictionary/constant/sorted/sequence/FSST-like encodings, and compressed-array execution/provider
  surfaces.
- Decision: execute initial ShardLoom-owned kernel pairs over reader-generated Vortex encoded
  batches when decoded-reference correctness, materialization/decode, no-fallback, and claim-gate
  evidence can be emitted. Broader encoded-native promotion still requires future correctness and
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
compressed_kernel_registry_input_rows
compressed_kernel_registry_decoded_reference_compared
compressed_kernel_registry_correctness_digest_status
compressed_kernel_registry_correctness_digests
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
input_rows
decoded_reference_compared
correctness_digest_status
correctness_digest
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
- per-pair input row counts and correctness digests.
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
- Executed rows expose input-row counts, decoded-reference comparison status, and correctness
  digests.
- Benchmark evidence and capability matrix rows can render registry posture without prose.

## Verification Plan

Current verification includes:

```text
cargo test -p shardloom-vortex selective_filter_lowers_observed_bitpacked_and_sequence_filter_columns --features vortex-traditional-analytics-benchmark
cargo test -p shardloom-vortex selective_filter_selection_vector_metric_aggregation_handles_empty_selection --features vortex-traditional-analytics-benchmark
cargo test -p shardloom-vortex dictionary_group_by_pair_executes_from_prepared_vortex_reader_chunk --features vortex-traditional-analytics-benchmark
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
