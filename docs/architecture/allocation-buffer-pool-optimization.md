# Allocation And Buffer-Pool Optimization

Status: planned/report-only reference for `GAR-PERF-2G`.

## Summary

`GAR-PERF-2G` defines the allocation profiling and scoped buffer-reuse layer for prepared/native
ShardLoom runtime work. The goal is to make memory behavior visible before optimizing it: which
operators allocate, which buffers can be reused safely, whether reuse was enabled, how many reuses
occurred, and whether correctness or evidence behavior changed.

This document does not implement a runtime buffer pool, change benchmark results, or authorize a
performance claim.

## Current State

ShardLoom already records resource and stage timing evidence in benchmark rows, and the scoped
prepared/native batch runner can reuse selected source metadata and source-state structures inside
one process. `GAR-PERF-2F` plans a caller-owned `ShardLoomSession` with explicit session state,
including a future buffer pool.

What is not yet claimable:

- no global allocation profiling pass.
- no uniform allocation-count or allocation-byte row contract.
- no scoped buffer-pool contract across result buffers, temporary vectors, hash tables,
  dictionary/string state, and source-state arrays.
- no benchmark lane that compares `minimal_runtime`, `certified`, or `full_replay` evidence levels
  against buffer reuse.
- no memory/resource report that proves buffer reuse preserved correctness and evidence behavior.

## Goals

- Add a reportable allocation profile for prepared/native runtime paths.
- Define scoped buffer-reuse evidence before runtime promotion.
- Keep buffer pools opt-in or explicitly scoped to a run/session.
- Preserve correctness digests and certificate/evidence semantics.
- Keep no-fallback fields visible on every profiled row.
- Prevent hidden global pools, hidden fast modes, and unsafe lifetime shortcuts.

## Non-Goals

- No global allocator replacement.
- No process-wide hidden buffer pool.
- No daemon, service, or remote runtime.
- No unsafe lifetime shortcuts to avoid allocations.
- No spill implementation in this slice.
- No object-store, lakehouse, SQL/DataFrame, Foundry, or production claim.
- No performance, superiority, or Spark-replacement claim.

## Planned Scope

The first allocation/buffer-pool pass should classify these allocation families:

- result buffers.
- temporary vectors.
- hash tables.
- dictionary/string state.
- source-state arrays.

The first implementation slice may be report-only if the runtime cannot safely expose allocation
counts yet. Report-only rows should still say whether a family is measurable, not measurable,
not needed, blocked, or unsupported.

## Evidence Contract

Future benchmark rows, memory/resource reports, or session reports should expose these fields where
measurable:

```text
allocation_profile_status
allocation_profile_scope
allocation_count
allocation_bytes
buffer_pool_enabled
buffer_pool_scope
buffer_reuse_count
buffer_reuse_family
peak_rss_delta
source_state_digest
output_digest
correctness_digest
evidence_regression_status
unsafe_lifetime_shortcut_used=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

`allocation_count`, `allocation_bytes`, and `peak_rss_delta` may be `not_available` until the
measurement mechanism is stable. `not_available` means unknown/not measured, not zero.

## Admission Rules

Buffer reuse may be admitted only when:

- the buffer family has a declared owner and lifecycle.
- the buffer contents cannot cross incompatible schema, dtype, encoding, nullability, or ordering
  boundaries.
- the reused buffer cannot outlive the session/run that owns it.
- correctness digests match the non-reuse path.
- evidence output remains semantically identical except for explicit resource fields.
- every row still reports `fallback_attempted=false`, `external_engine_invoked=false`, and a claim
  gate.

If any rule cannot be satisfied, the row should report a deterministic blocker instead of using a
fallback engine or silently disabling evidence.

## Claim Boundary

Allocation and buffer-pool evidence can support these scoped statements only after implementation:

- a specific local prepared/native row measured allocation/resource behavior.
- a specific local run/session used or did not use scoped buffer reuse.
- a specific buffer family was reused, blocked, or not measurable.

It cannot support these claims:

- ShardLoom is faster.
- ShardLoom is more memory efficient than another engine.
- ShardLoom is production ready.
- ShardLoom has broad SQL/DataFrame, object-store/lakehouse, Foundry, or distributed runtime
  support.

## Verification Plan

Future implementation should include:

- focused unit tests for buffer ownership, reset, reuse, and release behavior.
- differential correctness tests against a no-reuse path.
- benchmark smoke that emits resource fields for at least one prepared/native family.
- a memory/resource report that includes allocation profile status, buffer pool status, reuse
  counts, peak RSS status, correctness digest, no-fallback fields, and claim gate.
- website/readiness checks only after generated benchmark docs are refreshed.

## Risks

- Measured allocations may be incomplete if allocator hooks are not stable.
- Buffer reuse can hide correctness bugs if lifecycle and schema boundaries are underspecified.
- Peak RSS is noisy across operating systems and should not be used as a public performance claim
  without repeatable workload-scoped evidence.
- Reuse inside a session can be misread as a hidden benchmark fast mode unless the session/run
  scope, reuse counts, and claim gate remain explicit.
