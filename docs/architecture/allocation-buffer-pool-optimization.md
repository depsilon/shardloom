# Allocation And Buffer-Pool Optimization

Status: scoped report-only evidence slice for `GAR-PERF-2G`.

## Summary

`GAR-PERF-2G` defines the allocation profiling and scoped buffer-reuse layer for prepared/native
ShardLoom runtime work. The goal is to make memory behavior visible before optimizing it: which
operators allocate, which buffers can be reused safely, whether reuse was enabled, how many reuses
occurred, and whether correctness or evidence behavior changed.

This document does not implement a runtime buffer pool, enable allocation hooks, change benchmark
claim status, or authorize a performance claim.

## Current State

ShardLoom records resource and stage timing evidence in benchmark rows, and the scoped
prepared/native batch runner can reuse selected source metadata and source-state structures inside
one process. `GAR-PERF-2F` emits caller-owned scoped session evidence for prepared/native local
artifacts. The first `GAR-PERF-2G` slice now adds deterministic allocation/resource-profile fields
to that same session-backed batch evidence:

- allocation profile status/scope and family classification.
- allocation count, allocation bytes, and peak RSS status as explicit `not_available` values until
  stable measurement exists.
- `buffer_pool_enabled=false`, `buffer_reuse_count=0`, and a deterministic buffer-reuse blocker.
- correctness/evidence-regression posture, no unsafe lifetime shortcut, and no-fallback/no-external
  engine fields.

What is not yet claimable:

- no global allocation profiling pass.
- no measured allocation-count or allocation-byte contract.
- no enabled scoped buffer pool across result buffers, temporary vectors, hash tables,
  dictionary/string state, or source-state arrays.
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

The first implementation slice is report-only because the runtime does not yet safely expose
allocation counts or peak RSS. Report-only rows still say whether a family is measurable, not
measurable, not needed, blocked, or unsupported.

## Evidence Contract

Benchmark rows, memory/resource reports, or session reports should expose these fields where
measurable or explicitly report `not_available`/blocked:

```text
allocation_profile_status
allocation_profile_scope
allocation_count
allocation_count_status
allocation_bytes
allocation_bytes_status
buffer_pool_enabled
buffer_pool_scope
buffer_reuse_count
buffer_reuse_family
buffer_reuse_blocker
peak_rss_delta
peak_rss_delta_status
source_state_digest
output_digest
correctness_digest
evidence_regression_status
unsafe_lifetime_shortcut_used=false
allocation_fallback_attempted=false
allocation_external_engine_invoked=false
allocation_claim_gate_status
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

Allocation and buffer-pool evidence can support these scoped statements only when the corresponding
fields exist:

- a specific local prepared/native row reported allocation/resource posture.
- a specific local run/session used or did not use scoped buffer reuse.
- a specific buffer family was reused, blocked, or not measurable.

It cannot support these claims:

- ShardLoom is faster.
- ShardLoom is more memory efficient than another engine.
- ShardLoom is production ready.
- ShardLoom has broad SQL/DataFrame, object-store/lakehouse, Foundry, or distributed runtime
  support.

## Verification Plan

Implementation and future hardening should include:

- focused unit tests for the emitted report-only fields and, later, buffer ownership, reset, reuse,
  and release behavior.
- differential correctness tests against a no-reuse path before enabling any reuse.
- benchmark smoke that emits resource fields for prepared/native session-backed batch rows.
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
