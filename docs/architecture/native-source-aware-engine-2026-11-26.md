<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source-Aware Engine Foundation: Orders 1–5 Architecture (2026-11-26)

## 1. Grounding & Scope

- **Base commit SHA:** `04ad4b6d1ea1f12200b6bbd9bdb82e5ad5827bf3` (`origin/main`, after [PR #1722](https://github.com/depsilon/shardloom/pull/1722) merged the `2026-11-25` direct Parquet aggregation & early join-filter follow-on).
- **Retained prior owners credited without duplication:** `PR #1696` (`join_aggregate`), `PR #1699` (Chrysalis `resident_columnar_coverage`), `PR #1722` (demanded Parquet Stage-1 `RowSelection`, `ProbeRuntimeFilter` pushdown, contiguous-range persistent `Aggregate::merge_from`), and `E2E-FIX-01` (`CompactI128`).
- **Scope:** Implements and locally retains **Orders 1–5** of the 7-order Source-Aware Engine Performance Program inside item 20 of [the phased execution plan](phased-execution-plan.md). **Orders 6–7** remain queued in item 20 without speculative implementation. Preserves November 16 retired campaigns, closed Resonance, deferred `PulseWeave` finite feedback, and separate portable-serverless / Foundry acceptance untouched.
- **No-fallback & safety posture:** `fallback_attempted = false`, `external_engine_invoked = false`, `claim_gate_status = not_claim_grade`, `unsafe_code = "forbid"`, and zero upstream `vendor/` modifications.

## 2. Orders 1–5 Design & Contracts

### Order 1 — Native `.vortex` `FileKernel` Projection Unwrap & Selective Pushdown

- **`local_primitive_prepared_relational.rs` & `local_primitive_relational_scan_parallel.rs`:** Projection-only `NodeKind::FileKernel` nodes (`kernel.operation.is_projection()`, e.g., `Q12`-style projections) are unwrapped transparently during predicate pushdown, join `ProbeRuntimeFilter` propagation, and `ParallelSourceChain` construction (`unwrapped_file_kernels`) so selective filters and runtime join pushdown evaluate directly against native `.vortex` scans without blocking `FileKernel` evidence tracking (`file_pruning` invariant preserved).

### Order 2 — Wrapped Parquet Stage-1 Selection, Bloom, Dictionary Admission, & Chrysalis Gate

- **`universal_format_io/demanded.rs`, `demanded/early_filter.rs`, `demanded/coverage.rs`, & `resident_memory_owned_intake.rs`:**
  - Extends Stage-1 key-only decode, exact `RowSelection`, and filter-masked Bloom (`apply_bloom_mask_selected`) to Hive-partitioned and `union_by_name` Parquet collections (`ColumnSlot::Key`, `ColumnSlot::Payload`, `ColumnSlot::PartitionValue`, `ColumnSlot::MissingNull`).
  - Sorts and deduplicates `key_indices` and `payload_indices` via `BTreeSet<usize>` in `early_filter::plan` so `ColumnSlot::Key(_)` / `ColumnSlot::Payload(_)` indices match `ProjectionMask::roots`' ascending physical leaf order even when `union_by_name` member files reorder physical columns, while verifying all `key_names` upfront and carrying explicit `key_count` in `StagedKeyStream`.
  - Accounts Hive partition scalar string byte lengths in `extra_metadata` and admits flat owned scalar `ConstantArray` constants (`Bool`, `Primitive`, `Decimal`, `Utf8`, `Binary`) in `resident_memory_owned_intake::retain_metadata` without borrowed caller backing.
  - Adds dictionary key-domain probe admission and flat-buffer Chrysalis retention gating so complete unfiltered extents publish safely while filtered Stage-1 selections hold `ticket = None`.

### Order 3 — `RowSet` Cross-Batch Dictionary Reuse & `RowIndex::entry_hash` Reuse

- **`local_primitives/native_relational_set.rs` & `native_relational_index.rs`:**
  - Adds `EpochDomainMap` (`MAX_EPOCH_DOMAIN_LEN = 262_144`, `MAX_EPOCH_DOMAIN_COMPARE_LEN = 8_192`) on `RowSet` to cache group ordinals (`slots: ReservedVec<u32>`, `null_group`) across successive dictionary-encoded batches using an owned `LiveMemoryPool`-reserved dictionary snapshot (`offsets`, `bytes`, `valid_words`) for small domains (`<= 8,192`) and transient intra-batch `ReservedVec<u32>` slot storage dropped at batch end for bounded larger domains (`8,193..=262_144` when `domain_len <= batch.len()`). Zero input `ArrayRef`, `BufferHandle`, or `MemoryLease` references are retained across consumed batches (`verify_released` invariant preserved), and changed dictionary domains cleanly reset `epoch_domain`.
  - Reuses stored `RowIndex::entry_hash` across `merge_from` and hash-table growth/interning paths so already-hashed group entries never re-hash keys unnecessarily.

### Order 4 — Borrowed `DistinctMeasureRows`, `canonical_slots`, & `numeric_reuse`

- **`native_relational_rows.rs`, `native_relational_aggregate.rs`, `native_relational_aggregate_canonical.rs`, & `native_relational_numeric_reuse.rs`:**
  - Tracks per-group `COUNT(DISTINCT)` sets over borrowed key/value encodings via `DistinctMeasureRows` (dramatically accelerating `COUNT(DISTINCT ...)` on both Parquet and `.vortex` without eager per-row heap allocation).
  - Unifies `SUM` / `AVG` / `CountDistinct` updates in `canonical_slots` and reduces temporary copy overhead via `numeric_reuse::record_copy`.

### Order 5 — Shared Source/Operator Worker Controller, Lock-Free `SealedJoin`, & Multi-Measure `join_aggregate`

- **Shared parallel controller (`local_primitive_relational_scan_parallel.rs`):** Unifies bounded `ComputePool` worker orchestration and ordered range merging across Parquet `ScanSource::Batch`, `.vortex` `ScanSource::File`, and `OwnedArraySource`, while strictly holding **`operator_workers == 1`** on order-sensitive `Float` `SUM`/`AVG` (`FloatPolicy::Strict` and extended float summation) so IEEE-754 summation order stays bit-deterministic even when `source_workers >= 2`.
- **Lock-free parallel `SealedJoin` (`native_relational_join.rs`, `native_relational_index_adaptive.rs`, `local_primitive_relational_batch_parallel.rs`, `scan_parallel.rs`):** Moves build-side hash index into immutable lock-free `SealedJoin` (shared across workers via `RowSet::into_shared` and adaptive buckets), enabling parallel probe and `matched` bitset merge across all 6 keyed join kinds (`Inner`, `Left`, `Right`, `Full`, `LeftSemi`, `LeftAnti`) on Parquet, `.vortex`, and `OwnedArraySource`.
- **Multi-measure `join_aggregate` (`local_primitive_relational_join_aggregate.rs`, `native_relational_aggregate.rs`):** Enables multi-measure `Aggregate::consume_matches` inside fused `join_aggregate` with probe-side `ProbeRuntimeFilter` pushdown. Direct key borrowing in `join_aggregate` applies cleanly to flat Parquet batches while compressed `.vortex` (BitPack/FoR) batches explicitly decline `KeyColumn::try_borrow` (`source_batches_declined > 0`, preserving `probe_filters_pushed >= 1` and exact results).

## 3. Worker Observability Surface

- **Rust & CLI execution reports (`local_primitive_relational_writer_report.rs`, `local_primitive_relational_provider.rs`, `shardloom-cli/src/native.rs`):**
  - `native_relational_source_workers`: effective parallel source worker/lane count (`>= 2` when parallel source decoding runs, including `.vortex` `Strategy::Aggregate` concurrency or parallel `scan_parallel`/`batch_parallel` scans).
  - `native_relational_operator_workers`: effective parallel operator worker count (`>= 2` for parallel canonical/int/min/max `scan_parallel`, `SealedJoin` parallel probe, or Python standalone `PreparedVortexAggregate` `worker_pool`; explicitly `1` for float `SUM`/`AVG`, `scan_parallel` `COUNT(DISTINCT)`, and inline serial execution).
- **Python SDK (`python/src/shardloom/query.py`):** Exposes `Result.source_workers` and `Result.operator_workers` properties backed by the native execution report fields.

## 4. Queued Scope Remaining in Item 20 (Orders 6–7)

- **Order 6 (queued):** Operator-guided layout & encoding feedback across prepared `.vortex` and adaptive columnar layouts.
- **Order 7 (queued):** Adaptive skewed/selective runtime join and multi-stage pipeline scheduling.
