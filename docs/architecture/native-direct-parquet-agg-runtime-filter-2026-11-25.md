# Direct Parquet Aggregation Concurrency & Early Join Filtering (2026-11-25)

## 1. Grounding & Scope

- **Base commit SHA:** `dd7b441998430aebbcf61bbb3879cc54c339d0af` (`origin/main`).
- **Intervening fixes credited (not re-implemented):**
  - `E2E-FIX-01`: exact integer `CompactI128` accumulation in `native_relational_aggregate_compact.rs`.
  - `E2E-PERF-02`: `ResidentVortexSession::shared_compute_pool` (`ComputePool` + `scope_active_worker_limit`).
  - `E2E-PERF-04`: zero-copy `ControlledParquet` / `from_local_arrow_with_ownership` handoff with `batch_release_witness`.
  - `E2E-PERF-05A`: `ProbeRuntimeFilter` construction and row-group predicate pushdown / post-decode `apply_bloom_mask` in `local_primitive_relational_scan.rs` and `local_primitive_relational_batch_input.rs`.
  - `PR #1696`: fused `join_aggregate::Plan` in `local_primitive_relational_join_aggregate.rs`.
  - `PR #1699`: Chrysalis bounded complete-extent columnar coverage (`demanded/coverage.rs`, `resident_columnar_coverage.rs`).
- **Relationship to canonical phases & obligations:** Contributes within active Item 20 and obligations CG-5/6 (correctness & performance), CG-7/8 (operators & parallel execution), CG-14 (memory & adaptation), CG-16 (certificates & capability decisions), CG-19 (I/O), and CG-20/21 (public CLI/Python workflows). Preserves November 16 retired campaigns, Chrysalis (`PR #1699`), closed Resonance, deferred `PulseWeave` finite feedback, and separate portable-serverless / Foundry acceptance.
- **No-fallback & safety posture:** `fallback_attempted = false`, `external_engine_invoked = false`, `claim_gate_status = not_claim_grade`, `unsafe_code = "forbid"`. Upstream `vendor/` is untouched; native Parquet integration uses existing `ParquetRecordBatchReaderBuilder::with_row_selection` and `RowSelection::from_consecutive_ranges`.

## 2. Shared Source, Coordinate & Ownership Contract

All changes extend existing ShardLoom owners (`demanded`, `local_primitive_relational_batch_input`, `resident_execution_context`, `resident_memory_arrow`, `resident_columnar_coverage`, `ComputePool`, `LiveMemoryPool`):

1. **Worker-scoped execution context:** Parallel producer workers derive lightweight read-only worker execution state backed by the parent `ResidentVortexSession`, `LiveMemoryPool`, `FloatPolicy`, `RecursionLimit`, and child `CancellationToken` without re-locking exclusive execution mutexes; atomic diagnostics/evidence counters merge deterministically onto the parent context upon join.
2. **Bounded work-in-progress (`OwnedSourceBatch`):** Producer workers emit owned native `ArrayRef` chunks wrapped with `OwnedSourceBatch` carrying physical extent index, original 0-based row-group relative coordinates (`first_row_in_group..end_row_in_group`), physical decoded row count, Chrysalis coverage hit state, and `batch_release_witness` / `MemoryLease` RAII ownership. In-flight bounded channels prevent runaway buffering, ensure slow-consumer / 1-worker / tight-memory forward progress, and drain/drop cleanly on failure, limit stop, or cancellation (`reserved_bytes == 0`).
3. **Separated progress coordinates:**
   - Physical extent completion (`emitted_rows == expected_selected_rows`) is tracked distinctly from original row-group coordinates (`first_row..end`) so `RowSelection` skips never fail `Decoder::next` or poison Chrysalis.
   - Zero surviving rows inside a key-filtered extent cleanly close the extent without decoding payload columns.
4. **Conservative Chrysalis non-poisoning:** Filtered or sparse reads hold `ticket = None` and never append partial or filtered fragments to `resident_columnar_coverage::Store`. Complete unfiltered extents publish and validate only when complete row-group coverage (`expected_rows == group.rows`) finishes without limit truncation or error. Complete Chrysalis-cached regions can satisfy subsequent queries directly by exact original coordinate gather.

## 3. Track A — Demanded Parquet Concurrency & Durable Aggregate State

### Track A1: Demanded Parquet Concurrency

- Multi-row-group demanded Parquet scans (`ScanSource::Batch` in `local_primitive_relational_batch_input.rs`) admit bounded parallel decoding onto `ResidentVortexSession::shared_compute_pool` under `ComputePool::scope_active_worker_limit` (no nested pools or CPU multiplication).
- Both **ordered delivery** (required when downstream needs original order or contiguous-range reduction) and **unordered bounded delivery** are supported cleanly with explicit decline reasons when falling back to inline serial execution (`max_parallelism <= 1`, single row group, small work `< 16,384` rows, shared/replayed source, prefix limit too small, or `InsufficientHeadroom`).
- Error propagation is explicit and deterministic (no silent retry or fallback); physical prefix truncation (`max_rows`) stops cleanly without panicking.

### Track A2: Contiguous-Range Persistent Worker-Local Aggregate State

- Decouples `8,192`-row decoder batch lifetimes from `Aggregate` state lifetimes without altering `GroupOrdinals` first-seen group ordering guarantees:
  - Contiguous non-overlapping extent ranges `[0..M-1]` are assigned to `M <= 2 * workers` worker tasks.
  - Each task instantiates **one persistent local typed `Aggregate` state** (`Aggregate::new` / reused across all `8,192`-row batches in its assigned contiguous range) and merges left-to-right in strict monotonic range index order `0 -> 1 -> ... -> M-1` via `Aggregate::merge_from`.
- **Admitted aggregations:** grouped and ungrouped `Count` / `CountStar`, exact integer `Sum`/`Avg` (`CompactI128` + `typed::Table::Integer`), `Decimal` `Sum`/`Avg` (`i128` totals & counts), `Min`, `Max` (`SignedExtreme`, `UnsignedExtreme`, `FloatExtreme`, general values/strings via owned comparison), fused `ExtremaPair`, and group-only `DISTINCT`.
- **Explicit declines:** Order-sensitive `Float` `SUM`/`AVG` (`FloatPolicy::Strict` and extended float summation), `COUNT/SUM/AVG(DISTINCT ...)`, quantiles (`ApproxQuantile`/`Percentiles`), moments (`Variance`/`Stddev`), `AnyValue`/selectors, and collection/`spill` modes decline Track A2 and route cleanly onto A1 ordered parallel decode feeding single-owner serial `Aggregate::consume` so IEEE-754 summation order and semantics remain unchanged.

## 4. Track B — Early Key-First Parquet Filtering & `join_aggregate` Propagation

### Track B1: Early Parquet Filtering Before Payload Decode

- Extends `run_with_probe_filters` and `demanded.rs` so **every** admitted `ProbeRuntimeFilter` (`empty_build`, `key_predicate`, and/or 3-bit `ProbeBloomFilter` — including multi-key or >64-key Bloom-only filters) executes before payload-only columns are decoded:
  1. **Row-group metadata pruning:** existing `empty_build` and row-group `PredicateExpr` statistics pruning runs first.
  2. **Safety & validation proof gate:** skips payload decode for rejected rows **only** when demanded-domain validation guarantees hold on skipped payload columns (no `FloatPolicy::Strict` float payload columns unless NaN/Inf check isn't bypassed or region is already Chrysalis-validated, no unverified `Decimal` payload columns whose precision bounds aren't guaranteed by Parquet physical/logical schema, no unvalidated extension/nested payload columns unless already Chrysalis-validated), and no prefix/relational barrier blocks skipping (`prefix_rows`, `LIMIT`, `TopK`, `Sort`, `Window`, fallible expressions/filters). Declines record explicit diagnostic reasons and execute standard full decode + post-decode filter.
  3. **Two-stage row-group execution (when admitted):**
     - **Stage 1 (Key columns only):** Decode only the filter key projection (`key_indices`) for the row group; run native `PredicateExpr` + `Batch::hash` + `ProbeBloomFilter` probe directly on key batches (even with zero payload-only columns). Track key selection survivors as exact 0-based `RowSelection`.
     - **Zero survivors:** Immediately complete the extent without touching payload-only columns (`decoded_payload_rows += 0`).
     - **Non-zero survivors without payload columns:** Emit surviving key columns directly.
     - **Non-zero survivors with payload columns:** Open payload projection (`payload_only_indices`) on the same row group using `ParquetRecordBatchReaderBuilder::with_row_selection(survivor_selection)` and align key + payload batches by row count, skipping redundant Bloom re-hashing in `scan::accept_scan_batch` via pre-evaluated key filter witness while still applying non-pushdown residual predicates faithfully.
  4. **Sparse Chrysalis handling:** If complete validated Chrysalis coverage exists for the row group (`all_covered == true`, `acquired.missing.is_empty()`), gather directly from coverage without re-reading Parquet (`next_covered`). Otherwise, when an `early_filter` plan is active, discard any pending Chrysalis write ticket (`ticket = None`) and run Stage 1 -> Stage 2 (`RowSelection`) so sparse filtered output never pollutes Chrysalis and partial payload coverage never suppresses key-first filtering.

### Track B2: `join_aggregate` Propagation & Join Strategy Audit

- In `local_primitive_relational_join_aggregate.rs`, after building completed dimension join tables (`joins`), extract `join.probe_runtime_filters(context)?`, map probe column names through `stage.probe` columns that refer directly to `self.fact` (`origin.source == 0`), respect `fact_probe_filter_target` (skipping `ScanSource::Memory` and stripping `.bloom` on direct `.vortex` `File`/`FileKernel`/`OwnedArray` while keeping `.bloom` on Parquet `ColumnarBatch`) and `stage_validation_infallible` (preserving earlier-stage fallible validation order), locally align multi-chunk `.vortex` batches via `fact_chunk_subranges` when `Source::try_new` needs single-chunk slices, and invoke `prepared.run_with_probe_filters(self.fact, &probe_filters, ...)` on **both** direct unique-key aggregation and duplicate/representation fallback paths.
- **Join strategy propagation audit matrix:**
  - Ordinary `Join` (`local_primitive_prepared_relational.rs`): `left_filters` pushed into left child across `Scan`, `FileKernel` (projection), `Project`, `Filter`, `Pipeline`, and `UnionAll` when `declaration_accepts_probe_filters` holds and no right/left limit/order barrier blocks.
  - Fused/star `join_aggregate` (`local_primitive_relational_join_aggregate.rs`): completed dimension `ProbeRuntimeFilter`s projected onto `origin.source == 0` columns and pushed to `self.fact` (`run_with_probe_filters`), recording `probe_filters_pushed`.
  - Ordered join / limit-bearing / fallible-earlier-stage / spilled partition-local join: early payload-skipping or cross-partition filter pushdown declines cleanly with explicit structural audit reasons, preserving exact semantics.

## 5. Evidence Compatibility & Measurement Protocol

- Additive compatibility fields in `ColumnarReadReport`, `ExecutedVortexBatchInput`, `NativeJoinAggregateWork`, CLI JSON reports (`native_input_parquet_*`), and Python `Result` (`parquet_*` properties), preserving `scan_rows` and all `native_source_demand_*` / `relational_scan_rows_delivered` contracts unchanged:
  - CLI fields: `native_input_parquet_parallel_workers`, `native_input_parquet_parallel_range_tasks`, `native_input_parquet_parallel_declined_sources`, `native_input_parquet_persistent_aggregate_states`, `native_input_parquet_aggregate_batches_consumed`, `native_input_parquet_aggregate_partial_groups_emitted`, `native_input_parquet_aggregate_partial_groups_merged`, `native_input_parquet_aggregate_final_groups`, `native_input_parquet_key_filter_rows_decoded`, `native_input_parquet_key_filter_rows_selected`, `native_input_parquet_payload_rows_decoded`, `native_input_parquet_row_groups_pruned`, `native_input_parquet_payload_skipped_row_groups`, and `native_input_parquet_selected_chunk_bytes_read` (strictly omitted unless `columnar_parquet_sources > 0`).
  - Python `Result` properties expose `parquet_parallel_workers`, `parquet_parallel_range_tasks`, `parquet_persistent_aggregate_states`, `parquet_aggregate_partial_groups_emitted`, `parquet_aggregate_partial_groups_merged`, `parquet_aggregate_final_groups`, `parquet_key_filter_rows_decoded`, `parquet_key_filter_rows_selected`, `parquet_payload_rows_decoded`, and `parquet_payload_skipped_row_groups` (`None` when unobserved).
  - `local_primitive_native_io_join_work_json` `direct_aggregate.probe_filters_pushed` reports pushed join-aggregate filter count.
- **Measured disposition (`2026-11-25`):** Retained locally under the October 28 binary complete-cohort rule; see [benchmark report](../benchmarks/native-direct-parquet-agg-runtime-filter-2026-11-25.md) for the isolated 14-query cold/warm `engine_execution_seconds` comparison (`-50.92%` cold CLI, `-51.88%` warm resident, `fallback_attempted = false`) and validation receipts. Routine updates report `engine_execution_seconds` only per `AGENTS.md`.
