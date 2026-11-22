<!-- SPDX-License-Identifier: Apache-2.0 -->

# E2E Queue Reconciliation (v1.1.0) and R3 Module-Boundary Inventory (`E2E-ARCH-01`)

Status: active architectural inventory and queue reconciliation note on `main` (`v1.1.0`).

## Contract and Invariant Posture

```text
schema_version=shardloom.e2e_v110_queue_reconciliation.v1
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
production_claim_allowed=false
performance_claim_allowed=false
real_vortex_payload_separation_preserved=true
```

- All `E2E-*` identifiers (`E2E-FIX-01`, `E2E-VERIFY-01`..`03`, `E2E-ARCH-01`..`02`,
  `E2E-PERF-00`..`04`, `E2E-PERF-05A`..`05C`, `E2E-PERF-06`, `E2E-PGO-01`, `E2E-PYO3-01`; with old
  `E2E-ARCH-03` absorbed into `E2E-PERF-01` and old `E2E-ARCH-04` split into `E2E-PGO-01` +
  `E2E-PYO3-01`) are **queue identifiers only** mapped under existing canonical implementation
  phases (`PERF-01`..`PERF-13`), RFC owners, and competitive gates (`CG-1` through `CG-23`, all
  remaining visible and independent until their own acceptance). They introduce zero duplicate
  canonical phase IDs, zero external query-engine fallback (`Spark`, `DataFusion`, `DuckDB`,
  `Polars`, `Velox`, `vortex-datafusion`), and no premature superiority claims.
- This document preserves strict separation between placeholder text output artifacts
  (`shardloom-vortex/src/output_payload.rs`, `real_vortex_payload=false`) and real native Vortex
  output payload paths, as well as R3 cleanup sequencing guardrails from `AGENTS.md`: inventory and
  compatibility locks first, then targeted single-purpose cleanup slices without unapproved public
  symbol renames or bundled behavioral edits.

---

## 1. Reconciliation Against Current `v1.1.0` `main`

The revised 17-item `E2E-*` queue (`E2E-FIX-01` through `E2E-PYO3-01`) was drafted against the
historical September 27 (`0.3.1`-era `codex/json-direct-builders`) branch rather than current
`v1.1.0`. With consolidated [PR #1711](https://github.com/depsilon/shardloom/pull/1711) merged
(`91fbee00212e2b4267505ed1b0f63eb68110ce6e`), **Resource-admitted deep traversal**, **Resonance
Cells and Leases**, `E2E-ARCH-01` runner/`traditional_analytics.rs` removal, `E2E-PERF-02` kernel
foundations, and `E2E-PYO3-01` (`shardloom-python`) have already landed on `v1.1.0`, while PR
`#1713` adds `E2E-PERF-01` Slice 1 (exact Vortex footer metadata aggregates) and transcribes the
reconciled 17-item queue into `docs/architecture/phased-execution-plan.md` ahead of
**Version 1.3.0 source milestone**.

| Queue ID | Already Landed in `v1.1.0` (`main` + `#1711` / `#1713` Slice 1) | Genuinely Still Open on `v1.1.0` (Verified Paths) |
| --- | --- | --- |
| `E2E-FIX-01` | C7 whole-file JSON typed construction (pre-`v0.4.0`); Python `_bounded_materialization_report` / `_bounded_schema_report` (`python/src/shardloom/query.py:6923-6962`); 4-unit Arrow `Timestamp` (`NATIVE-TIMESTAMP-PROVIDER`, `#1543`); exact 8-byte-aligned `i128` (`CompactI128`) integer `SUM`/`AVG` accumulators across `shardloom-vortex` | Closed (`[x]` on `codex/pre-130-phased-plan-completion`, `F64` output schema preserved) |
| `E2E-VERIFY-01` | `scripts/clickbench_independent_reference.py` aligned to exact `HUGEINT`; `docs/benchmarks/e2e-perf-baseline-protocol-v110.md` + `scripts/evaluate_perf_ship_drop.py`; seeded `differential_oracle_tests.rs`; `scripts/run_ci_perf_smoke.py` | Same-commit 100M `Full43` + ingest baseline capture awaiting mounted 100M `hits.parquet` / offline DuckDB environment |
| `E2E-VERIFY-02` | Resident session & worker lifecycle tests + paired `cold_process_cli` / `warm_resident_session` / `warm_resident_concurrent` lanes in `scripts/run_clickbench_query_uat.py` with separate startup timing & `verify_zero_answer_cache` | Closed (`[x]` on `codex/pre-130-phased-plan-completion`) |
| `E2E-ARCH-01` | `traditional_analytics.rs` (`56.5K` LOC) removed from `shardloom-vortex`; `DocumentReadCache`, `scripts/run_all_checks.py`, and Section 2 R3 inventory landed; all 9 internal `Vec<(String, String)>` evidence bags replaced with typed `serde` structs; ~40K LOC extracted via thin facades (`inline_tests.rs`, `ingest/vortex_ingest_inline_tests.rs`, `routing/public_workflow_route_tests.rs`, `sql_syntax_tests.rs`, `_query_reports.py`) | Closed (`[x]` on `codex/pre-130-phased-plan-completion`; incremental production-body splits continue on touch during `E2E-PERF-01`..`06` and `E2E-ARCH-02`) |
| `E2E-PERF-00` | Workspace lint `unsafe_code = "forbid"` in library crates; [RFC 0048](../rfcs/0048-cli-global-allocator-mimalloc.md) + [mimalloc provenance review](../dependencies/mimalloc-dependency-review.md); zero workspace production `Ordering::SeqCst` (`native_local_selection_observation.rs`); thread-local/shared `CurrentThreadRuntime` in `prepared_source_binding.rs`; single-pass JSON in `count_where_report`; `NativeSinkPlan::with_durable_sync` opt-out | `mimalloc` crate offline cache intake + Full43 screen; remaining production `CurrentThreadRuntime` callers (`ingest_runtime.rs:35`, `local_primitives.rs:2622`, `resident_session.rs:688`, `vortex_ingest.rs:14493`) aligned with `E2E-PERF-01`; lazy `hot_runtime` deferral for remaining `16` production `json!` sites in `local_primitives.rs` |
| `E2E-PERF-01` | `ReservedHostAllocator` wired into several aggregate workers; `local_primitive_native_writer.rs:132` selected file stats; Slice 1 exact footer metadata completion (`local_primitive_footer_aggregate*.rs`); verified `reader_generated_encoded_kernel_inputs_from_vortex_chunk` (`local_primitives.rs:16026`) is `#[cfg(test)]` | Verify production `BitPacked`/`Dict`/`Sparse`/`RunEnd` encoded-input descriptors avoid heap `Vec` unpacking; eliminate 15 `execution_session.clear()` sites (`shardloom-cli/src/{lib.rs,public_resident_aggregate.rs,public_resident_count.rs}`) & per-query `with_segment_source` resets (`resident_execution_context.rs:864`, `resident_session.rs:1356`); `predicate_ordering_used: false` (`runtime_utilization.rs:397`); `force_residual_predicate_for_source_ordinals` (`local_primitives.rs:23854,24699`); absorbed `E2E-ARCH-03` coalesced async byte-range I/O screen |
| `E2E-PERF-02` | `aggregate_chunk_jobs.rs`, `string_count_partitions.rs`, `ComputePool` targeted wakeups & `OneshotSlot`, `Copy` static row kernels, and `O(1)` compensated `add_integer_repeated` (`#1711`) | Per-query `ComputePool::new` (`aggregate_chunk_jobs.rs:257`, `vortex_ingest.rs:11500`) & `ComputePool` vs. `MorselScheduler` unification; `Q34`/`Q35` scratch recycling; `compact_string_state.rs` / `string_count_owner_scheduling.rs` / `R10`; remaining `Vec<StatValue>` & `encoded_numeric_reduction.rs` vectorization |
| `E2E-PERF-03` | `mixed_distinct_workers.rs`, `winner_distinct.rs`, `dictionary_prepare_workers.rs` landed | Remaining parallel operator generalization (nullable/HAVING/type cliffs in `CountWorkers`, `TripleWorkers`/`PairWorkers` offload, `R2.a`/`R2.b`/`R3.a`/`R3.b`/`R4`/`R6.c` zero-reopen late materialization, `Q33` single-pass directory release, parallel `SortRows` Top-K, unordered `GROUP BY ... LIMIT K`) |
| `E2E-PERF-04` | `ingest_cpu_lanes.rs`, `ingest_bounded_layout.rs`, `native_input_*` dtype/JSON helpers landed | `IngestCpuLanes::pipeline` clamping `pipeline_demand(requested, 1)` (`ingest_cpu_lanes.rs:75`); `ingest_arrow_ownership::copy_batch` (`163`); `publish_vortex_prepared_olap_single_artifact_state` (`vortex_ingest.rs:2712`) duplicate SHA-256 readback; `VarBinView` canonical inflation & CSV/JSON/IPC reader modernization |
| `E2E-PERF-06` | `cli_output::emit_timed` removed; `local_primitive_columnar_compat_sink.rs` uses Snappy compression | Universal `OwnedVortexResultBatch` emission; `local_primitive_native_sink.rs:748` BtrBlocks/`ZonedLayout` upgrade; `local_primitive_columnar_compat_sink.rs:73,124` `batch_rows: 2048` cap; zero-serialization `OwnedArraySource` composition; binary Arrow IPC persistent worker framing; lean `hot_runtime` envelopes |
| `E2E-ARCH-02` | `shardloom-cli/src/sql_syntax.rs` extracted out of legacy monoliths | Parser RFC + Apache-2.0 provenance audit; zero DataFusion/execution crates in `Cargo.lock`; one canonical `SQL AST -> shardloom-plan` binder retiring handwritten splitters across `sql_syntax.rs`, `public_workflow_route.rs`, `sql_local_source_runtime.rs`, and `python/src/shardloom/query.py` |
| `E2E-PERF-05A` | Native columnar joins (`NATIVE-JOIN-PRESSURE`) landed | Wiring `E2E-ARCH-02` `shardloom-plan` binder into live execution, compiled columnar expression evaluation (`shardloom-core/src/expression.rs`), `vortex_middle` projection/filter pushdown, and full SQL/`LazyFrame` join convergence with build-side runtime filters |
| `E2E-PERF-05B` | Native columnar windows, subqueries, and set ops (`NATIVE-WINDOW-PRESSURE`, `NATIVE-SCALAR-SUBQUERIES`, `NATIVE-STREAM-SETS`) landed | Converging AST-bound windows, uncapped set operations, reusable `OwnedArraySource` CTEs, and decorrelated subqueries while retiring `sql_local_source_runtime.rs` `MAX_LIMIT_ROWS = 10_000` (`line 103`) row-map fallback |
| `E2E-PERF-05C` | Native query spill (`NATIVE-JOIN-PRESSURE`, `NATIVE-WINDOW-PRESSURE`, `NATIVE-PIVOT-PRESSURE`, `NATIVE-GENERAL-AGGREGATE-PRESSURE`) landed | Raising `MAX_BLOCK_ROWS = 1024` (`local_primitive_sort_spill.rs:38`) to `16K–64K` streaming blocks, broadening native Vortex spill across operators, raising k-way merge fan-in, and replacing triple `validate_run_bytes` SHA-256 disk re-reads (`local_primitive_query_run_store.rs:511,631,833`) |
| `E2E-VERIFY-03` | Single-table ClickBench oracle/UAT runners and storage guards exist | Reproducible multi-table TPC-H and Star Schema Benchmark (SSB) differential oracle and paired cold/warm benchmark suites under non-synced local storage guards |
| `E2E-PGO-01` | `scripts/pgo_local_experiment.py` harness exists (`C5` historical screens preserved) | Post-unification `PERF-13` non-ClickBench multi-family PGO training corpus + `llvm-profdata` missing-symbol coverage gate and paired ship/drop screen against portable ThinLTO |
| `E2E-PYO3-01` | **Shipped on `v1.1.0`**: `shardloom-python` (`PyO3 =0.29.3`, `abi3-py310`), `capsules.rs` (`__arrow_c_stream__`), GIL-released `session.rs`, `unsafe_code = "forbid"` in core crates, zero-dep `python/pyproject.toml`, 3-platform CI wheel acceptance | Closed (`[x]` on `v1.1.0`); future changes follow `E2E-PERF-06` and `Version 1.3.0 source milestone` |

---

## 2. R3 Module-Boundary & Public-Symbol Compatibility Inventory (`E2E-ARCH-01`)

This inventory honors `AGENTS.md` R3 cleanup sequencing guardrails:
1. Inventory public boundaries and lock invariants **before** moving code.
2. Keep public Rust exports (`pub use`), CLI command names (`REGISTERED_COMMANDS`), Python package imports (`shardloom.*`), `evidence_schema_registry.rs`, and `typed_envelope_compatibility_lock.rs` unchanged during file splits.
3. Do not bundle unrelated behavioral changes, SQL parser adoption, or `E2E-PERF-*` tuning into pure modularization PRs.

### 2.1 Large Source Files (`origin/main`, `aa8407d6f`) & Split Plan

| File | LOC (`origin/main`) | Current Mixed Responsibilities | Target Domain/Operator Submodules & Compatibility Guarantee |
| --- | --- | --- | --- |
| `shardloom-vortex/src/local_primitives.rs` | `70,802` | Request types, encoded scan kernel input extraction, `SortRows`, `DistinctRows`/`DropDuplicateRows`, residual filtering, partitioned dispatch, result assembly, tests | Split remaining inline scan/sort/distinct/dispatch logic and inline `#[cfg(test)]` suites into focused files under `shardloom-vortex/src/local_primitives/`, keeping `local_primitives.rs` as a thin re-export facade preserving every `pub` / `pub(crate)` signature |
| `shardloom-vortex/src/vortex_ingest.rs` | `22,351` | Format normalization, physical layout planning, Zstd/BtrBlocks encoding strategies, publication state hashing, tests | Extract publication state verification, layout strategy selection, and inline test modules into `shardloom-vortex/src/ingest/` while preserving `publish_vortex_prepared_olap_single_artifact_state` and public ingest entrypoints |
| `shardloom-cli/src/public_workflow_route.rs` | `18,837` | Route classification, SQL/workflow matching, `PublicExecutionSession`, compatibility fallback dispatch, envelopes | Separate session state, route classification, and Vortex vs local-source dispatch into `shardloom-cli/src/routing/` with facade re-exports preserving `typed_envelope_compatibility_lock` |
| `shardloom-cli/src/sql_syntax.rs` | `17,581` | Lexical scanning, clause splitting, expression/predicate/window/join parsing, diagnostics | Split by SQL grammar clause family ahead of `E2E-ARCH-02` AST binder migration while keeping public SQL parse API and `RFC 0012` diagnostic codes intact |
| `python/src/shardloom/query.py` | `13,774` | `LazyFrame`, `SqlWorkflow`, expression AST nodes, local-source guards, materialization helpers | Decompose into internal `_lazy.py`, `_sql_workflow.py`, `_expressions.py`, and `_materialize.py` re-exported from `shardloom.query` with zero public import breakage |
| `shardloom-cli/src/workflow_planning.rs` | `13,612` | Workflow plan lowering, source validation, capability & estimate assembly | Split planning vs validation/reporting helpers while keeping public CLI plan/explain/estimate outputs unchanged |
| `python/src/shardloom/context.py` | `12,608` | `ShardLoomContext` readers, capability helpers, workflow entrypoints, native session plumbing | Group capability discovery helpers vs reader/builder entrypoints into `_context_*.py` submodules re-exported on `ShardLoomContext` |
| `shardloom-cli/src/status_capabilities.rs` | `11,867` | Static capability snapshots, doctor/status builders, support matrix serialization | Split capability family builders into cohesive submodules preserving `capability_discovery_snapshots.rs` |
| `shardloom-cli/src/sql_local_source_runtime.rs` | `11,674` | Compatibility CSV/JSON/row-map SQL execution, window/join/subquery local helpers | Isolate format readers vs relational row fallback helpers pending `E2E-PERF-05` retirement |
| `python/src/shardloom/client.py` | `9,879` | Persistent worker client, CLI subprocess invocation, argv serialization, response decoding | Extract worker transport & subprocess framing cleanly ahead of `E2E-PERF-06` binary IPC framing |

Note: `shardloom-vortex/src/universal_format_io.rs` (`9,435` LOC) and `shardloom-cli/src/vortex_primitive_execution.rs` (`9,912` LOC) sit immediately behind the top ten and follow the same submodule-facade rule when touched in subsequent cleanups.

### 2.2 Stringly-Typed `Vec<(String, String)>` Evidence Bags

A subsequent dedicated `E2E-ARCH-01` slice (separate from file splits and separate from `run_all_checks.py`) will replace internal `Vec<(String, String)>` evidence bags with strongly-typed `serde` / adapter structs while proving key-order and JSON parity against `shardloom-cli/src/evidence_schema_registry.rs` and `shardloom-cli/tests/typed_envelope_compatibility_lock.rs`:

| Crate & Location | Symbol | Compatibility Constraint |
| --- | --- | --- |
| `shardloom-core/src/execution_resources.rs:322` | `ExecutionResourcePlan::evidence_fields` | Must emit identical keys/strings into `OutputEnvelope` slots |
| `shardloom-core/src/feature_footprint.rs:265` | `FeatureFootprintReport::crate_versions` | Preserve serialized doctor/feature-footprint field names |
| `shardloom-core/src/output.rs:461` | `ExecutionModeSelection::fields` | Must preserve all `evidence_schema_registry.rs` keys (`8` surfaces / `300` fields) |
| `shardloom-core/src/output.rs:565` | `OutputTypedPayload::fields` | Transition via typed adapter methods while keeping JSON wire compatibility |
| `shardloom-core/src/output.rs:691` | `OutputEnvelope::fields` | Retain backward-compatible mirror only where locked until command migration completes |
| `shardloom-core/src/schema.rs:128` | `SchemaField::metadata` | Preserve schema equality/hashing and FlatBuffer/Arrow metadata round-trips |
| `shardloom-core/src/security.rs:1286` | `WorkspaceSafeLocalWriteReport::evidence_fields` | Preserve workspace path-safety audit key names (`prefix` scoping) |
| `shardloom-exec/src/lib.rs:161` | `ShardLoomExecutionInlineArtifact::fields` | Preserve inline artifact key/value ordering in CLI envelopes |
| `shardloom-exec/src/pulseweave.rs:713` | `PulseWeavePlan::fields` | Preserve PulseWeave telemetry/policy keys |

---

## 3. Unified Gate Validator Inventory (`scripts/run_all_checks.py`)

`scripts/check_*.py` contains `68` Python files: `66` runnable CLI scripts and `2` library helper modules imported by other validators (`check_published_channel_proofs.py` and `check_registry_bundled_proof.py`).

`scripts/run_all_checks.py` keeps all `68` existing `scripts/check_*.py` paths untouched as standalone entrypoints and executes admitted pure-Python validators in-process inside `release_report_utils.cached_document_reads()` so shared documents (`Cargo.toml`, `Cargo.lock`, `phased-execution-plan.md`, `v1-inclusion-scope-matrix.md`, `public-status-matrix.md`) are read once per run into memory (`copy.deepcopy` isolated for JSON) while writing per-check JSON outputs into a disposable temporary directory (unless `--keep-reports-dir` is supplied).

### Suite Classifications (`68` Files Total)

- **`docs` (`37` clean-repo, offline, side-effect-free pure-Python validators)**:
  `check_benchmark_constitution`, `check_benchmark_optimization_targets`,
  `check_benchmark_publication_claim_gate`, `check_benchmark_publish_doctor`, `check_ci_gate_matrix`,
  `check_clickbench_olap_runtime_coverage`, `check_contribution_governance`,
  `check_enterprise_evidence_export_pack`, `check_foundry_package_proof_boundary`,
  `check_front_door_benchmark_publication`, `check_local_format_production_profiles`,
  `check_pre_5j_dependency_freshness`, `check_public_claim_language`, `check_public_status_docs`,
  `check_python_license_bundle`, `check_runtime_execution_envelopes`,
  `check_runtime_gap_family_burn_down`, `check_security_posture`,
  `check_sql_python_dataframe_parity`, `check_universal_ingress_routes`, `check_use_case_backlinks`,
  `check_use_case_index`, `check_user_route_capability_report`,
  `check_user_surface_graduation_matrix`, `check_user_surface_reference`,
  `check_user_surface_runtime_gap_inventory`, `check_v1_api_schema_stability`,
  `check_v1_docs_productization`, `check_v1_front_door_runtime_scope`, `check_v1_inclusion_scope`,
  `check_v1_local_output_sink_scope`, `check_v1_local_source_package_release`,
  `check_v1_source_prepared_state_scope`, `check_v1_vortex_runtime_scope`, `check_website_readiness`,
  `check_workflow_recipes`, `check_workspace_version_sources`.
- **`gated` (`29` scripts requiring compiled binaries, pre-generated sibling reports in `target/`, external CLI tools, `--allow-blocked`, or expected open-gap status)**:
  Includes binary/artifact/sibling-report gates (`check_admitted_semantics_matrix`,
  `check_benchmark_artifact_completeness`, `check_compatibility_output_translation_reports`,
  `check_dependency_audit`, `check_final_release_approval`, `check_finished_product_readiness`,
  `check_foundry_dev_stack_starter`, `check_golden_workflows`, `check_local_format_edge_case_fixtures`,
  `check_local_format_pushdown_fidelity`, `check_native_python_wheel`,
  `check_package_channel_readiness`, `check_production_certification_gate`,
  `check_production_usability_gate`, `check_python_user_surface_completion`,
  `check_release_architecture_tracker`, `check_release_readiness`, `check_release_security_gate`,
  `check_repo_wide_audit`, `check_v1_correctness_conformance`, `check_v1_example_replay`,
  `check_v1_local_resource_safety`, `check_v1_observability_support`, `check_v1_release_boundary`,
  `check_v1_security_ci_hardening`), hardcoded `target/` report writer (`check_evidence_schema_registry`),
  and open-backlog completion/atlas checks (`check_compute_engine_completion_gate`,
  `check_use_case_coverage`, `check_use_case_glossary`).
- **`library_helper` (`2` non-CLI helper modules)**:
  `check_published_channel_proofs`, `check_registry_bundled_proof`.
