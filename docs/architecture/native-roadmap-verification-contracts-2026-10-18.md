# Native roadmap verification contracts

These commands are part of the [active phase plan](phased-execution-plan.md), not a second queue. Each owner has an executable starting regression set and named evidence outputs. Existing regressions protect accepted foundations; they do not prove the remaining implementation by themselves. Add exact regression selectors for the changed behavior to the same owner before closing its substep. Preserve the original experiment protocols linked by the [reconciliation](native-roadmap-reconciliation-2026-10-18.md).

## Running and recording checks

Run from the repository root. On macOS, use a unique local evidence directory outside synced folders, resolve Cargo's target directory, and apply the [storage and process controls](local-development-storage.md) before builds or UAT:

```sh
mkdir -p "$HOME/LocalData/shardloom"
SHARDLOOM_ROADMAP_EVIDENCE_ROOT="$(mktemp -d "$HOME/LocalData/shardloom/roadmap-XXXXXXXX")"
cargo metadata --offline --no-deps --format-version 1 > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/cargo-metadata.json"
```

The commands below use exact library or integration targets. `release-user-surfaces` includes the native read/write and compatibility-boundary features needed by these tests; reducing it to `vortex-local-primitives` would omit some writer tests. Unix-specific fixtures must run on Linux/macOS; separate OS support still requires PERF-12 evidence. Save each command's exit code and complete log. A Rust result with zero matching tests is a failed verification, even if Cargo exits zero. A missing or renamed selector must be repaired against the current source, never silently skipped.

Each owner must write `<owner>-acceptance.json` in this directory (lowercase owner name), containing the commit/source identities, completed substep IDs, exact commands and positive test counts, complete values/schema/certificate evidence, resource and failure/cleanup observations, protocol/sample identities, retain/drop decision where applicable, and unresolved obligations. Link its raw logs and UAT summaries by size and SHA-256. An evidence filename is a required deliverable, not a claim that its report already exists.

For nontrivial runtime changes, run the repository gates once at the coherent batch boundary and record `workspace-fmt.log`, `workspace-clippy.log`, and `workspace-tests.log`:

```sh
cargo fmt --all -- --check > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/workspace-fmt.log" 2>&1
cargo clippy --workspace --all-targets -- -D warnings > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/workspace-clippy.log" 2>&1
cargo test --workspace --all-targets > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/workspace-tests.log" 2>&1
```

Also run the applicable native/provider/public and installed-wheel matrices; default workspace gates do not enable every optional provider. New behavior needs positive, negative and composed-workflow evidence beyond these starting selectors. Performance acceptance must use the frozen complete-workload protocol and controls. Required capability completion is not conditional on a Full43 speed gain.

## PERF-01

Protect engine/sink/process clock separation and missing-clock refusal:

```sh
python3 scripts/test_clickbench_query_timing.py QueryTimingTests.test_disjoint_engine_sink_and_process_overhead_reconcile QueryTimingTests.test_missing_clocks_are_refused_unless_explicit_historical_comparison > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-01-timing.log" 2>&1
```

Deliver `perf-01-acceptance.json`, exclusive/overlapping span records and the raw cost/control samples. At a batch requiring new ingest/Full43 acceptance, also retain the guarded ingest `prepare_summary.json`, query `summary.json` and all complete outputs as specified under PERF-12. A clock-validator pass is not a new runtime observation.

## PERF-02

Protect prepared reuse and changed-source invalidation through the public worker:

```sh
cargo test -p shardloom-cli --features release-user-surfaces --test resident_worker worker_reuses_prepared_native_execution_and_rebinds_changed_requests -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-02-reuse.log" 2>&1
cargo test -p shardloom-cli --features release-user-surfaces --test resident_worker worker_rejects_replaced_source_then_reprepares_without_stale_answers -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-02-invalidation.log" 2>&1
```

Deliver `perf-02-acceptance.json` with first/repeated execution, exact invalidation and ownership records for each newly admitted family. Builder, physical-state and changed-source candidates additionally need complete construction/validation/use/eviction costs; the existing worker tests do not establish those optimizations.

## PERF-03

Protect actual decoder workspace ownership and grant admission before source opening:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib native_zstd_exact_workspace_grants_release_only_temporary_owners -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-03-codec.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib mismatched_cpu_or_memory_grant_rejects_single_and_partitioned_before_open -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-03-grants.log" 2>&1
```

Deliver `perf-03-acceptance.json` with the concrete new owner inventory, simultaneous/serial controls, allocation/release/denial counters, bounded queues, cancellation/drain and actual operator progress. Region work needs complete coverage and skew/ordered-handoff evidence. Native credits do not establish a process-RSS ceiling.

## PERF-04

Protect shared aggregate workers and complete exact dictionary-domain reductions:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib owned_count_complete_file_workers_admission_pressure_ties_offsets_and_fresh_state -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-04-count.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib owned_utf8_count_complete_dictionary_domains_ties_offsets_and_native_pressure_match_oracle -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-04-domains.log" 2>&1
```

Deliver `perf-04-acceptance.json` with complete outputs, stable reduction/overflow/NULL/tie behavior, actual kernel/merge work and utilization at 1/2/4/8/12 workers for the new state family. Existing COUNT coverage does not close additional measures or topology work.

## PERF-05

Protect exact cross-domain keys and budget-pressure refinement:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib filtered_native_utf8_dictionary_histogram_is_exact_across_domains_and_ties -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-05-domains.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib filtered_exact_histogram_budget_pressure_preserves_exact_refinement -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-05-pressure.log" 2>&1
```

Deliver `perf-05-acceptance.json` with decoded-reference comparisons for new nullable/composite/nested keys, collision controls, generic/direct transitions, source-domain identity, retained-owner release and actual pressure behavior. Preserve rejected strategy decisions.

## PERF-06

Protect existing external-memory ordering, distinct-set membership, finite
relational unary membership and admitted sampling populations with explicit
spill permission, and resident refusal without that permission:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib native_relational_spill_stable_multikey_complete_values_and_repeated_cleanup -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-spill.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_set_membership_growth_is_denied_without_spill_permission -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-membership-disabled.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_set_membership_spill_completes_exact_values_beyond_the_resident_grant -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-membership-enabled.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_set_membership_spill_keeps_a_single_hot_key_bounded -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-membership-skew.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_set_membership_fanout_keeps_native_replay_blocks_bounded -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-membership-fanout.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_set_membership_faults_cannot_complete_or_publish_partial_output -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-membership-faults.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_set_membership_writes_reject_existing_targets_before_input -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-membership-existing-output.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_set_membership_recovery_preserves_live_and_unknown_owners_then_restarts_exactly -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-membership-recovery.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_unary_membership_spills_at_the_original_grant -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-pressure.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib streaming_future_duplicate_masks_charge_population_identities_separately_from_keys -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-identities.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib unary_membership_spilled_typed_and_dictionary_keys_preserve_values_and_null_parents -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-typed.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib unary_membership_private_payload_movement_preserves_unobserved_float_values -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-payload.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib unary_membership_validates_open_cache_gap_and_both_merge_generations -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-generations.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib unary_membership_quota_grant_and_cancelled_merge_release_owned_state -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-quota.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib unary_membership_late_producer_schema_consumer_and_cancellation_never_complete -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-failures.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib unary_membership_recovery_preserves_live_unknown_and_foreign_owners_then_restarts -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-recovery.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib unary_membership_native_file_generation_failure_cleans_then_fresh_preparation_completes -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-unary-source-generation.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib sampling_ -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-sampling-family.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib sampling_population_spills_at_original_grant_with_exact_rank_and_draw_outputs -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-sampling-pressure.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib sampling_population_validates_open_cached_and_both_merge_generations -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-sampling-generations.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib sampling_population_recovery_preserves_live_unknown_and_foreign_owners_then_restarts -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-06-sampling-recovery.log" 2>&1
```

Deliver `perf-06-acceptance.json` proving exact completion when newly supported logical state exceeds its grant, including quota, skew, truncation/corruption, cancellation, crash/restart or actual resume, and owned cleanup. The [distinct-set contract](native-set-membership-spill-2026-10-18.md) covers `UNION`, `INTERSECT` and `EXCEPT`; the [unary membership contract](native-unary-membership-spill-2026-10-18.md) covers distinct and three deduplication/mask policies. The [sampling population contract](native-sampling-population-spill-2026-10-19.md) covers unknown-bound fractional nonreplacement and fixed/fractional replacement, weighted or unweighted. Fixed-count and known-bound fractional nonreplacement, subquery indexes and legacy standalone unary retain their separate resident boundaries. These process-death routes clean owned abandoned runs and execute fresh queries; they do not resume interrupted execution. Enabled/disabled regressions replace the corresponding prior combined refusal selectors.

The [distinct-set acceptance](../benchmarks/native-set-membership-spill-2026-10-18.md) records 4,911 public cases. Subsequent [unary acceptance](../benchmarks/native-unary-membership-spill-2026-10-18.md) records 5,126 public cases, 274 independently reopened unary value archives, complete Full43 results and 704 native format workflows. Both bind the actual compiled source to their corrected test harnesses by exact source equality and preserve all failed observations. The later [sampling acceptance](../benchmarks/native-sampling-population-spill-2026-10-19.md) uses one frozen compiled/harness source, passes 5,492 public cases and independently reopens 406 sampling value archives, complete Full43 results and 704 native format workflows. Separate writer/readback grants do not replace the original 16/8-MiB unary or 16-MiB sampling pressure controls. None of these finite acceptances closes a broader PERF-06 substep. Freeze separate reconstruction/merge protocols for conditional experiments.

## PERF-07

Protect complete retained native output and failure-safe publication foundations:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib owned_count_native_sink_preserves_all_values_after_source_drop_and_existing_destination -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-07-retention.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib corrupt_or_cancelled_publication_removes_owned_staging_and_preserves_generation -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-07-publication.log" 2>&1
```

Deliver `perf-07-acceptance.json` with complete write/reopen values, source/result drop and slice lifetime, copy/decode counters, late failure/cancel cleanup, competing-writer and generation-conditional replacement cases. Distinguish single-file visibility/durability from multi-output recovery or group atomicity. Existing new-target publication does not prove replacement.

## PERF-08

Protect exact nullable text ingestion and conservative payload avoidance:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib text_zone_writer_reopens_exact_nullable_values_and_filtered_zone_boundaries -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-08-reopen.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib text_filter_pruning_skips_unprojected_disjoint_and_null_only_payloads_with_conservative_utf8_bounds -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-08-pruning.log" 2>&1
```

Deliver `perf-08-acceptance.json` with the measured repeated representation/traversal, exact unique-transform/code-replay counts, source preflight, complete derived-value parity and non-overlapping CPU/wait attribution. Retained ingest changes require the guarded lifecycle in PERF-12.

## PERF-09

Protect bounded writer handoff and durable exact data:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib shared_slot_preserves_startup_steady_empty_and_eof_bounds -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-09-slots.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib text_zone_writer_reopens_exact_nullable_values_and_filtered_zone_boundaries -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-09-reopen.log" 2>&1
```

Deliver `perf-09-acceptance.json`, per-column/whole-artifact identities and sizes, transient ownership and ordered EOF/cancel/publication evidence, plus complete first/repeated downstream results. Apply the retained writer's original ingest/size/query gate; rejected text profiles require a materially different mechanism before reconsideration.

## PERF-10

Protect selection semantics, metadata-only work and the shared public route:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib native_selection_preserves_values_nulls_and_row_order -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-10-selection.log" 2>&1
cargo test -p shardloom-vortex --features release-user-surfaces --lib count_where_metadata_physical_kernels_evaluate_without_io -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-10-metadata.log" 2>&1
cargo test -p shardloom-cli --features release-user-surfaces --test public_workflow_route public_route_admits_native_vortex_filter_project_payload -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-10-public-route.log" 2>&1
```

Deliver `perf-10-acceptance.json` with complete native/reference parity, negative pruning controls, changed plan/type boundaries, actual avoided rows/bytes and public-surface agreement. New summary, join, index or fusion strategies need their own exact semantics and matched workload controls.

The [compact-selection experiment](native-compact-selection-2026-10-18.md)
retains the exact ignored selector `native_selection_complete_cost_screen`.
It covers seven shapes, native projection/count consumers, both projected fields,
certificate checks and complete reservation cleanup. Execute it only through a
frozen, serial, guarded release comparison:

```sh
cargo test --release -p shardloom-vortex --features release-user-surfaces --lib native_selection_complete_cost_screen -- --ignored --nocapture --test-threads=1
```

The command alone does not establish an optimization. Preserve byte-identical
measurement source, source/binary/protocol identities, every sample and the
predeclared retain/drop gates. The October 18 contiguous-range candidate was
dropped after its permitted confirmation repeated projection regressions;
production and the existing regression selectors above are restored. The
candidate-specific range tests are archived with the rejected source. Do not
rerun that unchanged candidate or treat its correctness passes as retention.

## PERF-11

Protect real streaming resource lifetimes and public nullable write/reopen:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib result_stream_consumer_error_cancellation_and_retention_keep_resource_ownership -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-11-consumer.log" 2>&1
cargo test -p shardloom-cli --features release-user-surfaces --test public_workflow_route public_native_array_sink_reopens_full_nullable_projection_and_filter_values -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-11-public-reopen.log" 2>&1
```

Deliver `perf-11-acceptance.json` with declared input/state/output/queue grants, complete compositions, slow/failed consumers and late input failures, plus separate latency distributions for newly admitted native/worker/Python/fresh-process paths. Preserve the identities and lifetime of caller-owned inputs.

## PERF-12

Protect the complete-suite versus targeted/cohort distinction, and inspect current scope/claim blockers:

```sh
python3 scripts/test_clickbench_query_uat.py ClickBenchUatTests.test_full_suite_default_executes_each_query_once_and_has_no_hot_score ClickBenchUatTests.test_targeted_results_cannot_be_scored_as_complete_full_suite > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-12-harness.log" 2>&1
python3 scripts/check_v1_inclusion_scope.py --output "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-12-v1-scope.json"
python3 scripts/check_runtime_gap_family_burn_down.py --output "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-12-runtime-gaps.json"
python3 scripts/check_compute_engine_completion_gate.py --allow-incomplete --output "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-12-completion-inspection.json"
python3 scripts/check_release_architecture_tracker.py --allow-blocked --output "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-12-release-inspection.json"
```

The last two commands inspect blockers; their successful exit is not completion or release acceptance. Claim promotion requires the same gates without inspection allowances and the corresponding certification/platform evidence.

For a batch requiring ingestion and query UAT, freeze actual binary, input, resource and independent-reference paths in `perf-12-protocol.json` before execution. Use `scripts/run_clickbench_ingest_uat.sh` for replacement ingestion and `scripts/run_clickbench_query_uat.py --repeats 1` for a standard Full43; retain their complete invoked argument vectors, `prepare_summary.json`, `summary.json`, complete values and source/child-process checks. The [latest accepted packet](../benchmarks/native-roadmap-fresh-uat-2026-10-18.md) contains the executable frozen launch/verification helpers and exact protocol, including `run_native_relational_suite.py` for the 18-cohort public matrix. Adapt its declared paths/source identity to the new batch; do not reuse its results as new execution. Comparative repeats, topology experiments and format changes retain their separate frozen protocols.

Deliver `perf-12-acceptance.json` with complete native/independent-reference records, source/build/archive identities, supported platform/runtime envelopes, failure/recovery and install/upgrade/rollback proof. A harness test, build or inspection-only exit cannot substitute for these outputs.

## PERF-13

Protect failed-training refusal and frozen binary/profile inputs before any conditional PGO experiment:

```sh
python3 scripts/test_build_shardloom_pgo.py PgoTests.test_failed_training_never_merges_or_builds_profile_use PgoTests.test_three_frozen_binaries_flags_and_fresh_merge_inputs > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/perf-13-orchestration.log" 2>&1
```

Deliver `perf-13-acceptance.json` with either the evidenced prerequisite/drop decision or representative/held-out training, build/profile/binary provenance, portable-package correctness, full compilation/transfer break-even and matched complete-workflow results. These orchestration tests do not run or certify PGO.

## NATIVE-ENCODED-EXCHANGE

The first executable slice is the pinned-provider protocol/ownership review, before a transport implementation. Preserve local transfer foundations and validate its plan/scope references:

```sh
cargo test -p shardloom-vortex --features release-user-surfaces --lib encoded_transfer_preserves_complete_values_validity_and_physical_trees -- --nocapture > "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/native-encoded-exchange-local-foundation.log" 2>&1
python3 scripts/check_v1_inclusion_scope.py --output "$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/native-encoded-exchange-scope.json"
```

Deliver `native-encoded-exchange-protocol.md` and `native-encoded-exchange-acceptance.json` with pinned provider/license/security decisions, negotiated representation/identity rules, queue/receiver/dictionary grants and deterministic unsupported boundaries. No current two-process transport or independent Flight SQL client test exists. The protocol slice must register the concrete test targets, exact commands and `native-encoded-exchange-two-process.json` / `native-encoded-exchange-client-interop.json` outputs before admitting the implementation experiment. Keep experiment/client substeps unchecked until those tests execute real downstream native operations and prove malformed input, isolation, cancellation/truncation/reconnect, lifetime and complete matched-control results. Local transfer success is not transport acceptance.
