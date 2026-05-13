use std::fs;
use std::path::PathBuf;

#[test]
fn traditional_benchmark_harness_lists_all_required_engines() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    assert!(script.contains("\"spark-default\""));
    assert!(script.contains("\"spark-local-tuned\""));
    assert!(
        script.contains("ENGINE_ALIASES = {\"spark\": (\"spark-default\", \"spark-local-tuned\")}")
    );
    assert!(script.contains("\"fallback_execution_allowed\": False"));
    assert!(script.contains("\"external_engines_are_fallback\": False"));
    assert!(script.contains("\"performance_claim_allowed\": False"));
    assert!(script.contains("def render_markdown_report("));
    assert!(script.contains("def render_fairness_parameters("));
    assert!(script.contains("def render_read_this_first("));
    assert!(script.contains("def render_shardloom_native_table("));
    assert!(script.contains("def render_shardloom_why_table("));
    assert!(script.contains("def render_shardloom_work_avoidance_table("));
    assert!(script.contains("def render_shardloom_commit_table("));
    assert!(script.contains("def render_universal_io_table("));
    assert!(script.contains("def render_resource_metrics_table("));
    assert!(script.contains("def render_shardloom_effects_table("));
    assert!(script.contains("def render_coverage_table("));
    assert!(script.contains("def warmup_runner("));
    assert!(script.contains("\"startup_time_millis\""));
    assert!(script.contains("\"bytes_written\""));
    assert!(script.contains("\"shardloom_evidence\""));
    assert!(script.contains("\"native_io_certificate_status\""));
    assert!(script.contains("\"native_io_certificate_path_id\""));
    assert!(script.contains("\"native_io_per_path_certificate_emitted\""));
    assert!(script.contains("\"native_io_materializing_transitions_have_boundaries\""));
    assert!(script.contains("SHARDLOOM_CLAIM_GRADE_REQUIRED_EVIDENCE"));
    assert!(script.contains("def claim_grade_readiness("));
    assert!(script.contains("\"claim_gate_status\""));
    assert!(script.contains("\"claim_grade_requirements_met\""));
    assert!(script.contains("\"claim_grade_missing_evidence\""));
    assert!(script.contains("\"timing_row_claim_grade\""));
    assert!(script.contains("\"write_timing_present\""));
    assert!(script.contains("\"computed_result_sink_write_millis\""));
    assert!(script.contains("\"scenario_compute_millis\""));
    assert!(script.contains("\"reproducible_benchmark_row\""));
    assert!(script.contains("\"correctness_digest_stable\""));
    assert!(script.contains("MIN_CLAIM_GRADE_ITERATIONS = 3"));
    assert!(script.contains("--claim-readiness-rerun"));
    assert!(script.contains("def render_shardloom_result_sink_table("));
    assert!(script.contains("\"fixture_smoke_only\""));
    assert!(script.contains("\"not_claim_grade\""));
    assert!(script.contains("\"claim_grade\""));
    assert!(script.contains("CORRECTNESS_FLOAT_DIGITS = 4"));
    assert!(script.contains("\"status\", \"--short\", \"--untracked-files=no\""));
    assert!(script.contains("traditional-analytics-run"));
    assert!(script.contains("vortex-traditional-analytics-benchmark"));
    assert!(script.contains("--shardloom-build-profile"));
    assert!(script.contains("failed_result("));
    assert!(script.contains("\"very_wide_table\""));
    assert!(script.contains("\"null_heavy\""));
    assert!(script.contains("\"many_small_files\""));
    assert!(script.contains("\"few_large_files\""));
    assert!(script.contains("\"partitioned_by_date\""));
    assert!(script.contains("\"poorly_clustered\""));
    assert!(script.contains("\"well_clustered\""));
    assert!(script.contains("\"schema_drift\""));
    assert!(script.contains("\"dirty_csv\""));
    assert!(script.contains("\"nested_json\""));
    assert!(script.contains("\"cdc_delta_overlay\""));
    assert!(script.contains("\"dataset_file_shape\""));
    assert!(script.contains("\"fact_csv_part_count\""));
    assert!(script.contains("\"cdc_delta_csv\""));
    assert!(script.contains("\"nested_jsonl\""));
    assert!(script.contains("def generated_fact_extra_columns("));
    assert!(script.contains("def generated_extra_fact_values("));
    assert!(script.contains("def write_profile_sidecars("));
    assert!(script.contains("def write_csv_parts("));
    assert!(script.contains("def write_cdc_delta_overlay("));
    assert!(script.contains("def write_nested_json_fixture("));
    assert!(script.contains("def clean_cast_filter_write("));
    assert!(script.contains("scenario_outputs"));
    assert!(script.contains("def scenario_dataset_profile_block_reason("));
    assert!(script.contains("requires dataset_profile in"));
    assert!(script.contains("SHARDLOOM_TRADITIONAL_SCENARIOS"));
    assert!(script.contains("does not implement benchmark scenario"));
}

#[test]
fn traditional_benchmark_harness_covers_core_and_stress_scenarios() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    for scenario in [
        "csv/file ingest",
        "selective filter",
        "filter + projection + limit",
        "group by aggregation",
        "multi-key group by",
        "sort and top-k",
        "hash join",
        "join + aggregate",
        "row number window",
        "partition pruning",
        "many-small-files scan",
        "null-heavy aggregate",
        "high-cardinality string group/distinct",
        "top-N per group",
        "clean/cast/filter/write",
        "malformed timestamp / dirty CSV",
        "small change over large base",
        "nested JSON field scan",
        "wide projection",
        "distinct count",
        "scale stress skewed join aggregation",
        "scale stress multi-stage etl",
    ] {
        assert!(script.contains(scenario), "missing scenario {scenario}");
    }
    assert!(script.contains("--include-stress"));
    assert!(script.contains("--include-taxonomy-extra"));
    assert!(script.contains("scenario_catalog.json"));
}

#[test]
fn traditional_benchmark_harness_records_fairness_and_universal_io_boundaries() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    for required_text in [
        "\"cache_mode\"",
        "\"timing_scope\"",
        "\"benchmark_suite\"",
        "\"scenario_id\"",
        "\"scenario_category\"",
        "\"dataset_profile\"",
        "\"engine_role\"",
        "\"benchmark_constitution\"",
        "\"coverage_table\"",
        "\"claim_gate_status\"",
        "\"claim_grade_requirements_met\"",
        "\"claim_grade_missing_evidence\"",
        "\"timing_row_claim_grade\"",
        "\"write_timing_present\"",
        "\"computed_result_sink_write_millis\"",
        "\"scenario_compute_millis\"",
        "\"reproducible_benchmark_row\"",
        "\"correctness_digest_stable\"",
        "\"claim_readiness_rerun_profile\"",
        "\"taxonomy_extra_included\"",
        "\"stress_lane_included\"",
        "\"shardloom_build_profile\"",
        "\"shardloom_build_time_excluded\"",
        "\"dask_blocksize\"",
        "\"dask_scheduler\"",
        "\"spark_requires_java\"",
        "\"spark_profiles\"",
        "\"startup_time_millis\"",
        "\"bytes_written\"",
        "\"data_decoded\"",
        "\"data_materialized\"",
        "\"object_store_included\": False",
        "\"compatibility_to_vortex_included\": True",
        "\"resource_auto_sizing_enabled\"",
        "\"partitioning_auto_derived\"",
        "\"target_partition_count\"",
        "\"shardloom_universal_io_smoke_included\": True",
        "\"claim_grade_requirements\"",
        "CSV/JSONL/Parquet/Arrow IPC/Avro/ORC -> NativeWorkStream -> Vortex",
        "Compatibility source -> Vortex import -> encoded CountAll",
        "NativeIoCertificate",
        "SourceCapabilityReport",
        "AdapterFidelityReport",
        "MaterializationBoundaryReport",
    ] {
        assert!(
            script.contains(required_text),
            "missing required benchmark fairness text: {required_text}"
        );
    }
}

#[test]
fn traditional_benchmark_catalog_declares_taxonomy_and_planned_profiles() {
    let catalog = read_workspace_file("benchmarks/common/scenario_catalog.json");

    for required_text in [
        "\"schema_version\": \"shardloom.benchmark.scenario_catalog.v1\"",
        "\"local_analytics\"",
        "\"scan_and_pruning\"",
        "\"projection_and_layout\"",
        "\"aggregation\"",
        "\"joins\"",
        "\"sort_and_window\"",
        "\"etl_write\"",
        "\"messy_lakehouse_data\"",
        "\"incremental_state\"",
        "\"tiny_smoke\"",
        "\"narrow_fact_dim\"",
        "\"skewed_keys\"",
        "\"wide_table\"",
        "\"very_wide_table\"",
        "\"null_heavy\"",
        "\"partitioned_by_date\"",
        "\"poorly_clustered\"",
        "\"well_clustered\"",
        "\"many_small_files\"",
        "\"few_large_files\"",
        "\"schema_drift\"",
        "\"dirty_csv\"",
        "\"nested_json\"",
        "\"cdc_delta_overlay\"",
        "\"filter + projection + limit\"",
        "\"multi-key group by\"",
        "\"join + aggregate\"",
        "\"row number window\"",
        "\"partition pruning\"",
        "\"many-small-files scan\"",
        "\"null-heavy aggregate\"",
        "\"high-cardinality string group/distinct\"",
        "\"top-N per group\"",
        "\"clean/cast/filter/write\"",
        "\"malformed timestamp / dirty CSV\"",
        "\"small change over large base\"",
        "\"nested JSON field scan\"",
        "\"executable\": true",
        "\"Photon\"",
        "\"Microsoft Fabric\"",
        "\"Snowflake\"",
    ] {
        assert!(
            catalog.contains(required_text),
            "missing required benchmark catalog text: {required_text}"
        );
    }
}

#[test]
fn traditional_benchmark_harness_includes_shardloom_native_microbenchmark() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    assert!(script.contains("def run_shardloom_native_microbenchmarks("));
    assert!(script.contains("def run_shardloom_count_microbenchmark("));
    assert!(script.contains("def run_shardloom_vortex_run_microbenchmark("));
    assert!(script.contains("def first_meaningful_field("));
    assert!(script.contains("vortex-count-benchmark"));
    assert!(script.contains("vortex-run"));
    assert!(script.contains("metadata_footer_u64_20000.vortex"));
    assert!(script.contains("local primitive projection"));
    assert!(script.contains("local primitive count"));
    assert!(script.contains("\"work_avoided_metrics\""));
    assert!(script.contains("\"work_avoided_decode_avoided\""));
    assert!(script.contains("\"work_avoided_materialization_avoided\""));
    assert!(script.contains("\"work_avoided_segments_pruned\""));
    assert!(script.contains("\"work_avoided_bytes_not_read\""));
    assert!(script.contains("\"why_primary_reason\""));
    assert!(script.contains("\"why_blockers\""));
    assert!(script.contains("\"decision_trace_entries\""));
    assert!(script.contains("ShardLoom Decision / Why Evidence"));
    assert!(script.contains("ShardLoom Work-Avoidance Evidence"));
    assert!(script.contains("def run_shardloom_commit_microbenchmark("));
    assert!(script.contains("def prepare_shardloom_commit_workspace("));
    assert!(script.contains("local commit manifest"));
    assert!(script.contains("vortex-local-commit-execute"));
    assert!(script.contains("\"write_commit_latency_micros\""));
    assert!(script.contains("\"commit_executed\""));
    assert!(script.contains("ShardLoom Write/Commit Evidence"));
    assert!(script.contains("project:value"));
    assert!(script.contains("local primitive validity count"));
    assert!(script.contains("count-where:is_not_null:value"));
    assert!(script.contains("local primitive comparison count"));
    assert!(script.contains("count-where:gte:value:10000"));
    assert!(script.contains("local primitive filter projection"));
    assert!(script.contains("filter-project:gte:value:10000|value"));
    assert!(script.contains("\"timing_scope\""));
    assert!(script.contains("\"materialization_boundary_reported\""));
    assert!(script.contains("\"data_decoded\""));
    assert!(script.contains("\"data_materialized\""));
    assert!(script.contains("\"fallback_attempted\""));
}

#[test]
fn traditional_benchmark_docs_state_no_fallback_and_markdown_outputs() {
    let readme = read_workspace_file("benchmarks/traditional_analytics/README.md");
    let normalized = readme.replace('\n', " ");

    assert!(readme.contains("human-readable Markdown"));
    assert!(readme.contains("coverage_table"));
    assert!(readme.contains("fairness parameters"));
    assert!(readme.contains("resource metrics"));
    assert!(readme.contains("runtime-effect evidence"));
    assert!(readme.contains("DecisionTrace/WhyReport evidence"));
    assert!(readme.contains("work-avoidance evidence"));
    assert!(readme.contains("write/commit evidence"));
    assert!(readme.contains("result-sink write timing"));
    assert!(readme.contains("per-path certificate id/status"));
    assert!(readme.contains("row_read=true"));
    assert!(normalized.contains("never execute unsupported ShardLoom plans as fallback engines"));
    assert!(readme.contains("shardloom traditional-analytics-run"));
    assert!(readme.contains("vortex-traditional-analytics-benchmark"));
    assert!(readme.contains("universal-I/O smoke path"));
    assert!(readme.contains("--shardloom-build-profile"));
    assert!(readme.contains("--scenario \"group by aggregation\""));
    assert!(readme.contains("--include-stress"));
    assert!(readme.contains("scale stress multi-stage etl"));
    assert!(readme.contains("Spark-style engines"));
    assert!(readme.contains("spark-default"));
    assert!(readme.contains("spark-local-tuned"));
    assert!(readme.contains("startup/warmup time"));
    assert!(readme.contains("Dask is sensitive to partitioning"));
    assert!(readme.contains("rounded to four decimal places"));
    assert!(readme.contains("appends `-dirty`"));
    assert!(readme.contains("`vortex-run` primitive evidence"));
    assert!(normalized.contains("final `vortex-run` runtime effects"));
    assert!(readme.contains("decision-trace counts"));
    assert!(readme.contains("claim blockers"));
    assert!(readme.contains("Segment-prune and bytes-not-read values remain `unknown`"));
    assert!(readme.contains("average commit latency"));
    assert!(readme.contains("object-store commit"));
    assert!(readme.contains("timing scope"));
    assert!(readme.contains("benchmarks\\traditional_analytics\\.venv\\Scripts\\python"));
}

fn read_workspace_file(relative: &str) -> String {
    fs::read_to_string(workspace_root().join(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("contract tests crate is in workspace root")
        .to_path_buf()
}
