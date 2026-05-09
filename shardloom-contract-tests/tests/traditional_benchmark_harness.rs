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
    assert!(script.contains("def render_universal_io_table("));
    assert!(script.contains("def render_resource_metrics_table("));
    assert!(script.contains("def render_shardloom_effects_table("));
    assert!(script.contains("def warmup_runner("));
    assert!(script.contains("\"startup_time_millis\""));
    assert!(script.contains("\"bytes_written\""));
    assert!(script.contains("\"shardloom_evidence\""));
    assert!(script.contains("traditional-analytics-run"));
    assert!(script.contains("vortex-traditional-analytics-benchmark"));
    assert!(script.contains("--shardloom-build-profile"));
    assert!(script.contains("failed_result("));
}

#[test]
fn traditional_benchmark_harness_covers_core_and_stress_scenarios() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    for scenario in [
        "csv/file ingest",
        "selective filter",
        "group by aggregation",
        "sort and top-k",
        "hash join",
        "wide projection",
        "distinct count",
        "scale stress skewed join aggregation",
        "scale stress multi-stage etl",
    ] {
        assert!(script.contains(scenario), "missing scenario {scenario}");
    }
    assert!(script.contains("--include-stress"));
}

#[test]
fn traditional_benchmark_harness_records_fairness_and_universal_io_boundaries() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    for required_text in [
        "\"cache_mode\"",
        "\"timing_scope\"",
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
        "\"csv_to_vortex_included\": True",
        "\"shardloom_universal_io_smoke_included\": True",
        "\"claim_grade_requirements\"",
        "CSV -> ShardLoom NativeWorkStream -> Vortex",
        "CSV -> Vortex import -> encoded CountAll",
        "NativeIoCertificate",
    ] {
        assert!(
            script.contains(required_text),
            "missing required benchmark fairness text: {required_text}"
        );
    }
}

#[test]
fn traditional_benchmark_harness_includes_shardloom_native_microbenchmark() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    assert!(script.contains("def run_shardloom_native_microbenchmarks("));
    assert!(script.contains("vortex-count-benchmark"));
    assert!(script.contains("metadata_footer_u64_20000.vortex"));
    assert!(script.contains("\"data_decoded\""));
    assert!(script.contains("\"data_materialized\""));
    assert!(script.contains("\"fallback_attempted\""));
}

#[test]
fn traditional_benchmark_docs_state_no_fallback_and_markdown_outputs() {
    let readme = read_workspace_file("benchmarks/traditional_analytics/README.md");
    let normalized = readme.replace('\n', " ");

    assert!(readme.contains("human-readable Markdown"));
    assert!(readme.contains("fairness parameters"));
    assert!(readme.contains("resource metrics"));
    assert!(readme.contains("runtime-effect evidence"));
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
