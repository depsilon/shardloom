use std::fs;
use std::path::PathBuf;

#[test]
fn traditional_benchmark_harness_lists_all_required_engines() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    assert!(script.contains(
        "ENGINE_ORDER = (\"shardloom\", \"pandas\", \"polars\", \"duckdb\", \"spark\", \"datafusion\", \"dask\")"
    ));
    assert!(script.contains("\"fallback_execution_allowed\": False"));
    assert!(script.contains("\"external_engines_are_fallback\": False"));
    assert!(script.contains("\"performance_claim_allowed\": False"));
    assert!(script.contains("def render_markdown_report("));
    assert!(script.contains("def render_fairness_parameters("));
    assert!(script.contains("def render_read_this_first("));
    assert!(script.contains("def render_shardloom_native_table("));
    assert!(script.contains("def render_universal_io_table("));
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
        "\"dask_blocksize\"",
        "\"dask_scheduler\"",
        "\"spark_requires_java\"",
        "\"object_store_included\": False",
        "\"csv_to_vortex_included\": False",
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
    assert!(normalized.contains("never execute unsupported ShardLoom plans as fallback engines"));
    assert!(
        readme
            .contains("ShardLoom traditional analytics rows are expected to report `unsupported`")
    );
    assert!(readme.contains("--scenario \"group by aggregation\""));
    assert!(readme.contains("--include-stress"));
    assert!(readme.contains("scale stress multi-stage etl"));
    assert!(readme.contains("Spark-style engines"));
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
