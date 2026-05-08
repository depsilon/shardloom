use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, SegmentStats, StatValue};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexColumnMetadataSummary, VortexFileMetadataSummary, VortexLocalExecutionStatus,
    VortexLocalExecutionValue, VortexMetadataSummaryReport, VortexMetadataSummaryStatus,
    VortexQueryPrimitiveRequest, VortexQueryPrimitiveStatus, VortexQueryPrimitiveValue,
    VortexSegmentMetadataSummary, evaluate_vortex_query_primitive,
    execute_vortex_local_query_primitive,
};

fn uri() -> DatasetUri {
    DatasetUri::new("file://fixtures/cg5-metadata.vortex").expect("uri")
}

fn summary() -> VortexMetadataSummaryReport {
    VortexMetadataSummaryReport {
        status: VortexMetadataSummaryStatus::Summarized,
        summary: VortexFileMetadataSummary::empty(),
        diagnostics: vec![],
    }
}

fn stats_segment(
    row_count: u64,
    column: &str,
    stats: SegmentStats,
) -> VortexSegmentMetadataSummary {
    let mut segment = VortexSegmentMetadataSummary::unknown().with_row_count(row_count);
    segment.add_column(
        VortexColumnMetadataSummary::new(ColumnRef::new(column).expect("column"))
            .with_stats(stats)
            .with_statistics_available(true),
    );
    segment
}

fn assert_no_effects(report: &shardloom_vortex::VortexLocalExecutionReport) {
    assert!(report.is_side_effect_free());
    assert!(!report.tasks_executed);
    assert!(!report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.object_store_io);
    assert!(!report.write_io);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn count_all_uses_file_row_count_without_data_read() {
    let mut metadata = summary();
    metadata.summary.row_count = Some(42);

    let report = execute_vortex_local_query_primitive(
        VortexQueryPrimitiveRequest::count_all(uri()),
        Some(metadata),
    )
    .expect("local execution");

    assert_eq!(report.status, VortexLocalExecutionStatus::MetadataExecuted);
    assert_eq!(
        report.value,
        VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(42))
    );
    assert_no_effects(&report);
}

#[test]
fn count_all_sums_segment_rows_when_file_row_count_is_missing() {
    let mut metadata = summary();
    metadata.summary.segments = vec![
        VortexSegmentMetadataSummary::unknown().with_row_count(7),
        VortexSegmentMetadataSummary::unknown().with_row_count(13),
    ];

    let report = execute_vortex_local_query_primitive(
        VortexQueryPrimitiveRequest::count_all(uri()),
        Some(metadata),
    )
    .expect("local execution");

    assert_eq!(report.status, VortexLocalExecutionStatus::MetadataExecuted);
    assert_eq!(
        report.value,
        VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(20))
    );
    assert_no_effects(&report);
}

#[test]
fn count_where_metadata_false_returns_zero_without_scan() {
    let mut metadata = summary();
    let mut stats = SegmentStats::unknown();
    stats.null_count = Some(0);
    metadata.summary.segments = vec![stats_segment(9, "x", stats)];

    let report = execute_vortex_local_query_primitive(
        VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNull {
                column: ColumnRef::new("x").expect("column"),
            },
        ),
        Some(metadata),
    )
    .expect("local execution");

    assert_eq!(report.status, VortexLocalExecutionStatus::MetadataExecuted);
    assert_eq!(
        report.value,
        VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(0))
    );
    assert_no_effects(&report);
}

#[test]
fn count_where_metadata_true_sums_matching_segment_rows() {
    let mut metadata = summary();
    let mut stats_a = SegmentStats::unknown();
    stats_a.null_count = Some(0);
    let mut stats_b = SegmentStats::unknown();
    stats_b.null_count = Some(0);
    metadata.summary.segments = vec![
        stats_segment(2, "x", stats_a),
        stats_segment(3, "x", stats_b),
    ];

    let report = execute_vortex_local_query_primitive(
        VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").expect("column"),
            },
        ),
        Some(metadata),
    )
    .expect("local execution");

    assert_eq!(report.status, VortexLocalExecutionStatus::MetadataExecuted);
    assert_eq!(
        report.value,
        VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(5))
    );
    assert_no_effects(&report);
}

#[test]
fn count_where_inconclusive_metadata_defers_to_encoded_predicate_without_fallback() {
    let mut metadata = summary();
    metadata.summary.segments = vec![VortexSegmentMetadataSummary::unknown().with_row_count(5)];

    let report = execute_vortex_local_query_primitive(
        VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::Compare {
                column: ColumnRef::new("x").expect("column"),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(7),
            },
        ),
        Some(metadata),
    )
    .expect("local execution");

    assert_eq!(
        report.status,
        VortexLocalExecutionStatus::NeedsPredicateEvaluation
    );
    assert_eq!(report.value, VortexLocalExecutionValue::Deferred);
    assert_no_effects(&report);
}

#[test]
fn projection_known_columns_requires_projection_without_materialization() {
    let mut metadata = summary();
    let mut segment = VortexSegmentMetadataSummary::unknown().with_row_count(5);
    segment.add_column(VortexColumnMetadataSummary::new(
        ColumnRef::new("x").expect("column"),
    ));
    metadata.summary.segments = vec![segment];

    let result = evaluate_vortex_query_primitive(
        VortexQueryPrimitiveRequest::project(
            uri(),
            ProjectionRequest::columns(vec![ColumnRef::new("x").expect("column")]),
        ),
        &metadata,
    )
    .expect("query primitive");

    assert_eq!(result.status, VortexQueryPrimitiveStatus::NeedsProjection);
    assert!(result.is_side_effect_free());
    assert!(!result.data_read);
    assert!(!result.data_decoded);
    assert!(!result.data_materialized);
    assert!(!result.fallback_execution_allowed);
}
