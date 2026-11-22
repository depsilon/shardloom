//! Extracted unit test suite for `local_primitives` (`E2E-ARCH-01`).

use super::*;

// This test-only entry point declares the allocation used by these fixtures.
fn execute_vortex_local_primitive(
    request: &VortexQueryPrimitiveRequest,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    super::execute_vortex_local_primitive(
        request,
        shardloom_core::ExecutionResources::from_gib(
            4,
            1,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .expect("explicit fixture allocation"),
    )
}
#[cfg(feature = "universal-format-io")]
use crate::VortexStructuredProjectionColumn;
use crate::{
    VortexAggregateOrderExpr, VortexEncodedValuePredicateBatch, VortexExplodeProjectionRequest,
    VortexExpressionProjectionRequest, VortexExpressionRewrite,
    VortexReaderBackedEncodedExecutionStatus, VortexReaderGeneratedPreparedBatchStatus,
    VortexSourceBackedEncodedValuePredicateBatch,
    execute_vortex_reader_backed_filter_from_encoded_value_batches,
};
use shardloom_core::{
    ColumnRef, CorrectnessFixture, CorrectnessValidationPlan, EncodedSegment, EncodedValueBatch,
    EncodedValueRun, EncodingKind, ExecutionCertificateStatus, LayoutKind, LogicalDType,
    Nullability, SegmentId, SegmentLayout, SegmentStats, UniversalInputSource,
};

fn parsed_jsonl_rows(rows: &str) -> serde_json::Value {
    // JSON object field order is not part of the result; row order is.
    serde_json::Value::Array(
        rows.lines()
            .map(|line| serde_json::from_str(line).expect("complete JSON row"))
            .collect(),
    )
}

pub(super) fn unique_vortex_path(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shardloom-{name}-{}-{nanos}.vortex",
        std::process::id()
    ))
}

#[test]
fn local_vortex_worker_count_is_bounded_before_spawning() {
    let capacity = shardloom_exec::compute_pool::bounded_cpu_parallelism(usize::MAX) - 1;

    assert_eq!(bounded_local_vortex_worker_count(0), 0);
    assert_eq!(bounded_local_vortex_worker_count(1), 0);
    assert_eq!(
        bounded_local_vortex_worker_count(capacity.saturating_add(1)),
        capacity
    );
    assert!(bounded_local_vortex_worker_count(usize::MAX) <= capacity);
}

#[test]
fn physical_policy_classifies_stateless_count_without_state_knobs() {
    let uri = DatasetUri::new("file:///tmp/policy-count.vortex").expect("uri");
    let request = VortexQueryPrimitiveRequest::count_all(uri);
    let policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("resource policy");
    let (_effective, report) = policy.with_physical_policy_for_request(&request);

    assert_eq!(report.route_family, "stateless_scan_count");
    assert_eq!(
        report.state_pressure_reason,
        "metadata_or_scan_pushdown_without_unbounded_state"
    );
    assert!(
        report
            .rejected_alternatives
            .contains(&"state_heavy_hitter_capacity".to_string())
    );
    assert!(!report.row_ref_retention_enabled);
}

#[test]
fn physical_policy_classifies_bounded_sort_as_row_ref_topk() {
    let uri = DatasetUri::new("file:///tmp/policy-sort.vortex").expect("uri");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        uri,
        ProjectionRequest::all(),
        None,
        VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new("EventTime", true)]),
        100,
    );
    let policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("resource policy");
    let (_effective, report) = policy.with_physical_policy_for_request(&request);

    assert_eq!(report.route_family, "row_ref_sort_topk");
    assert!(report.row_ref_retention_enabled);
    assert!(
        report
            .rejected_alternatives
            .contains(&"full_payload_materialization_before_topk".to_string())
    );
}

#[test]
fn physical_policy_writer_sink_overlay_preserves_route_family() {
    let uri = DatasetUri::new("file:///tmp/policy-writer.vortex").expect("uri");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("EventTime").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(0),
        },
        ProjectionRequest::all(),
    );
    let policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("resource policy");
    let (_effective, report) = policy.with_writer_sink_physical_policy_for_request(&request);

    assert_eq!(report.route_family, "stateless_scan_count");
    assert!(report.writer_coalescing_enabled);
    assert!(
        report
            .rejected_alternatives
            .contains(&"uncoalesced_writer_sink_flushes".to_string())
    );
}

#[test]
fn physical_policy_classifies_string_and_numeric_utf8_heavy_hitters() {
    let uri = DatasetUri::new("file:///tmp/policy-heavy-hitter.vortex").expect("uri");
    let string_aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("URL").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("rows", true)]);
    let string_request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), string_aggregate)
            .with_source_order_limit(10);
    let policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("resource policy");
    let (string_effective, string_report) =
        policy.with_physical_policy_for_request(&string_request);
    assert_eq!(string_report.route_family, "string_heavy_hitter_topk");
    assert_eq!(
        string_effective
            .resource_envelope
            .string_topk_heavy_hitter_capacity,
        VortexLocalPrimitiveResourceEnvelope::COUNT_ONLY_STRING_HEAVY_HITTER_CAPACITY
    );
    assert_eq!(
        string_report.selected_string_topk_heavy_hitter_capacity,
        VortexLocalPrimitiveResourceEnvelope::COUNT_ONLY_STRING_HEAVY_HITTER_CAPACITY
    );
    assert!(
        string_report
            .rejected_alternatives
            .contains(&"oversized_count_only_string_heavy_hitter_window".to_string())
    );

    let numeric_utf8_aggregate = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("SearchEngineID").expect("column"),
            ColumnRef::new("SearchPhrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("rows", true)]);
    let numeric_utf8_request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), numeric_utf8_aggregate)
            .with_source_order_limit(10);
    let (_effective, numeric_utf8_report) =
        policy.with_physical_policy_for_request(&numeric_utf8_request);
    assert_eq!(
        numeric_utf8_report.route_family,
        "numeric_utf8_heavy_hitter_topk"
    );
    assert_eq!(
        numeric_utf8_report.selected_numeric_utf8_topk_heavy_hitter_capacity,
        VortexLocalPrimitiveResourceEnvelope::DEFAULT_HEAVY_HITTER_CAPACITY
    );

    let count_distinct_aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("SearchPhrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("UserID").expect("column")),
            "users".to_string(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("users", true)]);
    let count_distinct_request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri, count_distinct_aggregate)
            .with_source_order_limit(10);
    let (_effective, count_distinct_report) =
        policy.with_physical_policy_for_request(&count_distinct_request);
    assert_eq!(
        count_distinct_report.route_family,
        "string_count_distinct_heavy_hitter_topk"
    );
    assert_eq!(
        count_distinct_report.selected_string_topk_heavy_hitter_capacity,
        VortexLocalPrimitiveResourceEnvelope::DEFAULT_HEAVY_HITTER_CAPACITY
    );
    assert!(
        count_distinct_report
            .rejected_alternatives
            .contains(&"generic_count_distinct_group_state".to_string())
    );
}

#[test]
fn physical_policy_classifies_numeric_pair_aggregate_without_candidate_window() {
    let uri = DatasetUri::new("file:///tmp/policy-numeric-pair.vortex").expect("uri");
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("WatchID").expect("column"),
            ColumnRef::new("ClientIP").expect("column"),
        ],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("ResolutionWidth").expect("column")),
                "sum_width".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("ResolutionWidth").expect("column")),
                "avg_width".to_string(),
            ),
        ],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("rows", true)]);
    let request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate).with_source_order_limit(10);
    let policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("resource policy");
    let (effective, report) = policy.with_physical_policy_for_request(&request);

    assert_eq!(
        report.route_family,
        "near_input_cardinality_numeric_pair_aggregate"
    );
    assert_eq!(
        effective
            .resource_envelope
            .string_topk_heavy_hitter_capacity,
        0
    );
    assert_eq!(
        effective
            .resource_envelope
            .numeric_utf8_topk_heavy_hitter_capacity,
        0
    );
    assert!(
        report
            .rejected_alternatives
            .contains(&"heavy_hitter_candidate_window".to_string())
    );
}

#[test]
fn state_budget_classifies_high_cardinality_pressure_without_claiming_spill() {
    let low = VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
        "grouped_aggregate_state",
        vec!["vortex_scan"],
        vec!["group_cardinality"],
        42,
        Some(1_000),
        "local_vortex_grouped_aggregate",
    );
    assert_eq!(low.state_pressure_class, "low_cardinality_pressure");
    assert_eq!(
        low.state_budget_status,
        "bounded_in_memory_low_pressure_spill_not_required"
    );
    assert_eq!(low.diagnostic_code, "none");
    assert!(!low.spill_required);
    assert!(!low.spill_supported);

    let high = VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
        "grouped_aggregate_state+topk+compact_numeric_measures+numeric_pair",
        vec!["vortex_scan", "group_key_state"],
        vec!["group_cardinality", "numeric_pair_group_cardinality"],
        99_000_000,
        Some(100_000_000),
        "local_vortex_grouped_aggregate",
    );
    assert_eq!(
        high.state_pressure_class,
        "near_input_cardinality_high_pressure"
    );
    assert_eq!(
        high.state_budget_status,
        "bounded_in_memory_near_input_cardinality_spill_not_certified"
    );
    assert_eq!(
        high.diagnostic_code,
        "SL_STATE_BUDGET_HIGH_PRESSURE_NATIVE_SPILL_PENDING"
    );
    assert!(!high.spill_required);
    assert!(!high.spill_supported);
    assert!(!high.spill_io_performed);
    assert!(high.fail_closed_if_spill_required);
    assert!(high.next_action.contains("partitioned exact merge"));
    assert!(
        high.compact_summary()
            .contains("pressure_class=near_input_cardinality_high_pressure")
    );
}

#[test]
fn materialized_predicate_null_comparison_is_non_match_without_fallback() -> Result<()> {
    let columns = vec!["SearchPhrase".to_string(), "URL".to_string()];
    let column_values = vec![
        vec![StatValue::Utf8("alpha".to_string()), StatValue::Null],
        vec![
            StatValue::Utf8("https://example.com/google".to_string()),
            StatValue::Utf8("https://example.com/google".to_string()),
        ],
    ];
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::StringContains {
            column: ColumnRef::new("URL")?,
            needle: "google".to_string(),
            negated: false,
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("SearchPhrase")?,
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        },
    ]);

    assert!(predicate_matches_materialized_row(
        &predicate,
        &columns,
        &column_values,
        0
    )?);
    assert!(!predicate_matches_materialized_row(
        &predicate,
        &columns,
        &column_values,
        1
    )?);
    assert!(!compare_stat_value_with_op(
        None,
        &StatValue::Null,
        ComparisonOp::NotEq,
        &StatValue::Utf8(String::new())
    )?);

    Ok(())
}

#[test]
fn pushdown_predicate_accepts_iso_date_literals_for_date_named_unsigned_fields() -> Result<()> {
    use vortex::array::dtype::{DType, FieldNames, Nullability as VortexNullability, PType};

    let dtype = DType::Struct(
        vortex::array::dtype::StructFields::new(
            FieldNames::from(["EventDate", "UserID"]),
            vec![
                DType::Primitive(PType::U32, VortexNullability::NonNullable),
                DType::Primitive(PType::U64, VortexNullability::NonNullable),
            ],
        ),
        VortexNullability::NonNullable,
    );
    let date_predicate = PredicateExpr::Compare {
        column: ColumnRef::new("EventDate")?,
        op: ComparisonOp::GtEq,
        value: StatValue::Utf8("2013-07-01".to_string()),
    };
    let non_date_predicate = PredicateExpr::Compare {
        column: ColumnRef::new("UserID")?,
        op: ComparisonOp::Eq,
        value: StatValue::Utf8("2013-07-01".to_string()),
    };

    assert!(
        predicate_to_vortex_expr(
            &date_predicate,
            &dtype,
            VortexQueryPrimitiveKind::SimpleAggregate
        )
        .is_ok()
    );
    let non_date_error = predicate_to_vortex_expr(
        &non_date_predicate,
        &dtype,
        VortexQueryPrimitiveKind::SimpleAggregate,
    )
    .expect_err("non-date unsigned string literal stays blocked");
    assert!(
        non_date_error
            .to_string()
            .contains("unsigned predicates require integer literals")
    );

    Ok(())
}

#[test]
fn projection_all_hides_embedded_derived_columns_but_explicit_projection_can_use_them() -> Result<()>
{
    use vortex::array::dtype::{DType, FieldNames, Nullability as VortexNullability, PType};

    let dtype = DType::Struct(
        vortex::array::dtype::StructFields::new(
            FieldNames::from([
                "Referer",
                "__shardloom_derived_utf8_len_Referer",
                "__shardloom_derived_url_domain_Referer",
            ]),
            vec![
                DType::Utf8(VortexNullability::NonNullable),
                DType::Primitive(PType::U64, VortexNullability::NonNullable),
                DType::Utf8(VortexNullability::NonNullable),
            ],
        ),
        VortexNullability::NonNullable,
    );

    let user_columns = projected_column_names(
        &dtype,
        &ProjectionRequest::All,
        VortexQueryPrimitiveKind::ProjectColumns,
    )?;
    assert_eq!(user_columns, vec!["Referer".to_string()]);
    let planner_columns = projected_column_names(
        &dtype,
        &ProjectionRequest::columns(vec![ColumnRef::new(
            "__shardloom_derived_utf8_len_Referer",
        )?]),
        VortexQueryPrimitiveKind::SimpleAggregate,
    )?;
    assert_eq!(
        planner_columns,
        vec!["__shardloom_derived_utf8_len_Referer".to_string()]
    );

    Ok(())
}

#[test]
fn materialized_predicate_coerces_date_literals_and_ignores_impossible_unsigned_in_values()
-> Result<()> {
    let event_days = u64::try_from(parse_iso_date32("2013-07-14")?).expect("non-negative");
    let columns = vec!["EventDate".to_string(), "TraficSourceID".to_string()];
    let column_values = vec![
        vec![StatValue::UInt64(event_days)],
        vec![StatValue::UInt64(6)],
    ];
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("EventDate")?,
            op: ComparisonOp::GtEq,
            value: StatValue::Utf8("2013-07-01".to_string()),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("EventDate")?,
            op: ComparisonOp::LtEq,
            value: StatValue::Utf8("2013-07-31".to_string()),
        },
        PredicateExpr::InList {
            column: ColumnRef::new("TraficSourceID")?,
            values: vec![StatValue::Int64(-1), StatValue::Int64(6)],
            negated: false,
        },
    ]);

    assert!(predicate_matches_materialized_row(
        &predicate,
        &columns,
        &column_values,
        0
    )?);
    assert!(coerce_rewrite_value(&StatValue::UInt64(6), &StatValue::Int64(-1)).is_err());

    Ok(())
}

#[test]
fn materialized_null_predicates_read_row_values() -> Result<()> {
    let columns = vec!["SearchPhrase".to_string()];
    let column_values = vec![vec![StatValue::Utf8("alpha".to_string()), StatValue::Null]];
    let is_null = PredicateExpr::IsNull {
        column: ColumnRef::new("SearchPhrase")?,
    };
    let is_not_null = PredicateExpr::IsNotNull {
        column: ColumnRef::new("SearchPhrase")?,
    };

    assert!(!predicate_matches_materialized_row(
        &is_null,
        &columns,
        &column_values,
        0
    )?);
    assert!(predicate_matches_materialized_row(
        &is_null,
        &columns,
        &column_values,
        1
    )?);
    assert!(predicate_matches_materialized_row(
        &is_not_null,
        &columns,
        &column_values,
        0
    )?);
    assert!(!predicate_matches_materialized_row(
        &is_not_null,
        &columns,
        &column_values,
        1
    )?);

    Ok(())
}

pub(super) fn write_array(path: &std::path::Path, array: &vortex::array::ArrayRef) -> Result<()> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut bytes = Vec::new();
    let summary = runtime
        .block_on(
            session
                .write_options()
                .write(&mut bytes, array.to_array_stream()),
        )
        .map_err(vortex_error)?;
    assert_eq!(
        summary.row_count(),
        u64::try_from(array.len()).expect("len")
    );
    let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
    shardloom_core::write_workspace_safe_bytes(
        workspace_root,
        path,
        false,
        "local primitive Vortex fixture",
        &bytes,
    )
    .map(|_| ())
}

fn write_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["value", "metric"]),
        vec![
            [1_u32, 2, 3, 4, 5]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [10_i64, 20, 30, 40, 50]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        5,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_wide_sort_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from([
            "id", "metric", "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7",
        ]),
        vec![
            [1_i64, 2, 3]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [10_i64, 30, 20]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [100_i64, 200, 300]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [101_i64, 201, 301]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [102_i64, 202, 302]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [103_i64, 203, 303]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [104_i64, 204, 304]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [105_i64, 205, 305]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [106_i64, 206, 306]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [107_i64, 207, 307]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_wide_string_sort_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from([
            "id",
            "EventTime",
            "URL",
            "c0",
            "c1",
            "c2",
            "c3",
            "c4",
            "c5",
            "c6",
            "c7",
        ]),
        vec![
            [1_i64, 2, 3, 4]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [40_i64, 20, 30, 10]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str([
                "https://example.com/a",
                "https://www.google.com/search",
                "https://example.com/b",
                "https://maps.google.com/place",
            ])
            .into_array(),
            [100_i64, 200, 300, 400]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [101_i64, 201, 301, 401]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [102_i64, 202, 302, 402]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [103_i64, 203, 303, 403]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [104_i64, 204, 304, 404]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [105_i64, 205, 305, 405]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [106_i64, 206, 306, 406]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [107_i64, 207, 307, 407]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        4,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_melt_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["id", "amount_a", "amount_b"]),
        vec![
            [1_u32, 2, 3]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [10_i64, 20, 30]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [100_i64, 200, 300]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_melt_mixed_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["id", "amount", "label"]),
        vec![
            [1_u32, 2]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [10_i64, 20]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(["paid", "trial"]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_pivot_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["id", "label", "amount"]),
        vec![
            [1_u32, 1, 2]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(["paid", "paid", "trial"]).into_array(),
            [10_i64, 5, 7]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_numeric_pair_aggregate_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["watch_id", "client_ip", "is_refresh", "width"]),
        vec![
            [10_u64, 10, 10, 20, 20, 30]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [1_u32, 1, 2, 2, 2, 1]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [1_u8, 0, 1, 1, 0, 1]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [100_u32, 200, 300, 400, 500, 600]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        6,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_large_int_sort_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["id", "large"]),
        vec![
            [1_u32, 2]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [9_007_199_254_740_993_i64, 9_007_199_254_740_992_i64]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_explode_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let elements = [7_i64, 8, 9]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let offsets = [0_u32, 2, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let sizes = [2_u32, 0, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let items = ListViewArray::new(
        elements,
        offsets,
        sizes,
        Validity::NonNullable,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["id", "items"]),
        vec![
            [1_u32, 2, 3]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            items,
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_multi_explode_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let item_elements = [7_i64, 8, 9]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let label_elements = VarBinViewArray::from_iter_str(["red", "blue", "green"]).into_array();
    let item_offsets = [0_u32, 2, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let item_sizes = [2_u32, 0, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let label_offsets = [0_u32, 2, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let label_sizes = [2_u32, 0, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let items = ListViewArray::new(
        item_elements,
        item_offsets,
        item_sizes,
        Validity::NonNullable,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .into_array();
    let labels = ListViewArray::new(
        label_elements,
        label_offsets,
        label_sizes,
        Validity::NonNullable,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["id", "items", "labels"]),
        vec![
            [1_u32, 2, 3]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            items,
            labels,
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_nullable_nested_explode_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let element_struct = StructArray::try_new(
        FieldNames::from(["code", "label"]),
        vec![
            [7_i64, 8, 9]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(["red", "blue", "green"]).into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    let offsets = [0_u32, 2, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let sizes = [2_u32, 1, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let items = ListViewArray::new(
        element_struct,
        offsets,
        sizes,
        Validity::from_iter([true, false, true]),
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["id", "items"]),
        vec![
            [1_u32, 2, 3]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            items,
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_nested_row_key_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let elements = [7_i64, 8, 7, 8, 9]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let offsets = [0_u32, 2, 4]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let sizes = [2_u32, 2, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let items = ListViewArray::new(
        elements,
        offsets,
        sizes,
        Validity::NonNullable,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .into_array();
    let payload = StructArray::try_new(
        FieldNames::from(["code", "label"]),
        vec![
            [1_u32, 1, 2]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(["alpha", "alpha", "beta"]).into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["id", "items", "payload", "metric"]),
        vec![
            [1_u32, 2, 3]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            items,
            payload,
            [10_i64, 20, 30]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_nullable_parent_nested_row_key_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let nullable_elements = PrimitiveArray::new(
        vec![0_i64, 0, 0],
        Validity::from_iter([false, false, false]),
    )
    .into_array();
    let offsets = [0_u32, 1, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let sizes = [1_u32, 1, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let items = ListViewArray::new(
        nullable_elements,
        offsets,
        sizes,
        Validity::from_iter([true, false, true]),
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .into_array();
    let payload = StructArray::try_new(
        FieldNames::from(["code"]),
        vec![
            [9_u32, 9, 9]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        3,
        Validity::from_iter([true, false, true]),
    )
    .map_err(vortex_error)?
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["items", "payload"]),
        vec![items, payload],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_duplicate_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["value", "metric"]),
        vec![
            [1_u32, 1, 2, 2, 3]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [10_i64, 10, 20, 99, 30]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        5,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_sort_tie_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["key", "metric"]),
        vec![
            [1_i64, 1, 2, 2, 3]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [10_i64, 20, 30, 40, 50]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        5,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_string_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["label"]),
        vec![VarBinViewArray::from_iter_str(["bad", "good", "badly"]).into_array()],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_mixed_string_filter_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["URL", "SearchPhrase", "EventTime"]),
        vec![
            VarBinViewArray::from_iter_str([
                "https://google.example/a",
                "https://google.example/empty",
                "https://example.com/nope",
                "https://google.example/b",
            ])
            .into_array(),
            VarBinViewArray::from_iter_str(["alpha", "", "beta", "gamma"]).into_array(),
            [30_i64, 10, 20, 40]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        4,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_embedded_derived_referer_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let referers = [
        "https://www.google.com/search",
        "",
        "https://example.com/page",
        "http://www.google.com/ads",
    ];
    let referer_lengths = referers
        .iter()
        .map(|value| u64::try_from(value.len()).expect("length"))
        .collect::<PrimitiveArray>()
        .into_array();
    let referer_domains =
        VarBinViewArray::from_iter_str(["google.com", "", "example.com", "google.com"])
            .into_array();
    let array = StructArray::try_new(
        FieldNames::from([
            "Referer",
            "__shardloom_derived_utf8_len_Referer",
            "__shardloom_derived_url_domain_Referer",
        ]),
        vec![
            VarBinViewArray::from_iter_str(referers).into_array(),
            referer_lengths,
            referer_domains,
        ],
        referers.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_dictionary_embedded_derived_url_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let url_codes = [0_u8, 1, 2, 0]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let url_values = VarBinViewArray::from_iter_str([
        "https://www.google.com/search",
        "",
        "https://docs.rs/crate",
    ])
    .into_array();
    let urls = DictArray::try_new(url_codes, url_values)
        .expect("URL dictionary")
        .into_array();
    let length_codes = [0_u8, 1, 2, 0]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let length_values = [
        u32::try_from("https://www.google.com/search".len()).expect("len"),
        0,
        u32::try_from("https://docs.rs/crate".len()).expect("len"),
    ]
    .into_iter()
    .collect::<PrimitiveArray>()
    .into_array();
    let lengths = DictArray::try_new(length_codes, length_values)
        .expect("URL length dictionary")
        .into_array();
    let domain_codes = [0_u8, 1, 2, 0]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let domain_values = VarBinViewArray::from_iter_str(["google.com", "", "docs.rs"]).into_array();
    let domains = DictArray::try_new(domain_codes, domain_values)
        .expect("URL domain dictionary")
        .into_array();
    let array = StructArray::try_new(
        FieldNames::from([
            "URL",
            "__shardloom_derived_utf8_len_URL",
            "__shardloom_derived_url_domain_URL",
        ]),
        vec![urls, lengths, domains],
        4,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_embedded_derived_minute_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from([
            "UserID",
            "SearchPhrase",
            "EventTime",
            "__shardloom_derived_extract_minute_EventTime",
            "__shardloom_derived_date_trunc_minute_EventTime",
        ]),
        vec![
            [42_u64, 42, 7, 7, 42]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(["alpha", "alpha", "beta", "beta", "gamma"])
                .into_array(),
            [60_u64, 61, 120, 121, 62]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [1_u8, 1, 2, 2, 1]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
            [60_i64, 60, 120, 120, 60]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        5,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_all_null_aggregate_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let amount = PrimitiveArray::new(
        vec![0_i64, 0, 0],
        Validity::from_iter([false, false, false]),
    )
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["label", "amount"]),
        vec![
            VarBinViewArray::from_iter_str(["paid", "paid", "trial"]).into_array(),
            amount,
        ],
        3,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_nullable_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let values = PrimitiveArray::new(
        vec![1_u32, 0, 3, 4, 5],
        Validity::from_iter([true, false, true, true, true]),
    )
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["value"]),
        vec![values],
        5,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

fn write_nullable_duplicate_struct_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let values = PrimitiveArray::new(
        vec![1_u32, 0, 0, 2, 0],
        Validity::from_iter([true, false, false, true, false]),
    )
    .into_array();
    let metric =
        PrimitiveArray::new(vec![10_i64, 20, 99, 30, 44], Validity::NonNullable).into_array();
    let array = StructArray::try_new(
        FieldNames::from(["value", "metric"]),
        vec![values, metric],
        5,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    write_array(path, &array.into_array())
}

#[test]
#[ignore = "fixture regeneration helper; writes shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex"]
fn regenerate_checked_in_local_primitive_struct_fixture() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("local_primitive_struct_five.vortex");
    write_struct_fixture(&path).expect("fixture");
}

fn write_primitive_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::PrimitiveArray;

    let array = [7_u64, 8, 9]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    write_array(path, &array)
}

fn write_constant_primitive_fixture(path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::ConstantArray;

    let array = ConstantArray::new(7_i64, 4).into_array();
    write_array(path, &array)
}

fn correctness_fixture(id: &str) -> CorrectnessFixture {
    CorrectnessValidationPlan::default_foundation_plan()
        .fixtures
        .into_iter()
        .find(|fixture| fixture.id.as_str() == id)
        .expect("fixture")
}

fn checked_in_struct_fixture_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("local_primitive_struct_five.vortex")
}

fn checked_in_struct_fixture_uri() -> DatasetUri {
    DatasetUri::new(checked_in_struct_fixture_path().display().to_string()).expect("uri")
}

fn simple_aggregate_values_json(summary: &str) -> serde_json::Value {
    let (_, payload) = summary
        .rsplit_once(" values=")
        .expect("simple aggregate values payload");
    serde_json::from_str(payload).expect("simple aggregate values json")
}

fn count_where_values_json(summary: &str) -> serde_json::Value {
    let (_, payload) = summary
        .rsplit_once(" values=")
        .expect("count_where values payload");
    serde_json::from_str(payload).expect("count_where values json")
}

fn prepared_metric_segment(id: &str, row_count: u64) -> EncodedSegment {
    EncodedSegment::new(
        SegmentId::new(id).expect("segment"),
        ColumnRef::new("metric").expect("column"),
        LogicalDType::Int64,
        Nullability::Nullable,
        SegmentLayout::new(EncodingKind::Constant, LayoutKind::Flat),
        SegmentStats::with_row_count(row_count),
    )
}

fn scoped_pivot_projection(aggregate: &str) -> VortexPivotProjectionRequest {
    VortexPivotProjectionRequest::new(
        ColumnRef::new("id").expect("index column"),
        ColumnRef::new("label").expect("pivot column"),
        ColumnRef::new("amount").expect("value column"),
        aggregate,
    )
}

#[test]
fn pivot_first_unique_blocks_conflicting_duplicate_cells() {
    let projection = scoped_pivot_projection("first_unique");
    let mut index_keys = std::collections::BTreeSet::new();
    let mut pivot_columns = std::collections::BTreeMap::new();
    let mut first_cells = std::collections::BTreeMap::new();
    let mut aggregate_cells = std::collections::BTreeMap::new();

    let result = update_pivot_state(
        &projection,
        "first_unique",
        &[StatValue::UInt64(1), StatValue::UInt64(1)],
        &[
            StatValue::Utf8("paid".to_string()),
            StatValue::Utf8("paid".to_string()),
        ],
        &[StatValue::Int64(10), StatValue::Int64(20)],
        &mut index_keys,
        &mut pivot_columns,
        &mut first_cells,
        &mut aggregate_cells,
    );

    let error = result.expect_err("conflicting duplicate pivot cells must fail closed");
    assert!(error.to_string().contains("multiple values"), "{error:?}");
}

#[test]
fn pivot_table_sum_tracks_numeric_group_state() {
    let projection = scoped_pivot_projection("sum");
    let mut index_keys = std::collections::BTreeSet::new();
    let mut pivot_columns = std::collections::BTreeMap::new();
    let mut first_cells = std::collections::BTreeMap::new();
    let mut aggregate_cells = std::collections::BTreeMap::new();

    update_pivot_state(
        &projection,
        "sum",
        &[
            StatValue::UInt64(1),
            StatValue::UInt64(1),
            StatValue::UInt64(2),
        ],
        &[
            StatValue::Utf8("paid".to_string()),
            StatValue::Utf8("paid".to_string()),
            StatValue::Utf8("trial".to_string()),
        ],
        &[
            StatValue::Int64(10),
            StatValue::Float64(5.5),
            StatValue::UInt64(7),
        ],
        &mut index_keys,
        &mut pivot_columns,
        &mut first_cells,
        &mut aggregate_cells,
    )
    .expect("numeric pivot_table aggregate");

    let paid_cell = aggregate_cells
        .get(&(
            pivot_value_key(&StatValue::UInt64(1)),
            pivot_value_key(&StatValue::Utf8("paid".to_string())),
        ))
        .expect("paid aggregate cell");
    assert_eq!(paid_cell.count, 2);
    assert!((paid_cell.sum - 15.5).abs() < f64::EPSILON);
    assert_eq!(paid_cell.min, Some(5.5));
    assert_eq!(paid_cell.max, Some(10.0));
    assert_eq!(pivot_columns.len(), 2);
    assert!(first_cells.is_empty());
    assert_eq!(index_keys.len(), 2);
}

#[test]
fn pivot_table_sum_blocks_non_numeric_value_column() {
    let projection = scoped_pivot_projection("sum");
    let mut index_keys = std::collections::BTreeSet::new();
    let mut pivot_columns = std::collections::BTreeMap::new();
    let mut first_cells = std::collections::BTreeMap::new();
    let mut aggregate_cells = std::collections::BTreeMap::new();

    let result = update_pivot_state(
        &projection,
        "sum",
        &[StatValue::UInt64(1)],
        &[StatValue::Utf8("paid".to_string())],
        &[StatValue::Utf8("ten".to_string())],
        &mut index_keys,
        &mut pivot_columns,
        &mut first_cells,
        &mut aggregate_cells,
    );

    let error = result.expect_err("non-numeric pivot_table values must fail closed");
    assert!(
        error
            .to_string()
            .contains("requires a numeric value column"),
        "{error:?}"
    );
}

#[test]
fn feature_disabled_local_primitive_report_is_runtime_error() {
    let report = VortexLocalPrimitiveExecutionReport::feature_disabled(
        VortexQueryPrimitiveKind::CountAll,
        VortexLocalPrimitiveResourceEnvelope::new(4, 1).expect("explicit fixture allocation"),
    );

    assert_eq!(
        report.status,
        VortexLocalPrimitiveExecutionStatus::FeatureDisabled
    );
    assert!(report.has_errors());
}

#[test]
fn count_all_answers_from_vortex_metadata_without_scan() {
    use shardloom_core::native_io_decisions::{NativeIoCapability, NativeIoDecisionReason};
    let path = unique_vortex_path("count-all");
    write_primitive_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_all(uri);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.result_summary.as_deref(), Some("3"));
    assert!(!report.data_read);
    assert!(!report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert_eq!(report.max_parallelism_requested, 1);
    assert_eq!(report.scan_concurrency_per_worker, 1);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert!(!report.filter_pushdown_applied);
    assert!(!report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.upstream_scan_called);
    assert!(!report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.fallback_execution_allowed);
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    assert!(
        certificate.is_certified(),
        "metadata-only count should certify as native Vortex metadata I/O without a scan"
    );
    let row = certificate
        .capability_decisions
        .get(NativeIoCapability::MetadataCount)
        .unwrap();
    assert_eq!(
        (row.considered, row.admitted, row.executed, row.declined),
        (1, 1, 1, 0)
    );
    assert_eq!(row.observed.input_rows, Some(3));
    assert!(row.has_admission_reason(NativeIoDecisionReason::ExactHeldMetadata));
}

#[test]
fn local_scan_certifies_reader_chunks_without_unused_encoded_payloads() {
    let path = unique_vortex_path("constant-kernel-input");
    write_constant_primitive_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(uri, PredicateExpr::AlwaysTrue);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(4));
    let prepared_report = report
        .reader_generated_prepared_batch_report
        .as_ref()
        .expect("reader-generated prepared batch report");
    assert_eq!(
        prepared_report.status,
        VortexReaderGeneratedPreparedBatchStatus::PreparedReaderChunkEnvelopes
    );
    assert!(prepared_report.reader_generated_prepared_batches);
    assert!(prepared_report.reader_chunk_envelopes_available);
    assert!(prepared_report.provider_boundary.is_policy_admitted());
    assert!(!prepared_report.encoded_value_batch_available);
    assert!(!prepared_report.encoded_projection_batch_available);
    assert_eq!(prepared_report.encoded_kernel_input_count, 0);
    assert_eq!(prepared_report.reader_split_count, report.arrays_read_count);
    assert!(prepared_report.reader_source_uri_matches_source);
    // This nested report does not admit a second executable batch. The
    // native scan above has already executed using the original arrays.
    assert!(prepared_report.kernel_input_lowering_blocked);
    assert!(!prepared_report.runtime_execution_allowed);
    assert!(prepared_report.avoids_forbidden_effects());
    assert!(!prepared_report.has_errors());
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn aggregate_and_sort_handoffs_keep_reader_certificates_without_value_copies() {
    let path = unique_vortex_path("native-handoff-evidence");
    write_constant_primitive_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).unwrap();
    let aggregate = VortexQueryPrimitiveRequest::simple_aggregate(
        uri.clone(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("value").unwrap()],
            vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
        ),
    );
    let sort = VortexQueryPrimitiveRequest::sort_rows(
        uri,
        ProjectionRequest::columns(vec![ColumnRef::new("value").unwrap()]),
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("value", true)]),
        2,
    );
    for (request, expected_rows) in [(aggregate, 1), (sort, 2)] {
        let report = execute_vortex_local_primitive(&request).unwrap();
        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_projected, Some(expected_rows));
        assert!(report.arrays_read_count > 0);
        let reader = report
            .reader_generated_prepared_batch_report
            .as_ref()
            .unwrap();
        assert_eq!(reader.encoded_kernel_input_count, 0);
        assert_eq!(reader.reader_split_count, report.arrays_read_count);
        assert!(reader.reader_source_uri_matches_source);
        assert!(reader.reader_chunk_envelopes_available);
        assert!(reader.provider_boundary.is_policy_admitted());
        assert!(!reader.has_errors());
        assert!(
            local_primitive_native_io_certificate(&request, &report)
                .unwrap()
                .is_certified()
        );
        assert!(!report.fallback_execution_allowed);
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn reader_chunk_dictionary_values_lower_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray};

    let codes = [0_u8, 1, 0, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = [10_u64, 20, 30]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let chunk = DictArray::try_new(codes, values)
        .expect("dictionary array")
        .into_array();
    let source_uri = DatasetUri::new("file:///tmp/dictionary-values.vortex").expect("uri");

    let inputs =
        reader_generated_encoded_kernel_inputs_from_vortex_chunk(&source_uri, "split-dict", &chunk)
            .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(
        input.batch.segment.layout.encoding,
        EncodingKind::Dictionary
    );
    assert_eq!(input.batch.segment.dtype, LogicalDType::UInt64);
    assert_eq!(input.batch.segment.stats.row_count, Some(4));
    assert_eq!(input.batch.segment.stats.null_count, Some(0));
    match &input.batch.values {
        EncodedValueBatch::Dictionary { dictionary, codes } => {
            assert_eq!(
                dictionary,
                &vec![
                    Some(StatValue::UInt64(10)),
                    Some(StatValue::UInt64(20)),
                    Some(StatValue::UInt64(30)),
                ]
            );
            assert_eq!(codes, &vec![Some(0), Some(1), Some(0), Some(2)]);
        }
        other => panic!("expected dictionary batch, got {other:?}"),
    }
}

#[test]
fn reader_chunk_utf8_dictionary_values_lower_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, VarBinViewArray};

    let codes = [0_u8, 1, 0, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = VarBinViewArray::from_iter_str(["alpha", "beta", "gamma"]).into_array();
    let chunk = DictArray::try_new(codes, values)
        .expect("utf8 dictionary array")
        .into_array();
    let source_uri = DatasetUri::new("file:///tmp/utf8-dictionary.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-utf8-dict",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(
        input.batch.segment.layout.encoding,
        EncodingKind::Dictionary
    );
    assert_eq!(input.batch.segment.dtype, LogicalDType::Utf8);
    assert_eq!(input.batch.segment.stats.row_count, Some(4));
    assert_eq!(input.batch.segment.stats.null_count, Some(0));
    match &input.batch.values {
        EncodedValueBatch::Dictionary { dictionary, codes } => {
            assert_eq!(
                dictionary,
                &vec![
                    Some(StatValue::Utf8("alpha".to_string())),
                    Some(StatValue::Utf8("beta".to_string())),
                    Some(StatValue::Utf8("gamma".to_string())),
                ]
            );
            assert_eq!(codes, &vec![Some(0), Some(1), Some(0), Some(2)]);
        }
        other => panic!("expected dictionary batch, got {other:?}"),
    }
}

#[test]
fn fast_utf8_contains_count_only_admits_host_varbinview_without_decode() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, VarBinViewArray};

    let columns = vec!["label".to_string()];
    let direct = VarBinViewArray::from_iter_str(["google", "other", "google-news"]).into_array();
    let direct_count = fast_utf8_contains_count_from_chunk(
        &direct,
        &columns,
        0,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("direct host contains count");
    assert_eq!(direct_count, Some(2));
    let unicode_direct =
        VarBinViewArray::from_iter_str(["café", "cafeteria", "news-café"]).into_array();
    let unicode_count = fast_utf8_contains_count_from_chunk(
        &unicode_direct,
        &columns,
        0,
        "afé",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("direct UTF-8 byte-search contains count");
    assert_eq!(unicode_count, Some(2));

    let codes = [0_u8, 1, 0]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = VarBinViewArray::from_iter_str(["google", "other"]).into_array();
    let encoded = DictArray::try_new(codes, values)
        .expect("dictionary array")
        .into_array();
    let encoded_count = fast_utf8_contains_count_from_chunk(
        &encoded,
        &columns,
        0,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("encoded count decision");
    assert_eq!(
        encoded_count,
        Some(2),
        "dictionary UTF-8 should count through dictionary codes without executing into decoded VarBinView"
    );
    let encoded_negated = fast_utf8_contains_count_from_chunk(
        &encoded,
        &columns,
        0,
        "google",
        true,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("encoded negated count decision");
    assert_eq!(encoded_negated, Some(1));
}

#[test]
fn utf8_contains_matcher_preserves_ascii_unicode_negated_and_empty_semantics() {
    let ascii = Utf8ContainsMatcher::new("google", false);
    assert!(ascii.matches_str("https://google.example"));
    assert!(!ascii.matches_str("https://example.test"));
    assert_eq!(ascii.matches_bytes(b"https://google.example"), Some(true));

    let negated = Utf8ContainsMatcher::new("google", true);
    assert!(!negated.matches_str("https://google.example"));
    assert!(negated.matches_str("https://example.test"));

    let unicode = Utf8ContainsMatcher::new("cafe\u{301}", false);
    assert!(unicode.matches_str("https://example.test/cafe\u{301}"));
    assert_eq!(unicode.matches_bytes(b"\xff"), None);

    let empty = Utf8ContainsMatcher::new("", false);
    assert!(empty.matches_str(""));
    assert_eq!(empty.matches_bytes(b"\xff"), Some(true));
}

#[test]
fn fast_utf8_contains_skips_nulls_without_materialized_string_fallback() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, FilterArray, PrimitiveArray, VarBinViewArray};
    use vortex::array::validity::Validity;
    use vortex::mask::Mask;

    let columns = vec!["label".to_string()];
    let direct = VarBinViewArray::from_iter_nullable_str([
        Some("google"),
        None,
        Some("other"),
        Some("google-news"),
    ])
    .into_array();
    let direct_count = fast_utf8_contains_count_from_chunk(
        &direct,
        &columns,
        0,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable direct contains count");
    assert_eq!(direct_count, Some(2));
    let direct_negated = fast_utf8_contains_count_from_chunk(
        &direct,
        &columns,
        0,
        "google",
        true,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable direct negated contains count");
    assert_eq!(direct_negated, Some(1));
    let direct_indices = fast_utf8_contains_row_indices_from_chunk(
        &direct,
        &columns,
        0,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable direct contains row indices");
    assert_eq!(direct_indices, Some(vec![0, 3]));
    let direct_negated_indices = fast_utf8_contains_row_indices_from_chunk(
        &direct,
        &columns,
        0,
        "google",
        true,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable direct negated contains row indices");
    assert_eq!(direct_negated_indices, Some(vec![2]));

    let codes = [0_u8, 1, 0, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values =
        VarBinViewArray::from_iter_nullable_str([Some("google"), None, Some("other")]).into_array();
    let dictionary = DictArray::try_new(codes, values)
        .expect("nullable dictionary values")
        .into_array();
    let dictionary_count = fast_utf8_contains_count_from_chunk(
        &dictionary,
        &columns,
        0,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable dictionary contains count");
    assert_eq!(dictionary_count, Some(2));
    let dictionary_negated = fast_utf8_contains_count_from_chunk(
        &dictionary,
        &columns,
        0,
        "google",
        true,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable dictionary negated contains count");
    assert_eq!(dictionary_negated, Some(1));
    let dictionary_indices = fast_utf8_contains_row_indices_from_chunk(
        &dictionary,
        &columns,
        0,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable dictionary contains row indices");
    assert_eq!(dictionary_indices, Some(vec![0, 2]));

    let nullable_codes = PrimitiveArray::new(
        vec![0_u8, 1, 0, 2],
        Validity::from_iter([true, false, true, true]),
    )
    .into_array();
    let nullable_code_values =
        VarBinViewArray::from_iter_str(["google", "other", "example"]).into_array();
    let nullable_code_dictionary = DictArray::try_new(nullable_codes, nullable_code_values)
        .expect("nullable dictionary codes")
        .into_array();
    let nullable_code_count = fast_utf8_contains_count_from_chunk(
        &nullable_code_dictionary,
        &columns,
        0,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable dictionary code contains count");
    assert_eq!(nullable_code_count, Some(2));
    let nullable_code_negated = fast_utf8_contains_count_from_chunk(
        &nullable_code_dictionary,
        &columns,
        0,
        "google",
        true,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable dictionary code negated contains count");
    assert_eq!(nullable_code_negated, Some(1));

    let filtered_dictionary =
        FilterArray::new(dictionary, Mask::from_iter([true, true, false, true])).into_array();
    let filtered_count = fast_utf8_contains_count_from_array(
        &filtered_dictionary,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered nullable dictionary contains count");
    assert_eq!(filtered_count, Some(1));
    let filtered_indices = fast_utf8_contains_row_indices_from_array(
        &filtered_dictionary,
        "google",
        false,
        0,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered nullable dictionary contains row indices");
    assert_eq!(filtered_indices, Some(vec![0]));
    let filtered_negated_indices = fast_utf8_contains_row_indices_from_array(
        &filtered_dictionary,
        "google",
        true,
        0,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered nullable dictionary negated contains row indices");
    assert_eq!(filtered_negated_indices, Some(vec![2]));
}

#[test]
fn fast_utf8_contains_reuses_chunk_dictionary_counts() {
    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1, 0, 2, 1, 0],
        values: vec![
            std::sync::Arc::<str>::from("https://google.test/a"),
            std::sync::Arc::<str>::from("https://example.test/b"),
            std::sync::Arc::<str>::from("https://news.google.test/c"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::DecodedUtf8ChunkDictionary,
    };

    let count = fast_utf8_contains_count_from_dictionary_accessor(&accessor, "google", false)
        .expect("dictionary contains count");
    assert_eq!(count, 4);
    let negated_count =
        fast_utf8_contains_count_from_dictionary_accessor(&accessor, "google", true)
            .expect("dictionary negated contains count");
    assert_eq!(negated_count, 2);
    let indices =
        fast_utf8_contains_row_indices_from_dictionary_accessor(&accessor, "google", false)
            .expect("dictionary contains row indices");
    assert_eq!(indices, vec![0, 2, 3, 5]);
    let negated_indices =
        fast_utf8_contains_row_indices_from_dictionary_accessor(&accessor, "google", true)
            .expect("dictionary negated row indices");
    assert_eq!(negated_indices, vec![1, 4]);
}

#[test]
fn non_null_utf8_dictionary_match_count_shortcuts_preserve_masks() {
    use vortex::mask::Mask;

    let row_ids = vec![0, 1, 0, 2, 1, 0];
    assert_eq!(
        fast_non_null_utf8_dictionary_match_count_from_flags(
            &row_ids,
            &[false, false, false],
            "test none",
        )
        .expect("none count"),
        0
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_match_count_from_flags(
            &row_ids,
            &[true, true, true],
            "test all",
        )
        .expect("all count"),
        row_ids.len()
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_match_count_from_flags(
            &row_ids,
            &[true, false, true],
            "test mixed",
        )
        .expect("mixed count"),
        4
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_row_indices_from_match_flags(
            &row_ids,
            &[false, false, false],
            "test none indices",
        )
        .expect("none indices"),
        Vec::<usize>::new()
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_row_indices_from_match_flags(
            &row_ids,
            &[true, true, true],
            "test all indices",
        )
        .expect("all indices"),
        vec![0, 1, 2, 3, 4, 5]
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_row_indices_from_match_flags(
            &row_ids,
            &[true, false, true],
            "test mixed indices",
        )
        .expect("mixed indices"),
        vec![0, 2, 3, 5]
    );

    let mask = Mask::from_iter([true, false, true, true, false, false]);
    assert_eq!(
        fast_non_null_utf8_dictionary_match_count_from_flags_masked(
            &row_ids,
            &[false, false, false],
            &mask,
            "test masked none",
        )
        .expect("masked none"),
        0
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_match_count_from_flags_masked(
            &row_ids,
            &[true, true, true],
            &mask,
            "test masked all",
        )
        .expect("masked all"),
        3
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_match_count_from_flags_masked(
            &row_ids,
            &[true, false, true],
            &mask,
            "test masked mixed",
        )
        .expect("masked mixed"),
        3
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_row_indices_from_match_flags_masked(
            &row_ids,
            &[false, false, false],
            &mask,
            "test masked none indices",
        )
        .expect("masked none indices"),
        Vec::<usize>::new()
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_row_indices_from_match_flags_masked(
            &row_ids,
            &[true, true, true],
            &mask,
            "test masked all indices",
        )
        .expect("masked all indices"),
        vec![0, 1, 2]
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_row_indices_from_match_flags_masked(
            &row_ids,
            &[true, false, true],
            &mask,
            "test masked mixed indices",
        )
        .expect("masked mixed indices"),
        vec![0, 1, 2]
    );

    let all_true_mask = Mask::new_true(row_ids.len());
    assert_eq!(
        fast_non_null_utf8_dictionary_match_count_from_flags_masked(
            &row_ids,
            &[true, false, true],
            &all_true_mask,
            "test all-true mask count",
        )
        .expect("all-true mask count"),
        4
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_row_indices_from_match_flags_masked(
            &row_ids,
            &[true, false, true],
            &all_true_mask,
            "test all-true mask indices",
        )
        .expect("all-true mask indices"),
        vec![0, 2, 3, 5]
    );

    let all_false_mask = Mask::new_false(row_ids.len());
    assert_eq!(
        fast_non_null_utf8_dictionary_match_count_from_flags_masked(
            &row_ids,
            &[true, true, true],
            &all_false_mask,
            "test all-false mask count",
        )
        .expect("all-false mask count"),
        0
    );
    assert_eq!(
        fast_non_null_utf8_dictionary_row_indices_from_match_flags_masked(
            &row_ids,
            &[true, true, true],
            &all_false_mask,
            "test all-false mask indices",
        )
        .expect("all-false mask indices"),
        Vec::<usize>::new()
    );
}

#[test]
fn non_null_dictionary_frequency_counts_use_direct_code_path() {
    use vortex::mask::Mask;

    let row_ids = vec![0, 1, 0, 2, 1, 0];
    let (counts, null_count) =
        dictionary_value_counts_and_null_count_for_rows(&row_ids, 3, None, None, None)
            .expect("full counts");
    assert_eq!(counts, vec![3, 2, 1]);
    assert_eq!(null_count, 0);

    let (selected_counts, selected_null_count) =
        dictionary_value_counts_and_null_count_for_rows(&row_ids, 3, None, None, Some(&[0, 3, 5]))
            .expect("selected counts");
    assert_eq!(selected_counts, vec![2, 0, 1]);
    assert_eq!(selected_null_count, 0);

    let mask = Mask::from_iter([true, false, true, true, false, false]);
    let (masked_counts, masked_null_count) =
        dictionary_value_counts_and_null_count_for_mask(&row_ids, 3, None, None, &mask)
            .expect("masked counts");
    assert_eq!(masked_counts, vec![2, 0, 1]);
    assert_eq!(masked_null_count, 0);
}

#[test]
fn fast_utf8_contains_masked_dictionary_count_reuses_value_counts() {
    use vortex::mask::Mask;

    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1, 0, 2, 1, 0, 3],
        values: vec![
            std::sync::Arc::<str>::from("https://google.test/a"),
            std::sync::Arc::<str>::from("https://example.test/b"),
            std::sync::Arc::<str>::from("https://news.google.test/c"),
            std::sync::Arc::<str>::from("https://unused.google.test/d"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::DecodedUtf8ChunkDictionary,
    };
    let mask = Mask::from_iter([true, false, true, true, true, false, false]);

    let count =
        fast_utf8_contains_count_from_dictionary_accessor_masked(&accessor, "google", false, &mask)
            .expect("masked dictionary contains count");
    assert_eq!(count, 3);
    let negated_count =
        fast_utf8_contains_count_from_dictionary_accessor_masked(&accessor, "google", true, &mask)
            .expect("masked dictionary negated contains count");
    assert_eq!(negated_count, 1);
    let indices = fast_utf8_contains_row_indices_from_dictionary_accessor_masked(
        &accessor, "google", false, &mask,
    )
    .expect("masked dictionary contains row indices");
    assert_eq!(indices, vec![0, 1, 2]);
    let negated_indices = fast_utf8_contains_row_indices_from_dictionary_accessor_masked(
        &accessor, "google", true, &mask,
    )
    .expect("masked dictionary negated row indices");
    assert_eq!(negated_indices, vec![3]);

    let all_true_mask = Mask::new_true(accessor.len());
    let all_true_count = fast_utf8_contains_count_from_dictionary_accessor_masked(
        &accessor,
        "google",
        false,
        &all_true_mask,
    )
    .expect("all-true dictionary contains count");
    assert_eq!(all_true_count, 5);
    let all_true_indices = fast_utf8_contains_row_indices_from_dictionary_accessor_masked(
        &accessor,
        "google",
        false,
        &all_true_mask,
    )
    .expect("all-true dictionary contains row indices");
    assert_eq!(all_true_indices, vec![0, 2, 3, 5, 6]);

    let all_false_mask = Mask::new_false(accessor.len());
    let all_false_count = fast_utf8_contains_count_from_dictionary_accessor_masked(
        &accessor,
        "google",
        false,
        &all_false_mask,
    )
    .expect("all-false dictionary contains count");
    assert_eq!(all_false_count, 0);
    let all_false_indices = fast_utf8_contains_row_indices_from_dictionary_accessor_masked(
        &accessor,
        "google",
        false,
        &all_false_mask,
    )
    .expect("all-false dictionary contains row indices");
    assert_eq!(all_false_indices, Vec::<usize>::new());
}

#[test]
fn filtered_utf8_contains_uses_mask_first_host_and_dictionary_paths() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, FilterArray, PrimitiveArray, VarBinViewArray};
    use vortex::mask::Mask;

    let direct_values = [
        "google",
        "skip-google",
        "other",
        "google-again",
        "google-final",
    ];
    let direct = VarBinViewArray::from_iter_str(direct_values).into_array();
    let direct_filter =
        FilterArray::new(direct, Mask::from_iter([false, true, true, false, true])).into_array();
    let direct_count = fast_utf8_contains_count_from_array(
        &direct_filter,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered direct count");
    assert_eq!(direct_count, Some(2));
    let direct_indices = fast_utf8_contains_row_indices_from_array(
        &direct_filter,
        "google",
        false,
        0,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered direct indices");
    assert_eq!(direct_indices, Some(vec![0, 2]));

    let direct_all_true = FilterArray::new(
        VarBinViewArray::from_iter_str(direct_values).into_array(),
        Mask::new_true(direct_values.len()),
    )
    .into_array();
    let direct_all_true_count = fast_utf8_contains_count_from_array(
        &direct_all_true,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("all-true filtered direct count");
    assert_eq!(direct_all_true_count, Some(4));
    let direct_all_true_indices = fast_utf8_contains_row_indices_from_array(
        &direct_all_true,
        "google",
        false,
        0,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("all-true filtered direct indices");
    assert_eq!(direct_all_true_indices, Some(vec![0, 1, 3, 4]));

    let direct_all_false = FilterArray::new(
        VarBinViewArray::from_iter_str(direct_values).into_array(),
        Mask::new_false(direct_values.len()),
    )
    .into_array();
    let direct_all_false_count = fast_utf8_contains_count_from_array(
        &direct_all_false,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("all-false filtered direct count");
    assert_eq!(direct_all_false_count, Some(0));
    let direct_all_false_indices = fast_utf8_contains_row_indices_from_array(
        &direct_all_false,
        "google",
        false,
        0,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("all-false filtered direct indices");
    assert_eq!(direct_all_false_indices, Some(Vec::<usize>::new()));

    let make_dictionary = || {
        let codes = [0_u8, 1, 2, 0, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let values =
            VarBinViewArray::from_iter_str(["google", "other", "google-news"]).into_array();
        DictArray::try_new(codes, values)
            .expect("dictionary array")
            .into_array()
    };
    let dictionary = make_dictionary();
    let dictionary_filter = FilterArray::new(
        dictionary,
        Mask::from_iter([false, true, true, false, true]),
    )
    .into_array();
    let dictionary_count = fast_utf8_contains_count_from_array(
        &dictionary_filter,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered dictionary count");
    assert_eq!(dictionary_count, Some(1));
    let dictionary_indices = fast_utf8_contains_row_indices_from_array(
        &dictionary_filter,
        "google",
        false,
        0,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered dictionary indices");
    assert_eq!(dictionary_indices, Some(vec![1]));

    let dictionary_all_true =
        FilterArray::new(make_dictionary(), Mask::new_true(direct_values.len())).into_array();
    let dictionary_all_true_count = fast_utf8_contains_count_from_array(
        &dictionary_all_true,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("all-true filtered dictionary count");
    assert_eq!(dictionary_all_true_count, Some(3));
    let dictionary_all_true_indices = fast_utf8_contains_row_indices_from_array(
        &dictionary_all_true,
        "google",
        false,
        0,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("all-true filtered dictionary indices");
    assert_eq!(dictionary_all_true_indices, Some(vec![0, 2, 3]));

    let dictionary_all_false =
        FilterArray::new(make_dictionary(), Mask::new_false(direct_values.len())).into_array();
    let dictionary_all_false_count = fast_utf8_contains_count_from_array(
        &dictionary_all_false,
        "google",
        false,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("all-false filtered dictionary count");
    assert_eq!(dictionary_all_false_count, Some(0));
    let dictionary_all_false_indices = fast_utf8_contains_row_indices_from_array(
        &dictionary_all_false,
        "google",
        false,
        0,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("all-false filtered dictionary indices");
    assert_eq!(dictionary_all_false_indices, Some(Vec::<usize>::new()));
}

#[test]
fn reader_chunk_struct_lowers_utf8_dictionary_child_without_panic() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let dim_key = [1_u32, 2, 1, 3]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let label_codes = [0_u8, 1, 0, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let label_values = VarBinViewArray::from_iter_str(["one", "two", "three"]).into_array();
    let dim_label = DictArray::try_new(label_codes, label_values)
        .expect("utf8 dictionary labels")
        .into_array();
    let chunk = StructArray::try_new(
        FieldNames::from(["dim_key", "dim_label"]),
        vec![dim_key, dim_label],
        4,
        Validity::NonNullable,
    )
    .expect("struct chunk")
    .into_array();
    let source_uri = DatasetUri::new("file:///tmp/hash-join-dim.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-hash-join-dim",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    assert!(
        inputs
            .iter()
            .all(VortexReaderGeneratedEncodedKernelInput::mapping_evidence_complete)
    );
    assert!(inputs.iter().all(|input| !input.has_forbidden_effects()));
    let columns = inputs
        .iter()
        .map(|input| {
            (
                input.batch.segment.column.as_str().to_string(),
                input.batch.segment.dtype.clone(),
                input.batch.segment.layout.encoding.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        columns,
        vec![(
            "dim_label".to_string(),
            LogicalDType::Utf8,
            EncodingKind::Dictionary,
        )]
    );
}

#[test]
fn reader_chunk_run_end_values_lower_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::encodings::runend::RunEnd;

    let ends = [2_u64, 5]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = [5_i64, 9]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let mut ctx = vortex::session::VortexSession::empty().create_execution_ctx();
    let chunk = RunEnd::try_new(ends, values, &mut ctx)
        .expect("run-end array")
        .into_array();
    let source_uri = DatasetUri::new("file:///tmp/run-end-values.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-run-end",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(input.batch.segment.layout.encoding, EncodingKind::RunLength);
    assert_eq!(input.batch.segment.dtype, LogicalDType::Int64);
    assert_eq!(input.batch.segment.stats.row_count, Some(5));
    assert_eq!(input.batch.segment.stats.null_count, Some(0));
    assert_eq!(input.batch.segment.stats.run_count, Some(2));
    match &input.batch.values {
        EncodedValueBatch::RunLength { runs } => {
            assert_eq!(
                runs,
                &vec![
                    EncodedValueRun::new(Some(StatValue::Int64(5)), 2),
                    EncodedValueRun::new(Some(StatValue::Int64(9)), 3),
                ]
            );
        }
        other => panic!("expected run-length batch, got {other:?}"),
    }
}

#[test]
fn reader_chunk_bitpacked_values_lower_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::encodings::fastlanes::BitPackedData;

    let values = [0_u8, 1, 0, 1, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let mut ctx = vortex::session::VortexSession::empty().create_execution_ctx();
    let chunk = BitPackedData::encode(&values, 1, &mut ctx)
        .expect("bit-packed array")
        .as_array()
        .clone();
    let source_uri = DatasetUri::new("file:///tmp/bitpacked-values.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-bitpacked",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(input.batch.segment.layout.encoding, EncodingKind::BitPacked);
    assert_eq!(input.batch.segment.dtype, LogicalDType::UInt64);
    assert_eq!(input.batch.segment.stats.row_count, Some(5));
    assert_eq!(input.batch.segment.stats.null_count, Some(0));
    match &input.batch.values {
        EncodedValueBatch::BitPackedUnsigned { bit_width, values } => {
            assert_eq!(*bit_width, 1);
            assert_eq!(values, &vec![0, 1, 0, 1, 1]);
        }
        other => panic!("expected bit-packed batch, got {other:?}"),
    }
}

#[test]
fn reader_chunk_sequence_values_lower_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::dtype::{Nullability as VortexNullability, PType};
    use vortex::array::scalar::PValue;
    use vortex::encodings::sequence::Sequence;

    let chunk = Sequence::try_new(
        PValue::U32(10),
        PValue::U32(3),
        PType::U32,
        VortexNullability::NonNullable,
        4,
    )
    .expect("sequence array")
    .into_array();
    let source_uri = DatasetUri::new("file:///tmp/sequence-values.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-sequence",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(input.batch.segment.layout.encoding, EncodingKind::Sequence);
    assert_eq!(input.batch.segment.dtype, LogicalDType::UInt64);
    assert_eq!(input.batch.segment.stats.row_count, Some(4));
    assert_eq!(input.batch.segment.stats.null_count, Some(0));
    match &input.batch.values {
        EncodedValueBatch::ArithmeticSequence {
            base,
            multiplier,
            row_count,
        } => {
            assert_eq!(base, &StatValue::UInt64(10));
            assert_eq!(multiplier, &StatValue::UInt64(3));
            assert_eq!(*row_count, 4);
        }
        other => panic!("expected sequence batch, got {other:?}"),
    }
}

#[test]
fn sequence_kernel_mapping_keeps_unsigned_negative_steps_outside_the_contract() {
    use vortex::array::{
        IntoArray as _,
        dtype::{Nullability, PType},
    };
    use vortex::encodings::sequence::Sequence;

    let source_uri = DatasetUri::new("file:///tmp/sequence-step-domains.vortex").unwrap();
    for (base, step, admitted) in [(10u32, -2i64, false), (10, 0, true), (10, 2, true)] {
        let array = Sequence::try_new(
            base.into(),
            step.into(),
            PType::U32,
            Nullability::NonNullable,
            4,
        )
        .unwrap()
        .into_array();
        let input =
            sequence_kernel_input_from_vortex_array(&source_uri, "sequence", "value", 4, &array)
                .unwrap();
        assert_eq!(input.is_some(), admitted);
    }
}

#[test]
fn reader_generated_conjunctive_filter_intersects_bitpacked_and_sequence_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray};
    use vortex::array::dtype::{FieldNames, Nullability as VortexNullability, PType};
    use vortex::array::scalar::PValue;
    use vortex::array::validity::Validity;
    use vortex::encodings::fastlanes::BitPackedData;
    use vortex::encodings::sequence::Sequence;

    let flag_values = [0_u8, 1, 0, 1, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let mut ctx = vortex::session::VortexSession::empty().create_execution_ctx();
    let flag = BitPackedData::encode(&flag_values, 1, &mut ctx)
        .expect("bit-packed flag")
        .as_array()
        .clone();
    let value = Sequence::try_new(
        PValue::U32(4_990),
        PValue::U32(5),
        PType::U32,
        VortexNullability::NonNullable,
        5,
    )
    .expect("sequence value")
    .into_array();
    let chunk = StructArray::try_new(
        FieldNames::from(["flag", "value"]),
        vec![flag, value],
        5,
        Validity::NonNullable,
    )
    .expect("struct chunk")
    .into_array();
    let source_uri = DatasetUri::new("file:///tmp/filter-columns.vortex").expect("uri");
    let source = UniversalInputSource::from_dataset_uri(source_uri.clone()).expect("source");
    let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
        source_uri.clone(),
        0,
        chunk.len(),
        chunk.dtype().to_string(),
        chunk.encoding_id().to_string(),
        chunk.nchildren(),
        chunk.nbuffers(),
    )
    .expect("split");
    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        &split.split_ref,
        &chunk,
    )
    .expect("kernel inputs");
    let predicates = vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("flag").expect("flag column"),
            op: ComparisonOp::Eq,
            value: StatValue::UInt64(1),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("value column"),
            op: ComparisonOp::GtEq,
            value: StatValue::UInt64(5_000),
        },
    ];

    let bridge =
        crate::execute_vortex_reader_generated_conjunctive_filter_from_encoded_kernel_inputs(
            &predicates,
            &source,
            &[split],
            &inputs,
        )
        .expect("bridge");

    assert_eq!(inputs.len(), 2);
    assert_eq!(bridge.status.as_str(), "intersected_selection_vectors");
    assert_eq!(bridge.intersection_count, 1);
    assert_eq!(bridge.selected_row_count, Some(2));
    assert!(bridge.filter_column_batches_consumed);
    assert!(bridge.selection_vector_intersection_certified);
    assert!(!bridge.data_decoded);
    assert!(!bridge.data_materialized);
    assert!(!bridge.fallback_attempted);
    assert!(!bridge.external_engine_invoked);
}

#[test]
fn nullable_dictionary_reader_chunk_lowers_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray};
    use vortex::array::validity::Validity;

    let codes = PrimitiveArray::new(vec![0_u8, 0], Validity::from_iter([true, false])).into_array();
    let values =
        PrimitiveArray::new(vec![10_u64, 20], Validity::from_iter([true, true])).into_array();
    let chunk = DictArray::try_new(codes, values)
        .expect("nullable dictionary array")
        .into_array();
    let source_uri = DatasetUri::new("file:///tmp/nullable-dictionary.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-nullable-dict",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(
        input.batch.segment.layout.encoding,
        EncodingKind::Dictionary
    );
    assert_eq!(input.batch.segment.dtype, LogicalDType::UInt64);
    assert_eq!(
        input.batch.segment.nullability,
        ShardLoomNullability::Nullable
    );
    assert_eq!(input.batch.segment.stats.row_count, Some(2));
    assert_eq!(input.batch.segment.stats.null_count, Some(1));
    match &input.batch.values {
        EncodedValueBatch::Dictionary { dictionary, codes } => {
            assert_eq!(
                dictionary,
                &vec![Some(StatValue::UInt64(10)), Some(StatValue::UInt64(20))]
            );
            assert_eq!(codes, &vec![Some(0), None]);
        }
        other => panic!("expected nullable dictionary batch, got {other:?}"),
    }
}

#[test]
fn nullable_utf8_dictionary_values_lower_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, VarBinViewArray};

    let codes = [0_u8, 1, 0]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values =
        VarBinViewArray::from_iter_nullable_str([Some("alpha"), None, Some("gamma")]).into_array();
    let chunk = DictArray::try_new(codes, values)
        .expect("nullable utf8 dictionary array")
        .into_array();
    let source_uri = DatasetUri::new("file:///tmp/nullable-utf8-dictionary.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-nullable-utf8-dict",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(
        input.batch.segment.layout.encoding,
        EncodingKind::Dictionary
    );
    assert_eq!(input.batch.segment.dtype, LogicalDType::Utf8);
    assert_eq!(
        input.batch.segment.nullability,
        ShardLoomNullability::Nullable
    );
    assert_eq!(input.batch.segment.stats.row_count, Some(3));
    assert_eq!(input.batch.segment.stats.null_count, Some(1));
    match &input.batch.values {
        EncodedValueBatch::Dictionary { dictionary, codes } => {
            assert_eq!(
                dictionary,
                &vec![
                    Some(StatValue::Utf8("alpha".to_string())),
                    None,
                    Some(StatValue::Utf8("gamma".to_string())),
                ]
            );
            assert_eq!(codes, &vec![Some(0), None, Some(0)]);
        }
        other => panic!("expected nullable utf8 dictionary batch, got {other:?}"),
    }
}

#[test]
fn nullable_run_end_reader_chunk_lowers_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::validity::Validity;
    use vortex::encodings::runend::RunEnd;

    let ends = [2_u64, 5]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values =
        PrimitiveArray::new(vec![5_i64, 9], Validity::from_iter([true, false])).into_array();
    let mut ctx = vortex::session::VortexSession::empty().create_execution_ctx();
    let chunk = RunEnd::try_new(ends, values, &mut ctx)
        .expect("nullable run-end array")
        .into_array();
    let source_uri = DatasetUri::new("file:///tmp/nullable-run-end.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-nullable-run-end",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(input.batch.segment.layout.encoding, EncodingKind::RunLength);
    assert_eq!(input.batch.segment.dtype, LogicalDType::Int64);
    assert_eq!(
        input.batch.segment.nullability,
        ShardLoomNullability::Nullable
    );
    assert_eq!(input.batch.segment.stats.row_count, Some(5));
    assert_eq!(input.batch.segment.stats.null_count, Some(3));
    assert_eq!(input.batch.segment.stats.run_count, Some(2));
    match &input.batch.values {
        EncodedValueBatch::RunLength { runs } => {
            assert_eq!(
                runs,
                &vec![
                    EncodedValueRun::new(Some(StatValue::Int64(5)), 2),
                    EncodedValueRun::new(None, 3),
                ]
            );
        }
        other => panic!("expected nullable run-length batch, got {other:?}"),
    }
}

#[test]
fn sparse_reader_chunk_patch_fill_lowers_into_encoded_kernel_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::scalar::Scalar;
    use vortex::encodings::sparse::Sparse;

    let indices = [1_u64, 3]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = [42_i32, 77]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let chunk = Sparse::try_new(
        indices,
        values,
        5,
        Scalar::from(0_i32),
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("sparse array")
    .into_array();
    let source_uri = DatasetUri::new("file:///tmp/sparse.vortex").expect("uri");

    let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
        &source_uri,
        "split-sparse",
        &chunk,
    )
    .expect("kernel inputs");

    assert_eq!(inputs.len(), 1);
    let input = inputs.first().expect("input");
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(input.mapping_evidence_complete());
    assert!(!input.has_forbidden_effects());
    assert_eq!(input.batch.segment.layout.encoding, EncodingKind::RunLength);
    assert_eq!(input.batch.segment.layout.layout, LayoutKind::Sparse);
    assert_eq!(input.batch.segment.dtype, LogicalDType::Int64);
    assert_eq!(input.batch.segment.stats.row_count, Some(5));
    assert_eq!(input.batch.segment.stats.null_count, Some(0));
    assert_eq!(input.batch.segment.stats.run_count, Some(5));
    match &input.batch.values {
        EncodedValueBatch::RunLength { runs } => {
            assert_eq!(
                runs,
                &vec![
                    EncodedValueRun::new(Some(StatValue::Int64(0)), 1),
                    EncodedValueRun::new(Some(StatValue::Int64(42)), 1),
                    EncodedValueRun::new(Some(StatValue::Int64(0)), 1),
                    EncodedValueRun::new(Some(StatValue::Int64(77)), 1),
                    EncodedValueRun::new(Some(StatValue::Int64(0)), 1),
                ]
            );
        }
        other => panic!("expected sparse patch/fill run-length batch, got {other:?}"),
    }
}

#[test]
fn count_where_executes_over_local_vortex_values() {
    let path = unique_vortex_path("count-where");
    write_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(3));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(3));
    assert_eq!(
        payload.get("prepared_olap_query_summary_written"),
        None,
        "local primitive execution must not emit query-answer sidecar write evidence"
    );
    assert_eq!(payload.get("prepared_olap_query_summary_hit"), None);
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert_eq!(report.max_parallelism_requested, 1);
    assert_eq!(report.scan_concurrency_per_worker, 1);
    assert!(report.arrays_read_count > 0);
    assert_eq!(report.reader_splits.len(), report.arrays_read_count);
    assert!(report.reader_splits.iter().all(|split| split.data_read));
    assert!(
        report
            .reader_splits
            .iter()
            .all(|split| !split.has_forbidden_effects())
    );
    let prepared_report = report
        .reader_generated_prepared_batch_report
        .as_ref()
        .expect("reader-generated prepared batch report");
    assert!(prepared_report.reader_generated_prepared_batches);
    assert_eq!(
        prepared_report.generated_batch_count,
        report.arrays_read_count
    );
    assert_eq!(prepared_report.total_rows, 3);
    assert!(prepared_report.kernel_input_lowering_blocked);
    assert!(!prepared_report.encoded_value_batch_available);
    assert!(!prepared_report.encoded_projection_batch_available);
    assert!(!prepared_report.runtime_execution_allowed);
    assert!(prepared_report.avoids_forbidden_effects());
    assert!(report.max_chunk_rows > 0);
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.projected_columns, [] as [String; 0]);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn local_scan_split_refs_bind_reader_backed_prepared_filter_batches() {
    let path = unique_vortex_path("reader-backed-bind");
    write_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let predicate = PredicateExpr::Compare {
        column: ColumnRef::new("metric").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(5),
    };
    let request = VortexQueryPrimitiveRequest::count_where(uri.clone(), predicate.clone());

    let local_report = execute_vortex_local_primitive(&request).expect("local report");
    let source = UniversalInputSource::from_dataset_uri(uri).expect("source");
    let source_uri = source.uri.clone().expect("source uri");
    let first_split = local_report
        .reader_splits
        .first()
        .expect("reader split")
        .clone();
    let batch = VortexSourceBackedEncodedValuePredicateBatch::new(
        source_uri,
        first_split.split_ref.clone(),
        VortexEncodedValuePredicateBatch::new(
            prepared_metric_segment("reader-split.metric", first_split.row_count as u64),
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(5)),
                row_count: first_split.row_count as u64,
            },
        ),
    )
    .expect("prepared batch");

    let bridge_report = execute_vortex_reader_backed_filter_from_encoded_value_batches(
        &predicate,
        &source,
        &local_report.reader_splits,
        &[batch],
    )
    .expect("bridge report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(
        bridge_report.status,
        VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches
    );
    assert!(bridge_report.data_read);
    assert!(bridge_report.runtime_execution_allowed);
    assert!(bridge_report.prepared_batch_split_refs_covered_by_reader);
    assert!(bridge_report.reader_validated_prepared_batches_consumed);
    assert!(!bridge_report.reader_generated_prepared_batches);
    assert!(bridge_report.avoids_forbidden_effects());
    assert!(!bridge_report.has_errors());
}

#[test]
fn count_where_comparison_excludes_null_values() {
    let path = unique_vortex_path("count-where-null");
    write_nullable_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(0),
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(4));
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn count_where_result_summary_json_renders_single_pass_with_and_without_scope() {
    let plain = count_where_result_summary_json(7, &PredicateExpr::AlwaysTrue, &[], false);
    let plain_json: serde_json::Value = serde_json::from_str(&plain).expect("valid json");
    assert_eq!(plain_json["count"], serde_json::json!(7));
    assert_eq!(
        plain_json["count_where_strategy"],
        serde_json::json!("exact_vortex_filter_count")
    );
    assert!(plain_json.get("scan_work_scope").is_none());
    assert!(
        plain_json
            .get("embedded_derived_column_rewrite_status")
            .is_none()
    );

    let scoped = count_where_result_summary_json(
        0,
        &PredicateExpr::AlwaysTrue,
        &["derived_col".into()],
        true,
    );
    let scoped_json: serde_json::Value = serde_json::from_str(&scoped).expect("valid json");
    assert_eq!(scoped_json["count"], serde_json::json!(0));
    assert_eq!(
        scoped_json["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        scoped_json["embedded_derived_column_rewrites"],
        serde_json::json!(["derived_col"])
    );
    assert!(scoped_json["scan_work_scope"].is_string());
}

#[test]
fn count_where_single_value_in_list_rewrites_to_equality_pushdown_without_materialization() {
    let path = unique_vortex_path("count-where-in-list");
    write_pivot_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("id").expect("column"),
            values: vec![StatValue::UInt64(1)],
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(2));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.projection_pushdown_applied);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_multi_value_in_list_uses_vortex_equality_disjunction() {
    let path = unique_vortex_path("count-where-multi-value-in-list");
    write_pivot_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("id").expect("column"),
            values: vec![StatValue::UInt64(1), StatValue::UInt64(2)],
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(3));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(3));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.projection_pushdown_applied);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn fast_null_primitive_accessors_use_validity_posture() {
    let direct = AggregateDirectColumnAccessor::Int64(vec![10, 20, 30]);
    let nullable = AggregateDirectColumnAccessor::NullableFloat64 {
        values: vec![1.0, 2.0, 3.0, 4.0],
        row_nulls: vec![false, true, false, true],
    };

    assert_eq!(
        fast_null_count_from_accessor(&direct, true).expect("direct null count"),
        0
    );
    assert_eq!(
        fast_null_count_from_accessor(&direct, false).expect("direct not-null count"),
        3
    );
    assert_eq!(
        fast_null_row_indices_from_accessor(&direct, false).expect("direct not-null indices"),
        vec![0, 1, 2]
    );
    assert_eq!(
        fast_null_count_from_accessor(&nullable, true).expect("nullable null count"),
        2
    );
    assert_eq!(
        fast_null_count_from_accessor(&nullable, false).expect("nullable not-null count"),
        2
    );
    assert_eq!(
        fast_null_row_indices_from_accessor(&nullable, true).expect("nullable null indices"),
        vec![1, 3]
    );
    assert_eq!(
        fast_null_row_indices_from_accessor(&nullable, false).expect("nullable not-null indices"),
        vec![0, 2]
    );
}

#[test]
fn fast_boolean_accessors_use_direct_values_and_validity() {
    let direct = AggregateDirectColumnAccessor::Boolean(vec![true, false, true]);
    let nullable = AggregateDirectColumnAccessor::NullableBoolean {
        values: vec![true, false, true, false],
        row_nulls: vec![false, true, false, false],
    };

    assert_eq!(
        fast_null_count_from_accessor(&direct, true).expect("direct bool null count"),
        0
    );
    assert_eq!(
        fast_null_count_from_accessor(&direct, false).expect("direct bool not-null count"),
        3
    );
    assert_eq!(
        fast_compare_count_from_accessor(
            &direct,
            "flag",
            ComparisonOp::Eq,
            &StatValue::Boolean(true),
        )
        .expect("direct bool compare count"),
        2
    );
    assert_eq!(
        fast_compare_row_indices_from_accessor(
            &direct,
            "flag",
            ComparisonOp::Eq,
            &StatValue::Boolean(true),
        )
        .expect("direct bool compare indices"),
        vec![0, 2]
    );
    assert_eq!(
        fast_compare_count_from_accessor(
            &nullable,
            "flag",
            ComparisonOp::Eq,
            &StatValue::Boolean(false),
        )
        .expect("nullable bool compare count"),
        1
    );
    assert_eq!(
        fast_compare_row_indices_from_accessor(
            &nullable,
            "flag",
            ComparisonOp::Eq,
            &StatValue::Boolean(false),
        )
        .expect("nullable bool compare indices"),
        vec![3]
    );
    assert_eq!(
        fast_in_list_count_from_accessor(
            &nullable,
            "flag",
            &[StatValue::Boolean(true), StatValue::Null],
            false,
        )
        .expect("nullable bool IN count"),
        3
    );
    assert_eq!(
        fast_in_list_row_indices_from_accessor(
            &nullable,
            "flag",
            &[StatValue::Boolean(true), StatValue::Null],
            false,
        )
        .expect("nullable bool IN indices"),
        vec![0, 1, 2]
    );
    assert_eq!(
        fast_in_list_row_indices_from_accessor(&nullable, "flag", &[StatValue::Null], true)
            .expect("nullable bool NOT IN null indices"),
        vec![0, 2, 3]
    );

    let mut distinct_values = AggregateDistinctSet::default();
    let update = aggregate_direct_count_distinct_update(&nullable, None, &mut distinct_values)
        .expect("nullable bool count-distinct update");
    assert_eq!(update.non_null_rows, 3);
    assert!(!update.dense_integer_preunion_used);
    assert_eq!(distinct_values.len(), 2);
    assert!(distinct_values.contains(&AggregateDistinctValue::Boolean(true)));
    assert!(distinct_values.contains(&AggregateDistinctValue::Boolean(false)));
    assert!(!distinct_values.contains(&AggregateDistinctValue::Null));
}

#[test]
fn boolean_vortex_arrays_promote_to_direct_accessors() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::BoolArray;

    let direct_array = BoolArray::from_iter([true, false, true]).into_array();
    let direct = aggregate_direct_column_accessor(
        "flag",
        &direct_array,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("direct boolean accessor");
    assert_eq!(direct.evidence_kind(), "direct_bool");
    assert_eq!(
        fast_compare_row_indices_from_accessor(
            &direct,
            "flag",
            ComparisonOp::Eq,
            &StatValue::Boolean(true),
        )
        .expect("direct boolean row indices"),
        vec![0, 2]
    );

    let nullable_array =
        BoolArray::from_iter([Some(true), None, Some(false), Some(true)]).into_array();
    let nullable = aggregate_direct_column_accessor(
        "flag",
        &nullable_array,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("nullable boolean accessor");
    assert_eq!(nullable.evidence_kind(), "direct_bool_nullable");
    assert_eq!(
        fast_null_row_indices_from_accessor(&nullable, true)
            .expect("nullable boolean null indices"),
        vec![1]
    );
    assert_eq!(
        fast_in_list_row_indices_from_accessor(
            &nullable,
            "flag",
            &[StatValue::Boolean(true)],
            false,
        )
        .expect("nullable boolean IN indices"),
        vec![0, 3]
    );
}

#[test]
fn fast_null_utf8_dictionary_uses_validity_without_string_materialization() {
    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1, 2, 0, 2],
        values: ["alpha", "beta", "gamma"]
            .into_iter()
            .map(|value| std::sync::Arc::<str>::from(value).into())
            .collect(),
        value_nulls: Some(vec![false, true, false]),
        row_nulls: Some(vec![false, false, true, false, false]),
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    };

    let null_count = fast_null_count_from_accessor(&accessor, true).expect("null count");
    let not_null_count = fast_null_count_from_accessor(&accessor, false).expect("not-null count");
    let null_indices = fast_null_row_indices_from_accessor(&accessor, true).expect("null indices");
    let not_null_indices =
        fast_null_row_indices_from_accessor(&accessor, false).expect("not-null indices");

    assert_eq!(null_count, 2);
    assert_eq!(not_null_count, 3);
    assert_eq!(null_indices, vec![1, 2]);
    assert_eq!(not_null_indices, vec![0, 3, 4]);
}

#[test]
fn fast_compare_utf8_dictionary_counts_dictionary_values_once() {
    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 1, 2, 2, 2],
        values: ["alpha", "beta", "gamma"]
            .into_iter()
            .map(|value| std::sync::Arc::<str>::from(value).into())
            .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    };

    let gamma_selected = fast_compare_count_from_accessor(
        &accessor,
        "url_domain",
        ComparisonOp::Eq,
        &StatValue::Utf8("gamma".to_string()),
    )
    .expect("dictionary comparison count");
    let greater_than_beta = fast_compare_count_from_accessor(
        &accessor,
        "url_domain",
        ComparisonOp::Gt,
        &StatValue::Utf8("beta".to_string()),
    )
    .expect("dictionary comparison range count");
    let not_alpha = fast_compare_count_from_accessor(
        &accessor,
        "url_domain",
        ComparisonOp::NotEq,
        &StatValue::Utf8("alpha".to_string()),
    )
    .expect("dictionary comparison not-eq count");
    let gamma_indices = fast_compare_row_indices_from_accessor(
        &accessor,
        "url_domain",
        ComparisonOp::Eq,
        &StatValue::Utf8("gamma".to_string()),
    )
    .expect("dictionary comparison row indices");

    assert_eq!(gamma_selected, 3);
    assert_eq!(greater_than_beta, 3);
    assert_eq!(not_alpha, 4);
    assert_eq!(gamma_indices, vec![3, 4, 5]);
}

#[test]
fn fast_compare_primitive_accessors_coerce_once_and_use_validity() {
    let direct = AggregateDirectColumnAccessor::Int64(vec![1, 2, 3, 4]);
    let nullable = AggregateDirectColumnAccessor::NullableUInt64 {
        values: vec![1, 2, 3, 4],
        row_nulls: vec![false, true, false, false],
    };
    let all_null = AggregateDirectColumnAccessor::NullableFloat64 {
        values: vec![1.0, 2.0],
        row_nulls: vec![true, true],
    };

    let direct_count = fast_compare_count_from_accessor(
        &direct,
        "metric",
        ComparisonOp::GtEq,
        &StatValue::Int64(3),
    )
    .expect("direct primitive compare count");
    let direct_indices = fast_compare_row_indices_from_accessor(
        &direct,
        "metric",
        ComparisonOp::GtEq,
        &StatValue::Int64(3),
    )
    .expect("direct primitive compare indices");
    let nullable_not_eq = fast_compare_count_from_accessor(
        &nullable,
        "metric",
        ComparisonOp::NotEq,
        &StatValue::UInt64(1),
    )
    .expect("nullable primitive compare count");
    let nullable_indices = fast_compare_row_indices_from_accessor(
        &nullable,
        "metric",
        ComparisonOp::NotEq,
        &StatValue::UInt64(1),
    )
    .expect("nullable primitive compare indices");
    let all_null_incompatible = fast_compare_count_from_accessor(
        &all_null,
        "metric",
        ComparisonOp::Gt,
        &StatValue::Utf8("not-a-number".to_string()),
    )
    .expect("all-null incompatible literal does not execute coercion");
    let null_literal =
        fast_compare_count_from_accessor(&direct, "metric", ComparisonOp::Eq, &StatValue::Null)
            .expect("null literal compare count");

    assert_eq!(direct_count, 2);
    assert_eq!(direct_indices, vec![2, 3]);
    assert_eq!(nullable_not_eq, 2);
    assert_eq!(nullable_indices, vec![2, 3]);
    assert_eq!(all_null_incompatible, 0);
    assert_eq!(null_literal, 0);
}

#[test]
fn fast_compare_utf8_dictionary_preserves_null_semantics() {
    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1, 2, 0, 2],
        values: ["alpha", "beta", "gamma"]
            .into_iter()
            .map(|value| std::sync::Arc::<str>::from(value).into())
            .collect(),
        value_nulls: Some(vec![false, true, false]),
        row_nulls: Some(vec![false, false, true, false, false]),
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    };

    let not_alpha = fast_compare_count_from_accessor(
        &accessor,
        "url_domain",
        ComparisonOp::NotEq,
        &StatValue::Utf8("alpha".to_string()),
    )
    .expect("dictionary comparison nullable not-eq count");
    let not_alpha_indices = fast_compare_row_indices_from_accessor(
        &accessor,
        "url_domain",
        ComparisonOp::NotEq,
        &StatValue::Utf8("alpha".to_string()),
    )
    .expect("dictionary comparison nullable not-eq indices");
    let null_literal = fast_compare_count_from_accessor(
        &accessor,
        "url_domain",
        ComparisonOp::Eq,
        &StatValue::Null,
    )
    .expect("dictionary comparison null literal count");

    assert_eq!(not_alpha, 1);
    assert_eq!(not_alpha_indices, vec![4]);
    assert_eq!(null_literal, 0);

    let row_nullable = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1, 2, 0],
        values: ["alpha", "beta", "gamma"]
            .into_iter()
            .map(|value| std::sync::Arc::<str>::from(value).into())
            .collect(),
        value_nulls: None,
        row_nulls: Some(vec![false, true, false, false]),
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    };
    assert_eq!(
        fast_compare_row_indices_from_accessor(
            &row_nullable,
            "url_domain",
            ComparisonOp::Eq,
            &StatValue::Utf8("missing.example".to_string()),
        )
        .expect("nullable dictionary no-match row indices"),
        Vec::<usize>::new()
    );
    assert_eq!(
        fast_compare_row_indices_from_accessor(
            &row_nullable,
            "url_domain",
            ComparisonOp::NotEq,
            &StatValue::Utf8("missing.example".to_string()),
        )
        .expect("nullable dictionary all-match non-null row indices"),
        vec![0, 2, 3]
    );
}

#[test]
fn fast_in_list_count_utf8_dictionary_counts_dictionary_values_once() {
    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 1, 2, 2, 2],
        values: ["alpha", "beta", "gamma"]
            .into_iter()
            .map(|value| std::sync::Arc::<str>::from(value).into())
            .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    };

    let selected = fast_in_list_count_from_accessor(
        &accessor,
        "url_domain",
        &[
            StatValue::Utf8("alpha".to_string()),
            StatValue::Utf8("gamma".to_string()),
        ],
        false,
    )
    .expect("dictionary count");
    let rejected = fast_in_list_count_from_accessor(
        &accessor,
        "url_domain",
        &[StatValue::Utf8("alpha".to_string())],
        true,
    )
    .expect("negated dictionary count");
    let selected_indices = fast_in_list_row_indices_from_accessor(
        &accessor,
        "url_domain",
        &[
            StatValue::Utf8("alpha".to_string()),
            StatValue::Utf8("gamma".to_string()),
        ],
        false,
    )
    .expect("dictionary row indices");
    let rejected_indices = fast_in_list_row_indices_from_accessor(
        &accessor,
        "url_domain",
        &[StatValue::Utf8("alpha".to_string())],
        true,
    )
    .expect("negated dictionary row indices");
    let no_match_indices = fast_in_list_row_indices_from_accessor(
        &accessor,
        "url_domain",
        &[StatValue::Utf8("delta".to_string())],
        false,
    )
    .expect("dictionary no-match row indices");
    let negated_no_match_indices = fast_in_list_row_indices_from_accessor(
        &accessor,
        "url_domain",
        &[StatValue::Utf8("delta".to_string())],
        true,
    )
    .expect("dictionary negated no-match row indices");
    let all_match_indices = fast_in_list_row_indices_from_accessor(
        &accessor,
        "url_domain",
        &[
            StatValue::Utf8("alpha".to_string()),
            StatValue::Utf8("beta".to_string()),
            StatValue::Utf8("gamma".to_string()),
        ],
        false,
    )
    .expect("dictionary all-match row indices");

    assert_eq!(selected, 5);
    assert_eq!(rejected, 4);
    assert_eq!(selected_indices, vec![0, 1, 3, 4, 5]);
    assert_eq!(rejected_indices, vec![2, 3, 4, 5]);
    assert_eq!(no_match_indices, Vec::<usize>::new());
    assert_eq!(negated_no_match_indices, vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(all_match_indices, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn fast_in_list_count_utf8_dictionary_preserves_null_and_negated_semantics() {
    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1, 2, 0],
        values: ["alpha", "", "gamma"]
            .into_iter()
            .map(|value| std::sync::Arc::<str>::from(value).into())
            .collect(),
        value_nulls: Some(vec![false, true, false]),
        row_nulls: Some(vec![false, false, true, false]),
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    };

    let null_selected =
        fast_in_list_count_from_accessor(&accessor, "url_domain", &[StatValue::Null], false)
            .expect("null dictionary count");
    let alpha_or_null_selected = fast_in_list_count_from_accessor(
        &accessor,
        "url_domain",
        &[StatValue::Utf8("alpha".to_string()), StatValue::Null],
        false,
    )
    .expect("null and string dictionary count");
    let not_alpha_selected = fast_in_list_count_from_accessor(
        &accessor,
        "url_domain",
        &[StatValue::Utf8("alpha".to_string())],
        true,
    )
    .expect("negated string dictionary count");
    let not_null_selected =
        fast_in_list_count_from_accessor(&accessor, "url_domain", &[StatValue::Null], true)
            .expect("negated null dictionary count");
    let null_indices =
        fast_in_list_row_indices_from_accessor(&accessor, "url_domain", &[StatValue::Null], false)
            .expect("null dictionary indices");
    let all_non_null_indices = fast_in_list_row_indices_from_accessor(
        &accessor,
        "url_domain",
        &[
            StatValue::Utf8("alpha".to_string()),
            StatValue::Utf8("gamma".to_string()),
        ],
        false,
    )
    .expect("dictionary all non-null IN-list indices");
    let missing_indices = fast_in_list_row_indices_from_accessor(
        &accessor,
        "url_domain",
        &[StatValue::Utf8("missing.example".to_string())],
        false,
    )
    .expect("dictionary missing IN-list indices");
    let negated_missing_indices = fast_in_list_row_indices_from_accessor(
        &accessor,
        "url_domain",
        &[StatValue::Utf8("missing.example".to_string())],
        true,
    )
    .expect("dictionary negated missing IN-list indices");
    let not_null_indices =
        fast_in_list_row_indices_from_accessor(&accessor, "url_domain", &[StatValue::Null], true)
            .expect("negated null dictionary indices");

    assert_eq!(null_selected, 2);
    assert_eq!(alpha_or_null_selected, 4);
    assert_eq!(not_alpha_selected, 2);
    assert_eq!(not_null_selected, 2);
    assert_eq!(null_indices, vec![1, 2]);
    assert_eq!(all_non_null_indices, vec![0, 3]);
    assert_eq!(missing_indices, Vec::<usize>::new());
    assert_eq!(negated_missing_indices, vec![0, 1, 2, 3]);
    assert_eq!(not_null_indices, vec![0, 3]);
}

#[test]
fn fast_in_list_primitive_accessors_coerce_once_and_use_validity() {
    let direct = AggregateDirectColumnAccessor::Int64(vec![1, 2, 3, 4]);
    let nullable = AggregateDirectColumnAccessor::NullableUInt64 {
        values: vec![1, 2, 3, 4],
        row_nulls: vec![false, true, false, false],
    };
    let nullable_float = AggregateDirectColumnAccessor::NullableFloat64 {
        values: vec![1.0, f64::NAN, -0.0, 0.0],
        row_nulls: vec![false, false, true, false],
    };
    let all_null = AggregateDirectColumnAccessor::NullableInt64 {
        values: vec![1, 2],
        row_nulls: vec![true, true],
    };

    let direct_count = fast_in_list_count_from_accessor(
        &direct,
        "metric",
        &[
            StatValue::Utf8("not-a-number".to_string()),
            StatValue::UInt64(2),
            StatValue::Int64(4),
        ],
        false,
    )
    .expect("direct primitive IN-list count");
    let direct_indices = fast_in_list_row_indices_from_accessor(
        &direct,
        "metric",
        &[
            StatValue::Utf8("not-a-number".to_string()),
            StatValue::UInt64(2),
            StatValue::Int64(4),
        ],
        false,
    )
    .expect("direct primitive IN-list indices");
    let direct_negated_indices =
        fast_in_list_row_indices_from_accessor(&direct, "metric", &[StatValue::Int64(2)], true)
            .expect("direct primitive NOT IN-list indices");
    let nullable_count = fast_in_list_count_from_accessor(
        &nullable,
        "metric",
        &[StatValue::UInt64(3), StatValue::Null],
        false,
    )
    .expect("nullable primitive IN-list count");
    let nullable_indices = fast_in_list_row_indices_from_accessor(
        &nullable,
        "metric",
        &[StatValue::UInt64(3), StatValue::Null],
        false,
    )
    .expect("nullable primitive IN-list indices");
    let nullable_not_null_indices =
        fast_in_list_row_indices_from_accessor(&nullable, "metric", &[StatValue::Null], true)
            .expect("nullable primitive NOT IN null indices");
    let float_indices = fast_in_list_row_indices_from_accessor(
        &nullable_float,
        "metric",
        &[
            StatValue::Float64(0.0),
            StatValue::Float64(f64::NAN),
            StatValue::Null,
        ],
        false,
    )
    .expect("float primitive IN-list indices");
    let all_null_incompatible = fast_in_list_count_from_accessor(
        &all_null,
        "metric",
        &[StatValue::Utf8("not-a-number".to_string())],
        false,
    )
    .expect("all-null incompatible positive IN-list count");
    let all_null_incompatible_negated = fast_in_list_count_from_accessor(
        &all_null,
        "metric",
        &[StatValue::Utf8("not-a-number".to_string())],
        true,
    )
    .expect("all-null incompatible negated IN-list count");

    assert_eq!(direct_count, 2);
    assert_eq!(direct_indices, vec![1, 3]);
    assert_eq!(direct_negated_indices, vec![0, 2, 3]);
    assert_eq!(nullable_count, 2);
    assert_eq!(nullable_indices, vec![1, 2]);
    assert_eq!(nullable_not_null_indices, vec![0, 2, 3]);
    assert_eq!(float_indices, vec![2, 3]);
    assert_eq!(all_null_incompatible, 0);
    assert_eq!(all_null_incompatible_negated, 2);
}

#[test]
fn count_where_single_null_in_list_rewrites_to_null_pushdown_without_materialization() {
    let path = unique_vortex_path("count-where-single-null-in-list");
    write_nullable_duplicate_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("value").expect("column"),
            values: vec![StatValue::Null],
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(3));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(3));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_negated_single_null_in_list_rewrites_to_not_null_pushdown() {
    let path = unique_vortex_path("count-where-negated-single-null-in-list");
    write_nullable_duplicate_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("value").expect("column"),
            values: vec![StatValue::Null],
            negated: true,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_negated_non_null_singleton_in_list_uses_null_preserving_pushdown() {
    let path = unique_vortex_path("count-where-negated-single-value-in-list");
    write_nullable_duplicate_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("value").expect("column"),
            values: vec![StatValue::UInt64(1)],
            negated: true,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(4));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(4));
    assert!(report.projection_pushdown_applied);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_mixed_null_in_list_uses_vortex_disjunction_pushdown() {
    let path = unique_vortex_path("count-where-mixed-null-in-list");
    write_nullable_duplicate_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("value").expect("column"),
            values: vec![StatValue::Null, StatValue::UInt64(2)],
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(4));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(4));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_negated_mixed_null_in_list_uses_vortex_conjunction_pushdown() {
    let path = unique_vortex_path("count-where-negated-mixed-null-in-list");
    write_nullable_duplicate_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("value").expect("column"),
            values: vec![StatValue::Null, StatValue::UInt64(2)],
            negated: true,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(1));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(1));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_empty_in_list_rewrites_to_always_false_without_data_read() {
    let path = unique_vortex_path("count-where-empty-in-list");
    write_pivot_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("id").expect("column"),
            values: Vec::new(),
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(0));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(0));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert_eq!(report.arrays_read_count, 0);
    assert!(!report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_negated_empty_in_list_rewrites_to_always_true_pushdown() {
    let path = unique_vortex_path("count-where-negated-empty-in-list");
    write_pivot_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::InList {
            column: ColumnRef::new("id").expect("column"),
            values: Vec::new(),
            negated: true,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(3));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(3));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_and_short_circuits_always_false_before_residual_string_scan() {
    let path = unique_vortex_path("count-where-and-short-circuit-false");
    write_string_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::And(vec![
            PredicateExpr::StringContains {
                column: ColumnRef::new("label").expect("column"),
                needle: String::new(),
                negated: true,
            },
            PredicateExpr::StringContains {
                column: ColumnRef::new("label").expect("column"),
                needle: "bad".to_string(),
                negated: false,
            },
        ]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(0));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(0));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert_eq!(report.arrays_read_count, 0);
    assert!(!report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn count_where_rerun_stays_on_single_vortex_artifact_without_query_summary_sidecar() {
    let path = unique_vortex_path("count-where-single-vortex-artifact");
    write_string_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("column"),
            needle: "bad".to_string(),
            negated: false,
        },
    );

    let first = execute_vortex_local_primitive(&request).expect("first report");
    assert_eq!(
        first.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(first.rows_scanned, 3);
    assert_eq!(first.rows_selected, Some(2));
    assert!(first.upstream_scan_called);
    assert!(first.data_read);
    assert!(!first.data_decoded);
    assert!(!first.data_materialized);
    let first_payload =
        count_where_values_json(first.result_summary.as_deref().expect("first summary"));
    assert_eq!(first_payload["count"], serde_json::json!(2));
    assert_eq!(
        first_payload.get("prepared_olap_query_summary_written"),
        None
    );
    assert_eq!(first_payload.get("prepared_olap_query_summary_hit"), None);
    assert_eq!(
        first_payload.get("prepared_olap_query_summary_consumed"),
        None
    );

    let second = execute_vortex_local_primitive(&request).expect("second report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(
        second.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(second.rows_scanned, 3);
    assert_eq!(second.rows_selected, Some(2));
    assert!(second.upstream_scan_called);
    assert!(second.data_read);
    assert!(!second.data_decoded);
    assert!(!second.data_materialized);
    assert!(!second.row_read);
    assert!(!second.fallback_execution_allowed);
    let second_payload =
        count_where_values_json(second.result_summary.as_deref().expect("second summary"));
    assert_eq!(second_payload["count"], serde_json::json!(2));
    assert_eq!(second_payload.get("prepared_olap_query_summary_hit"), None);
    assert_eq!(
        second_payload.get("prepared_olap_query_summary_consumed"),
        None
    );
    assert_eq!(second_payload.get("prepared_olap_raw_scan_avoided"), None);
}

#[test]
fn count_where_string_contains_uses_native_utf8_count_without_fallback() {
    let path = unique_vortex_path("count-where-string-contains");
    write_string_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("column"),
            needle: "bad".to_string(),
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(2));
    assert!(report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn count_where_contains_combines_embedded_length_prefilter_with_vortex_like_pushdown() {
    let path = unique_vortex_path("count-where-contains-length-prefilter");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::StringContains {
            column: ColumnRef::new("Referer").expect("column"),
            needle: "google".to_string(),
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(2));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(2));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!(["__shardloom_derived_utf8_len_Referer"])
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn count_where_contains_embedded_length_prefilter_can_footer_prune_impossible_needles() {
    let path = unique_vortex_path("count-where-contains-length-pruned");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::StringContains {
            column: ColumnRef::new("Referer").expect("column"),
            needle: "this-substring-is-longer-than-any-referer-in-the-fixture".to_string(),
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(0));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(0));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!(["__shardloom_derived_utf8_len_Referer"])
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.arrays_read_count, 0);
    assert!(!report.data_read);
    assert!(!report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn count_where_empty_contains_rewrites_to_validity_pushdown_without_string_scan() {
    let path = unique_vortex_path("count-where-empty-contains-validity");
    write_string_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("column"),
            needle: String::new(),
            negated: false,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(3));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(3));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn count_where_negated_empty_contains_rewrites_to_always_false_without_scan() {
    let path = unique_vortex_path("count-where-negated-empty-contains");
    write_string_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("column"),
            needle: String::new(),
            negated: true,
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(0));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(0));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.projection_pushdown_applied);
    assert_eq!(report.arrays_read_count, 0);
    assert!(!report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn mixed_predicate_count_pushes_string_and_scalar_predicates_into_vortex_filter() {
    let path = unique_vortex_path("count-where-mixed-predicate");
    write_mixed_string_filter_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::And(vec![
            PredicateExpr::StringContains {
                column: ColumnRef::new("URL").expect("column"),
                needle: "google".to_string(),
                negated: false,
            },
            PredicateExpr::Compare {
                column: ColumnRef::new("SearchPhrase").expect("column"),
                op: ComparisonOp::NotEq,
                value: StatValue::Utf8(String::new()),
            },
        ]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(2));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn simple_aggregate_rewrites_length_and_domain_to_embedded_vortex_columns() {
    let path = unique_vortex_path("simple-aggregate-embedded-derived");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate = VortexSimpleAggregateRequest::new(vec![
        crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
        crate::VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("Referer").expect("referer")),
            "avg_len".to_string(),
        )
        .with_value_transform("length"),
    ])
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("Referer").expect("referer"),
        "url_domain",
    )]);
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate);
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("Referer").expect("referer"),
        op: ComparisonOp::NotEq,
        value: StatValue::Utf8(String::new()),
    });

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(
        report
            .projected_columns
            .iter()
            .all(|column| !is_shardloom_hidden_derived_column(column)),
        "user-facing aggregate output should not expose hidden metadata columns"
    );
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!([
            "__shardloom_derived_url_domain_Referer",
            "__shardloom_derived_utf8_len_Referer"
        ])
    );
    assert_eq!(payload["rows"], serde_json::json!(2));
    let values = payload["values"].as_array().expect("grouped rows");
    let google = values
        .iter()
        .find(|row| row["domain"] == serde_json::json!("google.com"))
        .expect("google group");
    let example = values
        .iter()
        .find(|row| row["domain"] == serde_json::json!("example.com"))
        .expect("example group");
    assert_eq!(google["rows"], serde_json::json!(2));
    assert_eq!(example["rows"], serde_json::json!(1));
    assert_eq!(
        google["avg_len"],
        serde_json::json!(
            (u64::try_from("https://www.google.com/search".len()).expect("len")
                + u64::try_from("http://www.google.com/ads".len()).expect("len"))
                as f64
                / 2.0
        )
    );
    assert_eq!(
        example["avg_len"],
        serde_json::json!(u64::try_from("https://example.com/page".len()).expect("len") as f64)
    );
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn simple_aggregate_rewrites_extract_minute_to_embedded_vortex_column() {
    let path = unique_vortex_path("simple-aggregate-embedded-minute");
    write_embedded_derived_minute_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("UserID").expect("user id"),
            ColumnRef::new("SearchPhrase").expect("search phrase"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "EventMinute".to_string(),
        ColumnRef::new("EventTime").expect("event time"),
        "extract_minute",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate).with_source_order_limit(2);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert!(report.projection_pushdown_applied);
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!(["__shardloom_derived_extract_minute_EventTime"])
    );
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_streaming_numeric_minute_string_count_topk"
    );
    assert_eq!(
        payload["aggregate_key_encoding_mode"],
        "typed_numeric_minute_dictionary_code_hash_key"
    );
    let values = payload["values"].as_array().expect("grouped rows");
    assert_eq!(values.len(), 2);
    assert_eq!(values[0]["UserID"], serde_json::json!(7));
    assert_eq!(values[0]["SearchPhrase"], serde_json::json!("beta"));
    assert_eq!(values[0]["EventMinute"], serde_json::json!(2));
    assert_eq!(values[0]["rows"], serde_json::json!(2));
    assert_eq!(values[1]["UserID"], serde_json::json!(42));
    assert_eq!(values[1]["SearchPhrase"], serde_json::json!("alpha"));
    assert_eq!(values[1]["EventMinute"], serde_json::json!(1));
    assert_eq!(values[1]["rows"], serde_json::json!(2));
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn simple_aggregate_rewrites_date_trunc_minute_to_embedded_vortex_column() {
    let path = unique_vortex_path("simple-aggregate-embedded-date-trunc-minute");
    write_embedded_derived_minute_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("SearchPhrase").expect("search phrase")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "EventMinuteBucket".to_string(),
        ColumnRef::new("EventTime").expect("event time"),
        "date_trunc_minute",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate).with_source_order_limit(3);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(report.projection_pushdown_applied);
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!(["__shardloom_derived_date_trunc_minute_EventTime"])
    );
    let values = payload["values"].as_array().expect("grouped rows");
    assert_eq!(values.len(), 3);
    assert_eq!(values[0]["SearchPhrase"], serde_json::json!("alpha"));
    assert_eq!(values[0]["EventMinuteBucket"], serde_json::json!(60));
    assert_eq!(values[0]["rows"], serde_json::json!(2));
    assert_eq!(values[1]["SearchPhrase"], serde_json::json!("beta"));
    assert_eq!(values[1]["EventMinuteBucket"], serde_json::json!(120));
    assert_eq!(values[1]["rows"], serde_json::json!(2));
    assert_eq!(values[2]["SearchPhrase"], serde_json::json!("gamma"));
    assert_eq!(values[2]["EventMinuteBucket"], serde_json::json!(60));
    assert_eq!(values[2]["rows"], serde_json::json!(1));
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn simple_aggregate_rewrites_time_transform_measures_to_embedded_vortex_columns() {
    let path = unique_vortex_path("simple-aggregate-embedded-time-measures");
    write_embedded_derived_minute_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate = VortexSimpleAggregateRequest::new(vec![
        crate::VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("EventTime").expect("event time")),
            "minute_sum".to_string(),
        )
        .with_value_transform("extract_minute"),
        crate::VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("EventTime").expect("event time")),
            "bucket_avg".to_string(),
        )
        .with_value_transform("date_trunc_minute"),
        crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("EventTime").expect("event time")),
            "minute_distinct".to_string(),
        )
        .with_value_transform("extract_minute"),
    ]);
    let request = VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(report.projection_pushdown_applied);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
    assert!(
        report
            .embedded_layout
            .per_column_metadata_contract
            .contains("shardloom_derived_extract_minute_EventTime")
    );
    assert!(
        report
            .embedded_layout
            .per_column_metadata_contract
            .contains("derived=extract_minute")
    );
    assert_eq!(
        report.embedded_layout.operator_selection_metadata_status,
        "per_column_embedded_metadata_available_for_operator_selection"
    );
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!([
            "__shardloom_derived_extract_minute_EventTime",
            "__shardloom_derived_date_trunc_minute_EventTime"
        ])
    );
    let values = payload["values"].as_object().expect("scalar result row");
    assert_eq!(values["minute_sum"], serde_json::json!(7.0));
    assert_eq!(values["bucket_avg"], serde_json::json!(84.0));
    assert_eq!(values["minute_distinct"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_rewrites_time_transform_measures_to_embedded_vortex_columns() {
    let path = unique_vortex_path("grouped-aggregate-embedded-time-measures");
    write_embedded_derived_minute_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("SearchPhrase").expect("search phrase")],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("EventTime").expect("event time")),
                "minute_sum".to_string(),
            )
            .with_value_transform("extract_minute"),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("EventTime").expect("event time")),
                "bucket_avg".to_string(),
            )
            .with_value_transform("date_trunc_minute"),
        ],
    );
    let request = VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(report.projection_pushdown_applied);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!([
            "__shardloom_derived_extract_minute_EventTime",
            "__shardloom_derived_date_trunc_minute_EventTime"
        ])
    );
    let values = payload["values"].as_array().expect("grouped rows");
    let alpha = values
        .iter()
        .find(|row| row["SearchPhrase"] == serde_json::json!("alpha"))
        .expect("alpha group");
    let beta = values
        .iter()
        .find(|row| row["SearchPhrase"] == serde_json::json!("beta"))
        .expect("beta group");
    let gamma = values
        .iter()
        .find(|row| row["SearchPhrase"] == serde_json::json!("gamma"))
        .expect("gamma group");
    assert_eq!(alpha["rows"], serde_json::json!(2));
    assert_eq!(alpha["minute_sum"], serde_json::json!(2.0));
    assert_eq!(alpha["bucket_avg"], serde_json::json!(60.0));
    assert_eq!(beta["rows"], serde_json::json!(2));
    assert_eq!(beta["minute_sum"], serde_json::json!(4.0));
    assert_eq!(beta["bucket_avg"], serde_json::json!(120.0));
    assert_eq!(gamma["rows"], serde_json::json!(1));
    assert_eq!(gamma["minute_sum"], serde_json::json!(1.0));
    assert_eq!(gamma["bucket_avg"], serde_json::json!(60.0));
}

#[test]
fn simple_aggregate_rewrites_url_domain_measures_to_embedded_vortex_column() {
    let path = unique_vortex_path("simple-aggregate-embedded-domain-measures");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate = VortexSimpleAggregateRequest::new(vec![
        crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("Referer").expect("referer")),
            "domain_count".to_string(),
        )
        .with_value_transform("url_domain"),
        crate::VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new("Referer").expect("referer")),
            "first_domain".to_string(),
        )
        .with_value_transform("url_domain"),
        crate::VortexSimpleAggregateMeasure::new(
            "max",
            Some(ColumnRef::new("Referer").expect("referer")),
            "last_domain".to_string(),
        )
        .with_value_transform("url_domain"),
    ]);
    let request = VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(report.projection_pushdown_applied);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!(["__shardloom_derived_url_domain_Referer"])
    );
    assert!(
        payload["aggregate_accessor_per_column_metadata_contract"]
            .as_str()
            .is_some_and(|value| value.contains("derived=url_domain"))
    );
    assert_eq!(
        payload["aggregate_accessor_operator_selection_metadata_status"],
        serde_json::json!("per_column_embedded_metadata_available_for_operator_selection")
    );
    let values = payload["values"].as_object().expect("scalar result row");
    assert_eq!(values["domain_count"], serde_json::json!(3));
    assert_eq!(values["first_domain"], serde_json::json!(""));
    assert_eq!(values["last_domain"], serde_json::json!("google.com"));
}

#[test]
fn count_where_rewrites_non_empty_string_predicate_to_embedded_length_column() {
    let path = unique_vortex_path("count-where-embedded-derived-length");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("Referer").expect("referer"),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_selected, Some(3));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(3));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!(["__shardloom_derived_utf8_len_Referer"])
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn count_where_rewrites_non_empty_string_predicate_to_dictionary_backed_length_column() {
    let path = unique_vortex_path("count-where-dictionary-embedded-derived-length");
    write_dictionary_embedded_derived_url_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("URL").expect("url"),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(3));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(3));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!(["__shardloom_derived_utf8_len_URL"])
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn simple_aggregate_rewrites_dictionary_backed_domain_and_length_to_code_group_partials() {
    let path = unique_vortex_path("simple-aggregate-dictionary-embedded-code-partials");
    write_dictionary_embedded_derived_url_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate = VortexSimpleAggregateRequest::new(vec![
        crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
        crate::VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("URL").expect("url")),
            "avg_len".to_string(),
        )
        .with_value_transform("length"),
    ])
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("URL").expect("url"),
        "url_domain",
    )]);
    let request = VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(report.projection_pushdown_applied);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(
        payload["embedded_derived_column_rewrite_status"],
        serde_json::json!("applied")
    );
    assert_eq!(
        payload["embedded_derived_column_rewrites"],
        serde_json::json!([
            "__shardloom_derived_url_domain_URL",
            "__shardloom_derived_utf8_len_URL"
        ])
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        serde_json::json!("dictionary_group_compact_count_sum_avg_group_update")
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        serde_json::json!("chunk_dictionary_compact_count_sum_avg_group_state")
    );
    assert_eq!(
        payload["group_state_mode"],
        serde_json::json!("chunk_dictionary_compact_measure_code_map")
    );
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    let values = payload["values"].as_array().expect("grouped rows");
    let google = values
        .iter()
        .find(|row| row["domain"] == serde_json::json!("google.com"))
        .expect("google group");
    let docs = values
        .iter()
        .find(|row| row["domain"] == serde_json::json!("docs.rs"))
        .expect("docs group");
    assert_eq!(google["rows"], serde_json::json!(2));
    assert_eq!(
        google["avg_len"],
        serde_json::json!(
            u64::try_from("https://www.google.com/search".len()).expect("len") as f64
        )
    );
    assert_eq!(docs["rows"], serde_json::json!(1));
    assert_eq!(
        docs["avg_len"],
        serde_json::json!(u64::try_from("https://docs.rs/crate".len()).expect("len") as f64)
    );
}

#[test]
fn simple_aggregate_preserves_source_dictionary_fusion_for_ordered_domain_length() {
    assert_source_dictionary_fusion_for_ordered_domain_length("url_domain");
}

#[test]
fn independent_regex_domain_preserves_source_dictionary_fusion_with_embedded_lengths() {
    assert_source_dictionary_fusion_for_ordered_domain_length("regex_domain");
}

fn assert_source_dictionary_fusion_for_ordered_domain_length(transform: &str) {
    let path = unique_vortex_path("simple-aggregate-dictionary-fused-domain-length");
    write_dictionary_embedded_derived_url_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate = VortexSimpleAggregateRequest::new(vec![
        crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
        crate::VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("URL").expect("url")),
            "avg_len".to_string(),
        )
        .with_value_transform("length"),
        crate::VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("URL").expect("url")),
            "sum_len".to_string(),
        )
        .with_value_transform("length"),
        crate::VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new("URL").expect("url")),
            "min_url".to_string(),
        ),
    ])
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("URL").expect("url"),
        transform,
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let mut request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate).with_source_order_limit(2);
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("URL").expect("url"),
        op: ComparisonOp::NotEq,
        value: StatValue::Utf8(String::new()),
    });

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(3));
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert!(
        payload
            .get("embedded_derived_column_rewrite_status")
            .is_none(),
        "ordered source-dictionary URL-domain/length aggregates should preserve the fused dictionary lane instead of rewriting to separate hidden columns"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        serde_json::json!("transformed_dictionary_dense_general_measure_group_update")
    );
    assert_eq!(
        payload["expression_fusion_strategy"],
        serde_json::json!("dictionary_weighted_transform_fusion")
    );
    assert_eq!(
        payload["group_state_mode"],
        serde_json::json!("dense_transformed_dictionary_general_accumulators")
    );
    assert_eq!(
        payload["aggregate_accessor_summary"],
        serde_json::json!("URL:chunk_utf8_dictionary")
    );
    let values = payload["values"].as_array().expect("grouped rows");
    assert_eq!(values.len(), 2);
    assert_eq!(values[0]["domain"], serde_json::json!("google.com"));
    assert_eq!(values[0]["rows"], serde_json::json!(2));
    assert_eq!(
        values[0]["avg_len"],
        serde_json::json!(
            u64::try_from("https://www.google.com/search".len()).expect("len") as f64
        )
    );
    assert_eq!(values[1]["domain"], serde_json::json!("docs.rs"));
    assert_eq!(values[1]["rows"], serde_json::json!(1));
}

#[test]
fn scalar_string_count_distinct_reports_dictionary_arc_state() {
    let path = unique_vortex_path("count-distinct-dictionary-arc-state");
    write_dictionary_embedded_derived_url_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let aggregate =
        VortexSimpleAggregateRequest::new(vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("URL").expect("url")),
            "unique_urls".to_string(),
        )]);
    let request = VortexQueryPrimitiveRequest::simple_aggregate(uri, aggregate);

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.state_budget.state_family,
        "scalar_count_distinct_state+direct_dictionary_arc"
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"dictionary_arc_distinct_state".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"dictionary_string_clone_bypass".to_string())
    );
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(
        payload["distinct_state_strategy"],
        "dictionary_arc_direct_exact"
    );
    let values = payload["values"].as_object().expect("scalar values");
    assert_eq!(values["unique_urls"], serde_json::json!(3));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn count_where_footer_pruning_avoids_scan_decode_and_materialization() {
    let path = unique_vortex_path("count-where-footer-pruned");
    write_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::Gt,
            value: StatValue::Int64(99),
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_selected, Some(0));
    let payload = count_where_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["count"], serde_json::json!(0));
    assert!(!report.data_read);
    assert!(!report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.upstream_scan_called);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.reader_splits.len(), 0);
    assert_eq!(report.evidence_collector.status, "measured");
    assert_eq!(report.evidence_collector.full_split_count, 0);
    assert_eq!(report.evidence_collector.compact_signature_count, 0);
    assert_eq!(report.evidence_collector.encoded_kernel_input_count, 0);
    assert!(report.evidence_collector.full_replay_evidence_preserved);
    assert!(report.embedded_layout.metadata_persisted_in_artifact);
    assert!(report.embedded_layout.metadata_first_pruning_available);
    assert!(report.embedded_layout.metadata_first_pruning_consulted);
    assert!(report.embedded_layout.metadata_pruned_entire_input);
    assert_eq!(report.embedded_layout.footer_row_count, 5);
    assert_ne!(report.embedded_layout.root_layout_encoding, "not_available");
    assert_ne!(
        report.embedded_layout.layout_encoding_inventory,
        "not_available"
    );
    assert_eq!(report.embedded_layout.layout_inventory_nodes_inspected, 1);
    assert_eq!(
        report.embedded_layout.layout_encoding_inventory,
        DEFERRED_LAYOUT_INVENTORY
    );
    assert!(
        report
            .embedded_layout
            .layout_inventory_scope
            .contains("full_tree_deferred")
    );
    assert!(
        report
            .embedded_layout
            .segment_membership_status
            .starts_with("segment_map_available")
    );
    assert_eq!(
        report.embedded_layout.layout_reader_cache_status,
        "enabled_for_local_open"
    );
    assert_eq!(report.embedded_layout.selected_segment_count, 0);
    assert!(report.embedded_layout.skipped_segment_count > 0);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.fallback_execution_allowed);
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    assert!(
        certificate.is_certified(),
        "metadata-pruned filter should certify as native Vortex metadata I/O without scan"
    );
}

#[test]
fn evidence_collector_compacts_repeated_signatures_without_dropping_replay_refs() {
    let uri = DatasetUri::new("memory://local-vortex-evidence-collector").expect("uri");
    let source = UniversalInputSource::from_dataset_uri(uri.clone()).expect("source");
    let splits = vec![
        VortexReaderBackedSplitEvidence::local_scan_chunk(
            uri.clone(),
            0,
            10,
            "Struct<value:Int64>",
            "vortex.fastlanes",
            1,
            1,
        )
        .expect("split"),
        VortexReaderBackedSplitEvidence::local_scan_chunk(
            uri.clone(),
            1,
            20,
            "Struct<value:Int64>",
            "vortex.fastlanes",
            1,
            1,
        )
        .expect("split"),
        VortexReaderBackedSplitEvidence::local_scan_chunk(
            uri,
            2,
            30,
            "Struct<value:Int64>",
            "vortex.runend",
            1,
            2,
        )
        .expect("split"),
    ];
    let prepared_report = plan_vortex_reader_generated_prepared_batch_envelopes(&source, &splits);

    let report =
        VortexLocalPrimitiveEvidenceCollectorReport::from_scan(17, 11, &splits, &prepared_report);

    assert_eq!(report.status, "measured");
    assert_eq!(
        report.strategy,
        "compact_signature_counts_with_full_replay_split_evidence"
    );
    assert_eq!(report.control_plane_micros, 17);
    assert_eq!(report.evidence_collection_micros, 11);
    assert_eq!(report.full_split_count, 3);
    assert_eq!(report.compact_signature_count, 2);
    assert_eq!(report.encoded_kernel_input_count, 0);
    assert_eq!(report.row_count, 60);
    assert_eq!(
        report.representative_first_split_ref.as_deref(),
        Some("vortex-local-scan-chunk-0")
    );
    assert_eq!(
        report.representative_last_split_ref.as_deref(),
        Some("vortex-local-scan-chunk-2")
    );
    assert!(report.full_replay_evidence_preserved);
}

#[test]
fn simple_aggregate_footer_pruning_avoids_scan_decode_and_materialization() {
    let path = unique_vortex_path("simple-aggregate-footer-pruned");
    write_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::new(vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )]),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("value").expect("column"),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(99),
    });

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(0));
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["values"]["rows"], serde_json::json!(0));
    assert!(!report.data_read);
    assert!(!report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.upstream_scan_called);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.reader_splits.len(), 0);
    assert!(report.embedded_layout.metadata_first_pruning_consulted);
    assert!(report.embedded_layout.metadata_pruned_entire_input);
    assert_eq!(report.embedded_layout.selected_segment_count, 0);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    assert!(
        certificate.is_certified(),
        "metadata-pruned aggregate should certify as native Vortex metadata I/O without scan"
    );
}

#[test]
fn sort_rows_footer_pruning_avoids_scan_decode_and_materialization() {
    let path = unique_vortex_path("sort-rows-footer-pruned");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("value").expect("column"),
            ColumnRef::new("metric").expect("column"),
        ]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::Gt,
            value: StatValue::Int64(99),
        }),
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("value", true)]),
        2,
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(0));
    let payload = simple_aggregate_values_json(report.result_summary.as_deref().expect("summary"));
    assert_eq!(payload["rows"], serde_json::json!(0));
    assert_eq!(payload["values"], serde_json::json!([]));
    assert!(!report.data_read);
    assert!(!report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.upstream_scan_called);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.reader_splits.len(), 0);
    assert!(report.embedded_layout.metadata_first_pruning_consulted);
    assert!(report.embedded_layout.metadata_pruned_entire_input);
    assert_eq!(report.embedded_layout.selected_segment_count, 0);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.fallback_execution_allowed);
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    assert!(
        certificate.is_certified(),
        "metadata-pruned sort should certify as native Vortex metadata I/O without scan"
    );
}

#[test]
fn filter_predicate_executes_over_local_vortex_values() {
    let path = unique_vortex_path("filter");
    write_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::filter(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(3));
    assert_eq!(report.rows_projected, None);
    assert_eq!(report.result_summary.as_deref(), Some("3"));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert_eq!(report.max_parallelism_requested, 2);
    assert_eq!(report.scan_concurrency_per_worker, 2);
    assert_eq!(report.evidence_collector.status, "measured");
    assert_eq!(
        report.evidence_collector.full_split_count,
        report.reader_splits.len()
    );
    assert!(report.evidence_collector.compact_signature_count > 0);
    assert_eq!(
        report.evidence_collector.encoded_kernel_input_count,
        report
            .reader_generated_prepared_batch_report
            .as_ref()
            .map_or(0, |batch| batch.encoded_kernel_input_count)
    );
    assert_eq!(
        report.evidence_collector.row_count,
        report
            .reader_splits
            .iter()
            .map(|split| split.row_count as u64)
            .sum::<u64>()
    );
    assert!(report.evidence_collector.full_replay_evidence_preserved);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.projection_pushdown_applied);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn projection_reports_projected_columns_from_local_vortex() {
    let path = unique_vortex_path("project");
    write_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::project(
        uri,
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_projected, Some(5));
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_projection_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn project_and_filter_apply_source_order_limit() {
    let path = unique_vortex_path("project-filter-limit");
    write_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let project_request = VortexQueryPrimitiveRequest::project(
        uri.clone(),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
    )
    .with_source_order_limit(2);
    let filter_request = VortexQueryPrimitiveRequest::filter(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
    )
    .with_source_order_limit(2);

    let project = execute_vortex_local_primitive(&project_request).expect("project report");
    let filter = execute_vortex_local_primitive(&filter_request).expect("filter report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(
        project.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert_eq!(project.rows_projected, Some(2));
    assert_eq!(project.source_order_limit_requested, Some(2));
    assert!(project.source_order_limit_applied);
    assert_eq!(project.source_order_limit_rows_output, Some(2));
    assert!(project.projection_pushdown_applied);
    assert!(!project.filter_pushdown_applied);
    assert!(!project.data_decoded);
    assert!(!project.data_materialized);

    assert_eq!(filter.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(filter.rows_selected, Some(2));
    assert_eq!(filter.source_order_limit_requested, Some(2));
    assert!(filter.source_order_limit_applied);
    assert_eq!(filter.source_order_limit_input_rows, Some(3));
    assert_eq!(filter.source_order_limit_rows_output, Some(2));
    assert!(filter.filter_pushdown_applied);
    assert!(!filter.projection_pushdown_applied);
    assert!(!filter.data_decoded);
    assert!(!filter.data_materialized);
}

#[test]
fn filter_and_project_uses_single_vortex_scan_pushdown_path() {
    let path = unique_vortex_path("filter-project");
    write_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(3));
    assert_eq!(report.rows_projected, Some(3));
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert_eq!(report.max_parallelism_requested, 2);
    assert_eq!(report.scan_concurrency_per_worker, 2);
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn filter_and_project_contains_uses_native_pushdown_without_fallback() {
    let path = unique_vortex_path("filter-project-contains");
    write_string_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        uri,
        PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("column"),
            needle: "bad".to_string(),
            negated: false,
        },
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("column")]),
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert!(report.upstream_scan_called);
    assert!(report.upstream_filter_expression_used);
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn distinct_rows_apply_pushdown_predicate_before_row_key_state_without_fallback() {
    let path = unique_vortex_path("distinct-rows-residual");
    write_string_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
        Some(PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("label"),
            needle: "bad".to_string(),
            negated: false,
        }),
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DistinctRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.data_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn distinct_rows_pushdown_predicate_columns_do_not_widen_distinct_key() {
    let path = unique_vortex_path("distinct-rows-residual-key-width");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("value")]),
        Some(PredicateExpr::InList {
            column: ColumnRef::new("metric").expect("metric"),
            values: vec![StatValue::Int64(20), StatValue::Int64(99)],
            negated: false,
        }),
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DistinctRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(1));
    assert_eq!(report.rows_projected, Some(1));
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn distinct_rows_embedded_length_footer_pruning_skips_row_key_state_without_scan() {
    let path = unique_vortex_path("distinct-rows-embedded-length-pruned");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("Referer").expect("referer")]),
        Some(PredicateExpr::StringContains {
            column: ColumnRef::new("Referer").expect("referer"),
            needle: "this-substring-is-longer-than-any-referer-in-the-fixture".to_string(),
            negated: false,
        }),
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DistinctRows
    );
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(0));
    assert_eq!(report.rows_projected, Some(0));
    assert_eq!(report.projected_columns, vec!["Referer".to_string()]);
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.upstream_scan_called);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.reader_splits.len(), 0);
    assert!(report.embedded_layout.metadata_first_pruning_consulted);
    assert!(report.embedded_layout.metadata_pruned_entire_input);
    assert!(
        report
            .result_summary
            .as_deref()
            .expect("summary")
            .contains("__shardloom_derived_utf8_len_Referer")
    );
    assert!(!report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn drop_duplicate_rows_apply_pushdown_predicate_before_row_key_state_without_fallback() {
    let path = unique_vortex_path("drop-duplicates-residual");
    write_string_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
    );
    request.predicate = Some(PredicateExpr::StringContains {
        column: ColumnRef::new("label").expect("label"),
        needle: "bad".to_string(),
        negated: false,
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DropDuplicateRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.data_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn drop_duplicate_rows_embedded_length_footer_pruning_skips_row_key_state_without_scan() {
    let path = unique_vortex_path("drop-duplicates-embedded-length-pruned");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("Referer").expect("referer")]),
        ProjectionRequest::columns(vec![ColumnRef::new("Referer").expect("referer")]),
    );
    request.predicate = Some(PredicateExpr::StringContains {
        column: ColumnRef::new("Referer").expect("referer"),
        needle: "this-substring-is-longer-than-any-referer-in-the-fixture".to_string(),
        negated: false,
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DropDuplicateRows
    );
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(0));
    assert_eq!(report.rows_projected, Some(0));
    assert_eq!(report.projected_columns, vec!["Referer".to_string()]);
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.upstream_scan_called);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.reader_splits.len(), 0);
    assert!(report.embedded_layout.metadata_first_pruning_consulted);
    assert!(report.embedded_layout.metadata_pruned_entire_input);
    assert!(
        report
            .result_summary
            .as_deref()
            .expect("summary")
            .contains("__shardloom_derived_utf8_len_Referer")
    );
    assert!(!report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.full_stream_collected);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn filter_and_project_applies_source_order_residual_limit_after_scan_pushdown() {
    let uri = checked_in_struct_fixture_uri();
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        uri,
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let fixture =
        local_primitive_correctness_fixture_for_request(&request, &report).expect("limit fixture");
    let execution_certificate = local_primitive_execution_certificate(&fixture, &request, &report)
        .expect("execution certificate");

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(3));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.fallback_execution_allowed);
    assert!(certificate.is_certified());
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "filter,project"
    );
    assert_eq!(
        certificate
            .source_pushdown_report
            .rejected_operation_order(),
        "limit_pushdown"
    );
    assert_eq!(
        certificate
            .source_pushdown_report
            .residual_expression
            .as_deref(),
        Some("source_order_limit:2")
    );
    assert_eq!(
        fixture.id.as_str(),
        "vortex-local-filter-project-limit-struct-five"
    );
    assert!(execution_certificate.is_certified());
    assert!(!execution_certificate.fallback_attempted);
}

#[test]
fn distinct_rows_materialize_bounded_vortex_rows_without_fallback() {
    let path = unique_vortex_path("distinct-rows");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(2),
        }),
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DistinctRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    let summary = report.result_summary.as_deref().expect("summary");
    assert!(summary.contains("distinct_rows=2"));
    assert!(summary.contains("projected_columns=value"));
    assert!(summary.contains("distinct_state_strategy=hash_row_key_exact"));
    assert!(
        report
            .state_budget
            .state_family
            .contains("row_key_distinct_state+hash_row_key")
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"hash_row_key_state".to_string())
    );
    assert_eq!(report.max_parallelism_requested, 2);
    assert_eq!(report.scan_concurrency_per_worker, 1);
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(2));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);

    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    assert!(certificate.is_certified());
    assert_eq!(certificate.status(), "certified");
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->decoded_columnar,decoded_columnar->materialized_rows"
    );
    assert_eq!(
        certificate.materialization_boundary_order(),
        "local_primitive.distinct_rows.bounded_materialization"
    );
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "filter,project,distinct"
    );
    assert!(certificate.sink_requirement_report.requires_rows);
    assert!(certificate.adapter_fidelity_report.materialization_required);
    assert!(!certificate.side_effects.fallback_attempted);
}

#[test]
fn drop_duplicate_rows_keep_last_retains_full_rows_by_key_without_fallback() {
    let path = unique_vortex_path("drop-duplicates-keep-last");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("value").expect("column"),
            ColumnRef::new("metric").expect("column"),
        ]),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    )
    .with_duplicate_keep(VortexDuplicateKeepPolicy::Last)
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DropDuplicateRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(
        report.projected_columns,
        vec!["value".to_string(), "metric".to_string()]
    );
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(3));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    // Keep one candidate per key, not the full duplicate population.
    assert!(!report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
    assert_eq!(
        report.state_budget.state_family,
        "retained_row_deduplicate_state+hash_row_key"
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"hash_row_key_state".to_string())
    );
}

#[test]
fn sample_rows_materialize_seeded_vortex_rows_without_fallback() {
    let path = unique_vortex_path("sample-rows");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(2),
        }),
        2,
        7,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(
        report.result_summary.as_deref(),
        Some("sample_rows=2 projected_columns=metric sample_seed=7 sample_size=2")
    );
    assert_eq!(report.max_parallelism_requested, 2);
    assert_eq!(report.scan_concurrency_per_worker, 1);
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(4));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
}

#[test]
fn sample_rows_apply_pushdown_predicate_before_sampling_without_fallback() {
    let path = unique_vortex_path("sample-rows-residual");
    write_string_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
        Some(PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("label"),
            needle: "bad".to_string(),
            negated: false,
        }),
        2,
        7,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.source_order_limit_input_rows, Some(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
}

#[test]
fn sample_rows_embedded_length_footer_pruning_skips_candidate_scan_without_fallback() {
    let path = unique_vortex_path("sample-rows-embedded-length-pruned");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("Referer").expect("referer")]),
        Some(PredicateExpr::StringContains {
            column: ColumnRef::new("Referer").expect("referer"),
            needle: "this-substring-is-longer-than-any-referer-in-the-fixture".to_string(),
            negated: false,
        }),
        2,
        7,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(0));
    assert_eq!(report.rows_projected, Some(0));
    assert_eq!(report.projected_columns, vec!["Referer".to_string()]);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(0));
    assert_eq!(report.source_order_limit_rows_output, Some(0));
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.upstream_projection_expression_used);
    assert!(!report.upstream_scan_called);
    assert_eq!(report.arrays_read_count, 0);
    assert!(report.embedded_layout.metadata_first_pruning_consulted);
    assert!(report.embedded_layout.metadata_pruned_entire_input);
    assert!(
        report
            .result_summary
            .as_deref()
            .expect("summary")
            .contains("__shardloom_derived_utf8_len_Referer")
    );
    assert!(!report.data_read);
    assert!(!report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn sample_rows_materialize_fractional_vortex_rows_without_fallback() {
    let path = unique_vortex_path("sample-fraction-rows");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_fraction_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(2),
        }),
        0.5,
        7,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(
        report.result_summary.as_deref(),
        Some("sample_rows=2 projected_columns=metric sample_seed=7 sample_fraction=0.5")
    );
    assert_eq!(report.source_order_limit_requested, None);
    assert!(!report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(4));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
}

#[test]
fn sample_rows_materialize_replacement_rows_without_fallback() {
    let path = unique_vortex_path("sample-replacement-rows");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(2),
        }),
        7,
        11,
    )
    .with_sample_replacement(true);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(7));
    assert_eq!(report.rows_projected, Some(7));
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(
        report.result_summary.as_deref(),
        Some(
            "sample_rows=7 projected_columns=metric sample_seed=11 sample_size=7 sample_replacement=true"
        )
    );
    assert_eq!(report.source_order_limit_requested, Some(7));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(4));
    assert_eq!(report.source_order_limit_rows_output, Some(7));
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
}

#[test]
fn sample_rows_materialize_fractional_replacement_rows_without_fallback() {
    let path = unique_vortex_path("sample-fraction-replacement-rows");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_fraction_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(2),
        }),
        0.5,
        19,
    )
    .with_sample_replacement(true);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(
        report.result_summary.as_deref(),
        Some(
            "sample_rows=2 projected_columns=metric sample_seed=19 sample_fraction=0.5 sample_replacement=true"
        )
    );
    assert_eq!(report.source_order_limit_requested, None);
    assert!(!report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(4));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
}

#[test]
fn sample_rows_materialize_weighted_vortex_rows_without_fallback() {
    let path = unique_vortex_path("sample-weighted-rows");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        None,
        2,
        7,
    )
    .with_sample_weight_column(ColumnRef::new("metric").expect("column"));

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    assert_eq!(
        report.result_summary.as_deref(),
        Some(
            "sample_rows=2 projected_columns=value sample_seed=7 sample_size=2 sample_weight_column=metric"
        )
    );
    assert!(report.projection_pushdown_applied);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
}

#[test]
fn expression_project_rows_materialize_typed_rewrites_without_fallback() {
    let path = unique_vortex_path("expression-project-rows");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::MaskScalar {
            target_column: ColumnRef::new("metric").expect("column"),
            predicate: PredicateExpr::Compare {
                column: ColumnRef::new("metric").expect("column"),
                op: ComparisonOp::Lt,
                value: StatValue::Int64(30),
            },
            replacement: shardloom_core::ScalarValue::Int64(0),
        }]),
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(
        report.result_summary.as_deref(),
        Some("expression_project_rows=2 projected_columns=metric expression_rewrites=mask_scalar")
    );
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.source_order_limit_applied);
    // The streaming producer stops rewriting once the output limit is met.
    assert_eq!(report.source_order_limit_input_rows, Some(2));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
    assert!(certificate.is_certified());
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "project,expression_project"
    );
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->decoded_columnar,decoded_columnar->materialized_rows"
    );
    assert_eq!(
        certificate.materialization_boundary_order(),
        "local_primitive.expression_project_rows.bounded_materialization"
    );
    assert!(!certificate.side_effects.fallback_attempted);
}

#[test]
fn expression_project_rows_footer_pruning_skips_scan_without_fallback() {
    let path = unique_vortex_path("expression-project-rows-footer-pruned");
    write_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![
            VortexExpressionRewrite::NumericScalarArithmetic {
                target_column: ColumnRef::new("metric").expect("column"),
                operator: "+".to_string(),
                operand: shardloom_core::ScalarValue::Int64(5),
            },
        ]),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("value").expect("column"),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(99),
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(0));
    assert_eq!(report.rows_projected, Some(0));
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(report.embedded_layout.metadata_pruned_entire_input);
    assert!(!report.data_read);
    assert!(!report.streaming_scan_used);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.upstream_scan_called);
    assert!(!report.fallback_execution_allowed);
    assert!(certificate.is_certified());
    assert!(!certificate.side_effects.fallback_attempted);
}

#[test]
fn expression_project_rows_embedded_length_footer_pruning_reports_rewrite_evidence() {
    let path = unique_vortex_path("expression-project-rows-embedded-length-pruned");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("Referer").expect("referer")]),
        VortexExpressionProjectionRequest::new(vec![
            VortexExpressionRewrite::StringReplaceScalar {
                target_column: ColumnRef::new("Referer").expect("referer"),
                needle: "google".to_string(),
                replacement: "search".to_string(),
            },
        ]),
    );
    request.predicate = Some(PredicateExpr::StringContains {
        column: ColumnRef::new("Referer").expect("referer"),
        needle: "this-substring-is-longer-than-any-referer-in-the-fixture".to_string(),
        negated: false,
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(0));
    assert_eq!(report.rows_projected, Some(0));
    assert_eq!(report.projected_columns, vec!["Referer".to_string()]);
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(!report.upstream_scan_called);
    assert_eq!(report.arrays_read_count, 0);
    assert!(report.embedded_layout.metadata_first_pruning_consulted);
    assert!(report.embedded_layout.metadata_pruned_entire_input);
    assert!(
        report
            .result_summary
            .as_deref()
            .expect("summary")
            .contains("__shardloom_derived_utf8_len_Referer")
    );
    assert!(!report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.row_read);
    assert!(!report.materialization_boundary_reported);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn expression_project_rows_filters_source_strings_before_rewrite() {
    let path = unique_vortex_path("expression-project-source-string-filter");
    write_string_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![
            VortexExpressionRewrite::StringReplaceScalar {
                target_column: ColumnRef::new("label").expect("column"),
                needle: "bad".to_string(),
                replacement: "ok".to_string(),
            },
        ]),
    );
    request.predicate = Some(PredicateExpr::StringContains {
        column: ColumnRef::new("label").expect("column"),
        needle: "bad".to_string(),
        negated: false,
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(report.materialization_boundary_reported);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn melt_rows_filters_source_predicate_before_expansion() {
    let path = unique_vortex_path("melt-rows-source-filter");
    write_melt_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::melt_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexMeltProjectionRequest::new(
            vec![ColumnRef::new("id").expect("column")],
            vec![
                ColumnRef::new("amount_a").expect("column"),
                ColumnRef::new("amount_b").expect("column"),
            ],
            "measure".to_string(),
            "amount".to_string(),
        ),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("id").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(2),
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::MeltRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(4));
    assert_eq!(report.rows_projected, Some(4));
    assert_eq!(
        report.projected_columns,
        vec![
            "id".to_string(),
            "measure".to_string(),
            "amount".to_string()
        ]
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(4));
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn explode_rows_filters_source_predicate_before_expansion() {
    let path = unique_vortex_path("explode-rows-source-filter");
    write_explode_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::explode_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("items").expect("column"),
        ]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column")),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("id").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(3),
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(1));
    assert_eq!(report.rows_projected, Some(1));
    assert_eq!(
        report.projected_columns,
        vec!["id".to_string(), "items".to_string()]
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(1));
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn pivot_rows_filters_pushdown_source_predicate_before_state_update() {
    let path = unique_vortex_path("pivot-rows-source-filter");
    write_pivot_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::pivot_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexPivotProjectionRequest::new(
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("label").expect("column"),
            ColumnRef::new("amount").expect("column"),
            "sum",
        ),
    );
    request.predicate = Some(PredicateExpr::StringContains {
        column: ColumnRef::new("label").expect("column"),
        needle: "paid".to_string(),
        negated: false,
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::PivotRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(1));
    assert_eq!(report.rows_projected, Some(1));
    assert_eq!(
        report.projected_columns,
        vec!["id".to_string(), "pivot_paid".to_string()]
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert!(report.row_read);
    assert!(report.materialization_boundary_reported);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn rolling_window_rows_filters_source_predicate_before_window_state() {
    let path = unique_vortex_path("rolling-window-rows-source-filter");
    write_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::rolling_window_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric_mean".to_string(),
            2,
            2,
            "mean".to_string(),
        ),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("value").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(3),
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::RollingWindowRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(
        report.projected_columns,
        vec!["rolling_metric_mean".to_string()]
    );
    assert!(report.filter_pushdown_applied);
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(2));
    assert!(report.state_budget.state_budget_required);
    assert!(report.data_read);
    assert!(report.row_read);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn rolling_window_rows_materialize_source_order_sums_without_fallback() {
    let path = unique_vortex_path("rolling-window-rows");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::rolling_window_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric".to_string(),
            3,
            3,
            "sum".to_string(),
        ),
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::RollingWindowRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(report.projected_columns, vec!["rolling_metric".to_string()]);
    assert_eq!(
        report.result_summary.as_deref(),
        Some(
            "rolling_window_rows=2 projected_columns=rolling_metric source_column=metric;output_column=rolling_metric;window_size=3;min_periods=3;aggregate=sum;center=false"
        )
    );
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(2));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
    assert!(report.state_budget.state_budget_required);
    assert_eq!(report.state_budget.state_family, "rolling_window_state");
    assert_eq!(report.state_budget.observed_state_items, 3);
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"rolling_window_state_fragment".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"window_state_memory".to_string())
    );
    assert!(report.state_budget.fail_closed_if_spill_required);
    assert!(certificate.is_certified());
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "project,rolling_window"
    );
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->decoded_columnar,decoded_columnar->materialized_rows"
    );
    assert!(!certificate.side_effects.fallback_attempted);
}

#[test]
fn rolling_window_rows_materialize_min_max_without_fallback() {
    let min_request = VortexRollingWindowRequest::new(
        ColumnRef::new("metric").expect("column"),
        "rolling_metric_min".to_string(),
        3,
        2,
        "min".to_string(),
    );
    let max_request = VortexRollingWindowRequest::new(
        ColumnRef::new("metric").expect("column"),
        "rolling_metric_max".to_string(),
        3,
        2,
        "max".to_string(),
    );
    let values = vec![vec![
        StatValue::Int64(10),
        StatValue::Int64(30),
        StatValue::Int64(20),
        StatValue::Int64(50),
    ]];

    let min_values = rolling_window_values(
        &values,
        &min_request,
        &mut RollingWindowState::new(min_request.window_size),
        4,
        false,
    )
    .expect("rolling min");
    let max_values = rolling_window_values(
        &values,
        &max_request,
        &mut RollingWindowState::new(max_request.window_size),
        4,
        false,
    )
    .expect("rolling max");

    assert_eq!(
        min_values,
        vec![
            StatValue::Float64(10.0),
            StatValue::Float64(10.0),
            StatValue::Float64(20.0)
        ]
    );
    assert_eq!(
        max_values,
        vec![
            StatValue::Float64(30.0),
            StatValue::Float64(30.0),
            StatValue::Float64(50.0)
        ]
    );
}

#[test]
fn rolling_window_values_use_valid_observation_min_periods_without_fallback() {
    let sum_request = VortexRollingWindowRequest::new(
        ColumnRef::new("metric").expect("column"),
        "rolling_metric_sum".to_string(),
        3,
        2,
        "sum".to_string(),
    );
    let count_request = VortexRollingWindowRequest::new(
        ColumnRef::new("metric").expect("column"),
        "rolling_metric_count".to_string(),
        3,
        2,
        "count".to_string(),
    );
    let values = vec![vec![
        StatValue::Int64(10),
        StatValue::Null,
        StatValue::Int64(20),
        StatValue::Null,
        StatValue::Int64(50),
    ]];

    let sum_values = rolling_window_values(
        &values,
        &sum_request,
        &mut RollingWindowState::new(sum_request.window_size),
        5,
        false,
    )
    .expect("rolling sum with nulls");
    let count_values = rolling_window_values(
        &values,
        &count_request,
        &mut RollingWindowState::new(count_request.window_size),
        5,
        false,
    )
    .expect("rolling count with nulls");

    assert_eq!(
        sum_values,
        vec![StatValue::Float64(30.0), StatValue::Float64(70.0)]
    );
    assert_eq!(
        count_values,
        vec![StatValue::UInt64(2), StatValue::UInt64(2)]
    );
}

#[test]
fn centered_rolling_window_values_use_bounded_lookahead_without_fallback() {
    let request = VortexRollingWindowRequest::new(
        ColumnRef::new("metric").expect("column"),
        "rolling_metric_centered_sum".to_string(),
        3,
        2,
        "sum".to_string(),
    )
    .with_center(true);
    let mut state = RollingWindowState::new(request.window_size);
    let first_values = rolling_window_values(
        &[vec![StatValue::Int64(10), StatValue::Int64(20)]],
        &request,
        &mut state,
        2,
        false,
    )
    .expect("centered rolling first chunk");
    let second_values = rolling_window_values(
        &[vec![
            StatValue::Int64(30),
            StatValue::Int64(40),
            StatValue::Int64(50),
        ]],
        &request,
        &mut state,
        3,
        true,
    )
    .expect("centered rolling second chunk");

    assert_eq!(first_values, vec![StatValue::Float64(30.0)]);
    assert_eq!(
        second_values,
        vec![
            StatValue::Float64(60.0),
            StatValue::Float64(90.0),
            StatValue::Float64(120.0),
            StatValue::Float64(90.0)
        ]
    );
}

#[test]
fn rolling_window_row_export_writes_mean_rows_without_fallback() {
    let path = unique_vortex_path("rolling-window-mean-row-export");
    let output_path =
        unique_vortex_path("rolling-window-mean-row-export-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::rolling_window_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric_mean".to_string(),
            2,
            2,
            "mean".to_string(),
        ),
    )
    .with_source_order_limit(3);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::RollingWindowRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 3);
    // No fourth result is computed after the three requested rows.
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(
        report.projected_columns,
        vec!["rolling_metric_mean".to_string()]
    );
    assert_eq!(
        rows,
        "{\"rolling_metric_mean\":15.0}\n{\"rolling_metric_mean\":25.0}\n{\"rolling_metric_mean\":35.0}\n"
    );
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(report.state_budget.state_budget_required);
    assert_eq!(report.state_budget.state_family, "rolling_window_state");
    assert_eq!(report.state_budget.observed_state_items, 2);
    assert!(report.state_budget.fail_closed_if_spill_required);
}

#[test]
fn rolling_window_row_export_writes_count_rows_without_fallback() {
    let path = unique_vortex_path("rolling-window-count-row-export");
    let output_path =
        unique_vortex_path("rolling-window-count-row-export-output").with_extension("jsonl");
    write_string_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::rolling_window_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexRollingWindowRequest::new(
            ColumnRef::new("label").expect("column"),
            "rolling_label_count".to_string(),
            3,
            1,
            "count".to_string(),
        ),
    )
    .with_source_order_limit(3);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::RollingWindowRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(
        report.projected_columns,
        vec!["rolling_label_count".to_string()]
    );
    assert_eq!(
        rows,
        "{\"rolling_label_count\":1}\n{\"rolling_label_count\":2}\n{\"rolling_label_count\":3}\n"
    );
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(report.state_budget.state_budget_required);
    assert_eq!(report.state_budget.state_family, "rolling_window_state");
    assert_eq!(report.state_budget.observed_state_items, 3);
    assert!(report.state_budget.fail_closed_if_spill_required);
}

#[test]
fn centered_rolling_window_row_export_writes_bounded_rows_without_fallback() {
    let path = unique_vortex_path("rolling-window-centered-row-export");
    let output_path =
        unique_vortex_path("rolling-window-centered-row-export-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::rolling_window_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric_centered_sum".to_string(),
            3,
            3,
            "sum".to_string(),
        )
        .with_center(true),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::RollingWindowRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(
        report.projected_columns,
        vec!["rolling_metric_centered_sum".to_string()]
    );
    assert_eq!(
        rows,
        "{\"rolling_metric_centered_sum\":60.0}\n{\"rolling_metric_centered_sum\":90.0}\n{\"rolling_metric_centered_sum\":120.0}\n"
    );
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(report.state_budget.state_budget_required);
    assert_eq!(report.state_budget.state_family, "rolling_window_state");
    assert_eq!(report.state_budget.observed_state_items, 3);
    assert!(report.state_budget.fail_closed_if_spill_required);
}

#[test]
fn explode_rows_materialize_list_elements_without_fallback() {
    let path = unique_vortex_path("explode-rows");
    write_explode_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::explode_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("items").expect("column"),
        ]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column")),
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert_eq!(
        report.projected_columns,
        vec!["id".to_string(), "items".to_string()]
    );
    assert_eq!(
        report.result_summary.as_deref(),
        Some("explode_rows=2 projected_columns=id,items column=items")
    );
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(2));
    assert_eq!(report.source_order_limit_rows_output, Some(2));
    assert!(report.data_read);
    assert!(report.streaming_scan_used);
    assert!(!report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.arrow_converted);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.materialization_boundary_reported);
    assert!(certificate.is_certified());
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "project,explode"
    );
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->decoded_columnar,decoded_columnar->materialized_rows"
    );
    assert_eq!(
        certificate.materialization_boundary_order(),
        "local_primitive.explode_rows.bounded_materialization"
    );
    assert!(!certificate.side_effects.fallback_attempted);
}

#[test]
fn distinct_row_export_writes_unique_vortex_rows_with_materialization_evidence() {
    let path = unique_vortex_path("distinct-row-export");
    let output_path = unique_vortex_path("distinct-row-export-output").with_extension("jsonl");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        None,
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DistinctRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    assert_eq!(report.max_parallelism_requested, 2);
    assert_eq!(report.scan_concurrency_per_worker, 1);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert_eq!(rows, "{\"value\":1}\n{\"value\":2}\n");
    assert!(!report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn drop_duplicate_row_export_keep_false_writes_only_unique_key_rows_without_fallback() {
    let path = unique_vortex_path("drop-duplicates-row-export");
    let output_path =
        unique_vortex_path("drop-duplicates-row-export-output").with_extension("jsonl");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("value").expect("column"),
            ColumnRef::new("metric").expect("column"),
        ]),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    )
    .with_duplicate_keep(VortexDuplicateKeepPolicy::AllDuplicates);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DropDuplicateRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 1);
    assert_eq!(report.pre_limit_result_row_count, 1);
    assert_eq!(
        report.projected_columns,
        vec!["value".to_string(), "metric".to_string()]
    );
    let emitted_rows: Vec<serde_json::Value> = rows
        .lines()
        .map(|line| serde_json::from_str(line).expect("jsonl row"))
        .collect();
    assert_eq!(
        emitted_rows,
        vec![serde_json::json!({"value": 3, "metric": 30})]
    );
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn drop_duplicate_row_export_keep_last_retains_nullable_scalar_key_without_fallback() {
    let path = unique_vortex_path("drop-duplicates-nullable-row-export");
    let output_path =
        unique_vortex_path("drop-duplicates-nullable-row-export-output").with_extension("jsonl");
    write_nullable_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("value").expect("column"),
            ColumnRef::new("metric").expect("column"),
        ]),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    )
    .with_duplicate_keep(VortexDuplicateKeepPolicy::Last);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);
    let emitted_rows: Vec<serde_json::Value> = rows
        .lines()
        .map(|line| serde_json::from_str(line).expect("jsonl row"))
        .collect();

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DropDuplicateRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(
        emitted_rows,
        vec![
            serde_json::json!({"value": 1, "metric": 10}),
            serde_json::json!({"value": 2, "metric": 30}),
            serde_json::json!({"value": null, "metric": 44}),
        ]
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn drop_duplicate_row_export_retains_rows_by_list_key_without_fallback() {
    let path = unique_vortex_path("drop-duplicates-list-key-row-export");
    let output_path =
        unique_vortex_path("drop-duplicates-list-key-row-export-output").with_extension("jsonl");
    write_nested_row_key_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        ProjectionRequest::columns(vec![ColumnRef::new("items").expect("column")]),
    )
    .with_duplicate_keep(VortexDuplicateKeepPolicy::Last);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);
    let emitted_rows: Vec<serde_json::Value> = rows
        .lines()
        .map(|line| serde_json::from_str(line).expect("jsonl row"))
        .collect();

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DropDuplicateRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(
        emitted_rows,
        vec![
            serde_json::json!({"metric": 20}),
            serde_json::json!({"metric": 30}),
        ]
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn duplicate_mask_row_export_writes_boolean_mask_without_fallback() {
    let path = unique_vortex_path("duplicate-mask-row-export");
    let output_path =
        unique_vortex_path("duplicate-mask-row-export-output").with_extension("jsonl");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DuplicateMaskRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 5);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["duplicated".to_string()]);
    assert_eq!(
        rows,
        "{\"duplicated\":false}\n{\"duplicated\":true}\n{\"duplicated\":false}\n{\"duplicated\":true}\n{\"duplicated\":false}\n"
    );
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn duplicate_mask_row_export_marks_struct_key_duplicates_without_fallback() {
    let path = unique_vortex_path("duplicate-mask-struct-key-row-export");
    let output_path =
        unique_vortex_path("duplicate-mask-struct-key-row-export-output").with_extension("jsonl");
    write_nested_row_key_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("payload").expect("column")]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DuplicateMaskRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(
        rows,
        "{\"duplicated\":false}\n{\"duplicated\":true}\n{\"duplicated\":false}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn duplicate_mask_row_export_preserves_list_parent_null_key_identity() {
    let path = unique_vortex_path("duplicate-mask-nullable-list-parent-key-row-export");
    let output_path =
        unique_vortex_path("duplicate-mask-nullable-list-parent-key-row-export-output")
            .with_extension("jsonl");
    write_nullable_parent_nested_row_key_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("items").expect("column")]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        rows,
        "{\"duplicated\":false}\n{\"duplicated\":false}\n{\"duplicated\":true}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn duplicate_mask_row_export_preserves_struct_parent_null_key_identity() {
    let path = unique_vortex_path("duplicate-mask-nullable-struct-parent-key-row-export");
    let output_path =
        unique_vortex_path("duplicate-mask-nullable-struct-parent-key-row-export-output")
            .with_extension("jsonl");
    write_nullable_parent_nested_row_key_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("payload").expect("column")]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        rows,
        "{\"duplicated\":false}\n{\"duplicated\":false}\n{\"duplicated\":true}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn duplicate_mask_row_export_writes_keep_last_mask_without_fallback() {
    let path = unique_vortex_path("duplicate-mask-row-export-last");
    let output_path =
        unique_vortex_path("duplicate-mask-row-export-last-output").with_extension("jsonl");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    )
    .with_source_order_limit(2)
    .with_duplicate_keep(VortexDuplicateKeepPolicy::Last);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert_eq!(rows, "{\"duplicated\":true}\n{\"duplicated\":false}\n");
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn duplicate_mask_row_export_marks_repeated_null_scalar_key_without_fallback() {
    let path = unique_vortex_path("duplicate-mask-nullable-row-export");
    let output_path =
        unique_vortex_path("duplicate-mask-nullable-row-export-output").with_extension("jsonl");
    write_nullable_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DuplicateMaskRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 5);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(
        rows,
        "{\"duplicated\":false}\n{\"duplicated\":false}\n{\"duplicated\":true}\n{\"duplicated\":false}\n{\"duplicated\":true}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn duplicate_mask_row_export_writes_all_duplicate_mask_without_fallback() {
    let path = unique_vortex_path("duplicate-mask-row-export-all");
    let output_path =
        unique_vortex_path("duplicate-mask-row-export-all-output").with_extension("jsonl");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    )
    .with_duplicate_keep(VortexDuplicateKeepPolicy::AllDuplicates);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 5);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(
        rows,
        "{\"duplicated\":true}\n{\"duplicated\":true}\n{\"duplicated\":true}\n{\"duplicated\":true}\n{\"duplicated\":false}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn tail_row_export_writes_source_order_tail_rows_without_fallback() {
    let path = unique_vortex_path("tail-row-export");
    let output_path = unique_vortex_path("tail-row-export-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::tail_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        2,
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::TailRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert_eq!(rows, "{\"metric\":40}\n{\"metric\":50}\n");
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn filter_row_export_rewrites_non_empty_string_predicate_to_embedded_length_column() {
    let path = unique_vortex_path("filter-row-export-embedded-derived-length");
    let output_path = unique_vortex_path("filter-row-export-embedded-derived-length-output")
        .with_extension("jsonl");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::Compare {
            column: ColumnRef::new("Referer").expect("referer"),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        },
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let emitted_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::FilterPredicate
    );
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(report.projected_columns, vec!["Referer".to_string()]);
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!rows.contains("__shardloom_derived"));
    assert_eq!(emitted_rows.len(), 3);
    assert!(
        emitted_rows
            .iter()
            .all(|row| row["Referer"] != serde_json::json!("")),
        "embedded length rewrite must remove empty Referer rows"
    );
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn filter_project_row_export_footer_pruning_writes_empty_output_without_scan() {
    let path = unique_vortex_path("filter-project-row-export-footer-pruned");
    let output_path =
        unique_vortex_path("filter-project-row-export-footer-pruned-output").with_extension("csv");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::Gt,
            value: StatValue::Int64(99),
        },
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("metric")]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Csv,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::FilterAndProject
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 0);
    assert_eq!(report.pre_limit_result_row_count, 0);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert_eq!(rows, "metric\n");
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(!report.evidence.side_effects.data_read);
    assert!(!report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.data_materialized);
    assert!(!report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn filter_row_export_footer_pruning_writes_empty_output_without_scan() {
    let path = unique_vortex_path("filter-row-export-footer-pruned");
    let output_path =
        unique_vortex_path("filter-row-export-footer-pruned-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::Gt,
            value: StatValue::Int64(99),
        },
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::FilterPredicate
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 0);
    assert_eq!(report.pre_limit_result_row_count, 0);
    assert!(report.projected_columns.contains(&"value".to_string()));
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert_eq!(rows, "");
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(!report.evidence.side_effects.data_read);
    assert!(!report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.data_materialized);
    assert!(!report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn distinct_row_export_footer_pruning_skips_row_key_state_without_scan() {
    let path = unique_vortex_path("distinct-row-export-footer-pruned");
    let output_path =
        unique_vortex_path("distinct-row-export-footer-pruned-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::Gt,
            value: StatValue::Int64(99),
        }),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DistinctRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 0);
    assert_eq!(report.pre_limit_result_row_count, 0);
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert_eq!(rows, "");
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(!report.evidence.side_effects.data_read);
    assert!(!report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.data_materialized);
    assert!(!report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn sample_row_export_footer_pruning_samples_empty_candidate_set_without_scan() {
    let path = unique_vortex_path("sample-row-export-footer-pruned");
    let output_path =
        unique_vortex_path("sample-row-export-footer-pruned-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::Gt,
            value: StatValue::Int64(99),
        }),
        2,
        7,
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 0);
    assert_eq!(report.pre_limit_result_row_count, 0);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert_eq!(rows, "");
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.pushdown.source_order_limit_applied);
    assert!(!report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(!report.evidence.side_effects.data_read);
    assert!(!report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.data_materialized);
    assert!(!report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn sample_row_export_rewrites_non_empty_string_predicate_before_sampling() {
    let path = unique_vortex_path("sample-row-export-embedded-derived-length");
    let output_path = unique_vortex_path("sample-row-export-embedded-derived-length-output")
        .with_extension("jsonl");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("Referer").expect("referer")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("Referer").expect("referer"),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        }),
        2,
        7,
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let emitted_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(report.projected_columns, vec!["Referer".to_string()]);
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!rows.contains("__shardloom_derived"));
    assert_eq!(emitted_rows.len(), 2);
    assert!(
        emitted_rows
            .iter()
            .all(|row| row["Referer"] != serde_json::json!("")),
        "sample must operate after the embedded length filter"
    );
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn sample_row_export_applies_pushdown_predicates_before_sampling() {
    let path = unique_vortex_path("sample-row-export-residual");
    let output_path =
        unique_vortex_path("sample-row-export-residual-output").with_extension("jsonl");
    write_string_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
        Some(PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("label"),
            needle: "bad".to_string(),
            negated: false,
        }),
        2,
        7,
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let emitted_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert_eq!(emitted_rows.len(), 2);
    assert!(
        emitted_rows.iter().all(|row| row["label"]
            .as_str()
            .is_some_and(|label| label.contains("bad"))),
        "contains predicate must filter before sampling"
    );
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn filter_project_row_export_applies_pushdown_predicate_without_exposing_helper_columns() {
    let path = unique_vortex_path("filter-project-row-export-residual");
    let output_path =
        unique_vortex_path("filter-project-row-export-residual-output").with_extension("jsonl");
    write_string_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("label"),
            needle: "bad".to_string(),
            negated: false,
        },
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let emitted_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::FilterAndProject
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert_eq!(
        emitted_rows,
        vec![
            serde_json::json!({"label": "bad"}),
            serde_json::json!({"label": "badly"}),
        ]
    );
    assert!(!rows.contains("__shardloom_derived"));
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn distinct_row_export_applies_residual_predicate_before_row_key_state() {
    let path = unique_vortex_path("distinct-row-export-residual");
    let output_path =
        unique_vortex_path("distinct-row-export-residual-output").with_extension("jsonl");
    write_string_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
        Some(PredicateExpr::StringContains {
            column: ColumnRef::new("label").expect("label"),
            needle: "bad".to_string(),
            negated: false,
        }),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let emitted_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DistinctRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert_eq!(
        emitted_rows,
        vec![
            serde_json::json!({"label": "bad"}),
            serde_json::json!({"label": "badly"}),
        ]
    );
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn distinct_row_export_pushdown_predicate_columns_do_not_widen_distinct_key() {
    let path = unique_vortex_path("distinct-row-export-residual-key-width");
    let output_path =
        unique_vortex_path("distinct-row-export-residual-key-width-output").with_extension("jsonl");
    write_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("value")]),
        Some(PredicateExpr::InList {
            column: ColumnRef::new("metric").expect("metric"),
            values: vec![StatValue::Int64(20), StatValue::Int64(99)],
            negated: false,
        }),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let emitted_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DistinctRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 1);
    assert_eq!(report.pre_limit_result_row_count, 1);
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    assert_eq!(emitted_rows, vec![serde_json::json!({"value": 2})]);
    assert!(!rows.contains("metric"));
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn drop_duplicate_row_export_applies_residual_predicate_before_row_key_state() {
    let path = unique_vortex_path("drop-duplicates-row-export-residual");
    let output_path =
        unique_vortex_path("drop-duplicates-row-export-residual-output").with_extension("jsonl");
    write_string_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("label")]),
    );
    request.predicate = Some(PredicateExpr::StringContains {
        column: ColumnRef::new("label").expect("label"),
        needle: "bad".to_string(),
        negated: false,
    });

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let emitted_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::DropDuplicateRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert_eq!(
        emitted_rows,
        vec![
            serde_json::json!({"label": "bad"}),
            serde_json::json!({"label": "badly"}),
        ]
    );
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn sample_row_export_writes_seeded_rows_without_fallback() {
    let path = unique_vortex_path("sample-row-export");
    let output_path = unique_vortex_path("sample-row-export-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        None,
        2,
        7,
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    let metrics = [10_i64, 20, 30, 40, 50];
    let mut expected_indices = metrics
        .iter()
        .enumerate()
        .map(|(index, _metric)| (deterministic_sample_score(7, index), index))
        .collect::<Vec<_>>();
    expected_indices.sort_by_key(|(score, _index)| std::cmp::Reverse(*score));
    expected_indices.truncate(2);
    expected_indices.sort_by_key(|(_score, index)| *index);
    let expected = expected_indices
        .iter()
        .map(|(_score, index)| format!("{{\"metric\":{}}}", metrics[*index]))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert_eq!(rows, expected);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn sample_row_export_writes_weighted_rows_without_exposing_weight_column() {
    let path = unique_vortex_path("sample-row-export-weighted");
    let output_path =
        unique_vortex_path("sample-row-export-weighted-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        None,
        2,
        7,
    )
    .with_sample_weight_column(ColumnRef::new("metric").expect("column"));

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    let values = [1_i64, 2, 3, 4, 5];
    let weights = [10.0_f64, 20.0, 30.0, 40.0, 50.0];
    let mut expected_indices = weights
        .iter()
        .enumerate()
        .map(|(index, weight)| {
            (
                deterministic_weighted_sample_score(7, index, *weight),
                index,
            )
        })
        .collect::<Vec<_>>();
    expected_indices.sort_by(|(left_score, _left), (right_score, _right)| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    expected_indices.truncate(2);
    expected_indices.sort_by_key(|(_score, index)| *index);
    let expected = expected_indices
        .iter()
        .map(|(_score, index)| format!("{{\"value\":{}}}", values[*index]))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert_eq!(rows, expected);
    assert!(!rows.contains("metric"));
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn sample_row_export_writes_fractional_seeded_rows_without_fallback() {
    let path = unique_vortex_path("sample-row-export-fraction");
    let output_path =
        unique_vortex_path("sample-row-export-fraction-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_fraction_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        None,
        0.4,
        7,
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    let metrics = [10_i64, 20, 30, 40, 50];
    let mut expected_indices = metrics
        .iter()
        .enumerate()
        .map(|(index, _metric)| (deterministic_sample_score(7, index), index))
        .collect::<Vec<_>>();
    expected_indices.sort_by_key(|(score, _index)| std::cmp::Reverse(*score));
    expected_indices.truncate(2);
    expected_indices.sort_by_key(|(_score, index)| *index);
    let expected = expected_indices
        .iter()
        .map(|(_score, index)| format!("{{\"metric\":{}}}", metrics[*index]))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(report.source_order_limit_requested, None);
    assert_eq!(rows, expected);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn sample_row_export_writes_replacement_rows_without_fallback() {
    let path = unique_vortex_path("sample-row-export-replacement");
    let output_path =
        unique_vortex_path("sample-row-export-replacement-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        None,
        7,
        11,
    )
    .with_sample_replacement(true);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    let metrics = [10_i64, 20, 30, 40, 50];
    let expected = (0..7)
        .map(|draw_index| {
            let row_index = deterministic_sample_replacement_index(11, draw_index, metrics.len());
            format!("{{\"metric\":{}}}", metrics[row_index])
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 7);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(report.source_order_limit_requested, Some(7));
    assert_eq!(rows, expected);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn sample_row_export_writes_fractional_replacement_rows_without_fallback() {
    let path = unique_vortex_path("sample-row-export-fraction-replacement");
    let output_path =
        unique_vortex_path("sample-row-export-fraction-replacement-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sample_fraction_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        None,
        0.4,
        23,
    )
    .with_sample_replacement(true);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    let metrics = [10_i64, 20, 30, 40, 50];
    let expected = (0..2)
        .map(|draw_index| {
            let row_index = deterministic_sample_replacement_index(23, draw_index, metrics.len());
            format!("{{\"metric\":{}}}", metrics[row_index])
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(report.source_order_limit_requested, None);
    assert_eq!(rows, expected);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn expression_project_row_export_writes_rewritten_rows_without_fallback() {
    let path = unique_vortex_path("expression-project-row-export");
    let output_path =
        unique_vortex_path("expression-project-row-export-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::ReplaceScalar {
            target_column: ColumnRef::new("metric").expect("column"),
            to_replace: shardloom_core::ScalarValue::Int64(20),
            replacement: shardloom_core::ScalarValue::Int64(99),
        }]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 5);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(
        rows,
        "{\"metric\":10}\n{\"metric\":99}\n{\"metric\":30}\n{\"metric\":40}\n{\"metric\":50}\n"
    );
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn expression_project_row_export_writes_null_rewrite_without_fallback() {
    let path = unique_vortex_path("expression-project-null-row-export");
    let output_path =
        unique_vortex_path("expression-project-null-row-export-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::ReplaceScalar {
            target_column: ColumnRef::new("metric").expect("column"),
            to_replace: shardloom_core::ScalarValue::Int64(20),
            replacement: shardloom_core::ScalarValue::Null,
        }]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_written, 5);
    assert_eq!(
        rows,
        "{\"metric\":10}\n{\"metric\":null}\n{\"metric\":30}\n{\"metric\":40}\n{\"metric\":50}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn expression_project_row_export_forward_fills_nulls_without_fallback() {
    let path = unique_vortex_path("expression-project-forward-fill-row-export");
    let output_path = unique_vortex_path("expression-project-forward-fill-row-export-output")
        .with_extension("jsonl");
    write_nullable_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::ForwardFillNull {
            target_column: ColumnRef::new("value").expect("column"),
            limit: Some(1),
        }]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 5);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    assert_eq!(
        rows,
        "{\"value\":1}\n{\"value\":1}\n{\"value\":3}\n{\"value\":4}\n{\"value\":5}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn structured_expression_project_row_export_writes_parquet_without_fallback() {
    let path = unique_vortex_path("structured-expression-project-row-export");
    let output_path = unique_vortex_path("structured-expression-project-row-export-output")
        .with_extension("parquet");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::structured_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexStructuredProjectionRequest::new(vec![
            VortexStructuredProjectionColumn::new(
                "value".to_string(),
                VortexStructuredProjectionExpr::SourceColumn(
                    ColumnRef::new("value").expect("column"),
                ),
            ),
            VortexStructuredProjectionColumn::new(
                "values".to_string(),
                VortexStructuredProjectionExpr::ArrayLiteral(vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Int64(2),
                    ScalarValue::Null,
                ]),
            ),
            VortexStructuredProjectionColumn::new(
                "payload".to_string(),
                VortexStructuredProjectionExpr::StructColumns(vec![
                    ColumnRef::new("value").expect("column"),
                    ColumnRef::new("metric").expect("column"),
                ]),
            ),
        ]),
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Parquet,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let table = crate::read_flat_parquet_source(&output_path, 10).expect("parquet rows");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.output_format, "parquet");
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(
        report.projected_columns,
        vec![
            "value".to_string(),
            "values".to_string(),
            "payload".to_string()
        ]
    );
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert_eq!(
        table.header,
        vec![
            "value".to_string(),
            "values".to_string(),
            "payload".to_string()
        ]
    );
    assert_eq!(table.rows.len(), 2);
    assert_eq!(table.rows[0]["value"], ScalarValue::UInt64(1));
    assert_eq!(
        table.rows[0]["values"],
        ScalarValue::List(vec![
            ScalarValue::Int64(1),
            ScalarValue::Int64(2),
            ScalarValue::Null
        ])
    );
    assert_eq!(
        table.rows[0]["payload"],
        ScalarValue::Struct(vec![
            ("value".to_string(), ScalarValue::UInt64(1)),
            ("metric".to_string(), ScalarValue::Int64(10))
        ])
    );
}

#[cfg(feature = "universal-format-io")]
#[test]
fn structured_parquet_row_export_footer_pruning_writes_empty_schema_without_scan() {
    let path = unique_vortex_path("structured-parquet-row-export-footer-pruned");
    let output_path = unique_vortex_path("structured-parquet-row-export-footer-pruned-output")
        .with_extension("parquet");
    write_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::structured_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexStructuredProjectionRequest::new(vec![
            VortexStructuredProjectionColumn::new(
                "value".to_string(),
                VortexStructuredProjectionExpr::SourceColumn(
                    ColumnRef::new("value").expect("column"),
                ),
            ),
            VortexStructuredProjectionColumn::new(
                "metric".to_string(),
                VortexStructuredProjectionExpr::SourceColumn(
                    ColumnRef::new("metric").expect("column"),
                ),
            ),
        ]),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("value").expect("column"),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(99),
    });

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Parquet,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let table = crate::read_flat_parquet_source(&output_path, 10).expect("parquet rows");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.output_format, "parquet");
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 0);
    assert_eq!(report.pre_limit_result_row_count, 0);
    assert_eq!(
        report.projected_columns,
        vec!["value".to_string(), "metric".to_string()]
    );
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(!report.evidence.side_effects.data_read);
    assert!(!report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.data_materialized);
    assert!(!report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert_eq!(
        table.header,
        vec!["value".to_string(), "metric".to_string()]
    );
    assert_eq!(
        table.rows,
        [] as [std::collections::BTreeMap<String, shardloom_core::ScalarValue>; 0]
    );
}

#[cfg(all(feature = "universal-format-io", feature = "vortex-write"))]
#[test]
fn structured_expression_project_row_export_writes_vortex_without_fallback() {
    let path = unique_vortex_path("structured-expression-project-vortex-row-export");
    let output_path = unique_vortex_path("structured-expression-project-vortex-row-export-output")
        .with_extension("vortex");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::structured_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexStructuredProjectionRequest::new(vec![
            VortexStructuredProjectionColumn::new(
                "value".to_string(),
                VortexStructuredProjectionExpr::SourceColumn(
                    ColumnRef::new("value").expect("column"),
                ),
            ),
            VortexStructuredProjectionColumn::new(
                "values".to_string(),
                VortexStructuredProjectionExpr::ArrayLiteral(vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Int64(2),
                    ScalarValue::Null,
                ]),
            ),
            VortexStructuredProjectionColumn::new(
                "payload".to_string(),
                VortexStructuredProjectionExpr::StructColumns(vec![
                    ColumnRef::new("value").expect("column"),
                    ColumnRef::new("metric").expect("column"),
                ]),
            ),
        ]),
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Vortex,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let count_report = execute_vortex_local_primitive(&VortexQueryPrimitiveRequest::count_all(
        DatasetUri::new(output_path.display().to_string()).expect("output uri"),
    ))
    .expect("count output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.output_format, "vortex");
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(
        report.projected_columns,
        vec![
            "value".to_string(),
            "values".to_string(),
            "payload".to_string()
        ]
    );
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert_eq!(
        count_report.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert_eq!(count_report.rows_scanned, 2);
    assert!(!count_report.upstream_scan_called);
    assert!(!count_report.fallback_execution_allowed);
}

#[cfg(all(feature = "universal-format-io", feature = "vortex-write"))]
#[test]
fn structured_vortex_row_export_footer_pruning_writes_empty_vortex_without_scan() {
    let path = unique_vortex_path("structured-vortex-row-export-footer-pruned");
    let output_path = unique_vortex_path("structured-vortex-row-export-footer-pruned-output")
        .with_extension("vortex");
    write_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::structured_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexStructuredProjectionRequest::new(vec![
            VortexStructuredProjectionColumn::new(
                "value".to_string(),
                VortexStructuredProjectionExpr::SourceColumn(
                    ColumnRef::new("value").expect("column"),
                ),
            ),
            VortexStructuredProjectionColumn::new(
                "metric".to_string(),
                VortexStructuredProjectionExpr::SourceColumn(
                    ColumnRef::new("metric").expect("column"),
                ),
            ),
        ]),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("value").expect("column"),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(99),
    });

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Vortex,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let count_report = execute_vortex_local_primitive(&VortexQueryPrimitiveRequest::count_all(
        DatasetUri::new(output_path.display().to_string()).expect("output uri"),
    ))
    .expect("count output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.output_format, "vortex");
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 0);
    assert_eq!(report.pre_limit_result_row_count, 0);
    assert_eq!(
        report.projected_columns,
        vec!["value".to_string(), "metric".to_string()]
    );
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(!report.evidence.side_effects.data_read);
    assert!(!report.evidence.side_effects.data_decoded);
    assert!(!report.evidence.side_effects.data_materialized);
    assert!(!report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert_eq!(
        count_report.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert_eq!(count_report.rows_scanned, 0);
    assert!(!count_report.upstream_scan_called);
    assert!(!count_report.fallback_execution_allowed);
}

#[test]
fn expression_project_row_export_writes_string_replacement_rows_without_fallback() {
    let path = unique_vortex_path("expression-project-string-replace");
    let output_path =
        unique_vortex_path("expression-project-string-replace-output").with_extension("jsonl");
    write_string_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![
            VortexExpressionRewrite::StringReplaceScalar {
                target_column: ColumnRef::new("label").expect("column"),
                needle: "bad".to_string(),
                replacement: "ok".to_string(),
            },
        ]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert_eq!(
        rows,
        "{\"label\":\"ok\"}\n{\"label\":\"good\"}\n{\"label\":\"okly\"}\n"
    );
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn expression_project_row_export_filters_source_strings_before_rewrite() {
    let path = unique_vortex_path("expression-project-row-export-source-string-filter");
    let output_path =
        unique_vortex_path("expression-project-row-export-source-string-filter-output")
            .with_extension("jsonl");
    write_string_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![
            VortexExpressionRewrite::StringReplaceScalar {
                target_column: ColumnRef::new("label").expect("column"),
                needle: "bad".to_string(),
                replacement: "ok".to_string(),
            },
        ]),
    );
    request.predicate = Some(PredicateExpr::StringContains {
        column: ColumnRef::new("label").expect("column"),
        needle: "bad".to_string(),
        negated: false,
    });

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert_eq!(rows, "{\"label\":\"ok\"}\n{\"label\":\"okly\"}\n");
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn expression_project_regex_replacement_translates_python_backrefs() {
    assert_eq!(
        python_regex_replacement_for_rust_regex(r"\1foo$\g<name>\g<2>"),
        "${1}foo$$${name}${2}"
    );
    assert_eq!(
        python_regex_replacement_for_rust_regex(r"\g<unterminated"),
        r"\g<unterminated"
    );
}

#[test]
fn expression_project_row_export_writes_regex_replacement_rows_without_fallback() {
    let path = unique_vortex_path("expression-project-regex-replace");
    let output_path =
        unique_vortex_path("expression-project-regex-replace-output").with_extension("jsonl");
    write_string_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("label").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::RegexReplaceScalar {
            target_column: ColumnRef::new("label").expect("column"),
            pattern: "^(bad)(.*)$".to_string(),
            replacement: "\\1-fixed\\2".to_string(),
        }]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(report.projected_columns, vec!["label".to_string()]);
    assert_eq!(
        rows,
        "{\"label\":\"bad-fixed\"}\n{\"label\":\"good\"}\n{\"label\":\"bad-fixedly\"}\n"
    );
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn expression_project_row_export_writes_numeric_scalar_assignment_without_fallback() {
    let path = unique_vortex_path("expression-project-numeric-assignment");
    let output_path =
        unique_vortex_path("expression-project-numeric-assignment-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![
            VortexExpressionRewrite::NumericScalarArithmetic {
                target_column: ColumnRef::new("metric").expect("column"),
                operator: "+".to_string(),
                operand: shardloom_core::ScalarValue::Int64(5),
            },
        ]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 5);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.projected_columns, vec!["metric".to_string()]);
    assert_eq!(
        rows,
        "{\"metric\":15}\n{\"metric\":25}\n{\"metric\":35}\n{\"metric\":45}\n{\"metric\":55}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn expression_project_row_export_adds_source_order_row_number_without_fallback() {
    let path = unique_vortex_path("expression-project-row-number");
    let output_path =
        unique_vortex_path("expression-project-row-number-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::RowNumber {
            target_column: ColumnRef::new("index").expect("column"),
            start: 0,
        }]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::ExpressionProjectRows
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 5);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(
        report.projected_columns,
        vec!["metric".to_string(), "index".to_string()]
    );
    assert_eq!(
        parsed_jsonl_rows(&rows),
        serde_json::json!([
            {"index": 0, "metric": 10}, {"index": 1, "metric": 20},
            {"index": 2, "metric": 30}, {"index": 3, "metric": 40},
            {"index": 4, "metric": 50}
        ])
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn melt_row_export_writes_expanded_rows_without_fallback() {
    let path = unique_vortex_path("melt-row-export");
    let output_path = unique_vortex_path("melt-row-export-output").with_extension("jsonl");
    write_melt_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::melt_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexMeltProjectionRequest::new(
            vec![ColumnRef::new("id").expect("column")],
            vec![
                ColumnRef::new("amount_a").expect("column"),
                ColumnRef::new("amount_b").expect("column"),
            ],
            "measure".to_string(),
            "amount".to_string(),
        ),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::MeltRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 6);
    assert_eq!(report.pre_limit_result_row_count, 6);
    assert_eq!(
        report.projected_columns,
        vec![
            "id".to_string(),
            "measure".to_string(),
            "amount".to_string()
        ]
    );
    assert_eq!(
        parsed_jsonl_rows(&rows),
        serde_json::json!([
            {"amount": 10, "id": 1, "measure": "amount_a"},
            {"amount": 100, "id": 1, "measure": "amount_b"},
            {"amount": 20, "id": 2, "measure": "amount_a"},
            {"amount": 200, "id": 2, "measure": "amount_b"},
            {"amount": 30, "id": 3, "measure": "amount_a"},
            {"amount": 300, "id": 3, "measure": "amount_b"}
        ])
    );
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn melt_row_export_filters_source_predicate_before_expansion() {
    let path = unique_vortex_path("melt-row-export-source-filter");
    let output_path =
        unique_vortex_path("melt-row-export-source-filter-output").with_extension("jsonl");
    write_melt_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::melt_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexMeltProjectionRequest::new(
            vec![ColumnRef::new("id").expect("column")],
            vec![
                ColumnRef::new("amount_a").expect("column"),
                ColumnRef::new("amount_b").expect("column"),
            ],
            "measure".to_string(),
            "amount".to_string(),
        ),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("id").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(2),
    });

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::MeltRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 4);
    assert_eq!(report.pre_limit_result_row_count, 4);
    assert_eq!(
        parsed_jsonl_rows(&rows),
        serde_json::json!([
            {"amount": 20, "id": 2, "measure": "amount_a"},
            {"amount": 200, "id": 2, "measure": "amount_b"},
            {"amount": 30, "id": 3, "measure": "amount_a"},
            {"amount": 300, "id": 3, "measure": "amount_b"}
        ])
    );
    assert!(report.evidence.pushdown.filter_pushdown_applied);
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.external_effects_executed);
}

#[test]
fn melt_row_export_writes_mixed_scalar_values_without_fallback() {
    let path = unique_vortex_path("melt-mixed-row-export");
    let output_path = unique_vortex_path("melt-mixed-row-export-output").with_extension("jsonl");
    write_melt_mixed_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::melt_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexMeltProjectionRequest::new(
            vec![ColumnRef::new("id").expect("column")],
            vec![
                ColumnRef::new("amount").expect("column"),
                ColumnRef::new("label").expect("column"),
            ],
            "field".to_string(),
            "value".to_string(),
        ),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::MeltRows);
    assert_eq!(report.rows_scanned, 2);
    assert_eq!(report.rows_written, 4);
    assert_eq!(report.pre_limit_result_row_count, 4);
    assert_eq!(
        report.projected_columns,
        vec!["id".to_string(), "field".to_string(), "value".to_string()]
    );
    assert_eq!(
        parsed_jsonl_rows(&rows),
        serde_json::json!([
            {"field": "amount", "id": 1, "value": 10},
            {"field": "label", "id": 1, "value": "paid"},
            {"field": "amount", "id": 2, "value": 20},
            {"field": "label", "id": 2, "value": "trial"}
        ])
    );
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);

    let limited_path = unique_vortex_path("pivot-fill-margins-limited-row-export");
    let limited_output_path =
        unique_vortex_path("pivot-fill-margins-limited-row-export-output").with_extension("jsonl");
    write_pivot_struct_fixture(&limited_path).expect("fixture");
    let limited_request = VortexQueryPrimitiveRequest::pivot_rows(
        DatasetUri::new(limited_path.display().to_string()).expect("uri"),
        VortexPivotProjectionRequest::new(
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("label").expect("column"),
            ColumnRef::new("amount").expect("column"),
            "sum",
        )
        .with_output_policy(
            Some(shardloom_core::ScalarValue::Int64(0)),
            false,
            true,
            "total",
        ),
    )
    .with_source_order_limit(2);

    let limited_report = execute_vortex_local_primitive_row_export_with_policy(
        &limited_request,
        &limited_output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("limited report");
    let limited_rows = std::fs::read_to_string(&limited_output_path).expect("limited output");
    let limited_parsed_rows = limited_rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&limited_path);
    let _ = std::fs::remove_file(&limited_output_path);

    assert_eq!(
        limited_report.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert_eq!(limited_report.rows_written, 2);
    assert_eq!(limited_report.pre_limit_result_row_count, 3);
    assert_eq!(limited_report.source_order_limit_requested, Some(2));
    assert_eq!(limited_parsed_rows.len(), 2);
    assert_eq!(limited_parsed_rows[0]["id"], serde_json::json!(1));
    assert_eq!(limited_parsed_rows[1]["id"], serde_json::json!("total"));
    assert!(limited_report.evidence.pushdown.source_order_limit_applied);
    assert!(!limited_report.evidence.side_effects.fallback_attempted);
}

#[test]
fn pivot_row_export_writes_sparse_wide_rows_without_fallback() {
    let path = unique_vortex_path("pivot-row-export");
    let output_path = unique_vortex_path("pivot-row-export-output").with_extension("jsonl");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::pivot_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexPivotProjectionRequest::new(
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("label").expect("column"),
            ColumnRef::new("amount").expect("column"),
            "sum",
        ),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let parsed_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::PivotRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert_eq!(
        report.projected_columns,
        vec![
            "id".to_string(),
            "pivot_paid".to_string(),
            "pivot_trial".to_string()
        ]
    );
    assert_eq!(parsed_rows.len(), 2);
    assert_eq!(parsed_rows[0]["id"], serde_json::json!(1));
    assert_eq!(parsed_rows[0]["pivot_paid"].as_f64(), Some(15.0));
    assert!(parsed_rows[0]["pivot_trial"].is_null());
    assert_eq!(parsed_rows[1]["id"], serde_json::json!(2));
    assert!(parsed_rows[1]["pivot_paid"].is_null());
    assert_eq!(parsed_rows[1]["pivot_trial"].as_f64(), Some(7.0));
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn pivot_row_export_writes_fill_and_margins_without_fallback() {
    let path = unique_vortex_path("pivot-fill-margins-row-export");
    let output_path =
        unique_vortex_path("pivot-fill-margins-row-export-output").with_extension("jsonl");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::pivot_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexPivotProjectionRequest::new(
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("label").expect("column"),
            ColumnRef::new("amount").expect("column"),
            "sum",
        )
        .with_output_policy(
            Some(shardloom_core::ScalarValue::Int64(0)),
            false,
            true,
            "total",
        ),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let parsed_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::PivotRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(
        report.projected_columns,
        vec![
            "id".to_string(),
            "pivot_paid".to_string(),
            "pivot_trial".to_string(),
            "pivot_total".to_string(),
        ]
    );
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(parsed_rows.len(), 3);
    assert_eq!(parsed_rows[0]["id"], serde_json::json!(1));
    assert_eq!(parsed_rows[0]["pivot_paid"].as_f64(), Some(15.0));
    assert_eq!(parsed_rows[0]["pivot_trial"], serde_json::json!(0));
    assert_eq!(parsed_rows[0]["pivot_total"].as_f64(), Some(15.0));
    assert_eq!(parsed_rows[1]["id"], serde_json::json!(2));
    assert_eq!(parsed_rows[1]["pivot_paid"], serde_json::json!(0));
    assert_eq!(parsed_rows[1]["pivot_trial"].as_f64(), Some(7.0));
    assert_eq!(parsed_rows[1]["pivot_total"].as_f64(), Some(7.0));
    assert_eq!(parsed_rows[2]["id"], serde_json::json!("total"));
    assert_eq!(parsed_rows[2]["pivot_paid"].as_f64(), Some(15.0));
    assert_eq!(parsed_rows[2]["pivot_trial"].as_f64(), Some(7.0));
    assert_eq!(parsed_rows[2]["pivot_total"].as_f64(), Some(22.0));
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn pivot_row_export_writes_min_max_aggregates_without_fallback() {
    let path = unique_vortex_path("pivot-min-max-row-export");
    let max_output_path = unique_vortex_path("pivot-max-row-export-output").with_extension("jsonl");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::pivot_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexPivotProjectionRequest::new(
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("label").expect("column"),
            ColumnRef::new("amount").expect("column"),
            "max",
        ),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &max_output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&max_output_path).expect("output");
    let parsed_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&max_output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::PivotRows);
    assert_eq!(parsed_rows.len(), 2);
    assert_eq!(parsed_rows[0]["pivot_paid"].as_f64(), Some(10.0));
    assert!(parsed_rows[0]["pivot_trial"].is_null());
    assert!(parsed_rows[1]["pivot_paid"].is_null());
    assert_eq!(parsed_rows[1]["pivot_trial"].as_f64(), Some(7.0));
    assert!(!report.evidence.side_effects.fallback_attempted);
}

#[test]
fn explode_row_export_writes_expanded_rows_without_fallback() {
    let path = unique_vortex_path("explode-row-export");
    let output_path = unique_vortex_path("explode-row-export-output").with_extension("jsonl");
    write_explode_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::explode_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("items").expect("column"),
        ]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column")),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(
        report.projected_columns,
        vec!["id".to_string(), "items".to_string()]
    );
    assert_eq!(
        rows,
        "{\"id\":1,\"items\":7}\n{\"id\":1,\"items\":8}\n{\"id\":3,\"items\":9}\n"
    );
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn explode_row_export_writes_multi_column_expanded_rows_without_fallback() {
    let path = unique_vortex_path("multi-explode-row-export");
    let output_path = unique_vortex_path("multi-explode-row-export-output").with_extension("jsonl");
    write_multi_explode_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::explode_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("items").expect("column"),
            ColumnRef::new("labels").expect("column"),
        ]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column")).with_columns(
            vec![
                ColumnRef::new("items").expect("column"),
                ColumnRef::new("labels").expect("column"),
            ],
        ),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 3);
    assert_eq!(report.pre_limit_result_row_count, 3);
    assert_eq!(
        report.projected_columns,
        vec!["id".to_string(), "items".to_string(), "labels".to_string()]
    );
    assert_eq!(
        rows,
        "{\"id\":1,\"items\":7,\"labels\":\"red\"}\n{\"id\":1,\"items\":8,\"labels\":\"blue\"}\n{\"id\":3,\"items\":9,\"labels\":\"green\"}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn explode_row_export_writes_nullable_nested_rows_without_fallback() {
    let path = unique_vortex_path("nullable-nested-explode-row-export");
    let output_path =
        unique_vortex_path("nullable-nested-explode-row-export-output").with_extension("jsonl");
    write_nullable_nested_explode_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::explode_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("items").expect("column"),
        ]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column")),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 4);
    assert_eq!(report.pre_limit_result_row_count, 4);
    assert_eq!(
        report.projected_columns,
        vec!["id".to_string(), "items".to_string()]
    );
    assert_eq!(
        rows,
        "{\"id\":1,\"items\":{\"code\":7,\"label\":\"red\"}}\n{\"id\":1,\"items\":{\"code\":8,\"label\":\"blue\"}}\n{\"id\":2,\"items\":null}\n{\"id\":3,\"items\":{\"code\":9,\"label\":\"green\"}}\n"
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn explode_row_export_writes_dotted_struct_field_rows_without_fallback() {
    let path = unique_vortex_path("dotted-field-explode-row-export");
    let output_path =
        unique_vortex_path("dotted-field-explode-row-export-output").with_extension("jsonl");
    write_nullable_nested_explode_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::explode_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("items").expect("column"),
        ]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column"))
            .with_element_field("code".to_string(), "code".to_string()),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let parsed_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 4);
    assert_eq!(report.pre_limit_result_row_count, 4);
    assert_eq!(
        report.projected_columns,
        vec!["id".to_string(), "code".to_string()]
    );
    assert_eq!(
        parsed_rows,
        vec![
            serde_json::json!({"id": 1, "code": 7}),
            serde_json::json!({"id": 1, "code": 8}),
            serde_json::json!({"id": 2, "code": null}),
            serde_json::json!({"id": 3, "code": 9}),
        ]
    );
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn local_primitive_native_io_certificate_covers_filter_project_path() {
    let path = unique_vortex_path("native-io-filter-project");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert!(certificate.is_certified());
    assert_eq!(certificate.status(), "certified");
    assert_eq!(
        certificate.certificate_id,
        "cg19.local_primitive.filter_and_project.native_io"
    );
    assert_eq!(
        certificate.path_id,
        "native_vortex_source_to_filtered_projected_result"
    );
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "filter,project"
    );
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->selection_vector_encoded"
    );
    assert_eq!(
        certificate.sink_requirement_report.target_format,
        "local_filtered_projected_stream_summary"
    );
    assert!(certificate.source_capability_report.streaming_capability);
    assert!(
        certificate
            .source_capability_report
            .encoded_representation_preserved
    );
    assert!(
        certificate
            .adapter_fidelity_report
            .encoded_representation_preserved
    );
    assert!(certificate.materializing_transitions_have_boundaries());
    assert_eq!(certificate.materialization_boundary_order(), "");
    assert!(certificate.side_effects.data_read);
    assert!(!certificate.side_effects.data_decoded);
    assert!(!certificate.side_effects.data_materialized);
    assert!(!certificate.side_effects.row_read);
    assert!(!certificate.side_effects.arrow_converted);
    assert!(!certificate.side_effects.object_store_io);
    assert!(!certificate.side_effects.write_io);
    assert!(!certificate.side_effects.spill_io_performed);
    assert!(!certificate.side_effects.fallback_attempted);
    assert!(!certificate.side_effects.fallback_execution_allowed);
}

#[test]
fn local_primitive_native_io_certificate_covers_filter_path() {
    let path = unique_vortex_path("native-io-filter");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert!(certificate.is_certified());
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "filter"
    );
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->selection_vector_encoded"
    );
    assert_eq!(
        certificate.sink_requirement_report.target_format,
        "local_filtered_stream_summary"
    );
    assert!(certificate.sink_requirement_report.accepts_encoded);
    assert!(!certificate.sink_requirement_report.requires_rows);
    assert!(certificate.side_effects.data_read);
    assert!(!certificate.side_effects.data_decoded);
    assert!(!certificate.side_effects.data_materialized);
    assert!(!certificate.side_effects.row_read);
    assert!(!certificate.side_effects.arrow_converted);
    assert!(!certificate.side_effects.fallback_attempted);
}

#[test]
fn local_primitive_native_io_certificate_covers_project_path() {
    let path = unique_vortex_path("native-io-project");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::project(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert!(certificate.is_certified());
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "project"
    );
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->vortex_encoded"
    );
    assert_eq!(
        certificate.sink_requirement_report.target_format,
        "local_projected_stream_summary"
    );
    assert!(certificate.sink_requirement_report.accepts_encoded);
    assert!(!certificate.sink_requirement_report.requires_rows);
}

#[test]
fn local_primitive_native_io_certificate_allows_primitive_projection_passthrough() {
    let path = unique_vortex_path("native-io-primitive-project");
    write_primitive_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::project(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.rows_projected, Some(3));
    assert_eq!(report.projected_columns, vec!["value".to_string()]);
    assert!(!report.projection_pushdown_applied);
    assert!(!report.upstream_projection_expression_used);
    assert!(certificate.is_certified());
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "project"
    );
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->vortex_encoded"
    );
}

#[test]
fn local_primitive_native_io_certificate_allows_primitive_filter_project_passthrough() {
    let path = unique_vortex_path("native-io-primitive-filter-project");
    write_primitive_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(8),
        },
        ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(!report.projection_pushdown_applied);
    assert!(!report.upstream_projection_expression_used);
    assert!(certificate.is_certified());
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        "filter,project"
    );
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->selection_vector_encoded"
    );
}

#[test]
fn local_primitive_native_io_certificate_blocks_unsafe_effects() {
    let path = unique_vortex_path("native-io-blocked");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
    );

    let mut report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);
    report.data_materialized = true;

    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");

    assert_eq!(certificate.status(), "blocked");
    assert!(!certificate.is_certified());
    assert_eq!(
        certificate.representation_transition_order(),
        "vortex_encoded->unsupported"
    );
    assert_eq!(
        certificate
            .source_pushdown_report
            .rejected_operation_order(),
        "filter_and_project"
    );
    assert!(certificate.side_effects.data_materialized);
    assert!(certificate.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == DiagnosticCode::NoFallbackExecution
            && diagnostic.severity == DiagnosticSeverity::Error
    }));
}

#[test]
fn local_primitive_certificates_cover_broader_runtime_fixtures() {
    let cases = [
        (
            "vortex-local-count-all-struct-five",
            VortexQueryPrimitiveRequest::count_all(
                DatasetUri::new("placeholder.vortex").expect("uri"),
            ),
        ),
        (
            "vortex-local-count-where-struct-five",
            VortexQueryPrimitiveRequest::count_where(
                DatasetUri::new("placeholder.vortex").expect("uri"),
                PredicateExpr::Compare {
                    column: ColumnRef::new("value").expect("column"),
                    op: ComparisonOp::GtEq,
                    value: StatValue::Int64(3),
                },
            ),
        ),
        (
            "vortex-local-project-struct-five",
            VortexQueryPrimitiveRequest::project(
                DatasetUri::new("placeholder.vortex").expect("uri"),
                ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            ),
        ),
        (
            "vortex-local-filter-struct-five",
            VortexQueryPrimitiveRequest::filter(
                DatasetUri::new("placeholder.vortex").expect("uri"),
                PredicateExpr::Compare {
                    column: ColumnRef::new("value").expect("column"),
                    op: ComparisonOp::GtEq,
                    value: StatValue::Int64(3),
                },
            ),
        ),
        (
            "vortex-local-filter-project-struct-five",
            VortexQueryPrimitiveRequest::filter_and_project(
                DatasetUri::new("placeholder.vortex").expect("uri"),
                PredicateExpr::Compare {
                    column: ColumnRef::new("value").expect("column"),
                    op: ComparisonOp::GtEq,
                    value: StatValue::Int64(3),
                },
                ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            ),
        ),
    ];

    for (fixture_id, mut request) in cases {
        request.source_uri = Some(checked_in_struct_fixture_uri());

        let report = execute_vortex_local_primitive(&request).expect("report");
        let certificate = local_primitive_execution_certificate(
            &correctness_fixture(fixture_id),
            &request,
            &report,
        )
        .expect("certificate");

        assert_eq!(
            certificate.status,
            ExecutionCertificateStatus::Certified,
            "{fixture_id}"
        );
        assert!(certificate.is_certified(), "{fixture_id}");
        assert_eq!(
            certificate.correctness_fixture_id.as_deref(),
            Some(fixture_id)
        );
        assert_eq!(certificate.expected_outcome, certificate.actual_outcome);
        assert_eq!(
            certificate.data_read,
            fixture_id != "vortex-local-count-all-struct-five",
            "{fixture_id}"
        );
        assert!(!certificate.data_decoded);
        assert!(!certificate.data_materialized);
        assert!(!certificate.row_read);
        assert!(!certificate.arrow_converted);
        assert!(!certificate.object_store_io);
        assert!(!certificate.write_io);
        assert!(!certificate.spill_io_performed);
        assert!(!certificate.external_effects_executed);
        assert!(certificate.fallback_free());
    }
}

#[test]
fn local_primitive_certificate_blocks_fixture_identity_mismatch() {
    let request = VortexQueryPrimitiveRequest::filter(
        checked_in_struct_fixture_uri(),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let certificate = local_primitive_execution_certificate(
        &correctness_fixture("vortex-local-filter-project-struct-five"),
        &request,
        &report,
    )
    .expect("certificate");

    assert_eq!(
        certificate.status,
        ExecutionCertificateStatus::EvidenceIncomplete
    );
    assert!(!certificate.is_certified());
    assert_eq!(
        certificate.correctness_fixture_id.as_deref(),
        Some("vortex-local-filter-project-struct-five")
    );
    assert_eq!(certificate.expected_outcome, certificate.actual_outcome);
    assert!(certificate.fallback_free());
}

#[test]
fn local_vortex_path_resolves_relative_targets_from_workspace_root() {
    let uri = DatasetUri::new("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
        .expect("uri");

    let path = local_vortex_path(&uri, VortexQueryPrimitiveKind::CountAll)
        .expect("path")
        .expect("existing path");

    assert_eq!(
        path.canonicalize().expect("canonical"),
        checked_in_struct_fixture_path()
            .canonicalize()
            .expect("fixture canonical")
    );
}

#[test]
fn simple_aggregate_accumulates_scalar_measures_without_fallback() {
    let path = unique_vortex_path("simple-aggregate");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::new(vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("metric").expect("column")),
                "sum_metric".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("metric").expect("column")),
                "avg_metric".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("value").expect("column")),
                "min_value".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "max",
                Some(ColumnRef::new("value").expect("column")),
                "max_value".to_string(),
            ),
        ]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(5));
    assert_eq!(report.rows_projected, Some(1));
    assert!(report.upstream_scan_called);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.state_budget.state_budget_required);
    assert_eq!(
        report.state_budget.state_family,
        "scalar_aggregate_state+direct_dictionary_or_typed+expression_fusion"
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"aggregate_state".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"dictionary_or_typed_direct_scalar_aggregate".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"expression_plan_fingerprint_reuse".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"aggregate_input_rows".to_string())
    );
    assert_eq!(
        report.state_budget.spill_policy,
        "fail_closed_before_uncertified_spill"
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"dictionary_or_typed_direct_scalar_update\""));
    assert!(summary.contains("\"rows\":5"));
    assert!(summary.contains("\"sum_metric\":150.0"));
    assert!(summary.contains("\"avg_metric\":30.0"));
    assert!(summary.contains("\"min_value\":1"));
    assert!(summary.contains("\"max_value\":5"));
}

#[test]
fn simple_aggregate_rerun_stays_on_single_vortex_artifact_without_query_summary_sidecar() {
    let path = unique_vortex_path("simple-aggregate-single-vortex-artifact");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("value").expect("column")],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("metric").expect("column")),
                    "sum_metric".to_string(),
                ),
            ],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]),
    )
    .with_source_order_limit(2);

    let first = execute_vortex_local_primitive(&request).expect("first report");
    assert_eq!(
        first.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert!(first.upstream_scan_called);
    assert!(first.data_read);
    let first_summary = first.result_summary.as_deref().expect("first summary");
    let first_payload = simple_aggregate_values_json(first_summary);
    assert_eq!(
        first_payload.get("prepared_olap_query_summary_written"),
        None
    );
    assert_eq!(first_payload.get("prepared_olap_query_summary_hit"), None);
    assert_eq!(
        first_payload.get("prepared_olap_query_summary_consumed"),
        None
    );

    let second = execute_vortex_local_primitive(&request).expect("second report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(
        second.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(second.rows_scanned, 5);
    assert_eq!(second.rows_selected, Some(5));
    assert_eq!(second.rows_projected, Some(2));
    assert!(second.upstream_scan_called);
    assert!(second.data_read);
    assert!(second.data_decoded);
    assert!(second.data_materialized);
    assert!(second.row_read);
    assert!(!second.fallback_execution_allowed);
    let second_summary = second.result_summary.as_deref().expect("second summary");
    let second_payload = simple_aggregate_values_json(second_summary);
    assert_eq!(second_payload.get("prepared_olap_query_summary_hit"), None);
    assert_eq!(
        second_payload.get("prepared_olap_query_summary_consumed"),
        None
    );
    assert_eq!(second_payload.get("prepared_olap_raw_scan_avoided"), None);
    assert_eq!(second_payload["rows"], serde_json::json!(2));
}

#[test]
fn simple_aggregate_skips_null_measures_without_fallback() {
    let path = unique_vortex_path("simple-aggregate-null-measures");
    write_all_null_aggregate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::new(vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "count",
                Some(ColumnRef::new("amount").expect("column")),
                "non_null_amount".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("amount").expect("column")),
                "unique_amount".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("amount").expect("column")),
                "sum_amount".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("amount").expect("column")),
                "avg_amount".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("amount").expect("column")),
                "min_amount".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "max",
                Some(ColumnRef::new("amount").expect("column")),
                "max_amount".to_string(),
            ),
        ]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(3));
    assert_eq!(report.rows_projected, Some(1));
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    let values = payload["values"].as_object().expect("values");
    assert_eq!(payload["rows"], serde_json::json!(1));
    assert_eq!(values["rows"], serde_json::json!(3));
    assert_eq!(values["non_null_amount"], serde_json::json!(0));
    assert_eq!(values["unique_amount"], serde_json::json!(0));
    assert!(values["sum_amount"].is_null());
    assert!(values["avg_amount"].is_null());
    assert!(values["min_amount"].is_null());
    assert!(values["max_amount"].is_null());
}

#[test]
fn direct_count_distinct_uses_used_dictionary_codes_not_all_dictionary_values() {
    let alpha = std::sync::Arc::<str>::from("alpha");
    let unused = std::sync::Arc::<str>::from("unused");
    let beta = std::sync::Arc::<str>::from("beta");
    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 2],
        values: vec![
            std::sync::Arc::clone(&alpha),
            std::sync::Arc::clone(&unused),
            std::sync::Arc::clone(&beta),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    };
    let mut distinct_values = AggregateDistinctSet::default();

    let update = aggregate_direct_count_distinct_update(&accessor, None, &mut distinct_values)
        .expect("dictionary distinct update");

    assert_eq!(update.non_null_rows, 3);
    assert!(!update.dense_integer_preunion_used);
    assert_eq!(distinct_values.len(), 2);
    assert!(
        distinct_values
            .iter()
            .any(|value| matches!(value, AggregateDistinctValue::Utf8(value) if std::sync::Arc::ptr_eq(value, &alpha)))
    );
    assert!(
        distinct_values
            .iter()
            .any(|value| matches!(value, AggregateDistinctValue::Utf8(value) if std::sync::Arc::ptr_eq(value, &beta)))
    );
    assert!(
        !distinct_values.contains(&AggregateDistinctValue::Utf8(std::sync::Arc::clone(
            &unused
        )))
    );

    let mut filtered_distinct_values = AggregateDistinctSet::default();
    let filtered_update = aggregate_direct_count_distinct_update(
        &accessor,
        Some(&[0, 1]),
        &mut filtered_distinct_values,
    )
    .expect("filtered dictionary distinct update");

    assert_eq!(filtered_update.non_null_rows, 2);
    assert!(!filtered_update.dense_integer_preunion_used);
    assert_eq!(filtered_distinct_values.len(), 1);
    assert!(
        filtered_distinct_values
            .iter()
            .any(|value| matches!(value, AggregateDistinctValue::Utf8(value) if std::sync::Arc::ptr_eq(value, &alpha)))
    );

    let nullable_accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1, 2, 0],
        values: vec![
            std::sync::Arc::<str>::from("alpha"),
            std::sync::Arc::<str>::from("null-value"),
            std::sync::Arc::<str>::from("beta"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: Some(vec![false, true, false]),
        row_nulls: Some(vec![false, false, true, false]),
        source: AggregateUtf8DictionarySource::VortexDictArray,
    };
    let mut nullable_distinct_values = AggregateDistinctSet::default();
    let nullable_update = aggregate_direct_count_distinct_update(
        &nullable_accessor,
        None,
        &mut nullable_distinct_values,
    )
    .expect("nullable dictionary distinct update");
    assert_eq!(nullable_update.non_null_rows, 2);
    assert_eq!(nullable_distinct_values.len(), 1);
    assert!(
        nullable_distinct_values.contains(&AggregateDistinctValue::Utf8(
            std::sync::Arc::<str>::from("alpha")
        ))
    );
}

#[test]
fn aggregate_accessor_keeps_numeric_vortex_dictionary_direct() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray};

    let codes = [0_u8, 1, 0, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = [7_u32, 11, 13]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let dictionary = DictArray::try_new(codes, values)
        .expect("numeric dictionary")
        .into_array();

    let accessor = aggregate_direct_column_accessor(
        "__shardloom_derived_utf8_len_URL",
        &dictionary,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("numeric dictionary accessor");

    assert_eq!(accessor.evidence_kind(), "direct_u64");
    let AggregateDirectColumnAccessor::UInt64(values) = accessor else {
        panic!("numeric dictionary should lower to direct u64 values");
    };
    assert_eq!(values, vec![7, 11, 7, 13]);
    let mut distinct_values = AggregateDistinctSet::default();
    let accessor = AggregateDirectColumnAccessor::UInt64(values);
    let update = aggregate_direct_count_distinct_update(&accessor, None, &mut distinct_values)
        .expect("numeric dictionary distinct update");
    assert_eq!(update.non_null_rows, 4);
    assert!(update.dense_integer_preunion_used);
    assert_eq!(distinct_values.len(), 3);
    assert!(distinct_values.contains(&AggregateDistinctValue::UInt64(7)));
    assert!(distinct_values.contains(&AggregateDistinctValue::UInt64(11)));
    assert!(distinct_values.contains(&AggregateDistinctValue::UInt64(13)));
}

#[test]
fn aggregate_accessor_keeps_encoded_numeric_vortex_dictionary_direct() {
    use vortex::array::IntoArray as _;
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray};
    use vortex::encodings::fastlanes::BitPackedData;

    let mut ctx = vortex::session::VortexSession::empty().create_execution_ctx();
    let codes = [0_u8, 1, 0, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = [7_u32, 11, 13]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let encoded_codes = BitPackedData::encode(&codes, 2, &mut ctx)
        .expect("encoded dictionary codes")
        .into_array();
    let encoded_values = BitPackedData::encode(&values, 4, &mut ctx)
        .expect("encoded dictionary values")
        .into_array();
    let dictionary = DictArray::try_new(encoded_codes, encoded_values)
        .expect("encoded numeric dictionary")
        .into_array();

    let accessor = aggregate_direct_column_accessor(
        "CounterID",
        &dictionary,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("encoded numeric dictionary accessor");

    assert_eq!(accessor.evidence_kind(), "direct_u64");
    let AggregateDirectColumnAccessor::UInt64(values) = accessor else {
        panic!("encoded numeric dictionary should lower to direct u64 values");
    };
    assert_eq!(values, vec![7, 11, 7, 13]);
}

#[test]
fn aggregate_accessor_keeps_filtered_primitives_typed() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{FilterArray, PrimitiveArray};
    use vortex::mask::Mask;

    let values = [10_i64, 20, 10, 30, 20]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let filtered =
        FilterArray::new(values, Mask::from_iter([true, false, true, true, false])).into_array();

    let accessor = aggregate_direct_column_accessor(
        "UserID",
        &filtered,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered primitive aggregate accessor");

    assert_eq!(accessor.evidence_kind(), "direct_i64");
    assert_eq!(accessor.len(), 3);
    let AggregateDirectColumnAccessor::NativeNumeric(owner) = accessor else {
        panic!("filtered primitive should retain its native typed owner");
    };
    drop(filtered);
    assert_eq!(owner.ptype(), vortex::array::dtype::PType::I64);
    assert!(owner.all_valid());
    assert_eq!(owner.null_rows(true), [] as [usize; 0]);
    assert_eq!(owner.i64_values(), Some([10_i64, 10, 30].as_slice()));
}

#[test]
fn aggregate_accessor_keeps_selected_valid_nullable_primitives_typed() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{FilterArray, PrimitiveArray};
    use vortex::array::validity::Validity;
    use vortex::mask::Mask;

    let values = PrimitiveArray::new(
        vec![10_i64, 20, 10, 30, 20],
        Validity::from_iter([true, false, true, true, false]),
    )
    .into_array();
    let filtered =
        FilterArray::new(values, Mask::from_iter([true, false, true, true, false])).into_array();

    let accessor = aggregate_direct_column_accessor(
        "UserID",
        &filtered,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered nullable primitive aggregate accessor");

    assert_eq!(accessor.evidence_kind(), "direct_i64");
    assert_eq!(accessor.len(), 3);
    let AggregateDirectColumnAccessor::NativeNumeric(owner) = accessor else {
        panic!("selected valid nullable primitive should retain its native typed owner");
    };
    drop(filtered);
    assert_eq!(owner.ptype(), vortex::array::dtype::PType::I64);
    assert!(owner.all_valid());
    assert_eq!(owner.null_rows(true), [] as [usize; 0]);
    assert_eq!(owner.i64_values(), Some([10_i64, 10, 30].as_slice()));
}

#[test]
fn aggregate_accessor_keeps_selected_nullable_primitives_direct_with_retained_nulls() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{FilterArray, PrimitiveArray};
    use vortex::array::validity::Validity;
    use vortex::mask::Mask;

    let values = PrimitiveArray::new(
        vec![10_i64, 20, 10, 30, 20],
        Validity::from_iter([true, false, true, true, false]),
    )
    .into_array();
    let filtered =
        FilterArray::new(values, Mask::from_iter([true, true, true, false, false])).into_array();

    let accessor = aggregate_direct_column_accessor(
        "UserID",
        &filtered,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("filtered nullable primitive aggregate accessor");

    assert_eq!(accessor.evidence_kind(), "direct_i64_nullable");
    assert_eq!(accessor.len(), 3);
    let AggregateDirectColumnAccessor::NativeNumeric(owner) = &accessor else {
        panic!("retained nullable primitive should retain its native typed owner and validity");
    };
    drop(filtered);
    assert_eq!(owner.ptype(), vortex::array::dtype::PType::I64);
    assert!(!owner.all_valid());
    assert_eq!(owner.null_rows(true), vec![1]);
    assert_eq!(owner.null_rows(false), vec![0, 2]);
    assert!(owner.i64_values().is_none());
    let logical_values = (0..accessor.len())
        .map(|row| aggregate_direct_stat_value(&accessor, row).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        logical_values,
        vec![StatValue::Int64(10), StatValue::Null, StatValue::Int64(10)]
    );
    assert_eq!(
        aggregate_direct_numeric_sum_count(&accessor, None)
            .map(|(count, sum)| (count, sum.total()))
            .unwrap(),
        (2, 20.0)
    );
}

#[test]
fn nullable_primitive_direct_aggregate_semantics_skip_nulls() {
    let accessor = AggregateDirectColumnAccessor::NullableInt64 {
        values: vec![10, 20, 10],
        row_nulls: vec![false, true, false],
    };

    assert_eq!(
        aggregate_direct_non_null_row_count(&accessor, None).expect("non-null row count"),
        2
    );
    assert_eq!(
        aggregate_direct_stat_value(&accessor, 1).expect("null stat value"),
        StatValue::Null
    );
    assert_eq!(
        aggregate_direct_numeric_sum_count(&accessor, None)
            .map(|(count, sum)| (count, sum.total()))
            .expect("sum/count"),
        (2, 20.0)
    );

    let mut distinct_values = AggregateDistinctSet::default();
    let update = aggregate_direct_count_distinct_update(&accessor, None, &mut distinct_values)
        .expect("direct nullable distinct");
    assert_eq!(update.non_null_rows, 2);
    assert!(update.dense_integer_preunion_used);
    assert_eq!(distinct_values.len(), 1);
    assert!(distinct_values.contains(&AggregateDistinctValue::Int64(10)));
    assert!(!distinct_values.contains(&AggregateDistinctValue::Int64(20)));
}

#[test]
fn direct_count_distinct_integer_uses_dense_id_preunion_for_compact_ranges() {
    let mut unsigned_values = AggregateDistinctSet::default();
    let dense_unsigned_used = aggregate_direct_count_distinct_update_dense_u64(
        &[7, 8, 7, 9, 8, 123],
        Some(&[false, false, false, false, false, true]),
        None,
        &mut unsigned_values,
    )
    .expect("dense unsigned distinct");
    assert!(dense_unsigned_used);
    assert_eq!(unsigned_values.len(), 3);
    assert!(unsigned_values.contains(&AggregateDistinctValue::UInt64(7)));
    assert!(unsigned_values.contains(&AggregateDistinctValue::UInt64(8)));
    assert!(unsigned_values.contains(&AggregateDistinctValue::UInt64(9)));
    assert!(!unsigned_values.contains(&AggregateDistinctValue::UInt64(123)));

    let mut signed_values = AggregateDistinctSet::default();
    let dense_signed_used = aggregate_direct_count_distinct_update_dense_i64(
        &[-2, -1, -2, 0, 17],
        Some(&[false, false, false, false, true]),
        None,
        &mut signed_values,
    )
    .expect("dense signed distinct");
    assert!(dense_signed_used);
    assert_eq!(signed_values.len(), 3);
    assert!(signed_values.contains(&AggregateDistinctValue::Int64(-2)));
    assert!(signed_values.contains(&AggregateDistinctValue::Int64(-1)));
    assert!(signed_values.contains(&AggregateDistinctValue::Int64(0)));
    assert!(!signed_values.contains(&AggregateDistinctValue::Int64(17)));

    let mut wide_values = AggregateDistinctSet::default();
    let dense_wide_used = aggregate_direct_count_distinct_update_dense_u64(
        &[0, AGGREGATE_DIRECT_DISTINCT_DENSE_RANGE_LIMIT + 1],
        None,
        None,
        &mut wide_values,
    )
    .expect("wide unsigned dense distinct attempt");
    assert!(!dense_wide_used);
    aggregate_direct_count_distinct_update_u64(
        &[0, AGGREGATE_DIRECT_DISTINCT_DENSE_RANGE_LIMIT + 1],
        None,
        None,
        &mut wide_values,
    )
    .expect("wide unsigned fallback distinct");
    assert_eq!(wide_values.len(), 2);
    assert!(wide_values.contains(&AggregateDistinctValue::UInt64(0)));
    assert!(wide_values.contains(&AggregateDistinctValue::UInt64(
        AGGREGATE_DIRECT_DISTINCT_DENSE_RANGE_LIMIT + 1
    )));
}

#[test]
fn partitioned_primitives_reserve_before_reading_and_preserve_scan_results() {
    let left = unique_vortex_path("partitioned-resource-left");
    let right = unique_vortex_path("partitioned-resource-right");
    std::fs::write(&left, b"not a Vortex footer").unwrap();
    std::fs::write(&right, b"not a Vortex footer").unwrap();
    let sources = vec![
        DatasetUri::new(left.display().to_string()).unwrap(),
        DatasetUri::new(right.display().to_string()).unwrap(),
    ];
    let uri = sources[0].clone();
    let predicate = PredicateExpr::Compare {
        column: ColumnRef::new("value").unwrap(),
        op: ComparisonOp::GtEq,
        value: StatValue::UInt64(3),
    };
    let projection = ProjectionRequest::columns(vec![ColumnRef::new("metric").unwrap()]);
    let requests = [
        VortexQueryPrimitiveRequest::count_all(uri.clone()),
        VortexQueryPrimitiveRequest::count_where(uri.clone(), predicate.clone()),
        VortexQueryPrimitiveRequest::filter(uri.clone(), predicate.clone()),
        VortexQueryPrimitiveRequest::project(uri.clone(), projection.clone()),
        VortexQueryPrimitiveRequest::filter_and_project(uri.clone(), predicate, projection.clone()),
        VortexQueryPrimitiveRequest::simple_aggregate(
            uri.clone(),
            VortexSimpleAggregateRequest::new(vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".into(),
            )]),
        ),
        VortexQueryPrimitiveRequest::sort_rows(
            uri,
            projection,
            None,
            VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("metric", true)]),
            3,
        ),
    ];
    let denied = VortexLocalPrimitiveExecutionPolicy::from_resources(
        shardloom_core::ExecutionResources::from_bytes(
            1,
            1,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .unwrap(),
    )
    .unwrap();
    for request in &requests {
        let error =
            execute_vortex_local_partitioned_primitive_with_policy(request, &sources, denied)
                .expect_err("memory admission must precede malformed-footer inspection");
        assert!(
            error.to_string().contains("memory"),
            "{}: {error}",
            request.kind.as_str()
        );
        assert_eq!(std::fs::read(&left).unwrap(), b"not a Vortex footer");
        assert_eq!(std::fs::read(&right).unwrap(), b"not a Vortex footer");
    }
    std::fs::remove_file(&left).unwrap();
    std::fs::remove_file(&right).unwrap();
    write_struct_fixture(&left).unwrap();
    write_struct_fixture(&right).unwrap();
    let admitted = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
    for request in &requests {
        let report =
            execute_vortex_local_partitioned_primitive_with_policy(request, &sources, admitted)
                .unwrap();
        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_scanned, 10);
        assert!(!report.fallback_execution_allowed);
        match request.kind {
            VortexQueryPrimitiveKind::CountAll => assert_eq!(report.rows_selected, Some(10)),
            VortexQueryPrimitiveKind::CountWhere | VortexQueryPrimitiveKind::FilterPredicate => {
                assert_eq!(report.rows_selected, Some(6));
            }
            VortexQueryPrimitiveKind::ProjectColumns => {
                assert_eq!(report.rows_projected, Some(10));
            }
            VortexQueryPrimitiveKind::FilterAndProject => {
                assert_eq!(report.rows_selected, Some(6));
                assert_eq!(report.rows_projected, Some(6));
            }
            VortexQueryPrimitiveKind::SimpleAggregate => {
                assert_eq!(report.rows_projected, Some(1));
            }
            VortexQueryPrimitiveKind::SortRows => assert_eq!(report.rows_projected, Some(3)),
            _ => unreachable!(),
        }
    }
    std::fs::remove_file(left).unwrap();
    std::fs::remove_file(right).unwrap();
}

#[test]
fn partitioned_local_primitive_counts_sources_without_fallback() {
    use shardloom_core::native_io_decisions::{NativeIoCapability, NativeIoDecisionReason};
    let left_path = unique_vortex_path("partitioned-count-left");
    let right_path = unique_vortex_path("partitioned-count-right");
    write_struct_fixture(&left_path).expect("left fixture");
    write_struct_fixture(&right_path).expect("right fixture");
    let source_uris = vec![
        DatasetUri::new(left_path.display().to_string()).expect("left uri"),
        DatasetUri::new(right_path.display().to_string()).expect("right uri"),
    ];
    let request = VortexQueryPrimitiveRequest::count_all(
        DatasetUri::new("placeholder.vortex").expect("request uri"),
    );

    let report = execute_vortex_local_partitioned_primitive_with_policy(
        &request,
        &source_uris,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&left_path);
    let _ = std::fs::remove_file(&right_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::CountAll);
    assert_eq!(report.rows_scanned, 10);
    assert_eq!(report.rows_selected, Some(10));
    assert_eq!(report.reader_splits.len(), 0);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert!(!report.streaming_scan_used);
    assert!(!report.upstream_scan_called);
    assert!(!report.data_read);
    assert!(!report.data_decoded);
    assert!(!report.data_materialized);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let certificate =
        local_primitive_native_io_certificate(&request, &report).expect("certificate");
    assert!(
        certificate.is_certified(),
        "partitioned metadata count should certify as native Vortex metadata I/O without a scan"
    );
    let row = certificate
        .capability_decisions
        .get(NativeIoCapability::MetadataCount)
        .unwrap();
    assert_eq!(
        (row.considered, row.admitted, row.executed, row.declined),
        (2, 2, 2, 0)
    );
    assert_eq!(row.observed.input_rows, Some(10));
    assert!(row.has_admission_reason(NativeIoDecisionReason::ExactHeldMetadata));
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("partitioned_vortex_sources=2"));
    assert!(summary.contains("partitioned_execution_strategy=sequential_capillary_parts"));
    assert!(summary.ends_with(";10"));
}

#[test]
#[allow(clippy::too_many_lines)]
fn partitioned_local_primitive_aggregate_uses_one_state_without_fallback() {
    let left_path = unique_vortex_path("partitioned-aggregate-left");
    let right_path = unique_vortex_path("partitioned-aggregate-right");
    write_struct_fixture(&left_path).expect("left fixture");
    write_struct_fixture(&right_path).expect("right fixture");
    let source_uris = vec![
        DatasetUri::new(left_path.display().to_string()).expect("left uri"),
        DatasetUri::new(right_path.display().to_string()).expect("right uri"),
    ];
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("placeholder.vortex").expect("request uri"),
        VortexSimpleAggregateRequest::new(vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("metric").expect("column")),
                "sum_metric".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("metric").expect("column")),
                "avg_metric".to_string(),
            ),
        ]),
    );

    let report = execute_vortex_local_partitioned_primitive_with_policy(
        &request,
        &source_uris,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&left_path);
    let _ = std::fs::remove_file(&right_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 10);
    assert_eq!(report.rows_selected, Some(10));
    assert_eq!(report.rows_projected, Some(1));
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.state_budget.state_budget_required);
    assert!(
        report
            .state_budget
            .state_family
            .starts_with("scalar_aggregate_state+direct_dictionary_or_typed"),
        "{}",
        report.state_budget.state_family
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"partitioned_vortex_source".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"partition_count".to_string())
    );
    assert!(report.embedded_layout.metadata_persisted_in_artifact);
    assert_eq!(
        report.embedded_layout.status,
        "partitioned_footer_inventory_available"
    );
    assert_eq!(report.embedded_layout.footer_row_count, 10);
    assert!(report.embedded_layout.footer_segment_count > 0);
    assert!(
        report
            .embedded_layout
            .root_layout_encoding
            .starts_with("partitioned_root_layouts=")
    );
    assert!(
        report
            .embedded_layout
            .layout_encoding_inventory
            .starts_with("partitioned_layout_encodings=")
    );
    assert!(
        report
            .embedded_layout
            .segment_membership_status
            .starts_with("partitioned_segment_maps_available")
    );
    assert_eq!(
        report.embedded_layout.layout_reader_cache_status,
        "enabled_for_partitioned_local_open"
    );
    assert_eq!(
        report.embedded_layout.selected_segment_count,
        report.embedded_layout.footer_segment_count
    );
    assert_eq!(report.embedded_layout.skipped_segment_count, 0);
    assert_eq!(
        report.embedded_layout.planner_consumption_status,
        "partitioned_footer_inventory_available"
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("partitioned_vortex_sources=2"));
    assert!(summary.contains("\"rows\":10"));
    assert!(summary.contains("\"sum_metric\":300.0"));
    assert!(summary.contains("\"avg_metric\":30.0"));
}

#[test]
fn partitioned_grouped_aggregate_reuses_numeric_pair_compact_state() {
    let left_path = unique_vortex_path("partitioned-numeric-pair-left");
    let right_path = unique_vortex_path("partitioned-numeric-pair-right");
    write_numeric_pair_aggregate_fixture(&left_path).expect("left fixture");
    write_numeric_pair_aggregate_fixture(&right_path).expect("right fixture");
    let source_uris = vec![
        DatasetUri::new(left_path.display().to_string()).expect("left uri"),
        DatasetUri::new(right_path.display().to_string()).expect("right uri"),
    ];
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("placeholder.vortex").expect("request uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![
                ColumnRef::new("watch_id").expect("watch id"),
                ColumnRef::new("client_ip").expect("client ip"),
            ],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("width").expect("width")),
                    "sum_width".to_string(),
                ),
            ],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]),
    )
    .with_source_order_limit(3);

    let report = execute_vortex_local_partitioned_primitive_with_policy(
        &request,
        &source_uris,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&left_path);
    let _ = std::fs::remove_file(&right_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 12);
    assert_eq!(report.rows_selected, Some(12));
    assert_eq!(report.rows_projected, Some(3));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(
        report.state_budget.state_family.contains("numeric_pair"),
        "{}",
        report.state_budget.state_family
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"typed_numeric_pair_group_state".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"numeric_pair_group_cardinality".to_string())
    );
    assert!(report.embedded_layout.metadata_persisted_in_artifact);
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("partitioned_vortex_sources=2"));
    assert!(summary.contains(
        "\"compact_group_state_strategy\":\"numeric_pair_compact_count_sum_avg_group_state\""
    ));
    assert!(summary.contains("\"watch_id\":10"));
    assert!(summary.contains("\"client_ip\":1"));
    assert!(summary.contains("\"rows\":4"));
}

#[test]
fn partitioned_local_primitive_sort_rows_merges_global_topn_without_fallback() {
    let left_path = unique_vortex_path("partitioned-sort-left");
    let right_path = unique_vortex_path("partitioned-sort-right");
    write_struct_fixture(&left_path).expect("left fixture");
    write_struct_fixture(&right_path).expect("right fixture");
    let source_uris = vec![
        DatasetUri::new(left_path.display().to_string()).expect("left uri"),
        DatasetUri::new(right_path.display().to_string()).expect("right uri"),
    ];
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new("placeholder.vortex").expect("request uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("metric").expect("column"),
            ColumnRef::new("value").expect("column"),
        ]),
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("metric", true)]),
        3,
    );

    let report = execute_vortex_local_partitioned_primitive_with_policy(
        &request,
        &source_uris,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&left_path);
    let _ = std::fs::remove_file(&right_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert_eq!(report.rows_scanned, 10);
    assert_eq!(report.rows_selected, Some(3));
    assert_eq!(report.rows_projected, Some(3));
    assert_eq!(report.source_order_limit_requested, Some(3));
    assert!(report.source_order_limit_applied);
    assert!(report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.state_budget.state_budget_required);
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"partitioned_vortex_source".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"topk_heap_state".to_string())
    );
    assert!(report.embedded_layout.metadata_persisted_in_artifact);
    assert_eq!(
        report.embedded_layout.status,
        "partitioned_footer_inventory_available"
    );
    assert_eq!(report.embedded_layout.footer_row_count, 10);
    assert!(report.embedded_layout.footer_segment_count > 0);
    assert!(
        report
            .embedded_layout
            .root_layout_encoding
            .starts_with("partitioned_root_layouts=")
    );
    assert!(
        report
            .embedded_layout
            .layout_encoding_inventory
            .starts_with("partitioned_layout_encodings=")
    );
    assert!(
        report
            .embedded_layout
            .segment_membership_status
            .starts_with("partitioned_segment_maps_available")
    );
    assert_eq!(
        report.embedded_layout.layout_reader_cache_status,
        "enabled_for_partitioned_local_open"
    );
    assert_eq!(
        report.embedded_layout.selected_segment_count,
        report.embedded_layout.footer_segment_count
    );
    assert_eq!(report.embedded_layout.skipped_segment_count, 0);
    assert_eq!(
        report.embedded_layout.planner_consumption_status,
        "partitioned_footer_inventory_available"
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("partitioned_vortex_sources=2"));
    assert!(summary.contains("\"rows\":3"));
    assert_eq!(summary.matches("\"metric\":50").count(), 2);
    assert!(summary.contains("\"metric\":40"));
}

#[test]
fn partitioned_sort_rows_wide_output_materializes_selected_partition_ordinals() {
    let left_path = unique_vortex_path("partitioned-wide-sort-left");
    let right_path = unique_vortex_path("partitioned-wide-sort-right");
    write_wide_sort_fixture(&left_path).expect("left fixture");
    write_wide_sort_fixture(&right_path).expect("right fixture");
    let source_uris = vec![
        DatasetUri::new(left_path.display().to_string()).expect("left uri"),
        DatasetUri::new(right_path.display().to_string()).expect("right uri"),
    ];
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new("placeholder.vortex").expect("request uri"),
        ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("metric", true)]),
        1,
    );

    let report = execute_vortex_local_partitioned_primitive_with_policy(
        &request,
        &source_uris,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&left_path);
    let _ = std::fs::remove_file(&right_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert_eq!(report.rows_scanned, 6);
    assert_eq!(report.rows_selected, Some(1));
    assert_eq!(report.rows_projected, Some(1));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"wide_output_second_pass\":true"));
    assert!(summary.contains("\"late_materialization_selected_row_refs_used\":true"));
    assert!(summary.contains("\"id\":2"));
    assert!(summary.contains("\"metric\":30"));
    assert!(summary.contains("\"c7\":207"));
}

#[test]
fn simple_aggregate_applies_sum_argument_offsets_without_fallback() {
    let path = unique_vortex_path("simple-aggregate-offset");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::new(vec![
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("metric").expect("column")),
                "sum_metric_plus_2".to_string(),
            )
            .with_argument_offset(2),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("metric").expect("column")),
                "sum_metric_minus_1".to_string(),
            )
            .with_argument_offset(-1),
        ]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_selected, Some(5));
    assert_eq!(report.rows_projected, Some(1));
    assert!(report.upstream_scan_called);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert_eq!(
        report.state_budget.state_family,
        "scalar_aggregate_state+direct_dictionary_or_typed+expression_fusion"
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"expression_plan_fingerprint_reuse".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"repeated_measure_evaluation_bypass".to_string())
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"expression_fusion_strategy\":\"numeric_additive_column_fusion\""));
    assert!(
        summary.contains("\"expression_plan_fingerprint_status\":\"shared_column_measure_update\"")
    );
    assert!(summary.contains("\"sum_metric_plus_2\":160.0"));
    assert!(summary.contains("\"sum_metric_minus_1\":145.0"));
}

#[test]
fn scalar_string_length_aggregate_reuses_dictionary_value_counts() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let request = VortexSimpleAggregateRequest::new(vec![
        crate::VortexSimpleAggregateMeasure::new(
            "count",
            Some(ColumnRef::new("URL").expect("url")),
            "count_url".to_string(),
        ),
        crate::VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("URL").expect("url")),
            "sum_len".to_string(),
        )
        .with_value_transform("length"),
        crate::VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("URL").expect("url")),
            "avg_len".to_string(),
        )
        .with_value_transform("length"),
        crate::VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new("URL").expect("url")),
            "min_len".to_string(),
        )
        .with_value_transform("length"),
        crate::VortexSimpleAggregateMeasure::new(
            "max",
            Some(ColumnRef::new("URL").expect("url")),
            "max_len".to_string(),
        )
        .with_value_transform("length"),
    ]);
    let declared_columns = vec!["URL".to_string()];
    let mut states = SimpleAggregateStates::new(&request, &declared_columns).expect("states");
    let codes = [0_u8, 0, 1, 2]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = VarBinViewArray::from_iter_str(["aa", "bbbb", "z"]).into_array();
    let urls = DictArray::try_new(codes, values)
        .expect("URL dictionary")
        .into_array();
    let chunk = StructArray::try_new(
        FieldNames::from(["URL"]),
        vec![urls],
        4,
        Validity::NonNullable,
    )
    .expect("struct")
    .into_array();

    assert!(
        states
            .update_direct_from_chunk(
                &chunk,
                &declared_columns,
                None,
                &mut NativeNumericAccessorWork::default(),
                &mut native_numeric_execution_ctx(&vortex::session::VortexSession::empty()),
            )
            .expect("dictionary weighted update")
    );

    let budget = states
        .state_budget_report(&request, 4, 1)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "scalar_aggregate_state+direct_dictionary_or_typed+expression_fusion"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_weighted_transform_fusion".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_weighted_utf8_minmax_allocation_bypass".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"repeated_string_transform_evaluation_bypass".to_string())
    );
    let payload: serde_json::Value = serde_json::from_str(
        &states
            .result_summary(&[])
            .expect("scalar aggregate summary"),
    )
    .expect("scalar aggregate summary json");
    assert_eq!(
        payload["expression_fusion_strategy"],
        "dictionary_weighted_transform_fusion"
    );
    assert_eq!(
        payload["expression_plan_fingerprint_status"],
        "shared_dictionary_value_transform_update"
    );
    assert_eq!(payload["values"]["count_url"], serde_json::json!(4));
    assert_eq!(payload["values"]["sum_len"], serde_json::json!(9.0));
    assert_eq!(payload["values"]["avg_len"], serde_json::json!(2.25));
    assert_eq!(payload["values"]["min_len"], serde_json::json!(1));
    assert_eq!(payload["values"]["max_len"], serde_json::json!(4));
}

#[test]
fn grouped_aggregate_accumulates_group_rows_without_fallback() {
    let path = unique_vortex_path("grouped-aggregate");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("amount").expect("column")),
                    "total_amount".to_string(),
                ),
            ],
        ),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(3));
    assert_eq!(report.rows_projected, Some(2));
    assert!(report.upstream_scan_called);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.state_budget.state_budget_required);
    assert!(
        report
            .state_budget
            .state_family
            .contains("grouped_aggregate_state")
    );
    assert!(
        report
            .state_budget
            .state_family
            .contains("compact_numeric_measures")
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"direct_count_sum_avg_group_update".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"group_cardinality".to_string())
    );
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_compact_count_sum_avg_group_state"
    );
    assert_eq!(
        payload["group_key_storage"],
        "typed_single_key+interned_utf8"
    );
    assert_eq!(
        payload["source_order_key_retention"],
        "source_order_keys_retained"
    );
    assert_eq!(
        payload["group_state_mode"],
        "chunk_dictionary_compact_measure_code_map"
    );
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    assert!(summary.contains("\"group_by\":\"label\""));
    assert!(summary.contains("\"label\":\"paid\""));
    assert!(summary.contains("\"label\":\"trial\""));
    assert!(summary.contains("\"total_amount\":15.0"));
    assert!(summary.contains("\"total_amount\":7.0"));
}

#[test]
fn grouped_aggregate_skips_all_null_group_measures_without_fallback() {
    let path = unique_vortex_path("grouped-aggregate-all-null-measures");
    write_all_null_aggregate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "count",
                    Some(ColumnRef::new("amount").expect("column")),
                    "non_null_amount".to_string(),
                ),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("amount").expect("column")),
                    "sum_amount".to_string(),
                ),
                crate::VortexSimpleAggregateMeasure::new(
                    "avg",
                    Some(ColumnRef::new("amount").expect("column")),
                    "avg_amount".to_string(),
                ),
            ],
        ),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(3));
    assert_eq!(report.rows_projected, Some(2));
    assert!(
        report
            .state_budget
            .state_family
            .contains("grouped_aggregate_state")
    );
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    let values = payload["values"].as_array().expect("group rows");
    assert_eq!(payload["rows"], serde_json::json!(2));
    assert_eq!(values.len(), 2);
    let paid = values
        .iter()
        .find(|row| row["label"] == serde_json::json!("paid"))
        .expect("paid group");
    let trial = values
        .iter()
        .find(|row| row["label"] == serde_json::json!("trial"))
        .expect("trial group");
    assert_eq!(paid["rows"], serde_json::json!(2));
    assert_eq!(paid["non_null_amount"], serde_json::json!(0));
    assert!(paid["sum_amount"].is_null());
    assert!(paid["avg_amount"].is_null());
    assert_eq!(trial["rows"], serde_json::json!(1));
    assert_eq!(trial["non_null_amount"], serde_json::json!(0));
    assert!(trial["sum_amount"].is_null());
    assert!(trial["avg_amount"].is_null());
}

#[test]
fn grouped_aggregate_filters_orders_and_limits_rows_without_fallback() {
    let path = unique_vortex_path("grouped-aggregate-filtered-topk");
    write_pivot_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("amount").expect("column")),
                    "total_amount".to_string(),
                ),
            ],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new(
            "total_amount",
            true,
        )]),
    )
    .with_source_order_limit(1);
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("amount").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(7),
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(1));
    assert_eq!(report.source_order_limit_requested, Some(1));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_input_rows, Some(2));
    assert_eq!(report.source_order_limit_rows_output, Some(1));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(report.upstream_scan_called);
    assert!(report.data_read);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.state_budget.state_budget_required);
    assert_eq!(
        report.state_budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+chunk_dictionary_compact_measures+compact_numeric_measures"
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"grouped_topk_heap".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"capillary_ordered_candidate_selection".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"topk_heap_rows".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"direct_count_sum_avg_group_update".to_string())
    );
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    assert_eq!(payload["group_output_strategy"], "capillary_ordered_topk");
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_compact_count_sum_avg_group_state"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(2));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(1));
    assert!(summary.contains("\"rows\":1"));
    assert!(summary.contains("\"label\":\"paid\""));
    assert!(summary.contains("\"total_amount\":10.0"));
    assert!(!summary.contains("\"label\":\"trial\""));
}

#[test]
fn grouped_aggregate_numeric_pair_uses_streaming_topk_compact_state() {
    let path = unique_vortex_path("grouped-aggregate-numeric-pair-topk");
    write_numeric_pair_aggregate_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![
                ColumnRef::new("watch_id").expect("column"),
                ColumnRef::new("client_ip").expect("column"),
            ],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("is_refresh").expect("column")),
                    "refreshes".to_string(),
                ),
                crate::VortexSimpleAggregateMeasure::new(
                    "avg",
                    Some(ColumnRef::new("width").expect("column")),
                    "avg_width".to_string(),
                ),
            ],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]),
    )
    .with_source_order_limit(1);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 6);
    assert_eq!(report.rows_selected, Some(6));
    assert_eq!(report.rows_projected, Some(1));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert_eq!(
        report.state_budget.state_family,
        "grouped_aggregate_state+topk+compact_numeric_measures+numeric_pair+direct_key_slices"
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"typed_numeric_pair_group_state".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"streaming_numeric_pair_topk_retention".to_string())
    );
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_streaming_ordered_topk"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "numeric_pair_compact_count_sum_avg_group_state"
    );
    assert_eq!(payload["group_state_mode"], "numeric_pair_hot_hash_map");
    assert_eq!(payload["group_key_storage"], "typed_numeric_pair_key");
    assert_eq!(
        payload["group_key_comparison_strategy"],
        "typed_numeric_pair_count_topk"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(4));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(1));
    assert_eq!(payload["values"][0]["watch_id"], serde_json::json!(10));
    assert_eq!(payload["values"][0]["client_ip"], serde_json::json!(1));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(2));
    assert_eq!(payload["values"][0]["refreshes"], serde_json::json!(1.0));
    assert_eq!(payload["values"][0]["avg_width"], serde_json::json!(150.0));
}

#[test]
fn grouped_aggregate_numeric_pair_large_retained_window_uses_select_nth_topk() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("watch_id").expect("column"),
            ColumnRef::new("client_ip").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["watch_id".to_string(), "client_ip".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(129), &declared_columns, true, false)
            .expect("states");
    let first_values = (0_u64..160).collect::<Vec<_>>();
    let second_values = vec![1_u64; first_values.len()];
    let direct_accessors = vec![
        AggregateDirectColumnAccessor::UInt64(first_values),
        AggregateDirectColumnAccessor::UInt64(second_values),
    ];

    assert!(
        states
            .update_numeric_pair_compact_direct_from_accessors(&direct_accessors, None)
            .expect("numeric-pair direct update")
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(129))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");

    assert_eq!(row_count, 129);
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_select_nth_numeric_pair_ordered_topk"
    );
    assert_eq!(
        payload["group_key_comparison_strategy"],
        "typed_numeric_pair_count_select_nth_topk"
    );
    assert_eq!(
        payload["topk_retention_strategy"],
        "select_nth_numeric_pair_retained_window"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(160));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(129));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows.len(), 129);
    let retained_watch_ids = rows
        .iter()
        .map(|row| {
            assert_eq!(row["rows"], serde_json::json!(1));
            row["watch_id"].as_u64().expect("watch id")
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(retained_watch_ids.len(), 129);
    assert!(retained_watch_ids.iter().all(|id| *id < 160));
}

#[test]
fn grouped_aggregate_numeric_pair_late_measure_uses_count_topk_second_pass() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("watch_id").expect("column"),
            ColumnRef::new("client_ip").expect("column"),
        ],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("is_refresh").expect("column")),
                "refreshes".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("width").expect("column")),
                "avg_width".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec![
        "watch_id".to_string(),
        "client_ip".to_string(),
        "is_refresh".to_string(),
        "width".to_string(),
    ];
    let mut states = GroupedAggregateStates::new(&request, Some(1), &declared_columns, true, false)
        .expect("states");
    let direct_accessors = vec![
        AggregateDirectColumnAccessor::UInt64(vec![10, 20, 10, 30, 10, 20]),
        AggregateDirectColumnAccessor::UInt64(vec![1, 2, 1, 3, 1, 2]),
        AggregateDirectColumnAccessor::UInt64(vec![1, 0, 1, 0, 0, 1]),
        AggregateDirectColumnAccessor::UInt64(vec![100, 200, 300, 400, 500, 600]),
    ];

    assert!(
        states
            .update_numeric_pair_late_measure_count_direct_from_accessors(&direct_accessors, None)
            .expect("late measure count pass")
    );
    assert!(states.needs_numeric_pair_late_measure_second_pass());
    states
        .prepare_numeric_pair_late_measure_second_pass(Some(1))
        .expect("prepare retained keys");
    assert!(
        states.numeric_pair_late_measure_count_groups.is_none(),
        "count state should be released before the second measure scan"
    );
    assert_eq!(
        states.numeric_pair_late_measure_candidate_group_count,
        Some(3)
    );
    assert!(
        states
            .update_numeric_pair_late_measure_direct_from_accessors(&direct_accessors)
            .expect("late measure second pass")
    );
    let budget = states
        .state_budget_report(&request, 6, 1)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+compact_numeric_measures+numeric_pair_late_measure+direct_key_slices"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_pair_count_pass_group_state".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_pair_topk_retained_measure_second_pass".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"cached_worst_numeric_pair_topk_retention".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_pair_direct_key_slice_update".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_pair_prepared_retained_candidate_reuse".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_pair_count_state_release_before_second_pass".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"late_measure_second_scan".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"numeric_pair_key_accessor_match_bypass".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"numeric_pair_retained_candidate_rescan_bypass".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"numeric_pair_count_state_memory_released_before_measure_scan".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(1))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 1);
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_two_pass_numeric_pair_count_topk_late_measures"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "numeric_pair_count_topk_late_measure_second_pass"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "numeric_pair_count_topk_late_measure_group_state"
    );
    assert_eq!(
        payload["retained_candidate_selection_source"],
        "prepared_first_pass_retained_candidates"
    );
    assert_eq!(
        payload["topk_retention_strategy"],
        "cached_worst_numeric_pair_retained_window"
    );
    assert_eq!(payload["retained_candidate_rescan_bypassed"], true);
    assert_eq!(
        payload["numeric_pair_late_measure_count_state_released_before_second_pass"],
        true
    );
    assert_eq!(
        payload["group_state_mode"],
        "numeric_pair_count_state_then_retained_measure_state"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(3));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(1));
    assert_eq!(
        payload["numeric_pair_late_measure_candidate_groups"],
        serde_json::json!(3)
    );
    assert_eq!(
        payload["numeric_pair_late_measure_retained_keys"],
        serde_json::json!(1)
    );
    assert_eq!(
        payload["numeric_pair_late_measure_second_pass"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_pair_chunk_compacted_updates"],
        serde_json::json!(false)
    );
    assert_eq!(payload["values"][0]["watch_id"], serde_json::json!(10));
    assert_eq!(payload["values"][0]["client_ip"], serde_json::json!(1));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(3));
    assert_eq!(payload["values"][0]["refreshes"], serde_json::json!(2.0));
    assert_eq!(payload["values"][0]["avg_width"], serde_json::json!(300.0));
}

#[test]
fn grouped_aggregate_numeric_pair_late_measure_uses_near_unique_directory() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("watch_id").expect("column"),
            ColumnRef::new("client_ip").expect("column"),
        ],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("is_refresh").expect("column")),
                "refreshes".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("width").expect("column")),
                "avg_width".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec![
        "watch_id".to_string(),
        "client_ip".to_string(),
        "is_refresh".to_string(),
        "width".to_string(),
    ];
    let mut states = GroupedAggregateStates::new(&request, Some(3), &declared_columns, true, false)
        .expect("states");
    let rows = 20_000_usize;
    let rows_i64 = i64::try_from(rows).expect("rows fit i64");
    let mut watch_ids = (0..rows_i64).collect::<Vec<_>>();
    let mut client_ips = (0..rows_i64).collect::<Vec<_>>();
    watch_ids[100] = 42;
    client_ips[100] = 42;
    let mut is_refresh = vec![0_i64; rows];
    is_refresh[42] = 1;
    is_refresh[100] = 1;
    let mut widths = vec![100_i64; rows];
    widths[100] = 300;
    let direct_accessors = vec![
        AggregateDirectColumnAccessor::Int64(watch_ids),
        AggregateDirectColumnAccessor::Int64(client_ips),
        AggregateDirectColumnAccessor::Int64(is_refresh),
        AggregateDirectColumnAccessor::Int64(widths),
    ];

    assert!(
        states
            .update_numeric_pair_late_measure_count_direct_from_accessors(&direct_accessors, None)
            .expect("late measure near-unique count pass")
    );
    assert!(states.numeric_pair_late_measure_near_unique_directory_updates);
    assert!(states.numeric_pair_late_measure_count_groups.is_none());
    assert!(
        states
            .numeric_pair_late_measure_near_unique_directory
            .is_some()
    );
    assert_eq!(
        states.numeric_pair_late_measure_near_unique_rows,
        rows as u64
    );
    assert_eq!(
        states.numeric_pair_late_measure_near_unique_seen_keys,
        rows as u64 - 1
    );
    assert_eq!(
        states.numeric_pair_late_measure_near_unique_duplicate_keys,
        1
    );
    assert!(states.needs_numeric_pair_late_measure_second_pass());
    states
        .prepare_numeric_pair_late_measure_second_pass(Some(3))
        .expect("prepare retained keys");
    assert!(
        states
            .numeric_pair_late_measure_near_unique_directory
            .is_none()
    );
    assert_eq!(
        states.numeric_pair_late_measure_candidate_group_count,
        Some(rows - 1)
    );
    assert!(
        states
            .update_numeric_pair_late_measure_direct_from_accessors(&direct_accessors)
            .expect("late measure second pass")
    );
    let budget = states
        .state_budget_report(&request, rows, 3)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+compact_numeric_measures+numeric_pair_late_measure+near_unique_directory+direct_key_slices"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_pair_near_unique_count_directory".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_pair_exact_duplicate_promotion_directory".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"numeric_pair_near_unique_duplicate_keys".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(3))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 3);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "numeric_pair_near_unique_directory_count_topk_late_measure_second_pass"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "numeric_pair_near_unique_count_topk_late_measure_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "numeric_pair_near_unique_count_directory_then_retained_measure_state"
    );
    assert_eq!(
        payload["group_key_storage"],
        "typed_numeric_pair_near_unique_seen_key_directory"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(rows - 1));
    assert_eq!(
        payload["numeric_pair_late_measure_near_unique_directory_updates"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_pair_late_measure_near_unique_rows"],
        serde_json::json!(rows)
    );
    assert_eq!(
        payload["numeric_pair_late_measure_near_unique_seen_keys"],
        serde_json::json!(rows - 1)
    );
    assert_eq!(
        payload["numeric_pair_late_measure_near_unique_duplicate_keys"],
        serde_json::json!(1)
    );
    assert_eq!(
        payload["numeric_pair_late_measure_near_unique_directory_released_before_second_pass"],
        serde_json::json!(true)
    );
    assert_eq!(payload["values"][0]["watch_id"], serde_json::json!(42));
    assert_eq!(payload["values"][0]["client_ip"], serde_json::json!(42));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(2));
    assert_eq!(payload["values"][0]["refreshes"], serde_json::json!(2.0));
    assert_eq!(payload["values"][0]["avg_width"], serde_json::json!(200.0));
    assert_eq!(payload["values"][1]["watch_id"], serde_json::json!(0));
    assert_eq!(payload["values"][1]["client_ip"], serde_json::json!(0));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(1));
    assert_eq!(payload["values"][2]["watch_id"], serde_json::json!(1));
    assert_eq!(payload["values"][2]["client_ip"], serde_json::json!(1));
    assert_eq!(payload["values"][2]["rows"], serde_json::json!(1));
}

#[test]
fn grouped_aggregate_numeric_pair_late_measure_compacts_repeated_chunks() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("watch_id").expect("column"),
            ColumnRef::new("client_ip").expect("column"),
        ],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("is_refresh").expect("column")),
                "refreshes".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec![
        "watch_id".to_string(),
        "client_ip".to_string(),
        "is_refresh".to_string(),
    ];
    let mut states = GroupedAggregateStates::new(&request, Some(1), &declared_columns, true, false)
        .expect("states");
    let rows = 20_000_usize;
    let direct_accessors = vec![
        AggregateDirectColumnAccessor::UInt64((0..rows).map(|index| (index % 4) as u64).collect()),
        AggregateDirectColumnAccessor::UInt64((0..rows).map(|index| (index % 2) as u64).collect()),
        AggregateDirectColumnAccessor::UInt64(vec![1; rows]),
    ];

    assert!(
        states
            .update_numeric_pair_late_measure_count_direct_from_accessors(&direct_accessors, None,)
            .expect("numeric-pair count pass")
    );
    assert!(states.numeric_pair_chunk_compacted_updates);
    assert_eq!(states.numeric_pair_chunk_compacted_input_rows, rows as u64);
    assert_eq!(states.numeric_pair_chunk_compacted_unique_keys, 4);
    states
        .prepare_numeric_pair_late_measure_second_pass(Some(1))
        .expect("prepare second pass");
    assert!(
        states
            .update_numeric_pair_late_measure_direct_from_accessors(&direct_accessors)
            .expect("measure pass")
    );
    let budget = states
        .state_budget_report(&request, rows, 1)
        .expect("state budget");
    assert!(budget.state_family.contains("chunk_compacted_partials"));
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_pair_chunk_compacted_count_partial".to_string())
    );
    let (_row_count, summary) = states
        .result_row_count_and_summary(Some(1))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(
        payload["numeric_pair_chunk_compacted_updates"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_pair_chunk_compacted_input_rows"],
        serde_json::json!(rows)
    );
    assert_eq!(
        payload["numeric_pair_chunk_compacted_unique_keys"],
        serde_json::json!(4)
    );
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(5000));
}

#[test]
fn numeric_pair_chunk_compaction_rejects_near_unique_samples() {
    let rows = 20_000_usize;
    let first = AggregateDirectColumnAccessor::UInt64((0..rows as u64).collect());
    let second = AggregateDirectColumnAccessor::UInt64((0..rows as u64).rev().collect());
    let (first_keys, second_keys) =
        aggregate_numeric_pair_direct_key_slices(&first, &second).expect("key slices");

    assert!(
        !numeric_pair_chunk_compaction_admitted(first_keys, second_keys)
            .expect("near-unique admission check")
    );
}

#[test]
fn grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["url".to_string()];
    let mut states = GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, true)
        .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 0, 1, 1, 2],
        values: vec![
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("warm"),
            std::sync::Arc::<str>::from("cold"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter count pass")
    );
    assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
    states
        .prepare_string_count_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_string_count_topk_heavy_hitter_exact_from_accessors(&accessors, None)
            .expect("exact recount")
    );
    assert!(
        states
            .string_count_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 6, 2)
        .expect("state budget");
    assert!(
        budget
            .state_family
            .contains("proofbound_string_heavy_hitter")
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_exact_recount".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_id_prefilter".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_signature_prefilter".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_code_bound_string_topk_key".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_dictionary_histogram_recount".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_candidate_id_prefilter_hits".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_candidate_signature_prefilter_hits".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"dictionary_code_string_key_reuse".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_topk_late_recount"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "string_dictionary_code_count_topk_heavy_hitter_late_recount"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "string_dictionary_code_heavy_hitter_candidate_count_star_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "string_dictionary_code_candidate_state_then_exact_recount"
    );
    assert_eq!(
        payload["string_count_topk_heavy_hitter_exact_proof"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_candidate_id_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_candidate_signature_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_candidate_code_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_dictionary_histogram_recount"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_exact_mirror_capacity"],
        serde_json::json!(
            states.string_count_topk_heavy_hitter_capacity()
                * STRING_COUNT_TOPK_EXACT_MIRROR_CAPACITY_MULTIPLIER
        )
    );
    assert_eq!(
        payload["string_count_topk_dictionary_code_reuse"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_candidate_clone_strategy"],
        "clone_only_surviving_retained_candidates"
    );
    assert_eq!(
        payload["topk_retention_strategy"],
        "cached_worst_string_count_retained_window"
    );
    assert_eq!(payload["values"][0]["url"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(3));
    assert_eq!(payload["values"][1]["url"], serde_json::json!("warm"));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(2));
}

#[test]
fn string_count_topk_first_pass_exact_histogram_admission_is_shape_and_budget_bound() {
    let uri = DatasetUri::new("file:///tmp/string-count-topk-exact-histogram.vortex").expect("uri");
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let primitive_request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), aggregate.clone())
            .with_source_order_limit(10);
    let production_envelope = VortexLocalPrimitiveResourceEnvelope::new(24, 12).expect("envelope");

    assert!(string_count_topk_first_pass_exact_histogram_route_enabled(
        100_000_000,
        &aggregate,
        &primitive_request,
        true,
        production_envelope,
    ));
    assert!(!string_count_topk_first_pass_exact_histogram_route_enabled(
        100_000_000,
        &aggregate,
        &primitive_request,
        false,
        production_envelope,
    ));
    assert!(!string_count_topk_first_pass_exact_histogram_route_enabled(
        999_999,
        &aggregate,
        &primitive_request,
        true,
        production_envelope,
    ));
    assert!(!string_count_topk_first_pass_exact_histogram_route_enabled(
        100_000_000,
        &aggregate,
        &primitive_request,
        true,
        VortexLocalPrimitiveResourceEnvelope::new(8, 12).expect("low-memory envelope"),
    ));

    let late_measure_aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("title").expect("column")),
                "first_title".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let late_measure_request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri, late_measure_aggregate.clone())
            .with_source_order_limit(10);
    assert!(!string_count_topk_first_pass_exact_histogram_route_enabled(
        100_000_000,
        &late_measure_aggregate,
        &late_measure_request,
        true,
        production_envelope,
    ));
}

#[test]
fn filtered_string_histogram_requires_complete_native_key_selection() {
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("renamed_key").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
    );
    for op in [
        ComparisonOp::Eq,
        ComparisonOp::NotEq,
        ComparisonOp::Lt,
        ComparisonOp::LtEq,
        ComparisonOp::Gt,
        ComparisonOp::GtEq,
    ] {
        let predicate = PredicateExpr::Compare {
            column: ColumnRef::new("renamed_key").unwrap(),
            op,
            value: StatValue::Utf8("東京".into()),
        };
        assert!(string_count_histogram_selected_input_admitted(
            &aggregate,
            Some(&predicate),
            None
        ));
        assert!(!string_count_histogram_selected_input_admitted(
            &aggregate,
            None,
            Some(&predicate)
        ));
    }
    for predicate in [
        PredicateExpr::Compare {
            column: ColumnRef::new("other").unwrap(),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("renamed_key").unwrap(),
            op: ComparisonOp::NotEq,
            value: StatValue::Null,
        },
        PredicateExpr::IsNull {
            column: ColumnRef::new("renamed_key").unwrap(),
        },
    ] {
        assert!(!string_count_histogram_selected_input_admitted(
            &aggregate,
            Some(&predicate),
            None
        ));
    }
}

#[test]
fn grouped_aggregate_string_count_topk_promotes_first_pass_exact_histogram() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["url".to_string()];
    let mut states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(2),
        &declared_columns,
        false,
        true,
        VortexLocalPrimitiveResourceEnvelope::new(24, 12).expect("envelope"),
        Some(&vortex::session::VortexSession::empty()),
    )
    .expect("states");
    states
        .enable_string_count_topk_first_pass_exact_histogram()
        .expect("enable histogram");
    let first_chunk = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 0, 1, 1, 2],
        values: vec![
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("warm"),
            std::sync::Arc::<str>::from("cold"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];
    let second_chunk = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![1, 0, 1, 2, 2, 2],
        values: vec![
            std::sync::Arc::<str>::from("warm"),
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("cold"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&first_chunk, None)
            .expect("first exact histogram chunk")
    );
    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&second_chunk, None)
            .expect("second exact histogram chunk")
    );
    assert!(states.string_count_topk_heavy_hitter_sketch.is_none());
    assert_eq!(states.string_count_topk_total_weight, 12);
    assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        states
            .promote_string_count_topk_exact_first_pass_if_possible(Some(2))
            .expect("promote exact histogram")
    );
    assert!(states.string_count_topk_heavy_hitter_sketch.is_none());
    assert!(!states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        states
            .string_count_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 12, 2)
        .expect("state budget");
    assert!(
        budget
            .state_family
            .contains("first_pass_dictionary_exact_histogram")
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_first_pass_dictionary_exact_histogram".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_exact_histogram_primary_update".to_string())
    );
    assert!(
        !budget
            .capillary_work_units
            .contains(&"string_heavy_hitter_sketch_update".to_string())
    );
    assert!(
        !budget
            .capillary_work_units
            .contains(&"string_topk_candidate_exact_recount".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_topk_second_scan_elided".to_string())
    );

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_topk_first_pass_exact"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "string_dictionary_code_count_topk_first_pass_exact_histogram"
    );
    assert_eq!(
        payload["group_state_mode"],
        "string_dictionary_code_first_pass_exact_histogram"
    );
    assert_eq!(
        payload["group_key_storage"],
        "exact_string_dictionary_histogram_groups"
    );
    assert_eq!(
        payload["uniqueness_proof_status"],
        "proofbound_heavy_hitter_exact_first_pass_dictionary_histogram"
    );
    assert_eq!(
        payload["string_count_topk_heavy_hitter_second_pass"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_topk_heavy_hitter_exact_counts_source"],
        serde_json::json!("first_pass_dictionary_exact_histogram")
    );
    assert_eq!(
        payload["string_count_topk_exact_mirror_capacity"],
        serde_json::json!(0)
    );
    assert_eq!(
        payload["string_count_topk_first_pass_exact_histogram_enabled"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_first_pass_exact_histogram_promoted"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_first_pass_exact_histogram_disabled"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_topk_first_pass_exact_histogram_entries"],
        serde_json::json!(3)
    );
    assert_eq!(
        payload["string_count_topk_first_pass_exact_histogram_input_rows"],
        serde_json::json!(12)
    );
    assert_eq!(
        payload["string_count_topk_first_pass_exact_histogram_input_values"],
        serde_json::json!(6)
    );
    assert_eq!(payload["values"][0]["url"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(5));
    assert_eq!(payload["values"][1]["url"], serde_json::json!("cold"));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(4));
}

#[test]
fn grouped_aggregate_string_count_topk_exact_histogram_disables_at_budget() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["url".to_string()];
    let mut envelope = VortexLocalPrimitiveResourceEnvelope::new(24, 12).expect("envelope");
    envelope.group_state_soft_item_budget = 1;
    let mut states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(1),
        &declared_columns,
        false,
        true,
        envelope,
        Some(&vortex::session::VortexSession::empty()),
    )
    .expect("states");
    states
        .enable_string_count_topk_first_pass_exact_histogram()
        .expect("enable histogram");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1],
        values: vec![
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("cold"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("budgeted exact histogram chunk")
    );
    assert!(states.string_count_topk_first_pass_exact_histogram_disabled);
    assert!(
        states
            .string_count_topk_first_pass_exact_histogram_counts
            .is_none()
    );
    assert!(states.string_count_topk_heavy_hitter_sketch.is_some());
    assert_eq!(states.string_count_topk_total_weight, 2);
    assert!(!states.promote_string_count_topk_first_pass_exact_histogram_if_possible());
}

#[test]
fn grouped_aggregate_string_count_topk_replays_exact_histogram_on_late_budget_overflow() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["url".to_string()];
    let mut envelope = VortexLocalPrimitiveResourceEnvelope::new(24, 12).expect("envelope");
    envelope.group_state_soft_item_budget = 2;
    let mut states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(2),
        &declared_columns,
        false,
        true,
        envelope,
        Some(&vortex::session::VortexSession::empty()),
    )
    .expect("states");
    states
        .enable_string_count_topk_first_pass_exact_histogram()
        .expect("enable histogram");
    let first_chunk = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 0, 1],
        values: vec![
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("warm"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];
    let second_chunk = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 1, 2, 2, 2, 3],
        values: vec![
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("warm"),
            std::sync::Arc::<str>::from("cold"),
            std::sync::Arc::<str>::from("cool"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&first_chunk, None)
            .expect("accepted exact histogram chunk")
    );
    assert!(states.string_count_topk_heavy_hitter_sketch.is_none());
    assert_eq!(states.string_count_topk_total_weight, 4);
    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&second_chunk, None)
            .expect("overflowing exact histogram chunk")
    );
    assert!(states.string_count_topk_first_pass_exact_histogram_disabled);
    assert!(
        states
            .string_count_topk_first_pass_exact_histogram_counts
            .is_none()
    );
    assert!(states.string_count_topk_heavy_hitter_sketch.is_some());
    assert_eq!(states.string_count_topk_total_weight, 10);
    assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        states
            .promote_string_count_topk_exact_first_pass_if_possible(Some(2))
            .expect("promote replayed first pass")
    );
    assert!(!states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        states
            .string_count_topk_exact_proved(Some(2))
            .expect("exact proof")
    );

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["string_count_topk_heavy_hitter_exact_counts_source"],
        serde_json::json!("first_pass_no_eviction")
    );
    assert_eq!(payload["values"][0]["url"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(4));
    assert_eq!(payload["values"][1]["url"], serde_json::json!("cold"));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(3));
}

#[test]
fn grouped_aggregate_string_count_topk_admits_constant_dependent_group_key() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![
        crate::VortexAggregateExpression::new(
            "literal_1".to_string(),
            ColumnRef::new("url").expect("column"),
            "constant_int",
        )
        .with_argument_offset(1),
    ])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let uri = DatasetUri::new("file:///tmp/string-constant-topk.vortex").expect("uri");
    let primitive_request = VortexQueryPrimitiveRequest::simple_aggregate(uri, request.clone())
        .with_source_order_limit(2);
    assert!(string_count_topk_heavy_hitter_route_enabled(
        1_000_000,
        &request,
        &primitive_request,
        true,
    ));
    let declared_columns = vec!["url".to_string()];
    let mut states = GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, true)
        .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 0, 1, 1, 2],
        values: vec![
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("warm"),
            std::sync::Arc::<str>::from("cold"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter count pass")
    );
    states
        .prepare_string_count_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_string_count_topk_heavy_hitter_exact_from_accessors(&accessors, None)
            .expect("exact recount")
    );
    assert!(
        states
            .string_count_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_topk_late_recount"
    );
    assert_eq!(
        payload["group_state_mode"],
        "string_dictionary_code_candidate_state_then_exact_recount"
    );
    assert_eq!(payload["values"][0]["url"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["literal_1"], serde_json::json!(1));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(3));
    assert_eq!(payload["values"][1]["url"], serde_json::json!("warm"));
    assert_eq!(payload["values"][1]["literal_1"], serde_json::json!(1));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_string_count_topk_late_measures_promote_selective_first_pass() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("phrase").expect("column")],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("url").expect("column")),
                "first_url".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("user_id").expect("column")),
                "users".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec![
        "phrase".to_string(),
        "url".to_string(),
        "user_id".to_string(),
    ];
    let mut states = GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, true)
        .expect("states");
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1, 1, 2],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
                std::sync::Arc::<str>::from("cold"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![1, 0, 3, 2, 1, 0],
            values: vec![
                std::sync::Arc::<str>::from("a.example"),
                std::sync::Arc::<str>::from("b.example"),
                std::sync::Arc::<str>::from("c.example"),
                std::sync::Arc::<str>::from("d.example"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 1, 2, 3, 3, 4]),
    ];
    let selected_rows = vec![0, 1, 2, 3, 4];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&accessors, Some(&selected_rows))
            .expect("heavy-hitter count and late-measure pass")
    );
    assert!(
        states.string_count_topk_heavy_hitter_sketch.is_some(),
        "retained first-pass late-measure route still maintains the proof sketch"
    );
    assert_eq!(
        states.string_count_topk_total_weight,
        selected_rows.len() as u64
    );
    assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        states
            .promote_string_count_topk_exact_first_pass_if_possible(Some(2))
            .expect("promote selective first pass")
    );
    assert!(
        states.string_count_topk_heavy_hitter_sketch.is_some(),
        "bounded exact mirror promotion keeps the sketch as proof evidence"
    );
    assert_eq!(
        states.string_count_topk_exact_counts_source,
        Some("first_pass_no_eviction")
    );
    assert!(!states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        states
            .string_count_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, selected_rows.len(), 2)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_heavy_hitter_sketch_update".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"proofbound_string_topk_retention".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_heavy_hitter_first_pass_exact_counts".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_first_pass_late_measure_state".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_first_pass_count_distinct_state".to_string())
    );
    assert!(
        !budget
            .capillary_work_units
            .contains(&"string_topk_candidate_late_measure_recount".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_heavy_hitter_no_eviction_exact_proof".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_topk_first_pass_late_measure_rows".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"retained_candidate_distinct_values".to_string())
    );
    assert!(
        !budget
            .pulseweave_pressure_signals
            .contains(&"late_measure_second_scan".to_string())
    );

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_topk_first_pass_late_measure_exact"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "string_dictionary_code_count_topk_first_pass_late_measure_exact"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "string_dictionary_code_heavy_hitter_exact_late_measure_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "string_dictionary_code_heavy_hitter_exact_first_pass_late_measure"
    );
    assert_eq!(
        payload["distinct_state_strategy"],
        "proofbound_first_pass_late_measure_exact"
    );
    assert_eq!(
        payload["uniqueness_proof_status"],
        "proofbound_heavy_hitter_exact_first_pass_no_eviction"
    );
    assert_eq!(
        payload["string_count_topk_heavy_hitter_exact_counts_source"],
        serde_json::json!("first_pass_no_eviction")
    );
    assert_eq!(
        payload["string_count_topk_exact_mirror_used"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_topk_late_measure_second_pass"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_topk_first_pass_late_measures"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_first_pass_late_measure_rows"],
        serde_json::json!(selected_rows.len())
    );
    assert_eq!(
        payload["string_count_topk_late_measure_candidate_groups"],
        serde_json::json!(2)
    );
    assert_eq!(payload["values"][0]["phrase"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(3));
    assert_eq!(
        payload["values"][0]["first_url"],
        serde_json::json!("a.example")
    );
    assert_eq!(payload["values"][0]["users"], serde_json::json!(2));
    assert_eq!(payload["values"][1]["phrase"], serde_json::json!("warm"));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(2));
    assert_eq!(
        payload["values"][1]["first_url"],
        serde_json::json!("b.example")
    );
    assert_eq!(payload["values"][1]["users"], serde_json::json!(1));
}

#[test]
fn grouped_aggregate_string_count_topk_late_measures_use_candidate_recount() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("phrase").expect("column")],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("url").expect("column")),
                "first_url".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("user_id").expect("column")),
                "users".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec![
        "phrase".to_string(),
        "url".to_string(),
        "user_id".to_string(),
    ];
    let mut states = GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, true)
        .expect("states");
    states.string_count_topk_first_pass_late_measure_disabled = true;
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1, 1, 2],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
                std::sync::Arc::<str>::from("cold"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![1, 0, 3, 2, 1, 0],
            values: vec![
                std::sync::Arc::<str>::from("a.example"),
                std::sync::Arc::<str>::from("b.example"),
                std::sync::Arc::<str>::from("c.example"),
                std::sync::Arc::<str>::from("d.example"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 1, 2, 3, 3, 4]),
    ];
    let selected_rows = vec![0, 1, 2, 3, 4];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&accessors, Some(&selected_rows))
            .expect("heavy-hitter count pass")
    );
    assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        !states
            .promote_string_count_topk_exact_first_pass_if_possible(Some(2))
            .expect("late measures require second pass")
    );
    states
        .prepare_string_count_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_string_count_topk_heavy_hitter_exact_from_accessors(
                &accessors,
                Some(&selected_rows)
            )
            .expect("exact candidate measure recount")
    );
    assert!(
        states
            .string_count_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, selected_rows.len(), 2)
        .expect("state budget");
    assert!(budget.state_family.contains("candidate_late_measures"));
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_late_measure_recount".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_dictionary_code_prefilter".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_count_distinct_state".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"retained_candidate_distinct_values".to_string())
    );

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_topk_late_measure_recount"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "string_dictionary_code_count_topk_late_measure_recount"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "string_dictionary_code_heavy_hitter_candidate_late_measure_group_state"
    );
    assert_eq!(
        payload["distinct_state_strategy"],
        "proofbound_candidate_late_measure_exact"
    );
    assert_eq!(
        payload["string_count_topk_late_measure_second_pass"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_late_measure_candidate_groups"],
        serde_json::json!(2)
    );
    assert_eq!(
        payload["string_count_topk_candidate_code_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_candidate_free_chunks_skipped"],
        serde_json::json!(0)
    );
    assert_eq!(payload["values"][0]["phrase"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(3));
    assert_eq!(
        payload["values"][0]["first_url"],
        serde_json::json!("a.example")
    );
    assert_eq!(payload["values"][0]["users"], serde_json::json!(2));
    assert_eq!(payload["values"][1]["phrase"], serde_json::json!("warm"));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(2));
    assert_eq!(
        payload["values"][1]["first_url"],
        serde_json::json!("b.example")
    );
    assert_eq!(payload["values"][1]["users"], serde_json::json!(1));
}

#[test]
fn grouped_aggregate_string_count_topk_skips_candidate_free_late_measure_chunks() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("phrase").expect("column")],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("url").expect("column")),
                "first_url".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["phrase".to_string(), "url".to_string()];
    let mut states = GroupedAggregateStates::new(&request, Some(1), &declared_columns, false, true)
        .expect("states");
    let candidate_chunk = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 0, 1],
            values: vec![
                std::sync::Arc::<str>::from("a.example"),
                std::sync::Arc::<str>::from("b.example"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
    ];
    let candidate_free_chunk = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0],
            values: vec![std::sync::Arc::<str>::from("cold")]
                .into_iter()
                .map(Into::into)
                .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0],
            values: vec![std::sync::Arc::<str>::from("z.example")]
                .into_iter()
                .map(Into::into)
                .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
    ];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&candidate_chunk, None)
            .expect("heavy-hitter count pass")
    );
    assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
    states
        .prepare_string_count_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_string_count_topk_heavy_hitter_exact_from_accessors(&candidate_free_chunk, None)
            .expect("candidate-free exact recount chunk")
    );
    assert_eq!(states.string_count_topk_candidate_free_chunks_skipped, 1);
    assert_eq!(states.string_count_topk_candidate_free_rows_skipped, 3);
    assert!(
        states
            .update_string_count_topk_heavy_hitter_exact_from_accessors(&candidate_chunk, None)
            .expect("candidate exact recount chunk")
    );
    assert!(
        states
            .string_count_topk_exact_proved(Some(1))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 7, 1)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_free_chunk_skip".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_topk_candidate_free_rows_skipped".to_string())
    );
    let (_row_count, summary) = states
        .result_row_count_and_summary(Some(1))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(
        payload["string_count_topk_candidate_free_chunks_skipped"],
        serde_json::json!(1)
    );
    assert_eq!(
        payload["string_count_topk_candidate_free_rows_skipped"],
        serde_json::json!(3)
    );
    assert_eq!(payload["values"][0]["phrase"], serde_json::json!("hot"));
    assert_eq!(
        payload["values"][0]["first_url"],
        serde_json::json!("a.example")
    );
}

#[test]
fn grouped_aggregate_string_count_topk_skips_candidate_free_count_only_chunks() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["url".to_string()];
    let mut states = GroupedAggregateStates::new(&request, Some(1), &declared_columns, false, true)
        .expect("states");
    let candidate_chunk = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 0, 1],
        values: vec![
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("warm"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];
    let candidate_free_chunk = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 0, 0],
        values: vec![std::sync::Arc::<str>::from("cold")]
            .into_iter()
            .map(Into::into)
            .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&candidate_chunk, None)
            .expect("heavy-hitter count pass")
    );
    states
        .prepare_string_count_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_string_count_topk_heavy_hitter_exact_from_accessors(&candidate_free_chunk, None)
            .expect("candidate-free exact recount chunk")
    );
    assert_eq!(states.string_count_topk_candidate_free_chunks_skipped, 1);
    assert_eq!(states.string_count_topk_candidate_free_rows_skipped, 4);
    assert!(
        states
            .update_string_count_topk_heavy_hitter_exact_from_accessors(&candidate_chunk, None)
            .expect("candidate exact recount chunk")
    );
    assert!(
        states
            .string_count_topk_exact_proved(Some(1))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 8, 1)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_free_chunk_skip".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_topk_candidate_dictionary_histogram_recount".to_string())
    );
    let (_row_count, summary) = states
        .result_row_count_and_summary(Some(1))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(
        payload["string_count_topk_dictionary_histogram_recount"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_candidate_free_chunks_skipped"],
        serde_json::json!(1)
    );
    assert_eq!(
        payload["string_count_topk_candidate_free_rows_skipped"],
        serde_json::json!(4)
    );
    assert_eq!(payload["values"][0]["url"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(3));
}

#[test]
fn grouped_aggregate_string_count_topk_skips_recount_when_first_pass_is_exact() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["url".to_string()];
    let mut states = GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, true)
        .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 0, 1, 1, 2],
        values: vec![
            std::sync::Arc::<str>::from("hot"),
            std::sync::Arc::<str>::from("warm"),
            std::sync::Arc::<str>::from("cold"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
    }];

    assert!(
        states
            .update_string_count_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter count pass")
    );
    assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        states
            .promote_string_count_topk_exact_first_pass_if_possible(Some(2))
            .expect("promote exact first pass")
    );
    assert!(!states.needs_string_count_topk_heavy_hitter_second_pass());
    assert!(
        states
            .string_count_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 6, 2)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_heavy_hitter_first_pass_exact_counts".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_heavy_hitter_bounded_exact_mirror".to_string())
    );
    assert!(
        !budget
            .capillary_work_units
            .contains(&"string_topk_candidate_exact_recount".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_topk_first_pass_exact"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "string_dictionary_code_count_topk_heavy_hitter_first_pass_exact"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "string_dictionary_code_heavy_hitter_exact_count_star_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "string_dictionary_code_heavy_hitter_exact_first_pass"
    );
    assert_eq!(
        payload["string_count_topk_heavy_hitter_second_pass"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_topk_heavy_hitter_exact_counts_source"],
        serde_json::json!("first_pass_bounded_exact_mirror")
    );
    assert_eq!(
        payload["string_count_topk_exact_mirror_used"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_topk_exact_mirror_disabled"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_topk_dictionary_code_reuse"],
        serde_json::json!(true)
    );
    assert_eq!(payload["values"][0]["url"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(3));
    assert_eq!(payload["values"][1]["url"], serde_json::json!("warm"));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_string_count_topk_skips_recount_when_retained_exact_boundary_is_proved() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["url".to_string()];
    let mut states = GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, true)
        .expect("states");
    let mut sketch = StringCountTopKHeavyHitterSketch::new(2);
    let hot_id = states.string_interner.intern("hot").expect("hot id");
    let warm_id = states.string_interner.intern("warm").expect("warm id");
    let cold_id = states.string_interner.intern("cold").expect("cold id");
    sketch.update(hot_id, 100).expect("hot update");
    sketch.update(warm_id, 90).expect("warm update");
    sketch
        .update(cold_id, 50)
        .expect("cold update triggers offset");
    assert!(
        sketch.offset > 0,
        "test must exercise the retained-boundary proof, not no-eviction exactness"
    );
    states.string_count_topk_heavy_hitter_sketch = Some(sketch);
    states.string_count_topk_total_weight = 240;
    states.string_count_topk_string_group_index = Some(0);
    states.count_star_direct_updates = true;
    states.chunk_dictionary_direct_updates = true;
    states.string_count_topk_heavy_hitter_direct_updates = true;
    states.string_count_topk_dictionary_code_reuse = true;

    assert!(
        states
            .promote_string_count_topk_exact_first_pass_if_possible(Some(2))
            .expect("promote retained-boundary exact first pass")
    );
    assert!(!states.needs_string_count_topk_heavy_hitter_second_pass());
    let budget = states
        .state_budget_report(&request, 240, 2)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_heavy_hitter_retained_boundary_exact_proof".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_heavy_hitter_retained_boundary_upper_bound".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_topk_first_pass_exact"
    );
    assert_eq!(
        payload["uniqueness_proof_status"],
        "proofbound_heavy_hitter_exact_first_pass_retained_boundary"
    );
    assert_eq!(
        payload["string_count_topk_heavy_hitter_second_pass"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_topk_heavy_hitter_exact_counts_source"],
        serde_json::json!("first_pass_retained_boundary_proof")
    );
    assert_eq!(
        payload["string_count_topk_dictionary_code_reuse"],
        serde_json::json!(true)
    );
    assert_eq!(payload["values"][0]["url"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(100));
    assert_eq!(payload["values"][1]["url"], serde_json::json!("warm"));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(90));
}

#[test]
fn heavy_hitter_routes_admit_pushdown_only_predicates() {
    let uri = DatasetUri::new("file:///tmp/pushdown-only-heavy-hitter.vortex").expect("uri");

    let string_aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("url").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let mut string_request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), string_aggregate.clone())
            .with_source_order_limit(10);
    string_request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("event_day").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(1),
    });
    assert!(string_count_topk_heavy_hitter_route_enabled(
        1_000_000,
        &string_aggregate,
        &string_request,
        true,
    ));
    assert!(!string_count_topk_heavy_hitter_route_enabled(
        1_000_000,
        &string_aggregate,
        &string_request,
        false,
    ));
    let string_late_measure_aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("SearchPhrase").expect("column")],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("URL").expect("column")),
                "first_url".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("UserID").expect("column")),
                "users".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let mut string_late_measure_request = VortexQueryPrimitiveRequest::simple_aggregate(
        uri.clone(),
        string_late_measure_aggregate.clone(),
    )
    .with_source_order_limit(10);
    string_late_measure_request.predicate = Some(PredicateExpr::StringContains {
        column: ColumnRef::new("URL").expect("column"),
        needle: "google".to_string(),
        negated: false,
    });
    assert!(string_count_topk_heavy_hitter_route_enabled(
        1_000_000,
        &string_late_measure_aggregate,
        &string_late_measure_request,
        true,
    ));
    assert!(!string_count_topk_heavy_hitter_route_enabled(
        1_000_000,
        &string_late_measure_aggregate,
        &string_late_measure_request,
        false,
    ));
    assert!(predicate_exact_row_filter_supported(
        string_late_measure_request
            .predicate
            .as_ref()
            .expect("string contains pushdown predicate")
    ));

    let numeric_utf8_aggregate = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("user_id").expect("column"),
            ColumnRef::new("search_phrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let mut numeric_utf8_request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), numeric_utf8_aggregate.clone())
            .with_source_order_limit(10);
    numeric_utf8_request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("event_day").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(1),
    });
    assert!(numeric_utf8_topk_heavy_hitter_route_enabled(
        1_000_000,
        &numeric_utf8_aggregate,
        &numeric_utf8_request,
        true,
    ));
    assert!(!numeric_utf8_topk_heavy_hitter_route_enabled(
        1_000_000,
        &numeric_utf8_aggregate,
        &numeric_utf8_request,
        false,
    ));

    let numeric_pair_aggregate = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("watch_id").expect("column"),
            ColumnRef::new("client_ip").expect("column"),
        ],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("width").expect("column")),
                "sum_width".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("width").expect("column")),
                "avg_width".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let mut numeric_pair_request =
        VortexQueryPrimitiveRequest::simple_aggregate(uri, numeric_pair_aggregate.clone())
            .with_source_order_limit(10);
    numeric_pair_request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("event_day").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(1),
    });
    assert!(numeric_pair_late_measure_route_enabled(
        1_000_000,
        &numeric_pair_aggregate,
        &numeric_pair_request,
        true,
    ));
    assert!(!numeric_pair_late_measure_route_enabled(
        1_000_000,
        &numeric_pair_aggregate,
        &numeric_pair_request,
        false,
    ));
}

#[test]
fn exact_row_filter_classifier_accepts_vortex_like_string_contains() {
    let compare = PredicateExpr::Compare {
        column: ColumnRef::new("event_day").expect("column"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(1),
    };
    let contains = PredicateExpr::StringContains {
        column: ColumnRef::new("URL").expect("column"),
        needle: "google".to_string(),
        negated: false,
    };

    assert!(predicate_exact_row_filter_supported(&compare));
    assert!(predicate_exact_row_filter_supported(&contains));
    assert!(predicate_exact_row_filter_supported(&PredicateExpr::And(
        vec![compare, contains]
    )));
}

#[test]
fn string_contains_pushdown_is_route_aware_for_aggregate_regression_guard() {
    let compare = PredicateExpr::Compare {
        column: ColumnRef::new("CounterID").expect("column"),
        op: ComparisonOp::Eq,
        value: StatValue::UInt64(62),
    };
    let (aggregate_compare_pushdown, aggregate_compare_residual) =
        split_predicate_for_vortex_pushdown(&compare, VortexQueryPrimitiveKind::SimpleAggregate);
    assert!(aggregate_compare_pushdown.is_some());
    assert!(aggregate_compare_residual.is_none());

    let contains = PredicateExpr::StringContains {
        column: ColumnRef::new("URL").expect("column"),
        needle: "google".to_string(),
        negated: false,
    };

    let (count_pushdown, count_residual) =
        split_predicate_for_vortex_pushdown(&contains, VortexQueryPrimitiveKind::CountWhere);
    assert!(count_pushdown.is_some());
    assert!(count_residual.is_none());

    let (sort_pushdown, sort_residual) =
        split_predicate_for_vortex_pushdown(&contains, VortexQueryPrimitiveKind::SortRows);
    assert!(sort_pushdown.is_some());
    assert!(sort_residual.is_none());

    let (aggregate_pushdown, aggregate_residual) =
        split_predicate_for_vortex_pushdown(&contains, VortexQueryPrimitiveKind::SimpleAggregate);
    assert!(aggregate_pushdown.is_some());
    assert!(aggregate_residual.is_none());
}

#[test]
fn string_topk_candidate_code_flags_allow_candidate_free_chunks() {
    let mut candidates = rustc_hash::FxHashSet::default();
    candidates.insert(99);

    let flags = string_topk_candidate_code_flags(
        &[10, 11, 12],
        &candidates,
        "string top-K candidate-free chunk",
    );

    assert_eq!(flags, vec![false, false, false]);
}

#[test]
fn grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("phrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("user_id").expect("column")),
            "users".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("users", true)]);
    let declared_columns = vec!["phrase".to_string(), "user_id".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    states.enable_string_count_distinct_topk_heavy_hitter();
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1, 1, 2],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
                std::sync::Arc::<str>::from("cold"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 2, 1, 3, 4, 5]),
    ];

    assert!(
        states
            .update_string_count_distinct_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter distinct bound pass")
    );
    assert!(states.needs_string_count_distinct_topk_heavy_hitter_second_pass());
    assert!(
        states
            .string_count_distinct_topk_heavy_hitter_exact_proof_possible(Some(2))
            .expect("proof possible")
    );
    states
        .prepare_string_count_distinct_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_string_count_distinct_topk_heavy_hitter_exact_from_accessors(&accessors)
            .expect("exact recount")
    );
    assert!(
        states
            .string_count_distinct_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 6, 2)
        .expect("state budget");
    assert!(
        budget
            .state_family
            .contains("proofbound_string_count_distinct_heavy_hitter")
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_count_distinct_candidate_exact_recount".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_code_bound_string_count_distinct_topk_key".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_count_distinct_candidate_id_prefilter".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_count_distinct_candidate_signature_prefilter".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_count_distinct_candidate_dictionary_code_prefilter".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_distinct_topk_late_recount"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "string_dictionary_code_count_distinct_topk_heavy_hitter_late_recount"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "string_dictionary_code_heavy_hitter_candidate_count_distinct_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "string_dictionary_code_count_distinct_candidate_state_then_exact_recount"
    );
    assert_eq!(
        payload["distinct_state_strategy"],
        "proofbound_candidate_exact_distinct_sets"
    );
    assert_eq!(
        payload["string_count_distinct_topk_heavy_hitter_exact_proof"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_distinct_topk_candidate_clone_strategy"],
        "clone_only_surviving_retained_candidates"
    );
    assert_eq!(
        payload["string_count_distinct_topk_dictionary_code_reuse"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_distinct_topk_candidate_id_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_distinct_topk_candidate_signature_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_distinct_topk_candidate_code_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(payload["values"][0]["phrase"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["users"], serde_json::json!(2));
    assert_eq!(payload["values"][1]["phrase"], serde_json::json!("warm"));
    assert_eq!(payload["values"][1]["users"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_string_count_distinct_topk_skips_recount_when_first_pass_is_exact() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("phrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("user_id").expect("column")),
            "users".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("users", true)]);
    let declared_columns = vec!["phrase".to_string(), "user_id".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    states.enable_string_count_distinct_topk_heavy_hitter();
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1, 1, 2],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
                std::sync::Arc::<str>::from("cold"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 2, 1, 3, 4, 5]),
    ];

    assert!(
        states
            .update_string_count_distinct_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter distinct bound pass")
    );
    assert!(states.needs_string_count_distinct_topk_heavy_hitter_second_pass());
    assert!(
        states
            .promote_string_count_distinct_topk_exact_first_pass_if_possible(Some(2))
            .expect("promote first pass exact sets")
    );
    assert!(!states.needs_string_count_distinct_topk_heavy_hitter_second_pass());
    assert!(
        states
            .string_count_distinct_topk_exact_proved(Some(2))
            .expect("first pass exact proof")
    );
    assert_eq!(
        states.string_count_distinct_topk_first_pass_exact_set_entries,
        5
    );
    let budget = states
        .state_budget_report(&request, 6, 2)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_count_distinct_heavy_hitter_first_pass_exact_sets".to_string())
    );
    assert!(
        !budget
            .capillary_work_units
            .contains(&"string_count_distinct_candidate_exact_recount".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_code_bound_string_count_distinct_topk_key".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_string_count_distinct_topk_first_pass_exact"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "string_dictionary_code_count_distinct_topk_heavy_hitter_first_pass_exact"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "string_dictionary_code_heavy_hitter_exact_count_distinct_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "string_dictionary_code_count_distinct_exact_first_pass"
    );
    assert_eq!(
        payload["string_count_distinct_topk_exact_sets_source"],
        "first_pass_bounded_exact_sets"
    );
    assert_eq!(
        payload["string_count_distinct_topk_heavy_hitter_second_pass"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_distinct_topk_first_pass_exact_sets"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_distinct_topk_first_pass_exact_sets_disabled"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["string_count_distinct_topk_first_pass_exact_set_entries"],
        serde_json::json!(5)
    );
    assert_eq!(payload["values"][0]["phrase"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["users"], serde_json::json!(2));
    assert_eq!(payload["values"][1]["phrase"], serde_json::json!("warm"));
    assert_eq!(payload["values"][1]["users"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_string_count_distinct_topk_first_pass_retains_all_null_groups() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("phrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("user_id").expect("column")),
            "users".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("users", true)]);
    let declared_columns = vec!["phrase".to_string(), "user_id".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    states.enable_string_count_distinct_topk_heavy_hitter();
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 0],
            values: vec![
                std::sync::Arc::<str>::from("alpha"),
                std::sync::Arc::<str>::from("beta"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::NullableUInt64 {
            values: vec![0, 0, 0],
            row_nulls: vec![true, true, true],
        },
    ];

    assert!(
        states
            .update_string_count_distinct_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter distinct bound pass")
    );
    assert!(
        states
            .promote_string_count_distinct_topk_exact_first_pass_if_possible(Some(2))
            .expect("promote first pass exact sets")
    );
    assert!(
        states
            .string_count_distinct_topk_exact_proved(Some(2))
            .expect("first pass exact proof")
    );
    assert_eq!(
        states.string_count_distinct_topk_first_pass_exact_set_entries,
        0
    );
    let exact_sets = states
        .string_count_distinct_topk_exact_sets
        .as_ref()
        .expect("first pass exact sets");
    assert_eq!(exact_sets.len(), 2);
    assert!(exact_sets.values().all(rustc_hash::FxHashSet::is_empty));
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");

    assert_eq!(row_count, 2);
    assert_eq!(
        payload["string_count_distinct_topk_first_pass_exact_sets"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["string_count_distinct_topk_first_pass_exact_set_entries"],
        serde_json::json!(0)
    );
    assert_eq!(payload["values"][0]["phrase"], serde_json::json!("alpha"));
    assert_eq!(payload["values"][0]["users"], serde_json::json!(0));
    assert_eq!(payload["values"][1]["phrase"], serde_json::json!("beta"));
    assert_eq!(payload["values"][1]["users"], serde_json::json!(0));
}

#[test]
fn string_count_distinct_topk_first_pass_exact_set_budget_respects_group_state_share() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("phrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("user_id").expect("column")),
            "users".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("users", true)]);
    let declared_columns = vec!["phrase".to_string(), "user_id".to_string()];
    let envelope = VortexLocalPrimitiveResourceEnvelope::new(1, 1).expect("resource envelope");
    let expected_budget = envelope.group_state_soft_item_budget / 4;
    let states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(2),
        &declared_columns,
        false,
        false,
        envelope,
        Some(&vortex::session::VortexSession::empty()),
    )
    .expect("states");

    assert_eq!(
        states
            .string_count_distinct_topk_first_pass_exact_set_entry_budget()
            .expect("exact-set entry budget"),
        u64::try_from(expected_budget).expect("budget fits u64")
    );

    let mut constrained_envelope = envelope;
    constrained_envelope.group_state_soft_item_budget = 8;
    let mut constrained_states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(1),
        &declared_columns,
        false,
        false,
        constrained_envelope,
        Some(&vortex::session::VortexSession::empty()),
    )
    .expect("constrained states");
    constrained_states.enable_string_count_distinct_topk_heavy_hitter();
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0],
            values: vec![std::sync::Arc::<str>::from("hot")]
                .into_iter()
                .map(Into::into)
                .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 2, 3]),
    ];

    assert_eq!(
        constrained_states
            .string_count_distinct_topk_first_pass_exact_set_entry_budget()
            .expect("constrained exact-set entry budget"),
        2
    );
    assert!(
        constrained_states
            .update_string_count_distinct_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter distinct bound pass")
    );
    assert!(constrained_states.string_count_distinct_topk_first_pass_exact_sets_disabled);
    assert!(
        !constrained_states
            .promote_string_count_distinct_topk_exact_first_pass_if_possible(Some(1))
            .expect("budget-disabled promotion")
    );
}

#[test]
fn grouped_aggregate_string_count_distinct_topk_skips_candidate_free_chunks() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("phrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("user_id").expect("column")),
            "users".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("users", true)]);
    let declared_columns = vec!["phrase".to_string(), "user_id".to_string()];
    let mut states = GroupedAggregateStates::new(&request, Some(1), &declared_columns, false, true)
        .expect("states");
    states.enable_string_count_distinct_topk_heavy_hitter();
    let candidate_chunk = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 2, 1, 3]),
    ];
    let candidate_free_chunk = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 0],
            values: vec![std::sync::Arc::<str>::from("cold")]
                .into_iter()
                .map(Into::into)
                .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::UInt64(vec![9, 10, 11, 12]),
    ];

    assert!(
        states
            .update_string_count_distinct_topk_heavy_hitter_from_accessors(&candidate_chunk, None)
            .expect("heavy-hitter distinct bound pass")
    );
    states
        .prepare_string_count_distinct_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_string_count_distinct_topk_heavy_hitter_exact_from_accessors(
                &candidate_free_chunk
            )
            .expect("candidate-free exact recount chunk")
    );
    assert_eq!(
        states.string_count_distinct_topk_candidate_free_chunks_skipped,
        1
    );
    assert_eq!(
        states.string_count_distinct_topk_candidate_free_rows_skipped,
        4
    );
    assert!(
        states
            .update_string_count_distinct_topk_heavy_hitter_exact_from_accessors(&candidate_chunk)
            .expect("candidate exact recount chunk")
    );
    assert!(
        states
            .string_count_distinct_topk_exact_proved(Some(1))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 8, 1)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"string_count_distinct_candidate_free_chunk_skip".to_string())
    );
    let (_row_count, summary) = states
        .result_row_count_and_summary(Some(1))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(
        payload["string_count_distinct_topk_candidate_free_chunks_skipped"],
        serde_json::json!(1)
    );
    assert_eq!(
        payload["string_count_distinct_topk_candidate_free_rows_skipped"],
        serde_json::json!(4)
    );
    assert_eq!(payload["values"][0]["phrase"], serde_json::json!("hot"));
    assert_eq!(payload["values"][0]["users"], serde_json::json!(2));
}

#[test]
fn numeric_utf8_candidate_prefilter_maps_dictionary_codes_to_numeric_parts() {
    let hot = std::sync::Arc::<str>::from("hot");
    let warm = std::sync::Arc::<str>::from("warm");
    let values = vec![hot.clone(), warm];
    let mut string_interner = AggregateStringInterner::default();
    let hot_id = string_interner
        .intern_dictionary(&hot.into())
        .expect("intern hot");
    let _warm_id = string_interner
        .intern_dictionary(&values[1].clone().into())
        .expect("intern warm");
    let mut candidates = rustc_hash::FxHashSet::default();
    candidates.insert(AggregateNumericUtf8InternedKey::new(
        AggregateIntegerKeyPart {
            bits: 7,
            signed: false,
        },
        hot_id,
    ));

    let candidate_parts_by_utf8_id =
        numeric_utf8_candidate_parts_by_utf8_id(&candidates).expect("candidate partitions");
    assert_eq!(candidate_parts_by_utf8_id.len(), 1);
    assert!(candidate_parts_by_utf8_id.contains_key(&hot_id));
    let candidate_numeric_by_code = numeric_utf8_candidate_parts_by_dictionary_code(
        &values.into_iter().map(Into::into).collect::<Vec<_>>(),
        &string_interner,
        &candidate_parts_by_utf8_id,
    );

    assert_eq!(candidate_numeric_by_code.len(), 2);
    let hot_code = candidate_numeric_by_code[0].as_ref().expect("hot code");
    assert_eq!(hot_code.utf8_id, hot_id);
    assert!(hot_code.numeric_parts.contains(&(7, false)));
    assert!(candidate_numeric_by_code[1].is_none());
}

#[test]
fn grouped_aggregate_numeric_utf8_count_topk_uses_proofbound_recount() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("user_id").expect("column"),
            ColumnRef::new("search_phrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["user_id".to_string(), "search_phrase".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    states.enable_numeric_utf8_topk_heavy_hitter();
    let accessors = vec![
        AggregateDirectColumnAccessor::Int64(vec![1, 1, 1, 2, 2, 3, 1]),
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1, 1, 2, 0],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
                std::sync::Arc::<str>::from("cold"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
    ];

    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter count pass")
    );
    assert!(states.needs_numeric_utf8_topk_heavy_hitter_second_pass());
    states
        .prepare_numeric_utf8_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .numeric_utf8_topk_candidate_parts_by_utf8_id
            .as_ref()
            .is_some_and(|partitions| !partitions.is_empty())
    );
    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_exact_from_accessors(&accessors)
            .expect("exact recount")
    );
    assert!(
        states
            .numeric_utf8_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 7, 2)
        .expect("state budget");
    assert!(
        budget
            .state_family
            .contains("proofbound_numeric_utf8_heavy_hitter")
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_topk_candidate_exact_recount".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_code_bound_numeric_utf8_key".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_candidate_utf8_id_partition_reuse".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"dictionary_code_string_key_reuse".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"numeric_utf8_candidate_string_id_partitions".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_numeric_utf8_count_topk_late_recount"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "numeric_utf8_dictionary_code_count_topk_heavy_hitter_late_recount"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "numeric_utf8_dictionary_code_heavy_hitter_candidate_count_star_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "numeric_utf8_dictionary_code_candidate_state_then_exact_recount"
    );
    assert_eq!(
        payload["numeric_utf8_topk_dictionary_code_reuse"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_utf8_topk_heavy_hitter_exact_proof"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_utf8_topk_candidate_id_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_utf8_topk_candidate_code_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_utf8_topk_candidate_utf8_id_partition_reuse"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_utf8_topk_candidate_utf8_id_partitions"],
        serde_json::json!(3)
    );
    assert_eq!(
        payload["numeric_utf8_topk_exact_count_key_storage"],
        "interned_numeric_utf8_id"
    );
    assert_eq!(
        payload["numeric_utf8_topk_candidate_clone_strategy"],
        "clone_only_surviving_retained_candidates"
    );
    assert_eq!(payload["values"][0]["user_id"], serde_json::json!(1));
    assert_eq!(
        payload["values"][0]["search_phrase"],
        serde_json::json!("hot")
    );
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(4));
    assert_eq!(payload["values"][1]["user_id"], serde_json::json!(2));
    assert_eq!(
        payload["values"][1]["search_phrase"],
        serde_json::json!("warm")
    );
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_numeric_utf8_count_topk_skips_candidate_free_chunks() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("user_id").expect("column"),
            ColumnRef::new("search_phrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["user_id".to_string(), "search_phrase".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(1), &declared_columns, false, false)
            .expect("states");
    states.enable_numeric_utf8_topk_heavy_hitter();
    let candidate_chunk = vec![
        AggregateDirectColumnAccessor::Int64(vec![1, 1, 1, 2]),
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
    ];
    let candidate_free_chunk = vec![
        AggregateDirectColumnAccessor::Int64(vec![9, 9, 9, 9]),
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 0],
            values: vec![std::sync::Arc::<str>::from("cold")]
                .into_iter()
                .map(Into::into)
                .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
    ];

    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_from_accessors(&candidate_chunk, None)
            .expect("heavy-hitter count pass")
    );
    states
        .prepare_numeric_utf8_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_exact_from_accessors(&candidate_free_chunk)
            .expect("candidate-free exact recount chunk")
    );
    assert_eq!(states.numeric_utf8_topk_candidate_free_chunks_skipped, 1);
    assert_eq!(states.numeric_utf8_topk_candidate_free_rows_skipped, 4);
    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_exact_from_accessors(&candidate_chunk)
            .expect("candidate exact recount chunk")
    );
    assert!(
        states
            .numeric_utf8_topk_exact_proved(Some(1))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 8, 1)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_candidate_dictionary_code_prefilter".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_candidate_utf8_id_partition_reuse".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_candidate_free_chunk_skip".to_string())
    );
    let (_row_count, summary) = states
        .result_row_count_and_summary(Some(1))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(
        payload["numeric_utf8_topk_candidate_code_prefilter"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_utf8_topk_candidate_utf8_id_partition_reuse"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_utf8_topk_candidate_free_chunks_skipped"],
        serde_json::json!(1)
    );
    assert_eq!(
        payload["numeric_utf8_topk_candidate_free_rows_skipped"],
        serde_json::json!(4)
    );
    assert_eq!(payload["values"][0]["user_id"], serde_json::json!(1));
    assert_eq!(
        payload["values"][0]["search_phrase"],
        serde_json::json!("hot")
    );
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(3));
}

#[test]
fn grouped_aggregate_numeric_utf8_topk_skips_recount_when_first_pass_is_exact() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("user_id").expect("column"),
            ColumnRef::new("search_phrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["user_id".to_string(), "search_phrase".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    states.enable_numeric_utf8_topk_heavy_hitter();
    let accessors = vec![
        AggregateDirectColumnAccessor::Int64(vec![1, 1, 1, 2, 2, 3, 1]),
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 1, 1, 2, 0],
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
                std::sync::Arc::<str>::from("cold"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
    ];

    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter count pass")
    );
    assert!(states.needs_numeric_utf8_topk_heavy_hitter_second_pass());
    assert!(
        states
            .promote_numeric_utf8_topk_exact_first_pass_if_possible(Some(2))
            .expect("promote exact first pass")
    );
    assert!(!states.needs_numeric_utf8_topk_heavy_hitter_second_pass());
    assert!(
        states
            .numeric_utf8_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 7, 2)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_heavy_hitter_first_pass_exact_counts".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_heavy_hitter_bounded_exact_mirror".to_string())
    );
    assert!(
        !budget
            .capillary_work_units
            .contains(&"numeric_utf8_topk_candidate_exact_recount".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "proofbound_heavy_hitter_numeric_utf8_count_topk_first_pass_exact"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "numeric_utf8_dictionary_code_count_topk_heavy_hitter_first_pass_exact"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "numeric_utf8_dictionary_code_heavy_hitter_exact_count_star_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "numeric_utf8_dictionary_code_heavy_hitter_exact_first_pass"
    );
    assert_eq!(
        payload["numeric_utf8_topk_heavy_hitter_second_pass"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["numeric_utf8_topk_heavy_hitter_exact_counts_source"],
        serde_json::json!("first_pass_bounded_exact_mirror")
    );
    assert_eq!(
        payload["numeric_utf8_topk_exact_mirror_used"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_utf8_topk_exact_mirror_disabled"],
        serde_json::json!(false)
    );
    assert_eq!(
        payload["numeric_utf8_topk_dictionary_code_reuse"],
        serde_json::json!(true)
    );
    assert_eq!(payload["values"][0]["user_id"], serde_json::json!(1));
    assert_eq!(
        payload["values"][0]["search_phrase"],
        serde_json::json!("hot")
    );
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(4));
    assert_eq!(payload["values"][1]["user_id"], serde_json::json!(2));
    assert_eq!(
        payload["values"][1]["search_phrase"],
        serde_json::json!("warm")
    );
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_numeric_utf8_topk_compacts_repeated_chunk_keys() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("user_id").expect("column"),
            ColumnRef::new("search_phrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["user_id".to_string(), "search_phrase".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    states.enable_numeric_utf8_topk_heavy_hitter();
    let mut numeric = Vec::with_capacity(20_000);
    let mut row_ids = Vec::with_capacity(20_000);
    for row_index in 0..20_000 {
        if row_index < 12_000 {
            numeric.push(1);
            row_ids.push(0);
        } else {
            numeric.push(2);
            row_ids.push(1);
        }
    }
    let accessors = vec![
        AggregateDirectColumnAccessor::Int64(numeric),
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids,
            values: vec![
                std::sync::Arc::<str>::from("hot"),
                std::sync::Arc::<str>::from("warm"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
    ];

    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("chunk-compacted heavy-hitter count pass")
    );
    assert!(states.numeric_utf8_topk_chunk_compacted_updates);
    states
        .prepare_numeric_utf8_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_exact_from_accessors(&accessors)
            .expect("chunk-compacted exact recount")
    );
    assert!(
        states
            .numeric_utf8_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let budget = states
        .state_budget_report(&request, 20_000, 2)
        .expect("state budget");
    assert!(budget.state_family.contains("chunk_compacted_updates"));
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_chunk_compacted_sketch_update".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_utf8_chunk_compacted_exact_merge".to_string())
    );
    let (_, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(
        payload["numeric_utf8_topk_chunk_compacted_updates"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["values"][0]["search_phrase"],
        serde_json::json!("hot")
    );
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(12000));
    assert_eq!(
        payload["values"][1]["search_phrase"],
        serde_json::json!("warm")
    );
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(8000));
}

#[test]
fn numeric_utf8_dictionary_interner_reuses_prepared_dictionary_arc() {
    let hot = std::sync::Arc::<str>::from("hot");
    let warm = std::sync::Arc::<str>::from("warm");
    let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 1],
        values: vec![hot.clone(), warm]
            .into_iter()
            .map(Into::into)
            .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    };
    let mut interner = AggregateStringInterner::default();

    let ids = aggregate_direct_utf8_dictionary_interner_ids(&accessor, &mut interner)
        .expect("dictionary ids")
        .expect("dictionary accessor");
    let hot_id = ids[0];
    let hot_value = interner.value_arc(hot_id).expect("interned hot");

    assert!(std::sync::Arc::ptr_eq(&hot, &hot_value));
}

#[test]
fn aggregate_string_interner_reuses_prepared_dictionary_arc_on_insert() {
    let hot = std::sync::Arc::<str>::from("hot");
    let duplicate = std::sync::Arc::<str>::from("hot");
    let mut interner = AggregateStringInterner::default();

    let first = interner
        .intern_dictionary(&std::sync::Arc::clone(&hot).into())
        .expect("first intern");
    let second = interner
        .intern_dictionary(&duplicate.into())
        .expect("duplicate intern");

    assert_eq!(first, second);
    assert_eq!(interner.len(), 1);
    let interned = interner.value_arc(first).expect("interned value");
    assert!(std::sync::Arc::ptr_eq(&hot, &interned));
}

#[test]
fn string_topk_lazy_update_skips_unretained_values_without_interning() {
    let hot = std::sync::Arc::<str>::from("hot");
    let cold = std::sync::Arc::<str>::from("cold");
    let mut interner = AggregateStringInterner::default();
    let mut sketch = StringCountTopKHeavyHitterSketch::new(1);

    sketch
        .update_lazy_utf8_value(&hot.clone().into(), 100, &mut interner)
        .expect("hot update");
    sketch
        .update_lazy_utf8_value(&cold.clone().into(), 1, &mut interner)
        .expect("cold update");

    assert!(interner.id("hot").is_some());
    assert_eq!(interner.id("cold"), None);
    assert_eq!(sketch.candidate_len(), 1);
}

#[test]
fn grouped_aggregate_numeric_utf8_topk_preserves_declared_group_key_ties() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("search_phrase").expect("column"),
            ColumnRef::new("user_id").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["search_phrase".to_string(), "user_id".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    states.enable_numeric_utf8_topk_heavy_hitter();
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 0, 0, 1, 1],
            values: vec![
                std::sync::Arc::<str>::from("alpha"),
                std::sync::Arc::<str>::from("beta"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        },
        AggregateDirectColumnAccessor::Int64(vec![1, 1, 9, 9, 1, 1]),
    ];

    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_from_accessors(&accessors, None)
            .expect("heavy-hitter count pass")
    );
    states
        .prepare_numeric_utf8_topk_heavy_hitter_second_pass()
        .expect("prepare candidates");
    assert!(
        states
            .update_numeric_utf8_topk_heavy_hitter_exact_from_accessors(&accessors)
            .expect("exact recount")
    );
    assert!(
        states
            .numeric_utf8_topk_exact_proved(Some(2))
            .expect("exact proof")
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");

    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_key_comparison_strategy"],
        "proofbound_count_desc_declared_group_key_order"
    );
    assert_eq!(
        payload["values"][0]["search_phrase"],
        serde_json::json!("alpha")
    );
    assert_eq!(payload["values"][0]["user_id"], serde_json::json!(1));
    assert_eq!(
        payload["values"][1]["search_phrase"],
        serde_json::json!("alpha")
    );
    assert_eq!(payload["values"][1]["user_id"], serde_json::json!(9));
}

#[test]
fn grouped_aggregate_single_numeric_count_uses_streaming_topk_compact_state() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("UserID").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["UserID".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let direct_accessors = vec![AggregateDirectColumnAccessor::UInt64(vec![
        42, 7, 42, 11, 42, 7,
    ])];

    assert!(
        states
            .update_single_numeric_count_direct_from_accessors(&direct_accessors, None)
            .expect("single numeric count update")
    );
    assert!(states.single_numeric_count_groups.is_some());
    assert!(states.single_numeric_count_direct_updates);
    let budget = states
        .state_budget_report(&request, 6, 2)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+compact_group_state+single_numeric"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"typed_single_numeric_group_state".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"streaming_single_numeric_topk_retention".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_streaming_single_numeric_count_topk"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "single_numeric_compact_count_star_group_state"
    );
    assert_eq!(payload["group_state_mode"], "single_numeric_hot_hash_map");
    assert_eq!(payload["group_key_storage"], "typed_single_numeric_key");
    assert_eq!(
        payload["group_key_comparison_strategy"],
        "typed_single_numeric_count_topk"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(3));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["UserID"], serde_json::json!(42));
    assert_eq!(rows[0]["rows"], serde_json::json!(3));
    assert_eq!(rows[1]["UserID"], serde_json::json!(7));
    assert_eq!(rows[1]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_single_numeric_large_retained_window_uses_select_nth_topk() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("UserID").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["UserID".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(129), &declared_columns, false, false)
            .expect("states");
    let direct_accessors = vec![AggregateDirectColumnAccessor::UInt64(
        (0_u64..160).collect::<Vec<_>>(),
    )];

    assert!(
        states
            .update_single_numeric_count_direct_from_accessors(&direct_accessors, None)
            .expect("single numeric count update")
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(129))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");

    assert_eq!(row_count, 129);
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_select_nth_single_numeric_count_topk"
    );
    assert_eq!(
        payload["group_key_comparison_strategy"],
        "typed_single_numeric_count_select_nth_topk"
    );
    assert_eq!(
        payload["topk_retention_strategy"],
        "select_nth_single_numeric_retained_window"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(160));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(129));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows.first().expect("first")["UserID"], serde_json::json!(0));
    assert_eq!(rows.last().expect("last")["UserID"], serde_json::json!(128));
}

#[test]
#[allow(clippy::too_many_lines)]
fn grouped_aggregate_numeric_minute_string_uses_streaming_topk_compact_state() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("UserID").expect("column"),
            ColumnRef::new("SearchPhrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "EventMinute".to_string(),
        ColumnRef::new("EventTime").expect("column"),
        "extract_minute",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec![
        "UserID".to_string(),
        "SearchPhrase".to_string(),
        "EventTime".to_string(),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let direct_accessors = vec![
        AggregateDirectColumnAccessor::UInt64(vec![42, 42, 7, 7, 42]),
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 1, 1, 2],
            values: vec![
                std::sync::Arc::<str>::from("alpha"),
                std::sync::Arc::<str>::from("beta"),
                std::sync::Arc::<str>::from("gamma"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
        AggregateDirectColumnAccessor::UInt64(vec![60, 61, 120, 121, 62]),
    ];

    assert!(
        states
            .update_numeric_minute_string_count_direct_from_accessors(&direct_accessors, None)
            .expect("numeric-minute-string count update")
    );
    assert!(states.numeric_minute_string_count_groups.is_some());
    assert!(states.numeric_minute_string_count_direct_updates);
    assert!(states.numeric_minute_string_direct_slice_updates);
    assert_eq!(states.numeric_minute_string_direct_slice_update_rows, 5);
    assert!(
        states.string_interner.ids.capacity() >= 3,
        "dictionary-backed compact route should reserve by dictionary cardinality before hot updates"
    );
    let budget = states
        .state_budget_report(&request, 5, 2)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+compact_group_state+numeric_minute_string"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"typed_numeric_minute_string_group_state".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"late_materialized_interned_string_keys".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"cached_worst_numeric_minute_string_topk_retention".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_code_bound_numeric_minute_string_key".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"numeric_minute_string_direct_slice_group_update".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"dictionary_code_string_key_reuse".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"numeric_minute_string_direct_slice_update_rows".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_streaming_numeric_minute_string_count_topk"
    );
    assert_eq!(
        payload["topk_retention_strategy"],
        "cached_worst_numeric_minute_string_retained_window"
    );
    assert_eq!(
        payload["numeric_minute_string_direct_slice_updates"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_minute_string_direct_slice_update_rows"],
        serde_json::json!(5)
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "numeric_minute_string_dictionary_code_compact_count_star_group_state"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "numeric_minute_string_dictionary_code_direct_group_update"
    );
    assert_eq!(
        payload["aggregate_key_encoding_mode"],
        "typed_numeric_minute_dictionary_code_hash_key"
    );
    assert_eq!(
        payload["group_state_mode"],
        "numeric_minute_string_dictionary_code_hot_hash_map"
    );
    assert_eq!(
        payload["group_key_storage"],
        "typed_numeric_minute_dictionary_code_key"
    );
    assert_eq!(
        payload["group_key_comparison_strategy"],
        "typed_numeric_minute_string_count_topk"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(3));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(2));
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    assert_eq!(payload["decoded_string_count"], serde_json::json!(3));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["UserID"], serde_json::json!(7));
    assert_eq!(rows[0]["SearchPhrase"], serde_json::json!("beta"));
    assert_eq!(rows[0]["EventMinute"], serde_json::json!(2));
    assert_eq!(rows[0]["rows"], serde_json::json!(2));
    assert_eq!(rows[1]["UserID"], serde_json::json!(42));
    assert_eq!(rows[1]["SearchPhrase"], serde_json::json!("alpha"));
    assert_eq!(rows[1]["EventMinute"], serde_json::json!(1));
    assert_eq!(rows[1]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_numeric_minute_string_uses_embedded_prepared_minute_key() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("UserID").expect("column"),
            ColumnRef::new("SearchPhrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "EventMinute".to_string(),
        ColumnRef::new("__shardloom_derived_extract_minute_EventTime").expect("prepared minute"),
        "identity",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec![
        "UserID".to_string(),
        "SearchPhrase".to_string(),
        "__shardloom_derived_extract_minute_EventTime".to_string(),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let direct_accessors = vec![
        AggregateDirectColumnAccessor::Int64(vec![42, 42, 7, 7, 42]),
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 0, 1, 1, 2],
            values: vec![
                std::sync::Arc::<str>::from("alpha"),
                std::sync::Arc::<str>::from("beta"),
                std::sync::Arc::<str>::from("gamma"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 1, 2, 2, 1]),
    ];

    assert!(
        states
            .update_numeric_minute_string_count_direct_from_accessors(&direct_accessors, None)
            .expect("prepared-minute numeric-minute-string count update")
    );
    assert!(
        states
            .numeric_minute_string_group_roles
            .expect("roles")
            .minute_column_prepared
    );
    assert!(states.numeric_minute_string_direct_slice_updates);
    assert_eq!(states.numeric_minute_string_direct_slice_update_rows, 5);
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_streaming_numeric_minute_string_count_topk"
    );
    assert_eq!(
        payload["aggregate_key_encoding_mode"],
        "typed_numeric_minute_dictionary_code_hash_key"
    );
    assert_eq!(
        payload["numeric_minute_string_direct_slice_updates"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["numeric_minute_string_direct_slice_update_rows"],
        serde_json::json!(5)
    );
    assert_eq!(payload["values"][0]["UserID"], serde_json::json!(7));
    assert_eq!(
        payload["values"][0]["SearchPhrase"],
        serde_json::json!("beta")
    );
    assert_eq!(payload["values"][0]["EventMinute"], serde_json::json!(2));
    assert_eq!(payload["values"][0]["rows"], serde_json::json!(2));
    assert_eq!(payload["values"][1]["UserID"], serde_json::json!(42));
    assert_eq!(
        payload["values"][1]["SearchPhrase"],
        serde_json::json!("alpha")
    );
    assert_eq!(payload["values"][1]["EventMinute"], serde_json::json!(1));
    assert_eq!(payload["values"][1]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_numeric_pair_rejects_nullable_materialized_keys_to_generic_state() {
    let path = unique_vortex_path("grouped-aggregate-nullable-numeric-pair");
    write_nullable_duplicate_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![
                ColumnRef::new("value").expect("column"),
                ColumnRef::new("metric").expect("column"),
            ],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("metric").expect("column")),
                    "total_metric".to_string(),
                ),
            ],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]),
    )
    .with_source_order_limit(2);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert_eq!(
        report.state_budget.state_family,
        "grouped_aggregate_state+topk+compact_numeric_measures"
    );
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    assert_eq!(
        payload["compact_group_state_strategy"],
        "compact_count_sum_avg_group_state"
    );
    assert_eq!(payload["group_state_mode"], "all_hot_hash_map");
    assert_ne!(
        payload["group_key_storage"],
        serde_json::json!("typed_numeric_pair_key")
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(5));
}

#[test]
fn grouped_aggregate_numeric_pair_demotes_before_generic_updates() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("watch_id").expect("column"),
            ColumnRef::new("client_ip").expect("column"),
        ],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("is_refresh").expect("column")),
                "refreshes".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("width").expect("column")),
                "avg_width".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec![
        "watch_id".to_string(),
        "client_ip".to_string(),
        "is_refresh".to_string(),
        "width".to_string(),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(10), &declared_columns, false, false)
            .expect("states");
    let direct_accessors = vec![
        AggregateDirectColumnAccessor::UInt64(vec![10, 20]),
        AggregateDirectColumnAccessor::UInt64(vec![1, 2]),
        AggregateDirectColumnAccessor::UInt64(vec![1, 0]),
        AggregateDirectColumnAccessor::UInt64(vec![100, 200]),
    ];

    assert!(
        states
            .update_numeric_pair_compact_direct_from_accessors(&direct_accessors, None)
            .expect("numeric pair update")
    );
    assert!(states.numeric_pair_compact_groups.is_some());
    let materialized_columns = vec![
        vec![StatValue::UInt64(10)],
        vec![StatValue::UInt64(1)],
        vec![StatValue::UInt64(1)],
        vec![StatValue::UInt64(300)],
    ];

    assert!(
        states
            .update_compact_direct_from_materialized_columns(&materialized_columns, 0)
            .expect("generic materialized update")
    );
    assert!(states.numeric_pair_compact_groups.is_none());
    assert!(!states.numeric_pair_compact_direct_updates);

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(10))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(payload["candidate_groups"], serde_json::json!(2));
    assert_eq!(
        payload["compact_group_state_strategy"],
        "compact_count_sum_avg_group_state"
    );
    assert_ne!(
        payload["group_key_storage"],
        serde_json::json!("typed_numeric_pair_key")
    );
    let rows = payload["values"].as_array().expect("values");
    let merged_row = rows
        .iter()
        .find(|row| row["watch_id"] == serde_json::json!(10))
        .expect("merged numeric-pair row");
    assert_eq!(merged_row["client_ip"], serde_json::json!(1));
    assert_eq!(merged_row["rows"], serde_json::json!(2));
    assert_eq!(merged_row["refreshes"], serde_json::json!(2.0));
    assert_eq!(merged_row["avg_width"], serde_json::json!(200.0));
}

#[test]
fn count_all_metadata_proof_basis_cites_metadata_not_scan() {
    let mut report = VortexLocalPrimitiveExecutionReport::feature_disabled(
        VortexQueryPrimitiveKind::CountAll,
        VortexLocalPrimitiveResourceEnvelope::new(4, 1).expect("explicit fixture allocation"),
    );
    report.status = VortexLocalPrimitiveExecutionStatus::Executed;
    report.mode = VortexLocalPrimitiveExecutionMode::MetadataPreservingCount;
    report.upstream_scan_called = false;

    let proof_basis = local_primitive_pushdown_proof_basis(&report);

    assert!(proof_basis.contains("row-count metadata"));
    assert!(!proof_basis.contains("scan yielded arrays"));
}

#[test]
fn grouped_aggregate_missing_having_or_order_column_fails_closed() {
    let path = unique_vortex_path("grouped-aggregate-missing-having-order");
    write_pivot_struct_fixture(&path).expect("fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    let grouped_request = |aggregate: VortexSimpleAggregateRequest| {
        VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), aggregate)
    };

    let having_request = grouped_request(
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".to_string(),
            )],
        )
        .with_having(vec![crate::VortexAggregateHavingExpr::new(
            "missing_metric",
            ComparisonOp::Gt,
            "0",
        )]),
    );
    let having_error = execute_vortex_local_primitive(&having_request)
        .expect_err("missing HAVING column must fail closed");
    assert!(
        having_error
            .to_string()
            .contains("HAVING column 'missing_metric' was not present in the result row"),
        "{having_error:?}"
    );

    let order_request = grouped_request(
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".to_string(),
            )],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new(
            "missing_metric",
            true,
        )]),
    );
    let order_error = execute_vortex_local_primitive(&order_request)
        .expect_err("missing ORDER BY column must fail closed");
    let _ = std::fs::remove_file(&path);
    assert!(
        order_error
            .to_string()
            .contains("order column 'missing_metric' is not a group column or aggregate alias"),
        "{order_error:?}"
    );
}

#[test]
fn grouped_aggregate_mixed_predicate_preserves_pushdown_and_residual_boundary() {
    let path = unique_vortex_path("grouped-aggregate-mixed-predicate");
    write_mixed_string_filter_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("SearchPhrase").expect("column")],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "min",
                    Some(ColumnRef::new("URL").expect("column")),
                    "min_url".to_string(),
                ),
            ],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]),
    )
    .with_source_order_limit(10);
    request.predicate = Some(PredicateExpr::And(vec![
        PredicateExpr::StringContains {
            column: ColumnRef::new("URL").expect("column"),
            needle: "google".to_string(),
            negated: false,
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("SearchPhrase").expect("column"),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        },
    ]));

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    assert_eq!(payload["group_output_strategy"], "ordered_group_sort");
    let values = payload["values"].as_array().expect("values");
    assert_eq!(values.len(), 2);
    assert!(
        values
            .iter()
            .any(|row| row["SearchPhrase"] == serde_json::json!("alpha"))
    );
    assert!(
        values
            .iter()
            .any(|row| row["SearchPhrase"] == serde_json::json!("gamma"))
    );
    assert!(!summary.contains("\"SearchPhrase\":\"\""));
    assert!(!summary.contains("\"SearchPhrase\":\"beta\""));
}

#[test]
fn grouped_aggregate_recount_projection_preserves_residual_columns() {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let path = unique_vortex_path("grouped-aggregate-residual-recount-projection");
    write_mixed_string_filter_fixture(&path).expect("fixture");
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(
            session
                .open_options()
                .with_layout_reader_cache()
                .open_path(&path),
        )
        .expect("open fixture");
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("SearchPhrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let predicate = PredicateExpr::StringContains {
        column: ColumnRef::new("URL").expect("column"),
        needle: "google".to_string(),
        negated: false,
    };

    let aggregate_plan = rewrite_simple_aggregate_for_embedded_derived_columns(
        file.dtype(),
        &aggregate,
        Some(&predicate),
    )
    .expect("embedded rewrite");
    assert_eq!(aggregate_plan.rewritten_columns, [] as [String; 0]);
    let mut projected_columns = aggregate_plan.aggregate.projected_columns();
    let (pushdown_predicate, residual_predicate) =
        aggregate_plan
            .predicate
            .as_ref()
            .map_or((None, None), |predicate| {
                split_predicate_for_vortex_pushdown(
                    predicate,
                    VortexQueryPrimitiveKind::SimpleAggregate,
                )
            });
    assert!(pushdown_predicate.is_some());
    assert!(residual_predicate.is_none());
    // Contains is admitted upstream. Explicitly exercise the residual helper
    // contract so a future residual does not lose its columns during recount.
    append_predicate_columns(&predicate, &mut projected_columns);
    let first_projection = ProjectionRequest::columns(projected_columns);
    let first_plan = projection_scan_plan(
        file.dtype(),
        &first_projection,
        VortexQueryPrimitiveKind::SimpleAggregate,
    )
    .expect("first projection plan");
    assert_eq!(
        first_plan.projected_columns,
        vec!["SearchPhrase".to_string(), "URL".to_string()]
    );

    let aggregate_only_projection =
        ProjectionRequest::columns(aggregate_plan.aggregate.projected_columns());
    let aggregate_only_plan = projection_scan_plan(
        file.dtype(),
        &aggregate_only_projection,
        VortexQueryPrimitiveKind::SimpleAggregate,
    )
    .expect("aggregate-only projection plan");
    assert_eq!(aggregate_only_plan.projected_columns, vec!["SearchPhrase"]);
    assert_ne!(
        aggregate_only_plan.projected_columns,
        first_plan.projected_columns
    );

    let recount_projection =
        projection_request_from_declared_columns(&first_plan.projected_columns)
            .expect("recount projection");
    let recount_plan = projection_scan_plan(
        file.dtype(),
        &recount_projection,
        VortexQueryPrimitiveKind::SimpleAggregate,
    )
    .expect("recount projection plan");
    let _ = std::fs::remove_file(&path);

    assert_eq!(recount_plan.projected_columns, first_plan.projected_columns);
}

#[test]
fn grouped_aggregate_exact_fallback_applies_residual_row_filter() {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let path = unique_vortex_path("grouped-aggregate-exact-residual");
    write_mixed_string_filter_fixture(&path).expect("fixture");
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(
            session
                .open_options()
                .with_layout_reader_cache()
                .open_path(&path),
        )
        .expect("open fixture");
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("SearchPhrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["SearchPhrase".to_string(), "URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&aggregate, Some(10), &declared_columns, false, false)
            .expect("states");
    let predicate = PredicateExpr::StringContains {
        column: ColumnRef::new("URL").expect("column"),
        needle: "google".to_string(),
        negated: false,
    };
    let residual_evaluator = MaterializedPredicateEvaluator::compile(&predicate, &declared_columns)
        .expect("residual evaluator");
    let projection =
        projection_request_from_declared_columns(&declared_columns).expect("projection request");
    let mut plan = projection_scan_plan(
        file.dtype(),
        &projection,
        VortexQueryPrimitiveKind::SimpleAggregate,
    )
    .expect("projection plan");
    let mut scan = file.scan().expect("scan");
    if let Some(projection) = plan.projection.take() {
        scan = scan.with_projection(
            bind_vortex_scan_expr(&file, &projection).expect("projection should bind"),
        );
    }
    let mut chunks = scan.into_array_iter(&runtime).expect("array iterator");
    let chunk = chunks
        .next()
        .expect("fixture chunk")
        .expect("read fixture chunk");

    update_grouped_exact_states_from_chunk(
        &mut states,
        &chunk,
        &declared_columns,
        Some(&residual_evaluator),
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("residual exact update");
    let _ = std::fs::remove_file(&path);

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(10))
        .expect("result summary");
    let payload: serde_json::Value = serde_json::from_str(&summary).expect("state summary json");
    let values = payload["values"].as_array().expect("values");
    assert_eq!(row_count, 3);
    assert_eq!(values.len(), 3);
    assert!(
        values
            .iter()
            .any(|row| row["SearchPhrase"] == serde_json::json!("alpha"))
    );
    assert!(
        values
            .iter()
            .any(|row| row["SearchPhrase"] == serde_json::json!("gamma"))
    );
    assert!(!summary.contains("\"SearchPhrase\":\"beta\""));
}

#[test]
fn grouped_aggregate_in_list_uses_candidate_indices_before_row_export() {
    let path = unique_vortex_path("grouped-aggregate-in-list-candidates");
    write_pivot_struct_fixture(&path).expect("fixture");
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("amount").expect("column")),
                    "total_amount".to_string(),
                ),
            ],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]),
    )
    .with_source_order_limit(10);
    request.predicate = Some(PredicateExpr::InList {
        column: ColumnRef::new("id").expect("column"),
        values: vec![StatValue::UInt64(1)],
        negated: false,
    });

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(2));
    assert!(report.projection_pushdown_applied);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    assert_eq!(payload["rows"], serde_json::json!(1));
    assert_eq!(payload["group_output_strategy"], "ordered_group_sort");
    assert_eq!(
        payload["aggregate_update_strategy"],
        "dictionary_group_compact_count_sum_avg_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_compact_count_sum_avg_group_state"
    );
    let values = payload["values"].as_array().expect("values");
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["label"], serde_json::json!("paid"));
    assert_eq!(values[0]["rows"], serde_json::json!(2));
    assert_eq!(values[0]["total_amount"], serde_json::json!(15.0));
    assert!(!summary.contains("\"trial\""));
}

#[test]
fn grouped_aggregate_source_order_limit_avoids_full_group_sort_without_fallback() {
    let path = unique_vortex_path("grouped-aggregate-source-order-limit");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".to_string(),
            )],
        ),
    )
    .with_source_order_limit(1);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.source_order_limit_requested, Some(1));
    assert!(report.source_order_limit_applied);
    assert_eq!(report.source_order_limit_rows_output, Some(1));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(
        report
            .state_budget
            .state_family
            .starts_with("grouped_aggregate_state"),
        "{}",
        report.state_budget.state_family
    );
    assert!(
        report
            .state_budget
            .state_family
            .contains("count_star_direct"),
        "{}",
        report.state_budget.state_family
    );
    assert!(
        report
            .state_budget
            .state_family
            .contains("compact_group_state"),
        "{}",
        report.state_budget.state_family
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"count_star_direct_group_update".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"compact_count_star_group_state".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"source_order_group_admission_gate".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"existing_group_key_probe".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"row_materialization_bypass".to_string())
    );
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    assert_eq!(
        payload["group_output_strategy"],
        "source_order_limited_group_admission_no_sort"
    );
    assert_eq!(
        payload["group_admission_strategy"],
        "first_k_source_order_groups_then_existing_key_updates"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "count_star_direct_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "compact_count_star_group_state"
    );
    assert_eq!(
        payload["group_key_storage"],
        "typed_single_key+interned_utf8"
    );
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(1));
    assert_eq!(payload["decoded_string_count"], serde_json::json!(1));
    assert_eq!(payload["rows"], serde_json::json!(1));
    let values = payload["values"].as_array().expect("values");
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["label"], serde_json::json!("paid"));
    assert_eq!(values[0]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_aggregate_source_order_limit_caps_new_group_admission_without_losing_counts() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("UserID").expect("column"),
            ColumnRef::new("SearchPhrase").expect("column"),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    );
    let declared_columns = vec!["UserID".to_string(), "SearchPhrase".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let direct_accessors = vec![
        AggregateDirectColumnAccessor::UInt64(vec![1, 2, 3, 1, 4, 2]),
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 2, 0, 3, 1],
            values: vec![
                std::sync::Arc::<str>::from("alpha"),
                std::sync::Arc::<str>::from("beta"),
                std::sync::Arc::<str>::from("gamma"),
                std::sync::Arc::<str>::from("delta"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
    ];

    assert!(
        states
            .update_source_order_numeric_utf8_count_from_accessors(&direct_accessors, None)
            .expect("source-order numeric UTF8 count update")
    );

    let budget = states
        .state_budget_report(&request, 6, 2)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+count_star_direct+compact_group_state+source_order_numeric_utf8_dictionary_slots+chunk_dictionary_counts+source_order_group_admission_cap"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"source_order_group_admission_gate".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"source_order_numeric_utf8_dictionary_candidate_slots".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"source_order_candidate_count_merge".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["group_output_strategy"],
        "source_order_limited_group_admission_no_sort"
    );
    assert_eq!(
        payload["group_admission_strategy"],
        "first_k_source_order_groups_then_existing_key_updates"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "source_order_numeric_utf8_dictionary_code_count_star_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "source_order_numeric_utf8_dictionary_code_compact_count_star_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "source_order_numeric_utf8_dictionary_candidate_slots"
    );
    assert_eq!(
        payload["group_key_storage"],
        "source_order_retained_numeric_utf8_dictionary_code_keys"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(2));
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["UserID"], serde_json::json!(1));
    assert_eq!(rows[0]["SearchPhrase"], serde_json::json!("alpha"));
    assert_eq!(rows[0]["rows"], serde_json::json!(2));
    assert_eq!(rows[1]["UserID"], serde_json::json!(2));
    assert_eq!(rows[1]["SearchPhrase"], serde_json::json!("beta"));
    assert_eq!(rows[1]["rows"], serde_json::json!(2));
}

#[test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
fn partition_source_owned_pressure_replays_filtered_native_file_once_and_preserves_corruption() {
    use aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault};
    use vortex::{
        VortexSessionDefault as _,
        array::{
            IntoArray as _,
            arrays::{StructArray, VarBinViewArray},
            dtype::FieldNames,
            validity::Validity,
        },
        file::WriteOptionsSessionExt as _,
        io::{
            runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
            session::RuntimeSessionExt as _,
        },
        session::VortexSession,
    };
    let path = unique_vortex_path("partition-source-pressure-replay");
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let chunks = [
        ["tea", "tea", "skip", "東京"],
        ["coffee", "東京", "skip", "tea"],
        ["東京", "tea", "coffee", "skip"],
    ]
    .map(|values| {
        StructArray::try_new(
            FieldNames::from(["renamed_key"]),
            vec![VarBinViewArray::from_iter_str(values).into_array()],
            4,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    });
    let mut bytes = Vec::new();
    let mut writer = session
        .write_options()
        .with_strategy(native_flat_layout::SequentialNativeFlatLayout::strategy(
            chunks.len(),
        ))
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut bytes, chunks[0].dtype().clone());
    for chunk in chunks {
        writer.push(chunk).unwrap();
    }
    assert_eq!(writer.finish().unwrap().row_count(), 12);
    let workspace = shardloom_core::infer_local_output_workspace_root(&path).unwrap();
    shardloom_core::write_workspace_safe_bytes(
        workspace,
        &path,
        false,
        "partition source pressure fixture",
        &bytes,
    )
    .unwrap();
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("renamed_key").unwrap()],
            vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "n".into(),
            )],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("n", true)]),
    )
    .with_source_order_limit(2);
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("renamed_key").unwrap(),
        op: ComparisonOp::NotEq,
        value: StatValue::Utf8("skip".into()),
    });
    SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(Some(SourceScanTestFault::OwnedDenial)));
    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
    )
    .unwrap();
    assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
    let summary = simple_aggregate_values_json(report.result_summary.as_deref().unwrap());
    assert_eq!(
        summary["values"],
        serde_json::json!([{"renamed_key":"tea","n":4}, {"renamed_key":"東京","n":3}])
    );
    assert_eq!(summary["aggregate_workers_partition_source_replays"], 1);
    assert_eq!(
        summary["aggregate_provider_background_workers"],
        bounded_local_vortex_worker_count(report.resource_envelope.max_parallelism)
    );
    assert!(
        summary["aggregate_provider_cpu_scope"]
            .as_str()
            .unwrap()
            .contains("aggregate_workers_cancelled_and_joined;temporary_provider_drivers_during_native_replay;no_concurrent_aggregate_worker_pool")
    );
    assert!(
        summary["aggregate_workers_partition_discarded_input_rows"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(!report.fallback_execution_allowed);
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"complete_key_partition_source_pressure_native_replay".to_owned())
    );
    SOURCE_SCAN_TEST_FAULT
        .with(|fault| fault.set(Some(SourceScanTestFault::CorruptionWithConcurrentDenial)));
    let error = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("injected source corruption"));
    assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
    std::fs::remove_file(path).unwrap();
}

#[test]
fn grouped_count_star_ordered_topk_uses_compact_counts_without_fallback() {
    let path = unique_vortex_path("grouped-count-star-ordered-compact");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".to_string(),
            )],
        )
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]),
    )
    .with_source_order_limit(1);

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert_eq!(
        report.state_budget.state_family,
        if cfg!(unix) {
            "grouped_aggregate_state+topk+count_star_direct+compact_group_state"
        } else {
            "grouped_aggregate_state+topk+count_star_direct+compact_group_state+chunk_dictionary_counts"
        }
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"compact_group_state_memory".to_string())
    );
    assert!(
        report.state_budget.capillary_work_units.contains(
            &if cfg!(unix) {
                "count_star_direct_group_update"
            } else {
                "chunk_dictionary_count_star_group_update"
            }
            .to_string()
        )
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"streaming_count_star_topk_retention".to_string())
    );
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    assert_eq!(
        payload["group_output_strategy"],
        if cfg!(unix) {
            "complete_key_partition_exact_topk"
        } else {
            "capillary_streaming_count_star_topk"
        }
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        if cfg!(unix) {
            "compact_count_star_group_state"
        } else {
            "chunk_dictionary_count_star_group_state"
        }
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        if cfg!(unix) {
            "count_star_direct_group_update"
        } else {
            "chunk_dictionary_count_star_group_update"
        }
    );
    assert_eq!(
        payload["group_key_storage"],
        if cfg!(unix) {
            "owned_utf8_complete_key_partitions"
        } else {
            "typed_single_key+interned_utf8"
        }
    );
    assert_eq!(
        payload["source_order_key_retention"],
        "ordered_route_source_order_keys_elided"
    );
    assert_eq!(
        payload["group_key_comparison_strategy"],
        "typed_count_star_key_topk"
    );
    assert_eq!(
        payload["topk_candidate_key_clone_strategy"],
        "clone_only_surviving_retained_candidates"
    );
    assert_eq!(
        payload["evicted_or_spilled_group_count"],
        serde_json::json!(0)
    );
    assert_eq!(payload["spill_state"], "not_spilled");
    assert_eq!(
        payload["uniqueness_proof_status"],
        "not_required_count_star_exact_state"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(2));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(1));
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    #[cfg(unix)]
    {
        assert_eq!(payload["aggregate_workers_rows"], 3);
        assert_eq!(payload["aggregate_workers_submitted_chunks"], 1);
        assert_eq!(payload["aggregate_workers_completed_chunks"], 1);
        assert_eq!(payload["aggregate_workers_partial_entries"], 2);
        assert_eq!(payload["aggregate_workers_outstanding_chunks"], 0);
        assert_eq!(payload["aggregate_workers_compute_threads"], 0);
        assert_eq!(payload["aggregate_workers_provider_background_workers"], 0);
        assert!(
            payload["aggregate_workers_count_work_nanos"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            payload["aggregate_workers_caller_merge_nanos"]
                .as_u64()
                .unwrap()
                > 0
        );
    }
    let values = payload["values"].as_array().expect("values");
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["label"], serde_json::json!("paid"));
    assert_eq!(values[0]["rows"], serde_json::json!(2));
}

#[test]
fn grouped_count_star_offset_topk_uses_select_nth_retention() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("label").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)])
    .with_offset(150);
    let declared_columns = vec!["label".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(10), &declared_columns, false, false)
            .expect("states");
    let values = (0..200)
        .map(|index| std::sync::Arc::<str>::from(format!("label-{index:03}")))
        .collect::<Vec<_>>();
    let row_ids = (0..200).map(|index| index as u32).collect::<Vec<_>>();
    let direct_accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids,
        values: values.into_iter().map(Into::into).collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    for row_index in 0..200 {
        states
            .update_count_star_direct_row(&direct_accessors, row_index)
            .expect("count-star row update");
    }
    states.count_star_direct_updates = true;

    let budget = states
        .state_budget_report(&request, 200, 10)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"select_nth_count_star_topk_retention".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"select_nth_count_star_retained_candidates".to_string())
    );

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(10))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 10);
    assert_eq!(
        payload["group_output_strategy"],
        "capillary_select_nth_count_star_topk"
    );
    assert_eq!(
        payload["group_key_comparison_strategy"],
        "typed_count_star_key_select_nth_topk"
    );
    assert_eq!(
        payload["topk_candidate_key_clone_strategy"],
        "bulk_candidate_keys_for_select_nth"
    );
    assert_eq!(payload["candidate_groups"], serde_json::json!(200));
    assert_eq!(payload["retained_candidate_groups"], serde_json::json!(160));
    assert_eq!(payload["offset"], serde_json::json!(150));
    assert_eq!(payload["values"].as_array().expect("values").len(), 10);
}

#[test]
fn grouped_count_star_chunk_dictionary_updates_once_per_chunk_value() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("URL").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(1), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 1, 0, 1],
        values: vec![
            std::sync::Arc::<str>::from("http://example.test/a"),
            std::sync::Arc::<str>::from("http://example.test/b"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    assert!(
        states
            .update_count_star_direct_from_chunk_dictionary(&accessors, None)
            .expect("chunk dictionary update")
    );
    states.count_star_direct_updates = true;
    states.chunk_dictionary_direct_updates = true;
    assert_eq!(states.group_order.len(), 0);

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_count_star_group_state"
    );
    assert_eq!(payload["group_state_mode"], "chunk_dictionary_code_map");
    assert_eq!(
        payload["source_order_key_retention"],
        "ordered_route_source_order_keys_elided"
    );
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["URL"], serde_json::json!("http://example.test/a"));
    assert_eq!(rows[0]["c"], serde_json::json!(3));
    assert_eq!(rows[1]["URL"], serde_json::json!("http://example.test/b"));
    assert_eq!(rows[1]["c"], serde_json::json!(2));
}

#[test]
fn grouped_count_star_masked_chunk_dictionary_updates_selected_values_once() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("URL").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0, 1, 0, 1],
        values: vec![
            std::sync::Arc::<str>::from("http://example.test/a"),
            std::sync::Arc::<str>::from("http://example.test/b"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    assert!(
        states
            .update_count_star_direct_from_chunk_dictionary(&accessors, Some(&[1, 2, 4]))
            .expect("masked chunk dictionary update")
    );
    states.count_star_direct_updates = true;
    states.chunk_dictionary_direct_updates = true;

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_count_star_group_state"
    );
    assert_eq!(payload["group_state_mode"], "chunk_dictionary_code_map");
    assert_eq!(payload["candidate_groups"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["URL"], serde_json::json!("http://example.test/b"));
    assert_eq!(rows[0]["c"], serde_json::json!(2));
    assert_eq!(rows[1]["URL"], serde_json::json!("http://example.test/a"));
    assert_eq!(rows[1]["c"], serde_json::json!(1));
}

#[test]
fn grouped_count_star_vortex_dict_array_child_uses_dictionary_accessor() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("URL").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["URL".to_string()];
    let url_codes = [0_u8, 0, 1, 0, 1]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let url_values =
        VarBinViewArray::from_iter_str(["http://example.test/a", "http://example.test/b"])
            .into_array();
    let url = DictArray::try_new(url_codes, url_values)
        .expect("URL dictionary")
        .into_array();
    let chunk = StructArray::try_new(
        FieldNames::from(["URL"]),
        vec![url],
        5,
        Validity::NonNullable,
    )
    .expect("struct chunk")
    .into_array();
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");

    assert!(
        states
            .update_count_star_direct_from_chunk(&chunk, &declared_columns, None)
            .expect("dictionary chunk update")
    );

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "chunk_dictionary_count_star_group_update"
    );
    assert_eq!(
        payload["aggregate_accessor_summary"],
        "URL:vortex_utf8_dictionary"
    );
    assert_eq!(
        payload["aggregate_accessor_materialization_status"],
        "vortex_dictionary_or_primitive_only"
    );
    assert_eq!(
        payload["aggregate_vortex_dictionary_accessor_columns"],
        "URL"
    );
    assert_eq!(payload["aggregate_materialized_accessor_columns"], "none");
    assert_eq!(payload["aggregate_accessor_blockers"], "none");
    assert_eq!(payload["group_state_mode"], "chunk_dictionary_code_map");
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["URL"], serde_json::json!("http://example.test/a"));
    assert_eq!(rows[0]["c"], serde_json::json!(3));
    assert_eq!(rows[1]["URL"], serde_json::json!("http://example.test/b"));
    assert_eq!(rows[1]["c"], serde_json::json!(2));
}

#[test]
fn aggregate_accessor_uses_utf8_chunk_dictionary_before_materialized_rows() {
    use vortex::array::IntoArray as _;
    use vortex::array::VortexSessionExecute as _;

    use vortex::array::arrays::varbin::builder::VarBinBuilder;
    use vortex::array::dtype::{DType, Nullability};
    use vortex::encodings::fsst::{fsst_compress, fsst_train_compressor};

    let mut input = VarBinBuilder::<i32>::with_capacity_in(
        DType::Utf8(Nullability::NonNullable),
        5,
        &vortex::buffer::BufferAllocatorRef::statically_allocated(),
    );
    input.append_value(b"http://example.test/a");
    input.append_value(b"http://example.test/b");
    input.append_value(b"http://example.test/a");
    input.append_value(b"http://example.test/c");
    input.append_value(b"http://example.test/b");
    let input = input.finish_into_varbin().into_array();
    let mut ctx = vortex::session::VortexSession::empty().create_execution_ctx();
    let compressor = fsst_train_compressor(&input, &mut ctx).expect("train FSST compressor");
    let compressed = fsst_compress(&input, &compressor, &mut ctx)
        .expect("compress FSST array")
        .into_array();

    let accessor = aggregate_direct_column_accessor(
        "URL",
        &compressed,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("UTF-8 aggregate accessor");
    let AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids,
        values,
        source,
        ..
    } = accessor
    else {
        panic!("UTF-8 should use a chunk dictionary accessor");
    };

    assert!(
        matches!(
            source.evidence_kind(),
            "chunk_utf8_dictionary" | "decoded_utf8_chunk_dictionary"
        ),
        "unexpected UTF-8 chunk dictionary evidence kind {}",
        source.evidence_kind()
    );
    assert_eq!(row_ids, vec![0, 1, 0, 2, 1]);
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_ref(), "http://example.test/a");
    assert_eq!(values[1].as_ref(), "http://example.test/b");
    assert_eq!(values[2].as_ref(), "http://example.test/c");
}

#[test]
fn fsst_string_contains_uses_vortex_like_kernel_without_materialized_rows() {
    use vortex::array::IntoArray as _;
    use vortex::array::VortexSessionExecute as _;

    use vortex::array::arrays::varbin::builder::VarBinBuilder;
    use vortex::array::dtype::{DType, Nullability};
    use vortex::encodings::fsst::{fsst_compress, fsst_train_compressor};
    use vortex::mask::Mask;

    let mut input = VarBinBuilder::<i32>::with_capacity_in(
        DType::Utf8(Nullability::NonNullable),
        5,
        &vortex::buffer::BufferAllocatorRef::statically_allocated(),
    );
    input.append_value(b"https://google.example/a");
    input.append_value(b"https://maps.example/b");
    input.append_value(b"https://mail.google.example/c");
    input.append_value(b"https://example.test/d");
    input.append_value(b"https://google.example/e");
    let input = input.finish_into_varbin().into_array();
    let mut ctx = vortex::session::VortexSession::empty().create_execution_ctx();
    let compressor = fsst_train_compressor(&input, &mut ctx).expect("train FSST compressor");
    let compressed = fsst_compress(&input, &compressor, &mut ctx)
        .expect("compress FSST array")
        .into_array();

    assert!(compressed.is::<vortex::encodings::fsst::FSST>());
    assert_eq!(
        fast_utf8_contains_count_from_array(
            &compressed,
            "google",
            false,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("contains count"),
        Some(3)
    );
    assert_eq!(
        fast_utf8_contains_row_indices_from_array(
            &compressed,
            "google",
            false,
            0,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("contains row indices"),
        Some(vec![0, 2, 4])
    );
    assert_eq!(
        fast_utf8_contains_count_from_array(
            &compressed,
            "google",
            true,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("negated contains count"),
        Some(2)
    );

    let all_true_mask = Mask::new_true(compressed.len());
    assert_eq!(
        fast_utf8_contains_count_from_masked_fsst_array(
            &compressed,
            &all_true_mask,
            "google",
            false,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("all-true masked FSST count"),
        Some(3)
    );
    assert_eq!(
        fast_utf8_contains_row_indices_from_masked_fsst_array(
            &compressed,
            &all_true_mask,
            "google",
            false,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("all-true masked FSST row indices"),
        Some(vec![0, 2, 4])
    );

    let all_false_mask = Mask::new_false(compressed.len());
    assert_eq!(
        fast_utf8_contains_count_from_masked_fsst_array(
            &compressed,
            &all_false_mask,
            "google",
            false,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("all-false masked FSST count"),
        Some(0)
    );
    assert_eq!(
        fast_utf8_contains_row_indices_from_masked_fsst_array(
            &compressed,
            &all_false_mask,
            "google",
            false,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("all-false masked FSST row indices"),
        Some(Vec::<usize>::new())
    );
}

#[test]
fn aggregate_accessor_keeps_nullable_utf8_chunk_dictionary_direct() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::VarBinViewArray;

    let input =
        VarBinViewArray::from_iter_nullable_str([Some("alpha"), None, Some("alpha"), Some("beta")])
            .into_array();
    let accessor = aggregate_direct_column_accessor(
        "SearchPhrase",
        &input,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("UTF-8 accessor");
    let AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids,
        values,
        row_nulls,
        source,
        ..
    } = accessor
    else {
        panic!("nullable UTF-8 chunk should use null-aware chunk dictionary accessor");
    };
    assert!(
        matches!(
            source.evidence_kind(),
            "chunk_utf8_dictionary" | "decoded_utf8_chunk_dictionary"
        ),
        "unexpected UTF-8 chunk dictionary evidence kind {}",
        source.evidence_kind()
    );
    assert_eq!(row_ids, vec![0, 0, 0, 1]);
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_ref(), "alpha");
    assert_eq!(values[1].as_ref(), "beta");
    assert_eq!(
        row_nulls.expect("row nulls"),
        vec![false, true, false, false]
    );
}

#[test]
fn aggregate_utf8_chunk_dictionary_handles_empty_and_all_null_inputs() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::VarBinViewArray;

    for input_values in [vec![], vec![None::<&str>, None, None]] {
        let len = input_values.len();
        let input = VarBinViewArray::from_iter_nullable_str(input_values).into_array();
        let accessor = aggregate_direct_utf8_chunk_dictionary_accessor(
            "text",
            &input,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("accessor")
        .expect("UTF8");
        let AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids,
            values,
            row_nulls,
            ..
        } = accessor
        else {
            panic!("expected chunk dictionary");
        };
        assert_eq!(row_ids, vec![0; len]);
        assert!(values.is_empty());
        assert_eq!(row_nulls, (len != 0).then(|| vec![true; len]));
    }
}

#[test]
fn transformed_chunk_dictionary_retains_min_after_input_and_accessor_drop() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::VarBinViewArray;

    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".into()),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("Referer").expect("column")),
                "m".into(),
            ),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "k".into(),
        ColumnRef::new("Referer").expect("column"),
        "url_domain",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let columns = vec!["Referer".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(1), &columns, false, false).expect("states");
    let input = VarBinViewArray::from_iter_nullable_str([
        Some("http://example.test/z"),
        None,
        Some("http://example.test/a"),
        Some("http://example.test/a"),
        Some("http://ignored.test/a"),
    ])
    .into_array();
    let accessor = aggregate_direct_utf8_chunk_dictionary_accessor(
        "Referer",
        &input,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("accessor")
    .expect("UTF8");
    drop(input);
    let accessors = vec![accessor];
    assert!(
        states
            .update_dense_general_direct_from_transformed_dictionary(&accessors, Some(&[0, 2, 3]),)
            .expect("selected update")
    );
    drop(accessors);
    let (_, summary) = states
        .result_row_count_and_summary(Some(1))
        .expect("result");
    let payload: serde_json::Value = serde_json::from_str(&summary).expect("JSON");
    assert_eq!(
        payload["values"],
        serde_json::json!([
            {"k": "example.test", "c": 3, "m": "http://example.test/a"}
        ])
    );
}

#[test]
fn grouped_aggregate_nullable_utf8_dictionary_stays_direct_without_materialization() {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, VarBinViewArray};

    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("SearchPhrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("rows", true)]);
    let declared_columns = vec!["SearchPhrase".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let codes = [0_u8, 1, 0]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values =
        VarBinViewArray::from_iter_nullable_str([Some("alpha"), None, Some("unused")]).into_array();
    let chunk = DictArray::try_new(codes, values)
        .expect("nullable utf8 dictionary array")
        .into_array();

    assert!(
        states
            .update_count_star_direct_from_chunk(&chunk, &declared_columns, None)
            .expect("direct nullable dictionary aggregate update")
    );

    let (_row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(
        payload["aggregate_accessor_summary"],
        "SearchPhrase:vortex_utf8_dictionary"
    );
    assert_eq!(
        payload["aggregate_accessor_materialization_status"],
        "vortex_dictionary_or_primitive_only"
    );
    assert_eq!(payload["aggregate_accessor_blockers"], "none");
    assert_eq!(payload["aggregate_materialized_accessor_columns"], "none");
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_count_star_group_state"
    );
    let values = payload["values"].as_array().expect("values");
    assert_eq!(values[0]["SearchPhrase"], serde_json::json!("alpha"));
    assert_eq!(values[0]["rows"], serde_json::json!(2));
    assert!(values[1]["SearchPhrase"].is_null());
    assert_eq!(values[1]["rows"], serde_json::json!(1));
}

#[test]
fn aggregate_layout_correlation_reports_artifact_dictionary_materialization_gap() {
    let mut summary = serde_json::json!({
        "rows": 2,
        "group_by": "SearchPhrase",
        "aggregate_accessor_summary": "SearchPhrase:materialized_stat_values",
        "aggregate_materialized_accessor_columns": "SearchPhrase",
        "aggregate_accessor_blockers": "SearchPhrase:cg21.aggregate_accessor.vortex_dictionary_provider_miss",
    })
    .to_string();
    let mut embedded_layout = VortexLocalPrimitiveEmbeddedLayoutReport::not_available();
    embedded_layout.metadata_persisted_in_artifact = true;
    embedded_layout.domain_dictionary_status = "vortex_dictionary_layout_present".to_string();
    embedded_layout.layout_encoding_inventory = "chunked,dict".to_string();

    annotate_simple_aggregate_layout_correlation_summary(&mut summary, &embedded_layout)
        .expect("layout/accessor correlation annotation");

    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("layout correlation summary json");
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_status"],
        "artifact_dictionary_layout_present_accessor_materialized"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_columns"],
        "SearchPhrase"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_blockers"],
        "SearchPhrase:cg21.aggregate_accessor.vortex_dictionary_provider_miss"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_dictionary_status"],
        "vortex_dictionary_layout_present"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_encoding_inventory"],
        "chunked,dict"
    );
}

#[test]
fn aggregate_layout_correlation_reports_string_encoding_materialization_gap() {
    let mut summary = serde_json::json!({
        "rows": 2,
        "group_by": "URL",
        "aggregate_accessor_summary": "URL:materialized_stat_values",
        "aggregate_materialized_accessor_columns": "URL",
        "aggregate_accessor_blockers": "URL:cg21.aggregate_accessor.utf8_chunk_dictionary_provider_miss",
    })
    .to_string();
    let mut embedded_layout = VortexLocalPrimitiveEmbeddedLayoutReport::not_available();
    embedded_layout.metadata_persisted_in_artifact = true;
    embedded_layout.domain_dictionary_status =
        "utf8_domain_columns_present_dictionary_layout_not_observed".to_string();
    embedded_layout.layout_encoding_inventory = "chunked,fsst".to_string();

    annotate_simple_aggregate_layout_correlation_summary(&mut summary, &embedded_layout)
        .expect("string layout/accessor correlation annotation");

    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("layout correlation summary json");
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_status"],
        "artifact_string_encoding_layout_present_accessor_materialized"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_columns"],
        "URL"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_blockers"],
        "URL:cg21.aggregate_accessor.utf8_chunk_dictionary_provider_miss"
    );
}

#[test]
fn aggregate_layout_correlation_reports_complete_no_materialized_contract() {
    let mut summary = serde_json::json!({
        "rows": 2,
        "group_by": "URL",
        "aggregate_accessor_summary": "URL:vortex_utf8_dictionary",
        "aggregate_materialized_accessor_columns": "none",
        "aggregate_accessor_blockers": "none",
    })
    .to_string();
    let mut embedded_layout = VortexLocalPrimitiveEmbeddedLayoutReport::not_available();
    embedded_layout.domain_dictionary_status = "vortex_dictionary_layout_present".to_string();
    embedded_layout.layout_encoding_inventory = "chunked,dict".to_string();

    annotate_simple_aggregate_layout_correlation_summary(&mut summary, &embedded_layout)
        .expect("no-materialized layout/accessor correlation annotation");

    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("layout correlation summary json");
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_status"],
        "not_required_no_materialized_accessors"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_columns"],
        "none"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_blockers"],
        "none"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_dictionary_status"],
        "vortex_dictionary_layout_present"
    );
    assert_eq!(
        payload["aggregate_accessor_layout_correlation_encoding_inventory"],
        "chunked,dict"
    );
}

#[test]
fn grouped_count_star_transformed_dictionary_reuses_group_transform_per_dictionary_value() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("URL").expect("column"),
        "url_domain",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 2, 0, 1, 2],
        values: vec![
            std::sync::Arc::<str>::from("http://example.test/a"),
            std::sync::Arc::<str>::from("https://other.test/b"),
            std::sync::Arc::<str>::from("http://example.test/c"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    assert!(
        states
            .update_count_star_direct_from_transformed_dictionary(&accessors, None)
            .expect("transformed dictionary update")
    );
    assert!(
        states
            .update_count_star_direct_from_transformed_dictionary(&accessors, None)
            .expect("transformed dictionary repeated update")
    );
    states.count_star_direct_updates = true;
    states.chunk_dictionary_direct_updates = true;
    states.transformed_dictionary_direct_updates = true;

    let budget = states
        .state_budget_report(&request, 5, 2)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+compact_group_state+chunk_dictionary_counts+transformed"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"transformed_dictionary_group_key_cache".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"transformed_dictionary_transform_code_map".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_transform_reuse_by_dictionary_id".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"transform_code_map_cache_hits".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "transformed_dictionary_count_star_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_transformed_count_star_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "transformed_chunk_dictionary_code_map"
    );
    assert_eq!(
        payload["transformed_dictionary_transform_code_map_status"],
        "bounded_transform_key_cache_reused"
    );
    assert_eq!(
        payload["transformed_dictionary_transform_code_map_entries"],
        serde_json::json!(3)
    );
    assert_eq!(
        payload["transformed_dictionary_transform_code_map_hits"],
        serde_json::json!(3)
    );
    assert_eq!(
        payload["transformed_dictionary_transform_code_map_misses"],
        serde_json::json!(3)
    );
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["domain"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(8));
    assert_eq!(rows[1]["domain"], serde_json::json!("other.test"));
    assert_eq!(rows[1]["c"], serde_json::json!(2));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn aggregate_numeric_dictionary_provider_skips_utf8_dictionary_values_without_panic() {
    use vortex::array::IntoArray as _;
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::{DictArray, PrimitiveArray, VarBinViewArray};

    let codes = [0_u8, 1, 0]
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array();
    let values = VarBinViewArray::from_iter_str(["https://example.test/a", ""]).into_array();
    let dictionary = DictArray::try_new(codes, values)
        .expect("UTF-8 dictionary")
        .into_array();

    assert!(
        aggregate_direct_numeric_dictionary_accessor(
            &dictionary,
            &mut vortex::session::VortexSession::empty().create_execution_ctx(),
        )
        .expect("numeric provider should return a miss, not panic")
        .is_none()
    );
    assert!(matches!(
        aggregate_direct_utf8_chunk_dictionary_accessor(
            "URL",
            &dictionary,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("UTF-8 dictionary provider should accept dictionary values"),
        Some(AggregateDirectColumnAccessor::Utf8Dictionary { .. })
    ));
}

#[test]
fn grouped_count_star_masked_transformed_dictionary_reuses_selected_values() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("URL").expect("column"),
        "url_domain",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 2, 0, 1, 2],
        values: vec![
            std::sync::Arc::<str>::from("http://example.test/a"),
            std::sync::Arc::<str>::from("https://other.test/b"),
            std::sync::Arc::<str>::from("http://example.test/c"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    assert!(
        states
            .update_count_star_direct_from_transformed_dictionary(&accessors, Some(&[1, 3, 4]))
            .expect("masked transformed dictionary update")
    );
    states.count_star_direct_updates = true;
    states.chunk_dictionary_direct_updates = true;
    states.transformed_dictionary_direct_updates = true;

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_transformed_count_star_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "transformed_chunk_dictionary_code_map"
    );
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["domain"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(2));
    assert_eq!(rows[1]["domain"], serde_json::json!("other.test"));
    assert_eq!(rows[1]["c"], serde_json::json!(1));
}

#[test]
fn grouped_count_star_transformed_dictionary_applies_count_having_after_merge() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("URL").expect("column"),
        "url_domain",
    )])
    .with_having(vec![crate::VortexAggregateHavingExpr::new(
        "c",
        ComparisonOp::GtEq,
        "2",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 2, 0, 1, 2],
        values: vec![
            std::sync::Arc::<str>::from("http://example.test/a"),
            std::sync::Arc::<str>::from("https://other.test/b"),
            std::sync::Arc::<str>::from("http://example.test/c"),
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    assert!(
        states
            .update_count_star_direct_from_transformed_dictionary(&accessors, None)
            .expect("transformed dictionary count update")
    );
    states.count_star_direct_updates = true;
    states.chunk_dictionary_direct_updates = true;
    states.transformed_dictionary_direct_updates = true;

    let budget = states
        .state_budget_report(&request, 5, 1)
        .expect("state budget");
    assert!(!budget.state_family.contains("count_having_prefilter"));
    assert!(
        !budget
            .capillary_work_units
            .contains(&"count_having_preupdate_group_filter".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 1);
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["domain"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(4));
}

#[test]
fn grouped_compact_measures_transformed_dictionary_reuses_group_and_measure_dictionaries() {
    assert!(transformed_dictionary_code_pair_partial_admitted(4, 9));
    assert!(transformed_dictionary_code_pair_partial_admitted(
        100_000_000,
        100_000
    ));
    assert!(!transformed_dictionary_code_pair_partial_admitted(
        100_000_000,
        10_000_000
    ));

    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("URL").expect("column")),
                "l".to_string(),
            )
            .with_value_transform("length"),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "k".to_string(),
        ColumnRef::new("Referer").expect("column"),
        "url_domain",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["Referer".to_string(), "URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 0, 2],
            values: vec![
                std::sync::Arc::<str>::from("http://example.test/a"),
                std::sync::Arc::<str>::from("https://other.test/b"),
                std::sync::Arc::<str>::from("www.example.test/c"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 2, 0],
            values: vec![
                std::sync::Arc::<str>::from("a"),
                std::sync::Arc::<str>::from("abcd"),
                std::sync::Arc::<str>::from("abcdefgh"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
    ];

    assert!(
        states
            .update_compact_direct_from_transformed_dictionary(&accessors, None)
            .expect("transformed dictionary compact update")
    );

    let budget = states
        .state_budget_report(&request, 4, 2)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+chunk_dictionary_transformed_compact_measures+code_pair_partials+compact_numeric_measures"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"transformed_dictionary_compact_measure_update".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"transformed_dictionary_code_pair_partial".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"dictionary_measure_transform_reuse".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"dictionary_pair_state_reuse".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "transformed_dictionary_compact_code_pair_partial_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_transformed_compact_code_pair_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "transformed_chunk_dictionary_compact_code_pair_map"
    );
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["k"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(3));
    assert_eq!(rows[0]["l"], serde_json::json!(10.0 / 3.0));
    assert_eq!(rows[1]["k"], serde_json::json!("other.test"));
    assert_eq!(rows[1]["c"], serde_json::json!(1));
    assert_eq!(rows[1]["l"], serde_json::json!(4.0));
}

#[test]
fn grouped_compact_measures_embedded_dictionary_group_reuses_group_codes() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("__shardloom_derived_utf8_len_URL").expect("hidden length")),
                "l".to_string(),
            ),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("__shardloom_derived_url_domain_Referer").expect("hidden domain"),
        "identity",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec![
        "__shardloom_derived_url_domain_Referer".to_string(),
        "__shardloom_derived_utf8_len_URL".to_string(),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 0, 2],
            values: vec![
                std::sync::Arc::<str>::from("example.test"),
                std::sync::Arc::<str>::from("other.test"),
                std::sync::Arc::<str>::from("example.test"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 4, 8, 1]),
    ];

    assert!(
        states
            .update_compact_direct_from_dictionary_group(&accessors, None)
            .expect("embedded dictionary compact update")
    );

    let budget = states
        .state_budget_report(&request, 4, 2)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+chunk_dictionary_compact_measures+compact_numeric_measures"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_group_compact_measure_update".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_code_group_measure_merge".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"dictionary_group_code_reuse".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "dictionary_group_compact_count_sum_avg_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_compact_count_sum_avg_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "chunk_dictionary_compact_measure_code_map"
    );
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["domain"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(3));
    assert_eq!(rows[0]["l"], serde_json::json!(10.0 / 3.0));
    assert_eq!(rows[1]["domain"], serde_json::json!("other.test"));
    assert_eq!(rows[1]["c"], serde_json::json!(1));
    assert_eq!(rows[1]["l"], serde_json::json!(4.0));
}

#[test]
fn grouped_compact_measures_embedded_dictionary_group_applies_count_having_after_merge() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("__shardloom_derived_utf8_len_URL").expect("hidden length")),
                "l".to_string(),
            ),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("__shardloom_derived_url_domain_Referer").expect("hidden domain"),
        "identity",
    )])
    .with_having(vec![crate::VortexAggregateHavingExpr::new(
        "c",
        ComparisonOp::GtEq,
        "2",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec![
        "__shardloom_derived_url_domain_Referer".to_string(),
        "__shardloom_derived_utf8_len_URL".to_string(),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 0, 2],
            values: vec![
                std::sync::Arc::<str>::from("example.test"),
                std::sync::Arc::<str>::from("other.test"),
                std::sync::Arc::<str>::from("docs.test"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
        AggregateDirectColumnAccessor::UInt64(vec![1, 4, 8, 100]),
    ];

    assert!(
        states
            .update_compact_direct_from_dictionary_group(&accessors, None)
            .expect("embedded dictionary compact update")
    );

    let budget = states
        .state_budget_report(&request, 4, 1)
        .expect("state budget");
    assert!(!budget.state_family.contains("count_having_prefilter"));
    assert!(
        !budget
            .capillary_work_units
            .contains(&"count_having_preupdate_group_filter".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 1);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "dictionary_group_compact_count_sum_avg_group_update"
    );
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["domain"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(2));
    assert_eq!(rows[0]["l"], serde_json::json!(4.5));
}

#[test]
fn grouped_compact_measures_embedded_dictionary_group_having_survives_chunk_split() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("__shardloom_derived_utf8_len_URL").expect("hidden length")),
                "l".to_string(),
            ),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("__shardloom_derived_url_domain_Referer").expect("hidden domain"),
        "identity",
    )])
    .with_having(vec![crate::VortexAggregateHavingExpr::new(
        "c",
        ComparisonOp::GtEq,
        "2",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec![
        "__shardloom_derived_url_domain_Referer".to_string(),
        "__shardloom_derived_utf8_len_URL".to_string(),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    for length in [1_u64, 3_u64] {
        let accessors = vec![
            AggregateDirectColumnAccessor::Utf8Dictionary {
                row_ids: vec![0],
                values: vec![std::sync::Arc::<str>::from("example.test")]
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                value_nulls: None,
                row_nulls: None,
                source: AggregateUtf8DictionarySource::VortexDictArray,
            },
            AggregateDirectColumnAccessor::UInt64(vec![length]),
        ];
        assert!(
            states
                .update_compact_direct_from_dictionary_group(&accessors, None)
                .expect("embedded dictionary compact update")
        );
    }

    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 1);
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["domain"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(2));
    assert_eq!(rows[0]["l"], serde_json::json!(2.0));
}

#[test]
fn grouped_compact_measures_transformed_dictionary_applies_count_having_after_merge() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("URL").expect("column")),
                "l".to_string(),
            )
            .with_value_transform("length"),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "k".to_string(),
        ColumnRef::new("Referer").expect("column"),
        "url_domain",
    )])
    .with_having(vec![crate::VortexAggregateHavingExpr::new(
        "c",
        ComparisonOp::GtEq,
        "2",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["Referer".to_string(), "URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 0, 2],
            values: vec![
                std::sync::Arc::<str>::from("http://example.test/a"),
                std::sync::Arc::<str>::from("https://other.test/b"),
                std::sync::Arc::<str>::from("www.example.test/c"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: vec![0, 1, 2, 0],
            values: vec![
                std::sync::Arc::<str>::from("a"),
                std::sync::Arc::<str>::from("abcd"),
                std::sync::Arc::<str>::from("abcdefgh"),
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::VortexDictArray,
        },
    ];

    assert!(
        states
            .update_compact_direct_from_transformed_dictionary(&accessors, None)
            .expect("transformed dictionary compact update")
    );

    let budget = states
        .state_budget_report(&request, 4, 1)
        .expect("state budget");
    assert!(!budget.state_family.contains("count_having_prefilter"));
    assert!(
        !budget
            .capillary_work_units
            .contains(&"count_having_preupdate_group_filter".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 1);
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["k"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(3));
    assert_eq!(rows[0]["l"], serde_json::json!(10.0 / 3.0));
}

#[test]
fn grouped_general_measures_transformed_dictionary_reuses_selected_dictionary_counts() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("Referer").expect("column")),
                "l".to_string(),
            )
            .with_value_transform("length"),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("Referer").expect("column")),
                "s".to_string(),
            )
            .with_value_transform("length"),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("Referer").expect("column")),
                "m".to_string(),
            ),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "k".to_string(),
        ColumnRef::new("Referer").expect("column"),
        "url_domain",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["Referer".to_string()];
    let values = [
        std::sync::Arc::<str>::from("http://example.test/z"),
        std::sync::Arc::<str>::from("https://other.test/b"),
        std::sync::Arc::<str>::from("http://example.test/a"),
        std::sync::Arc::<str>::from("www.example.test/q"),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 2, 0, 1, 2, 3],
        values: values.iter().cloned().map(Into::into).collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    assert!(
        states
            .update_general_direct_from_transformed_dictionary(&accessors, Some(&[0, 2, 3, 4, 5]),)
            .expect("transformed dictionary general update")
    );

    let budget = states
        .state_budget_report(&request, 5, 2)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+chunk_dictionary_transformed_general_measures+expression_fusion"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"transformed_dictionary_general_measure_update".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"dictionary_weighted_transform_fusion".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"dictionary_group_transform_reuse".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"repeated_string_transform_evaluation_bypass".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "transformed_dictionary_general_measure_group_update"
    );
    assert_eq!(
        payload["expression_fusion_strategy"],
        "dictionary_weighted_transform_fusion"
    );
    assert_eq!(
        payload["expression_plan_fingerprint_status"],
        "shared_dictionary_value_transform_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_dictionary_transformed_general_measure_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "transformed_chunk_dictionary_general_measure_code_map"
    );
    assert_eq!(
        payload["group_key_storage"],
        "typed_single_key+interned_utf8"
    );
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    assert_eq!(
        payload["transformed_dictionary_lazy_utf8_minmax_updates"],
        serde_json::json!(true)
    );
    assert!(
        payload
            .get("transformed_dictionary_transform_code_map_entries")
            .is_none(),
        "general transformed-measure route should not populate the string interner/cache"
    );
    let rows = payload["values"].as_array().expect("values");
    let example_sum = values[0].len() * 2 + values[2].len() + values[3].len();
    let example_avg = (example_sum as f64) / 4.0;
    assert_eq!(rows[0]["k"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(4));
    assert_eq!(rows[0]["l"], serde_json::json!(example_avg));
    assert_eq!(rows[0]["s"], serde_json::json!(example_sum as f64));
    assert_eq!(rows[0]["m"], serde_json::json!("http://example.test/a"));
    assert_eq!(rows[1]["k"], serde_json::json!("other.test"));
    assert_eq!(rows[1]["c"], serde_json::json!(1));
    assert_eq!(rows[1]["l"], serde_json::json!(values[1].len() as f64));
    assert_eq!(rows[1]["s"], serde_json::json!(values[1].len() as f64));
    assert_eq!(rows[1]["m"], serde_json::json!("https://other.test/b"));
}

#[test]
fn grouped_dense_general_measures_transformed_dictionary_uses_compact_domain_state() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("Referer").expect("column")),
                "l".to_string(),
            )
            .with_value_transform("length"),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("Referer").expect("column")),
                "m".to_string(),
            ),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "k".to_string(),
        ColumnRef::new("Referer").expect("column"),
        "url_domain",
    )])
    .with_having(vec![crate::VortexAggregateHavingExpr::new(
        "c",
        ComparisonOp::GtEq,
        "2",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("l", true)]);
    let declared_columns = vec!["Referer".to_string()];
    let values = [
        std::sync::Arc::<str>::from("http://example.test/z"),
        std::sync::Arc::<str>::from("https://other.test/b"),
        std::sync::Arc::<str>::from("http://example.test/a"),
        std::sync::Arc::<str>::from("www.example.test/q"),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 2, 0, 1, 2, 3],
        values: values.iter().cloned().map(Into::into).collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    assert!(
        states
            .update_dense_general_direct_from_transformed_dictionary(
                &accessors,
                Some(&[0, 2, 3, 4, 5]),
            )
            .expect("dense transformed dictionary general update")
    );

    let budget = states
        .state_budget_report(&request, 5, 1)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+chunk_dictionary_transformed_dense_general_measures"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"transformed_dictionary_dense_general_measure_update".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"dense_general_measure_state_memory".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 1);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "transformed_dictionary_dense_general_measure_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "dense_transformed_dictionary_general_measure_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "dense_transformed_dictionary_general_accumulators"
    );
    assert_eq!(
        payload["group_key_storage"],
        "dense_transformed_dictionary_interned_utf8_key"
    );
    assert_eq!(
        payload["materialized_group_value_count"],
        serde_json::json!(0)
    );
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    assert_eq!(
        payload["transformed_dictionary_dense_general_updates"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["transformed_dictionary_lazy_utf8_minmax_updates"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["transformed_dictionary_dense_general_pre_having_groups"],
        serde_json::json!(2)
    );
    assert_eq!(
        payload["transformed_dictionary_dense_general_selected_groups"],
        serde_json::json!(1)
    );
    let rows = payload["values"].as_array().expect("values");
    let example_sum = values[0].len() * 2 + values[2].len() + values[3].len();
    let example_avg = (example_sum as f64) / 4.0;
    assert_eq!(rows[0]["k"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(4));
    assert_eq!(rows[0]["l"], serde_json::json!(example_avg));
    assert_eq!(rows[0]["m"], serde_json::json!("http://example.test/a"));
}

#[test]
fn grouped_dense_general_measures_transformed_dictionary_uses_chunk_partials() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("Referer").expect("column")),
                "l".to_string(),
            )
            .with_value_transform("length"),
            crate::VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("Referer").expect("column")),
                "m".to_string(),
            ),
        ],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "k".to_string(),
        ColumnRef::new("Referer").expect("column"),
        "url_domain",
    )])
    .with_having(vec![crate::VortexAggregateHavingExpr::new(
        "c",
        ComparisonOp::GtEq,
        "2",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["Referer".to_string()];
    let mut values = Vec::new();
    let mut row_ids = Vec::new();
    for index in 0..80_u32 {
        values.push(std::sync::Arc::<str>::from(format!(
            "http://example.test/{index:03}"
        )));
        row_ids.push(index);
    }
    for index in 0..20_u32 {
        values.push(std::sync::Arc::<str>::from(format!(
            "https://other.test/{index:03}"
        )));
        row_ids.push(80 + index);
    }
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids,
        values: values.into_iter().map(Into::into).collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }];

    assert!(
        states
            .update_dense_general_direct_from_transformed_dictionary(&accessors, None)
            .expect("dense transformed dictionary general chunk-partial update")
    );

    let budget = states
        .state_budget_report(&request, 100, 1)
        .expect("state budget");
    assert!(
        budget
            .capillary_work_units
            .contains(&"transformed_dictionary_dense_general_chunk_partial".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"global_intern_probe_reduction".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "transformed_dictionary_dense_general_chunk_partial_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "dense_transformed_dictionary_chunk_partial_measure_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "dense_transformed_dictionary_chunk_partial_accumulators"
    );
    assert_eq!(
        payload["transformed_dictionary_dense_general_chunk_partials"],
        serde_json::json!(true)
    );
    assert_eq!(
        payload["transformed_dictionary_dense_general_chunk_partial_input_values"],
        serde_json::json!(100)
    );
    assert_eq!(
        payload["transformed_dictionary_dense_general_chunk_partial_groups"],
        serde_json::json!(2)
    );
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["k"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(80));
    assert_eq!(rows[0]["m"], serde_json::json!("http://example.test/000"));
    assert_eq!(rows[1]["k"], serde_json::json!("other.test"));
    assert_eq!(rows[1]["c"], serde_json::json!(20));
    assert_eq!(rows[1]["m"], serde_json::json!("https://other.test/000"));
}

#[test]
fn grouped_count_star_materialized_url_domain_uses_chunk_local_partials() {
    let request = VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "domain".to_string(),
        ColumnRef::new("URL").expect("column"),
        "url_domain",
    )])
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["URL".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::materialized(
        vec![
            StatValue::Utf8("http://example.test/a".to_string()),
            StatValue::Utf8("https://other.test/b".to_string()),
            StatValue::Utf8("http://example.test/c".to_string()),
            StatValue::Utf8("http://example.test/d".to_string()),
            StatValue::Utf8("www.example.test/e".to_string()),
        ],
        "cg21.aggregate_accessor.materialized_after_direct_provider_miss",
    )];

    assert!(
        states
            .update_count_star_direct_from_materialized_string_partials(&accessors, None)
            .expect("materialized string partial update")
    );
    states.count_star_direct_updates = true;
    states.chunk_materialized_partial_updates = true;

    let budget = states
        .state_budget_report(&request, 5, 2)
        .expect("state budget");
    assert_eq!(
        budget.state_family,
        "grouped_aggregate_state+topk+count_star_direct+compact_group_state+chunk_materialized_partials+transformed"
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"chunk_materialized_string_partial_group_update".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"chunk_local_partial_count_merge".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"transformed_materialized_string_partial_cache".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"string_transform_reuse_by_chunk_value".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 2);
    assert_eq!(
        payload["aggregate_update_strategy"],
        "transformed_materialized_string_partial_count_star_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "chunk_materialized_transformed_partial_count_star_group_state"
    );
    assert_eq!(
        payload["group_state_mode"],
        "transformed_chunk_materialized_partial_map"
    );
    assert_eq!(payload["decoded_string_count"], serde_json::json!(2));
    let rows = payload["values"].as_array().expect("values");
    assert_eq!(rows[0]["domain"], serde_json::json!("example.test"));
    assert_eq!(rows[0]["c"], serde_json::json!(4));
    assert_eq!(rows[1]["domain"], serde_json::json!("other.test"));
    assert_eq!(rows[1]["c"], serde_json::json!(1));
}

#[test]
fn grouped_count_star_materialized_string_partials_decline_numeric_identity_keys() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("AdvEngineID").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec!["AdvEngineID".to_string()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![AggregateDirectColumnAccessor::materialized(
        vec![
            StatValue::Int64(7),
            StatValue::Int64(9),
            StatValue::Int64(7),
        ],
        "cg21.aggregate_accessor.materialized_after_direct_provider_miss",
    )];

    assert!(
        !states
            .update_count_star_direct_from_materialized_string_partials(&accessors, None)
            .expect("materialized string partial admission")
    );
    assert!(!states.chunk_materialized_partial_updates);
    assert!(!states.transformed_materialized_partial_updates);
}

#[test]
fn native_sort_block_rejects_replaced_partition_schema_after_admission() {
    use vortex::VortexSessionDefault as _;
    use vortex::array::{IntoArray as _, arrays::PrimitiveArray, validity::Validity};
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::{BlockingRuntime as _, single::SingleThreadRuntime};
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let path = unique_vortex_path("native-sort-replaced-partition-schema");
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let inspect = || {
        runtime
            .block_on(session.open_options().open_path(&path))
            .unwrap()
            .dtype()
            .clone()
    };
    let original = PrimitiveArray::new(vec![1_i64], Validity::NonNullable).into_array();
    write_array(&path, &original).unwrap();
    let admitted = inspect();
    native_sort_block::validate_partition_dtype(true, Some(&admitted), &inspect()).unwrap();
    std::fs::remove_file(&path).unwrap();
    let replacement = PrimitiveArray::new(vec![0_u64], Validity::NonNullable).into_array();
    write_array(&path, &replacement).unwrap();
    let reopened = inspect();
    std::fs::remove_file(&path).unwrap();

    let error = native_sort_block::validate_partition_dtype(true, Some(&admitted), &reopened)
        .unwrap_err()
        .to_string();
    assert!(error.contains("partition schema changed after native cutoff admission"));
    assert!(error.contains("no fallback execution was attempted"));
    // Existing mixed-schema execution never acquired this pruning proof.
    native_sort_block::validate_partition_dtype(false, Some(&admitted), &reopened).unwrap();
}

#[test]
fn native_sort_block_partition_admission_checks_all_source_types_before_pruning() {
    use vortex::array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        validity::Validity,
    };
    for mixed in [false, true] {
        let paths = (0..3)
            .map(|index| {
                unique_vortex_path(&format!("native-sort-partition-types-{mixed}-{index}"))
            })
            .collect::<Vec<_>>();
        for (index, path) in paths.iter().enumerate() {
            let metric = if mixed && index == 1 {
                PrimitiveArray::new(vec![0_u64], Validity::NonNullable).into_array()
            } else {
                PrimitiveArray::new(vec![i64::from(index == 0)], Validity::NonNullable).into_array()
            };
            let array =
                StructArray::try_new(["metric"].into(), vec![metric], 1, Validity::NonNullable)
                    .unwrap()
                    .into_array();
            write_array(path, &array).unwrap();
        }
        let uris = paths
            .iter()
            .map(|path| DatasetUri::new(path.display().to_string()).unwrap())
            .collect::<Vec<_>>();
        let order = vec![crate::VortexAggregateOrderExpr::new("metric", false)];
        let request = VortexQueryPrimitiveRequest::sort_rows(
            DatasetUri::new("placeholder.vortex").unwrap(),
            ProjectionRequest::All,
            None,
            VortexSortRowsRequest::new(order.clone()),
            1,
        );
        let report = execute_vortex_local_partitioned_primitive_with_policy(
            &request,
            &uris,
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
        )
        .unwrap();
        for path in paths {
            std::fs::remove_file(path).unwrap();
        }
        let payload = simple_aggregate_values_json(report.result_summary.as_deref().unwrap());
        assert_eq!(
            payload["native_sort_block"]["chunks"],
            if mixed { 0 } else { 3 }
        );
        let mut oracle = (0..3)
            .map(|index| SortRowCandidate {
                ordinal: index,
                source_partition_index: index,
                source_ordinal: 0,
                values: vec![if mixed && index == 1 {
                    StatValue::UInt64(0)
                } else {
                    StatValue::Int64(i64::from(index == 0))
                }],
            })
            .collect::<Vec<_>>();
        sort_materialized_rows(&mut oracle, &order, &[0], VortexSortTiePolicy::First);
        assert_eq!(
            payload["values"],
            serde_json::json!([{"metric":stat_value_to_json_value(&oracle[0].values[0]).unwrap()}])
        );
        assert!(!report.fallback_execution_allowed);
        assert_eq!(report.rows_scanned, 3);
    }
}

#[test]
fn sort_rows_reports_topk_offset_state_budget_without_fallback() {
    let path = unique_vortex_path("sort-rows-state-budget");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("label").expect("column"),
            ColumnRef::new("amount").expect("column"),
        ]),
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("amount", true)])
            .with_offset(1),
        1,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(1));
    assert!(report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(report.state_budget.state_budget_required);
    assert_eq!(
        report.state_budget.state_family,
        "raw_row_topk_sort_state+offset"
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"offset_drain".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"materialized_sort_rows".to_string())
    );
    assert!(report.state_budget.fail_closed_if_spill_required);
}

#[test]
fn sort_rows_offset_returns_second_ordered_row_without_fallback() {
    let path = unique_vortex_path("sort-rows-offset-second-row");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("label").expect("column"),
            ColumnRef::new("amount").expect("column"),
        ]),
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("amount", true)])
            .with_offset(1),
        1,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(1));
    assert_eq!(report.rows_projected, Some(1));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let summary = report.result_summary.expect("summary");
    let payload = simple_aggregate_values_json(&summary);
    let values = payload["values"].as_array().expect("values array");
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["id"], serde_json::json!(2));
    assert_eq!(values[0]["label"], serde_json::json!("trial"));
    assert_eq!(values[0]["amount"], serde_json::json!(7));
}

#[test]
fn sort_rows_rerun_stays_on_single_vortex_artifact_without_query_summary_sidecar() {
    let path = unique_vortex_path("sort-rows-single-vortex-artifact");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("label").expect("column"),
            ColumnRef::new("amount").expect("column"),
        ]),
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("amount", true)]),
        2,
    );

    let first = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("first report");
    assert_eq!(
        first.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(first.rows_scanned, 3);
    assert!(first.data_read);
    let first_summary = first.result_summary.as_deref().expect("first summary");
    let first_payload = simple_aggregate_values_json(first_summary);
    assert_eq!(
        first_payload.get("prepared_olap_query_summary_written"),
        None
    );
    assert_eq!(first_payload.get("prepared_olap_query_summary_hit"), None);

    let second = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).expect("policy"),
    )
    .expect("second report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(
        second.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(second.rows_scanned, 3);
    assert_eq!(second.rows_selected, Some(2));
    assert_eq!(second.rows_projected, Some(2));
    assert!(second.upstream_scan_called);
    assert!(second.data_read);
    assert!(second.data_decoded);
    assert!(second.data_materialized);
    assert!(second.row_read);
    assert!(!second.fallback_execution_allowed);
    let second_summary = second.result_summary.as_deref().expect("second summary");
    let second_payload = simple_aggregate_values_json(second_summary);
    assert_eq!(second_payload.get("prepared_olap_query_summary_hit"), None);
    assert_eq!(
        second_payload.get("prepared_olap_query_summary_consumed"),
        None
    );
    assert_eq!(second_payload.get("prepared_olap_raw_scan_avoided"), None);
    assert_eq!(second_payload["rows"], serde_json::json!(2));
}

#[test]
fn sort_rows_uses_capillary_retention_window_for_small_limits() {
    assert!(sort_retention_flush_threshold(10) > 20);
    assert_eq!(sort_retention_flush_threshold(10), 4106);
    assert_eq!(sort_retention_flush_threshold(1_000), 64_000);
}

#[test]
fn sort_rows_threshold_prunes_candidate_free_descending_chunks() {
    let retained = vec![
        SortRowCandidate {
            ordinal: 0,
            source_partition_index: 0,
            source_ordinal: 0,
            values: vec![StatValue::Int64(100)],
        },
        SortRowCandidate {
            ordinal: 1,
            source_partition_index: 0,
            source_ordinal: 1,
            values: vec![StatValue::Int64(90)],
        },
    ];
    let columns = vec![vec![
        StatValue::Int64(80),
        StatValue::Int64(70),
        StatValue::Int64(60),
    ]];
    let order_by = vec![crate::VortexAggregateOrderExpr::new("metric", true)];

    assert!(
        sort_chunk_pruned_by_topk_threshold(
            &columns,
            3,
            &retained,
            &order_by,
            &[0],
            &[0],
            2,
            VortexSortTiePolicy::First,
            2,
        )
        .expect("chunk threshold pruning")
    );
}

#[test]
fn sort_rows_threshold_respects_last_tie_policy() {
    let retained = vec![
        SortRowCandidate {
            ordinal: 0,
            source_partition_index: 0,
            source_ordinal: 0,
            values: vec![StatValue::Int64(100)],
        },
        SortRowCandidate {
            ordinal: 1,
            source_partition_index: 0,
            source_ordinal: 1,
            values: vec![StatValue::Int64(90)],
        },
    ];
    let columns = vec![vec![StatValue::Int64(90), StatValue::Int64(80)]];
    let order_by = vec![crate::VortexAggregateOrderExpr::new("metric", true)];

    assert!(
        sort_chunk_pruned_by_topk_threshold(
            &columns,
            2,
            &retained,
            &order_by,
            &[0],
            &[0],
            2,
            VortexSortTiePolicy::First,
            2,
        )
        .expect("first tie pruning")
    );
    assert!(
        !sort_chunk_pruned_by_topk_threshold(
            &columns,
            2,
            &retained,
            &order_by,
            &[0],
            &[0],
            2,
            VortexSortTiePolicy::Last,
            2,
        )
        .expect("last tie pruning")
    );
}

#[test]
fn sort_rows_retention_window_preserves_last_tie_candidates() {
    let order_by = vec![crate::VortexAggregateOrderExpr::new("metric", true)];
    let tied_candidates = || {
        vec![
            SortRowCandidate {
                ordinal: 0,
                source_partition_index: 0,
                source_ordinal: 0,
                values: vec![StatValue::Int64(100)],
            },
            SortRowCandidate {
                ordinal: 1,
                source_partition_index: 0,
                source_ordinal: 1,
                values: vec![StatValue::Int64(100)],
            },
            SortRowCandidate {
                ordinal: 2,
                source_partition_index: 0,
                source_ordinal: 2,
                values: vec![StatValue::Int64(100)],
            },
        ]
    };

    let mut first = tied_candidates();
    retain_sort_top_window(&mut first, &order_by, &[0], 1, VortexSortTiePolicy::First);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].ordinal, 0);

    let mut last = tied_candidates();
    retain_sort_top_window(&mut last, &order_by, &[0], 1, VortexSortTiePolicy::Last);
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].ordinal, 2);
}

#[test]
fn sort_rows_late_materialization_policy_uses_row_refs_for_large_payload_topk() {
    let order = VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new(
        "EventTime",
        true,
    )]);
    let payload_columns = vec![
        "WatchID".to_string(),
        "URL".to_string(),
        "EventTime".to_string(),
    ];

    let large_policy =
        sort_rows_late_materialization_policy(100_000_000, &payload_columns, &order, 10);
    assert!(large_policy.enabled);
    assert_eq!(large_policy.reason, "bounded_row_ref_final_materialization");
    assert_eq!(large_policy.payload_column_count, 2);

    let small_policy = sort_rows_late_materialization_policy(3, &payload_columns, &order, 10);
    assert!(!small_policy.enabled);
    assert_eq!(small_policy.reason, "small_or_unbounded_payload_projection");

    let key_only = vec!["EventTime".to_string()];
    let key_only_policy = sort_rows_late_materialization_policy(100_000_000, &key_only, &order, 10);
    assert!(!key_only_policy.enabled);
    assert_eq!(key_only_policy.reason, "projection_is_order_key_only");

    let all_ties = VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new(
        "EventTime",
        true,
    )])
    .with_tie_policy(VortexSortTiePolicy::All);
    let all_ties_policy =
        sort_rows_late_materialization_policy(100_000_000, &payload_columns, &all_ties, 10);
    assert!(!all_ties_policy.enabled);
    assert_eq!(
        all_ties_policy.reason,
        "tie_expansion_requires_candidate_values"
    );
}

#[test]
fn sort_rows_wide_all_projection_materializes_only_selected_ordinals_without_fallback() {
    let path = unique_vortex_path("sort-rows-wide-all");
    write_wide_sort_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("metric", true)]),
        1,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(1));
    assert_eq!(report.rows_projected, Some(1));
    assert!(report.full_stream_collected);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(
        report
            .state_budget
            .state_family
            .contains("row_ref_late_materialization")
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"row_ref_candidate_scan".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"order_key_only_topk_candidate_state".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"selected_row_ref_materialization".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"final_retained_row_materialization".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"sort_key_candidate_values".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"selected_row_refs".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"payload_materialization_boundary".to_string())
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"late_output_materialization\":true"));
    assert!(summary.contains("\"row_ref_topk_materialization_policy\":\"wide_output_projection\""));
    assert!(summary.contains("\"late_materialization_chunks_scanned\":1"));
    assert!(summary.contains("\"late_materialization_early_stop_applied\":false"));
    assert!(summary.contains("\"late_materialization_row_index_selection_applied\":true"));
    assert!(summary.contains("\"late_materialization_requested_row_indices\":1"));
    assert!(summary.contains("\"late_materialization_min_selected_source_ordinal\":1"));
    assert!(summary.contains("\"late_materialization_max_selected_source_ordinal\":1"));
    assert!(summary.contains("\"late_materialization_selected_row_refs_used\":true"));
    assert!(
        summary.contains("\"sort_candidate_value_strategy\":\"order_key_only_row_ref_candidates\"")
    );
    assert!(summary.contains("\"sort_candidate_value_column_count\":1"));
    assert!(summary.contains("\"sort_candidate_value_columns\":[\"metric\"]"));
    assert!(summary.contains("\"id\":2"));
    assert!(summary.contains("\"metric\":30"));
    assert!(summary.contains("\"c7\":207"));
    assert!(!summary.contains("\"id\":1"));
    assert!(!summary.contains("\"id\":3"));
}

#[test]
fn sort_rows_wide_projection_with_pushdown_keeps_filtered_ordinals() {
    let path = unique_vortex_path("sort-rows-wide-pushdown-residual");
    write_wide_sort_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::All,
        Some(PredicateExpr::And(vec![
            PredicateExpr::Compare {
                column: ColumnRef::new("metric").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(20),
            },
            PredicateExpr::InList {
                column: ColumnRef::new("c0").expect("column"),
                values: vec![StatValue::Int64(300)],
                negated: false,
            },
        ])),
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("metric", true)]),
        1,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.materialization_boundary_reported);
    assert_eq!(report.rows_selected, Some(1));
    assert!(report.state_budget.state_family.contains("vortex_row_idx"));
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"vortex_row_idx_projection".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"row_index_selected_payload_scan".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"order_key_only_topk_candidate_state".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"root_source_row_ids".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"sort_key_candidate_values".to_string())
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"wide_output_second_pass\":true"));
    assert!(
        summary.contains("\"sort_predicate_strategy\":\"vortex_filter_pushdown_with_row_idx\"")
    );
    assert!(summary.contains("\"late_materialization_chunks_scanned\":1"));
    assert!(summary.contains("\"late_materialization_early_stop_applied\":false"));
    assert!(summary.contains("\"late_materialization_row_index_selection_applied\":true"));
    assert!(summary.contains("\"late_materialization_requested_row_indices\":1"));
    assert!(summary.contains("\"late_materialization_min_selected_source_ordinal\":2"));
    assert!(summary.contains("\"late_materialization_max_selected_source_ordinal\":2"));
    assert!(summary.contains("\"late_materialization_selected_row_refs_used\":true"));
    assert!(summary.contains("\"late_materialization_source_row_id_projection_applied\":true"));
    assert!(
        summary.contains("\"sort_candidate_value_strategy\":\"order_key_only_row_ref_candidates\"")
    );
    assert!(summary.contains("\"sort_candidate_value_column_count\":1"));
    assert!(summary.contains("\"sort_candidate_value_columns\":[\"metric\"]"));
    assert!(summary.contains("\"id\":3"));
    assert!(summary.contains("\"metric\":20"));
    assert!(summary.contains("\"c7\":307"));
    assert!(
        !summary.contains("\"id\":2"),
        "filtered ordinals must not be applied to the unfiltered source during wide output materialization"
    );
}

#[test]
fn sort_rows_wide_string_contains_uses_vortex_like_row_id_pushdown() {
    let path = unique_vortex_path("sort-rows-wide-string-like-pushdown");
    write_wide_string_sort_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::All,
        Some(PredicateExpr::StringContains {
            column: ColumnRef::new("URL").expect("url"),
            needle: "google".to_string(),
            negated: false,
        }),
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new(
            "EventTime",
            false,
        )]),
        1,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.materialization_boundary_reported);
    assert_eq!(report.rows_selected, Some(1));
    assert!(report.state_budget.state_family.contains("vortex_row_idx"));
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"vortex_row_idx_projection".to_string())
    );
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"row_index_selected_payload_scan".to_string())
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"wide_output_second_pass\":true"));
    assert!(
        summary.contains("\"sort_predicate_strategy\":\"vortex_filter_pushdown_with_row_idx\"")
    );
    assert!(summary.contains("\"late_materialization_source_row_id_projection_applied\":true"));
    assert!(
        summary.contains("\"sort_candidate_value_strategy\":\"order_key_only_row_ref_candidates\"")
    );
    assert!(summary.contains("\"id\":4"));
    assert!(summary.contains("\"EventTime\":10"));
    assert!(summary.contains("\"URL\":\"https://maps.google.com/place\""));
    assert!(!summary.contains("\"id\":2"));
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn sort_rows_rewrites_non_empty_string_predicate_to_embedded_length_column() {
    let path = unique_vortex_path("sort-rows-embedded-derived-length");
    write_embedded_derived_referer_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![ColumnRef::new("Referer").expect("referer")]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("Referer").expect("referer"),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        }),
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("Referer", false)]),
        10,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert_eq!(report.rows_selected, Some(3));
    assert!(
        report
            .projected_columns
            .iter()
            .all(|column| !is_shardloom_hidden_derived_column(column)),
        "sort output should not expose hidden embedded metadata columns"
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"embedded_derived_column_rewrite_status\":\"applied\""));
    assert!(summary.contains("__shardloom_derived_utf8_len_Referer"));
    assert!(summary.contains("\"Referer\":\"http://www.google.com/ads\""));
    assert!(summary.contains("\"Referer\":\"https://example.com/page\""));
    assert!(summary.contains("\"Referer\":\"https://www.google.com/search\""));
    assert!(!summary.contains("\"Referer\":\"\""));
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);
}

#[test]
fn residual_predicate_exact_candidates_intersect_compare_and_in_list() {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let path = unique_vortex_path("residual-predicate-exact-candidates");
    write_wide_sort_fixture(&path).expect("fixture");
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(
            session
                .open_options()
                .with_layout_reader_cache()
                .open_path(&path),
        )
        .expect("open vortex");
    let columns =
        local_field_names(file.dtype(), VortexQueryPrimitiveKind::SortRows).expect("columns");
    let mut scan = file.scan().expect("scan");
    scan = scan.with_concurrency(1);
    let chunk = scan
        .into_array_iter(&runtime)
        .expect("iter")
        .next()
        .expect("chunk")
        .expect("array");
    let _ = std::fs::remove_file(&path);
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("metric").expect("column"),
            op: ComparisonOp::LtEq,
            value: StatValue::Int64(20),
        },
        PredicateExpr::InList {
            column: ColumnRef::new("c0").expect("column"),
            values: vec![StatValue::Int64(200), StatValue::Int64(300)],
            negated: false,
        },
    ]);
    let evaluator =
        MaterializedPredicateEvaluator::compile(&predicate, &columns).expect("evaluator");

    assert_eq!(
        evaluator
            .fast_exact_candidate_row_indices_in_chunk(
                &chunk,
                &columns,
                &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                    &vortex::session::VortexSession::empty(),
                ),
            )
            .expect("exact")
            .expect("exact candidates"),
        vec![2]
    );
    assert!(predicate_candidate_materialization_admitted(2, chunk.len()));
    let retained = MaterializedPredicateEvaluator::compile(
        &PredicateExpr::InList {
            column: ColumnRef::new("c0").expect("column"),
            values: vec![StatValue::Int64(200), StatValue::Int64(300)],
            negated: false,
        },
        &columns,
    )
    .expect("candidate predicate")
    .filter_materialized_candidate_row_indices_in_chunk(
        &chunk,
        &columns,
        &[0, 2],
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::session::VortexSession::empty(),
        ),
    )
    .expect("candidate materialized filter");
    assert_eq!(retained, vec![2]);
}

#[test]
fn residual_predicate_candidate_refinement_uses_column_scoped_filter() {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let path = unique_vortex_path("residual-candidate-column-scoped-filter");
    write_mixed_string_filter_fixture(&path).expect("fixture");
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(
            session
                .open_options()
                .with_layout_reader_cache()
                .open_path(&path),
        )
        .expect("open vortex");
    let columns = local_field_names(file.dtype(), VortexQueryPrimitiveKind::SimpleAggregate)
        .expect("columns");
    let mut scan = file.scan().expect("scan");
    scan = scan.with_concurrency(1);
    let chunk = scan
        .into_array_iter(&runtime)
        .expect("iter")
        .next()
        .expect("chunk")
        .expect("array");
    let _ = std::fs::remove_file(&path);
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::StringContains {
            column: ColumnRef::new("URL").expect("column"),
            needle: "google".to_string(),
            negated: false,
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("SearchPhrase").expect("column"),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        },
    ]);
    let evaluator =
        MaterializedPredicateEvaluator::compile(&predicate, &columns).expect("evaluator");

    let filtered = evaluator
        .filter_column_scoped_candidate_row_indices_in_chunk(
            &chunk,
            &columns,
            &[0, 1, 2, 3],
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::session::VortexSession::empty(),
            ),
        )
        .expect("column-scoped candidate filter")
        .expect("candidate filter available");
    assert_eq!(filtered, vec![0, 3]);
    assert_eq!(
        evaluator
            .fast_exact_candidate_row_indices_in_chunk(
                &chunk,
                &columns,
                &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                    &vortex::session::VortexSession::empty(),
                ),
            )
            .expect("exact")
            .expect("exact candidates"),
        vec![0, 3]
    );
}

#[test]
fn simple_aggregate_string_contains_uses_vortex_scan_pushdown_without_residual_materialization() {
    let path = unique_vortex_path("simple-aggregate-string-contains-pushdown");
    write_mixed_string_filter_fixture(&path).expect("fixture");
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("SearchPhrase").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "c".to_string(),
        )],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        aggregate,
    )
    .with_source_order_limit(10);
    request.predicate = Some(PredicateExpr::And(vec![
        PredicateExpr::StringContains {
            column: ColumnRef::new("URL").expect("column"),
            needle: "google".to_string(),
            negated: false,
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("SearchPhrase").expect("column"),
            op: ComparisonOp::NotEq,
            value: StatValue::Utf8(String::new()),
        },
    ]));

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::VortexScanPushdown
    );
    assert_eq!(report.rows_scanned, 4);
    assert_eq!(report.rows_selected, Some(2));
    assert_eq!(report.rows_projected, Some(2));
    assert!(report.filter_pushdown_applied);
    assert!(report.upstream_filter_expression_used);
    assert!(report.projection_pushdown_applied);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn sort_rows_keep_last_uses_source_order_tie_reversal_without_fallback() {
    let path = unique_vortex_path("sort-rows-keep-last");
    write_sort_tie_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("key").expect("column"),
            ColumnRef::new("metric").expect("column"),
        ]),
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("key", false)])
            .with_tie_policy(VortexSortTiePolicy::Last),
        1,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert!(report.state_budget.state_family.contains("tie_last"));
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"source_order_tie_reversal".to_string())
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"tie_policy\":\"last\""));
    assert!(summary.contains("\"metric\":20"));
    assert!(!summary.contains("\"metric\":10"));
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn sort_rows_keep_all_expands_cutoff_ties_without_fallback() {
    let path = unique_vortex_path("sort-rows-keep-all");
    write_sort_tie_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("key").expect("column"),
            ColumnRef::new("metric").expect("column"),
        ]),
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("key", false)])
            .with_tie_policy(VortexSortTiePolicy::All),
        3,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert_eq!(report.rows_projected, Some(4));
    assert!(report.state_budget.state_family.contains("tie_all"));
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"cutoff_tie_expansion".to_string())
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"tie_policy\":\"all\""));
    assert!(summary.contains("\"rows\":4"));
    assert!(summary.contains("\"metric\":40"));
    assert!(!summary.contains("\"metric\":50"));
    assert!(!report.fallback_execution_allowed);
}

#[test]
fn sort_rows_preserves_exact_integer_ordering_without_fallback() {
    let path = unique_vortex_path("sort-rows-exact-integer");
    write_large_int_sort_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        ProjectionRequest::columns(vec![
            ColumnRef::new("id").expect("column"),
            ColumnRef::new("large").expect("column"),
        ]),
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("large", false)]),
        1,
    );

    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
    assert_eq!(report.rows_scanned, 2);
    assert_eq!(report.rows_projected, Some(1));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"id\":2"));
    assert!(summary.contains("9007199254740992"));
    assert!(!summary.contains("9007199254740993"));
}

#[test]
fn grouped_aggregate_applies_expression_groups_value_transforms_and_having_without_fallback() {
    let path = unique_vortex_path("grouped-aggregate-expressions");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "avg",
                    Some(ColumnRef::new("label").expect("column")),
                    "avg_label_len".to_string(),
                )
                .with_value_transform("length"),
            ],
        )
        .with_group_expressions(vec![
            crate::VortexAggregateExpression::new(
                "amount_minus_1".to_string(),
                ColumnRef::new("amount").expect("column"),
                "add_offset",
            )
            .with_argument_offset(-1),
        ])
        .with_having(vec![crate::VortexAggregateHavingExpr::new(
            "rows",
            ComparisonOp::GtEq,
            "1",
        )]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_selected, Some(3));
    assert_eq!(report.rows_projected, Some(3));
    assert!(report.data_decoded);
    assert!(report.data_materialized);
    assert!(report.row_read);
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"compact_numeric_group_state".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"row_materialization_bypass".to_string())
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"direct_count_sum_avg_group_update\""));
    assert!(summary.contains("\"compact_count_sum_avg_group_state\""));
    assert!(summary.contains("\"amount_minus_1\":9"));
    assert!(summary.contains("\"amount_minus_1\":4"));
    assert!(summary.contains("\"amount_minus_1\":6"));
    assert!(summary.contains("\"avg_label_len\":4.0"));
    assert!(summary.contains("\"avg_label_len\":5.0"));
}

#[test]
fn grouped_aggregate_uses_expression_groups_without_column_group_by() {
    let path = unique_vortex_path("grouped-aggregate-expression-only");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            Vec::new(),
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("amount").expect("column")),
                    "total_amount".to_string(),
                ),
            ],
        )
        .with_group_expressions(vec![
            crate::VortexAggregateExpression::new(
                "amount_bucket".to_string(),
                ColumnRef::new("amount").expect("column"),
                "add_offset",
            )
            .with_argument_offset(-5),
        ]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_projected, Some(3));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    assert!(
        report
            .state_budget
            .state_family
            .contains("grouped_aggregate_state")
    );
    let summary = report.result_summary.expect("summary");
    assert!(summary.contains("\"amount_bucket\":5"));
    assert!(summary.contains("\"amount_bucket\":0"));
    assert!(summary.contains("\"amount_bucket\":2"));
    assert!(summary.contains("\"total_amount\":10.0"));
    assert!(summary.contains("\"total_amount\":5.0"));
    assert!(summary.contains("\"total_amount\":7.0"));
}

#[test]
fn grouped_aggregate_prunes_functionally_dependent_offset_keys_without_dropping_outputs() {
    let path = unique_vortex_path("grouped-aggregate-functional-key");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("amount").expect("column")],
            vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".to_string(),
            )],
        )
        .with_group_expressions(vec![
            crate::VortexAggregateExpression::new(
                "amount_minus_1".to_string(),
                ColumnRef::new("amount").expect("column"),
                "add_offset",
            )
            .with_argument_offset(-1),
            crate::VortexAggregateExpression::new(
                "amount_minus_2".to_string(),
                ColumnRef::new("amount").expect("column"),
                "add_offset",
            )
            .with_argument_offset(-2),
        ]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_projected, Some(3));
    assert!(
        report
            .state_budget
            .capillary_work_units
            .contains(&"functional_dependency_key_pruning".to_string())
    );
    assert!(
        report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"derived_group_key_width_reduction".to_string())
    );
    let summary = report.result_summary.expect("summary");
    assert!(
        summary
            .contains("\"aggregate_key_encoding_mode\":\"typed_hash_key_functional_dependency\"")
    );
    assert!(summary.contains("\"amount\":10"));
    assert!(summary.contains("\"amount_minus_1\":9"));
    assert!(summary.contains("\"amount_minus_2\":8"));
    assert!(summary.contains("\"amount\":5"));
    assert!(summary.contains("\"amount_minus_1\":4"));
    assert!(summary.contains("\"amount_minus_2\":3"));
}

#[test]
fn aggregate_expression_transforms_cover_datetime_domain_and_case_without_fallback() {
    let extract_minute =
        AggregateValueTransform::from_parts("extract_minute", None).expect("extract");
    let date_trunc =
        AggregateValueTransform::from_parts("date_trunc_minute", None).expect("date trunc");
    let domain = AggregateValueTransform::from_parts("url_domain", None).expect("domain");
    let case = AggregateValueTransform::from_parts("case_search_adv_zero_referer_else_empty", None)
        .expect("case");

    assert_eq!(
        extract_minute
            .apply(&StatValue::Utf8("2026-06-18 12:34:56".to_string()))
            .expect("minute"),
        StatValue::UInt64(34)
    );
    assert_eq!(
        date_trunc
            .apply(&StatValue::Utf8("2026-06-18T12:34:56".to_string()))
            .expect("trunc"),
        StatValue::Utf8("2026-06-18T12:34:00".to_string())
    );
    assert_eq!(
        domain
            .apply(&StatValue::Utf8(
                "https://www.example.com/path?q=1".to_string()
            ))
            .expect("domain"),
        StatValue::Utf8("example.com".to_string())
    );
    assert_eq!(
        case.apply_values(
            &StatValue::Utf8("https://source.example/path".to_string()),
            &[&StatValue::Int64(0), &StatValue::Int64(0)]
        )
        .expect("case true"),
        StatValue::Utf8("https://source.example/path".to_string())
    );
    assert_eq!(
        case.apply_values(
            &StatValue::Utf8("https://source.example/path".to_string()),
            &[&StatValue::Int64(1), &StatValue::Int64(0)]
        )
        .expect("case false"),
        StatValue::Utf8(String::new())
    );
}

#[test]
fn aggregate_direct_date_trunc_minute_keeps_typed_keys_without_embedded_column() {
    let group_column = AggregateGroupRuntimeColumn {
        name: "EventMinuteBucket".to_string(),
        source_column: "EventTime".to_string(),
        column_index: 0,
        extra_column_indices: Vec::new(),
        transform: AggregateValueTransform::DateTruncMinute,
    };
    let unsigned_accessors = vec![AggregateDirectColumnAccessor::UInt64(vec![61, 119, 120])];
    let signed_accessors = vec![AggregateDirectColumnAccessor::Int64(vec![61, -1, 120])];
    let nullable_accessors = vec![AggregateDirectColumnAccessor::NullableInt64 {
        values: vec![61, -1, 120],
        row_nulls: vec![false, true, false],
    }];
    let mut interner = AggregateStringInterner::default();

    assert_eq!(
        aggregate_direct_transformed_group_key(
            &group_column,
            &unsigned_accessors,
            0,
            &mut interner,
        )
        .expect("unsigned row 0"),
        Some(AggregateDistinctValue::UInt64(60))
    );
    assert_eq!(
        aggregate_direct_transformed_group_key(
            &group_column,
            &unsigned_accessors,
            1,
            &mut interner,
        )
        .expect("unsigned row 1"),
        Some(AggregateDistinctValue::UInt64(60))
    );
    assert_eq!(
        aggregate_direct_transformed_group_key(
            &group_column,
            &unsigned_accessors,
            2,
            &mut interner,
        )
        .expect("unsigned row 2"),
        Some(AggregateDistinctValue::UInt64(120))
    );
    assert_eq!(
        aggregate_direct_transformed_group_key(&group_column, &signed_accessors, 1, &mut interner,)
            .expect("signed row"),
        Some(AggregateDistinctValue::Int64(-60))
    );
    assert_eq!(
        aggregate_direct_transformed_group_key(
            &group_column,
            &nullable_accessors,
            1,
            &mut interner,
        )
        .expect("nullable row"),
        Some(AggregateDistinctValue::Null)
    );
    assert_eq!(
        aggregate_direct_transformed_group_key(
            &group_column,
            &nullable_accessors,
            2,
            &mut interner,
        )
        .expect("nullable row 2"),
        Some(AggregateDistinctValue::Int64(120))
    );
}

#[test]
fn aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback() {
    let scalar_path = unique_vortex_path("count-distinct-scalar");
    let grouped_path = unique_vortex_path("count-distinct-grouped");
    write_duplicate_struct_fixture(&scalar_path).expect("scalar fixture");
    write_pivot_struct_fixture(&grouped_path).expect("grouped fixture");
    let scalar_request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(scalar_path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::new(vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("value").expect("column")),
            "unique_values".to_string(),
        )]),
    );
    let grouped_request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(grouped_path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![crate::VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("id").expect("column")),
                "unique_ids".to_string(),
            )],
        ),
    );

    let scalar_report = execute_vortex_local_primitive(&scalar_request).expect("scalar report");
    let grouped_report = execute_vortex_local_primitive(&grouped_request).expect("grouped report");
    let _ = std::fs::remove_file(&scalar_path);
    let _ = std::fs::remove_file(&grouped_path);

    assert_eq!(
        scalar_report.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert_eq!(scalar_report.rows_scanned, 5);
    assert_eq!(scalar_report.rows_projected, Some(1));
    assert_eq!(
        scalar_report.state_budget.state_family,
        "scalar_count_distinct_state+direct_dictionary_or_typed+dense_integer_preunion"
    );
    assert!(
        scalar_report
            .state_budget
            .capillary_work_units
            .contains(&"count_distinct_set".to_string())
    );
    assert!(
        scalar_report
            .state_budget
            .capillary_work_units
            .contains(&"dictionary_or_typed_direct_count_distinct".to_string())
    );
    assert!(
        scalar_report
            .state_budget
            .capillary_work_units
            .contains(&"dense_integer_distinct_preunion".to_string())
    );
    let scalar_summary = scalar_report.result_summary.expect("scalar summary");
    assert!(scalar_summary.contains("\"unique_values\":3"));
    assert!(scalar_summary.contains("\"typed_dense_integer_preunion_exact\""));
    assert!(!scalar_report.external_effects_executed);
    assert!(!scalar_report.fallback_execution_allowed);

    assert_eq!(
        grouped_report.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert_eq!(grouped_report.rows_scanned, 3);
    assert_eq!(grouped_report.rows_projected, Some(2));
    assert!(
        grouped_report
            .state_budget
            .state_family
            .contains("count_distinct")
    );
    assert!(
        grouped_report
            .state_budget
            .pulseweave_pressure_signals
            .contains(&"distinct_value_cardinality".to_string())
    );
    let grouped_summary = grouped_report.result_summary.expect("grouped summary");
    assert!(grouped_summary.contains("\"label\":\"paid\""));
    assert!(grouped_summary.contains("\"label\":\"trial\""));
    assert!(grouped_summary.contains("\"unique_ids\":1"));
    assert!(!grouped_report.external_effects_executed);
    assert!(!grouped_report.fallback_execution_allowed);
}

#[test]
fn grouped_count_distinct_direct_accessor_update_avoids_row_materialization() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("value").expect("column")],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("metric").expect("column")),
            "unique_metrics".to_string(),
        )],
    );
    let declared_columns = vec!["value".to_string(), "metric".to_string()];
    let mut states = GroupedAggregateStates::new(&request, None, &declared_columns, false, false)
        .expect("states");
    let accessors = vec![
        AggregateDirectColumnAccessor::UInt64(vec![1, 1, 2, 2, 2, 3]),
        AggregateDirectColumnAccessor::Int64(vec![10, 10, 20, 20, 30, 30]),
    ];

    states.observe_aggregate_accessors(&declared_columns, &accessors);
    assert_eq!(
        states
            .update_general_direct_from_accessors(&accessors, None, 6)
            .expect("direct accessor update"),
        true
    );
    let budget = states
        .state_budget_report(&request, 6, 3)
        .expect("state budget");
    assert!(
        budget
            .state_family
            .contains("direct_accessor_general_state+pre_reserved"),
        "{}",
        budget.state_family
    );
    assert!(
        budget.state_family.contains("direct_count_distinct"),
        "{}",
        budget.state_family
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"direct_general_group_state_pre_reservation".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"direct_accessor_count_distinct_group_update".to_string())
    );
    assert!(
        budget
            .pulseweave_pressure_signals
            .contains(&"row_materialization_bypass".to_string())
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(None)
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    assert_eq!(row_count, 3);
    assert_eq!(
        payload["aggregate_accessor_materialization_status"],
        "primitive_only"
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "direct_accessor_count_distinct_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "generic_direct_accessor_group_state"
    );
    assert_eq!(payload["group_state_mode"], "direct_accessor_hot_hash_map");
    assert_eq!(
        payload["distinct_state_strategy"],
        serde_json::json!("typed_hash_exact")
    );
    assert!(
        summary.contains(
            "\"aggregate_update_strategy\":\"direct_accessor_count_distinct_group_update\""
        )
    );
    assert!(summary.contains("\"value\":1"));
    assert!(summary.contains("\"unique_metrics\":1"));
    assert!(summary.contains("\"value\":2"));
    assert!(summary.contains("\"unique_metrics\":2"));
}

#[test]
fn grouped_mixed_count_distinct_preunions_integer_pairs_without_losing_measures() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("RegionID").expect("column")],
        vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "c".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("AdvEngineID").expect("column")),
                "sum_adv".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("ResolutionWidth").expect("column")),
                "avg_width".to_string(),
            ),
            crate::VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("UserID").expect("column")),
                "unique_users".to_string(),
            ),
        ],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("c", true)]);
    let declared_columns = vec![
        "RegionID".to_string(),
        "AdvEngineID".to_string(),
        "ResolutionWidth".to_string(),
        "UserID".to_string(),
    ];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &declared_columns, false, false)
            .expect("states");
    let accessors = vec![
        AggregateDirectColumnAccessor::UInt64(vec![1, 1, 1, 2, 2, 2, 2, 2]),
        AggregateDirectColumnAccessor::Int64(vec![4, 5, 6, 10, 11, 20, 21, 21]),
        AggregateDirectColumnAccessor::UInt64(vec![100, 200, 300, 400, 500, 600, 700, 800]),
        AggregateDirectColumnAccessor::UInt64(vec![100, 100, 101, 200, 201, 200, 202, 202]),
    ];

    states.observe_aggregate_accessors(&declared_columns, &accessors);
    assert_eq!(
        states
            .update_general_direct_from_accessors(&accessors, None, 8)
            .expect("direct accessor update"),
        true
    );
    let (row_count, summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("result summary");
    let payload: serde_json::Value =
        serde_json::from_str(&summary).expect("grouped aggregate summary json");
    let budget = states
        .state_budget_report(&request, 8, row_count)
        .expect("state budget");

    assert_eq!(row_count, 2);
    assert!(
        budget.state_family.contains("packed_integer_pair_preunion"),
        "{}",
        budget.state_family
    );
    assert!(
        budget.state_family.contains("chunk_group_partials"),
        "{}",
        budget.state_family
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"direct_accessor_count_distinct_pair_preunion".to_string())
    );
    assert!(
        budget
            .capillary_work_units
            .contains(&"direct_accessor_count_distinct_pair_chunk_group_partials".to_string())
    );
    assert_eq!(
        payload["aggregate_update_strategy"],
        "direct_accessor_count_distinct_pair_preunion_chunk_group_update"
    );
    assert_eq!(
        payload["compact_group_state_strategy"],
        "generic_direct_accessor_group_state+packed_integer_pair_preunion+chunk_group_partials"
    );
    assert_eq!(
        payload["group_state_mode"],
        "direct_accessor_count_distinct_pair_preunion_chunk_group_partials"
    );
    assert_eq!(
        payload["distinct_state_strategy"],
        "typed_hash_exact+packed_integer_pair_preunion"
    );
    assert_eq!(
        payload["grouped_count_distinct_pair_preunion_updates"],
        true
    );
    assert_eq!(
        payload["grouped_count_distinct_pair_preunion_input_rows"],
        8
    );
    assert_eq!(
        payload["grouped_count_distinct_pair_preunion_unique_pairs"],
        5
    );
    assert_eq!(
        payload["grouped_count_distinct_pair_preunion_chunk_group_partials"],
        true
    );
    assert_eq!(
        payload["grouped_count_distinct_pair_preunion_chunk_groups"],
        2
    );
    assert_eq!(
        payload["grouped_count_distinct_pair_preunion_duplicate_rows_elided"],
        3
    );
    let rows = payload["values"].as_array().expect("rows");
    assert_eq!(rows[0]["RegionID"], serde_json::json!(2));
    assert_eq!(rows[0]["c"], serde_json::json!(5));
    assert_eq!(rows[0]["sum_adv"], serde_json::json!(83.0));
    assert_eq!(rows[0]["avg_width"], serde_json::json!(600.0));
    assert_eq!(rows[0]["unique_users"], serde_json::json!(3));
    assert_eq!(rows[1]["RegionID"], serde_json::json!(1));
    assert_eq!(rows[1]["c"], serde_json::json!(3));
    assert_eq!(rows[1]["sum_adv"], serde_json::json!(15.0));
    assert_eq!(rows[1]["avg_width"], serde_json::json!(200.0));
    assert_eq!(rows[1]["unique_users"], serde_json::json!(2));
}

#[test]
fn aggregate_distinct_key_keeps_typed_values_exact_without_string_prefix_work() {
    let mut values = std::collections::HashSet::new();
    values.insert(AggregateDistinctValue::Int64(1));
    values.insert(AggregateDistinctValue::UInt64(1));
    values.insert(AggregateDistinctValue::Utf8(std::sync::Arc::<str>::from(
        "1",
    )));
    values.insert(AggregateDistinctValue::Float64Bits(1.0_f64.to_bits()));
    values.insert(AggregateDistinctValue::Boolean(true));

    assert_eq!(values.len(), 5);
    assert!(values.contains(&AggregateDistinctValue::Int64(1)));
    assert!(
        values.contains(&AggregateDistinctValue::Utf8(std::sync::Arc::<str>::from(
            "1",
        )))
    );
}

#[test]
fn simple_aggregate_row_export_writes_scalar_result_without_fallback() {
    let path = unique_vortex_path("simple-aggregate-row-export");
    let output_path =
        unique_vortex_path("simple-aggregate-row-export-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::new(vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
            crate::VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("metric").expect("column")),
                "sum_metric".to_string(),
            ),
        ]),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let parsed = serde_json::from_str::<serde_json::Value>(rows.trim()).expect("json row");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_written, 1);
    assert_eq!(report.pre_limit_result_row_count, 1);
    assert_eq!(
        report.projected_columns,
        vec!["rows".to_string(), "sum_metric".to_string()]
    );
    assert_eq!(parsed["rows"], serde_json::json!(5));
    assert_eq!(parsed["sum_metric"].as_f64(), Some(150.0));
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(!report.evidence.pushdown.source_order_limit_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_decoded);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.row_read);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn simple_aggregate_having_can_filter_scalar_result_without_fallback() {
    let path = unique_vortex_path("simple-aggregate-having-filtered");
    let output_path =
        unique_vortex_path("simple-aggregate-having-filtered-output").with_extension("jsonl");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::new(vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )])
        .with_having(vec![crate::VortexAggregateHavingExpr::new(
            "rows",
            ComparisonOp::Gt,
            "10",
        )]),
    );

    let report = execute_vortex_local_primitive(&request).expect("report");
    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(report.rows_scanned, 5);
    assert_eq!(report.rows_projected, Some(0));
    assert!(!report.external_effects_executed);
    assert!(!report.fallback_execution_allowed);
    let summary = report.result_summary.as_deref().expect("summary");
    assert!(summary.contains("\"rows\":0"));
    assert!(summary.contains("\"values\":{}"));

    let export_report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("row export report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(
        export_report.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert_eq!(export_report.rows_written, 0);
    assert_eq!(export_report.pre_limit_result_row_count, 0);
    assert_eq!(rows.trim(), "");
    assert!(!export_report.evidence.side_effects.fallback_attempted);
    assert!(
        !export_report
            .evidence
            .side_effects
            .fallback_execution_allowed
    );
}

#[test]
fn compact_count_having_prepared_comparison_matches_json_ordering() {
    let count = 12_u64;
    let left = serde_json::Value::Number(count.into());
    for right in [
        serde_json::Value::Null,
        serde_json::json!(true),
        serde_json::json!(-1),
        serde_json::json!(12),
        serde_json::json!(13),
        serde_json::json!(12.5),
        serde_json::json!("12"),
    ] {
        assert_eq!(
            compare_u64_json_value(count, &right),
            compare_json_values(&left, &right),
            "prepared compact-count comparison drifted for {right:?}"
        );
    }
}

#[test]
fn grouped_count_order_value_comparison_reuses_typed_u64_ordering() {
    let count = GroupedAggregateOrderValue::CountStar(12);
    let json = GroupedAggregateOrderValue::Json(serde_json::json!(13));
    assert_eq!(
        compare_grouped_order_values(&count, &json),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_grouped_order_values(&json, &count),
        std::cmp::Ordering::Greater
    );
    let string = GroupedAggregateOrderValue::Json(serde_json::json!("12"));
    assert_eq!(
        compare_grouped_order_values(&count, &string),
        compare_json_values(&serde_json::json!(12), &serde_json::json!("12"))
    );
}

#[test]
fn compact_numeric_measure_order_value_comparison_matches_json_ordering() {
    let value = CompactAggregateMeasureValue {
        count: 2,
        sum: NumericAggregateSum::Ordered(15.0),
    };
    let sum_value = value
        .order_value(SimpleAggregateFunction::Sum)
        .expect("sum order value");
    for right in [
        serde_json::Value::Null,
        serde_json::json!(14.0),
        serde_json::json!(15.0),
        serde_json::json!(16),
        serde_json::json!("15"),
    ] {
        assert_eq!(
            compare_grouped_order_value_to_json(&sum_value, &right),
            compare_json_values(&serde_json::json!(15.0), &right),
            "compact sum comparison drifted for {right:?}"
        );
    }

    let avg_value = value
        .order_value(SimpleAggregateFunction::Avg)
        .expect("avg order value");
    assert_eq!(
        compare_grouped_order_value_to_json(&avg_value, &serde_json::json!(8.0)),
        std::cmp::Ordering::Less
    );

    let null_sum = CompactAggregateMeasureValue::default()
        .order_value(SimpleAggregateFunction::Sum)
        .expect("null sum order value");
    assert_eq!(
        compare_grouped_order_value_to_json(&null_sum, &serde_json::Value::Null),
        std::cmp::Ordering::Equal
    );
    assert_eq!(
        compare_grouped_order_value_to_json(&null_sum, &serde_json::json!(0)),
        std::cmp::Ordering::Less
    );
}

#[test]
fn grouped_aggregate_row_export_writes_group_rows_without_fallback() {
    let path = unique_vortex_path("grouped-aggregate-row-export");
    let output_path =
        unique_vortex_path("grouped-aggregate-row-export-output").with_extension("jsonl");
    write_pivot_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("label").expect("column")],
            vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("amount").expect("column")),
                    "total_amount".to_string(),
                ),
            ],
        ),
    );

    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy"),
    )
    .expect("report");
    let rows = std::fs::read_to_string(&output_path).expect("output");
    let parsed_rows = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
        .collect::<Vec<_>>();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert_eq!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::SimpleAggregate
    );
    assert_eq!(report.rows_scanned, 3);
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert_eq!(
        report.projected_columns,
        vec![
            "label".to_string(),
            "rows".to_string(),
            "total_amount".to_string()
        ]
    );
    assert_eq!(parsed_rows.len(), 2);
    assert_eq!(parsed_rows[0]["label"], serde_json::json!("paid"));
    assert_eq!(parsed_rows[0]["rows"], serde_json::json!(2));
    assert_eq!(parsed_rows[0]["total_amount"].as_f64(), Some(15.0));
    assert_eq!(parsed_rows[1]["label"], serde_json::json!("trial"));
    assert_eq!(parsed_rows[1]["rows"], serde_json::json!(1));
    assert_eq!(parsed_rows[1]["total_amount"].as_f64(), Some(7.0));
    assert!(report.evidence.pushdown.projection_pushdown_applied);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.materialization_boundary_reported);
    assert!(!report.evidence.side_effects.external_effects_executed);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[test]
fn local_primitive_certificate_blocks_unsafe_effects() {
    let path = unique_vortex_path("certificate-blocked");
    write_struct_fixture(&path).expect("fixture");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(path.display().to_string()).expect("uri"),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3),
        },
        ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
    );

    let mut report = execute_vortex_local_primitive(&request).expect("report");
    let _ = std::fs::remove_file(&path);
    report.data_materialized = true;

    let certificate = local_primitive_execution_certificate(
        &correctness_fixture("vortex-local-filter-project-struct-five"),
        &request,
        &report,
    )
    .expect("certificate");

    assert_eq!(certificate.status, ExecutionCertificateStatus::Blocked);
    assert!(!certificate.is_certified());
    assert!(certificate.data_materialized);
}
