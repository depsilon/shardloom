use super::*;
use crate::{VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::{ColumnRef, DatasetUri, PredicateExpr};
use std::sync::Arc;
use vortex::array::{
    dtype::{FieldNames, StructFields},
    scalar::ScalarValue,
    stats::StatsSet,
};

fn dtype(field: DType, root_nullable: Nullability) -> DType {
    DType::Struct(
        StructFields::new(FieldNames::from(["value"]), vec![field]),
        root_nullable,
    )
}

fn request() -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("footer-unit.vortex").unwrap(),
        VortexSimpleAggregateRequest::new(vec![
            VortexSimpleAggregateMeasure::new("count", None, "all".into()),
            VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("value").unwrap()),
                "lo".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "max",
                Some(ColumnRef::new("value").unwrap()),
                "hi".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "count",
                Some(ColumnRef::new("value").unwrap()),
                "present".into(),
            ),
        ]),
    )
}

fn states(request: &VortexQueryPrimitiveRequest) -> SimpleAggregateStates {
    let aggregate = required_simple_aggregate(request).unwrap();
    SimpleAggregateStates::new(aggregate, &["value".to_owned()]).unwrap()
}

fn statistics(
    field: DType,
    min: Option<Precision<ScalarValue>>,
    max: Option<Precision<ScalarValue>>,
    nulls: Option<u64>,
) -> FileStatistics {
    let mut stats = StatsSet::default();
    if let Some(min) = min {
        stats.set(Stat::Min, min);
    }
    if let Some(max) = max {
        stats.set(Stat::Max, max);
    }
    if let Some(nulls) = nulls {
        stats.set(Stat::NullCount, Precision::Exact(nulls.into()));
    }
    FileStatistics::new(Arc::from([stats]), Arc::from([field]))
}

#[test]
fn footer_aggregate_preserves_every_integer_width_and_extreme_without_float_conversion() {
    let cases = [
        (
            PType::I8,
            ScalarValue::from(i8::MIN),
            ScalarValue::from(i8::MAX),
            serde_json::json!(i8::MIN),
            serde_json::json!(i8::MAX),
        ),
        (
            PType::I16,
            ScalarValue::from(i16::MIN),
            ScalarValue::from(i16::MAX),
            serde_json::json!(i16::MIN),
            serde_json::json!(i16::MAX),
        ),
        (
            PType::I32,
            ScalarValue::from(i32::MIN),
            ScalarValue::from(i32::MAX),
            serde_json::json!(i32::MIN),
            serde_json::json!(i32::MAX),
        ),
        (
            PType::I64,
            ScalarValue::from(i64::MIN),
            ScalarValue::from(i64::MAX),
            serde_json::json!(i64::MIN),
            serde_json::json!(i64::MAX),
        ),
        (
            PType::U8,
            ScalarValue::from(0_u8),
            ScalarValue::from(u8::MAX),
            serde_json::json!(0),
            serde_json::json!(u8::MAX),
        ),
        (
            PType::U16,
            ScalarValue::from(0_u16),
            ScalarValue::from(u16::MAX),
            serde_json::json!(0),
            serde_json::json!(u16::MAX),
        ),
        (
            PType::U32,
            ScalarValue::from(0_u32),
            ScalarValue::from(u32::MAX),
            serde_json::json!(0),
            serde_json::json!(u32::MAX),
        ),
        (
            PType::U64,
            ScalarValue::from(0_u64),
            ScalarValue::from(u64::MAX),
            serde_json::json!(0),
            serde_json::json!(u64::MAX),
        ),
    ];
    for (ptype, min, max, expected_min, expected_max) in cases {
        let field = DType::Primitive(ptype, Nullability::NonNullable);
        let statistics = statistics(
            field.clone(),
            Some(Precision::Exact(min)),
            Some(Precision::Exact(max)),
            Some(0),
        );
        let request = request();
        let mut states = states(&request);
        let completion = complete_from_stats(
            &dtype(field, Nullability::NonNullable),
            9,
            Some(&statistics),
            &request,
            &mut states,
        )
        .unwrap()
        .unwrap();
        let mut summary = states.result_summary(&[]).unwrap();
        completion.annotate(&mut summary).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            payload["values"],
            serde_json::json!({"all":9,"present":9,"lo":expected_min,"hi":expected_max})
        );
        assert_eq!(payload["metadata_aggregate"]["source_rows_visited"], 0);
        assert_eq!(payload["metadata_aggregate"]["measures_proven"], 4);
    }
}

#[test]
fn footer_aggregate_nulls_and_empty_input_require_positive_proof() {
    let field = DType::Primitive(PType::I64, Nullability::Nullable);
    for (rows, nulls, extrema, expected) in [
        (
            5,
            Some(2),
            true,
            serde_json::json!({"all":5,"present":3,"lo":-9,"hi":19}),
        ),
        (
            5,
            Some(5),
            false,
            serde_json::json!({"all":5,"present":0,"lo":null,"hi":null}),
        ),
        (
            0,
            None,
            false,
            serde_json::json!({"all":0,"present":0,"lo":null,"hi":null}),
        ),
    ] {
        let stats = statistics(
            field.clone(),
            extrema.then(|| Precision::Exact((-9_i64).into())),
            extrema.then(|| Precision::Exact(19_i64.into())),
            nulls,
        );
        let request = request();
        let mut states = states(&request);
        assert!(
            complete_from_stats(
                &dtype(field.clone(), Nullability::NonNullable),
                rows,
                Some(&stats),
                &request,
                &mut states
            )
            .unwrap()
            .is_some()
        );
        assert_eq!(
            serde_json::Value::Object(states.result_values().unwrap()),
            expected
        );
    }
    let request = request();
    for stats in [
        None,
        Some(statistics(field.clone(), None, None, None)),
        Some(statistics(
            field.clone(),
            Some(Precision::Exact(1_i64.into())),
            Some(Precision::Exact(5_i64.into())),
            None,
        )),
    ] {
        let mut states = states(&request);
        let before = states.result_summary(&[]).unwrap();
        assert!(
            complete_from_stats(
                &dtype(field.clone(), Nullability::NonNullable),
                5,
                stats.as_ref(),
                &request,
                &mut states
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(states.result_summary(&[]).unwrap(), before);
    }
}

#[test]
fn footer_aggregate_count_uses_nonnullable_schema_but_never_inexact_nullable_null_count() {
    let mut request = request();
    request
        .simple_aggregate
        .as_mut()
        .unwrap()
        .measures
        .retain(|measure| measure.function == "count");
    let mut nonnullable_states = states(&request);
    let field = DType::Primitive(PType::I64, Nullability::NonNullable);
    let completion = complete_from_stats(
        &dtype(field, Nullability::NonNullable),
        7,
        None,
        &request,
        &mut nonnullable_states,
    )
    .unwrap()
    .unwrap();
    assert_eq!(completion.exact_statistics, 0);
    assert_eq!(
        serde_json::Value::Object(nonnullable_states.result_values().unwrap()),
        serde_json::json!({"all":7,"present":7})
    );

    let field = DType::Primitive(PType::I64, Nullability::Nullable);
    let mut stats = StatsSet::default();
    stats.set(Stat::NullCount, Precision::Inexact(0_u64.into()));
    let statistics = FileStatistics::new(Arc::from([stats]), Arc::from([field.clone()]));
    let mut nullable_states = states(&request);
    assert!(
        complete_from_stats(
            &dtype(field, Nullability::NonNullable),
            7,
            Some(&statistics),
            &request,
            &mut nullable_states
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(
        serde_json::Value::Object(nullable_states.result_values().unwrap()),
        serde_json::json!({"all":0,"present":0})
    );
}

#[test]
fn footer_aggregate_inexact_or_mismatched_metadata_cannot_partially_install_counts() {
    let field = DType::Primitive(PType::U64, Nullability::NonNullable);
    let request = request();
    let candidates = [
        statistics(
            field.clone(),
            None,
            Some(Precision::Exact(u64::MAX.into())),
            Some(0),
        ),
        statistics(
            field.clone(),
            Some(Precision::Inexact(0_u64.into())),
            Some(Precision::Exact(u64::MAX.into())),
            Some(0),
        ),
        statistics(
            field.clone(),
            Some(Precision::Exact(0_u64.into())),
            Some(Precision::Inexact(u64::MAX.into())),
            Some(0),
        ),
        statistics(
            DType::Primitive(PType::I64, Nullability::NonNullable),
            Some(Precision::Exact(0_i64.into())),
            Some(Precision::Exact(i64::MAX.into())),
            Some(0),
        ),
    ];
    for stats in candidates {
        let mut states = states(&request);
        let before = states.result_summary(&[]).unwrap();
        assert!(
            complete_from_stats(
                &dtype(field.clone(), Nullability::NonNullable),
                5,
                Some(&stats),
                &request,
                &mut states
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(states.result_summary(&[]).unwrap(), before);
    }
}

#[test]
fn footer_aggregate_rejects_contradictory_null_count_and_parent_validity() {
    let request = request();
    for (nullable, rows, nulls) in [
        (Nullability::Nullable, 5, 6),
        (Nullability::NonNullable, 5, 1),
        (Nullability::Nullable, 0, 0),
        (Nullability::Nullable, 5, 5),
    ] {
        let field = DType::Primitive(PType::I64, nullable);
        let stats = statistics(
            field.clone(),
            Some(Precision::Exact((-9_i64).into())),
            Some(Precision::Exact(19_i64.into())),
            Some(nulls),
        );
        let mut states = states(&request);
        let before = states.result_summary(&[]).unwrap();
        assert!(
            complete_from_stats(
                &dtype(field, Nullability::NonNullable),
                rows,
                Some(&stats),
                &request,
                &mut states
            )
            .is_err()
        );
        assert_eq!(states.result_summary(&[]).unwrap(), before);
    }
    let field = DType::Primitive(PType::I64, Nullability::NonNullable);
    let stats = statistics(
        field.clone(),
        Some(Precision::Exact((-9_i64).into())),
        Some(Precision::Exact(19_i64.into())),
        Some(0),
    );
    let mut states = states(&request);
    assert!(
        complete_from_stats(
            &dtype(field, Nullability::Nullable),
            5,
            Some(&stats),
            &request,
            &mut states
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(states.states[0].count, 0);
    let field = DType::Primitive(PType::I64, Nullability::NonNullable);
    let inverted = statistics(
        field.clone(),
        Some(Precision::Exact(19_i64.into())),
        Some(Precision::Exact((-9_i64).into())),
        Some(0),
    );
    assert!(
        complete_from_stats(
            &dtype(field, Nullability::NonNullable),
            5,
            Some(&inverted),
            &request,
            &mut states
        )
        .is_err()
    );
    assert_eq!(states.states[0].count, 0);
}

#[test]
fn footer_aggregate_declines_mixed_arithmetic_predicates_groups_and_count_star_only() {
    let field = DType::Primitive(PType::I64, Nullability::NonNullable);
    let stats = statistics(
        field.clone(),
        Some(Precision::Exact((-9_i64).into())),
        Some(Precision::Exact(19_i64.into())),
        Some(0),
    );
    for case in 0..8 {
        let mut request = request();
        let mut states = states(&request);
        let aggregate = request.simple_aggregate.as_mut().unwrap();
        match case {
            0 => aggregate.measures[1].function = "sum".into(),
            1 => aggregate.measures[1].function = "avg".into(),
            2 => aggregate.measures[1].function = "count_distinct".into(),
            3 => aggregate.measures[1].argument_offset = Some(1),
            4 => aggregate.measures[1].value_transform = Some("length".into()),
            5 => request.predicate = Some(PredicateExpr::AlwaysTrue),
            6 => aggregate.group_by = vec![ColumnRef::new("value").unwrap()],
            _ => aggregate.measures.truncate(1),
        }
        let before = states.result_summary(&[]).unwrap();
        assert!(
            complete_from_stats(
                &dtype(field.clone(), Nullability::NonNullable),
                5,
                Some(&stats),
                &request,
                &mut states
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(states.result_summary(&[]).unwrap(), before);
    }
}

#[test]
fn footer_aggregate_uses_schema_position_and_rejects_duplicate_aliases() {
    let field = DType::Primitive(PType::I64, Nullability::NonNullable);
    let dtype = DType::Struct(
        StructFields::new(
            FieldNames::from(["unused", "value"]),
            vec![field.clone(), field.clone()],
        ),
        Nullability::NonNullable,
    );
    let decoy = statistics(
        field.clone(),
        Some(Precision::Exact((-999_i64).into())),
        Some(Precision::Exact(999_i64.into())),
        Some(0),
    );
    let selected = statistics(
        field.clone(),
        Some(Precision::Exact((-9_i64).into())),
        Some(Precision::Exact(19_i64.into())),
        Some(0),
    );
    let stats = FileStatistics::new(
        Arc::from([
            decoy.stats_sets()[0].clone(),
            selected.stats_sets()[0].clone(),
        ]),
        Arc::from([field.clone(), field]),
    );
    let mut request = request();
    let mut states = states(&request);
    complete_from_stats(&dtype, 5, Some(&stats), &request, &mut states)
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::Value::Object(states.result_values().unwrap()),
        serde_json::json!({"all":5,"present":5,"lo":-9,"hi":19})
    );
    request.simple_aggregate.as_mut().unwrap().measures[1].alias = "all".into();
    assert!(
        SimpleAggregateStates::new(
            required_simple_aggregate(&request).unwrap(),
            &["value".to_owned()]
        )
        .is_err()
    );
}

#[test]
fn footer_aggregate_report_requires_exact_completion_and_zero_scan_work() {
    let field = DType::Primitive(PType::I64, Nullability::NonNullable);
    let stats = statistics(
        field.clone(),
        Some(Precision::Exact((-9_i64).into())),
        Some(Precision::Exact(19_i64.into())),
        Some(0),
    );
    let request = request();
    let mut states = states(&request);
    let completion = complete_from_stats(
        &dtype(field, Nullability::NonNullable),
        5,
        Some(&stats),
        &request,
        &mut states,
    )
    .unwrap()
    .unwrap();
    let mut summary = states.result_summary(&[]).unwrap();
    completion.annotate(&mut summary).unwrap();
    let mut report = VortexLocalPrimitiveExecutionReport::feature_disabled(
        VortexQueryPrimitiveKind::SimpleAggregate,
    );
    report.mode = VortexLocalPrimitiveExecutionMode::MetadataPreservingAggregate;
    report.embedded_layout.metadata_persisted_in_artifact = true;
    report.embedded_layout.footer_row_count = 5;
    report.rows_scanned = 5;
    report.rows_selected = Some(5);
    report.rows_projected = Some(1);
    report.result_summary = Some(format!("simple_aggregate values={summary}"));
    assert!(report_is_safe(&request, &report));
    for case in 0..7 {
        let mut report = report.clone();
        match case {
            0 => report.upstream_scan_called = true,
            1 => report.arrays_read_count = 1,
            2 => report.data_read = true,
            3 => report.projection_pushdown_applied = true,
            4 => report.embedded_layout.metadata_pruned_entire_input = true,
            5 => report.rows_projected = Some(0),
            _ => {
                report.result_summary =
                    Some("simple_aggregate values={\"rows\":1,\"values\":{\"all\":5}}".into());
            }
        }
        assert!(!report_is_safe(&request, &report));
    }
    for (field, value) in [
        (
            "rows_scanned_field_scope",
            serde_json::json!("source_rows_visited"),
        ),
        ("file_preparation_reads_excluded", serde_json::json!(false)),
        ("exact_statistics_consumed", serde_json::json!(9)),
        ("exact_statistics_consumed", serde_json::json!(-1)),
        ("exact_statistics_consumed", serde_json::Value::Null),
    ] {
        let mut payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        payload["metadata_aggregate"][field] = value;
        let mut report = report.clone();
        report.result_summary = Some(format!("simple_aggregate values={payload}"));
        assert!(!report_is_safe(&request, &report));
    }
}
