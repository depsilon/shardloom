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
    FileStatistics::from_stats(Arc::from([stats]), Arc::from([field]))
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
    let statistics = FileStatistics::from_stats(Arc::from([stats]), Arc::from([field.clone()]));
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
fn footer_aggregate_completes_aliased_count_star_from_row_count_without_statistics() {
    let field = DType::Primitive(PType::U64, Nullability::NonNullable);
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("footer-count-star.vortex").unwrap(),
        VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "arbitrary_total".into(),
        )]),
    );
    for (rows, expected) in [(0_u64, 0_u64), (23, 23)] {
        let mut states = states(&request);
        let completion = complete_from_stats(
            &dtype(field.clone(), Nullability::NonNullable),
            rows,
            None,
            &request,
            &mut states,
        )
        .unwrap()
        .unwrap();
        assert_eq!(completion.exact_statistics, 0);
        let mut summary = states.result_summary(&[]).unwrap();
        completion.annotate(&mut summary).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            payload["values"],
            serde_json::json!({"arbitrary_total": expected})
        );
        assert_eq!(
            payload["values"]["arbitrary_total"].as_u64(),
            Some(expected)
        );
        assert_eq!(payload["metadata_aggregate"]["source_rows_covered"], rows);
        assert_eq!(payload["metadata_aggregate"]["source_rows_visited"], 0);
        assert_eq!(
            payload["metadata_aggregate"]["source_payload_arrays_read"],
            0
        );
        assert_eq!(payload["metadata_aggregate"]["measures_proven"], 1);
        assert_eq!(
            payload["metadata_aggregate"]["all_measures_completed"],
            true
        );
    }

    request.simple_aggregate.as_mut().unwrap().measures = vec![
        VortexSimpleAggregateMeasure::new("count", None, "records".into()),
        VortexSimpleAggregateMeasure::new("count", None, "row_total".into()),
    ];
    let mut states = states(&request);
    let completion = complete_from_stats(
        &dtype(field, Nullability::NonNullable),
        23,
        None,
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
        serde_json::json!({"records":23,"row_total":23})
    );
    assert_eq!(payload["values"]["records"].as_u64(), Some(23));
    assert_eq!(payload["values"]["row_total"].as_u64(), Some(23));
    assert_eq!(payload["metadata_aggregate"]["source_rows_visited"], 0);
    assert_eq!(
        payload["metadata_aggregate"]["source_payload_arrays_read"],
        0
    );
    assert_eq!(payload["metadata_aggregate"]["measures_proven"], 2);
}

#[test]
fn footer_aggregate_filtered_count_star_declines_without_mutating_fresh_state() {
    let field = DType::Primitive(PType::U64, Nullability::NonNullable);
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("footer-filtered-count-star.vortex").unwrap(),
        VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "filtered_total".into(),
        )]),
    );
    request.predicate = Some(PredicateExpr::AlwaysTrue);
    let mut states = states(&request);
    let before = states.result_summary(&[]).unwrap();
    assert!(
        complete_from_stats(
            &dtype(field, Nullability::NonNullable),
            23,
            None,
            &request,
            &mut states,
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(states.result_summary(&[]).unwrap(), before);
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
fn footer_aggregate_declines_mixed_arithmetic_predicates_and_groups() {
    let field = DType::Primitive(PType::I64, Nullability::NonNullable);
    let stats = statistics(
        field.clone(),
        Some(Precision::Exact((-9_i64).into())),
        Some(Precision::Exact(19_i64.into())),
        Some(0),
    );
    for case in 0..7 {
        let mut request = request();
        let aggregate = request.simple_aggregate.as_mut().unwrap();
        match case {
            0 => aggregate.measures[1].function = "sum".into(),
            1 => aggregate.measures[1].function = "avg".into(),
            2 => aggregate.measures[1].function = "count_distinct".into(),
            3 => {
                aggregate.measures[1].function = "sum".into();
                aggregate.measures[1].argument_offset = Some(1);
            }
            4 => aggregate.measures[1].value_transform = Some("length".into()),
            5 => request.predicate = Some(PredicateExpr::AlwaysTrue),
            _ => aggregate.group_by = vec![ColumnRef::new("value").unwrap()],
        }
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
    let stats = FileStatistics::from_stats(
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
        crate::VortexLocalPrimitiveResourceEnvelope::new(4, 1)
            .expect("explicit fixture allocation"),
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

fn stats_for(field: DType, entries: &[(Stat, Precision<ScalarValue>)]) -> FileStatistics {
    let mut set = StatsSet::default();
    for (stat, value) in entries {
        set.set(*stat, value.clone());
    }
    FileStatistics::from_stats(Arc::from([set]), Arc::from([field]))
}

#[test]
fn footer_aggregate_completes_count_on_all_nonnullable_and_nullable_dtypes() {
    for field in [
        DType::Bool(Nullability::NonNullable),
        DType::Utf8(Nullability::NonNullable),
        DType::Binary(Nullability::NonNullable),
        DType::Primitive(PType::F64, Nullability::NonNullable),
    ] {
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new("footer-any-count.vortex").unwrap(),
            VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
                "count",
                Some(ColumnRef::new("value").unwrap()),
                "present".into(),
            )]),
        );
        let mut states = states(&request);
        let completion = complete_from_stats(
            &dtype(field, Nullability::NonNullable),
            12,
            None,
            &request,
            &mut states,
        )
        .unwrap()
        .unwrap();
        assert_eq!(completion.exact_statistics, 0);
        assert_eq!(
            serde_json::Value::Object(states.result_values().unwrap()),
            serde_json::json!({"present": 12})
        );
    }

    for field in [
        DType::Bool(Nullability::Nullable),
        DType::Utf8(Nullability::Nullable),
        DType::Binary(Nullability::Nullable),
        DType::Primitive(PType::F64, Nullability::Nullable),
    ] {
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new("footer-nullable-count.vortex").unwrap(),
            VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
                "count",
                Some(ColumnRef::new("value").unwrap()),
                "present".into(),
            )]),
        );
        let exact = stats_for(
            field.clone(),
            &[(Stat::NullCount, Precision::Exact(3_u64.into()))],
        );
        let mut states_exact = states(&request);
        assert!(
            complete_from_stats(
                &dtype(field.clone(), Nullability::NonNullable),
                12,
                Some(&exact),
                &request,
                &mut states_exact,
            )
            .unwrap()
            .is_some()
        );
        assert_eq!(
            serde_json::Value::Object(states_exact.result_values().unwrap()),
            serde_json::json!({"present": 9})
        );

        let inexact = stats_for(
            field.clone(),
            &[(Stat::NullCount, Precision::Inexact(3_u64.into()))],
        );
        let mut states_inexact = states(&request);
        assert!(
            complete_from_stats(
                &dtype(field, Nullability::NonNullable),
                12,
                Some(&inexact),
                &request,
                &mut states_inexact,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(states_inexact.states[0].count, 0);
    }
}

#[test]
fn footer_aggregate_completes_bool_utf8_and_finite_nonzero_float_minmax() {
    for (field, min, max, entries, expected) in [
        (
            DType::Bool(Nullability::Nullable),
            ScalarValue::from(false),
            ScalarValue::from(true),
            vec![(Stat::NullCount, Precision::Exact(1_u64.into()))],
            serde_json::json!({"all": 6, "present": 5, "lo": false, "hi": true}),
        ),
        (
            DType::Utf8(Nullability::Nullable),
            ScalarValue::from("alpha"),
            ScalarValue::from("zeta"),
            vec![(Stat::NullCount, Precision::Exact(2_u64.into()))],
            serde_json::json!({"all": 6, "present": 4, "lo": "alpha", "hi": "zeta"}),
        ),
        (
            DType::Primitive(PType::F32, Nullability::Nullable),
            ScalarValue::from(-3.5_f32),
            ScalarValue::from(12.25_f32),
            vec![
                (Stat::NullCount, Precision::Exact(1_u64.into())),
                (Stat::NaNCount, Precision::Exact(0_u64.into())),
            ],
            serde_json::json!({"all": 6, "present": 5, "lo": -3.5, "hi": 12.25}),
        ),
        (
            DType::Primitive(PType::F64, Nullability::Nullable),
            ScalarValue::from(1.25_f64),
            ScalarValue::from(99.5_f64),
            vec![
                (Stat::NullCount, Precision::Exact(1_u64.into())),
                (Stat::NaNCount, Precision::Exact(0_u64.into())),
            ],
            serde_json::json!({"all": 6, "present": 5, "lo": 1.25, "hi": 99.5}),
        ),
    ] {
        let mut all = entries;
        all.push((Stat::Min, Precision::Exact(min)));
        all.push((Stat::Max, Precision::Exact(max)));
        let stats = stats_for(field.clone(), &all);
        let request = request();
        let mut states = states(&request);
        assert!(
            complete_from_stats(
                &dtype(field, Nullability::NonNullable),
                6,
                Some(&stats),
                &request,
                &mut states,
            )
            .unwrap()
            .is_some()
        );
        assert_eq!(
            serde_json::Value::Object(states.result_values().unwrap()),
            expected
        );
    }

    // Float MIN/MAX must decline cleanly when NaNCount is missing, inexact, or > 0, or extremum is zero/non-finite.
    let f64_field = DType::Primitive(PType::F64, Nullability::Nullable);
    for (extra_nan, min, max, expect_err) in [
        (None, -1.5, 2.5, false),
        (Some(Precision::Inexact(0_u64.into())), -1.5, 2.5, false),
        (Some(Precision::Exact(1_u64.into())), -1.5, 2.5, false),
        (Some(Precision::Exact(0_u64.into())), -0.0, 2.5, false),
        (Some(Precision::Exact(0_u64.into())), 0.0, 2.5, false),
        (
            Some(Precision::Exact(0_u64.into())),
            -1.5,
            f64::INFINITY,
            false,
        ),
        (Some(Precision::Exact(6_u64.into())), -1.5, 2.5, true),
    ] {
        let mut entries = vec![
            (Stat::NullCount, Precision::Exact(1_u64.into())),
            (Stat::Min, Precision::Exact(min.into())),
            (Stat::Max, Precision::Exact(max.into())),
        ];
        if let Some(nan_count) = extra_nan {
            entries.push((Stat::NaNCount, nan_count));
        }
        let stats = stats_for(f64_field.clone(), &entries);
        let request = request();
        let mut states = states(&request);
        let result = complete_from_stats(
            &dtype(f64_field.clone(), Nullability::NonNullable),
            6,
            Some(&stats),
            &request,
            &mut states,
        );
        if expect_err {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_none());
        }
        assert_eq!(states.states[0].count, 0);
    }
}

fn sum_avg_test_request() -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("footer-sum-avg.vortex").unwrap(),
        VortexSimpleAggregateRequest::new(vec![
            VortexSimpleAggregateMeasure::new(
                "count",
                Some(ColumnRef::new("value").unwrap()),
                "present".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("value").unwrap()),
                "total".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("value").unwrap()),
                "mean".into(),
            ),
        ]),
    )
}

#[test]
fn footer_aggregate_completes_narrow_integer_sum_and_avg() {
    let sum_request = sum_avg_test_request();
    for (ptype, sum_scalar, rows, nulls, expected) in [
        (
            PType::I32,
            ScalarValue::from(100_i64),
            5_u64,
            1_u64,
            serde_json::json!({"present": 4, "total": 100.0, "mean": 25.0}),
        ),
        (
            PType::U32,
            ScalarValue::from(150_u64),
            5_u64,
            0_u64,
            serde_json::json!({"present": 5, "total": 150.0, "mean": 30.0}),
        ),
        (
            PType::I32,
            ScalarValue::from(0_i64),
            5_u64,
            5_u64,
            serde_json::json!({"present": 0, "total": null, "mean": null}),
        ),
    ] {
        let field = DType::Primitive(ptype, Nullability::Nullable);
        let stats = stats_for(
            field.clone(),
            &[
                (Stat::NullCount, Precision::Exact(nulls.into())),
                (Stat::Sum, Precision::Exact(sum_scalar)),
            ],
        );
        let mut states = states(&sum_request);
        assert!(
            complete_from_stats(
                &dtype(field, Nullability::NonNullable),
                rows,
                Some(&stats),
                &sum_request,
                &mut states,
            )
            .unwrap()
            .is_some()
        );
        assert_eq!(
            serde_json::Value::Object(states.result_values().unwrap()),
            expected
        );
    }

    // Contradictory non-zero sum on all-NULL input must fail explicitly.
    let i32_field = DType::Primitive(PType::I32, Nullability::Nullable);
    let bad_empty_sum = stats_for(
        i32_field.clone(),
        &[
            (Stat::NullCount, Precision::Exact(5_u64.into())),
            (Stat::Sum, Precision::Exact(7_i64.into())),
        ],
    );
    let mut bad_states = states(&sum_request);
    assert!(
        complete_from_stats(
            &dtype(i32_field, Nullability::NonNullable),
            5,
            Some(&bad_empty_sum),
            &sum_request,
            &mut bad_states,
        )
        .is_err()
    );
}

#[test]
fn footer_aggregate_enforces_64bit_and_float_sum_avg_bounds() {
    let sum_request = sum_avg_test_request();
    // 64-bit integer SUM/AVG admits values within 2^53 and declines beyond 2^53 or missing extrema.
    let i64_field = DType::Primitive(PType::I64, Nullability::NonNullable);
    let exact_i64 = stats_for(
        i64_field.clone(),
        &[
            (Stat::Min, Precision::Exact((-10_i64).into())),
            (Stat::Max, Precision::Exact(((1_i64) << 53).into())),
            (Stat::Sum, Precision::Exact((((1_i64) << 53) + 10).into())),
        ],
    );
    let mut i64_states = states(&sum_request);
    let completion = complete_from_stats(
        &dtype(i64_field.clone(), Nullability::NonNullable),
        4,
        Some(&exact_i64),
        &sum_request,
        &mut i64_states,
    )
    .unwrap()
    .unwrap();
    let mut summary = i64_states.result_summary(&[]).unwrap();
    completion.annotate(&mut summary).unwrap();
    let mut report = VortexLocalPrimitiveExecutionReport::feature_disabled(
        VortexQueryPrimitiveKind::SimpleAggregate,
        crate::VortexLocalPrimitiveResourceEnvelope::new(4, 1).unwrap(),
    );
    report.mode = VortexLocalPrimitiveExecutionMode::MetadataPreservingAggregate;
    report.embedded_layout.metadata_persisted_in_artifact = true;
    report.embedded_layout.footer_row_count = 4;
    report.rows_scanned = 4;
    report.rows_selected = Some(4);
    report.rows_projected = Some(1);
    report.result_summary = Some(format!("simple_aggregate values={summary}"));
    assert!(report_is_safe(&sum_request, &report));

    for uncertified_entries in [
        vec![(Stat::Sum, Precision::Exact(42_i64.into()))],
        vec![
            (Stat::Min, Precision::Exact(0_i64.into())),
            (Stat::Max, Precision::Exact((((1_i64) << 53) + 1).into())),
            (Stat::Sum, Precision::Exact(100_i64.into())),
        ],
    ] {
        let stats = stats_for(i64_field.clone(), &uncertified_entries);
        let mut declined_states = states(&sum_request);
        assert!(
            complete_from_stats(
                &dtype(i64_field.clone(), Nullability::NonNullable),
                4,
                Some(&stats),
                &sum_request,
                &mut declined_states,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(declined_states.states[0].count, 0);
        assert_eq!(declined_states.states[1].sum.total().to_bits(), 0);
    }

    // Float SUM/AVG is always declined from metadata.
    let f64_field = DType::Primitive(PType::F64, Nullability::NonNullable);
    let f64_stats = stats_for(
        f64_field.clone(),
        &[(Stat::Sum, Precision::Exact(42.0_f64.into()))],
    );
    let mut f64_states = states(&sum_request);
    assert!(
        complete_from_stats(
            &dtype(f64_field, Nullability::NonNullable),
            4,
            Some(&f64_stats),
            &sum_request,
            &mut f64_states,
        )
        .unwrap()
        .is_none()
    );
}
