//! Consumer dependencies must reach row-count carriers and list expansion.

use super::*;
use crate::{
    query_primitive::{
        VortexExplodeProjectionRequest, VortexQueryPrimitiveKind, VortexSimpleAggregateMeasure,
    },
    relational_query::{VortexRelationalAggregate, VortexRelationalUnary},
};
use arrow_array::{Array as _, ListArray, types::Int64Type};

#[test]
fn parquet_demand_cardinality_uses_the_already_required_predicate_field() {
    let fixture = ParquetFixture::new();
    let prepared = fixture.prepare(&count_tests::count(
        fixture.scan(&[], Some(equal("other", 105))),
    ));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![serde_json::json!({"rows": 1})]);
    let report = result.execution.input.as_ref().unwrap();
    assert_eq!(report.metadata_count_sources, 0);
    assert_eq!(report.columnar_projected_columns, 1);
    assert_eq!(report.columnar_physical_leaf_columns, 1);
    assert!(report.columnar_rows_decoded > 0);
}

#[test]
fn parquet_demand_cardinality_does_not_read_an_unneeded_row_carrier() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(1, 0);
    let prepared = fixture.prepare(&count_tests::count(
        fixture.scan(&[], Some(equal("other", 105))),
    ));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![serde_json::json!({"rows": 1})]);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        1
    );
}

#[test]
fn parquet_demand_cardinality_reaches_explode_without_losing_null_or_empty_lists() {
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        Some(vec![]),
        None,
        Some(vec![Some(3), None]),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Utf8, false),
            Field::new("group", DataType::Int64, false),
            Field::new("items", items.data_type().clone(), true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int64Array::from(vec![0, 0, 1, 1])),
            Arc::new(items),
        ],
    )
    .unwrap();
    let fixture = ParquetFixture::with_batch(batch, EnabledStatistics::Chunk);
    let mut request = crate::query_primitive::VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::ExplodeRows,
        ProjectionRequest::All,
    );
    request.explode_projection = Some(VortexExplodeProjectionRequest::new(
        ColumnRef::new("items").unwrap(),
    ));
    let input = VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary {
        input: fixture.scan(&[], None),
        request,
    }));
    let plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: vec![],
        grouping_sets: None,
        measures: vec![
            VortexSimpleAggregateMeasure::new("count", None, "rows".into()),
            VortexSimpleAggregateMeasure::new(
                "count",
                Some(ColumnRef::new("items").unwrap()),
                "present".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("items").unwrap()),
                "total".into(),
            ),
        ],
    }));
    let prepared = fixture.prepare(&plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![serde_json::json!({"rows": 5, "present": 3, "total": 6.0})]
    );
    let report = result.execution.input.as_ref().unwrap();
    assert_eq!(report.columnar_projected_columns, 1);
    assert_eq!(report.columnar_physical_leaf_columns, 1);
    assert_eq!(report.columnar_rows_decoded, 4);
}

#[test]
fn parquet_demand_cardinality_keeps_nullable_conjuncts_aliases_and_zero_matches() {
    let fixture = ParquetFixture::new();
    // The carrier itself is nullable. COUNT(*) must include selected NULL rows.
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::AlwaysTrue,
        PredicateExpr::IsNull {
            column: ColumnRef::new("text").unwrap(),
        },
    ]);
    let input = project(fixture.scan(&[], Some(predicate)), &[("text", "nullable")]);
    let plan = count_tests::count(project(input, &[("nullable", "alias")]));
    for equivalent in [plan.clone(), plan.compact_streaming()] {
        let prepared = fixture.prepare(&equivalent);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![serde_json::json!({"rows":4})]);
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .columnar_projected_columns,
            1
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
    let prepared = fixture.prepare(&count_tests::count(
        fixture.scan(&[], Some(equal("other", -1))),
    ));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![serde_json::json!({"rows":0})]);

    // A hidden predicate also bears the selected rows. A cardinality consumer
    // need not read the unused value column from an owned demanded source.
    fixture.corrupt_chunk(1, 0);
    let prepared = fixture.prepare(&count_tests::count(
        fixture.scan(&["n"], Some(equal("other", 105))),
    ));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![serde_json::json!({"rows":1})]);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        1
    );
    let mut value_count = count_tests::count(fixture.scan(&["n"], Some(equal("other", 105))));
    let VortexRelationalPlan::Aggregate(aggregate) = &mut value_count else {
        unreachable!()
    };
    aggregate.measures[0] = VortexSimpleAggregateMeasure::new(
        "count",
        Some(ColumnRef::new("n").unwrap()),
        "rows".into(),
    );
    assert!(
        fixture
            .prepare(&value_count)
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn parquet_demand_cardinality_constant_outputs_and_late_predicate_errors_stay_observed() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(1, 0);
    let plan = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], Some(equal("other", 105))),
        expressions: vec![(
            "constant".into(),
            Expression::literal(ExprId::new("constant").unwrap(), ScalarValue::Int64(7)),
        )],
    }));
    let prepared = fixture.prepare(&plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![serde_json::json!({"constant":7})]);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        1
    );

    let fixture = ParquetFixture::with_batch(ParquetFixture::batch(), EnabledStatistics::None);
    fixture.corrupt_chunk(3, 1);
    let prepared = fixture.prepare(&count_tests::count(
        fixture.scan(&[], Some(equal("other", 105))),
    ));
    let initial = prepared.snapshot().memory.reserved_bytes;
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

fn explode(
    input: VortexRelationalPlan,
    request: crate::query_primitive::VortexQueryPrimitiveRequest,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
}

fn explode_request(names: &[&str]) -> crate::query_primitive::VortexQueryPrimitiveRequest {
    let mut request = crate::query_primitive::VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::ExplodeRows,
        ProjectionRequest::All,
    );
    request.explode_projection = Some(
        VortexExplodeProjectionRequest::new(ColumnRef::new(names[0]).unwrap()).with_columns(
            names
                .iter()
                .map(|name| ColumnRef::new(*name).unwrap())
                .collect(),
        ),
    );
    request
}

fn list_batch(mismatch: bool) -> RecordBatch {
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        Some(vec![]),
        None,
        Some(vec![Some(3), None]),
    ]);
    let other = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(10), Some(20)]),
        Some(vec![]),
        None,
        Some(if mismatch {
            vec![Some(30)]
        } else {
            vec![Some(30), None]
        }),
    ]);
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Utf8, false),
            Field::new("group", DataType::Int64, false),
            Field::new("items", items.data_type().clone(), true),
            Field::new("other", other.data_type().clone(), true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int64Array::from(vec![0, 0, 1, 1])),
            Arc::new(items),
            Arc::new(other),
        ],
    )
    .unwrap()
}

#[test]
fn parquet_demand_explode_keeps_all_zip_lists_and_hidden_predicates_for_count() {
    for mismatch in [false, true] {
        let fixture = ParquetFixture::with_batch(list_batch(mismatch), EnabledStatistics::None);
        fixture.corrupt_chunk(0, 0);
        let mut request = explode_request(&["items", "other"]);
        let plan = count_tests::count(explode(fixture.scan(&[], None), request.clone()));
        let prepared = fixture.prepare(&plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared.collect_jsonl(&CancellationToken::default());
        if mismatch {
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("equal list lengths")
            );
            assert_eq!(prepared.snapshot().completed_executions, 0);
        } else {
            let result = result.unwrap();
            assert_eq!(json_rows(&result), vec![serde_json::json!({"rows":5})]);
            assert_eq!(
                result
                    .execution
                    .input
                    .as_ref()
                    .unwrap()
                    .columnar_projected_columns,
                2
            );
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        // This predicate executes before expansion, so its excluded mismatch
        // is not an error, but the hidden group column is still read.
        request.predicate = Some(equal("group", 0));
        let prepared = fixture.prepare(&count_tests::count(explode(
            fixture.scan(&[], None),
            request,
        )));
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![serde_json::json!({"rows":2})]);
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .columnar_projected_columns,
            3
        );
    }
}

#[test]
fn parquet_demand_explode_explicit_projection_works_without_a_downstream_consumer() {
    let fixture = ParquetFixture::with_batch(list_batch(false), EnabledStatistics::Chunk);
    fixture.corrupt_chunk(0, 0);
    let mut request = explode_request(&["items"]);
    request.projection = ProjectionRequest::Columns(vec![ColumnRef::new("items").unwrap()]);
    let plan = explode(fixture.scan(&[], None), request);
    let prepared = fixture.prepare(&plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        [
            1.into(),
            2.into(),
            serde_json::Value::Null,
            3.into(),
            serde_json::Value::Null
        ]
        .map(|item| serde_json::json!({"items":item}))
    );
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        1
    );
}

#[test]
fn parquet_demand_explode_preserves_source_completion_and_unused_computation_errors() {
    let fixture = ParquetFixture::with_batch_rows(list_batch(false), EnabledStatistics::None, 1);
    fixture.corrupt_chunk(3, 2);
    let mut request = explode_request(&["items"]);
    request.source_order_limit = Some(1);
    let plan = project(
        explode(fixture.scan(&[], None), request.clone()),
        &[("items", "value")],
    );
    let prepared = fixture.prepare(&plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);

    let fixture = ParquetFixture::with_batch(list_batch(false), EnabledStatistics::None);
    let input = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], None),
        expressions: vec![
            ("items".into(), column("items")),
            (
                "unused".into(),
                Expression::cast(
                    ExprId::new("fallible").unwrap(),
                    column("unused"),
                    shardloom_core::LogicalDType::Int64,
                ),
            ),
        ],
    }));
    let plan = project(explode(input, request), &[("items", "value")]);
    for equivalent in [plan.clone(), plan.compact_streaming()] {
        let prepared = fixture.prepare(&equivalent);
        let initial = prepared.snapshot().memory.reserved_bytes;
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_demand_explode_cancellation_pressure_and_consumer_failure_refund() {
    let fixture = ParquetFixture::with_batch(list_batch(false), EnabledStatistics::Chunk);
    let plan = project(
        explode(fixture.scan(&[], None), explode_request(&["items"])),
        &[("items", "value")],
    );
    let prepared = fixture.prepare(&plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let token = CancellationToken::default();
    token.cancel();
    assert!(prepared.collect_jsonl(&token).is_err());
    let token = CancellationToken::default();
    assert!(
        prepared
            .for_each_batch(&token, |_, _| {
                token.cancel();
                Ok(())
            })
            .is_err()
    );
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| Err(failed(
                "consumer stopped"
            )))
            .is_err()
    );
    let pressure = prepared
        .session
        .memory()
        .reserve(prepared.snapshot().memory.limit_bytes - initial)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    drop(pressure);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result).len(), 5);
    assert!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .end_of_input_observed
    );
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn parquet_demand_explode_element_alias_preserves_nullable_structs_and_empty_schema() {
    use arrow_array::builder::{Int64Builder, ListBuilder, StructBuilder};
    let mut items = ListBuilder::new(StructBuilder::from_fields(
        vec![Field::new("code", DataType::Int64, true)],
        2,
    ));
    for code in [Some(10), None] {
        items
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_option(code);
        items.values().append(true);
    }
    items.append(true);
    items.append(false);
    items.append(true);
    let items = items.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Utf8, false),
            Field::new("items", items.data_type().clone(), true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(items),
        ],
    )
    .unwrap();
    let mut output_dtype = None;
    for batch in [batch.clone(), RecordBatch::new_empty(batch.schema())] {
        let empty = batch.num_rows() == 0;
        let fixture = ParquetFixture::with_batch(batch, EnabledStatistics::Chunk);
        if !empty {
            fixture.corrupt_chunk(0, 0);
        }
        let input = project(
            fixture.scan(&[], None),
            &[("unused", "ignored"), ("items", "packed")],
        );
        let mut request = explode_request(&["packed"]);
        request.explode_projection = Some(
            VortexExplodeProjectionRequest::new(ColumnRef::new("packed").unwrap())
                .with_element_field("code".into(), "element".into()),
        );
        let plan = project(explode(input, request), &[("element", "answer")]);
        for equivalent in [plan.clone(), plan.compact_streaming()] {
            let prepared = fixture.prepare(&equivalent);
            let dtype = prepared.output_dtype().unwrap();
            if let Some(prior) = &output_dtype {
                assert_eq!(prior, &dtype);
            }
            output_dtype = Some(dtype);
            let initial = prepared.snapshot().memory.reserved_bytes;
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let expected = if empty {
                vec![]
            } else {
                vec![
                    serde_json::json!({"answer":10}),
                    serde_json::json!({"answer":null}),
                    serde_json::json!({"answer":null}),
                ]
            };
            assert_eq!(json_rows(&result), expected);
            if !empty {
                assert_eq!(
                    result
                        .execution
                        .input
                        .as_ref()
                        .unwrap()
                        .columnar_projected_columns,
                    1
                );
            }
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
}

#[test]
fn parquet_demand_unary_explicit_inputs_keep_hidden_predicates_without_output_leakage() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(1, 2);
    let mut request = crate::query_primitive::VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::DistinctRows,
        ProjectionRequest::Columns(vec![ColumnRef::new("n").unwrap()]),
    );
    request.predicate = Some(equal("other", 105));
    let plan = VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary {
        input: fixture.scan(&[], None),
        request,
    }));
    let prepared = fixture.prepare(&plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![serde_json::json!({"n":5})]);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        2
    );
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}
