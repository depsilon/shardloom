use super::*;
use crate::relational_query::{
    VortexRelationalLimit, VortexRelationalNullOrder, VortexRelationalOrderKey,
    VortexRelationalSort,
};
use serde_json::json;
use shardloom_core::{BinaryOp, LogicalDType};

fn limit(input: VortexRelationalPlan, offset: usize, count: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset,
        count,
    }))
}

fn compare(name: &str, value: i64) -> Expression {
    Expression::new(
        ExprId::new("eq").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(column(name)),
            op: ComparisonOp::Eq,
            right: Box::new(Expression::literal(
                ExprId::new("value").unwrap(),
                ScalarValue::Int64(value),
            )),
        },
    )
}

#[test]
fn parquet_demand_residual_filter_keeps_hidden_dependencies_and_drops_unrelated_columns() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(1, 1);
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: fixture.scan(&[], None),
        predicate: Expression::new(
            ExprId::new("or").unwrap(),
            ExpressionKind::Binary {
                left: Box::new(compare("n", 5)),
                op: BinaryOp::Or,
                right: Box::new(compare("n", 13)),
            },
        ),
    }));
    let prepared = fixture.prepare(&project(filtered, &[("text", "label")]));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![json!({"label":"row-5-λ"}), json!({"label":"row-13-λ"})]
    );
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.columnar_projected_columns,
            input.columnar_rows_decoded
        ),
        (2, 16)
    );
    assert_eq!(input.columnar_prefix_sources, 0);
}

#[test]
fn parquet_demand_nested_ranges_and_aliases_request_one_complete_source_prefix() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(3, 0);
    let plan = limit(
        project(limit(fixture.scan(&[], None), 2, 8), &[("n", "renamed")]),
        1,
        3,
    );
    let prepared = fixture.prepare(&plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (3..6).map(|n| json!({"renamed":n})).collect::<Vec<_>>()
    );
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.columnar_projected_columns,
            input.columnar_row_groups_read,
            input.columnar_rows_decoded
        ),
        (1, 2, 6)
    );
    assert_eq!(
        (
            input.columnar_prefix_sources,
            input.columnar_requested_prefix_rows
        ),
        (1, 6)
    );
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    assert!(
        fixture
            .prepare(&fixture.scan(&["n"], None))
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn parquet_demand_zero_prefix_preserves_schema_without_reading_corrupt_pages() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(0, 0);
    let prepared = fixture.prepare(&limit(fixture.scan(&["n"], None), 9, 0));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [] as [serde_json::Value; 0]);
    assert_eq!(
        prepared
            .output_dtype()
            .unwrap()
            .as_struct_fields_opt()
            .unwrap()
            .names(),
        &FieldNames::from(["n"])
    );
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.columnar_prefix_sources,
            input.columnar_rows_decoded,
            input.columnar_data_read_requests
        ),
        (1, 0, 0)
    );
    assert!(input.end_of_input_observed);
}

#[test]
fn parquet_demand_repeated_ranges_union_prefixes_and_unrestricted_use_wins() {
    let fixture = ParquetFixture::new();
    for unrestricted in [false, true] {
        let left = limit(fixture.scan(&["n"], None), 2, 2);
        let right = if unrestricted {
            fixture.scan(&["n"], None)
        } else {
            limit(fixture.scan(&["n"], None), 8, 3)
        };
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left,
            right,
            kind: VortexRelationalSetKind::UnionAll,
        }));
        let prepared = fixture.prepare(&plan);
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let mut expected = vec![json!({"n":2}), json!({"n":3})];
        expected.extend((if unrestricted { 0..16 } else { 8..11 }).map(|n| json!({"n":n})));
        assert_eq!(json_rows(&result), expected);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_prefix_sources, u64::from(!unrestricted));
        assert_eq!(
            input.columnar_rows_decoded,
            if unrestricted { 16 } else { 11 }
        );
        assert_eq!(input.replay_sources, 1);
    }
}

#[test]
fn parquet_demand_filter_sort_and_fallible_projection_block_prefix_propagation() {
    let fixture = ParquetFixture::new();
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: fixture.scan(&["n"], None),
        predicate: compare("n", 15),
    }));
    let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: fixture.scan(&["n"], None),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("n").unwrap(),
            descending: true,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }));
    for plan in [filtered, sorted] {
        let prepared = fixture.prepare(&limit(plan, 0, 1));
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![json!({"n":15})]);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_prefix_sources, 0);
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["1", "2", "3", "invalid"]))],
    )
    .unwrap();
    let fixture = ParquetFixture::with_batch_rows(batch, EnabledStatistics::None, 2);
    for count in [0, 1] {
        let projected = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input: fixture.scan(&[], None),
            expressions: vec![(
                "casted".into(),
                Expression::cast(
                    ExprId::new("cast").unwrap(),
                    column("text"),
                    LogicalDType::Int64,
                ),
            )],
        }));
        let prepared = fixture.prepare(&limit(projected, 0, count));
        let initial = prepared.snapshot().memory.reserved_bytes;
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}
