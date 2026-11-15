//! Ordering preserves hidden keys while propagating downstream column demand.

use super::*;
use crate::relational_query::{
    VortexRelationalLimit, VortexRelationalNullOrder, VortexRelationalOrderKey,
    VortexRelationalSort,
};
use arrow_array::builder::{Float64Builder, ListBuilder};
use serde_json::json;

fn ordered(input: VortexRelationalPlan, key: &str, descending: bool) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new(key).unwrap(),
            descending,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }))
}

fn limited(input: VortexRelationalPlan, offset: usize, count: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset,
        count,
    }))
}

#[test]
fn parquet_demand_sort_keeps_hidden_keys_and_skips_unused_corrupt_payload() {
    let fixture = ParquetFixture::new();
    for group in 0..4 {
        fixture.corrupt_chunk(group, 2);
    }
    let aliases = project(
        fixture.scan(&[], None),
        &[("text", "unused"), ("n", "value"), ("other", "hidden_key")],
    );
    let plan = project(
        limited(ordered(aliases, "hidden_key", true), 1, 3),
        &[("value", "result")],
    );
    for equivalent in [plan.clone(), plan.compact_streaming()] {
        let prepared = fixture.prepare(&equivalent);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            [
                json!({"result":14}),
                json!({"result":13}),
                json!({"result":12})
            ]
        );
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_projected_columns, 2);
        assert_eq!(input.columnar_physical_leaf_columns, 2);
        assert_eq!(input.columnar_row_groups_read, 4);
        assert_eq!(input.columnar_rows_decoded, 16);
        assert_eq!(input.columnar_prefix_sources, 0);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_demand_sort_observes_late_hidden_key_and_computation_failures() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(3, 1);
    let plan = project(
        limited(ordered(fixture.scan(&[], None), "other", false), 0, 1),
        &[("n", "result")],
    );
    for equivalent in [plan.clone(), plan.compact_streaming()] {
        let prepared = fixture.prepare(&equivalent);
        let initial = prepared.snapshot().memory.reserved_bytes;
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }

    let fixture = ParquetFixture::with_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("text", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["0", "1", "2", "3", "invalid"])),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
    );
    let computed = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], None),
        expressions: vec![
            ("key".into(), column("n")),
            (
                "unused".into(),
                Expression::cast(
                    ExprId::new("fallible").unwrap(),
                    column("text"),
                    shardloom_core::LogicalDType::Int64,
                ),
            ),
        ],
    }));
    let plan = project(
        limited(ordered(computed, "key", false), 0, 1),
        &[("key", "result")],
    );
    for equivalent in [plan.clone(), plan.compact_streaming()] {
        let prepared = fixture.prepare(&equivalent);
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

#[test]
fn parquet_demand_sort_preserves_nullable_keys_stable_ties_and_empty_schema() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("key", DataType::Int64, true),
            Field::new("unused", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![
                Some(2),
                None,
                Some(1),
                Some(2),
                None,
            ])),
            Arc::new(StringArray::from(vec!["unused"; 5])),
        ],
    )
    .unwrap();
    let empty = RecordBatch::new_empty(batch.schema());
    for (batch, expected) in [
        (
            batch,
            vec![
                json!({"result":2}),
                json!({"result":0}),
                json!({"result":3}),
                json!({"result":1}),
                json!({"result":4}),
            ],
        ),
        (empty, vec![]),
    ] {
        let fixture = ParquetFixture::with_batch(batch, EnabledStatistics::Chunk);
        let plan = project(
            ordered(fixture.scan(&[], None), "key", false),
            &[("n", "result")],
        );
        let prepared = fixture.prepare(&plan);
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .columnar_projected_columns,
            2
        );
        assert_eq!(
            prepared.output_dtype().unwrap(),
            DType::struct_(
                [(
                    "result",
                    DType::Primitive(PType::I64, Nullability::NonNullable)
                )],
                Nullability::NonNullable,
            )
        );
    }
}

#[test]
fn parquet_demand_sort_rebinds_raw_vector_field_observation_after_pruning() {
    use arrow_array::Array as _;

    // Compatibility input validates values before vector observation. Keep that
    // boundary while checking raw-field policy rebinding and exact delivered bits.
    let bits = [1.25_f64.to_bits(), 2.5_f64.to_bits(), (-0.0_f64).to_bits()];
    let mut vectors = ListBuilder::new(Float64Builder::new());
    for value in bits {
        vectors.values().append_value(f64::from_bits(value));
        vectors.append(true);
    }
    let vectors = vectors.finish();
    let fixture = ParquetFixture::with_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("unused", DataType::Utf8, false),
                Field::new("n", DataType::Int64, false),
                Field::new("v", vectors.data_type().clone(), false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["unused"; 3])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(vectors),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
    );
    fixture.corrupt_chunk(0, 0);
    let selected = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], None),
        expressions: vec![
            ("unused".into(), column("unused")),
            ("key".into(), column("n")),
            (
                "raw".into(),
                Expression::new(
                    ExprId::new("element").unwrap(),
                    ExpressionKind::FunctionCall {
                        name: "vector_element".into(),
                        args: vec![
                            column("v"),
                            Expression::literal(
                                ExprId::new("index").unwrap(),
                                ScalarValue::Int64(0),
                            ),
                        ],
                    },
                ),
            ),
        ],
    }));
    let plan = project(ordered(selected, "key", true), &[("raw", "result")]);
    for equivalent in [plan.clone(), plan.compact_streaming()] {
        let prepared = fixture.prepare(&equivalent);
        let PreparedRoot::Bound(root) = &prepared.root else {
            panic!("expected bound source-demand plan");
        };
        let (NodeKind::Project { input: sort, .. } | NodeKind::Pipeline { input: sort, .. }) =
            root.semantic_kind()
        else {
            panic!("expected final field selection");
        };
        let NodeKind::Sort { spec, .. } = sort.semantic_kind() else {
            panic!("expected ordering boundary");
        };
        assert_eq!(
            spec.fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["key", "raw"]
        );
        assert_eq!(spec.unobserved_fields, [1]);
        let memory = prepared.session.memory().clone();
        let mut actual = Vec::new();
        let mut held = Vec::new();
        let report = prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                let mut execution = context.native_session().create_execution_ctx();
                let values =
                    crate::local_primitives::logical_field_from_native_array(&array, "result")?
                        .execute::<PrimitiveArray>(&mut execution)
                        .map_err(vortex_error)?;
                actual.extend(
                    values
                        .to_buffer::<f64>()
                        .iter()
                        .map(|value| value.to_bits()),
                );
                held.push(array);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, bits.into_iter().rev().collect::<Vec<_>>());
        assert_eq!(report.input.as_ref().unwrap().columnar_projected_columns, 2);
        assert!(report.native_io_certificate.is_certified());
        drop((report, prepared));
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(held);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
