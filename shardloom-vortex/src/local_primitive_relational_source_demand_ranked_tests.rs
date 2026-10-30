//! Fresh held-generation ranking, complete values and actual skipped chunks.
use super::*;
use crate::relational_query::{
    VortexRelationalLimit, VortexRelationalNullOrder as NullOrder, VortexRelationalOrderKey,
    VortexRelationalSort,
};
use arrow_array::{
    Array as _, Date32Array, Decimal128Array, StructArray as ArrowStructArray,
    TimestampMicrosecondArray,
    builder::{Int64Builder, ListBuilder},
};
use arrow_buffer::NullBuffer;
use serde_json::{Value, json};

#[path = "local_primitive_relational_source_demand_ranked_lifecycle_tests.rs"]
mod lifecycle;

#[path = "local_primitive_relational_source_demand_ranked_admission_tests.rs"]
mod admission;

fn key_value(id: i64) -> Option<i64> {
    (id % 11 != 0).then_some(((id * 97 + 23) % 397) % 13 - 6)
}

fn text_value(id: i64) -> Option<String> {
    (id % 7 != 2).then(|| format!("{id}-λ-{}", "source-payload-雪".repeat(24)))
}

fn expected(id: i64) -> Value {
    json!({
        "id":id,"key":key_value(id),"tie":id%3,"text":text_value(id),
        "day":id-200,"instant":(id-200)*1_000_000-1,
        "amount":format!("decimal128(12,2):{}",(id-200)*321),
        "items":if id%5==0 {json!(null)} else {json!([id,null,-id])},
        "record":if id%5==0 {json!(null)} else {json!({"value":(id%9!=0).then_some(id-17)})},
    })
}

fn batch(start: i64, rows: usize) -> RecordBatch {
    let ids: Vec<_> = (start..start + i64::try_from(rows).unwrap()).collect();
    let mut lists = ListBuilder::new(Int64Builder::new());
    for &id in &ids {
        if id % 5 == 0 {
            lists.append(false);
        } else {
            lists.values().append_value(id);
            lists.values().append_null();
            lists.values().append_value(-id);
            lists.append(true);
        }
    }
    let lists = lists.finish();
    let record = ArrowStructArray::new(
        vec![Field::new("value", DataType::Int64, true)].into(),
        vec![Arc::new(Int64Array::from_iter(
            ids.iter().map(|id| (id % 9 != 0).then_some(id - 17)),
        ))],
        Some(NullBuffer::from(
            ids.iter().map(|id| id % 5 != 0).collect::<Vec<_>>(),
        )),
    );
    let arrays: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.clone())),
        Arc::new(Int64Array::from_iter(ids.iter().copied().map(key_value))),
        Arc::new(Int64Array::from_iter_values(ids.iter().map(|id| id % 3))),
        Arc::new(StringArray::from_iter(ids.iter().copied().map(text_value))),
        Arc::new(Date32Array::from_iter_values(
            ids.iter().map(|id| i32::try_from(id - 200).unwrap()),
        )),
        Arc::new(TimestampMicrosecondArray::from_iter_values(
            ids.iter().map(|id| (id - 200) * 1_000_000 - 1),
        )),
        Arc::new(
            Decimal128Array::from_iter_values(ids.iter().map(|id| i128::from((id - 200) * 321)))
                .with_precision_and_scale(12, 2)
                .unwrap(),
        ),
        Arc::new(lists),
        Arc::new(record),
    ];
    let names = [
        "id", "key", "tie", "text", "day", "instant", "amount", "items", "record",
    ];
    let fields = names
        .iter()
        .zip(&arrays)
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn fixture(start: i64, rows: usize, group_rows: usize) -> ParquetFixture {
    ParquetFixture::with_properties(
        batch(start, rows),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(group_rows))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build(),
    )
}

fn sorted(
    input: VortexRelationalPlan,
    keys: Vec<VortexRelationalOrderKey>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort { input, keys }))
}

fn key(name: &str, descending: bool, nulls: NullOrder) -> VortexRelationalOrderKey {
    VortexRelationalOrderKey {
        column: ColumnRef::new(name).unwrap(),
        descending,
        nulls: Some(nulls),
    }
}

fn limit(input: VortexRelationalPlan, offset: usize, count: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset,
        count,
    }))
}

fn reference(
    mut ids: Vec<i64>,
    descending: bool,
    nulls: NullOrder,
    offset: usize,
    count: usize,
) -> Vec<Value> {
    ids.sort_by(|left, right| {
        let order = match (key_value(*left), key_value(*right)) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => {
                if nulls == NullOrder::First {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (Some(_), None) => {
                if nulls == NullOrder::First {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (Some(left), Some(right)) => {
                if descending {
                    right.cmp(&left)
                } else {
                    left.cmp(&right)
                }
            }
        };
        order.then_with(|| (right % 3).cmp(&(left % 3)))
    });
    ids.into_iter()
        .skip(offset)
        .take(count)
        .map(expected)
        .collect()
}

#[test]
fn ranked_demand_stable_rich_values_offsets_and_zero_prefix_reduce_actual_chunk_reads() {
    let fixture = fixture(0, 397, 7);
    let full = fixture
        .prepare(&fixture.scan(&[], None))
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let full_bytes = full
        .execution
        .input
        .as_ref()
        .unwrap()
        .columnar_data_read_bytes;
    drop(full);
    for (descending, nulls) in [
        (false, NullOrder::First),
        (true, NullOrder::Last),
        (true, NullOrder::First),
        (false, NullOrder::Last),
    ] {
        for (offset, count) in [(0, 1), (3, 7), (17, 0)] {
            let plan = limit(
                sorted(
                    fixture.scan(&[], None),
                    vec![
                        key("key", descending, nulls),
                        key("tie", true, NullOrder::Last),
                    ],
                ),
                offset,
                count,
            );
            let prepared = fixture.prepare(&plan);
            let initial = prepared.snapshot().memory.reserved_bytes;
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                json_rows(&result),
                reference((0..397).collect(), descending, nulls, offset, count)
            );
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.ranked_sources, 1, "{input:?}");
            assert_eq!(
                (
                    input.ranked_rank_columns,
                    input.ranked_rank_rows,
                    input.ranked_rank_groups
                ),
                (2, 397, 57)
            );
            assert_eq!(
                input.ranked_selected_rows,
                if count == 0 {
                    0
                } else {
                    (offset + count) as u64
                }
            );
            assert_eq!(input.columnar_row_groups_read, 57);
            assert_eq!(input.columnar_rows_decoded, 397 + input.ranked_payload_rows);
            assert_eq!(
                input.columnar_data_read_bytes,
                input.ranked_rank_read_bytes + input.ranked_payload_read_bytes
            );
            assert!(input.columnar_data_read_bytes < full_bytes);
            assert!(input.ranked_payload_groups <= input.ranked_selected_rows);
            if count == 0 {
                assert_eq!(
                    (input.ranked_payload_groups, input.ranked_payload_read_bytes),
                    (0, 0)
                );
            }
            assert!(input.end_of_input_observed && input.output_ownership_detached);
            assert!(result.execution.native_io_certificate.is_certified());
            assert!(
                result
                    .execution
                    .native_io_certificate
                    .source_pushdown_report
                    .proof_basis
                    .contains(
                        "complete_rank_dependencies_before_complete_decoded_winning_group_payload"
                    )
            );
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
}

#[test]
fn ranked_demand_maps_renamed_repeated_and_hidden_keys_through_direct_projections() {
    let fixture = fixture(0, 397, 7);
    let projected = project(
        project(
            fixture.scan(&[], None),
            &[
                ("key", "k"),
                ("id", "id"),
                ("text", "label"),
                ("text", "again"),
            ],
        ),
        &[
            ("label", "payload"),
            ("k", "__shardloom_source_position"),
            ("again", "repeated"),
            ("id", "row"),
        ],
    );
    let plan = project(
        limit(
            sorted(
                projected,
                vec![key("__shardloom_source_position", false, NullOrder::First)],
            ),
            2,
            5,
        ),
        &[
            ("payload", "payload"),
            ("repeated", "repeated"),
            ("row", "row"),
        ],
    );
    let result = fixture
        .prepare(&plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let mut ids: Vec<i64> = (0..397).collect();
    ids.sort_by_key(|id| key_value(*id));
    let expected = ids
        .into_iter()
        .skip(2)
        .take(5)
        .map(|id| json!({"payload":text_value(id),"repeated":text_value(id),"row":id}))
        .collect::<Vec<_>>();
    assert_eq!(json_rows(&result), expected);
    assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 1);
}

#[test]
fn ranked_demand_predicate_metadata_exclusions_preserve_absolute_positions() {
    let fixture = fixture(0, 397, 7);
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("id").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(71),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("id").unwrap(),
            op: ComparisonOp::Lt,
            value: StatValue::Int64(269),
        },
    ]);
    let plan = limit(
        sorted(
            fixture.scan(&[], Some(predicate)),
            vec![
                key("key", false, NullOrder::Last),
                key("tie", true, NullOrder::Last),
            ],
        ),
        1,
        5,
    );
    let result = fixture
        .prepare(&plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        reference((71..269).collect(), false, NullOrder::Last, 1, 5)
    );
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!((input.ranked_sources, input.ranked_rank_columns), (1, 3));
    assert!(
        input.ranked_rank_groups < 57 && input.ranked_payload_groups < input.ranked_rank_groups
    );
    assert_eq!(input.columnar_row_groups_read, input.ranked_rank_groups);
}

#[test]
fn ranked_demand_directory_positions_ties_and_native_output_survive_source_closure() {
    let first = fixture(0, 197, 7);
    let fixture = fixture(197, 200, 7);
    fs::copy(first.path(), fixture.0.join("a.parquet")).unwrap();
    let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(fixture.0.display().to_string()).unwrap(),
        projection: ProjectionRequest::All,
        predicate: None,
    });
    let plan = limit(
        sorted(
            scan,
            vec![
                key("key", true, NullOrder::First),
                key("tie", true, NullOrder::Last),
            ],
        ),
        3,
        7,
    );
    let prepared = prepare_relational_with_schema(policy(), |preparation| {
        let source = LocalColumnarSource::admit(
            &fixture.0,
            "parquet",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation
            .register_local_source(DatasetUri::new(fixture.0.display().to_string())?, source)?;
        Ok(plan)
    })
    .unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        reference((0..397).collect(), true, NullOrder::First, 3, 7)
    );
    assert_eq!(result.execution.input.as_ref().unwrap().columnar_files, 2);
    assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 1);
    let output = fixture.0.join("selected.vortex");
    prepared
        .write(
            &output,
            crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
        )
        .unwrap();
    let native = prepare_relational(
        &VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(output.display().to_string()).unwrap(),
            projection: ProjectionRequest::All,
            predicate: None,
        }),
        policy(),
    )
    .unwrap();
    assert_eq!(
        native.output_dtype().unwrap(),
        prepared.output_dtype().unwrap()
    );
    assert_eq!(
        json_rows(&native.collect_jsonl(&CancellationToken::default()).unwrap()),
        json_rows(&result)
    );
    drop((prepared, first, fixture));
    assert_eq!(
        json_rows(&result),
        reference((0..397).collect(), true, NullOrder::First, 3, 7)
    );
}

#[test]
fn ranked_demand_declines_single_group_all_keys_and_large_prefix() {
    for (groups, columns, count) in [(397, vec![], 3), (7, vec!["id"], 3), (7, vec![], 397)] {
        let fixture = fixture(0, 397, groups);
        let plan = limit(
            sorted(
                fixture.scan(&columns, None),
                vec![key("id", false, NullOrder::Last)],
            ),
            0,
            count,
        );
        let result = fixture
            .prepare(&plan)
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let expected = (0..i64::try_from(count).unwrap())
            .map(|id| {
                if columns.is_empty() {
                    expected(id)
                } else {
                    json!({"id":id})
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(json_rows(&result), expected);
        assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 0);
    }
    let fixture = fixture(0, 0, 7);
    let result = fixture
        .prepare(&limit(
            sorted(
                fixture.scan(&[], None),
                vec![key("id", false, NullOrder::Last)],
            ),
            0,
            5,
        ))
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [] as [Value; 0]);
    assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 0);
}

#[test]
fn ranked_demand_validation_observes_rank_first_and_every_decoded_winning_group() {
    for (group, column, count, success) in [
        (56, 3, 1, true),
        (0, 3, 1, false),
        (56, 0, 1, false),
        (0, 3, 0, true),
        (56, 0, 0, false),
    ] {
        let fixture = fixture(0, 397, 7);
        fixture.corrupt_chunk(group, column);
        let plan = limit(
            sorted(
                fixture.scan(&[], None),
                vec![key("id", false, NullOrder::Last)],
            ),
            0,
            count,
        );
        let prepared = fixture.prepare(&plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared.collect_jsonl(&CancellationToken::default());
        assert_eq!(
            result.is_ok(),
            success,
            "group={group} column={column} count={count}"
        );
        if let Ok(ref output) = result {
            assert_eq!(output.execution.input.as_ref().unwrap().ranked_sources, 1);
            assert_eq!(
                json_rows(output),
                (0..i64::try_from(count).unwrap())
                    .map(expected)
                    .collect::<Vec<_>>()
            );
        } else {
            assert_eq!(prepared.snapshot().completed_executions, 0);
        }
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
    let fixture = fixture(0, 397, 7);
    fixture.corrupt_chunk(0, 3);
    let plan = limit(
        sorted(
            fixture.scan(&[], None),
            vec![VortexRelationalOrderKey {
                column: ColumnRef::new("key").unwrap(),
                descending: false,
                nulls: None,
            }],
        ),
        0,
        1,
    );
    let error = fixture
        .prepare(&plan)
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("ORDER BY requires explicit NULLS"),
        "{error}"
    );
}

#[test]
fn ranked_demand_validates_unselected_rows_within_winning_groups_but_not_other_groups() {
    for bad_row in [6, 396] {
        let values = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..397)),
                Arc::new(arrow_array::Float64Array::from_iter_values((0..397).map(
                    |row| {
                        if row == bad_row {
                            f64::NAN
                        } else {
                            f64::from(row)
                        }
                    },
                ))),
            ],
        )
        .unwrap();
        let fixture = ParquetFixture::with_properties(
            values.clone(),
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(7))
                .set_dictionary_enabled(false)
                .build(),
        );
        let plan = limit(
            sorted(
                fixture.scan(&[], None),
                vec![key("id", false, NullOrder::Last)],
            ),
            0,
            1,
        );
        let result = fixture
            .prepare(&plan)
            .collect_jsonl(&CancellationToken::default());
        if bad_row == 6 {
            assert!(result.is_err());
        } else {
            let result = result.unwrap();
            assert_eq!(json_rows(&result), vec![json!({"id":0,"value":0.0})]);
            assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 1);
        }
        let uri = DatasetUri::new("memory://parquet/ranked-control").unwrap();
        let prepared = prepare_relational_with_schema(policy(), |preparation| {
            preparation.register_arrow_batch_source(
                uri.clone(),
                values.schema(),
                &CancellationToken::default(),
            )?;
            Ok(limit(
                sorted(
                    VortexRelationalPlan::Scan(VortexRelationalScan {
                        source_uri: uri,
                        projection: ProjectionRequest::All,
                        predicate: None,
                    }),
                    vec![key("id", false, NullOrder::Last)],
                ),
                0,
                1,
            ))
        })
        .unwrap();
        let mut values = Some(values);
        let mut provider = || Ok(values.take());
        assert!(
            prepared
                .with_arrow_batch_input(&mut provider)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

#[test]
fn ranked_demand_declines_computation_ranges_replay_and_spill_without_changing_results() {
    use crate::relational_query::VortexRelationalSpillPolicy;
    let fixture = fixture(0, 397, 7);
    let computed = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], None),
        expressions: vec![
            (
                "id".into(),
                Expression::new(
                    ExprId::new("computed_id").unwrap(),
                    ExpressionKind::Binary {
                        left: Box::new(column("id")),
                        op: shardloom_core::BinaryOp::Add,
                        right: Box::new(Expression::literal(
                            ExprId::new("zero").unwrap(),
                            ScalarValue::Int64(0),
                        )),
                    },
                ),
            ),
            ("text".into(), column("text")),
        ],
    }));
    let range = limit(fixture.scan(&["id", "text"], None), 0, 32);
    for input in [computed, range] {
        let prepared = fixture.prepare(&limit(
            sorted(input, vec![key("id", false, NullOrder::Last)]),
            0,
            1,
        ));
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![json!({"id":0,"text":text_value(0)})]
        );
        assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 0);
    }
    let ordered = limit(
        sorted(
            fixture.scan(&["id", "text"], None),
            vec![key("id", false, NullOrder::Last)],
        ),
        0,
        1,
    );
    let spill = fixture
        .prepare(&ordered)
        .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, 16 << 20, 1 << 20).unwrap())
        .unwrap();
    let result = spill.collect_jsonl(&CancellationToken::default()).unwrap();
    assert_eq!(
        json_rows(&result),
        vec![json!({"id":0,"text":text_value(0)})]
    );
    assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 0);
    assert!(
        result
            .execution
            .spill
            .as_ref()
            .unwrap()
            .owned_cleanup_completed
    );
    let repeated =
        VortexRelationalPlan::Set(Box::new(crate::relational_query::VortexRelationalSet {
            left: ordered.clone(),
            right: ordered,
            kind: VortexRelationalSetKind::UnionAll,
        }));
    let result = fixture
        .prepare(&repeated)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![json!({"id":0,"text":text_value(0)}); 2]
    );
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!((input.ranked_sources, input.replay_sources), (0, 1));
}

#[test]
fn ranked_demand_source_positions_cross_decoder_batch_boundaries_exactly() {
    let fixture = fixture(0, 20_003, 9000);
    let prepared = fixture.prepare(&limit(
        sorted(
            fixture.scan(&[], None),
            vec![key("id", false, NullOrder::Last)],
        ),
        0,
        1,
    ));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![expected(0)]);
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.ranked_sources,
            input.ranked_rank_rows,
            input.ranked_payload_rows
        ),
        (1, 20_003, 9000)
    );
    assert_eq!(
        (input.ranked_rank_groups, input.ranked_payload_groups),
        (3, 1)
    );
    assert_eq!(input.payload_batches, 7); // Five rank batches and two payload batches.
}
