use super::*;
use crate::resident_memory_source::{MemoryColumn, MemoryColumnValues};
use arrow_array::{
    Array as _, BooleanArray, Date32Array, Decimal128Array, TimestampMicrosecondArray,
    builder::{Int64Builder, ListBuilder},
};
use serde_json::json;

#[test]
fn parquet_demand_rejects_metadata_from_a_different_memory_owner() {
    let fixture = ParquetFixture::new();
    let foreign = shardloom_exec::live_memory::LiveMemoryPool::new(32 << 20).unwrap();
    let source = LocalColumnarSource::admit(
        &fixture.path(),
        "parquet",
        &foreign,
        &CancellationToken::default(),
    )
    .unwrap();
    let result = prepare_relational_with_schema(policy(), |preparation| {
        preparation.register_local_source(
            DatasetUri::new(fixture.path().display().to_string())?,
            Arc::clone(&source),
        )?;
        Ok(fixture.scan(&["n"], None))
    });
    let error = result
        .err()
        .expect("a separate metadata budget cannot expand this query's grant");
    assert!(
        error.to_string().contains("different memory owner"),
        "{error}"
    );
    drop(source);
    assert_eq!(foreign.snapshot().reserved_bytes, 0);
}

fn typed_batch() -> RecordBatch {
    let mut list = ListBuilder::new(Int64Builder::new());
    list.values().append_value(-9);
    list.values().append_null();
    list.append(true);
    list.append(true);
    list.append(false);
    list.values().append_value(i64::MAX);
    list.append(true);
    let list = list.finish();
    let decimal = Decimal128Array::from(vec![Some(-12345), Some(0), None, Some(999_999)])
        .with_precision_and_scale(12, 2)
        .unwrap();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("day", DataType::Date32, true),
            Field::new(
                "instant",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("amount", DataType::Decimal128(12, 2), true),
            Field::new("items", list.data_type().clone(), true),
            Field::new("flag", DataType::Boolean, true),
        ])),
        vec![
            Arc::new(Date32Array::from(vec![
                Some(-1),
                Some(20000),
                None,
                Some(0),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1),
                Some(1_700_000_000_123_456),
                None,
                Some(0),
            ])),
            Arc::new(decimal),
            Arc::new(list),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
        ],
    )
    .unwrap()
}

#[test]
fn parquet_demand_typed_nested_values_empty_input_and_native_output_remain_exact() {
    let batch = typed_batch();
    let empty = RecordBatch::new_empty(batch.schema());
    let fixture = ParquetFixture::with_batch(batch, EnabledStatistics::Chunk);
    let prepared = fixture.prepare(&fixture.scan(&[], None));
    let expected = vec![
        json!({"day":-1,"instant":-1,"amount":"decimal128(12,2):-12345","items":[-9,null],"flag":true}),
        json!({"day":20000,"instant":1_700_000_000_123_456_i64,"amount":"decimal128(12,2):0","items":[],"flag":false}),
        json!({"day":null,"instant":null,"amount":null,"items":null,"flag":null}),
        json!({"day":0,"instant":0,"amount":"decimal128(12,2):999999","items":[i64::MAX],"flag":true}),
    ];
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&collected), expected);
    let destination = fixture.0.join("native.vortex");
    prepared
        .write(
            &destination,
            crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
        )
        .unwrap();
    let native = prepare_relational(
        &VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(destination.display().to_string()).unwrap(),
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
        expected
    );
    let fixture = ParquetFixture::with_batch(empty, EnabledStatistics::Chunk);
    let empty = fixture.prepare(&fixture.scan(&[], None));
    assert_eq!(
        empty.output_dtype().unwrap(),
        prepared.output_dtype().unwrap()
    );
    let collected = empty.collect_jsonl(&CancellationToken::default()).unwrap();
    assert_eq!(json_rows(&collected), [] as [serde_json::Value; 0]);
    let input = collected.execution.input.as_ref().unwrap();
    assert_eq!(
        (input.columnar_rows_decoded, input.columnar_data_read_bytes),
        (0, 0)
    );
    assert!(input.end_of_input_observed);
}

#[test]
fn parquet_demand_mixed_native_file_and_caller_batches_keep_independent_owners() {
    let fixture = ParquetFixture::new();
    let native = Fixture::new(
        StructArray::from_fields(&[("n", PrimitiveArray::from_iter([90i64, 91]).into_array())])
            .unwrap()
            .into_array(),
        1,
    );
    let stream = DatasetUri::new("memory://mixed-demand").unwrap();
    let union = |left, right| {
        VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left,
            right,
            kind: VortexRelationalSetKind::UnionAll,
        }))
    };
    let plan = union(
        union(fixture.scan(&["n"], Some(equal("n", 5))), native.scan()),
        VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: stream.clone(),
            projection: ProjectionRequest::All,
            predicate: None,
        }),
    );
    let prepared = prepare_relational_with_schema(policy(), |preparation| {
        let local = LocalColumnarSource::admit(
            &fixture.path(),
            "parquet",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation.register_local_source(
            DatasetUri::new(fixture.path().display().to_string())?,
            local,
        )?;
        preparation.register_batch_source(stream, |session| {
            ResidentMemorySource::from_batch_columns(
                session,
                &[MemoryColumn {
                    name: "n",
                    values: MemoryColumnValues::Int64(&[]),
                }],
            )
        })?;
        Ok(plan)
    })
    .unwrap();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let mut calls = 0;
    let mut provider = |session: &ResidentVortexSession| {
        calls += 1;
        if calls == 1 {
            ResidentMemorySource::from_batch_columns(
                session,
                &[MemoryColumn {
                    name: "n",
                    values: MemoryColumnValues::Int64(&[Some(1000), None]),
                }],
            )
            .map(Some)
        } else {
            Ok(None)
        }
    };
    let collected = prepared
        .with_batch_input(&mut provider)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&collected),
        vec![
            json!({"n":5}),
            json!({"n":90}),
            json!({"n":91}),
            json!({"n":1000}),
            json!({"n":null})
        ]
    );
    assert_eq!(calls, 2);
    assert_eq!(collected.execution.prepared_file_sources, 1);
    let input = collected.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.sources,
            input.columnar_sources,
            input.columnar_projected_columns
        ),
        (2, 1, 1)
    );
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    drop(collected);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn parquet_demand_selected_decoder_workspace_competes_with_native_memory_grant() {
    let texts = (0..4)
        .map(|n| format!("{n}{}", "x".repeat(1 << 20)))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("wide", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..4)),
            Arc::new(StringArray::from_iter_values(&texts)),
        ],
    )
    .unwrap();
    let fixture = ParquetFixture::with_batch(batch, EnabledStatistics::Chunk);
    let mut resources = policy();
    resources.resource_envelope.memory_budget_bytes = 2 << 20;
    for (columns, succeeds) in [(&["n"][..], true), (&["n", "wide"][..], false)] {
        let prepared = prepare_relational_with_schema(resources, |preparation| {
            let local = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                &preparation.shared_memory_pool(),
                &preparation.cancellation_token(),
            )?;
            preparation.register_local_source(
                DatasetUri::new(fixture.path().display().to_string())?,
                local,
            )?;
            Ok(fixture.scan(columns, None))
        })
        .unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared.collect_jsonl(&CancellationToken::default());
        if succeeds {
            let collected = result.unwrap();
            assert_eq!(
                json_rows(&collected),
                (0..4).map(|n| json!({"n":n})).collect::<Vec<_>>()
            );
            assert!(
                collected
                    .execution
                    .input
                    .as_ref()
                    .unwrap()
                    .columnar_data_read_bytes
                    < 1024
            );
        } else {
            let error = result
                .err()
                .expect("selected chunk cannot fit the decoder workspace grant");
            assert!(
                error.to_string().contains("memory reservation denied"),
                "{error}"
            );
            assert_eq!(prepared.snapshot().completed_executions, 0);
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_demand_streams_output_larger_than_the_grant_one_batch_at_a_time() {
    let row_count = 131_072;
    let texts = (0..row_count)
        .map(|n| format!("{n:06}{}", "x".repeat(128)))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..row_count)),
            Arc::new(StringArray::from_iter_values(&texts)),
        ],
    )
    .unwrap();
    let fixture = ParquetFixture::with_batch_rows(batch, EnabledStatistics::Chunk, 4096);
    let mut resources = policy();
    resources.resource_envelope.memory_budget_bytes = 8 << 20;
    let prepared = prepare_relational_with_schema(resources, |preparation| {
        let local = LocalColumnarSource::admit(
            &fixture.path(),
            "parquet",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation.register_local_source(
            DatasetUri::new(fixture.path().display().to_string())?,
            local,
        )?;
        Ok(fixture.scan(&[], None))
    })
    .unwrap();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let mut delivered = 0;
    let mut batches = 0;
    let result = prepared
        .for_each_batch(&CancellationToken::default(), |batch, context| {
            assert!(batch.len() <= BATCH_ROWS);
            delivered += batch.len();
            batches += 1;
            // A synchronous consumer can hold the batch without reader prefetch.
            std::thread::yield_now();
            assert!(context.memory().snapshot().reserved_bytes <= 8 << 20);
            Ok(())
        })
        .unwrap();
    assert_eq!(delivered, 131_072);
    // Native output may split a physical row group into smaller deliveries.
    assert!(batches >= 32);
    let input = result.input.as_ref().unwrap();
    assert!(input.input_logical_bytes > 8 << 20);
    assert_eq!(input.max_retained_input_batches, 1);
    assert_eq!(
        (input.columnar_row_groups_read, input.columnar_rows_decoded),
        (32, 131_072)
    );
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn parquet_demand_late_decode_failure_delivers_only_provisional_batches() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(3, 0);
    let prepared = fixture.prepare(&fixture.scan(&["n"], None));
    let initial = prepared.snapshot().memory.reserved_bytes;
    let mut delivered = 0;
    let result = prepared.for_each_batch(&CancellationToken::default(), |batch, _| {
        delivered += batch.len();
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(delivered, 12);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}
