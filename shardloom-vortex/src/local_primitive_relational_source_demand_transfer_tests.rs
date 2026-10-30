//! Actual private decoder admission, counted at the shared native query intake.
use super::*;
use arrow_array::Float64Array;
use serde_json::json;

#[path = "local_primitive_relational_source_demand_transfer_types_tests.rs"]
mod types_tests;

fn numeric_fixture(rows: usize, group_rows: usize, dictionary: bool) -> ParquetFixture {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|n| i64::try_from(n).unwrap() * 17 - 91),
            )),
            Arc::new(Float64Array::from_iter((0..rows).map(|n| {
                (n % 11 != 3).then_some(if n % 7 == 0 {
                    -0.0
                } else {
                    f64::from(u32::try_from(n).unwrap()) / 8.0
                })
            }))),
        ],
    )
    .unwrap();
    ParquetFixture::with_properties(
        batch,
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(group_rows))
            .set_dictionary_enabled(dictionary)
            .build(),
    )
}

fn referenced_bytes(fixture: &ParquetFixture) -> u64 {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(fixture.path()).unwrap()).unwrap();
    let groups = builder.metadata().num_row_groups();
    (0..groups)
        .map(|group| {
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(fixture.path()).unwrap())
                .unwrap()
                .with_row_groups(vec![group])
                .with_batch_size(8192)
                .build()
                .unwrap()
                .map(|batch| {
                    crate::vortex_ingest::arrow_ownership::batch_copy_buffer_bytes(&batch.unwrap())
                        .unwrap()
                })
                .sum::<u64>()
        })
        .sum()
}

#[test]
fn parquet_query_transfer_counts_actual_buffers_and_preserves_all_rows() {
    for dictionary in [false, true] {
        for (rows, group_rows) in [(0, 17), (1, 1), (131, 17), (16_385, 16_384)] {
            let fixture = numeric_fixture(rows, group_rows, dictionary);
            let prepared = fixture.prepare(&fixture.scan(&[], None));
            let initial = prepared.snapshot().memory.reserved_bytes;
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let expected = (0..rows).map(|n| json!({
                "n":i64::try_from(n).unwrap() * 17 - 91,
                "value":(n % 11 != 3).then_some(if n % 7 == 0 { -0.0 } else { f64::from(u32::try_from(n).unwrap()) / 8.0 }),
            })).collect::<Vec<_>>();
            assert_eq!(json_rows(&result), expected);
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.rows, rows as u64);
            assert!(input.end_of_input_observed && input.output_ownership_detached);
            assert!(
                input.intake_columnar_allocation_bytes_transferred
                    >= input.intake_columnar_buffer_bytes_transferred
            );
            let referenced = referenced_bytes(&fixture);
            assert_eq!(
                input.intake_columnar_buffer_bytes_copied
                    + input.intake_columnar_buffer_bytes_transferred,
                referenced
            );
            if rows >= 128 && group_rows >= 128 {
                assert!(
                    input.intake_columnar_buffer_bytes_transferred > 0,
                    "{input:?}"
                );
                assert!(input.intake_columnar_buffer_regions_transferred > 0);
                assert!(
                    input.intake_columnar_buffer_bytes_copied < referenced,
                    "{input:?}"
                );
            }
            let proof = &result
                .execution
                .native_io_certificate
                .source_pushdown_report
                .proof_basis;
            assert!(proof.contains(&format!(
                "intake_columnar_buffer_bytes_transferred={}",
                input.intake_columnar_buffer_bytes_transferred
            )));
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
}

#[test]
fn parquet_query_transfer_keeps_demanded_projection_and_floating_bits() {
    let fixture = numeric_fixture(131, 128, false);
    // An excluded payload page is still excluded under the prior demand scope.
    fixture.corrupt_chunk(1, 1);
    let prepared = fixture.prepare(&fixture.scan(&["n"], None));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..131)
            .map(|n| json!({"n":n * 17 - 91}))
            .collect::<Vec<_>>()
    );
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(input.columnar_projected_columns, 1);
    assert_eq!(input.columnar_available_columns, 2);
    assert_eq!(
        input.intake_columnar_buffer_bytes_transferred + input.intake_columnar_buffer_bytes_copied,
        131 * 8
    );
    assert!(input.intake_columnar_buffer_bytes_transferred > 0);
    assert!(
        fixture
            .prepare(&fixture.scan(&[], None))
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );

    let fixture = numeric_fixture(131, 17, true);
    let result = fixture
        .prepare(&fixture.scan(&["value"], None))
        .execute_native(&CancellationToken::default())
        .unwrap();
    let mut context = result.result.create_execution_ctx();
    let mut row = 0;
    for batch in result.result.arrays() {
        let values = crate::local_primitives::logical_field_from_native_array(batch, "value")
            .unwrap()
            .execute::<PrimitiveArray>(&mut context)
            .unwrap();
        for i in 0..values.len() {
            if row % 11 != 3 {
                let expected = if row % 7 == 0 {
                    -0.0_f64
                } else {
                    f64::from(row) / 8.0
                };
                assert_eq!(values.as_slice::<f64>()[i].to_bits(), expected.to_bits());
            }
            row += 1;
        }
    }
    assert_eq!(row, 131);
}

#[test]
fn parquet_reader_transfer_child_outlives_reader_source_and_session() {
    let fixture = numeric_fixture(128, 128, false);
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.shared_memory_pool();
    let cancellation = CancellationToken::default();
    let (child, witness, retained_bytes) = session
        .with_native_execution_context(&cancellation, |context| {
            let local =
                LocalColumnarSource::admit(&fixture.path(), "parquet", &memory, &cancellation)?;
            let demand = shardloom_plan::SourceDemand {
                projection: ProjectionRequest::Columns(vec![ColumnRef::new("n")?]),
                ..shardloom_plan::SourceDemand::default()
            };
            let prepared = local.prepare(&demand, &session, &cancellation)?;
            let mut reader = prepared.open(context)?;
            let source = reader.next_native(&session, context)?.unwrap();
            assert_eq!(source.intake_columnar_buffer_bytes_transferred(), 128 * 8);
            assert_eq!(source.intake_columnar_buffer_regions_transferred(), 1);
            assert_eq!(source.intake_columnar_buffer_bytes_copied(), 0);
            let witness = source.batch_release_witness()?;
            let metadata = witness.upgrade().unwrap().bytes();
            let retained_bytes = source.intake_columnar_allocation_bytes_transferred() + metadata;
            let field = source.batch_array(context)?.slots()[1]
                .as_ref()
                .unwrap()
                .clone();
            let child = field.buffers()[0].slice(8..24);
            drop((source, field));
            assert!(reader.next_native(&session, context)?.is_none());
            assert_eq!(reader.report()?.rows_decoded, 128);
            drop((reader, prepared, local));
            // The execution still holds generation-validation metadata until
            // its final drain. The exact payload/structural balance is checked
            // after that context ends below.
            assert!(memory.snapshot().reserved_bytes >= retained_bytes);
            Ok((child, witness, retained_bytes))
        })
        .unwrap();
    drop((session, fixture));
    assert!(witness.upgrade().is_some());
    assert_eq!(memory.snapshot().reserved_bytes, retained_bytes);
    assert_eq!(
        i64::from_ne_bytes(child.as_slice()[..8].try_into().unwrap()),
        -74
    );
    let clone = child.clone();
    drop(child);
    assert_eq!(memory.snapshot().reserved_bytes, retained_bytes);
    std::thread::spawn(move || drop(clone)).join().unwrap();
    assert!(witness.upgrade().is_none());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn parquet_query_transfer_failure_paths_release_every_owner_and_allow_fresh_retry() {
    for failure in ["cancel", "consumer", "pressure", "generation"] {
        let fixture = numeric_fixture(128, 128, false);
        let plan = fixture.scan(&["n"], None);
        let prepared = fixture.prepare(&plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let mut calls = 0;
        let mut pressure = None;
        let error = prepared
            .for_each_batch(&token, |_, context| {
                calls += 1;
                match failure {
                    "cancel" => token.cancel(),
                    "consumer" => return Err(failed("reader transfer consumer sentinel")),
                    "pressure" if calls == 1 => {
                        let snapshot = context.memory().snapshot();
                        pressure = Some(
                            context
                                .memory()
                                .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?,
                        );
                        // A consumer's own denied allocation must unwind the live
                        // transferred input without certifying partial completion.
                        context.memory().reserve(1)?;
                    }
                    "generation" if calls == 1 => {
                        let replacement = fixture.0.join("new-generation.parquet");
                        fs::copy(fixture.path(), &replacement).unwrap();
                        fs::rename(replacement, fixture.path()).unwrap();
                    }
                    _ => {}
                }
                Ok(())
            })
            .err()
            .expect("failure cannot produce a completion certificate");
        assert!(calls > 0);
        if failure == "consumer" {
            assert!(error.to_string().contains("consumer sentinel"));
        }
        if failure == "cancel" {
            assert!(error.to_string().contains("cancelled"));
        }
        if failure == "pressure" {
            assert!(error.to_string().contains("memory reservation denied"));
        }
        drop(pressure);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        if failure == "generation" {
            assert!(
                prepared
                    .collect_jsonl(&CancellationToken::default())
                    .is_err()
            );
        }
        let fresh = fixture.prepare(&plan);
        let result = fresh.collect_jsonl(&CancellationToken::default()).unwrap();
        assert_eq!(
            json_rows(&result),
            (0..128)
                .map(|n| json!({"n":n * 17 - 91}))
                .collect::<Vec<_>>()
        );
        assert!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .intake_columnar_buffer_bytes_transferred
                > 0
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn public_arrow_query_producer_cannot_claim_private_reader_transfer() {
    let uri = DatasetUri::new("memory://parquet/private-reader-label").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_arrow_batch_source(
            uri.clone(),
            Arc::clone(&schema),
            &CancellationToken::default(),
        )?;
        Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: ProjectionRequest::All,
            predicate: None,
        }))
    })
    .unwrap();
    let mut calls = 0;
    let mut input = || {
        calls += 1;
        Ok((calls == 1).then(|| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(0..128))],
            )
            .unwrap()
        }))
    };
    let result = prepared
        .with_arrow_batch_input(&mut input)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..128).map(|n| json!({"n":n})).collect::<Vec<_>>()
    );
    assert_eq!(calls, 2);
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(input.intake_columnar_buffer_bytes_copied, 128 * 8);
    assert_eq!(input.intake_columnar_buffer_bytes_transferred, 0);
    assert_eq!(input.intake_columnar_buffer_regions_transferred, 0);
    assert_eq!(input.intake_columnar_allocation_bytes_transferred, 0);
    assert!(input.end_of_input_observed && input.output_ownership_detached);
}
