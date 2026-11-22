//! Adversarial public realization, invalidation, null and retention boundaries.

use super::*;
use crate::resident_session::columnar_coverage::Region;

fn extended_float_fixture() -> (Fixture, [Option<u64>; 7]) {
    let expected = [
        Some(0.0_f64.to_bits()),
        Some((-0.0_f64).to_bits()),
        Some(0x7ff8_0000_0000_0042),
        Some(f64::INFINITY.to_bits()),
        Some(f64::NEG_INFINITY.to_bits()),
        Some(1.25_f64.to_bits()),
        None,
    ];
    let fixture = Fixture::new(expected.len(), expected.len());
    fixture.write_batch(
        &RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from_iter(
                expected.iter().map(|bits| bits.map(f64::from_bits)),
            ))],
        )
        .unwrap(),
        expected.len(),
    );
    (fixture, expected)
}

#[test]
fn columnar_coverage_preserves_extended_float_bits_and_keeps_policy_owners_separate() {
    let (fixture, expected) = extended_float_fixture();
    let session = ResidentVortexSession::new_with_float_policy(
        64 << 20,
        1,
        shardloom_core::FloatPolicy::Extended,
    )
    .unwrap();
    let pool = session.shared_memory_pool();
    for turn in 0..2 {
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let source = LocalColumnarSource::admit(
                    &fixture.path(),
                    "parquet",
                    context.memory(),
                    context.cancellation(),
                )?;
                let prepared =
                    source.prepare(&demand(&["value"], None), &session, context.cancellation())?;
                let mut reader = prepared.open(context)?;
                let mut observed = 0;
                while let Some((native, first)) = reader.next_native_located(&session, context)? {
                    assert_eq!(first, observed as u64);
                    let witness = native.batch_release_witness()?;
                    let field = native.batch_array(context)?.slots()[1]
                        .as_ref()
                        .unwrap()
                        .clone();
                    let mut execution = context.native_session().create_execution_ctx();
                    for offset in 0..field.len() {
                        let actual = field
                            .execute_scalar(offset, &mut execution)
                            .unwrap()
                            .as_primitive()
                            .as_::<f64>()
                            .map(f64::to_bits);
                        assert_eq!(actual, expected[observed + offset]);
                    }
                    observed += field.len();
                    drop((native, field));
                    assert_eq!(witness.strong_count(), 0);
                }
                assert_eq!(observed, expected.len());
                let report = reader.report()?;
                assert_eq!(report.rows_decoded, if turn == 0 { 7 } else { 0 });
                assert_eq!(
                    report.native_coverage_field_rows,
                    if turn == 0 { 0 } else { 7 }
                );
                Ok(())
            })
            .unwrap();
    }
    let strict = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let strict_pool = strict.shared_memory_pool();
    let source = LocalColumnarSource::admit(
        &fixture.path(),
        "parquet",
        &strict_pool,
        &CancellationToken::default(),
    )
    .unwrap();
    assert!(
        source
            .realize_native_columns(
                &strict,
                &[ColumnRef::new("value").unwrap()],
                &CancellationToken::default()
            )
            .is_err()
    );
    drop((source, strict, session));
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    assert_eq!(strict_pool.snapshot().reserved_bytes, 0);
}

#[test]
fn columnar_coverage_explicit_realization_shares_ordinary_demand_and_refuses_foreign_owner() {
    let fixture = Fixture::new(37, 17);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let cancellation = CancellationToken::default();
    let pool = session.shared_memory_pool();
    let source =
        LocalColumnarSource::admit(&fixture.path(), "parquet", &pool, &cancellation).unwrap();
    let columns = [
        ColumnRef::new("a").unwrap(),
        ColumnRef::new("text").unwrap(),
    ];
    let realized = source
        .realize_native_columns(&session, &columns, &cancellation)
        .unwrap();
    assert_eq!(
        (realized.rows_decoded, realized.native_coverage_field_rows),
        (37, 0)
    );
    let ordinary = read(&session, &fixture, &["a", "text"], None);
    assert_eq!(
        (ordinary.rows_decoded, ordinary.native_coverage_field_rows),
        (0, 74)
    );
    let reused = source
        .realize_native_columns(&session, &columns, &cancellation)
        .unwrap();
    assert_eq!((reused.rows_decoded, reused.data_read_bytes), (0, 0));
    assert!(
        source
            .realize_native_columns(&session, &[], &cancellation)
            .is_err()
    );
    assert!(
        source
            .realize_native_columns(
                &session,
                &[ColumnRef::new("missing").unwrap()],
                &cancellation
            )
            .is_err()
    );
    let foreign = ResidentVortexSession::new(64 << 20, 1).unwrap();
    assert!(
        source
            .realize_native_columns(&foreign, &columns, &cancellation)
            .is_err()
    );
    assert_eq!(foreign.shared_memory_pool().snapshot().reserved_bytes, 0);
    drop((source, session));
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    assert_eq!(fs::read_dir(&fixture.directory).unwrap().count(), 1);
}

#[test]
fn columnar_coverage_same_operation_and_revocation_after_eof_cannot_publish_a_hit() {
    let fixture = Fixture::new(37, 17);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for _ in 0..2 {
                let report = read_in(&session, &fixture, &["a"], None, 0, context, |_| Ok(()))?;
                assert_eq!(
                    (report.rows_decoded, report.native_coverage_field_rows),
                    (37, 0)
                );
            }
            // Every decoder reached EOF, but publication still belongs to the
            // enclosing operation. Pressure may revoke all validated pending data.
            context.memory().reclaim_optional_memory();
            Ok(())
        })
        .unwrap();
    assert_eq!(
        read(&session, &fixture, &["a"], None).native_coverage_field_rows,
        0
    );
    assert_eq!(
        read(&session, &fixture, &["a"], None).native_coverage_field_rows,
        37
    );
}

#[test]
fn columnar_coverage_cached_reads_detect_replacement_and_deletion_before_completion() {
    for delete in [false, true] {
        let fixture = Fixture::new(17_003, 9_001);
        let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
        read(&session, &fixture, &["a"], None);
        let result =
            session.with_native_execution_context(&CancellationToken::default(), |context| {
                read_in(&session, &fixture, &["a"], None, 0, context, |batch| {
                    if batch == 1 {
                        fs::rename(fixture.path(), fixture.directory.join("previous.parquet"))
                            .unwrap();
                        if !delete {
                            fixture.write(9_001, 1_000);
                        }
                    }
                    Ok(())
                })
            });
        assert!(result.is_err(), "changed cached source cannot complete");
        assert_eq!(session.shared_memory_pool().snapshot().reserved_bytes, 0);
        if delete {
            fixture.write(9_001, 1_000);
        }
        let fresh = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                read_in(&session, &fixture, &["a"], None, 1_000, context, |_| Ok(()))
            })
            .unwrap();
        assert_eq!(
            (fresh.rows_decoded, fresh.native_coverage_field_rows),
            (17_003, 0)
        );
    }
}

#[test]
fn columnar_coverage_all_null_fields_and_empty_sources_keep_typed_owners() {
    for rows in [0, 19] {
        let fixture = Fixture::new(rows, 19);
        fixture.write_batch(
            &RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("text", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![None; rows])),
                    Arc::new(StringArray::from(vec![None::<&str>; rows])),
                ],
            )
            .unwrap(),
            19,
        );
        let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
        let pool = session.shared_memory_pool();
        for turn in 0..2 {
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let source = LocalColumnarSource::admit(
                        &fixture.path(),
                        "parquet",
                        context.memory(),
                        context.cancellation(),
                    )?;
                    let prepared = source.prepare(
                        &demand(&["a", "text"], None),
                        &session,
                        context.cancellation(),
                    )?;
                    let mut reader = prepared.open(context)?;
                    let mut observed = 0;
                    while let Some((native, first)) =
                        reader.next_native_located(&session, context)?
                    {
                        assert_eq!(first, observed as u64);
                        let witness = native.batch_release_witness()?;
                        let field = native.batch_array(context)?.slots()[1]
                            .as_ref()
                            .unwrap()
                            .clone();
                        let text = native.batch_array(context)?.slots()[2]
                            .as_ref()
                            .unwrap()
                            .clone();
                        let mut execution = context.native_session().create_execution_ctx();
                        for offset in 0..field.len() {
                            assert!(
                                field
                                    .execute_scalar(offset, &mut execution)
                                    .unwrap()
                                    .is_null()
                            );
                            assert!(
                                text.execute_scalar(offset, &mut execution)
                                    .unwrap()
                                    .is_null()
                            );
                        }
                        observed += native.row_count();
                        drop((native, text));
                        assert!(
                            witness.upgrade().is_some(),
                            "constant field owns delivery metadata"
                        );
                        drop(field);
                        assert_eq!(witness.strong_count(), 0);
                    }
                    assert_eq!(observed, rows);
                    let report = reader.report()?;
                    assert_eq!(report.rows_decoded, if turn == 0 { rows as u64 } else { 0 });
                    assert_eq!(
                        report.native_coverage_field_rows,
                        if turn == 0 { 0 } else { rows as u64 * 2 }
                    );
                    Ok(())
                })
                .unwrap();
        }
        drop(session);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn columnar_coverage_unadmitted_boolean_field_keeps_direct_values_without_retention() {
    use arrow_array::BooleanArray;
    let fixture = Fixture::new(19, 7);
    let expected: Vec<_> = (0_usize..19)
        .map(|row| (!row.is_multiple_of(3)).then_some(row.is_multiple_of(2)))
        .collect();
    fixture.write_batch(
        &RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "flag",
                DataType::Boolean,
                true,
            )])),
            vec![Arc::new(BooleanArray::from(expected.clone()))],
        )
        .unwrap(),
        7,
    );
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    for _ in 0..2 {
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let source = LocalColumnarSource::admit(
                    &fixture.path(),
                    "parquet",
                    context.memory(),
                    context.cancellation(),
                )?;
                let prepared =
                    source.prepare(&demand(&["flag"], None), &session, context.cancellation())?;
                let mut reader = prepared.open(context)?;
                let mut values = Vec::new();
                while let Some(native) = reader.next_native(&session, context)? {
                    let field = native.batch_array(context)?.slots()[1].as_ref().unwrap();
                    let mut execution = context.native_session().create_execution_ctx();
                    for row in 0..field.len() {
                        values.push(
                            field
                                .execute_scalar(row, &mut execution)
                                .unwrap()
                                .as_bool()
                                .value(),
                        );
                    }
                }
                assert_eq!(values, expected);
                let report = reader.report()?;
                assert_eq!(
                    (
                        report.rows_decoded,
                        report.native_coverage_field_rows,
                        report.native_coverage_retained_bytes
                    ),
                    (19, 0, 0)
                );
                Ok(())
            })
            .unwrap();
    }
}

fn append_numeric(
    session: &ResidentVortexSession,
    context: &NativeExecutionContext<'_>,
    ticket: crate::resident_session::columnar_coverage::Ticket,
    name: &str,
    start: usize,
    stop: usize,
) -> Result<()> {
    let values: Vec<_> = (start..stop)
        .map(|row| {
            let value = i64::try_from(row).unwrap();
            if name == "b" {
                (!row.is_multiple_of(7)).then_some(-value)
            } else {
                Some(value)
            }
        })
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Int64,
            name == "b",
        )])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .map_err(failed)?;
    let (source, detached) =
        ResidentMemorySource::from_local_arrow_with_coverage(session, batch, context)?;
    context.columnar_coverage().unwrap().0.append(
        ticket,
        start as u64,
        detached.expect("proved primitive backing"),
    );
    drop(source);
    Ok(())
}

#[test]
fn columnar_coverage_different_fragment_boundaries_compose_exact_original_rows() {
    let fixture = Fixture::new(37, 37);
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                context.memory(),
                context.cancellation(),
            )?;
            let (store, operation) = context.columnar_coverage().unwrap();
            let region = Region {
                index: 0,
                first_row: 0,
                rows: 37,
            };
            for (index, name, ranges) in [
                (0, "a", vec![0..9, 9..37]),
                (1, "b", vec![0..5, 5..21, 21..37]),
            ] {
                let ticket = store
                    .begin(&source.files[0].identity, region, &[index], operation)
                    .unwrap();
                for range in ranges {
                    append_numeric(&session, context, ticket, name, range.start, range.end)?;
                }
                store.validate(ticket);
            }
            source.validate_generation_controlled(context.cancellation())?;
            Ok(())
        })
        .unwrap();
    let report = read(&session, &fixture, &["b", "a"], None);
    assert_eq!(
        (
            report.rows_decoded,
            report.data_read_bytes,
            report.native_coverage_field_rows
        ),
        (0, 0, 74)
    );
}

#[test]
#[allow(clippy::too_many_lines)] // One capacity lifecycle checks failed publication, bounded replacement and later lookup.
fn columnar_coverage_fragment_limit_and_record_limit_decline_before_unbounded_retention() {
    let fixture = Fixture::new(129, 129);
    let session = ResidentVortexSession::new(512 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                context.memory(),
                context.cancellation(),
            )?;
            let identity = &source.files[0].identity;
            let (store, operation) = context.columnar_coverage().unwrap();
            let region = Region {
                index: 0,
                first_row: 0,
                rows: 129,
            };
            let ticket = store.begin(identity, region, &[0], operation).unwrap();
            for row in 0..129 {
                append_numeric(&session, context, ticket, "a", row, row + 1)?;
            }
            store.validate(ticket);
            // A 129th native fragment revokes the entire region; it cannot publish
            // a truncated hit even though its logical rows are individually valid.
            Ok(())
        })
        .unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                context.memory(),
                context.cancellation(),
            )?;
            let identity = &source.files[0].identity;
            let (store, operation) = context.columnar_coverage().unwrap();
            let region = Region {
                index: 0,
                first_row: 0,
                rows: 129,
            };
            let acquired = store.acquire(identity, region, &[0], operation).unwrap();
            assert!(acquired.hits.iter().all(Option::is_none));
            drop(acquired);
            for index in 0..129 {
                let region = Region {
                    index,
                    first_row: 0,
                    rows: 1,
                };
                let ticket = store.begin(identity, region, &[0], operation).unwrap();
                append_numeric(&session, context, ticket, "a", 0, 1)?;
                store.validate(ticket);
            }
            Ok(())
        })
        .unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                context.memory(),
                context.cancellation(),
            )?;
            let identity = &source.files[0].identity;
            let (store, operation) = context.columnar_coverage().unwrap();
            assert!(
                store
                    .acquire(
                        identity,
                        Region {
                            index: 0,
                            first_row: 0,
                            rows: 1
                        },
                        &[0],
                        operation
                    )
                    .unwrap()
                    .hits[0]
                    .is_none()
            );
            assert!(
                store
                    .acquire(
                        identity,
                        Region {
                            index: 128,
                            first_row: 0,
                            rows: 1
                        },
                        &[0],
                        operation
                    )
                    .unwrap()
                    .hits[0]
                    .is_some()
            );
            assert!(store.retained_bytes().unwrap() <= 32 << 20);
            Ok(())
        })
        .unwrap();
}

#[test]
fn columnar_coverage_narrower_operation_grant_reclaims_the_same_session_owner() {
    use crate::resident_session::ConfigurableResidentSession;
    use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
    let resources = |bytes| {
        ExecutionResources::from_bytes(bytes, 1, ExecutionResourceOrigin::ExecutionCall).unwrap()
    };
    let wide = resources(64 << 20);
    let narrow = resources(8 << 20);
    let owner = ConfigurableResidentSession::new(wide).unwrap();
    let fixture = Fixture::new(37, 17);
    let cancellation = CancellationToken::default();
    owner
        .with_resources(wide, &cancellation, |session, _| {
            assert_eq!(
                read(session, &fixture, &["a", "text"], None).native_coverage_field_rows,
                0
            );
            assert_eq!(
                read(session, &fixture, &["a", "text"], None).native_coverage_field_rows,
                74
            );
            Ok(())
        })
        .unwrap();
    assert!(owner.snapshot().memory.reserved_bytes > 0);
    owner
        .with_resources(narrow, &cancellation, |session, admitted| {
            assert_eq!(admitted.memory_bytes(), 8 << 20);
            assert_eq!(session.shared_memory_pool().snapshot().reserved_bytes, 0);
            let report = read(session, &fixture, &["a", "text"], None);
            assert_eq!(
                (report.rows_decoded, report.native_coverage_field_rows),
                (37, 0)
            );
            assert!(report.native_coverage_retained_bytes <= (8 << 20) / 16);
            Ok(())
        })
        .unwrap();
}
