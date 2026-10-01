use super::super::super as runtime;
use super::*;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::array::VortexSessionExecute as _;

fn large_source(fixture: &Fixture, rows: usize) -> PathBuf {
    let path = fixture.0.join("large.vortex");
    let runtime =
        runtime::local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let array = StructArray::new(
        [KEY, VALUE].into(),
        vec![
            PrimitiveArray::new(
                (0..rows)
                    .map(|row| i64::try_from(row).unwrap())
                    .collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            PrimitiveArray::new(vec![1_u64; rows], Validity::NonNullable).into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .into_array();
    let mut file = fs::File::create(&path).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            runtime::native_flat_layout::SequentialNativeFlatLayout::strategy(rows.div_ceil(4096)),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, array.dtype().clone());
    for start in (0..rows).step_by(4096) {
        writer
            .push(array.slice(start..rows.min(start + 4096)).unwrap())
            .unwrap();
    }
    assert_eq!(writer.finish().unwrap().row_count(), rows as u64);
    path
}

fn count_query(path: &Path, rows: usize) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(KEY).unwrap()],
            vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new(KEY, false)]),
    )
    .with_source_order_limit(rows)
}

fn check_count_batch(array: &ArrayRef, context: &mut vortex::array::ExecutionCtx, next: &mut i64) {
    let keys = runtime::logical_field_from_native_array(array, KEY)
        .unwrap()
        .execute::<PrimitiveArray>(context)
        .unwrap();
    let counts = runtime::logical_field_from_native_array(array, "n")
        .unwrap()
        .execute::<PrimitiveArray>(context)
        .unwrap();
    for (key, count) in keys.as_slice::<i64>().iter().zip(counts.as_slice::<u64>()) {
        assert_eq!(*key, *next);
        assert_eq!(*count, 1);
        *next += 1;
    }
}

#[test]
fn result_stream_complete_aggregate_exceeds_collect_rows_and_reopens_every_native_value() {
    const ROWS: usize = 70_017;
    let fixture = Fixture::new();
    let path = large_source(&fixture, ROWS);
    let prepared = prepare_aggregate(
        &count_query(&path, ROWS),
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert!(prepared.execute_owned().is_err());
    let mut next = 0;
    let mut batches = 0;
    let executed = prepared
        .for_each_batch(&CancellationToken::default(), |array, context| {
            assert!(array.len() <= 2048);
            check_count_batch(
                &array,
                &mut context.native_session().create_execution_ctx(),
                &mut next,
            );
            batches += 1;
            Ok(())
        })
        .unwrap();
    assert!(executed.native_io_certificate.is_certified());
    assert_eq!(next, i64::try_from(ROWS).expect("test row count fits i64"));
    assert!(batches > 1);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    let output = fixture.0.join("complete.vortex");
    let report = prepared
        .write(
            &output,
            crate::VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
        )
        .unwrap();
    assert_eq!(report.rows_written, ROWS as u64);
    assert!(
        !report
            .evidence
            .native_array_sink
            .as_ref()
            .unwrap()
            .pre_limit_result_row_count_exact,
        "the writer only observes the rows delivered after the aggregate limit"
    );
    assert_eq!(prepared.snapshot().completed_executions, 2);
    let session =
        crate::resident_session::ResidentVortexSession::new(128 * 1024 * 1024, 1).unwrap();
    let source = session.prepare_file(&output).unwrap();
    let mut next = 0;
    source
        .with_native_execution(|file, session, runtime| {
            let mut ctx = session.create_execution_ctx();
            for array in file.scan().unwrap().into_array_iter(runtime).unwrap() {
                check_count_batch(&array.unwrap(), &mut ctx, &mut next);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(next, i64::try_from(ROWS).expect("test row count fits i64"));
}

#[test]
fn result_stream_batches_more_than_eight_mib_under_a_small_buffer_budget() {
    use runtime::{completed_result::CompletedRows, result_batch::Value};
    let memory = LiveMemoryPool::new(2 * 1024 * 1024).unwrap();
    let text = "native-\"text\",\n".repeat(16);
    let fields = vec![
        (
            "record".into(),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ),
        ("payload".into(), DType::Utf8(Nullability::Nullable)),
    ];
    let columns = vec!["record".into(), "payload".into()];
    let mut scalar_context = VortexSession::default().create_execution_ctx();
    let mut seen = 0;
    let mut bytes = 0;
    let mut consume = |array: ArrayRef| {
        assert!(array.len() <= 512);
        bytes += array.nbytes();
        let numbers = runtime::logical_field_from_native_array(&array, "record")?;
        let payload = runtime::logical_field_from_native_array(&array, "payload")?;
        for row in 0..array.len() {
            assert_eq!(
                runtime::vortex_scalar_to_stat_value(
                    &numbers.execute_scalar(row, &mut scalar_context).unwrap()
                ),
                Some(shardloom_core::StatValue::UInt64(seen))
            );
            let value = payload.execute_scalar(row, &mut scalar_context).unwrap();
            if seen % 7 == 0 {
                assert!(value.is_null());
            } else {
                assert_eq!(
                    runtime::vortex_scalar_to_stat_value(&value),
                    Some(shardloom_core::StatValue::Utf8(text.clone()))
                );
            }
            seen += 1;
        }
        Ok(())
    };
    let mut output = CompletedRows::streaming(
        fields,
        &memory,
        512,
        CancellationToken::default(),
        &mut consume,
    )
    .unwrap();
    output
        .finish_values(&columns, 70_017, |row, column| {
            Ok(if column == 0 {
                Value::UInt(row as u64)
            } else if row % 7 == 0 {
                Value::Null
            } else {
                Value::Text(text.as_str().into())
            })
        })
        .unwrap();
    drop(output);
    assert_eq!(seen, 70_017);
    assert!(bytes > 8 * 1024 * 1024);
    assert!(memory.snapshot().peak_reserved_bytes < 1024 * 1024);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the source-to-consumer-to-file proof in one operation.
fn result_stream_chains_native_filter_projection_and_sink_without_nested_admission() {
    use crate::resident_session::NativeExecutionContext;
    use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};
    use vortex::array::arrays::{BoolArray, bool::BoolArrayExt as _};
    use vortex::expr::{get_item, gt_eq, lit, root, select};
    const ROWS: usize = 70_017;
    let fixture = Fixture::new();
    let path = large_source(&fixture, ROWS);
    let mut query = count_query(&path, ROWS);
    query.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new(KEY).unwrap(),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(13),
    });
    let prepared = prepare_aggregate(
        &query,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    let before = prepared.snapshot();
    let plan = runtime::native_sink::NativeSinkPlan::produced(
        prepared.session.clone(),
        DType::struct_(
            [(KEY, DType::Primitive(PType::I64, Nullability::NonNullable))],
            Nullability::NonNullable,
        ),
        ROWS as u64,
        Some(path.clone()),
        None,
    )
    .unwrap();
    let mut batches = 0;
    let mut producer = |context: &NativeExecutionContext<'_>,
                        batch_rows,
                        consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
        let mut execution = context.native_session().create_execution_ctx();
        let completed = prepared.consume_native_in_context(context, batch_rows, &mut |array| {
            batches += 1;
            let predicate = gt_eq(get_item(KEY, root()), lit(69_000_i64))
                .bind(array.dtype())
                .map_err(runtime::vortex_error)?;
            let predicate = array
                .clone()
                .apply_bound(&predicate)
                .and_then(|array| array.execute::<BoolArray>(&mut execution))
                .map_err(runtime::vortex_error)?;
            let filtered = array
                .filter(predicate.to_mask_fill_null_false(&mut execution))
                .map_err(runtime::vortex_error)?;
            let projection = select(vec![KEY], root())
                .bind(filtered.dtype())
                .map_err(runtime::vortex_error)?;
            let selected = filtered
                .apply_bound(&projection)
                .and_then(|array| array.execute::<StructArray>(&mut execution))
                .map_err(runtime::vortex_error)?
                .into_array();
            assert!(consume(selected)?);
            Ok(())
        })?;
        assert!(completed.native_io_certificate.is_certified());
        Ok(())
    };
    let target = fixture.0.join("chained.vortex");
    let report = runtime::completed_result::write_stream(
        plan,
        &query,
        &target,
        crate::VortexLocalPrimitiveRowExportFormat::Vortex,
        false,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &mut producer,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(report.rows_written, 1017);
    assert!(batches > 1);
    assert_eq!(
        prepared.snapshot().completed_executions,
        before.completed_executions + 1
    );
    assert_eq!(
        prepared.snapshot().prepared_source_opens,
        before.prepared_source_opens
    );
    assert!(!report.evidence.side_effects.fallback_attempted);
    let reader = crate::resident_session::ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = reader.prepare_file(target).unwrap();
    let mut next = 69_000_i64;
    source
        .with_native_execution(|file, session, handle| {
            let mut context = session.create_execution_ctx();
            for array in file
                .scan()
                .unwrap()
                .with_ordered(true)
                .into_array_iter(handle)
                .unwrap()
            {
                let array = array.unwrap();
                assert_eq!(
                    array.dtype().as_struct_fields_opt().unwrap().names().len(),
                    1
                );
                let keys = runtime::logical_field_from_native_array(&array, KEY)?
                    .execute::<PrimitiveArray>(&mut context)
                    .map_err(runtime::vortex_error)?;
                for key in keys.as_slice::<i64>() {
                    assert_eq!(*key, next);
                    next += 1;
                }
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(next, i64::try_from(ROWS).unwrap());
}

#[test]
fn result_stream_consumer_error_cancellation_and_retention_keep_resource_ownership() {
    use runtime::{completed_result::CompletedRows, result_batch::Value};
    let fields = vec![(
        "record".into(),
        DType::Primitive(PType::U64, Nullability::NonNullable),
    )];
    let columns = vec!["record".into()];
    for cancel in [false, true] {
        let memory = LiveMemoryPool::new(1024 * 1024).unwrap();
        let token = CancellationToken::default();
        let mut retained = None;
        let mut calls = 0;
        let mut consume = |array: ArrayRef| {
            calls += 1;
            retained = Some(array.slice(0..1).unwrap());
            if cancel {
                token.cancel();
                Ok(())
            } else {
                Err(ShardLoomError::InvalidOperation(
                    "test consumer failed".into(),
                ))
            }
        };
        let mut output =
            CompletedRows::streaming(fields.clone(), &memory, 32, token.clone(), &mut consume)
                .unwrap();
        assert!(
            output
                .finish_values(&columns, 100, |row, _| Ok(Value::UInt(row as u64)))
                .is_err()
        );
        drop(output);
        assert_eq!(calls, 1);
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn result_stream_public_ordered_output_exceeds_collect_and_spill_reopens_complete_values() {
    use crate::{
        VortexLocalPrimitiveRowExportFormat as Format, VortexSortRowsRequest, VortexSortSpillPolicy,
    };
    const ROWS: usize = 70_017;
    let fixture = Fixture::new();
    let path = large_source(&fixture, ROWS);
    for spill in [false, true] {
        let workspace = fixture.0.join("sort-workspace");
        fs::create_dir_all(&workspace).unwrap();
        let mut sort = VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new(KEY, true)])
            .with_offset(19);
        let limit = if spill { 1034 } else { ROWS - 19 };
        if spill {
            sort =
                sort.with_spill(VortexSortSpillPolicy::new(&workspace, 32 << 20, 4 << 20).unwrap());
        }
        let query = VortexQueryPrimitiveRequest::sort_rows(
            DatasetUri::new(path.display().to_string()).unwrap(),
            shardloom_plan::ProjectionRequest::All,
            None,
            sort,
            limit,
        );
        let target = fixture.0.join(format!("ordered-{spill}.jsonl"));
        let report = runtime::execute_vortex_local_primitive_row_export_with_policy(
            &query,
            &target,
            Format::Jsonl,
            false,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        assert_eq!(report.rows_written, limit as u64);
        if spill {
            let evidence = report.state_budget.native_sort_spill.as_ref().unwrap();
            assert!(evidence.runs_written > 0);
            assert!(evidence.owned_cleanup_completed);
        }
        let text = fs::read_to_string(&target).unwrap();
        assert_eq!(text.lines().count(), limit);
        for (index, row) in text.lines().enumerate() {
            let row: serde_json::Value = serde_json::from_str(row).unwrap();
            assert_eq!(row[KEY], (ROWS - 1 - 19 - index) as u64);
            assert_eq!(row[VALUE], 1);
        }
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}

#[test]
fn result_stream_public_exact_distinct_spill_writes_native_columns_and_cleans_runs() {
    const ROWS: usize = 70_017;
    let fixture = Fixture::new();
    let path = large_source(&fixture, ROWS);
    let workspace = fixture.0.join("distinct-workspace");
    fs::create_dir(&workspace).unwrap();
    let spill = crate::VortexAggregateSpillPolicy::new(&workspace, 64 << 20, 4 << 20).unwrap();
    let query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(KEY).unwrap()],
            vec![VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new(VALUE).unwrap()),
                "n".into(),
            )],
        )
        .with_order_by(vec![
            VortexAggregateOrderExpr::new("n", true),
            VortexAggregateOrderExpr::new(KEY, false),
        ])
        .with_offset(5)
        .with_spill(spill),
    )
    .with_source_order_limit(127);
    let target = fixture.0.join("distinct.vortex");
    let report = runtime::execute_vortex_local_primitive_row_export_with_policy(
        &query,
        &target,
        crate::VortexLocalPrimitiveRowExportFormat::Vortex,
        false,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert_eq!(report.rows_written, 127);
    let evidence = report.state_budget.native_aggregate_spill.as_ref().unwrap();
    assert!(evidence.runs_written > 0);
    assert!(evidence.owned_cleanup_completed);
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    let session = crate::resident_session::ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = session.prepare_file(&target).unwrap();
    let mut next = 5;
    source
        .with_native_execution(|file, session, runtime| {
            let mut context = session.create_execution_ctx();
            for array in file
                .scan()
                .unwrap()
                .with_ordered(true)
                .into_array_iter(runtime)
                .unwrap()
            {
                check_count_batch(&array.unwrap(), &mut context, &mut next);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(next, 132);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn result_stream_all_writers_drop_staging_and_buffers_on_late_error_or_cancellation() {
    use crate::VortexLocalPrimitiveRowExportFormat as Format;
    use crate::resident_session::{NativeExecutionContext, ResidentVortexSession};
    use runtime::{
        completed_result::{CompletedRows, write_stream},
        result_batch::Value,
    };
    let fixture = Fixture::new();
    let fields = vec![(
        "record".into(),
        DType::Primitive(PType::U64, Nullability::NonNullable),
    )];
    let columns = vec!["record".into()];
    for format in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Orc,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ] {
        for cancel in [false, true] {
            let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
            let baseline = session.snapshot().memory.reserved_bytes;
            let plan = runtime::native_sink::NativeSinkPlan::produced(
                session.clone(),
                DType::struct_(fields.clone(), Nullability::NonNullable),
                4096,
                None,
                None,
            )
            .unwrap();
            let token = CancellationToken::default();
            let mut calls = 0;
            let mut producer =
                |context: &NativeExecutionContext<'_>,
                 batch_rows,
                 consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
                    let mut accept = |array| {
                        calls += 1;
                        assert!(consume(array)?);
                        if cancel {
                            token.cancel();
                            Ok(())
                        } else {
                            Err(shardloom_core::ShardLoomError::InvalidOperation(
                                "test late producer error".into(),
                            ))
                        }
                    };
                    CompletedRows::streaming(
                        fields.clone(),
                        context.memory(),
                        batch_rows,
                        context.cancellation().clone(),
                        &mut accept,
                    )?
                    .finish_values(&columns, 4096, |row, _| Ok(Value::UInt(row as u64)))
                };
            let target = fixture
                .0
                .join(format!("failure-{cancel}.{}", format.as_str()));
            let query = count_query(Path::new("unopened-test-source.vortex"), 4096);
            let error = write_stream(
                plan,
                &query,
                &target,
                format,
                false,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
                &mut producer,
                &token,
            )
            .unwrap_err();
            assert!(
                error.to_string().contains(if cancel {
                    "cancel"
                } else {
                    "test late producer error"
                }),
                "{format:?}: {error}"
            );
            assert_eq!(calls, 1, "{format:?}");
            assert!(!target.exists());
            assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0, "{format:?}");
            assert_eq!(
                session.snapshot().memory.reserved_bytes,
                baseline,
                "{format:?}"
            );
        }
    }
}

#[test]
fn result_stream_native_writer_metadata_denial_releases_pending_task_owners() {
    use crate::resident_session::{NativeExecutionContext, ResidentVortexSession};
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(192 * 1024, 1).unwrap();
    let baseline = session.snapshot().memory.reserved_bytes;
    // Reusing a pre-existing test array isolates growth in the writer's footer
    // reservation from producer allocation failure.
    let array = StructArray::new(
        ["record"].into(),
        vec![PrimitiveArray::new(vec![7_u64; 16], Validity::NonNullable).into_array()],
        16,
        Validity::NonNullable,
    )
    .into_array();
    let plan = runtime::native_sink::NativeSinkPlan::produced(
        session.clone(),
        array.dtype().clone(),
        1024,
        None,
        None,
    )
    .unwrap();
    let mut calls = 0;
    let mut producer =
        |_: &NativeExecutionContext<'_>, _, consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
            for _ in 0..64 {
                calls += 1;
                assert!(consume(array.clone())?);
            }
            Ok(())
        };
    let target = fixture.0.join("pressure.vortex");
    let error = runtime::completed_result::write_stream(
        plan,
        &count_query(Path::new("unopened-test-source.vortex"), 1024),
        &target,
        crate::VortexLocalPrimitiveRowExportFormat::Vortex,
        false,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &mut producer,
        &CancellationToken::default(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native leaf metadata reservation failed"),
        "{error}"
    );
    assert!(calls > 1);
    assert!(!target.exists());
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the exact cross-dtype stream oracle in one test.
fn result_stream_batch_type_null_and_pressure_boundaries_preserve_exact_values() {
    use runtime::{completed_result::CompletedRows, result_batch::Value};
    let fields = vec![
        ("flag".into(), DType::Bool(Nullability::Nullable)),
        (
            "i8".into(),
            DType::Primitive(PType::I8, Nullability::NonNullable),
        ),
        (
            "i16".into(),
            DType::Primitive(PType::I16, Nullability::NonNullable),
        ),
        (
            "i32".into(),
            DType::Primitive(PType::I32, Nullability::NonNullable),
        ),
        (
            "i64".into(),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        ),
        (
            "u8".into(),
            DType::Primitive(PType::U8, Nullability::NonNullable),
        ),
        (
            "u16".into(),
            DType::Primitive(PType::U16, Nullability::NonNullable),
        ),
        (
            "u32".into(),
            DType::Primitive(PType::U32, Nullability::NonNullable),
        ),
        (
            "u64".into(),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ),
        (
            "f32".into(),
            DType::Primitive(PType::F32, Nullability::Nullable),
        ),
        (
            "f64".into(),
            DType::Primitive(PType::F64, Nullability::Nullable),
        ),
    ];
    let values = [
        Value::Bool(true),
        Value::Int(i8::MIN.into()),
        Value::Int(i16::MAX.into()),
        Value::Int(i32::MIN.into()),
        Value::Int(i64::MIN),
        Value::UInt(u8::MAX.into()),
        Value::UInt(u16::MAX.into()),
        Value::UInt(u32::MAX.into()),
        Value::UInt(u64::MAX),
        Value::Float(-0.0),
        Value::Float(f64::MAX),
    ];
    let columns = fields
        .iter()
        .map(|(name, _): &(String, DType)| name.clone())
        .collect::<Vec<_>>();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut seen = 0;
    let mut context = VortexSession::default().create_execution_ctx();
    let mut consume = |array: ArrayRef| {
        assert_eq!(
            array.dtype(),
            &DType::struct_(fields.clone(), Nullability::NonNullable)
        );
        for (column, (name, dtype)) in fields.iter().enumerate() {
            let field = runtime::logical_field_from_native_array(&array, name)?;
            for row in 0..array.len() {
                let scalar = field.execute_scalar(row, &mut context).unwrap();
                if seen + row == 1 && dtype.is_nullable() {
                    assert!(scalar.is_null());
                } else {
                    let actual = runtime::vortex_scalar_to_stat_value(&scalar).unwrap();
                    let expected = &values[column];
                    match (actual, expected) {
                        (shardloom_core::StatValue::Boolean(a), Value::Bool(b)) => {
                            assert_eq!(a, *b);
                        }
                        (shardloom_core::StatValue::Int64(a), Value::Int(b)) => assert_eq!(a, *b),
                        (shardloom_core::StatValue::UInt64(a), Value::UInt(b)) => assert_eq!(a, *b),
                        (shardloom_core::StatValue::Float64(a), Value::Float(b)) => {
                            assert_eq!(a.to_bits(), b.to_bits());
                        }
                        _ => panic!("dtype/value mismatch"),
                    }
                }
            }
        }
        seen += array.len();
        Ok(())
    };
    let mut output = CompletedRows::streaming(
        fields.clone(),
        &memory,
        1,
        CancellationToken::default(),
        &mut consume,
    )
    .unwrap();
    output
        .finish_values(&columns, 2, |row, column| {
            Ok(if row == 1 && fields[column].1.is_nullable() {
                Value::Null
            } else {
                values[column].clone()
            })
        })
        .unwrap();
    drop(output);
    assert_eq!(seen, 2);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    for (dtype, value) in [
        (
            DType::Primitive(PType::U8, Nullability::NonNullable),
            Value::UInt(256),
        ),
        (
            DType::Primitive(PType::I8, Nullability::NonNullable),
            Value::Int(-129),
        ),
        (
            DType::Primitive(PType::F32, Nullability::NonNullable),
            Value::Float(0.1),
        ),
        (DType::Bool(Nullability::NonNullable), Value::Null),
    ] {
        let mut calls = 0;
        let mut consume = |_: ArrayRef| {
            calls += 1;
            Ok(())
        };
        let mut output = CompletedRows::streaming(
            vec![("value".into(), dtype)],
            &memory,
            1,
            CancellationToken::default(),
            &mut consume,
        )
        .unwrap();
        assert!(
            output
                .finish_values(&["value".into()], 1, |_, _| Ok(value.clone()))
                .is_err()
        );
        drop(output);
        assert_eq!(calls, 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let oversized = "x".repeat(8 * 1024 * 1024 + 1);
    let mut calls = 0;
    let mut consume = |_: ArrayRef| {
        calls += 1;
        Ok(())
    };
    let mut output = CompletedRows::streaming(
        vec![("value".into(), DType::Utf8(Nullability::NonNullable))],
        &memory,
        1,
        CancellationToken::default(),
        &mut consume,
    )
    .unwrap();
    assert!(
        output
            .finish_values(&["value".into()], 1, |_, _| Ok(Value::Text(
                oversized.as_str().into()
            )))
            .unwrap_err()
            .to_string()
            .contains("one complete output row")
    );
    drop(output);
    assert_eq!(calls, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
