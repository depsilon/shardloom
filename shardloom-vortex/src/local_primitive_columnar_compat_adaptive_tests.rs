use super::*;
use crate::local_primitives::native_payload;
use crate::resident_session::ResidentVortexSession;
use std::io::BufReader;

const ROWS: usize = 128;

fn formats() -> [VortexLocalPrimitiveRowExportFormat; 4] {
    [
        VortexLocalPrimitiveRowExportFormat::Parquet,
        VortexLocalPrimitiveRowExportFormat::ArrowIpc,
        VortexLocalPrimitiveRowExportFormat::Avro,
        VortexLocalPrimitiveRowExportFormat::Orc,
    ]
}

fn label(row: usize) -> Option<String> {
    (!row.is_multiple_of(7)).then(|| {
        if row.is_multiple_of(11) {
            String::new()
        } else {
            format!("{row:04}-{}", "λ".repeat(64))
        }
    })
}

fn array() -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["id", "label"]),
        vec![
            PrimitiveArray::from_iter((0..ROWS).map(|row| i64::try_from(row).unwrap()))
                .into_array(),
            VarBinViewArray::from_iter_nullable_str((0..ROWS).map(label)).into_array(),
        ],
        ROWS,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn prepared(format: VortexLocalPrimitiveRowExportFormat) -> PreparedCompatibilityExport {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let plan = native_sink::NativeSinkPlan::produced(
        session,
        array().dtype().clone(),
        ROWS as u64,
        None,
        None,
    )
    .unwrap();
    let request = VortexQueryPrimitiveRequest::project(
        DatasetUri::new("memory://adaptive-writer").unwrap(),
        ProjectionRequest::All,
    );
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 32 << 20;
    prepare_plan(
        &request,
        plan,
        format,
        configured,
        CompatibilityLimits::streaming(ROWS as u64, &CancellationToken::default()),
    )
    .unwrap()
    .unwrap()
}

fn verify(path: &Path, format: VortexLocalPrimitiveRowExportFormat) {
    let reader: Box<dyn arrow_array::RecordBatchReader> = match format {
        VortexLocalPrimitiveRowExportFormat::Parquet => Box::new(
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap(),
        ),
        VortexLocalPrimitiveRowExportFormat::ArrowIpc => Box::new(
            arrow_ipc::reader::FileReader::try_new(File::open(path).unwrap(), None).unwrap(),
        ),
        VortexLocalPrimitiveRowExportFormat::Avro => Box::new(
            arrow_avro::reader::ReaderBuilder::new()
                .build(BufReader::new(File::open(path).unwrap()))
                .unwrap(),
        ),
        VortexLocalPrimitiveRowExportFormat::Orc => Box::new(
            orc_rust::ArrowReaderBuilder::try_new(File::open(path).unwrap())
                .unwrap()
                .build(),
        ),
        _ => unreachable!(),
    };
    let mut ordinal = 0;
    for batch in reader {
        let batch = batch.unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(ids.value(row), i64::try_from(ordinal).unwrap());
            assert_eq!(
                (!labels.is_null(row)).then(|| labels.value(row)),
                label(ordinal).as_deref(),
            );
            ordinal += 1;
        }
    }
    assert_eq!(ordinal, ROWS);
}

#[test]
fn adaptive_compatibility_canonicalization_shares_the_admitted_root_dtype() {
    let prepared = prepared(VortexLocalPrimitiveRowExportFormat::ArrowIpc);
    let memory = prepared.plan.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    prepared
        .plan
        .session
        .with_native_execution_context(&CancellationToken::default(), |execution| {
            let input = native_payload::detach(&array(), execution)?;
            let credit = memory.reserve(sizing::canonical_metadata(&prepared.plan.dtype)?)?;
            let mut context = execution.native_session().create_execution_ctx();
            let (canonical, _) =
                admitted_canonical(&input, &prepared.plan.dtype, &mut context, &prepared.limits)?;
            assert_eq!(canonical.len(), ROWS);
            assert_eq!(canonical.dtype(), input.dtype());
            // Equal values alone would miss reconstruction of the wide name/type
            // tree. Vortex's checked constructor must retain the prepared owner.
            assert!(std::ptr::eq(
                canonical.dtype().as_struct_fields().names(),
                prepared.plan.dtype.as_struct_fields().names(),
            ));
            drop((canonical, credit, input));
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)] // Exercise real writers, pressure feedback, complete values and final credit release together.
fn adaptive_compatibility_writers_use_prepaid_workspace_and_regrow_after_pressure() {
    for format in formats() {
        let fixture = Fixture::new();
        let prepared = prepared(format);
        let memory = prepared.plan.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let path = fixture.0.join(format!("adaptive.{}", format.as_str()));
        prepared
            .plan
            .session
            .with_native_execution_context(&CancellationToken::default(), |execution| {
                let array = native_payload::detach(&array(), execution)?;
                let mut file = File::create(&path).map_err(vortex_error)?;
                let mut metadata = prepared
                    .limits
                    .metadata_reservation(prepared.schema_fields)?;
                let initial = prepared.writer_overhead()? + metadata;
                let mut owner = memory.reserve(initial)?;
                let mut writer = Writer::new(
                    format,
                    Arc::clone(&prepared.schema),
                    &mut file,
                    &prepared.limits,
                    64 << 10,
                    usize::try_from(prepared.parquet_page_bytes()).map_err(vortex_error)?,
                    prepared.parquet_dictionary_enabled(),
                )?;
                let mut work = CompatibilityWork {
                    writer_reserved_bytes: initial,
                    ..Default::default()
                };
                let mut context = execution.native_session().create_execution_ctx();
                let (measured, bytes) = admitted_canonical(
                    &array,
                    &prepared.plan.dtype,
                    &mut context,
                    &prepared.limits,
                )?;
                drop(measured);
                let window = sizing::ConversionWindow::new(&prepared, initial, ROWS)?;
                let cost =
                    window.cost(&writer, 0, owner.bytes(), 4, bytes.div_ceil(ROWS as u64))?;
                let keep = sizing::canonical_metadata(&prepared.plan.dtype)?
                    + cost.fixed_bytes
                    + 4 * cost.bytes_per_unit;
                assert!(keep < initial);
                let snapshot = memory.snapshot();
                let mut pressure =
                    Some(memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - keep)?);
                let (mut rows, mut delivered) = (0, 0);
                let mut sizes = Vec::new();
                prepared.write_native_batch(
                    &array,
                    execution,
                    &mut context,
                    &mut writer,
                    &mut work,
                    &mut owner,
                    &mut metadata,
                    initial,
                    &mut rows,
                    &mut |complete| {
                        sizes.push(complete - delivered);
                        delivered = complete;
                        pressure.take();
                        Ok(())
                    },
                )?;
                assert_eq!(rows, ROWS as u64);
                // The starting estimate permits at most four rows. Measured
                // per-part conversion can narrow it further before any write.
                assert!((1..=4).contains(&sizes[0]), "{format:?}: {sizes:?}");
                assert!(sizes.iter().any(|rows| *rows > sizes[0]));
                assert!(sizes.windows(2).all(|pair| pair[1] <= 2 * pair[0]));
                assert_eq!(writer.sizing.report().completed_windows, work.arrow_batches);
                assert!(writer.sizing.report().headroom_limited_windows > 0);
                assert!(writer.sizing.report().grown_windows > 0);
                assert!(owner.bytes() >= initial);
                writer.finish()?;
                let mut proof = String::new();
                execution.append_adaptive_work_certificate(&mut proof);
                assert!(proof.contains("adaptive_compatibility_writer_completed_windows="));
                Ok(())
            })
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        verify(&path, format);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn adaptive_compatibility_minimum_denial_never_writes_a_batch_or_publishes() {
    for format in formats() {
        let fixture = Fixture::new();
        let prepared = prepared(format);
        let memory = prepared.plan.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let path = fixture.0.join("denied");
        let mut callbacks = 0;
        let mut calls = 0;
        let error = prepared
            .write_produced_observed(
                &path,
                false,
                Some(&mut |context, _, consume| {
                    calls += 1;
                    let array = native_payload::detach(&array(), context)?;
                    let snapshot = memory.snapshot();
                    let keep = sizing::canonical_metadata(&prepared.plan.dtype)? + 1;
                    let _pressure =
                        memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - keep)?;
                    consume(array)?;
                    Ok(())
                }),
                &CancellationToken::default(),
                None,
                |_| {
                    callbacks += 1;
                    Ok(())
                },
            )
            .err()
            .expect("minimum conversion must be denied");
        assert!(
            error.to_string().contains("memory reservation denied"),
            "{error}"
        );
        assert_eq!(calls, 1);
        assert_eq!(callbacks, 0);
        assert!(!path.exists());
        assert!(!temporary_output_path(&path).unwrap().exists());
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn adaptive_compatibility_writer_cancel_and_consumer_error_do_not_retry_or_publish() {
    for format in formats() {
        for cancel in [false, true] {
            let fixture = Fixture::new();
            let prepared = prepared(format);
            let memory = prepared.plan.session.memory().clone();
            let baseline = memory.snapshot().reserved_bytes;
            let path = fixture.0.join("failed");
            let token = CancellationToken::default();
            let (mut callbacks, mut calls) = (0, 0);
            let result = prepared.write_produced_observed(
                &path,
                false,
                Some(&mut |context, _, consume| {
                    calls += 1;
                    consume(native_payload::detach(&array(), context)?)?;
                    Ok(())
                }),
                &token,
                None,
                |_| {
                    callbacks += 1;
                    if cancel {
                        token.cancel();
                        Ok(())
                    } else {
                        Err(error("test consumer stopped"))
                    }
                },
            );
            assert!(result.is_err());
            assert_eq!(calls, 1);
            assert_eq!(callbacks, 1);
            assert!(!path.exists());
            assert!(!temporary_output_path(&path).unwrap().exists());
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}
