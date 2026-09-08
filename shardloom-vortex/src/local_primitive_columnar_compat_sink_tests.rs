use super::super::{
    ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, ProjectionRequest, StatValue,
    execute_vortex_local_structured_binary_row_export_enabled, local_vortex_runtime,
    temporary_output_path,
};
use super::*;
use crate::{
    VortexStructuredProjectionColumn, VortexStructuredProjectionExpr,
    VortexStructuredProjectionRequest,
};
use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use serde_json::{Value, json};
use std::{fs, path::PathBuf};
use vortex::io::runtime::BlockingRuntime as _;
use vortex::{
    VortexSessionDefault as _,
    array::{
        arrays::{BoolArray, PrimitiveArray},
        iter::ArrayIteratorAdapter,
    },
    file::WriteOptionsSessionExt as _,
    io::session::RuntimeSessionExt as _,
    session::VortexSession,
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-columnar-compat-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn source(&self, rows: usize) -> PathBuf {
        let path = self.0.join("source.vortex");
        let runtime = local_vortex_runtime(policy());
        let session = VortexSession::default().with_handle(runtime.handle());
        let array = StructArray::try_new(
            FieldNames::from([
                "row_ordinal",
                "exact_identifier",
                "text_value",
                "flag_value",
                "weight_value",
            ]),
            vec![
                PrimitiveArray::from_iter((0..rows).map(|row| u64::try_from(row).unwrap()))
                    .into_array(),
                PrimitiveArray::from_iter((0..rows).map(identifier)).into_array(),
                VarBinViewArray::from_iter_nullable_str((0..rows).map(text)).into_array(),
                BoolArray::from_iter(
                    (0..rows).map(|row| (!row.is_multiple_of(5)).then_some(row.is_multiple_of(2))),
                )
                .into_array(),
                PrimitiveArray::from_option_iter(
                    (0..rows).map(|row| (!row.is_multiple_of(11)).then_some(weight(row))),
                )
                .into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        session
            .write_options()
            .blocking(&runtime)
            .write(
                File::create(&path).unwrap(),
                ArrayIteratorAdapter::new(array.dtype().clone(), [Ok(array)].into_iter()),
            )
            .unwrap();
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn policy() -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.memory_budget_bytes = 256 * 1024 * 1024;
    policy
}
fn identifier(row: usize) -> i64 {
    match row {
        0 => i64::MIN,
        1 => i64::MAX,
        _ => (1_i64 << 60) + i64::try_from(row).unwrap(),
    }
}
fn text(row: usize) -> Option<String> {
    (!row.is_multiple_of(7)).then(|| {
        if row.is_multiple_of(13) {
            String::new()
        } else {
            format!("東京-λ-{row}-literal%_\\")
        }
    })
}
fn weight(row: usize) -> f64 {
    f64::from(u32::try_from(row).unwrap()) * 0.125
}
fn expected(row: usize) -> Value {
    json!({"note": text(row), "id": identifier(row),
        "flag": (!row.is_multiple_of(5)).then_some(row.is_multiple_of(2)),
        "weight": (!row.is_multiple_of(11)).then_some(weight(row)),
        "position": row})
}
fn request(path: &Path) -> VortexQueryPrimitiveRequest {
    let columns = [
        ("note", "text_value"),
        ("id", "exact_identifier"),
        ("flag", "flag_value"),
        ("weight", "weight_value"),
        ("position", "row_ordinal"),
    ]
    .into_iter()
    .map(|(alias, source)| {
        VortexStructuredProjectionColumn::new(
            alias.to_string(),
            VortexStructuredProjectionExpr::SourceColumn(ColumnRef::new(source).unwrap()),
        )
    })
    .collect();
    VortexQueryPrimitiveRequest::structured_project_rows(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexStructuredProjectionRequest::new(columns),
    )
}
fn predicate(value: u64) -> PredicateExpr {
    PredicateExpr::Compare {
        column: ColumnRef::new("row_ordinal").unwrap(),
        op: ComparisonOp::GtEq,
        value: StatValue::UInt64(value),
    }
}
fn read(path: &Path, format: VortexLocalPrimitiveRowExportFormat) -> (SchemaRef, Vec<Value>) {
    let (schema, batches) = match format {
        VortexLocalPrimitiveRowExportFormat::ArrowIpc => {
            let reader =
                arrow_ipc::reader::FileReader::try_new(File::open(path).unwrap(), None).unwrap();
            let schema = reader.schema();
            (
                schema,
                reader.collect::<std::result::Result<Vec<_>, _>>().unwrap(),
            )
        }
        VortexLocalPrimitiveRowExportFormat::Parquet => {
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).unwrap(),
            )
            .unwrap();
            let schema = Arc::clone(builder.schema());
            (
                schema,
                builder
                    .build()
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap(),
            )
        }
        _ => panic!("test reader format"),
    };
    let mut values = Vec::new();
    for batch in batches {
        let text = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let flag = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let weight = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let pos = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.push(json!({"note": (!text.is_null(row)).then(|| text.value(row)),
                "id": ids.value(row), "flag": (!flag.is_null(row)).then(|| flag.value(row)),
                "weight": (!weight.is_null(row)).then(|| weight.value(row)), "position": pos.value(row)}));
        }
    }
    (schema, values)
}

#[test]
fn columnar_compatibility_matches_full_legacy_and_independent_values_for_both_sinks() {
    for format in [
        VortexLocalPrimitiveRowExportFormat::ArrowIpc,
        VortexLocalPrimitiveRowExportFormat::Parquet,
    ] {
        let fixture = Fixture::new();
        let source = fixture.source(4097);
        for (name, start, count) in [
            ("full", 0, 4097),
            ("filtered", 3, 2051),
            ("all_null_selected", 0, 1),
        ] {
            let mut request = request(&source);
            if start > 0 {
                request.predicate = Some(predicate(start));
            }
            if count < 4097 {
                request.source_order_limit = Some(count);
            }
            let prepared = prepare(
                &request,
                &source,
                format,
                policy(),
                CompatibilityLimits::default(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(prepared.plan.filter.is_some(), start > 0);
            assert_eq!(
                prepared.plan.columns,
                ["note", "id", "flag", "weight", "position"]
            );
            let pool = prepared.plan.session.memory().clone();
            let candidate = fixture
                .0
                .join(format!("{name}-candidate.{}", format.as_str()));
            let baseline = fixture
                .0
                .join(format!("{name}-baseline.{}", format.as_str()));
            let result = prepared.write(&candidate, false).unwrap();
            // Call the retained scalar exporter directly: a future public
            // dispatch hook must not turn this control into the candidate too.
            execute_vortex_local_structured_binary_row_export_enabled(
                &request,
                &source,
                &baseline,
                format,
                false,
                policy(),
            )
            .unwrap();
            let (schema, actual) = read(&candidate, format);
            let (old_schema, old) = read(&baseline, format);
            let first = usize::try_from(start).unwrap();
            let expected = (first..first + count).map(expected).collect::<Vec<_>>();
            assert_eq!(actual, expected);
            assert_eq!(actual, old);
            assert_eq!(schema, old_schema);
            assert_eq!(result.work.native_batches > 1, count > 2048);
            assert!(result.work.admitted_arrow_expansion_bytes > 0);
            let evidence = result.report.evidence.native_array_sink.as_ref().unwrap();
            assert_eq!(evidence.scalar_values_materialized, 0);
            assert!(result.report.evidence.side_effects.arrow_converted);
            assert!(!result.report.evidence.side_effects.row_read);
            assert!(!result.report.evidence.side_effects.fallback_attempted);
            assert_eq!(
                result.report.rows_written,
                u64::try_from(expected.len()).unwrap()
            );
            assert_eq!(
                result.work.output_bytes,
                fs::metadata(&candidate).unwrap().len()
            );
            assert_eq!(prepared.plan.session.snapshot().prepared_source_opens, 1);
            let again = fixture.0.join(format!("{name}-again.{}", format.as_str()));
            prepared.write(&again, false).unwrap();
            assert_eq!(read(&again, format).1, expected);
            assert_eq!(prepared.plan.session.snapshot().prepared_source_opens, 1);
            assert_eq!(prepared.plan.session.snapshot().completed_executions, 2);
            drop(prepared);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
            assert_eq!(pool.snapshot().denied_reservations, 0);
        }
    }
}

#[test]
fn columnar_compatibility_empty_and_metadata_pruned_outputs_preserve_schema() {
    for format in [
        VortexLocalPrimitiveRowExportFormat::ArrowIpc,
        VortexLocalPrimitiveRowExportFormat::Parquet,
    ] {
        for rows in [0, 37] {
            let fixture = Fixture::new();
            let source = fixture.source(rows);
            let mut request = request(&source);
            if rows > 0 {
                request.predicate = Some(predicate(1000));
            }
            let prepared = prepare(
                &request,
                &source,
                format,
                policy(),
                CompatibilityLimits::default(),
            )
            .unwrap()
            .unwrap();
            let output = fixture.0.join("empty");
            let result = prepared.write(&output, false).unwrap();
            let (schema, values) = read(&output, format);
            assert_eq!(schema, prepared.schema);
            assert!(values.is_empty());
            assert_eq!(result.report.rows_written, 0);
            assert_eq!(result.work.arrow_batches, 0);
            assert!(!result.report.evidence.side_effects.arrow_converted);
            assert!(!result.report.evidence.side_effects.row_read);
            if rows > 0 {
                assert!(prepared.plan.metadata_pruned);
            }
        }
    }
}

#[test]
fn columnar_compatibility_zero_match_scan_is_not_reported_as_zero_read_or_decode() {
    for format in [
        VortexLocalPrimitiveRowExportFormat::ArrowIpc,
        VortexLocalPrimitiveRowExportFormat::Parquet,
    ] {
        let fixture = Fixture::new();
        let source = fixture.source(37);
        let mut request = request(&source);
        // MIN/MAX span 17, but the complete independent input contains no 17.
        // The native all-false task is omitted before the array iterator yields.
        request.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("exact_identifier").unwrap(),
            op: ComparisonOp::Eq,
            value: StatValue::Int64(17),
        });
        assert!((0..37).all(|row| identifier(row) != 17));
        let prepared = prepare(
            &request,
            &source,
            format,
            policy(),
            CompatibilityLimits::default(),
        )
        .unwrap()
        .unwrap();
        assert!(!prepared.plan.metadata_pruned);
        let output = fixture.0.join("unprunable-zero-match");
        let result = prepared.write(&output, false).unwrap();
        let (schema, actual) = read(&output, format);
        assert_eq!(schema, prepared.schema);
        assert!(actual.is_empty());
        assert_eq!(result.report.rows_written, 0);
        assert_eq!(result.report.arrays_read_count, 0);
        assert_eq!(result.work.native_batches, 0);
        assert_eq!(result.work.arrow_batches, 0);
        let evidence = &result.report.evidence;
        assert!(evidence.upstream_scan_called);
        assert!(evidence.side_effects.data_read);
        assert!(evidence.side_effects.data_decoded);
        assert!(evidence.side_effects.data_materialized);
        assert!(!evidence.side_effects.arrow_converted);
        assert!(!evidence.side_effects.row_read);
        assert!(
            evidence
                .native_array_sink
                .as_ref()
                .unwrap()
                .metadata_fidelity
                .contains("conservative_scan_scope_not_observed_bytes")
        );
    }
}

#[test]
fn columnar_compatibility_pressure_and_expansion_fail_without_published_or_leaked_output() {
    for format in [
        VortexLocalPrimitiveRowExportFormat::ArrowIpc,
        VortexLocalPrimitiveRowExportFormat::Parquet,
    ] {
        for fault in [
            "reservation",
            "string",
            "expanded_batch",
            "file",
            "batch_count",
            "output_rows",
        ] {
            let fixture = Fixture::new();
            let source = fixture.source(4097);
            let mut limits = CompatibilityLimits::default();
            let mut policy = policy();
            match fault {
                "reservation" => policy.resource_envelope.memory_budget_bytes = 1024 * 1024,
                "string" => limits.string_bytes = 1,
                "expanded_batch" => limits.arrow_batch_bytes = 1024,
                "file" => limits.file_bytes = 16,
                "batch_count" => limits.batches = 1,
                "output_rows" => limits.output_rows = 1,
                _ => unreachable!(),
            }
            let prepared = prepare(&request(&source), &source, format, policy, limits)
                .unwrap()
                .unwrap();
            let pool = prepared.plan.session.memory().clone();
            let output = fixture.0.join(fault);
            assert!(prepared.write(&output, false).is_err(), "{fault}");
            assert!(!output.exists());
            assert!(!temporary_output_path(&output).unwrap().exists());
            drop(prepared);
            assert_eq!(pool.snapshot().reserved_bytes, 0, "{fault}");
            assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
        }
    }
}

#[test]
fn columnar_compatibility_after_batch_cancel_error_and_mutation_never_publish() {
    for format in [
        VortexLocalPrimitiveRowExportFormat::ArrowIpc,
        VortexLocalPrimitiveRowExportFormat::Parquet,
    ] {
        for fault in ["cancel", "error", "mutation", "target_race"] {
            let fixture = Fixture::new();
            let source = fixture.source(4097);
            let limits = CompatibilityLimits::default();
            let cancellation = Arc::clone(&limits.cancellation);
            let prepared = prepare(&request(&source), &source, format, policy(), limits)
                .unwrap()
                .unwrap();
            let pool = prepared.plan.session.memory().clone();
            let output = fixture.0.join(fault);
            let mut callbacks = 0;
            let result = prepared.write_observed(&output, true, |rows| {
                assert!(rows > 0);
                callbacks += 1;
                if callbacks > 1 {
                    return Ok(());
                }
                match fault {
                    "cancel" => cancellation.store(true, Ordering::Release),
                    "error" => return Err(error("deterministic sink consumer failure")),
                    "mutation" => fs::OpenOptions::new()
                        .append(true)
                        .open(&source)
                        .unwrap()
                        .write_all(b"!")
                        .unwrap(),
                    "target_race" => fs::write(&output, b"independent creator").unwrap(),
                    _ => unreachable!(),
                }
                Ok(())
            });
            assert!(callbacks > 0, "{fault}");
            assert!(result.is_err(), "{fault}");
            if fault == "target_race" {
                assert_eq!(fs::read(&output).unwrap(), b"independent creator");
            } else {
                assert!(!output.exists());
            }
            assert!(!temporary_output_path(&output).unwrap().exists());
            drop(prepared);
            assert_eq!(pool.snapshot().reserved_bytes, 0, "{fault}");
        }
    }
}

#[test]
fn columnar_compatibility_pruned_generation_and_existing_target_still_validate() {
    let fixture = Fixture::new();
    let source = fixture.source(37);
    let mut request = request(&source);
    request.predicate = Some(predicate(1000));
    let prepared = prepare(
        &request,
        &source,
        VortexLocalPrimitiveRowExportFormat::ArrowIpc,
        policy(),
        CompatibilityLimits::default(),
    )
    .unwrap()
    .unwrap();
    assert!(prepared.plan.metadata_pruned);
    let output = fixture.0.join("destination");
    fs::write(&output, b"prior owner").unwrap();
    assert!(prepared.write(&output, true).is_err());
    assert_eq!(fs::read(&output).unwrap(), b"prior owner");
    fs::remove_file(&output).unwrap();
    fs::rename(&source, fixture.0.join("old-source")).unwrap();
    fixture.source(37);
    assert!(prepared.write(&output, false).is_err());
    assert!(!output.exists());
}

#[test]
fn columnar_compatibility_unsupported_shapes_and_bounds_do_not_change_admission() {
    let fixture = Fixture::new();
    let source = fixture.source(37);
    let mut request = request(&source);
    request.structured_projection.as_mut().unwrap().columns[0].expr =
        VortexStructuredProjectionExpr::StructColumns(vec![ColumnRef::new("text_value").unwrap()]);
    assert!(
        prepare(
            &request,
            &source,
            VortexLocalPrimitiveRowExportFormat::Parquet,
            policy(),
            CompatibilityLimits::default()
        )
        .unwrap()
        .is_none()
    );
    let simple = VortexQueryPrimitiveRequest::project(
        DatasetUri::new(source.display().to_string()).unwrap(),
        ProjectionRequest::All,
    );
    assert!(
        prepare(
            &simple,
            &source,
            VortexLocalPrimitiveRowExportFormat::Avro,
            policy(),
            CompatibilityLimits::default()
        )
        .unwrap()
        .is_none()
    );
    let limits = CompatibilityLimits {
        source_rows: 1,
        ..Default::default()
    };
    assert!(
        prepare(
            &simple,
            &source,
            VortexLocalPrimitiveRowExportFormat::ArrowIpc,
            policy(),
            limits
        )
        .unwrap()
        .is_none()
    );
    assert!(
        CompatibilityLimits {
            batch_rows: usize::MAX,
            ..Default::default()
        }
        .check()
        .is_err()
    );
    assert!(
        CompatibilityLimits {
            file_bytes: 0,
            ..Default::default()
        }
        .check()
        .is_err()
    );
}

#[test]
fn columnar_compatibility_output_cap_is_checked_before_forwarding() {
    let fixture = Fixture::new();
    let path = fixture.0.join("raw");
    let mut file = File::create(&path).unwrap();
    let limits = CompatibilityLimits {
        file_bytes: 1,
        ..Default::default()
    };
    {
        let mut writer = CappedWriter {
            file: &mut file,
            limits: &limits,
            written: 0,
        };
        assert!(writer.write_all(b"ab").is_err());
        assert_eq!(writer.written, 0);
        writer.write_all(b"a").unwrap();
        assert!(writer.write_all(b"b").is_err());
    }
    assert_eq!(fs::read(path).unwrap(), b"a");
}

#[path = "local_primitive_columnar_compat_sink_bench.rs"]
mod benchmark;
