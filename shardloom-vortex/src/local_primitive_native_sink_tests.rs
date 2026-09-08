use super::super::{
    ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, ProjectionRequest, ScalarValue, StatValue,
    VortexLocalPrimitiveRowExportFormat, execute_vortex_local_primitive_row_export_with_policy,
    local_field_names, local_vortex_runtime, row_export_columns_from_chunk,
    stat_value_to_json_value,
};
use super::*;
use crate::VortexStructuredProjectionRequest;
use vortex::{
    VortexSessionDefault as _,
    array::{
        arrays::{PrimitiveArray, StructArray, VarBinViewArray},
        iter::ArrayIteratorAdapter,
        validity::Validity,
    },
    io::session::RuntimeSessionExt as _,
    session::VortexSession,
};

struct Fixture(PathBuf);
#[test]
fn target_created_after_preflight_never_becomes_an_overwrite_admission() {
    use std::io::Write as _;
    for allow_overwrite in [false, true] {
        let fixture = Fixture::new();
        let target = fixture.0.join("raced-target.vortex");
        let mut output = OwnedOutput::new_with_after_preflight(&target, allow_overwrite, || {
            fs::write(&target, b"independent creator bytes").map_err(vortex_error)
        })
        .unwrap();
        let temporary = output.temporary.clone();
        output.file.write_all(b"candidate bytes").unwrap();
        assert!(output.commit().is_err());
        assert_eq!(fs::read(&target).unwrap(), b"independent creator bytes");
        drop(output);
        assert!(!temporary.exists());
        assert_eq!(fs::read(&target).unwrap(), b"independent creator bytes");
    }
}

#[test]
fn existing_targets_are_rejected_without_staging_even_with_overwrite_permission() {
    for allow_overwrite in [false, true] {
        let fixture = Fixture::new();
        let target = fixture.0.join("existing.vortex");
        fs::write(&target, b"independent writer bytes").unwrap();
        let before = fs::metadata(&target).unwrap();
        let error = OwnedOutput::new(&target, allow_overwrite).err().unwrap();
        assert!(
            error
                .to_string()
                .contains("atomic generation-conditional replacement is unavailable")
        );
        assert!(error.to_string().contains("choose a new output path"));
        assert_eq!(fs::read(&target).unwrap(), b"independent writer bytes");
        assert_eq!(
            fs::metadata(&target).unwrap().modified().unwrap(),
            before.modified().unwrap()
        );
        assert!(!temporary_output_path(&target).unwrap().exists());
    }
}

#[test]
fn overwrite_permission_still_allows_atomic_creation_of_an_absent_target() {
    use std::io::Write as _;
    let fixture = Fixture::new();
    let target = fixture.0.join("new.vortex");
    let mut output = OwnedOutput::new(&target, true).unwrap();
    output.file.write_all(b"complete output").unwrap();
    let temporary = output.temporary.clone();
    output.commit().unwrap();
    drop(output);
    assert_eq!(fs::read(target).unwrap(), b"complete output");
    assert!(!temporary.exists());
}

impl Fixture {
    fn new() -> Self {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-native-sink-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn source(&self, rows: usize) -> PathBuf {
        let path = self.0.join("source.vortex");
        let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
        let session = VortexSession::default().with_handle(runtime.handle());
        let array = StructArray::new(
            ["shipment_sequence", "priority", "destination"].into(),
            vec![
                PrimitiveArray::from_iter(0..u64::try_from(rows).unwrap()).into_array(),
                PrimitiveArray::from_iter(
                    (0..rows).map(|index| i64::try_from(index % 97).unwrap() - 48),
                )
                .into_array(),
                VarBinViewArray::from_iter_nullable_str(
                    (0..rows)
                        .map(|index| (!index.is_multiple_of(7)).then(|| format!("港-{index}"))),
                )
                .into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .into_array();
        session
            .write_options()
            .blocking(&runtime)
            .write(
                fs::File::create(&path).unwrap(),
                ArrayIteratorAdapter::new(array.dtype().clone(), [Ok(array)].into_iter()),
            )
            .unwrap();
        path
    }
    fn request(path: &Path) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::project(
            DatasetUri::new(path.display().to_string()).unwrap(),
            ProjectionRequest::All,
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn read_complete(path: &Path) -> (DType, Vec<serde_json::Value>) {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .unwrap();
    let names = local_field_names(file.dtype(), VortexQueryPrimitiveKind::ProjectColumns).unwrap();
    let mut rows = Vec::new();
    for chunk in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&runtime)
        .unwrap()
    {
        let chunk = chunk.unwrap();
        let columns = row_export_columns_from_chunk(&chunk, &names).unwrap();
        for index in 0..chunk.len() {
            let mut row = serde_json::Map::new();
            for (name, values) in names.iter().zip(&columns) {
                row.insert(
                    name.clone(),
                    stat_value_to_json_value(&values[index]).unwrap(),
                );
            }
            rows.push(serde_json::Value::Object(row));
        }
    }
    (file.dtype().clone(), rows)
}

#[test]
fn public_native_sink_streams_nullable_filter_projection_and_ordered_limit_exactly() {
    const ROWS: usize = 40_000;
    let fixture = Fixture::new();
    let source = fixture.source(ROWS);
    let output = fixture.0.join("selected.vortex");
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(source.display().to_string()).unwrap(),
        PredicateExpr::Compare {
            column: ColumnRef::new("priority").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(0),
        },
        ProjectionRequest::columns(vec![
            ColumnRef::new("destination").unwrap(),
            ColumnRef::new("shipment_sequence").unwrap(),
        ]),
    )
    .with_source_order_limit(10_003);
    let report = execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &output,
        VortexLocalPrimitiveRowExportFormat::Vortex,
        false,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    let evidence = report.evidence.native_array_sink.as_ref().unwrap();
    assert!(evidence.native_arrays_submitted > 1);
    assert_eq!(evidence.scalar_values_materialized, 0);
    assert_eq!(evidence.adapter_payload_bytes_copied, 0);
    assert!(!report.evidence.side_effects.row_read);
    assert!(!report.evidence.side_effects.arrow_converted);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(evidence.source_generation_validated && evidence.dtype_and_row_count_validated);
    assert_eq!(evidence.output_sha256.len(), 64);
    assert!(!evidence.pre_limit_result_row_count_exact);
    assert!(evidence.pre_limit_result_row_count >= 10_003);
    let expected = (0..ROWS).filter(|index| index % 97 >= 48).take(10_003).map(|index| serde_json::json!({"shipment_sequence":index,"destination":(!index.is_multiple_of(7)).then(|| format!("港-{index}"))})).collect::<Vec<_>>();
    let (dtype, actual) = read_complete(&output);
    assert_eq!(actual, expected);
    let DType::Struct(fields, _) = dtype else {
        panic!("struct dtype expected")
    };
    assert_eq!(
        fields.field("destination"),
        Some(DType::Utf8(Nullability::Nullable))
    );
    assert_eq!(report.rows_written, 10_003);
    assert!(!temporary_output_path(&output).unwrap().exists());
}

#[test]
fn native_sink_preserves_source_alias_schema_and_empty_typed_output() {
    for rows in [0, 37] {
        let fixture = Fixture::new();
        let source = fixture.source(rows);
        let mut request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(source.display().to_string()).unwrap(),
            ProjectionRequest::All,
            VortexExpressionProjectionRequest::new(Vec::new()),
        );
        request.structured_projection = Some(VortexStructuredProjectionRequest::new(vec![
            crate::VortexStructuredProjectionColumn::new(
                "renamed_port".into(),
                VortexStructuredProjectionExpr::SourceColumn(
                    ColumnRef::new("destination").unwrap(),
                ),
            ),
        ]));
        let output = fixture.0.join("aliases.vortex");
        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output,
            VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        assert!(report.evidence.native_array_sink.is_some());
        let evidence = report.evidence.native_array_sink.as_ref().unwrap();
        assert!(evidence.pre_limit_result_row_count_exact);
        assert_eq!(evidence.pre_limit_result_row_count, rows as u64);
        let (dtype, actual) = read_complete(&output);
        let DType::Struct(fields, _) = dtype else {
            panic!("struct dtype")
        };
        assert_eq!(
            fields
                .names()
                .iter()
                .map(std::convert::AsRef::as_ref)
                .collect::<Vec<_>>(),
            vec!["renamed_port"]
        );
        assert_eq!(
            fields.field("renamed_port"),
            Some(DType::Utf8(Nullability::Nullable))
        );
        assert_eq!(actual, (0..rows).map(|index| serde_json::json!({"renamed_port":(!index.is_multiple_of(7)).then(|| format!("港-{index}"))})).collect::<Vec<_>>());
    }
}

#[test]
fn pre_limit_count_is_exact_when_filter_scan_exhausts_or_footer_proves_it() {
    let fixture = Fixture::new();
    let source = fixture.source(37);
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(source.display().to_string()).unwrap(),
        PredicateExpr::Compare {
            column: ColumnRef::new("priority").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(-45),
        },
        ProjectionRequest::All,
    )
    .with_source_order_limit(100);
    for (name, request, expected) in [
        ("exhausted", request, 34),
        (
            "footer",
            Fixture::request(&source).with_source_order_limit(7),
            37,
        ),
    ] {
        let report = try_execute(
            &request,
            &source,
            &fixture.0.join(format!("{name}.vortex")),
            false,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap()
        .unwrap();
        let evidence = report.evidence.native_array_sink.unwrap();
        assert!(evidence.pre_limit_result_row_count_exact);
        assert_eq!(evidence.pre_limit_result_row_count, expected);
        assert_eq!(report.pre_limit_result_row_count, expected);
    }
}

#[test]
fn source_generation_failure_and_output_failures_preserve_destination() {
    let fixture = Fixture::new();
    let source = fixture.source(37);
    let request = Fixture::request(&source);
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let prepared = prepare(&request, &source, policy).unwrap().unwrap();
    let output = fixture.0.join("existing.vortex");
    fs::write(&output, b"existing destination").unwrap();
    fs::rename(&source, fixture.0.join("old-source.vortex")).unwrap();
    fixture.source(38);
    assert!(prepared.write(&request, &output, true, policy).is_err());
    assert_eq!(fs::read(&output).unwrap(), b"existing destination");
    assert!(!temporary_output_path(&output).unwrap().exists());
    assert!(try_execute(&request, &source, &output, false, policy).is_err());
    assert!(try_execute(&request, &source, &source, true, policy).is_err());
    let prepared = prepare(&request, &source, policy).unwrap().unwrap();
    let _pressure = prepared
        .session
        .memory()
        .reserve(
            policy.resource_envelope().memory_budget_bytes
                - prepared.session.snapshot().memory.reserved_bytes,
        )
        .unwrap();
    assert!(prepared.write(&request, &output, true, policy).is_err());
    assert_eq!(fs::read(&output).unwrap(), b"existing destination");
    assert!(!temporary_output_path(&output).unwrap().exists());
}

#[test]
fn failed_temporary_unlink_never_removes_the_published_destination() {
    use std::io::Write as _;
    let fixture = Fixture::new();
    let target = fixture.0.join("unlink-failure.vortex");
    let mut output = OwnedOutput::new(&target, false).unwrap();
    output.file.write_all(b"complete output").unwrap();
    let temporary = output.temporary.clone();
    let error = output
        .commit_with_unlink(|_| Err(std::io::Error::other("injected unlink failure")))
        .unwrap_err();
    assert!(error.to_string().contains("output was published"));
    assert!(error.to_string().contains("destination preserved"));
    assert_eq!(fs::read(&target).unwrap(), b"complete output");
    drop(output);
    assert!(!temporary.exists());

    let target = fixture.0.join("replaced-after-publication.vortex");
    let mut output = OwnedOutput::new(&target, false).unwrap();
    let temporary = output.temporary.clone();
    let replacement = fixture.0.join("replacement.vortex");
    fs::write(&replacement, b"another writer's output").unwrap();
    let error = output
        .commit_with_unlink(|_| {
            fs::rename(&replacement, &target)?;
            Err(std::io::Error::other(
                "injected unlink failure after replacement",
            ))
        })
        .unwrap_err();
    assert!(error.to_string().contains("destination preserved"));
    assert_eq!(fs::read(&target).unwrap(), b"another writer's output");
    drop(output);
    assert!(!temporary.exists());
    assert_eq!(fs::read(&target).unwrap(), b"another writer's output");
}

#[test]
fn final_source_generation_failure_discards_staged_output_before_commit() {
    let fixture = Fixture::new();
    let source = fixture.source(17);
    let session = ResidentVortexSession::new(8 * 1024 * 1024, 1).unwrap();
    let prepared = session.prepare_file(&source).unwrap();
    let output = fixture.0.join("new.vortex");
    let staged = prepared.with_native_execution(|_, _, _| {
        let output = OwnedOutput::new(&output, true)?;
        fs::rename(&source, fixture.0.join("old-source.vortex")).unwrap();
        fixture.source(18);
        Ok(output)
    });
    assert!(staged.is_err());
    assert!(!output.exists());
    assert!(!temporary_output_path(&output).unwrap().exists());
}

#[test]
fn source_read_reservation_failure_cleans_staging_after_writer_admission() {
    let fixture = Fixture::new();
    let source = fixture.source(40_000);
    let request = Fixture::request(&source);
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let prepared = prepare(&request, &source, policy).unwrap().unwrap();
    let metadata = 40_000_u64.div_ceil(SCAN_ROWS as u64) * METADATA_BYTES_PER_CHUNK + 128 * 1024;
    let available = policy.resource_envelope().memory_budget_bytes
        - prepared.session.snapshot().memory.reserved_bytes;
    // Admit the complete metadata lease, leaving one byte for the source read.
    // The actual provider allocator must reject data buffers after staging begins.
    let _pressure = prepared
        .session
        .memory()
        .reserve(available - metadata - 1)
        .unwrap();
    let output = fixture.0.join("new.vortex");
    let error = prepared.write(&request, &output, true, policy).unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert!(!output.exists());
    assert!(!temporary_output_path(&output).unwrap().exists());
}

#[test]
fn false_filter_sink_writes_typed_empty_output_without_data_arrays() {
    let fixture = Fixture::new();
    let source = fixture.source(37);
    let request = VortexQueryPrimitiveRequest::filter_and_project(
        DatasetUri::new(source.display().to_string()).unwrap(),
        PredicateExpr::AlwaysFalse,
        ProjectionRequest::columns(vec![ColumnRef::new("destination").unwrap()]),
    );
    let output = fixture.0.join("empty.vortex");
    let report = try_execute(
        &request,
        &source,
        &output,
        false,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.rows_written, 0);
    assert!(report.evidence.upstream_scan_called);
    assert_eq!(report.arrays_read_count, 0);
    assert!(!report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.write_io);
    let (dtype, rows) = read_complete(&output);
    let DType::Struct(fields, _) = dtype else {
        panic!("struct dtype")
    };
    assert_eq!(
        fields.field("destination"),
        Some(DType::Utf8(Nullability::Nullable))
    );
    assert!(rows.is_empty());
}

#[test]
fn owned_staging_rejects_new_destination_and_preserves_replaced_temporary() {
    let fixture = Fixture::new();
    let target = fixture.0.join("target.vortex");
    let mut output = OwnedOutput::new(&target, false).unwrap();
    fs::write(&target, b"concurrent destination").unwrap();
    assert!(output.commit().is_err());
    let saved = fixture.0.join("saved-temporary");
    fs::rename(&output.temporary, &saved).unwrap();
    fs::write(&output.temporary, b"unrelated temporary").unwrap();
    let temporary = output.temporary.clone();
    drop(output);
    assert_eq!(fs::read(target).unwrap(), b"concurrent destination");
    assert_eq!(fs::read(temporary).unwrap(), b"unrelated temporary");
}

#[test]
fn native_source_alias_export_filters_source_fields_before_projection_and_limit() {
    let fixture = Fixture::new();
    let source = fixture.source(257);
    for threshold in [0, 1000] {
        let mut request = VortexQueryPrimitiveRequest::structured_project_rows(
            DatasetUri::new(source.display().to_string()).unwrap(),
            VortexStructuredProjectionRequest::new(vec![
                crate::VortexStructuredProjectionColumn::new(
                    "renamed_port".into(),
                    VortexStructuredProjectionExpr::SourceColumn(
                        ColumnRef::new("destination").unwrap(),
                    ),
                ),
                crate::VortexStructuredProjectionColumn::new(
                    "position".into(),
                    VortexStructuredProjectionExpr::SourceColumn(
                        ColumnRef::new("shipment_sequence").unwrap(),
                    ),
                ),
            ]),
        )
        .with_source_order_limit(17);
        // priority is a source-only field, absent from the projected aliases.
        request.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("priority").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(threshold),
        });
        let output = fixture.0.join(format!("source-filter-{threshold}.vortex"));
        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output,
            VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        assert!(report.evidence.native_array_sink.is_some());
        let expected = (0..257_usize)
            .filter(|index| i64::try_from(index % 97).unwrap() - 48 >= threshold)
            .take(17)
            .map(|index| {
                serde_json::json!({
                    "renamed_port":(!index.is_multiple_of(7)).then(|| format!("港-{index}")),
                    "position":index,
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(read_complete(&output).1, expected);
        assert_eq!(report.rows_written, u64::try_from(expected.len()).unwrap());
        assert!(!temporary_output_path(&output).unwrap().exists());
    }
}

#[test]
fn native_sink_declines_complex_structured_expressions_without_effects() {
    let fixture = Fixture::new();
    let source = fixture.source(3);
    let mut request = VortexQueryPrimitiveRequest::expression_project_rows(
        DatasetUri::new(source.display().to_string()).unwrap(),
        ProjectionRequest::All,
        VortexExpressionProjectionRequest::new(Vec::new()),
    );
    request.structured_projection = Some(VortexStructuredProjectionRequest::new(vec![
        crate::VortexStructuredProjectionColumn::new(
            "array".into(),
            VortexStructuredProjectionExpr::ArrayLiteral(vec![ScalarValue::Int64(1)]),
        ),
    ]));
    let output = fixture.0.join("not-created.vortex");
    assert!(
        try_execute(
            &request,
            &source,
            &output,
            false,
            VortexLocalPrimitiveExecutionPolicy::single_threaded()
        )
        .unwrap()
        .is_none()
    );
    assert!(!output.exists());
    request.predicate = Some(PredicateExpr::AlwaysTrue);
    let error = try_execute(
        &request,
        &source,
        &output,
        false,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .err()
    .unwrap();
    assert!(
        error
            .to_string()
            .contains("filtered expression output requires source-column projections")
    );
    assert!(!output.exists());
    assert!(!temporary_output_path(&output).unwrap().exists());
}
