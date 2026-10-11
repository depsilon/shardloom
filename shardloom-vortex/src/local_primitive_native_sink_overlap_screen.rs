//! Guarded complete native-output attribution, deliberately outside normal CI.

use super::*;
use sha2::Sha256;
use std::time::Instant;

const ROWS: usize = 524_288;
const PADDING: usize = 96;
const FILTER_LIMIT: usize = 100_003;

fn hex(bytes: impl IntoIterator<Item = u8>) -> String {
    let mut result = String::new();
    for byte in bytes {
        write!(&mut result, "{byte:02x}").unwrap();
    }
    result
}

fn digest(path: &Path) -> String {
    use std::io::Read as _;
    let mut file = fs::File::open(path).unwrap();
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 64 << 10];
    loop {
        let length = file.read(&mut buffer).unwrap();
        if length == 0 {
            break;
        }
        hash.update(&buffer[..length]);
    }
    hex(hash.finalize())
}

// Check every row without retaining a large decoded oracle or output vector.
fn verify_complete(path: &Path, filtered: bool) -> (usize, String, String) {
    let runtime = local_vortex_runtime(
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
            .expect("explicit fixture allocation"),
    );
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .unwrap();
    let names = local_field_names(file.dtype(), VortexQueryPrimitiveKind::ProjectColumns).unwrap();
    let expected_names = if filtered {
        vec!["destination", "shipment_sequence"]
    } else {
        vec!["shipment_sequence", "priority", "destination"]
    };
    assert_eq!(names, expected_names);
    let fields = file.dtype().as_struct_fields_opt().unwrap();
    assert_eq!(
        fields.field("destination"),
        Some(DType::Utf8(vortex::array::dtype::Nullability::Nullable))
    );
    assert_eq!(
        fields.field("shipment_sequence"),
        Some(DType::Primitive(
            vortex::array::dtype::PType::U64,
            vortex::array::dtype::Nullability::NonNullable,
        ))
    );
    assert!(!file.dtype().is_nullable());
    if !filtered {
        assert_eq!(
            fields.field("priority"),
            Some(DType::Primitive(
                vortex::array::dtype::PType::I64,
                vortex::array::dtype::Nullability::NonNullable,
            ))
        );
    }
    let count = if filtered { FILTER_LIMIT } else { ROWS };
    assert_eq!(file.row_count(), u64::try_from(count).unwrap());
    let mut expected_indices = (0..ROWS)
        .filter(|index| !filtered || index % 97 >= 48)
        .take(count);
    let suffix = "x".repeat(PADDING);
    let mut checked = 0_usize;
    let mut hash = Sha256::new();
    for chunk in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .with_concurrency(1)
        .with_split_by(SplitBy::RowCount(SCAN_ROWS))
        .into_array_iter(&runtime)
        .unwrap()
    {
        let chunk = chunk.unwrap();
        let columns = row_export_columns_from_chunk(&chunk, &names).unwrap();
        for row in 0..chunk.len() {
            let index = expected_indices.next().unwrap();
            let mut actual = serde_json::Map::new();
            for (name, column) in names.iter().zip(&columns) {
                actual.insert(
                    name.clone(),
                    stat_value_to_json_value(&column[row]).unwrap(),
                );
            }
            let mut expected = serde_json::json!({
                "shipment_sequence": index,
                "destination": (!index.is_multiple_of(7)).then(|| format!("港-{index}{suffix}")),
            });
            if !filtered {
                expected["priority"] = (i64::try_from(index % 97).unwrap() - 48).into();
            }
            let actual = serde_json::Value::Object(actual);
            assert_eq!(actual, expected);
            hash.update(serde_json::to_vec(&actual).unwrap());
            hash.update(b"\n");
            checked += 1;
        }
    }
    assert_eq!(checked, count);
    assert!(expected_indices.next().is_none());
    (checked, hex(hash.finalize()), file.dtype().to_string())
}

#[test]
#[ignore = "native sink attribution; release, guarded local TMPDIR and serial execution required"]
#[allow(clippy::assertions_on_constants, clippy::too_many_lines)]
fn complete_native_sink_overlap_screen() {
    assert!(!cfg!(debug_assertions));
    let root = PathBuf::from(std::env::var_os("SHARDLOOM_PERF_UAT_ROOT").unwrap())
        .canonicalize()
        .unwrap();
    let temporary = std::env::temp_dir().canonicalize().unwrap();
    assert!(temporary.starts_with(&root) && temporary != root);
    let fixture = Fixture::new();
    assert!(fixture.0.starts_with(&temporary));
    let source = fixture.source_with_padding(ROWS, PADDING);
    let source_sha256 = digest(&source);
    let source_bytes = fs::metadata(&source).unwrap().len();
    let mut samples = Vec::new();
    for (case, filtered) in [("project_all", false), ("filter_project_limit", true)] {
        let request = if filtered {
            VortexQueryPrimitiveRequest::filter_and_project(
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
            .with_source_order_limit(FILTER_LIMIT)
        } else {
            Fixture::request(&source)
        };
        for parallelism in [1, 4] {
            let mut policy =
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 4).unwrap();
            policy.resource_envelope.memory_budget_bytes = 64 << 20;
            for repetition in 1..=3 {
                let output = fixture
                    .0
                    .join(format!("{case}-{parallelism}-{repetition}.vortex"));
                let start = Instant::now();
                let (report, timings) = overlap_timing::measure(|| {
                    execute_vortex_local_primitive_row_export_with_policy(
                        &request,
                        &output,
                        VortexLocalPrimitiveRowExportFormat::Vortex,
                        false,
                        policy,
                    )
                    .unwrap()
                });
                let call_nanos = start.elapsed().as_nanos();
                assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
                let evidence = report.evidence.native_array_sink.as_ref().unwrap();
                assert_eq!(evidence.adapter_payload_bytes_copied, 0);
                assert_eq!(evidence.scalar_values_materialized, 0);
                assert!(evidence.peak_reserved_bytes <= 64 << 20);
                assert!(
                    evidence.source_generation_validated && evidence.dtype_and_row_count_validated
                );
                assert!(!report.evidence.side_effects.row_read);
                assert!(!report.evidence.side_effects.arrow_converted);
                assert!(!report.evidence.side_effects.fallback_attempted);
                let output_sha256 = digest(&output);
                assert_eq!(evidence.output_sha256, output_sha256);
                let (rows_checked, values_sha256, dtype) = verify_complete(&output, filtered);
                assert_eq!(report.rows_written, u64::try_from(rows_checked).unwrap());
                assert!(!temporary_output_path(&output).unwrap().exists());
                let timings = timings.into_json();
                let measured_nanos = timings["nanos"]
                    .as_object()
                    .unwrap()
                    .values()
                    .map(|value| u128::from(value.as_u64().unwrap()))
                    .sum::<u128>();
                assert!(measured_nanos <= call_nanos);
                assert_eq!(
                    timings["calls"]["writer_push"],
                    evidence.native_arrays_submitted
                );
                for stage in ["writer_finish", "sync_and_reopen", "checksum_and_commit"] {
                    assert_eq!(timings["calls"][stage], 1);
                }
                let record = serde_json::json!({
                    "case": case, "parallelism": parallelism, "repetition": repetition,
                    "source_rows": ROWS, "padding_bytes": PADDING,
                    "source_sha256": source_sha256, "source_bytes": source_bytes,
                    "public_call_nanos": call_nanos, "caller_stage_timings": timings,
                    "rows_checked": rows_checked, "complete_values_sha256": values_sha256,
                    "dtype": dtype, "output_sha256": output_sha256,
                    "output_bytes": fs::metadata(&output).unwrap().len(),
                    "native_arrays_submitted": evidence.native_arrays_submitted,
                    "native_array_logical_bytes": evidence.native_array_logical_bytes,
                    "peak_reserved_bytes": evidence.peak_reserved_bytes,
                    "writer_input_batch_bound": evidence.writer_input_batch_bound,
                    "native_no_fallback": true, "staging_cleaned": true,
                });
                let start = Instant::now();
                drop(report);
                let drop_nanos = start.elapsed().as_nanos();
                let mut record = record;
                record["report_drop_nanos"] = serde_json::json!(drop_nanos);
                record["complete_nanos"] = serde_json::json!(call_nanos + drop_nanos);
                samples.push(record);
                fs::remove_file(output).unwrap();
            }
        }
    }
    assert_eq!(digest(&source), source_sha256);
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    println!(
        "SHARDLOOM_NATIVE_SINK_SCREEN={}",
        serde_json::json!({
            "schema_version": "shardloom.native_sink_overlap_screen.v1",
            "timing_boundary": "public native call through validated synchronized publication and report release; fixture/request construction, oracle verification and observer output excluded",
            "stage_scope": "caller elapsed only; provider jobs may overlap; scan_next includes provider progress, not exclusive IO; buffered-byte samples cover the existing layout counter, not all queued/provider memory",
            "samples": samples,
        })
    );
}
