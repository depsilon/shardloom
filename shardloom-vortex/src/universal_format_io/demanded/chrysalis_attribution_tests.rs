//! Explicit owner-cost diagnostic; never part of ordinary tests or execution.

use super::*;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_core::ColumnRef;
use std::{fmt::Write as _, path::Path, time::Instant};
use vortex::array::VortexSessionExecute as _;

fn file_hash(path: &Path) -> String {
    let mut file = std::fs::File::open(path).unwrap();
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
    loop {
        let length = file.read(&mut buffer).unwrap();
        if length == 0 {
            break;
        }
        hash.update(&buffer[..length]);
    }
    let mut encoded = String::with_capacity(64);
    for byte in hash.finalize() {
        write!(encoded, "{byte:02x}").unwrap();
    }
    encoded
}

fn validate(
    source: &ResidentMemorySource,
    first: u64,
    names: &[&str],
    text: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let array = source.batch_array(context)?;
    let mut execution = context.native_session().create_execution_ctx();
    for name in names {
        let index: u64 = name[1..].parse().unwrap();
        let field = crate::local_primitives::logical_field_from_native_array(array, name)?;
        for offset in 0..source.row_count() {
            let row = first + offset as u64;
            let null = (text || index != 0) && (row + index).is_multiple_of(17);
            let actual = field
                .execute_scalar(offset, &mut execution)
                .map_err(failed)?;
            assert_eq!(actual.is_null(), null, "{name}/{row}");
            if text {
                let expected = (!null).then(|| {
                    format!(
                        "region-{}-{}-tail",
                        row % 997,
                        char::from(b'A' + u8::try_from(index).unwrap())
                            .to_string()
                            .repeat(80)
                    )
                });
                assert_eq!(
                    actual
                        .as_utf8()
                        .value()
                        .map(|value| value.as_str().to_owned()),
                    expected,
                    "{name}/{row}"
                );
            } else {
                let expected = (!null).then(|| {
                    if index == 0 {
                        i64::try_from(row).unwrap()
                    } else {
                        i64::try_from((row * (index + 3)) % 100_003).unwrap() - 50_001
                    }
                });
                assert_eq!(actual.as_primitive().as_::<i64>(), expected, "{name}/{row}");
            }
        }
    }
    Ok(())
}

#[test]
#[ignore = "explicit frozen Chrysalis source-owner diagnostic; requires supervised local inputs"]
#[allow(clippy::too_many_lines)] // One frozen six-call diagnostic, including complete validation and drain.
#[allow(clippy::assertions_on_constants)] // Ignored diagnostic must reject debug execution, not debug compilation.
fn chrysalis_exact_source_owner_attribution() {
    assert!(
        !cfg!(debug_assertions),
        "owner attribution requires release"
    );
    let manifest = std::env::var_os("SHARDLOOM_CHRYSALIS_EXPOSURE_MANIFEST")
        .expect("explicit frozen exposure manifest is required");
    let manifest_path = Path::new(&manifest);
    let manifest_hash = file_hash(manifest_path);
    let manifest: Value = serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["rows"].as_u64(), Some(131_072));
    assert_eq!(manifest["row_group_rows"].as_u64(), Some(16_384));
    let mut calls = 0;
    for input in manifest["inputs"].as_array().unwrap() {
        let path = Path::new(input["path"].as_str().unwrap());
        let expected_hash = input["sha256"].as_str().unwrap();
        assert_eq!(file_hash(path), expected_hash);
        let text = input["kind"].as_str().unwrap() == "text";
        let names: Vec<_> = (0..4)
            .map(|index| format!("{}{index}", if text { 's' } else { 'n' }))
            .collect();
        for indices in [vec![0, 1], vec![1, 2], vec![0, 2, 3]] {
            let selected: Vec<_> = indices.iter().map(|&index| names[index].as_str()).collect();
            let session = ResidentVortexSession::new(512 << 20, 1).unwrap();
            let pool = session.shared_memory_pool();
            let cancellation = CancellationToken::default();
            let mut owner_nanos = 0_u128;
            let mut batches = 0_u64;
            let mut rows = 0_u64;
            let report = session
                .with_native_execution_context(&cancellation, |context| {
                    let source = LocalColumnarSource::admit(path, "parquet", &pool, &cancellation)?;
                    let demand = SourceDemand {
                        projection: ProjectionRequest::Columns(
                            selected
                                .iter()
                                .map(|name| ColumnRef::new(*name))
                                .collect::<Result<_>>()?,
                        ),
                        ..SourceDemand::default()
                    };
                    let prepared = source.prepare(&demand, &session, &cancellation)?;
                    let mut reader = prepared.open(context)?;
                    loop {
                        let started = Instant::now();
                        let next = reader.next_native_located(&session, context)?;
                        owner_nanos += started.elapsed().as_nanos();
                        let Some((native, first)) = next else {
                            break;
                        };
                        assert_eq!(first, rows);
                        validate(&native, first, &selected, text, context)?;
                        rows += native.row_count() as u64;
                        batches += 1;
                        // Ordinary streaming ownership: do not retain earlier
                        // batches just to simplify this diagnostic's validation.
                        drop(native);
                    }
                    source.validate_generation_controlled(&cancellation)?;
                    reader.report()
                })
                .unwrap();
            assert_eq!(rows, 131_072);
            assert_eq!(report.rows_decoded, rows);
            assert_eq!(report.row_groups_read, 8);
            assert_eq!(report.projected_columns, selected.len());
            assert!(owner_nanos > 0 && report.data_read_bytes > 0);
            let peak = pool.snapshot().peak_reserved_bytes;
            drop(session);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
            eprintln!(
                "CHRYSALIS_OWNER {}",
                json!({
                    "kind": input["kind"], "columns": selected,
                    "source_sha256": expected_hash, "rows": rows, "batches": batches,
                    "row_groups_read": report.row_groups_read,
                    "data_read_bytes": report.data_read_bytes,
                    "data_read_requests": report.data_read_requests,
                    "reader_native_intake_nanos": owner_nanos,
                    "owner_clock_scope": "next_native_located_including_source_validation_selected_chunk_reads_parquet_decode_native_intake_and_eof;excludes_initial_admission_reference_validation_and_query_operators;validation_between_batches_changes_scheduling;not_a_complete_query_or_speed_comparison",
                    "peak_reserved_bytes": peak, "final_reserved_bytes": 0,
                    "fallback_attempted": false, "external_engine_invoked": false,
                    "complete_values_verified": true,
                })
            );
            calls += 1;
        }
        assert_eq!(file_hash(path), expected_hash);
    }
    assert_eq!(calls, 6);
    assert_eq!(file_hash(manifest_path), manifest_hash);
    eprintln!("CHRYSALIS_OWNER_COMPLETE {calls}");
}
