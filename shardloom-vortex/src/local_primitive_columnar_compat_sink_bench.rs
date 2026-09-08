//! Serial, bounded lifecycle comparison. The parent test fixture supplies the
//! complete independent scalar oracle; query/sink execution remains native.

use super::*;
use sha2::{Digest as _, Sha256};
use std::{fmt::Write as _, io::Read as _, time::Instant};

#[derive(Clone, Copy, Debug)]
enum Variant {
    Legacy,
    Candidate,
}

fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap()
}

fn checksum(path: &Path) -> String {
    let mut file = File::open(path).unwrap();
    let mut bytes = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut digest = Sha256::new();
    loop {
        let count = file.read(&mut bytes).unwrap();
        if count == 0 {
            break;
        }
        digest.update(&bytes[..count]);
    }
    let mut hex = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(hex, "{byte:02x}").unwrap();
    }
    hex
}

fn expected_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("note", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("weight", DataType::Float64, true),
        Field::new("position", DataType::UInt64, false),
    ]))
}

fn work_json(work: &CompatibilityWork) -> Value {
    json!({"native_batches": work.native_batches, "native_logical_bytes": work.native_logical_bytes,
        "arrow_batches": work.arrow_batches, "admitted_arrow_expansion_bytes": work.admitted_arrow_expansion_bytes,
        "max_arrow_batch_bytes": work.max_arrow_batch_bytes,
        "max_observed_parquet_in_progress_bytes": work.max_observed_parquet_in_progress_bytes,
        "writer_reserved_bytes": work.writer_reserved_bytes, "output_bytes": work.output_bytes})
}

fn run(
    variant: Variant,
    source: &Path,
    request: &VortexQueryPrimitiveRequest,
    output: &Path,
    format: VortexLocalPrimitiveRowExportFormat,
    rows: usize,
) -> Value {
    let schema = expected_schema();
    let start = Instant::now();
    let (report, prepare_ns, work, source_opens, provider_workers, pool) = match variant {
        Variant::Legacy => {
            let report = execute_vortex_local_structured_binary_row_export_enabled(
                request,
                source,
                output,
                format,
                false,
                policy(),
            )
            .unwrap();
            (report, None, None, None, None, None)
        }
        Variant::Candidate => {
            let prepared = prepare(
                request,
                source,
                format,
                policy(),
                CompatibilityLimits::default(),
            )
            .unwrap()
            .unwrap();
            let prepare_ns = nanos(start);
            let pool = prepared.plan.session.memory().clone();
            let completed = prepared.write(output, false).unwrap();
            let runtime = prepared.plan.session.snapshot();
            let work = completed.work;
            drop(prepared); // fresh-call elapsed includes joined runtime teardown
            (
                completed.report,
                Some(prepare_ns),
                Some(work),
                Some(runtime.prepared_source_opens),
                Some(runtime.provider_background_workers),
                Some(pool),
            )
        }
    };
    let api_ns = nanos(start);
    // The legacy API does not perform the candidate's sync, full checksum and
    // reopen checks. Report its API elapsed separately, then time those same
    // output checks for a more comparable complete-artifact lifecycle.
    let digest = match variant {
        Variant::Legacy => {
            File::open(output).unwrap().sync_all().unwrap();
            let digest = checksum(output);
            validate_reopen(
                output,
                format,
                &schema,
                u64::try_from(rows).unwrap(),
                &CompatibilityLimits::default(),
            )
            .unwrap();
            digest
        }
        Variant::Candidate => report
            .evidence
            .native_array_sink
            .as_ref()
            .unwrap()
            .output_sha256
            .clone(),
    };
    let complete_ns = nanos(start);
    let owned_after_drop = pool.as_ref().map(|pool| pool.snapshot().reserved_bytes);
    assert!(!report.has_errors());
    assert_eq!(report.rows_written, u64::try_from(rows).unwrap());
    assert!(!report.evidence.side_effects.fallback_attempted);
    if let Some(owned) = owned_after_drop {
        assert_eq!(owned, 0);
    }
    json!({"variant": format!("{variant:?}"), "api_nanos": api_ns,
        "complete_artifact_nanos": complete_ns, "prepare_nanos_in_api": prepare_ns,
        "output_sha256": digest, "output_bytes": fs::metadata(output).unwrap().len(),
        "source_opens": source_opens, "owned_bytes_after_drop": owned_after_drop,
        "max_parallelism_requested": report.max_parallelism_requested,
        "provider_background_workers": provider_workers,
        "row_read": report.evidence.side_effects.row_read,
        "arrow_converted": report.evidence.side_effects.arrow_converted,
        "compatibility_work": work.as_ref().map(work_json),
        "scope": "fresh source preparation through complete artifact and joined source teardown; legacy extra sync/SHA/reopen included only in complete_artifact_nanos; source-generation safety differs and remains reported"})
}

#[test]
#[ignore = "bounded serial native compatibility lifecycle; root owns release timing"]
#[allow(clippy::too_many_lines)] // Keep alternating order and complete verification together.
fn columnar_compatibility_release_lifecycle() {
    assert!(!std::hint::black_box(cfg!(debug_assertions)));
    for rows in [4096, 65_536] {
        let fixture = Fixture::new();
        let source = fixture.source(rows);
        let source_sha = checksum(&source);
        let request = request(&source);
        let oracle = (0..rows).map(expected).collect::<Vec<_>>();
        for format in [
            VortexLocalPrimitiveRowExportFormat::ArrowIpc,
            VortexLocalPrimitiveRowExportFormat::Parquet,
        ] {
            for sample in 0..8 {
                let order = if sample % 2 == 0 {
                    [Variant::Legacy, Variant::Candidate]
                } else {
                    [Variant::Candidate, Variant::Legacy]
                };
                let mut records = Vec::new();
                for (position, variant) in order.into_iter().enumerate() {
                    let output = fixture
                        .0
                        .join(format!("{sample}-{position}.{}", format.as_str()));
                    let mut record = run(variant, &source, &request, &output, format, rows);
                    let verify = Instant::now();
                    let (schema, values) = read(&output, format);
                    assert_eq!(schema, expected_schema());
                    assert_eq!(values, oracle);
                    record["independent_complete_value_verification_nanos"] = json!(nanos(verify));
                    record["complete_values_verified"] = json!(true);
                    record["position"] = json!(position);
                    records.push(record);
                    fs::remove_file(&output).unwrap();
                }
                println!(
                    "COLUMNAR_COMPAT_LIFECYCLE {}",
                    json!({
                        "rows": rows, "format": format.as_str(), "sample": sample,
                        "warmup": sample == 0, "source_sha256": source_sha,
                        "provider_version": crate::UPSTREAM_VORTEX_PROVIDER_VERSION,
                        "actual_release_user_surfaces": cfg!(feature = "release-user-surfaces"),
                        "pairs": records, "source_and_binary_manifest": "pinned by serial runner outside measured process",
                        "cache_scope": "warm local immutable file; no cold-device/RSS claim",
                    })
                );
            }
            let preparation = Instant::now();
            let prepared = prepare(
                &request,
                &source,
                format,
                policy(),
                CompatibilityLimits::default(),
            )
            .unwrap()
            .unwrap();
            let preparation_nanos = nanos(preparation);
            let provider_background_workers =
                prepared.plan.session.snapshot().provider_background_workers;
            let pool = prepared.plan.session.memory().clone();
            let mut samples = Vec::new();
            for sample in 0..8 {
                let output = fixture
                    .0
                    .join(format!("resident-{sample}.{}", format.as_str()));
                let start = Instant::now();
                let complete = prepared.write(&output, false).unwrap();
                let elapsed = nanos(start);
                assert_eq!(read(&output, format).1, oracle);
                let snapshot = prepared.plan.session.snapshot();
                assert_eq!(snapshot.prepared_source_opens, 1);
                assert_eq!(snapshot.completed_executions, sample + 1);
                samples.push(json!({"sample": sample, "warmup": sample == 0, "nanos": elapsed,
                    "source_opens": snapshot.prepared_source_opens, "completed_executions": snapshot.completed_executions,
                    "compatibility_work": work_json(&complete.work), "complete_values_verified": true}));
                fs::remove_file(output).unwrap();
            }
            let closing = Instant::now();
            drop(prepared);
            let close_ns = nanos(closing);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
            println!(
                "COLUMNAR_COMPAT_PREPARED {}",
                json!({"rows": rows, "format": format.as_str(),
                "source_sha256": source_sha, "samples": samples, "close_nanos": close_ns,
                "preparation_nanos": preparation_nanos,
                "max_parallelism_requested": policy().max_parallelism,
                "provider_background_workers": provider_background_workers,
                "owned_bytes_after_drop": pool.snapshot().reserved_bytes,
                "scope": "complete repeated native sink calls; preparation and final joined close separate; not a transport measurement"})
            );
        }
    }
}
