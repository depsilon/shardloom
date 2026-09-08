//! Release-only paired native-query experiment alongside segment reuse I/O tests.
//! Fixture/oracle creation and independent scalar verification are untimed.

use super::*;
use crate::resident_session::{OwnedVortexResultBatch, read_observer::ReadObservation};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_exec::live_memory::Budgeted;
use std::fmt::Write as _;
use vortex::array::{
    Columnar,
    dtype::{DType, Nullability, PType},
};

const PAIRS: usize = 7;
const ARRAY_LIMIT: usize = 64;
const OUTPUT_BYTES: u64 = 8 << 20;
const SESSION_BYTES: u64 = 32 << 20;

fn candidate_policy(file: &vortex::file::VortexFile) -> SegmentReusePolicy {
    let column = ColumnRef::new("renamed_text").unwrap();
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::IsNotNull {
            column: column.clone(),
        },
        PredicateExpr::Compare {
            column,
            op: shardloom_core::ComparisonOp::GtEq,
            value: shardloom_core::StatValue::Utf8("m".into()),
        },
    ]);
    let projected = [ColumnRef::new("exact_identifier").unwrap()];
    let admitted = SegmentReusePolicy::for_scan(
        &predicate,
        &projected,
        file.footer().layout().as_ref(),
        SESSION_BYTES,
    );
    if let Some(policy) = admitted {
        policy
    } else {
        // Explicit whole-struct Flat negative control only. Match the positive
        // case's production byte/entry limits; automatic admission stays off.
        assert!(
            file.footer()
                .layout()
                .is::<vortex::layout::layouts::flat::Flat>()
        );
        SegmentReusePolicy {
            max_retained_bytes: SESSION_BYTES / 16,
            max_segment_bytes: usize::try_from(SESSION_BYTES / 16).unwrap(),
            max_entries: 128,
        }
    }
}

fn elapsed(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap()
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    let mut value = String::new();
    for byte in bytes.as_ref() {
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value
}

fn observation(value: &ReadObservation) -> Value {
    assert_eq!(
        value.failed_read_calls + value.failed_before_read + value.cancelled_before_read,
        0
    );
    assert_eq!(value.rejected_requests, 0);
    json!({
        "closed": value.closed, "pending_jobs": value.pending_jobs,
        "admitted_requests": value.admitted_requests,
        "completed_read_calls": value.completed_read_calls,
        "completed_read_bytes": value.completed_read_bytes,
        "attempted_bytes": value.attempted_bytes,
        "peak_in_flight": value.peak_in_flight,
        "failures": 0, "cancellations": 0,
        "completed_ranges": value.completed_ranges.iter().map(|range| {
            json!([range.offset, range.length])
        }).collect::<Vec<_>>(),
    })
}

fn verify(result: &OwnedVortexResultBatch) -> (usize, String) {
    let mut expected = (0..ROWS).filter(|row| row % 3 == 2);
    let mut context = result.create_execution_ctx();
    let mut digest = Sha256::new();
    digest.update(b"exact_identifier:i64:nonnullable;ordered;v1");
    let mut rows = 0;
    for array in result.arrays() {
        let fields = array.dtype().as_struct_fields();
        assert_eq!(
            fields.names().iter().map(AsRef::as_ref).collect::<Vec<_>>(),
            ["exact_identifier"]
        );
        assert_eq!(
            fields.field("exact_identifier"),
            Some(DType::Primitive(PType::I64, Nullability::NonNullable))
        );
        let expression = vortex::expr::get_item("exact_identifier", vortex::expr::root())
            .bind(array.dtype())
            .unwrap();
        let identifiers = array
            .clone()
            .apply_bound(&expression)
            .unwrap()
            .execute::<Columnar>(&mut context)
            .unwrap()
            .into_array();
        for row in 0..array.len() {
            let source_row = expected.next().expect("extra native result row");
            let exact = BASE + i64::try_from(source_row).unwrap();
            assert_eq!(
                identifiers.execute_scalar(row, &mut context).unwrap(),
                exact.into()
            );
            // Hash only after the actual typed scalar matched the independent
            // integer oracle; JSON number conversion is not used for equality.
            digest.update(exact.to_le_bytes());
            rows += 1;
        }
    }
    assert!(
        expected.next().is_none(),
        "native result omitted expected rows"
    );
    assert_eq!(result.row_count(), u64::try_from(rows).unwrap());
    (rows, hex(digest.finalize()))
}

fn collect_owned(
    file: &vortex::file::VortexFile,
    resident: &ResidentVortexSession,
    runtime: &CurrentThreadRuntime,
) -> OwnedVortexResultBatch {
    let field = vortex::expr::get_item("renamed_text", vortex::expr::root());
    let filter = vortex::expr::and(
        vortex::expr::is_not_null(field.clone()),
        vortex::expr::gt_eq(field, vortex::expr::lit("m")),
    );
    let scan = file
        .scan()
        .unwrap()
        .with_ordered(true)
        .with_concurrency(2)
        .with_projection(
            vortex::expr::select(["exact_identifier"], vortex::expr::root())
                .bind(file.dtype())
                .unwrap(),
        )
        .with_filter(filter.bind(file.dtype()).unwrap());
    let lease = resident
        .memory()
        .reserve(u64::try_from(ARRAY_LIMIT * std::mem::size_of::<ArrayRef>()).unwrap())
        .unwrap();
    let mut arrays = Vec::new();
    arrays.try_reserve_exact(ARRAY_LIMIT).unwrap();
    assert!(arrays.capacity() <= ARRAY_LIMIT);
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    for array in scan.into_array_iter(runtime).unwrap() {
        let array = array.unwrap();
        rows = rows
            .checked_add(u64::try_from(array.len()).unwrap())
            .unwrap();
        bytes = bytes.checked_add(array.nbytes()).unwrap();
        assert!(rows <= ROWS as u64 && bytes <= OUTPUT_BYTES && arrays.len() < ARRAY_LIMIT);
        arrays.push(array);
    }
    OwnedVortexResultBatch {
        arrays: Budgeted::new(arrays, lease),
        runtime: Arc::clone(&resident.0),
        rows,
        logical_buffer_bytes: bytes,
    }
}

#[allow(clippy::too_many_lines)] // Keep timing, read drain, and ownership boundaries together.
fn run(fixture: &Fixture, cached: bool) -> Value {
    let session_started = Instant::now();
    let resident = ResidentVortexSession::new(SESSION_BYTES, 2).unwrap();
    let session_prepare_nanos = elapsed(session_started);
    let mut report = resident.with_native_session(|session, runtime| {
        let open_started = Instant::now();
        let reader = observer(&fixture.path(), session, runtime);
        let mut file = runtime.block_on(session.open_options().open_read(reader.clone())).unwrap();
        let open_nanos = elapsed(open_started);
        let opened = reader.snapshot().unwrap();
        assert_eq!(opened.pending_jobs, 0);
        let query_started = Instant::now();
        let validator = reader.clone();
        let cache = cached.then(|| ScanSegmentReuse::new(file.segment_source(),
            resident.memory().clone(), candidate_policy(&file), move || validator.validate_generation()).unwrap());
        if let Some(cache) = &cache { file = file.with_segment_source(Arc::new(cache.clone())); }
        let result = collect_owned(&file, &resident, runtime);
        reader.validate_generation().unwrap();
        let native_query_to_owned_result_nanos = elapsed(query_started);
        let close_started = Instant::now();
        let cache_snapshot = cache.as_ref().map(|cache| cache.close().unwrap());
        drop(file);
        drop(cache);
        let cache_and_file_close_nanos = elapsed(close_started);
        let drain_started = Instant::now();
        let final_reads = reader.close_and_drain(Duration::from_secs(10)).unwrap();
        reader.validate_generation().unwrap();
        let completed_read_drain_nanos = elapsed(drain_started);
        // One directly measured inclusive interval through ready owned output,
        // optional cache/file teardown, and stable completed I/O. No scalar
        // oracle or evidence JSON is inside this clock.
        let native_query_close_drain_owned_result_nanos = elapsed(query_started);
        assert!(final_reads.closed);
        assert_eq!(final_reads.pending_jobs, 0);
        assert_eq!(reader.snapshot().unwrap(), final_reads);
        let owned_rows = result.row_count();
        let owned_logical_bytes = result.logical_buffer_bytes();
        let retained_result_memory = resident.memory().snapshot();
        let verify_started = Instant::now();
        let (rows, result_sha256) = verify(&result);
        let independent_scalar_verification_nanos = elapsed(verify_started);
        assert_eq!(reader.snapshot().unwrap(), final_reads,
            "independent result verification triggered additional source reads");
        let drop_started = Instant::now();
        drop(result);
        let owned_result_drop_nanos = elapsed(drop_started);
        let mut cache_evidence = "{}".to_owned();
        if let Some(snapshot) = cache_snapshot { snapshot.annotate(&mut cache_evidence).unwrap(); }
        drop(reader);
        Ok(json!({
            "cached": cached, "rows": rows, "owned_rows": owned_rows,
            "owned_logical_buffer_bytes": owned_logical_bytes,
            "complete_typed_values_equal": true, "result_sha256": result_sha256,
            "session_prepare_nanos": session_prepare_nanos, "native_open_nanos": open_nanos,
            "native_query_to_owned_result_nanos": native_query_to_owned_result_nanos,
            "cache_and_file_close_nanos": cache_and_file_close_nanos,
            "completed_read_drain_nanos": completed_read_drain_nanos,
            "native_query_close_drain_owned_result_nanos": native_query_close_drain_owned_result_nanos,
            "independent_scalar_verification_nanos": independent_scalar_verification_nanos,
            "owned_result_drop_nanos": owned_result_drop_nanos,
            "open_observation": observation(&opened), "final_observation": observation(&final_reads),
            "query_completed_read_bytes": final_reads.completed_read_bytes - opened.completed_read_bytes,
            "query_completed_read_calls": final_reads.completed_read_calls - opened.completed_read_calls,
            "session_owned_bytes_with_result": retained_result_memory.reserved_bytes,
            "cache": serde_json::from_str::<Value>(&cache_evidence).unwrap(),
        }))
    }).unwrap();
    let snapshot = resident.snapshot();
    assert_eq!(snapshot.memory.reserved_bytes, 0);
    assert_eq!(snapshot.memory.denied_reservations, 0);
    report["session_peak_owned_bytes"] = json!(snapshot.memory.peak_reserved_bytes);
    report["session_owned_bytes_after_all_results_and_reads_drop"] =
        json!(snapshot.memory.reserved_bytes);
    report["session_denied_reservations"] = json!(snapshot.memory.denied_reservations);
    report["provider_background_cpu_drivers"] = json!(snapshot.provider_background_workers);
    let teardown_started = Instant::now();
    drop(resident);
    report["resident_teardown_nanos"] = json!(elapsed(teardown_started));
    report
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    assert!(!values.is_empty());
    let mut values = values.to_vec();
    values.sort_unstable();
    values[(values.len() * percentile).div_ceil(100).saturating_sub(1)]
}

#[test]
#[ignore = "paired release-mode native query lifecycle experiment; run serially with --release --ignored --exact"]
#[allow(clippy::too_many_lines)] // Emit every pair and its measured scope in one experiment.
#[allow(clippy::assertions_on_constants)] // Compile in debug; reject only if this ignored experiment is explicitly run there.
fn scan_segment_reuse_release_lifecycle_pairs() {
    assert!(
        !cfg!(debug_assertions),
        "this experiment requires --release"
    );
    let mut layouts = Vec::new();
    for columnar in [false, true] {
        let fixture = Fixture::with_layout(columnar);
        let fixture_bytes = std::fs::read(fixture.path()).unwrap();
        let source_sha256 = hex(Sha256::digest(&fixture_bytes));
        let source_file_bytes = fixture_bytes.len();
        drop(fixture_bytes);
        let mut records = Vec::new();
        let mut baseline_times = Vec::new();
        let mut candidate_times = Vec::new();
        for iteration in 0..=PAIRS {
            let warmup = iteration == 0;
            let order = if iteration % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            let first = run(&fixture, order[0]);
            let second = run(&fixture, order[1]);
            let (baseline, candidate) = if order[0] {
                (second, first)
            } else {
                (first, second)
            };
            assert_eq!(baseline["result_sha256"], candidate["result_sha256"]);
            assert_eq!(baseline["rows"], candidate["rows"]);
            let bytes = |value: &Value| value["query_completed_read_bytes"].as_u64().unwrap();
            if columnar {
                assert!(bytes(&candidate) < bytes(&baseline));
            } else {
                assert_eq!(bytes(&candidate), bytes(&baseline));
            }
            assert_eq!(
                baseline["open_observation"]["completed_read_bytes"],
                candidate["open_observation"]["completed_read_bytes"]
            );
            if !warmup {
                baseline_times.push(
                    baseline["native_query_close_drain_owned_result_nanos"]
                        .as_u64()
                        .unwrap(),
                );
                candidate_times.push(
                    candidate["native_query_close_drain_owned_result_nanos"]
                        .as_u64()
                        .unwrap(),
                );
            }
            records.push(json!({"iteration": iteration, "warmup": warmup,
                "order": order, "baseline": baseline, "candidate": candidate}));
        }
        let distribution = |times: &[u64]| {
            json!({
                "samples": times, "p50_nanos": percentile(times, 50),
                "p95_nanos": percentile(times, 95), "p99_nanos": percentile(times, 99),
                "percentile_method": "nearest_rank;seven_samples;p95_and_p99_are_observed_maximum",
            })
        };
        layouts.push(json!({
            "layout": if columnar { "native_struct_separate_fields" } else { "whole_struct_flat_negative_control" },
            "source_file_sha256": source_sha256, "source_file_bytes": source_file_bytes,
            "automatic_gate_admits_layout": columnar,
            "measured_pairs": PAIRS, "warmup_pairs": 1,
            "baseline_native_query_close_drain": distribution(&baseline_times),
            "candidate_native_query_close_drain": distribution(&candidate_times),
            "candidate_to_baseline_p50_ratio_exact": {
                "numerator_nanos": percentile(&candidate_times, 50),
                "denominator_nanos": percentile(&baseline_times, 50),
            },
            "records": records,
        }));
    }
    let report = json!({
        "schema": "shardloom.scan_segment_reuse_lifecycle.v1",
        "intended_validation_feature": "release-user-surfaces",
        "release_user_surfaces_enabled": cfg!(feature = "release-user-surfaces"),
        "debug_assertions": cfg!(debug_assertions),
        "exact_cases": 2 * (PAIRS + 1) * 2, "measured_cases": 2 * PAIRS * 2,
        "rows_per_fixture": ROWS, "session_budget_bytes": SESSION_BYTES,
        "cache_retention_limit_bytes": SESSION_BYTES / 16,
        "cache_segment_limit_bytes": SESSION_BYTES / 16, "cache_entry_limit": 128,
        "output_array_limit": ARRAY_LIMIT, "output_logical_byte_limit": OUTPUT_BYTES,
        "scope": "one_local_process;fresh_resident_session_and_native_open_per_case;alternating_cache_setting;same_file_within_each_layout;OS_page_cache_not_flushed;no_external_engine",
        "timing_scope": "native_query_to_owned_result_includes_cache_construction_and_copy;query_close_drain_is_direct_inclusive_wall_through_ready_owned_result_and_stable_IO;scalar_verification_outside_all_query_clocks;result_drop_after_verification_reported_separately;no_JSON_sink_timing",
        "read_scope": "successful_positional_OS_read_bytes_including_repeats_and_coalescing_gaps;open_separate;not_physical_device_or_cold_cache_bytes",
        "memory_scope": "session_allocator_payload_plus_owned_result_reference_capacity;cache_retention_full_capacity_including_live_result_slices;fixture_oracle_provider_metadata_OS_cache_and_RSS_excluded",
        "provenance_scope": "fixture_file_SHA_recorded;runner_must_capture_source_revision_patch_and_release_executable_SHA_separately",
        "layouts": layouts,
    });
    eprintln!("scan_segment_reuse_lifecycle={report}");
}
