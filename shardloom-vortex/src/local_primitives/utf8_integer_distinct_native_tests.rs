//! Ordinary-only public and held-source controls for the focused DISTINCT port.
//! UTF8 prepared/owned admission and result projection stay outside this patch.
use super::*;
use crate::{
    VortexAggregateSpillPolicy, local_primitives as runtime,
    resident_session::ResidentVortexSession,
};
use runtime::prepared_aggregate::{
    PreparedAggregateDisposition, UnretainedVortexAggregate, prepare_aggregate_for_optional_reuse,
    prepare_aggregate_in_session,
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-focused-utf8-distinct-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn write(&self, name: &str, chunks: Vec<ArrayRef>) -> PathBuf {
        let path = self.0.join(name);
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut file = fs::File::create_new(&path).unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(
                runtime::native_flat_layout::SequentialNativeFlatLayout::strategy(chunks.len()),
            )
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut file, chunks[0].dtype().clone());
        let expected_rows = chunks.iter().map(|array| array.len() as u64).sum::<u64>();
        for array in chunks {
            writer.push(array).unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), expected_rows);
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn policy(parallelism: usize) -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
    policy.resource_envelope.memory_budget_bytes = 32 << 20;
    policy
}
fn payload(report: &runtime::VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
    assert!(!report.fallback_execution_allowed);
    assert!(!report.spill_io_performed);
    serde_json::from_str(
        report
            .result_summary
            .as_ref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap()
}
fn unretained(
    request: &VortexQueryPrimitiveRequest,
    parallelism: usize,
) -> UnretainedVortexAggregate {
    match prepare_aggregate_for_optional_reuse(request, policy(parallelism)).unwrap() {
        Some(PreparedAggregateDisposition::Unretained(operation)) => operation,
        Some(PreparedAggregateDisposition::Reusable(_)) => {
            panic!("focused port must not widen retained UTF8 admission")
        }
        None => {
            panic!("ordinary UTF8 DISTINCT request must retain its one-shot source disposition")
        }
    }
}
fn assert_values(actual: &serde_json::Value, expected: &serde_json::Value) {
    assert_eq!(actual, expected);
    for row in actual.as_array().unwrap() {
        assert_eq!(row.as_object().unwrap().len(), 2);
        assert!(row[GROUP].is_string());
        assert!(row[COUNT].as_u64().is_some());
    }
}

#[test]
fn utf8_integer_distinct_native_ordinary_unretained_complete_values_and_worker_grants() {
    let fixture = Fixture::new();
    let (chunks, oracle, rows) = corpus();
    let path = fixture.write("source.vortex", chunks);
    for parallelism in [1, 2, 4, 8, 12] {
        for offset in [0, 1, 20] {
            let request = query(&path, offset, 4);
            let reference = expected(&oracle, offset, 4);
            let report =
                runtime::execute_vortex_local_primitive_with_policy(&request, policy(parallelism))
                    .unwrap();
            let work = payload(&report);
            assert_values(&work["values"], &reference);
            assert_eq!(work["aggregate_workers_rows"], rows);
            assert_eq!(work["candidate_groups"], oracle.len());
            assert_eq!(report.rows_selected, Some(rows as u64));
            assert_eq!(
                report.rows_projected,
                Some(reference.as_array().unwrap().len() as u64)
            );
            assert!(work["aggregate_workers_submitted_chunks"].as_u64().unwrap() > 0);
            assert_eq!(work["aggregate_workers_provider_background_workers"], 0);
            // Each repetition creates a fresh ordinary source; this proves no
            // new retained API or cached answer, not one-open prepared reuse.
            for _ in 0..2 {
                let executed = unretained(&request, parallelism).execute().unwrap();
                assert!(executed.native_io_certificate.is_certified());
                assert!(
                    executed
                        .native_io_certificate
                        .source_pushdown_report
                        .proof_basis
                        .contains("aggregate_preparation_disposition=unretained_source")
                );
                assert_values(&payload(&executed.report)["values"], &reference);
            }
        }
    }
}

#[test]
fn utf8_integer_distinct_native_dictionary_domains_and_slices_keep_complete_values() {
    let fixture = Fixture::new();
    let arrays = vec![
        chunk(
            dictionary(&[2, 0, 1, 2], &["東京", "", "東京"]),
            integers(&[i64::MIN, i64::MIN, i64::MAX, 7]),
        ),
        chunk(
            dictionary(&[0, 1, 2, 0, 1], &["unused", "東京", ""]),
            integers(&[99, 7, i64::MAX, 99, i64::MIN]),
        )
        .slice(1..3)
        .unwrap(),
    ];
    let path = fixture.write("dictionary.vortex", arrays);
    let request = query(&path, 0, 4);
    let reference = serde_json::json!([{GROUP:"東京",COUNT:2},{GROUP:"",COUNT:1}]);
    let report = runtime::execute_vortex_local_primitive_with_policy(&request, policy(2)).unwrap();
    assert_values(&payload(&report)["values"], &reference);
    assert_eq!(payload(&report)["aggregate_workers_rows"], 6);
    let executed = unretained(&request, 2).execute().unwrap();
    assert!(executed.native_io_certificate.is_certified());
    assert_values(&payload(&executed.report)["values"], &reference);
}

#[test]
fn utf8_integer_distinct_native_unretained_generation_rejected_before_publication() {
    let fixture = Fixture::new();
    let (chunks, _, _) = corpus();
    let path = fixture.write("source.vortex", chunks.clone());
    let replacement = fixture.write("replacement.vortex", chunks);
    let operation = unretained(&query(&path, 0, 4), 2);
    fs::rename(replacement, &path).unwrap();
    assert!(operation.execute().is_err());
}

#[test]
#[allow(clippy::too_many_lines)]
fn utf8_integer_distinct_native_admission_pressure_and_late_generation_release_exactly() {
    let fixture = Fixture::new();
    let (chunks, oracle, rows) = corpus();
    let path = fixture.write("source.vortex", chunks.clone());
    let replacement = fixture.write("replacement.vortex", chunks);
    let request = query(&path, 0, 4);
    let uri = request.source_uri.as_ref().unwrap();
    let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 4).unwrap();
    let source = resident.prepare_file(&path).unwrap();
    let baseline = resident.memory().snapshot().reserved_bytes;
    // This uses the existing internal held-source execution seam; it does not
    // admit PreparedVortexAggregate or an owned UTF8 result through public APIs.
    for pressure in [false, true, false] {
        let denied_before = resident.memory().snapshot().denied_reservations;
        aggregate_count_workers::ADMISSION_TEST_PRESSURE.with(|current| current.set(pressure));
        let scan = source
            .with_native_execution(|file, session, runtime| {
                runtime::read_prepared_vortex_simple_aggregate_scan(
                    uri,
                    &request,
                    policy(4),
                    file,
                    session,
                    runtime,
                    Some(resident.memory()),
                    None,
                )
            })
            .unwrap();
        assert!(!aggregate_count_workers::ADMISSION_TEST_PRESSURE.with(std::cell::Cell::get));
        let work: serde_json::Value = serde_json::from_str(&scan.result_summary).unwrap();
        assert_values(&work["values"], &expected(&oracle, 0, 4));
        assert_eq!(scan.scan.pre_limit_result_row_count, rows);
        if pressure {
            assert!(resident.memory().snapshot().denied_reservations > denied_before);
            assert!(
                work["aggregate_provider_cpu_scope"]
                    .as_str()
                    .unwrap()
                    .contains("actual_aggregate_worker_admission_declined_before_scan")
            );
            assert!(work.get("aggregate_workers_submitted_chunks").is_none());
        } else {
            assert_eq!(work["aggregate_workers_rows"], rows);
            assert!(work["aggregate_workers_submitted_chunks"].as_u64().unwrap() > 0);
        }
        drop(scan);
        assert_eq!(resident.memory().snapshot().reserved_bytes, baseline);
    }
    let completed_before = resident.snapshot().completed_executions;
    let rejected = source.with_native_execution(|file, session, runtime| {
        let scan = runtime::read_prepared_vortex_simple_aggregate_scan(
            uri,
            &request,
            policy(4),
            file,
            session,
            runtime,
            Some(resident.memory()),
            None,
        )?;
        let work: serde_json::Value = serde_json::from_str(&scan.result_summary).unwrap();
        assert_values(&work["values"], &expected(&oracle, 0, 4));
        fs::rename(&replacement, &path).unwrap();
        Ok(scan)
    });
    assert!(rejected.is_err());
    assert_eq!(resident.snapshot().completed_executions, completed_before);
    assert_eq!(resident.memory().snapshot().reserved_bytes, baseline);
    drop(source);
    assert_eq!(resident.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn utf8_integer_distinct_native_empty_ordinary_and_unretained_values() {
    let fixture = Fixture::new();
    let path = fixture.write("empty.vortex", vec![chunk(strings(&[]), integers(&[]))]);
    let request = query(&path, 0, 4);
    let report = runtime::execute_vortex_local_primitive_with_policy(&request, policy(1)).unwrap();
    assert_values(&payload(&report)["values"], &serde_json::json!([]));
    assert_eq!(payload(&report)["candidate_groups"], 0);
    assert_eq!(report.rows_selected, Some(0));
    let executed = unretained(&request, 1).execute().unwrap();
    assert!(executed.native_io_certificate.is_certified());
    assert_values(&payload(&executed.report)["values"], &serde_json::json!([]));
}

#[test]
fn utf8_integer_distinct_native_nullable_schema_keeps_existing_ordinary_route() {
    let fixture = Fixture::new();
    let text = VarBinViewArray::from_iter_nullable_str([Some("x"), None, Some("x")]).into_array();
    let values = PrimitiveArray::from_option_iter([Some(1_i64), Some(2), None]).into_array();
    let path = fixture.write("nullable.vortex", vec![chunk(text, values)]);
    let request = query(&path, 0, 4);
    let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 2).unwrap();
    assert!(prepare_aggregate_in_session(&request, policy(2), &resident).is_err());
    let report = runtime::execute_vortex_local_primitive_with_policy(&request, policy(2)).unwrap();
    let work = payload(&report);
    assert!(
        work.get("aggregate_workers_partition_complete_pairs")
            .is_none()
    );
    let actual = work["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row[GROUP].as_str().map(str::to_owned),
                row[COUNT].as_u64().unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, BTreeMap::from([(None, 1), (Some("x".into()), 1)]));
    let executed = unretained(&request, 2).execute().unwrap();
    assert!(executed.native_io_certificate.is_certified());
    assert_eq!(payload(&executed.report)["values"], work["values"]);
    assert_eq!(resident.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn utf8_integer_distinct_native_explicit_spill_remains_outside_integer_spill_admission() {
    let fixture = Fixture::new();
    let path = fixture.write(
        "source.vortex",
        vec![chunk(strings(&["x"]), integers(&[1]))],
    );
    let workspace = fixture.0.join("spill");
    fs::create_dir(&workspace).unwrap();
    let mut request = query(&path, 0, 3);
    request.simple_aggregate = Some(
        request
            .simple_aggregate
            .take()
            .unwrap()
            .with_spill(VortexAggregateSpillPolicy::new(&workspace, 8 << 20, 4 << 20).unwrap()),
    );
    let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 1).unwrap();
    let source = resident.prepare_file(&path).unwrap();
    assert!(
        !runtime::exact_distinct_pairs::workers::request_schema_may_be_admitted(
            &request,
            source.dtype()
        )
    );
    assert!(
        prepare_aggregate_for_optional_reuse(&request, policy(1))
            .unwrap()
            .is_none()
    );
    assert!(runtime::execute_vortex_local_primitive_with_policy(&request, policy(1)).is_err());
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    drop(source);
    assert_eq!(resident.memory().snapshot().reserved_bytes, 0);
}
