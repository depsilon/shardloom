//! Bounded diagnostics through the production prepared native aggregate path.

use super::{Fixture, MEMORY_BYTES, payload, policy};
use crate::{
    local_primitives::{
        aggregate_chunk_jobs::observation::Session,
        prepared_aggregate::prepare_aggregate_in_session,
    },
    resident_session::ResidentVortexSession,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shardloom_exec::compute_pool::CancellationToken;
use std::{fmt::Write as _, io::Read as _};

fn source_hash(fixture: &Fixture) -> String {
    let mut source = std::fs::File::open(fixture.path()).unwrap();
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8 * 1024];
    loop {
        let bytes = source.read(&mut buffer).unwrap();
        if bytes == 0 {
            break;
        }
        hasher.update(&buffer[..bytes]);
    }
    hasher
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            write!(text, "{byte:02x}").unwrap();
            text
        })
}

fn run(fixture: &Fixture, parallelism: usize, observe_waits: bool) -> Value {
    let session = ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, parallelism).unwrap();
    let memory = session.memory().clone();
    let request = fixture.request();
    let prepared = prepare_aggregate_in_session(&request, policy(parallelism), &session).unwrap();
    let cancellation = CancellationToken::default();
    let observation = Session::start(observe_waits);
    let result = prepared.collect_jsonl(&cancellation).unwrap();
    assert!(!cancellation.is_cancelled());
    assert!(!result.execution.report.has_errors());
    assert!(!result.execution.report.fallback_execution_allowed);
    assert!(!result.execution.report.arrow_converted);
    assert!(!result.execution.report.spill_io_performed);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(!result.execution.native_io_certificate.fallback_attempted);
    assert_eq!(
        result.execution.report.rows_selected,
        Some(fixture.source_rows())
    );
    assert_eq!(result.execution.runtime.prepared_source_opens, 1);
    assert_eq!(result.execution.runtime.completed_executions, 1);
    let rows: Vec<Value> = result
        .result_jsonl
        .value()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(json!(rows), fixture.expected());
    let schema: Value = serde_json::from_str(result.result_schema_json.value()).unwrap();
    assert_eq!(
        schema,
        json!({"Struct": [{
        "names": ["entity_key", "origin_key", "frequency", "weight_sum", "span_avg"],
        "dtypes": [
            {"Primitive": ["i64", false]}, {"Primitive": ["i32", false]},
            {"Primitive": ["u64", false]}, {"Primitive": ["f64", true]},
            {"Primitive": ["f64", true]},
        ],
    }, false]})
    );
    let summary = payload(&result.execution.report);
    let mut output = json!({
        "chunks": fixture.chunks,
        "chunk_rows": fixture.chunk_rows,
        "source_rows": fixture.source_rows(),
        "near_unique": fixture.near_unique,
        "parallelism": parallelism,
        "memory_grant_bytes": MEMORY_BYTES,
        "observe_waits": observe_waits,
        "request_debug": format!("{request:?}"),
        "engine_execution_seconds": result.timing.execution.as_secs_f64(),
        "engine_execution_nanos": u64::try_from(result.timing.execution.as_nanos()).unwrap(),
        "result_serialization_nanos": u64::try_from(result.timing.result_serialization.as_nanos()).unwrap(),
        "rows": rows,
        "schema": schema,
        "summary": summary,
        "native_io_certificate_debug": format!("{:?}", result.execution.native_io_certificate),
        "runtime_debug": format!("{:?}", result.execution.runtime),
        "fallback_attempted": result.execution.native_io_certificate.fallback_attempted,
    });
    drop((result, prepared, session));
    let owners = observation.finish();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    if fixture.near_unique && fixture.chunks == super::CHUNKS {
        assert_eq!(
            summary["aggregate_workers_family"],
            "complete_numeric_pair_partition_sort_reduce"
        );
        assert!(owners.iter().any(|record| record.submitted > 0));
        if parallelism > 1 {
            assert!(
                owners
                    .iter()
                    .any(|record| { record.pool.is_some_and(|pool| pool.completed_jobs > 0) })
            );
        }
    }
    output["owners"] = json!(
        owners
            .iter()
            .map(|record| record.completed_json(parallelism, observe_waits))
            .collect::<Vec<_>>()
    );
    output["final_reserved_bytes"] = json!(memory.snapshot().reserved_bytes);
    output
}

#[test]
fn native_pair_wait_capture_preserves_complete_tiny_results_and_refunds() {
    for near_unique in [false, true] {
        let fixture = Fixture::with_geometry(near_unique, 2, 128);
        let hash = source_hash(&fixture);
        for parallelism in [1, 3] {
            for observe_waits in [false, true] {
                let output = run(&fixture, parallelism, observe_waits);
                assert!(output["owners"].is_array());
            }
        }
        assert_eq!(source_hash(&fixture), hash);
    }
    // Completion and unwinding both clear the thread-local observation owner.
    drop(Session::start(false));
    assert!(Session::start(true).finish().is_empty());
    assert!(
        std::panic::catch_unwind(|| {
            let _session = Session::start(false);
            panic!("injected observation-owner unwind");
        })
        .is_err()
    );
    assert!(Session::start(true).finish().is_empty());
}

#[test]
#[ignore = "isolated release-only bounded worker attribution; no performance assertion"]
fn native_pair_worker_wait_attribution() {
    assert!(
        !std::hint::black_box(cfg!(debug_assertions)),
        "use the release test binary"
    );
    // Construct and hash all inputs before the first engine call.
    let fixtures: Vec<_> = [(2, 128), (4, 8_192), (32, 32_768)]
        .into_iter()
        .flat_map(|(chunks, rows)| {
            [false, true].map(|unique| Fixture::with_geometry(unique, chunks, rows))
        })
        .collect();
    let hashes: Vec<_> = fixtures.iter().map(source_hash).collect();
    let mut case = 0;
    for (fixture, hash) in fixtures.iter().zip(&hashes) {
        for parallelism in [1, 2, 4] {
            let order = if case % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for observe_waits in order {
                let mut output = run(fixture, parallelism, observe_waits);
                output["case"] = json!(case);
                output["source_sha256"] = json!(hash);
                println!("PAIR_WAIT_OBSERVATION {output}");
            }
            case += 1;
        }
    }
    assert_eq!(case, 18);
    for (fixture, hash) in fixtures.iter().zip(&hashes) {
        assert_eq!(&source_hash(fixture), hash);
    }
    println!("PAIR_WAIT_SOURCES_UNCHANGED true");
}
