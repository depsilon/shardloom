//! Actual UTF8 worker results keep ownership through failures and native sinks.

use super::{Fixture, MEMORY_BYTES};
use crate::{
    VortexLocalPrimitiveExecutionPolicy as Policy, VortexLocalPrimitiveRowExportFormat as Format,
    local_primitives::{
        aggregate_chunk_jobs::observation::{Record, Session},
        logical_field_from_native_array, output_fanout,
        prepared_aggregate::prepare_aggregate_in_session,
    },
    resident_session::{OwnedVortexResultBatch, ResidentVortexSession},
};
use serde_json::{Value, json};
use shardloom_core::ShardLoomError;
use shardloom_exec::compute_pool::CancellationToken;
use std::panic::{AssertUnwindSafe, catch_unwind};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, VortexSessionExecute as _,
        dtype::{DType, Nullability, PType},
        scalar::{PValue, ScalarValue},
    },
    session::VortexSession,
};

fn policy() -> Policy {
    let mut policy = Policy::new_with_memory_gb(4, 1).unwrap();
    policy.resource_envelope.memory_budget_bytes = MEMORY_BYTES;
    policy
}

fn dtype() -> DType {
    DType::struct_(
        [
            ("group_key", DType::Utf8(Nullability::NonNullable)),
            (
                "frequency",
                DType::Primitive(PType::U64, Nullability::NonNullable),
            ),
        ],
        Nullability::NonNullable,
    )
}

fn array_rows(array: &ArrayRef) -> Vec<Value> {
    assert_eq!(array.dtype(), &dtype());
    let keys = logical_field_from_native_array(array, "group_key").unwrap();
    let counts = logical_field_from_native_array(array, "frequency").unwrap();
    let mut context = VortexSession::default().create_execution_ctx();
    (0..array.len())
        .map(|row| {
            let key = keys.execute_scalar(row, &mut context).unwrap();
            let Some(ScalarValue::Utf8(key)) = key.value() else {
                panic!("nonnullable native UTF8 key required");
            };
            let count = counts.execute_scalar(row, &mut context).unwrap();
            let Some(ScalarValue::Primitive(PValue::U64(count))) = count.value() else {
                panic!("exact native u64 count required");
            };
            json!({"group_key": key.as_str(), "frequency": count})
        })
        .collect()
}

fn assert_complete(result: &OwnedVortexResultBatch, expected: &Value) {
    assert_eq!(result.dtype(), &dtype());
    assert_eq!(
        result.row_count(),
        expected.as_array().unwrap().len() as u64
    );
    let rows = result
        .arrays()
        .iter()
        .flat_map(array_rows)
        .collect::<Vec<_>>();
    assert_eq!(&json!(rows), expected);
}

fn assert_workers_drained(records: &[Record]) {
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.submitted, 32 + 64);
    assert_eq!(record.joined, record.submitted);
    assert_eq!(record.pending_before_drop, 0);
    assert_eq!(record.outstanding_after_drain, 0);
    assert!(record.peak_outstanding <= record.window_limit);
    let pool = record.pool.expect("real native worker pool");
    assert!(pool.workers_created > 0 && pool.workers_created < 4);
    assert!(pool.queue_closed);
    assert_eq!(pool.queued_jobs, 0);
    assert_eq!(pool.active_workers, 0);
    assert_eq!(pool.queued_bytes, 0);
    assert_eq!(pool.waiting_producers, 0);
    assert_eq!(pool.completed_jobs, record.submitted);
    assert!(pool.waits.is_none());
}

#[test]
fn native_string_worker_result_slice_keeps_credit_after_source_and_result_close() {
    let fixture = Fixture::new(32, 16_384, 98_317);
    let expected = fixture.expected();
    let session = ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, 4).unwrap();
    let memory = session.memory().clone();
    let prepared = prepare_aggregate_in_session(&fixture.request(), policy(), &session).unwrap();
    let observation = Session::start(false);
    let completed = prepared
        .execute_native_result(&CancellationToken::default())
        .unwrap();
    assert_complete(&completed.result, &expected);
    assert!(completed.execution.native_io_certificate.is_certified());
    assert!(!completed.execution.native_io_certificate.fallback_attempted);
    assert_workers_drained(&observation.finish());
    assert_eq!(completed.execution.runtime.completed_executions, 1);
    let parent = completed.result.arrays()[0].clone();
    let keys = logical_field_from_native_array(&parent, "group_key").unwrap();
    let retained = keys.slice(1..4).unwrap();
    drop((parent, keys, completed, prepared, session));
    std::fs::remove_file(fixture.path()).unwrap();
    assert!(memory.snapshot().reserved_bytes > 0);
    let mut context = VortexSession::default().create_execution_ctx();
    for row in 0..retained.len() {
        let value = retained.execute_scalar(row, &mut context).unwrap();
        let Some(ScalarValue::Utf8(value)) = value.value() else {
            panic!("retained native UTF8 slice required");
        };
        assert_eq!(
            value.as_str(),
            expected[row + 1]["group_key"].as_str().unwrap()
        );
    }
    drop((retained, context));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(memory.snapshot().peak_reserved_bytes <= MEMORY_BYTES);
}

#[test]
fn native_string_worker_failed_consumers_keep_retained_credit_and_allow_fresh_retry() {
    let fixture = Fixture::new(32, 16_384, 98_317);
    let expected = fixture.expected();
    for fault in ["cancel", "consumer", "panic", "pressure"] {
        let mut session = ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, 4).unwrap();
        let mut memory = session.memory().clone();
        let mut prepared =
            prepare_aggregate_in_session(&fixture.request(), policy(), &session).unwrap();
        let baseline = session.snapshot().reserved_without_source_segment_cache();
        let token = CancellationToken::default();
        let observation = Session::start(false);
        let mut retained = None;
        let mut calls = 0;
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            prepared.for_each_batch(&token, |array, context| {
                calls += 1;
                assert_eq!(json!(array_rows(&array)), expected);
                retained = Some(array);
                match fault {
                    "cancel" => {
                        token.cancel();
                        Ok(())
                    }
                    "consumer" => Err(ShardLoomError::InvalidOperation(
                        "string consumer sentinel".into(),
                    )),
                    "panic" => panic!("string consumer unwind sentinel"),
                    "pressure" => context.memory().reserve(MEMORY_BYTES).map(|_| ()),
                    _ => unreachable!(),
                }
            })
        }));
        assert_eq!(calls, 1);
        if fault == "panic" {
            assert!(outcome.is_err());
        } else {
            let error = outcome
                .unwrap()
                .err()
                .expect("failed delivery must not certify completion");
            let expected_error = match fault {
                "cancel" => "cancel",
                "consumer" => "string consumer sentinel",
                _ => "reservation denied",
            };
            assert!(error.to_string().contains(expected_error), "{error}");
        }
        assert_workers_drained(&observation.finish());
        assert_eq!(session.snapshot().completed_executions, 0);
        assert!(session.snapshot().reserved_without_source_segment_cache() > baseline);
        assert_eq!(json!(array_rows(retained.as_ref().unwrap())), expected);
        drop(retained);
        assert_eq!(
            session.snapshot().reserved_without_source_segment_cache(),
            baseline
        );
        if fault == "panic" {
            // Unwinding poisons exclusive admission by contract. Keep its
            // cleanup proof separate from ordinary error/token retry.
            let error = prepared
                .execute_native_result(&CancellationToken::default())
                .err()
                .expect("unwound exclusive admission must remain poisoned");
            assert!(error.to_string().contains("admission poisoned"));
            drop((prepared, session));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            session = ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, 4).unwrap();
            memory = session.memory().clone();
            prepared =
                prepare_aggregate_in_session(&fixture.request(), policy(), &session).unwrap();
        }
        let completed = prepared
            .execute_native_result(&CancellationToken::default())
            .unwrap();
        assert_complete(&completed.result, &expected);
        assert!(completed.execution.native_io_certificate.is_certified());
        assert!(!completed.execution.native_io_certificate.fallback_attempted);
        assert_eq!(session.snapshot().prepared_source_opens, 1);
        assert_eq!(session.snapshot().completed_executions, 1);
        drop((completed, prepared, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(memory.snapshot().peak_reserved_bytes <= MEMORY_BYTES);
    }
}

#[test]
fn native_string_worker_fanout_computes_once_and_reopens_complete_native_output() {
    let fixture = Fixture::new(32, 16_384, 98_317);
    let expected = fixture.expected();
    let hash = fixture.source_hash();
    let session = ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, 4).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(fixture.path()).unwrap();
    let targets = [
        (fixture.directory.join("result.vortex"), Format::Vortex),
        (fixture.directory.join("result.jsonl"), Format::Jsonl),
    ];
    let observation = Session::start(false);
    let written = output_fanout::write_primitive_with_source(
        &fixture.request(),
        &targets,
        false,
        policy(),
        &source,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_workers_drained(&observation.finish());
    let replay = written.replay.as_ref().unwrap();
    assert_eq!(replay.rows_detached, 10);
    assert_eq!(replay.replays, 2);
    assert!(!replay.spilled);
    for report in &written.outputs {
        assert_eq!(report.rows_written, 10);
        assert!(!report.evidence.side_effects.fallback_attempted);
    }
    drop((written, source, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(memory.snapshot().peak_reserved_bytes <= MEMORY_BYTES);
    assert_eq!(fixture.source_hash(), hash);
    let text = std::fs::read_to_string(&targets[1].0).unwrap();
    let rows = text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(json!(rows), expected);
    let reader = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = reader.prepare_file(&targets[0].0).unwrap();
    assert_eq!(source.dtype(), &dtype());
    let reopened = source
        .prepare_projection(&["group_key", "frequency"], 10, 64 << 10)
        .unwrap()
        .execute()
        .unwrap();
    assert_complete(&reopened, &expected);
    drop((reopened, source));
    assert_eq!(reader.snapshot().reserved_without_source_segment_cache(), 0);
}
