//! Complete public-call and ownership evidence for count-selected mixed measures.
use super::*;
use crate::{
    VortexLocalPrimitiveRowExportFormat, local_primitives as runtime,
    resident_session::ResidentVortexSession,
};
use runtime::prepared_aggregate::prepare_aggregate_in_session;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        arrays::{PrimitiveArray, StructArray},
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

const ROWS: usize = 1_048_576;
const COLS: [&str; 5] = ["bucket", "total", "n", "mean", "unique"];
type Oracle = BTreeMap<i64, (i64, u64, u64, BTreeSet<u64>)>;

struct ClearHook;
impl Drop for ClearHook {
    fn drop(&mut self) {
        AFTER_COUNT_TEST_HOOK.with(|hook| drop(hook.borrow_mut().take()));
    }
}

struct Fixture {
    dir: PathBuf,
    path: PathBuf,
    oracle: Oracle,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "shardloom-winner-distinct-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("source.vortex");
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut file = fs::File::create_new(&path).unwrap();
        let mut oracle = Oracle::new();
        let mut chunks = Vec::new();
        for start in (0..ROWS).step_by(65_536) {
            let mut bucket = Vec::new();
            let mut metric = Vec::new();
            let mut width = Vec::new();
            let mut identity = Vec::new();
            for index in start..start + 65_536 {
                let key = i64::try_from(index % 16).unwrap() - 8;
                let measure = i64::try_from(index % 7).unwrap() - 3;
                let w = u64::try_from(index % 11).unwrap();
                let id = u64::MAX - u64::try_from((index / 16) % 32).unwrap();
                bucket.push(key);
                metric.push(measure);
                width.push(w);
                identity.push(id);
                let state = oracle.entry(key).or_default();
                state.0 += measure;
                state.1 += 1;
                state.2 += w;
                state.3.insert(id);
            }
            chunks.push(
                StructArray::new(
                    ["identity", "width", "bucket", "measure"].into(),
                    vec![
                        PrimitiveArray::new(identity, Validity::NonNullable).into_array(),
                        PrimitiveArray::new(width, Validity::NonNullable).into_array(),
                        PrimitiveArray::new(bucket, Validity::NonNullable).into_array(),
                        PrimitiveArray::new(metric, Validity::NonNullable).into_array(),
                    ],
                    65_536,
                    Validity::NonNullable,
                )
                .into_array(),
            );
        }
        let mut writer = session
            .write_options()
            .with_strategy(
                runtime::native_flat_layout::SequentialNativeFlatLayout::strategy(chunks.len()),
            )
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut file, chunks[0].dtype().clone());
        for chunk in chunks {
            writer.push(chunk).unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), ROWS as u64);
        Self { dir, path, oracle }
    }
    fn expected(&self, offset: usize, limit: usize) -> serde_json::Value {
        let mut groups = self.oracle.iter().collect::<Vec<_>>();
        groups.sort_by(|(lk, l), (rk, r)| r.1.cmp(&l.1).then_with(|| lk.cmp(rk)));
        serde_json::Value::Array(
            groups
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(key, state)| {
                    serde_json::json!({"bucket":key, "total":f64::from(i32::try_from(state.0).unwrap()), "n":state.1,
                "mean":f64::from(u32::try_from(state.2).unwrap()) / f64::from(u32::try_from(state.1).unwrap()), "unique":state.3.len()})
                })
                .collect(),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn query(path: &Path, offset: usize, limit: usize) -> VortexQueryPrimitiveRequest {
    let aggregate = super::request()
        .with_order_by(vec![
            VortexAggregateOrderExpr::new("n", true),
            VortexAggregateOrderExpr::new("bucket", false),
        ])
        .with_offset(offset);
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        aggregate,
    )
    .with_source_order_limit(limit)
}
fn policy() -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new(2).unwrap();
    policy.resource_envelope.memory_budget_bytes = 64 << 20;
    policy
}
fn payload(report: &runtime::VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
    assert!(!report.fallback_execution_allowed);
    assert!(!report.has_errors());
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
fn rendered(result: &crate::resident_session::OwnedVortexResultBatch) -> serde_json::Value {
    serde_json::from_str(
        result
            .to_bounded_json(&COLS.map(str::to_string), 64 * 1024)
            .unwrap()
            .value(),
    )
    .unwrap()
}

#[test]
fn winner_distinct_native_ordinary_prepared_and_owned_result_share_complete_values() {
    let fixture = Fixture::new();
    let request = query(&fixture.path, 1, 2);
    let expected = fixture.expected(1, 2);
    let ordinary = runtime::execute_vortex_local_primitive_with_policy(&request, policy()).unwrap();
    let work = payload(&ordinary);
    assert_eq!(work["values"], expected);
    assert_eq!(
        work["aggregate_winner_distinct"]["decision"],
        "complete_count_then_selected_measures"
    );
    assert_eq!(
        work["aggregate_winner_distinct"]["count_columns"],
        serde_json::json!(["bucket"])
    );
    assert_eq!(
        work["aggregate_winner_distinct"]["retained_rows"],
        3 * (ROWS / 16)
    );
    assert_eq!(ordinary.rows_selected, Some(ROWS as u64));
    let session = ResidentVortexSession::new(64 << 20, 2).unwrap();
    let memory = session.memory().clone();
    let prepared = prepare_aggregate_in_session(&request, policy(), &session).unwrap();
    for completed in 1..=2 {
        let executed = prepared.execute().unwrap();
        assert!(executed.native_io_certificate.is_certified());
        assert!(!executed.native_io_certificate.fallback_attempted);
        assert_eq!(executed.runtime.completed_executions, completed);
        assert_eq!(payload(&executed.report)["values"], expected);
    }
    let completed = prepared.execute_owned().unwrap();
    assert_eq!(rendered(&completed.result), expected);
    drop(prepared);
    fs::remove_file(&fixture.path).unwrap();
    let target = fixture.dir.join("result.vortex");
    let written = completed
        .write(&target, VortexLocalPrimitiveRowExportFormat::Vortex, false)
        .unwrap();
    assert_eq!(written.rows_written, 2);
    assert_eq!(session.snapshot().completed_executions, 3);
    let reader = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = reader.prepare_file(&target).unwrap();
    let arrays = source
        .prepare_projection(&COLS, 10, 4096)
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(rendered(&arrays), expected);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn winner_distinct_native_cost_decline_preserves_the_original_complete_aggregate() {
    let fixture = Fixture::new();
    let request = query(&fixture.path, 0, 16);
    let ordinary = runtime::execute_vortex_local_primitive_with_policy(&request, policy()).unwrap();
    let work = payload(&ordinary);
    assert_eq!(work["values"], fixture.expected(0, 16));
    let decision = &work["aggregate_winner_distinct"];
    assert_eq!(decision["decision"], "declined_high_sample_winner_share");
    assert_eq!(decision["count_rows"], SAMPLE_ROWS);
    assert_eq!(decision["measure_rows"], ROWS);
}

#[test]
fn winner_distinct_native_cancel_between_passes_refund_and_fresh_execution() {
    let _clear = ClearHook;
    let fixture = Fixture::new();
    let request = query(&fixture.path, 1, 2);
    let session = ResidentVortexSession::new(64 << 20, 2).unwrap();
    let memory = session.memory().clone();
    let prepared = prepare_aggregate_in_session(&request, policy(), &session).unwrap();
    let retained = memory.snapshot().reserved_bytes;
    let cancellation = CancellationToken::default();
    let at_handoff = cancellation.clone();
    AFTER_COUNT_TEST_HOOK
        .with(|hook| *hook.borrow_mut() = Some(Box::new(move || at_handoff.cancel())));
    let error = prepared
        .execute_cancellable(&cancellation)
        .err()
        .expect("cancellation must fail");
    assert!(error.to_string().contains("cancel"));
    assert!(AFTER_COUNT_TEST_HOOK.with(|hook| hook.borrow().is_none()));
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    assert_eq!(
        payload(
            &prepared
                .execute_cancellable(&CancellationToken::default())
                .unwrap()
                .report
        )["values"],
        fixture.expected(1, 2)
    );
    let before = session.snapshot().completed_executions;
    let snapshot = memory.snapshot();
    let pressure = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)
        .unwrap();
    assert!(prepared.execute_owned().is_err());
    assert_eq!(session.snapshot().completed_executions, before);
    drop(pressure);
    assert_eq!(
        rendered(&prepared.execute_owned().unwrap().result),
        fixture.expected(1, 2)
    );
    drop(prepared);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn winner_distinct_native_source_replacement_between_passes_cannot_publish() {
    let _clear = ClearHook;
    let fixture = Fixture::new();
    let replacement = fixture.dir.join("replacement.vortex");
    fs::copy(&fixture.path, &replacement).unwrap();
    let request = query(&fixture.path, 1, 2);
    let session = ResidentVortexSession::new(64 << 20, 2).unwrap();
    let memory = session.memory().clone();
    let prepared = prepare_aggregate_in_session(&request, policy(), &session).unwrap();
    let retained = memory.snapshot().reserved_bytes;
    let path = fixture.path.clone();
    AFTER_COUNT_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || fs::rename(replacement, path).unwrap()));
    });
    assert!(prepared.execute().is_err());
    assert!(AFTER_COUNT_TEST_HOOK.with(|hook| hook.borrow().is_none()));
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(prepared);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
