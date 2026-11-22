//! Native-file coverage for pair reduction and its held-source CPU handoff.

use super::{
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionReport,
    aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault},
    bounded_local_vortex_worker_count,
    native_flat_layout::SequentialNativeFlatLayout,
    prepared_aggregate::{ExecutedVortexAggregate, prepare_aggregate_in_session},
};
use crate::{
    VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest, resident_session::ResidentVortexSession,
};
use shardloom_core::{ColumnRef, DatasetUri};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::FieldNames,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

const CHUNKS: usize = 32;
const CHUNK_ROWS: usize = 32_768;
const SOURCE_ROWS: u64 = 1_048_576;
const MEMORY_BYTES: u64 = 256 << 20;
static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    directory: PathBuf,
    near_unique: bool,
    chunks: usize,
    chunk_rows: usize,
}

impl Fixture {
    fn new(near_unique: bool) -> Self {
        Self::with_geometry(near_unique, CHUNKS, CHUNK_ROWS)
    }

    fn with_geometry(near_unique: bool, chunks: usize, chunk_rows: usize) -> Self {
        assert!(chunks >= 2 && chunk_rows >= 4 && chunk_rows.is_multiple_of(2));
        let directory = std::env::temp_dir().join(format!(
            "shardloom-pair-partition-native-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        let fixture = Self {
            directory,
            near_unique,
            chunks,
            chunk_rows,
        };
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let first = fixture.chunk(0);
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(fixture.path())
            .unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(SequentialNativeFlatLayout::strategy(chunks))
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut output, first.dtype().clone());
        writer.push(first).unwrap();
        for index in 1..chunks {
            writer.push(fixture.chunk(index)).unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), fixture.source_rows());
        drop(output);
        fixture
    }

    fn chunk(&self, index: usize) -> ArrayRef {
        let mut first = Vec::with_capacity(self.chunk_rows);
        let mut second = Vec::with_capacity(self.chunk_rows);
        for row in 0..self.chunk_rows {
            let pair = if self.near_unique {
                if row == self.chunk_rows - 1 {
                    // One complete pair spans every chunk, while each first
                    // chunk's sample remains genuinely near-unique.
                    (-7, -3)
                } else {
                    (
                        i64::try_from(index * self.chunk_rows + row).unwrap(),
                        i32::try_from(row % 17).unwrap(),
                    )
                }
            } else if row % 2 == 0 {
                (-2, 3)
            } else {
                (7, -4)
            };
            first.push(pair.0);
            second.push(pair.1);
        }
        StructArray::try_new(
            FieldNames::from(["entity_key", "origin_key", "weight", "span"]),
            vec![
                PrimitiveArray::new(first, Validity::NonNullable).into_array(),
                PrimitiveArray::new(second, Validity::NonNullable).into_array(),
                PrimitiveArray::new(
                    vec![i16::try_from(index + 1).unwrap(); self.chunk_rows],
                    Validity::NonNullable,
                )
                .into_array(),
                PrimitiveArray::new(
                    vec![10 + i32::try_from(index).unwrap(); self.chunk_rows],
                    Validity::NonNullable,
                )
                .into_array(),
            ],
            self.chunk_rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    }

    fn path(&self) -> PathBuf {
        self.directory.join("source.vortex")
    }

    fn source_rows(&self) -> u64 {
        u64::try_from(self.chunks.checked_mul(self.chunk_rows).unwrap()).unwrap()
    }

    fn request(&self) -> VortexQueryPrimitiveRequest {
        let measure = |function, column: Option<&str>, alias: &str| {
            VortexSimpleAggregateMeasure::new(
                function,
                column.map(|name| ColumnRef::new(name).unwrap()),
                alias.to_owned(),
            )
        };
        VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.path().display().to_string()).unwrap(),
            VortexSimpleAggregateRequest::grouped(
                vec![
                    ColumnRef::new("entity_key").unwrap(),
                    ColumnRef::new("origin_key").unwrap(),
                ],
                vec![
                    measure("count", None, "frequency"),
                    measure("sum", Some("weight"), "weight_sum"),
                    measure("avg", Some("span"), "span_avg"),
                ],
            )
            .with_order_by(vec![VortexAggregateOrderExpr::new("frequency", true)]),
        )
        .with_source_order_limit(3)
    }

    fn expected(&self) -> serde_json::Value {
        // Fixture bounds make every integer and half-integer exactly representable.
        let chunks = u32::try_from(self.chunks).unwrap();
        let weights = chunks.checked_mul(chunks + 1).unwrap() / 2;
        let average = 10.0 + f64::from(chunks - 1) / 2.0;
        if self.near_unique {
            serde_json::json!([
                {"entity_key":-7,"origin_key":-3,"frequency":chunks,"weight_sum":f64::from(weights),"span_avg":average},
                {"entity_key":0,"origin_key":0,"frequency":1,"weight_sum":1.0,"span_avg":10.0},
                {"entity_key":1,"origin_key":1,"frequency":1,"weight_sum":1.0,"span_avg":10.0},
            ])
        } else {
            let rows_per_key = u32::try_from(self.chunk_rows / 2).unwrap();
            let weight_sum = f64::from(rows_per_key.checked_mul(weights).unwrap());
            let frequency = self.source_rows() / 2;
            serde_json::json!([
                {"entity_key":-2,"origin_key":3,"frequency":frequency,"weight_sum":weight_sum,"span_avg":average},
                {"entity_key":7,"origin_key":-4,"frequency":frequency,"weight_sum":weight_sum,"span_avg":average},
            ])
        }
    }
}

#[path = "local_primitive_pair_wait_attribution_tests.rs"]
mod wait_attribution;

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn policy(parallelism: usize) -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 4).unwrap();
    policy.resource_envelope.memory_budget_bytes = MEMORY_BYTES;
    policy
}

#[test]
fn native_pair_partition_worker_observes_operation_cancel_and_fresh_execution_recovers() {
    use super::aggregate_chunk_jobs::WORKER_START_TEST_HOOK;
    use shardloom_exec::compute_pool::CancellationToken;
    use std::sync::{Arc, atomic::AtomicBool};

    struct ClearHook;
    impl Drop for ClearHook {
        fn drop(&mut self) {
            WORKER_START_TEST_HOOK.with(|hook| drop(hook.borrow_mut().take()));
        }
    }
    let _clear = ClearHook;
    let fixture = Fixture::new(true);
    for parallelism in [1, 3] {
        let session =
            ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, parallelism).unwrap();
        let memory = session.memory().clone();
        let prepared =
            prepare_aggregate_in_session(&fixture.request(), policy(parallelism), &session)
                .unwrap();
        let baseline = memory.snapshot().reserved_bytes;
        let parent = CancellationToken::default();
        let operation = parent.clone();
        let observed = Arc::new(AtomicBool::new(false));
        let worker_observed = Arc::clone(&observed);
        WORKER_START_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |worker| {
                operation.cancel();
                worker_observed.store(worker.check_cancelled().is_err(), Ordering::Release);
            }));
        });
        let error = prepared.execute_cancellable(&parent).err().unwrap();
        assert!(error.to_string().contains("cancel"), "{error}");
        assert!(observed.load(Ordering::Acquire));
        assert!(WORKER_START_TEST_HOOK.with(|hook| hook.borrow().is_none()));
        assert_eq!(session.snapshot().completed_executions, 0);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        let fresh = CancellationToken::default();
        let completed = prepared.execute_cancellable(&fresh).unwrap();
        assert!(!fresh.is_cancelled());
        let (_, values) = completed
            .report
            .result_summary
            .as_deref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap();
        let values: serde_json::Value = serde_json::from_str(values).unwrap();
        assert_eq!(values["values"], fixture.expected());
        drop((completed, prepared, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn payload(report: &VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
    serde_json::from_str(
        report
            .result_summary
            .as_deref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap()
}

fn assert_complete(execution: &ExecutedVortexAggregate, fixture: &Fixture, execution_number: u64) {
    assert!(!execution.report.has_errors());
    assert!(!execution.report.fallback_execution_allowed);
    assert!(!execution.report.arrow_converted);
    assert!(!execution.report.spill_io_performed);
    assert!(execution.native_io_certificate.is_certified());
    assert!(!execution.native_io_certificate.fallback_attempted);
    assert_eq!(execution.report.rows_selected, Some(SOURCE_ROWS));
    assert_eq!(execution.runtime.prepared_source_opens, 1);
    assert_eq!(execution.runtime.completed_executions, execution_number);
    assert_eq!(
        execution.runtime.provider_background_workers,
        bounded_local_vortex_worker_count(execution.report.resource_envelope.max_parallelism)
    );
    assert!(
        execution.native_io_certificate.source_pushdown_report.proof_basis.contains(
            "resident_source_generation_validation=before_and_after_every_execution_including_pruned_result"
        )
    );
    let result = payload(&execution.report);
    assert_eq!(result["values"], fixture.expected());
    assert_eq!(result["numeric_pair_late_measure_second_pass"], true);
    assert_eq!(
        result["aggregate_workers_pair_retired_before_provider_resume"],
        true
    );
    assert_eq!(
        result["aggregate_provider_background_workers"],
        bounded_local_vortex_worker_count(execution.report.resource_envelope.max_parallelism)
    );
    assert!(
        result["aggregate_provider_cpu_scope"].as_str().unwrap().contains(
            "numeric_pair_workers_retired_before_provider_resume;temporary_provider_drivers;no_concurrent_aggregate_worker_pool;no_source_reopen_or_replay"
        )
    );
    assert!(
        result
            .get("aggregate_workers_partition_source_replays")
            .is_none()
    );
    if fixture.near_unique {
        assert_eq!(
            result["aggregate_workers_family"],
            "complete_numeric_pair_partition_sort_reduce"
        );
        assert_eq!(result["aggregate_workers_pair_rows"], SOURCE_ROWS);
        assert_eq!(result["aggregate_workers_pair_groups"], SOURCE_ROWS - 31);
        assert_eq!(result["aggregate_workers_pair_duplicate_keys"], 1);
        assert!(
            result["aggregate_workers_pair_source_chunks"]
                .as_u64()
                .unwrap()
                > 1
        );
        assert_eq!(
            result["aggregate_workers_pair_submitted_partition_tasks"],
            64
        );
        assert_eq!(
            result["aggregate_workers_pair_submitted_partition_tasks"],
            result["aggregate_workers_pair_joined_partition_tasks"]
        );
    } else {
        assert_eq!(
            result["aggregate_workers_family"],
            "numeric_pair_partition_sample_declined"
        );
        assert_eq!(result["aggregate_workers_pair_source_chunks"], 0);
        assert_eq!(
            result["aggregate_workers_pair_submitted_partition_tasks"],
            0
        );
        assert_eq!(result["aggregate_workers_pair_rows"], 0);
        assert_eq!(result["candidate_groups"], 2);
    }
}

#[test]
fn native_pair_partitions_resume_provider_workers_and_reexecute_on_one_held_source() {
    let fixture = Fixture::new(true);
    let request = fixture.request();
    for parallelism in [1, 4] {
        let session =
            ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, parallelism).unwrap();
        let prepared =
            prepare_aggregate_in_session(&request, policy(parallelism), &session).unwrap();
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        for execution in 1..=2 {
            assert_complete(&prepared.execute().unwrap(), &fixture, execution);
        }
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn native_pair_sample_decline_resumes_the_same_chunk_and_source_without_replay() {
    let fixture = Fixture::new(false);
    let request = fixture.request();
    for parallelism in [1, 4] {
        let session =
            ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, parallelism).unwrap();
        let prepared =
            prepare_aggregate_in_session(&request, policy(parallelism), &session).unwrap();
        for execution in 1..=2 {
            assert_complete(&prepared.execute().unwrap(), &fixture, execution);
        }
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

struct ScanFaultGuard;
impl Drop for ScanFaultGuard {
    fn drop(&mut self) {
        SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(None));
    }
}

#[test]
fn native_pair_committed_source_denial_and_corruption_fail_without_replay_or_retained_leases() {
    let fixture = Fixture::new(true);
    let request = fixture.request();
    let _reset = ScanFaultGuard;
    for parallelism in [1, 4] {
        for fault in [
            SourceScanTestFault::OwnedDenial,
            SourceScanTestFault::CorruptionWithConcurrentDenial,
        ] {
            let session =
                ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, parallelism).unwrap();
            let prepared =
                prepare_aggregate_in_session(&request, policy(parallelism), &session).unwrap();
            SOURCE_SCAN_TEST_FAULT.with(|current| current.set(Some(fault)));
            let error = prepared
                .execute()
                .err()
                .expect("committed source faults cannot return partial values or replay serially");
            assert!(
                SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none(),
                "fault must fire after input commitment"
            );
            match fault {
                SourceScanTestFault::OwnedDenial => assert!(
                    error.to_string().contains("memory reservation denied"),
                    "{error}"
                ),
                SourceScanTestFault::CorruptionWithConcurrentDenial => assert!(
                    error.to_string().contains("injected source corruption"),
                    "{error}"
                ),
            }
            assert_eq!(prepared.snapshot().prepared_source_opens, 1);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert!(session.snapshot().memory.denied_reservations > 0);
            // The same held operation remains usable, with fresh aggregate
            // state and no consumed fault reclassified into a hidden replay.
            assert_complete(&prepared.execute().unwrap(), &fixture, 1);
            drop(prepared);
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }
}
