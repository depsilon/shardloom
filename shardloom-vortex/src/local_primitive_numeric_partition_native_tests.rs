//! Complete weighted integer reduction on one held native source, including
//! ordinary source faults after real partition commitment.

use super::{
    VortexLocalPrimitiveExecutionPolicy,
    aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault},
    native_flat_layout::SequentialNativeFlatLayout,
    prepared_aggregate::{ExecutedVortexAggregate, prepare_aggregate_in_session},
};
use crate::{
    VortexAggregateExpression, VortexAggregateOrderExpr, VortexQueryPrimitiveRequest,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    resident_session::ResidentVortexSession,
};
use shardloom_core::{ColumnRef, DatasetUri};
use std::{collections::BTreeMap, path::PathBuf};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
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
const ROWS: usize = 8192;
const BUDGET: u64 = 64 << 20;

struct Fixture {
    path: PathBuf,
    expected: serde_json::Value,
    groups: usize,
}

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-numeric-partition-{}-{}.vortex",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut counts = BTreeMap::<i32, u64>::new();
        let mut arrays = Vec::new();
        for index in 0..CHUNKS {
            let values = (0..ROWS)
                .map(|row| {
                    if (row + index) % 5 == 0 {
                        9000
                    } else {
                        i32::try_from(row % 4096).unwrap() - 2048
                    }
                })
                .collect::<Vec<_>>();
            for &key in &values {
                *counts.entry(key).or_default() += 1;
            }
            arrays.push(
                StructArray::try_new(
                    FieldNames::from(["parcel_code"]),
                    vec![PrimitiveArray::new(values, Validity::NonNullable).into_array()],
                    ROWS,
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array(),
            );
        }
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(SequentialNativeFlatLayout::strategy(CHUNKS))
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut output, arrays[0].dtype().clone());
        for array in arrays {
            writer.push(array).unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), (CHUNKS * ROWS) as u64);
        drop(output);
        let groups = counts.len();
        let mut ranked = counts.into_iter().collect::<Vec<_>>();
        ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let expected = serde_json::Value::Array(
            ranked
                .into_iter()
                .skip(1)
                .take(10)
                .map(|(key, count)| {
                    serde_json::json!({"parcel_code":key,"prior_one":i64::from(key)-1,
                "prior_two":i64::from(key)-2,"prior_three":i64::from(key)-3,"frequency":count})
                })
                .collect(),
        );
        Self {
            path,
            expected,
            groups,
        }
    }

    fn request(&self) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.path.display().to_string()).unwrap(),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("parcel_code").unwrap()],
                vec![VortexSimpleAggregateMeasure::new(
                    "count",
                    None,
                    "frequency".into(),
                )],
            )
            .with_group_expressions(
                [("prior_one", -1), ("prior_two", -2), ("prior_three", -3)]
                    .into_iter()
                    .map(|(name, offset)| {
                        VortexAggregateExpression::new(
                            name.to_owned(),
                            ColumnRef::new("parcel_code").unwrap(),
                            "add_offset",
                        )
                        .with_argument_offset(offset)
                    })
                    .collect(),
            )
            .with_order_by(vec![VortexAggregateOrderExpr::new("frequency", true)])
            .with_offset(1),
        )
        .with_source_order_limit(10)
    }

    fn prepared_request(&self) -> VortexQueryPrimitiveRequest {
        self.request()
    }

    fn verify(&self, execution: &ExecutedVortexAggregate, completed: u64) {
        assert!(!execution.report.has_errors());
        assert!(!execution.report.fallback_execution_allowed);
        assert!(!execution.report.arrow_converted);
        assert!(!execution.report.spill_io_performed);
        assert!(execution.native_io_certificate.is_certified());
        assert!(!execution.native_io_certificate.fallback_attempted);
        assert_eq!(execution.runtime.prepared_source_opens, 1);
        assert_eq!(execution.runtime.completed_executions, completed);
        assert_eq!(execution.report.rows_selected, Some((CHUNKS * ROWS) as u64));
        let summary: serde_json::Value = serde_json::from_str(
            execution
                .report
                .result_summary
                .as_deref()
                .unwrap()
                .rsplit_once(" values=")
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(summary["values"], self.expected);
        assert_eq!(summary["candidate_groups"], self.groups);
        assert_eq!(
            summary["group_output_strategy"],
            "complete_weighted_integer_partition_topk"
        );
        assert_eq!(
            summary["aggregate_workers_integer_partition_rows"],
            CHUNKS * ROWS
        );
        assert_eq!(
            summary["aggregate_workers_integer_partition_selection_jobs"],
            64
        );
        assert_eq!(
            summary["aggregate_workers_submitted_chunks"],
            summary["aggregate_workers_completed_chunks"]
        );
        assert!(
            summary["aggregate_workers_submitted_chunks"]
                .as_u64()
                .unwrap()
                > 8
        );
        assert_eq!(summary["aggregate_workers_outstanding_chunks"], 0);
        assert!(
            summary
                .get("aggregate_workers_partition_source_replays")
                .is_none()
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).unwrap();
    }
}

fn policy(parallelism: usize) -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
    policy.resource_envelope.memory_budget_bytes = BUDGET;
    policy
}

#[test]
fn native_numeric_partitions_reexecute_exact_topk_on_one_source() {
    let fixture = Fixture::new();
    for parallelism in [1, 4] {
        let session = ResidentVortexSession::for_external_cpu_pool(BUDGET, parallelism).unwrap();
        let memory = session.memory().clone();
        let prepared = prepare_aggregate_in_session(
            &fixture.prepared_request(),
            policy(parallelism),
            &session,
        )
        .unwrap();
        for completed in 1..=2 {
            fixture.verify(&prepared.execute().unwrap(), completed);
        }
        drop(prepared);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

struct FaultGuard;

#[test]
fn native_numeric_partitions_preserve_derived_values_through_ordinary_execution() {
    let fixture = Fixture::new();
    for parallelism in [1, 4] {
        let report = super::execute_vortex_local_primitive_with_policy(
            &fixture.request(),
            policy(parallelism),
        )
        .unwrap();
        assert!(!report.has_errors());
        assert!(!report.fallback_execution_allowed);
        assert!(!report.arrow_converted);
        assert_eq!(report.rows_selected, Some((CHUNKS * ROWS) as u64));
        let summary: serde_json::Value = serde_json::from_str(
            report
                .result_summary
                .as_deref()
                .unwrap()
                .rsplit_once(" values=")
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(summary["values"], fixture.expected);
        assert_eq!(summary["candidate_groups"], fixture.groups);
        assert_eq!(
            summary["group_output_strategy"],
            "complete_weighted_integer_partition_topk"
        );
        assert_eq!(
            summary["aggregate_workers_integer_partition_rows"],
            CHUNKS * ROWS
        );
    }
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(None));
    }
}

#[test]
fn native_numeric_partitions_fail_committed_source_faults_and_release_owners() {
    let fixture = Fixture::new();
    let _guard = FaultGuard;
    for parallelism in [1, 4] {
        for fault in [
            SourceScanTestFault::OwnedDenial,
            SourceScanTestFault::CorruptionWithConcurrentDenial,
        ] {
            let session =
                ResidentVortexSession::for_external_cpu_pool(BUDGET, parallelism).unwrap();
            let memory = session.memory().clone();
            let prepared = prepare_aggregate_in_session(
                &fixture.prepared_request(),
                policy(parallelism),
                &session,
            )
            .unwrap();
            SOURCE_SCAN_TEST_FAULT.with(|pending| pending.set(Some(fault)));
            let error = prepared
                .execute()
                .err()
                .expect("committed faults must not return values or replay");
            assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
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
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().prepared_source_opens, 1);
            fixture.verify(&prepared.execute().unwrap(), 1);
            drop(prepared);
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}
