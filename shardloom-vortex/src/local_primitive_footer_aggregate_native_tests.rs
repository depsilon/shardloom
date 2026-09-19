//! Complete scalar oracles and actual completed native reads after preparation.
//! Completed filesystem bytes include cache/coalescing effects, not device I/O.

use super::*;
use crate::{
    VortexAggregateHavingExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    local_primitives::{self as runtime, VortexLocalPrimitiveExecutionMode},
    resident_session::read_observer::{ObservedFileReadAt, ReadObservationLimits},
};
use serde_json::{Value, json};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        expr::stats::Stat,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    layout::layouts::flat::writer::FlatLayoutStrategy,
    session::VortexSession,
};

const SIGNED: &str = "renamed_signed";
const UNSIGNED: &str = "renamed_unsigned";
const DRAIN: Duration = Duration::from_secs(10);
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    directory: PathBuf,
    signed: Vec<Option<i64>>,
    unsigned: Vec<Option<u64>>,
}

impl Fixture {
    #[allow(clippy::too_many_lines)] // Keep independent nullable values and the three native metadata controls together.
    fn new(rows: usize, all_null: bool) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-footer-aggregate-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&directory).unwrap();
        let signed = (0..rows)
            .map(|row| {
                if all_null {
                    None
                } else {
                    match row % 7 {
                        0 => Some(i64::MIN),
                        1 => Some(i64::MAX),
                        2 => None,
                        3 => Some(9_007_199_254_740_993),
                        4 => Some(-9_007_199_254_740_993),
                        _ => Some(i64::try_from(row).unwrap()),
                    }
                }
            })
            .collect::<Vec<_>>();
        let unsigned = (0..rows)
            .map(|row| {
                if all_null {
                    None
                } else {
                    match row % 5 {
                        0 => Some(u64::MAX),
                        1 => Some(0),
                        2 => None,
                        3 => Some(9_007_199_254_740_993),
                        _ => Some(u64::try_from(row).unwrap()),
                    }
                }
            })
            .collect::<Vec<_>>();
        let fixture = Self {
            directory,
            signed,
            unsigned,
        };
        let array = StructArray::try_new(
            [SIGNED, UNSIGNED].into(),
            vec![
                PrimitiveArray::from_option_iter(fixture.signed.iter().copied()).into_array(),
                PrimitiveArray::from_option_iter(fixture.unsigned.iter().copied()).into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        for (name, statistics) in [
            ("complete", vec![Stat::Min, Stat::Max, Stat::NullCount]),
            ("disabled", vec![]),
            ("missing-max", vec![Stat::Min, Stat::NullCount]),
        ] {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(fixture.path(name))
                .unwrap();
            let mut writer = session
                .write_options()
                .with_strategy(Arc::new(FlatLayoutStrategy::default()))
                .with_file_statistics(statistics)
                .blocking(&runtime)
                .writer(&mut file, array.dtype().clone());
            writer.push(array.clone()).unwrap();
            assert_eq!(
                writer.finish().unwrap().row_count(),
                u64::try_from(rows).unwrap()
            );
            file.sync_all().unwrap();
        }
        fixture
    }

    fn path(&self, profile: &str) -> PathBuf {
        self.directory.join(format!("{profile}.vortex"))
    }

    fn request(&self, profile: &str) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.path(profile).display().to_string()).unwrap(),
            VortexSimpleAggregateRequest::new(vec![
                measure("count", None, "all_rows"),
                measure("count", Some(SIGNED), "signed_present"),
                measure("min", Some(SIGNED), "signed_low"),
                measure("max", Some(SIGNED), "signed_high"),
                measure("count", Some(UNSIGNED), "unsigned_present"),
                measure("min", Some(UNSIGNED), "unsigned_low"),
                measure("max", Some(UNSIGNED), "unsigned_high"),
            ]),
        )
    }

    fn oracle(&self) -> Value {
        // Independent source values and exact standard-library integer operations;
        // never reconstruct a result from footer statistics or the engine report.
        json!({"all_rows":self.signed.len(),
            "signed_present":self.signed.iter().flatten().count(),
            "signed_low":self.signed.iter().flatten().min(),
            "signed_high":self.signed.iter().flatten().max(),
            "unsigned_present":self.unsigned.iter().flatten().count(),
            "unsigned_low":self.unsigned.iter().flatten().min(),
            "unsigned_high":self.unsigned.iter().flatten().max()})
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn measure(function: &str, column: Option<&str>, alias: &str) -> VortexSimpleAggregateMeasure {
    VortexSimpleAggregateMeasure::new(
        function,
        column.map(|name| ColumnRef::new(name).unwrap()),
        alias.into(),
    )
}

fn payload(report: &VortexLocalPrimitiveExecutionReport) -> Value {
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

fn observed_prepare(
    request: &VortexQueryPrimitiveRequest,
    parallelism: usize,
) -> (PreparedVortexAggregate, ObservedFileReadAt) {
    canonical(request).unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
    validate_policy(policy).unwrap();
    let (policy, physical_policy) = policy.with_physical_policy_for_request(request);
    let session = ResidentVortexSession::new(16 << 20, parallelism).unwrap();
    let uri = request.source_uri.as_ref().unwrap();
    let path = local_vortex_path(uri, request.kind).unwrap().unwrap();
    let (source, observer) = session
        .prepare_observed_file(&path, ReadObservationLimits::default())
        .unwrap();
    let (_, source_parallelism) = source.resource_limits();
    let (policy, physical_policy) = cap_session_cpu(policy, physical_policy, source_parallelism);
    let lowering = AggregateLowering::new(request, source.dtype()).unwrap();
    assert!(lowering.residual.is_none());
    drop(
        SimpleAggregateStates::new(
            &lowering.rewrite.aggregate,
            &lowering.plan.projected_columns,
        )
        .unwrap(),
    );
    source.validate_generation().unwrap();
    // The only substituted boundary is the actual read-at observer. This
    // unfiltered scalar family uses the ordinary provider runtime, no key pool.
    (
        PreparedVortexAggregate {
            request: request.clone(),
            source,
            session,
            lowering,
            policy,
            physical_policy,
            worker_pool: false,
            temporary_provider_drivers: false,
            reuse: None,
        },
        observer,
    )
}

fn assert_metadata(report: &VortexLocalPrimitiveExecutionReport, rows: usize, measures: usize) {
    assert_eq!(
        report.mode,
        VortexLocalPrimitiveExecutionMode::MetadataPreservingAggregate
    );
    assert!(!report.has_errors());
    assert!(
        !report.fallback_execution_allowed && !report.arrow_converted && !report.spill_io_performed
    );
    assert!(
        !report.data_read && !report.data_decoded && !report.data_materialized && !report.row_read
    );
    assert!(
        !report.upstream_scan_called
            && !report.streaming_scan_used
            && !report.full_stream_collected
    );
    assert!(!report.filter_pushdown_applied && !report.projection_pushdown_applied);
    assert!(!report.upstream_filter_expression_used && !report.upstream_projection_expression_used);
    assert_eq!(report.arrays_read_count, 0);
    assert_eq!(report.max_chunk_rows, 0);
    assert!(report.reader_splits.is_empty());
    assert_eq!(report.rows_scanned, u64::try_from(rows).unwrap());
    let work = payload(report);
    let proof = &work["metadata_aggregate"];
    assert_eq!(proof["source"], "held_vortex_file_footer");
    assert_eq!(proof["source_rows_covered"], rows);
    assert_eq!(proof["source_rows_visited"], 0);
    assert_eq!(proof["source_payload_arrays_read"], 0);
    assert_eq!(proof["measures_proven"], measures);
    assert_eq!(proof["all_measures_completed"], true);
    assert_eq!(proof["nonnullable_struct_root"], true);
    assert_eq!(proof["file_preparation_reads_excluded"], true);
}

#[test]
fn footer_aggregate_native_complete_scalar_values_avoid_actual_post_prepare_reads() {
    // Flat primitive payload exceeds footer prefetch; a tiny cached file would
    // not provide a non-vacuous completed-read control after preparation.
    let fixture = Fixture::new(65_536, false);
    for parallelism in [1, 2] {
        for profile in ["complete", "disabled"] {
            let request = fixture.request(profile);
            let (prepared, observer) = observed_prepare(&request, parallelism);
            let memory = prepared.session.memory().clone();
            let baseline = memory.snapshot().reserved_bytes;
            let before = observer.snapshot().unwrap();
            assert!(before.completed_read_bytes > 0);
            assert_eq!(prepared.snapshot().prepared_source_opens, 1);
            for execution in 1..=3 {
                let result = prepared.execute().unwrap();
                assert!(result.native_io_certificate.is_certified());
                assert!(!result.native_io_certificate.fallback_attempted);
                assert_eq!(payload(&result.report)["values"], fixture.oracle());
                assert_eq!(result.runtime.completed_executions, execution);
                assert_eq!(result.runtime.prepared_source_opens, 1);
                if profile == "complete" {
                    assert_metadata(&result.report, fixture.signed.len(), 7);
                    assert!(
                        payload(&result.report)["metadata_aggregate"]["exact_statistics_consumed"]
                            .as_u64()
                            .unwrap()
                            > 0
                    );
                    assert_eq!(observer.snapshot().unwrap(), before);
                } else {
                    assert!(payload(&result.report)["metadata_aggregate"].is_null());
                    assert!(
                        result.report.arrays_read_count > 0 && result.report.upstream_scan_called
                    );
                }
                drop(result);
                assert_eq!(memory.snapshot().reserved_bytes, baseline);
            }
            let after = observer.snapshot().unwrap();
            if profile == "disabled" {
                assert!(after.completed_read_bytes > before.completed_read_bytes);
                assert!(after.completed_read_calls > before.completed_read_calls);
            }
            assert_eq!(after.failed_read_calls, 0);
            assert_eq!(after.failed_before_read, 0);
            assert_eq!(after.rejected_requests, 0);
            drop(prepared);
            let final_reads = observer.close_and_drain(DRAIN).unwrap();
            assert!(final_reads.closed);
            assert_eq!(final_reads.pending_jobs, 0);
            assert_eq!(final_reads.completed_read_bytes, after.completed_read_bytes);
            drop(observer);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn footer_aggregate_native_empty_all_null_and_missing_statistic_match_full_scan() {
    for (rows, all_null) in [(0, false), (11, true), (19, false)] {
        let fixture = Fixture::new(rows, all_null);
        for profile in ["complete", "disabled", "missing-max"] {
            let request = fixture.request(profile);
            let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
            let ordinary =
                runtime::execute_vortex_local_primitive_with_policy(&request, policy).unwrap();
            assert_eq!(payload(&ordinary)["values"], fixture.oracle());
            let prepared = prepare_aggregate(&request, policy).unwrap();
            let memory = prepared.session.memory().clone();
            for _ in 0..2 {
                let result = prepared.execute().unwrap();
                assert!(result.native_io_certificate.is_certified());
                assert_eq!(payload(&result.report)["values"], fixture.oracle());
                if rows == 0 || profile == "complete" || (all_null && profile == "missing-max") {
                    assert_metadata(&result.report, rows, 7);
                } else {
                    assert!(payload(&result.report)["metadata_aggregate"].is_null());
                    assert!(result.report.arrays_read_count > 0);
                }
            }
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)] // One complete scalar contract across ordinary/prepared/HAVING/native text exports.
fn footer_aggregate_native_aliases_having_and_text_exports_share_scalar_contract() {
    let fixture = Fixture::new(19, false);
    for reject in [false, true] {
        let mut request = fixture.request("complete");
        let aggregate = request.simple_aggregate.as_mut().unwrap();
        aggregate.measures = vec![
            measure("max", Some(UNSIGNED), "hi"),
            measure("count", None, "n"),
        ];
        aggregate.having = vec![VortexAggregateHavingExpr::new(
            "n",
            ComparisonOp::Gt,
            if reject { "20" } else { "0" },
        )];
        request.projection =
            shardloom_plan::ProjectionRequest::columns(aggregate.projected_columns());
        let expected = if reject {
            json!({})
        } else {
            json!({"hi":u64::MAX,"n":19})
        };
        let policy = VortexLocalPrimitiveExecutionPolicy::new(2).unwrap();
        let ordinary =
            runtime::execute_vortex_local_primitive_with_policy(&request, policy).unwrap();
        assert_metadata(&ordinary, 19, 2);
        assert_eq!(payload(&ordinary)["values"], expected);
        let prepared = prepare_aggregate(&request, policy).unwrap();
        let result = prepared.execute().unwrap();
        assert_metadata(&result.report, 19, 2);
        assert_eq!(payload(&result.report)["values"], expected);
        assert_eq!(result.report.projected_columns, ["hi", "n"]);
        assert!(result.native_io_certificate.is_certified());
        assert!(prepared.execute_owned().is_err()); // Scalar owned admission remains unchanged.
        for format in [
            runtime::VortexLocalPrimitiveRowExportFormat::Jsonl,
            runtime::VortexLocalPrimitiveRowExportFormat::Csv,
        ] {
            let target = fixture
                .directory
                .join(format!("result-{reject}.{}", format.as_str()));
            let report = runtime::execute_vortex_local_primitive_row_export_with_policy(
                &request, &target, format, false, policy,
            )
            .unwrap();
            assert_eq!(report.projected_columns, ["hi", "n"]);
            assert_eq!(report.arrays_read_count, 0);
            assert!(!report.evidence.upstream_scan_called);
            assert!(
                !report.evidence.side_effects.data_read
                    && !report.evidence.side_effects.data_decoded
            );
            let text = fs::read_to_string(target).unwrap();
            match format {
                runtime::VortexLocalPrimitiveRowExportFormat::Jsonl => {
                    if reject {
                        assert!(text.is_empty());
                    } else {
                        assert_eq!(
                            serde_json::from_str::<Value>(text.trim()).unwrap(),
                            expected
                        );
                    }
                }
                runtime::VortexLocalPrimitiveRowExportFormat::Csv => {
                    assert_eq!(
                        text,
                        if reject {
                            "hi,n\n".into()
                        } else {
                            format!("hi,n\n{},19\n", u64::MAX)
                        }
                    );
                }
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn footer_aggregate_native_generation_is_checked_before_and_after_metadata_completion() {
    for after_completion in [false, true] {
        let fixture = Fixture::new(19, false);
        let request = fixture.request("complete");
        let (prepared, observer) = observed_prepare(&request, 1);
        let memory = prepared.session.memory().clone();
        let before = observer.snapshot().unwrap();
        let mutate = || {
            fs::OpenOptions::new()
                .write(true)
                .open(fixture.path("complete"))
                .unwrap()
                .set_len(8)
                .unwrap();
        };
        if after_completion {
            let outcome = prepared
                .source
                .with_native_execution(|file, session, runtime| {
                    let scan = read_lowered_vortex_simple_aggregate_scan(
                        request.source_uri.as_ref().unwrap(),
                        &request,
                        prepared.policy,
                        file,
                        session,
                        runtime,
                        None,
                        None,
                        None,
                        &prepared.lowering,
                        Instant::now(),
                        None,
                    )?;
                    let work: Value = serde_json::from_str(&scan.result_summary).unwrap();
                    assert_eq!(work["values"], fixture.oracle());
                    assert_eq!(work["metadata_aggregate"]["all_measures_completed"], true);
                    mutate();
                    Ok(scan)
                });
            assert!(
                outcome
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("prepared source changed")
            );
        } else {
            mutate();
            assert!(
                prepared
                    .execute()
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("prepared source changed")
            );
        }
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        assert_eq!(observer.snapshot().unwrap(), before);
        drop(prepared);
        observer.close_and_drain(DRAIN).unwrap();
        drop(observer);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
