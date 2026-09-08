use super::super::super::{
    VortexLocalPrimitiveExecutionPolicy, execute_vortex_local_primitive_with_policy,
    local_primitive_native_io_safe, local_primitive_row_count,
    native_flat_layout::SequentialNativeFlatLayout,
};
use crate::{
    VortexAggregateOrderExpr, VortexAggregateSpillPolicy, VortexQueryPrimitiveRequest,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    resident_session::ResidentVortexSession,
};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
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

struct Fixture {
    directory: PathBuf,
    rows: Vec<(i16, u64)>,
}
impl Fixture {
    fn new(count: usize) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-public-distinct-spill-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::create_dir(directory.join("workspace")).unwrap();
        let rows = (0..count)
            .map(|row| {
                let logical = row % 65_536;
                (
                    i16::try_from(logical % 257).unwrap() - 128,
                    (1_u64 << 61) + u64::try_from(logical).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let fixture = Self { directory, rows };
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let array = |rows: &[(i16, u64)]| {
            StructArray::try_new(
                FieldNames::from(["member_renamed", "cohort_renamed"]),
                vec![
                    PrimitiveArray::new(
                        rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                        Validity::NonNullable,
                    )
                    .into_array(),
                    PrimitiveArray::new(
                        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                        Validity::NonNullable,
                    )
                    .into_array(),
                ],
                rows.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        };
        let mut output = std::fs::File::create(fixture.path()).unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(SequentialNativeFlatLayout::strategy(
                count.div_ceil(4096).max(1),
            ))
            .blocking(&runtime)
            .writer(&mut output, array(&[]).dtype().clone());
        if fixture.rows.is_empty() {
            writer.push(array(&[])).unwrap();
        }
        for rows in fixture.rows.chunks(4096) {
            writer.push(array(rows)).unwrap();
        }
        assert_eq!(
            writer.finish().unwrap().row_count(),
            u64::try_from(count).unwrap()
        );
        fixture
    }
    fn path(&self) -> PathBuf {
        self.directory.join("source.vortex")
    }
    fn workspace(&self) -> PathBuf {
        self.directory.join("workspace")
    }
    fn query(&self, offset: usize, limit: usize) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.path().display().to_string()).unwrap(),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("cohort_renamed").unwrap()],
                vec![VortexSimpleAggregateMeasure::new(
                    "count_distinct",
                    Some(ColumnRef::new("member_renamed").unwrap()),
                    "members_count".into(),
                )],
            )
            .with_order_by(vec![
                VortexAggregateOrderExpr::new("members_count", true),
                VortexAggregateOrderExpr::new("cohort_renamed", false),
            ])
            .with_offset(offset)
            .with_spill(
                VortexAggregateSpillPolicy::new(self.workspace(), 64 << 20, 4 << 20).unwrap(),
            ),
        )
        .with_source_order_limit(limit)
    }
    fn expected(&self, offset: usize, limit: usize, minimum: i16) -> serde_json::Value {
        let mut groups = BTreeMap::<i16, BTreeSet<u64>>::new();
        for &(group, member) in &self.rows {
            if group >= minimum {
                groups.entry(group).or_default().insert(member);
            }
        }
        let mut groups = groups
            .into_iter()
            .map(|(group, values)| (group, values.len()))
            .collect::<Vec<_>>();
        groups.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        groups
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(group, count)| serde_json::json!({"cohort_renamed":group,"members_count":count}))
            .collect::<Vec<_>>()
            .into()
    }
    fn empty(&self) {
        assert_eq!(std::fs::read_dir(self.workspace()).unwrap().count(), 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn values(report: &super::super::super::VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
    let summary = report.result_summary.as_ref().unwrap();
    serde_json::from_str::<serde_json::Value>(summary.split_once(" values=").unwrap().1).unwrap()["values"].clone()
}

#[test]
fn public_exact_distinct_spill_many_runs_filtered_reordered_values_and_scoped_certificate() {
    let fixture = Fixture::new(131_072);
    for filtered in [false, true] {
        let mut request = fixture.query(123, 7);
        let minimum = if filtered { 0 } else { i16::MIN };
        if filtered {
            request.predicate = Some(PredicateExpr::Compare {
                column: ColumnRef::new("cohort_renamed").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(0),
            });
        }
        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        let expected = fixture.expected(123, 7, minimum);
        let expected_rows = u64::try_from(expected.as_array().unwrap().len()).unwrap();
        assert_eq!(values(&report), expected);
        let evidence = report.state_budget.native_aggregate_spill.as_ref().unwrap();
        assert!(evidence.runs_written >= 2);
        assert_eq!(evidence.runs_written, evidence.runs_validated);
        assert!(evidence.peak_reserved_bytes <= evidence.memory_bytes);
        assert!(evidence.peak_disk_bytes <= evidence.quota_bytes);
        assert!(evidence.owned_cleanup_completed && report.write_io && report.spill_io_performed);
        assert!(!report.arrow_converted && !report.fallback_execution_allowed);
        assert!(local_primitive_native_io_safe(&request, &report));
        assert_eq!(local_primitive_row_count(&report), Some(expected_rows));
        for changed in ["input", "output", "source", "limit"] {
            let mut forged = report.clone();
            match changed {
                "input" => forged.source_order_limit_input_rows = Some(0),
                "output" => forged.source_order_limit_rows_output = Some(0),
                "source" => forged.rows_scanned = 0,
                _ => forged.source_order_limit_requested = Some(expected_rows - 1),
            }
            assert!(!local_primitive_native_io_safe(&request, &forged));
        }
        let mut forged = report.clone();
        forged
            .state_budget
            .native_aggregate_spill
            .as_mut()
            .unwrap()
            .family = "unsupported_group_family".into();
        assert!(!local_primitive_native_io_safe(&request, &forged));
        let mut wrong_policy = request.clone();
        wrong_policy
            .simple_aggregate
            .as_mut()
            .unwrap()
            .spill
            .as_mut()
            .unwrap()
            .quota_bytes += 1;
        assert!(!local_primitive_native_io_safe(&wrong_policy, &report));
        for secondary in [
            VortexAggregateOrderExpr::new("cohort_renamed", true),
            VortexAggregateOrderExpr::new("member_renamed", false),
        ] {
            let mut wrong_order = request.clone();
            wrong_order.simple_aggregate.as_mut().unwrap().order_by[1] = secondary;
            assert!(!local_primitive_native_io_safe(&wrong_order, &report));
            wrong_order.source_uri = Some(
                DatasetUri::new(
                    fixture
                        .directory
                        .join("absent.vortex")
                        .display()
                        .to_string(),
                )
                .unwrap(),
            );
            assert!(
                execute_vortex_local_primitive_with_policy(
                    &wrong_order,
                    VortexLocalPrimitiveExecutionPolicy::single_threaded()
                )
                .unwrap_err()
                .to_string()
                .contains("admits only bounded identity integer grouped COUNT DISTINCT")
            );
        }
        fixture.empty();
    }
}

#[test]
fn public_exact_distinct_spill_empty_small_and_unsupported_before_source_open() {
    for rows in [0, 32] {
        let fixture = Fixture::new(rows);
        let request = fixture.query(0, 7);
        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        assert_eq!(values(&report), fixture.expected(0, 7, i16::MIN));
        assert_eq!(
            report
                .state_budget
                .native_aggregate_spill
                .as_ref()
                .unwrap()
                .runs_written,
            0
        );
        assert!(!report.write_io && !report.spill_io_performed);
        assert!(local_primitive_native_io_safe(&request, &report));
        if rows == 0 {
            assert_eq!(local_primitive_row_count(&report), Some(0));
            assert!(report.upstream_scan_called && report.streaming_scan_used);
            assert!(!report.data_read && !report.data_decoded && !report.data_materialized);
            let mut forged = report.clone();
            forged.arrays_read_count = 1;
            assert!(!local_primitive_native_io_safe(&request, &forged));
            let mut forged = report.clone();
            forged.rows_selected = Some(1);
            assert!(!local_primitive_native_io_safe(&request, &forged));
        }
        fixture.empty();
        let mut unsupported = request;
        unsupported.source_uri = Some(
            DatasetUri::new(
                fixture
                    .directory
                    .join("absent.vortex")
                    .display()
                    .to_string(),
            )
            .unwrap(),
        );
        unsupported.simple_aggregate.as_mut().unwrap().measures[0].function = "sum".into();
        let error = execute_vortex_local_primitive_with_policy(
            &unsupported,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("admits only bounded identity integer grouped COUNT DISTINCT")
        );
        fixture.empty();
    }
}

#[test]
fn public_exact_distinct_spill_quota_workspace_cancel_and_source_generation_errors_cleanup() {
    let fixture = Fixture::new(65_537);
    let mut request = fixture.query(0, 7);
    request
        .simple_aggregate
        .as_mut()
        .unwrap()
        .spill
        .as_mut()
        .unwrap()
        .quota_bytes = 32_769;
    let error = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native exact integer COUNT DISTINCT spill")
    );
    fixture.empty();
    let foreign = fixture.directory.join("foreign.txt");
    std::fs::write(&foreign, b"preserve foreign payload").unwrap();
    let link = fixture.directory.join("linked-workspace");
    std::os::unix::fs::symlink(fixture.workspace(), &link).unwrap();
    for workspace in [&foreign, &link] {
        let mut request = fixture.query(0, 7);
        request
            .simple_aggregate
            .as_mut()
            .unwrap()
            .spill
            .as_mut()
            .unwrap()
            .workspace = workspace.clone();
        assert!(
            execute_vortex_local_primitive_with_policy(
                &request,
                VortexLocalPrimitiveExecutionPolicy::single_threaded()
            )
            .is_err()
        );
        fixture.empty();
    }
    assert_eq!(
        std::fs::read(&foreign).unwrap(),
        b"preserve foreign payload"
    );
    let request = fixture.query(0, 7);
    request
        .simple_aggregate
        .as_ref()
        .unwrap()
        .spill
        .as_ref()
        .unwrap()
        .cancel();
    assert!(
        execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded()
        )
        .unwrap_err()
        .to_string()
        .contains("cancelled")
    );
    fixture.empty();

    let request = fixture.query(0, 7);
    let resident = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let prepared = resident.prepare_file(fixture.path()).unwrap();
    let memory = resident.memory().clone();
    let path = fixture.path();
    super::AFTER_FINISH.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::fs::write(path, b"mutated generation").unwrap();
        }));
    });
    let result = prepared.with_native_execution(|file, session, runtime| {
        super::execute(
            request.source_uri.as_ref().unwrap(),
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            file,
            session,
            runtime,
            resident.memory(),
        )
    });
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("prepared source changed; prepare the source again")
    );
    fixture.empty();
    drop(prepared);
    drop(resident);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
