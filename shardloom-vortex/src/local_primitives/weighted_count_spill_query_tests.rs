use super::super::{
    VortexLocalPrimitiveExecutionPolicy, execute_vortex_local_primitive_with_policy,
    local_primitive_native_io_certificate, local_primitive_native_io_safe,
    native_flat_layout::SequentialNativeFlatLayout,
};
use crate::{
    VortexAggregateOrderExpr, VortexAggregateSpillPolicy, VortexQueryPrimitiveRequest,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    resident_session::ResidentVortexSession,
};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use std::{collections::BTreeMap, path::PathBuf};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

struct Fixture {
    directory: PathBuf,
    rows: Vec<(i64, String, u64)>,
}
impl Fixture {
    fn new(count: usize, width: usize, dictionary: bool, nullable: bool) -> Self {
        Self::with_unique_keys(count, width, dictionary, nullable, false)
    }
    fn with_unique_keys(
        count: usize,
        width: usize,
        dictionary: bool,
        nullable: bool,
        unique: bool,
    ) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-weighted-public-{}-{}",
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
                let late = count >= 2048 && row >= count - 1024;
                let group = if late {
                    i64::MIN
                } else {
                    [i64::MIN, i64::MAX, -7, 0, 7][row % 5]
                };
                let name = if late {
                    "winner00".to_owned()
                } else {
                    format!("k{:07}", if unique { row } else { row % 1021 })
                };
                (
                    group,
                    format!("{name}{}", "x".repeat(width.saturating_sub(8))),
                    row as u64,
                )
            })
            .collect::<Vec<_>>();
        let fixture = Self { directory, rows };
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut output = std::fs::File::create(fixture.path()).unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(SequentialNativeFlatLayout::strategy(
                count.div_ceil(4096).max(1),
            ))
            .blocking(&runtime)
            .writer(
                &mut output,
                Self::array(&[], 0, dictionary, nullable).dtype().clone(),
            );
        if count == 0 {
            writer
                .push(Self::array(&[], 0, dictionary, nullable))
                .unwrap();
        }
        for (batch, rows) in fixture.rows.chunks(4096).enumerate() {
            writer
                .push(Self::array(rows, batch, dictionary, nullable))
                .unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), count as u64);
        fixture
    }
    fn array(
        rows: &[(i64, String, u64)],
        batch: usize,
        dictionary: bool,
        nullable: bool,
    ) -> ArrayRef {
        let text = if nullable {
            VarBinViewArray::from_iter_nullable_str(
                rows.iter()
                    .enumerate()
                    .map(|(index, row)| (index != 0).then_some(row.1.as_str())),
            )
            .into_array()
        } else if dictionary && !rows.is_empty() {
            let mut values = rows.iter().map(|row| row.1.as_str()).collect::<Vec<_>>();
            let mut codes = (0..rows.len())
                .map(|index| u16::try_from(index).unwrap())
                .collect::<Vec<_>>();
            if batch % 2 == 1 {
                values.reverse();
                codes.reverse();
            }
            DictArray::try_new(
                PrimitiveArray::new(codes, Validity::NonNullable).into_array(),
                VarBinViewArray::from_iter_str(values).into_array(),
            )
            .unwrap()
            .into_array()
        } else {
            VarBinViewArray::from_iter_str(rows.iter().map(|row| row.1.as_str())).into_array()
        };
        StructArray::try_new(
            FieldNames::from(["event_ordinal", "label_renamed", "cohort_renamed"]),
            vec![
                PrimitiveArray::new(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                    Validity::NonNullable,
                )
                .into_array(),
                text,
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
    }
    fn path(&self) -> PathBuf {
        self.directory.join("source.vortex")
    }
    fn workspace(&self) -> PathBuf {
        self.directory.join("workspace")
    }
    fn query(&self, groups: &[&str], offset: usize, limit: usize) -> VortexQueryPrimitiveRequest {
        let mut order = vec![VortexAggregateOrderExpr::new("frequency", true)];
        order.extend(
            groups
                .iter()
                .map(|group| VortexAggregateOrderExpr::new(*group, false)),
        );
        VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.path().display().to_string()).unwrap(),
            VortexSimpleAggregateRequest::grouped(
                groups
                    .iter()
                    .map(|group| ColumnRef::new(*group).unwrap())
                    .collect(),
                vec![VortexSimpleAggregateMeasure::new(
                    "count",
                    None,
                    "frequency".into(),
                )],
            )
            .with_order_by(order)
            .with_offset(offset)
            .with_spill(
                VortexAggregateSpillPolicy::new(self.workspace(), 64 << 20, 8 << 20).unwrap(),
            ),
        )
        .with_source_order_limit(limit)
    }
    fn expected(
        &self,
        groups: &[&str],
        offset: usize,
        limit: usize,
        minimum: u64,
    ) -> serde_json::Value {
        let numeric = groups.contains(&"cohort_renamed");
        let mut counts = BTreeMap::new();
        for (number, text, ordinal) in &self.rows {
            if *ordinal >= minimum {
                *counts
                    .entry((numeric.then_some(*number), text.clone()))
                    .or_insert(0_u64) += 1;
            }
        }
        let mut rows = counts.into_iter().collect::<Vec<_>>();
        rows.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| {
                if groups[0] == "cohort_renamed" {
                    left.0.cmp(&right.0)
                } else {
                    left.0
                        .1
                        .cmp(&right.0.1)
                        .then_with(|| left.0.0.cmp(&right.0.0))
                }
            })
        });
        rows.into_iter()
            .skip(offset)
            .take(limit)
            .map(|((number, text), count)| {
                let mut row = serde_json::Map::new();
                row.insert("label_renamed".into(), text.into());
                if let Some(number) = number {
                    row.insert("cohort_renamed".into(), number.into());
                }
                row.insert("frequency".into(), count.into());
                serde_json::Value::Object(row)
            })
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
fn summary(report: &super::super::VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
    serde_json::from_str(
        report
            .result_summary
            .as_deref()
            .unwrap()
            .split_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap()
}

#[test]
fn public_weighted_count_spill_native_domains_both_group_orders_filter_global_values_and_certificate()
 {
    let fixture = Fixture::new(65_536, 128, true, false);
    for groups in [
        vec!["label_renamed"],
        vec!["cohort_renamed", "label_renamed"],
        vec!["label_renamed", "cohort_renamed"],
    ] {
        let minimum = if groups[0] == "cohort_renamed" {
            16_384
        } else {
            0
        };
        let mut request = fixture.query(&groups, 7, 7);
        if minimum != 0 {
            request.predicate = Some(PredicateExpr::Compare {
                column: ColumnRef::new("event_ordinal").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::UInt64(minimum),
            });
        }
        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        let output = summary(&report);
        assert_eq!(output["values"], fixture.expected(&groups, 7, 7, minimum));
        assert_eq!(report.rows_selected, Some(65_536 - minimum));
        let evidence = report
            .state_budget
            .native_weighted_count_spill
            .as_ref()
            .unwrap();
        assert!(report.state_budget.native_aggregate_spill.is_none());
        if groups.len() == 1 {
            assert_eq!(evidence.runs_written, 0);
            assert_eq!(
                output["weighted_count_spill"]["fitted_partition_selection"],
                true
            );
            assert!(
                output["weighted_count_spill"]["worker_jobs_completed"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
            assert_eq!(evidence.min_run_block_rows, 0);
            assert_eq!(evidence.max_run_key_bytes, 0);
            assert!(!report.write_io && !report.spill_io_performed);
        } else {
            assert!(evidence.runs_written >= 4 && evidence.merge_passes > 0);
            assert!(evidence.min_run_block_rows > 1 && evidence.max_run_key_bytes == 128);
            assert!(report.write_io && report.spill_io_performed);
        }
        assert_eq!(evidence.runs_written, evidence.runs_validated);
        assert!(evidence.peak_reserved_bytes <= evidence.memory_bytes);
        assert!(evidence.peak_disk_bytes <= evidence.quota_bytes);
        assert!(evidence.owned_cleanup_completed);
        assert!(!report.arrow_converted && !report.fallback_execution_allowed);
        assert!(
            local_primitive_native_io_certificate(&request, &report)
                .unwrap()
                .is_certified()
        );
        if minimum == 0 {
            assert!(
                output["weighted_count_spill"]["dictionary_batches"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
        }
        for field in ["family", "source", "geometry", "cleanup"] {
            let mut forged = report.clone();
            let spill = forged
                .state_budget
                .native_weighted_count_spill
                .as_mut()
                .unwrap();
            match field {
                "family" => spill.family = "generic_join".into(),
                "source" => spill.source_rows += 1,
                "geometry" => spill.min_run_block_rows = usize::from(evidence.runs_written == 0),
                _ => spill.owned_cleanup_completed = false,
            }
            assert!(!local_primitive_native_io_safe(&request, &forged));
        }
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
        fixture.empty();
    }
}

#[test]
fn public_weighted_count_spill_empty_small_and_unprunable_zero_are_lazy_exact_and_certified() {
    for count in [0, 32] {
        let fixture = Fixture::new(count, 8, true, false);
        let request = fixture.query(&["label_renamed"], 0, 7);
        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        assert_eq!(
            summary(&report)["values"],
            fixture.expected(&["label_renamed"], 0, 7, 0)
        );
        let evidence = report
            .state_budget
            .native_weighted_count_spill
            .as_ref()
            .unwrap();
        assert_eq!(evidence.runs_written, 0);
        assert_eq!(evidence.min_run_block_rows, 0);
        assert!(!report.write_io && !report.spill_io_performed);
        assert!(local_primitive_native_io_safe(&request, &report));
        fixture.empty();
        if count != 0 {
            let mut empty = request;
            empty.predicate = Some(PredicateExpr::Compare {
                column: ColumnRef::new("cohort_renamed").unwrap(),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(6),
            });
            let report = execute_vortex_local_primitive_with_policy(
                &empty,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            )
            .unwrap();
            assert_eq!(summary(&report)["values"], serde_json::json!([]));
            assert!(report.data_read && report.streaming_scan_used);
            assert!(local_primitive_native_io_safe(&empty, &report));
            fixture.empty();
        }
    }
}

#[test]
fn public_weighted_count_workers_force_exact_spill_with_global_ties_and_offset() {
    let fixture = Fixture::with_unique_keys(16_384, 128, true, false, true);
    let request = fixture.query(&["label_renamed"], 1, 7);
    for parallelism in [1, 2, 4] {
        let mut policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
        policy.resource_envelope.group_state_soft_item_budget = 1;
        let report = execute_vortex_local_primitive_with_policy(&request, policy).unwrap();
        let output = summary(&report);
        assert_eq!(
            output["values"],
            fixture.expected(&["label_renamed"], 1, 7, 0)
        );
        let work = &output["weighted_count_spill"];
        assert_eq!(work["drained_epochs"], 1);
        assert!(work["worker_jobs_completed"].as_u64().unwrap() > 0);
        assert!(work["committed_weight"].as_u64().unwrap() > 0);
        assert!(work["deferred_weight"].as_u64().unwrap() > 0);
        assert_eq!(work["source_weight"], 16_384);
        assert_eq!(work["fitted_partition_selection"], false);
        let spill = report
            .state_budget
            .native_weighted_count_spill
            .as_ref()
            .unwrap();
        assert!(spill.runs_written > 0);
        assert_eq!(spill.runs_written, spill.runs_validated);
        assert!(spill.peak_reserved_bytes <= spill.memory_bytes);
        assert!(local_primitive_native_io_safe(&request, &report));
        fixture.empty();
    }
}

#[test]
fn public_weighted_count_spill_rejects_unsupported_before_source_open_and_nullable_schema() {
    let fixture = Fixture::new(8, 8, false, true);
    let request = fixture.query(&["label_renamed"], 0, 7);
    assert!(
        execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded()
        )
        .unwrap_err()
        .to_string()
        .contains("nonnullable UTF8")
    );
    fixture.empty();
    for variant in ["measure", "column", "order", "limit", "memory", "payload"] {
        let mut request = fixture.query(&["label_renamed"], 0, 7);
        request.source_uri = Some(
            DatasetUri::new(
                fixture
                    .directory
                    .join("absent.vortex")
                    .display()
                    .to_string(),
            )
            .unwrap(),
        );
        match variant {
            "measure" => {
                request.simple_aggregate.as_mut().unwrap().measures[0].function = "sum".into();
            }
            "column" => {
                request.simple_aggregate.as_mut().unwrap().measures[0].column =
                    Some(ColumnRef::new("label_renamed").unwrap());
            }
            "order" => request.simple_aggregate.as_mut().unwrap().order_by[1].descending = true,
            "limit" => request.source_order_limit = Some(0),
            "memory" => {
                request
                    .simple_aggregate
                    .as_mut()
                    .unwrap()
                    .spill
                    .as_mut()
                    .unwrap()
                    .memory_bytes = 2 << 20;
            }
            _ => request.sample_seed = Some(7),
        }
        assert!(
            execute_vortex_local_primitive_with_policy(
                &request,
                VortexLocalPrimitiveExecutionPolicy::single_threaded()
            )
            .unwrap_err()
            .to_string()
            .contains("native aggregate spill admits only")
        );
        fixture.empty();
    }
}

#[test]
fn public_weighted_count_spill_quota_cancel_long_keys_and_source_generation_cleanup() {
    let fixture = Fixture::with_unique_keys(16_384, 128, true, false, true);
    let mut request = fixture.query(&["label_renamed"], 0, 7);
    request
        .simple_aggregate
        .as_mut()
        .unwrap()
        .spill
        .as_mut()
        .unwrap()
        .quota_bytes = 32 * 1024 + 1;
    let mut pressure = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    pressure.resource_envelope.group_state_soft_item_budget = 1;
    assert!(execute_vortex_local_primitive_with_policy(&request, pressure).is_err());
    fixture.empty();
    let request = fixture.query(&["label_renamed"], 0, 7);
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
    let long = Fixture::new(1, 65_537, false, false);
    let request = long.query(&["label_renamed"], 0, 1);
    assert!(
        execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded()
        )
        .unwrap_err()
        .to_string()
        .contains("admitted byte bound")
    );
    long.empty();
    let request = fixture.query(&["label_renamed"], 0, 7);
    let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 1).unwrap();
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

#[test]
fn public_weighted_count_spill_parent_envelope_survives_source_and_session_owners() {
    let fixture = Fixture::new(64, 8, true, false);
    let request = fixture.query(&["cohort_renamed", "label_renamed"], 7, 7);
    let resident = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let prepared = resident.prepare_file(fixture.path()).unwrap();
    let memory = resident.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let (scan, owner) = prepared
        .with_native_execution(|file, session, runtime| {
            super::execute(
                request.source_uri.as_ref().unwrap(),
                &request,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
                file,
                session,
                runtime,
                resident.memory(),
            )
        })
        .unwrap();
    assert_eq!(scan.scan.result_row_count, 7);
    assert!(memory.snapshot().reserved_bytes >= baseline + (8 << 20));
    assert_eq!(owner.reserved_bytes(), 8 << 20);
    let (_, result) = super::result_summary(&owner, &request).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&result).unwrap()["values"],
        fixture.expected(&["cohort_renamed", "label_renamed"], 7, 7, 0)
    );
    drop(scan);
    drop(prepared);
    drop(resident);
    assert_eq!(memory.snapshot().reserved_bytes, 8 << 20);
    drop(owner);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    fixture.empty();
}
