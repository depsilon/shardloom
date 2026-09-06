use super::super::super::{
    VortexLocalPrimitiveExecutionPolicy,
    aggregate_count_workers::{self, SOURCE_SCAN_TEST_FAULT, SourceScanTestFault},
    execute_vortex_local_primitive_with_policy,
    native_flat_layout::SequentialNativeFlatLayout,
    read_prepared_vortex_simple_aggregate_scan,
};
use crate::{VortexQueryPrimitiveRequest, resident_session::ResidentVortexSession};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read as _, Seek as _, Write as _},
    path::PathBuf,
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

struct Fixture {
    directory: PathBuf,
    pairs: Vec<(i16, u64)>,
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-exact-distinct-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let base = 1_u64 << 61;
        let pairs = vec![
            (1, base),
            (1, base + 1),
            (2, base),
            (0, 0),
            (1, base),
            (2, base + 2),
            (3, base),
            (3, base),
            (1, base + 3),
            (2, base + 2),
            (3, base + 1),
            (0, 0),
        ];
        let fixture = Self { directory, pairs };
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let arrays = fixture
            .pairs
            .chunks(4)
            .map(|pairs| {
                let values = PrimitiveArray::new(
                    pairs.iter().map(|pair| pair.1).collect::<Vec<_>>(),
                    Validity::NonNullable,
                )
                .into_array();
                let groups = PrimitiveArray::new(
                    pairs.iter().map(|pair| pair.0).collect::<Vec<_>>(),
                    Validity::NonNullable,
                )
                .into_array();
                StructArray::try_new(
                    FieldNames::from(["member_alias", "cohort_alias"]),
                    vec![values, groups],
                    pairs.len(),
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array()
            })
            .collect::<Vec<ArrayRef>>();
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(fixture.path())
            .unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(SequentialNativeFlatLayout::strategy(arrays.len()))
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut output, arrays[0].dtype().clone());
        for array in arrays {
            writer.push(array).unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), 12);
        fixture
    }
    fn path(&self) -> PathBuf {
        self.directory.join("source.vortex")
    }
    fn uri(&self) -> DatasetUri {
        DatasetUri::new(self.path().display().to_string()).unwrap()
    }
    fn query(&self) -> VortexQueryPrimitiveRequest {
        let mut query =
            VortexQueryPrimitiveRequest::simple_aggregate(self.uri(), super::tests::request())
                .with_source_order_limit(2);
        query.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("cohort_alias").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(1),
        });
        query
    }
    fn expected(&self) -> serde_json::Value {
        let mut groups = BTreeMap::<i16, BTreeSet<u64>>::new();
        for &(group, value) in &self.pairs {
            if group >= 1 {
                groups.entry(group).or_default().insert(value);
            }
        }
        let mut groups = groups
            .into_iter()
            .map(|(group, values)| (group, values.len()))
            .collect::<Vec<_>>();
        groups.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        serde_json::Value::Array(groups.into_iter().skip(1).take(2).map(|(group, count)| serde_json::json!({"cohort_alias":group,"uniques_alias":count})).collect())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn payload(report: &super::super::super::VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
    let (_, json) = report
        .result_summary
        .as_deref()
        .unwrap()
        .rsplit_once(" values=")
        .unwrap();
    serde_json::from_str(json).unwrap()
}

#[test]
fn exact_distinct_public_native_values_global_order_and_pressure_handoff() {
    let fixture = Fixture::new();
    for parallelism in [1, 2] {
        for entries in [2, 1000] {
            let mut policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
            policy.resource_envelope.group_state_soft_item_budget = entries;
            let report =
                execute_vortex_local_primitive_with_policy(&fixture.query(), policy).unwrap();
            assert!(!report.fallback_execution_allowed);
            let result = payload(&report);
            assert_eq!(result["values"], fixture.expected());
            assert_eq!(
                result["aggregate_workers_partition_native_handoffs"],
                u64::from(entries == 2)
            );
            assert_eq!(result["aggregate_workers_provider_background_workers"], 0);
            assert_eq!(result["aggregate_workers_outstanding_chunks"], 0);
            if entries == 1000 {
                assert_eq!(
                    result["aggregate_update_strategy"],
                    "complete_integer_pair_partition_distinct"
                );
                assert_eq!(result["candidate_groups"], 3);
                assert_eq!(result["retained_candidate_groups"], 3);
                assert_eq!(result["aggregate_workers_rows"], 10);
                assert_eq!(result["aggregate_workers_exact_distinct_complete_pairs"], 7);
                assert!(
                    result["exact_distinct_final_reserved_bytes"]
                        .as_u64()
                        .unwrap()
                        > 0
                );
                assert!(
                    report
                        .state_budget
                        .capillary_work_units
                        .iter()
                        .any(|unit| unit == "complete_distinct_eof_group_reduction")
                );
                assert!(
                    !report
                        .state_budget
                        .capillary_work_units
                        .iter()
                        .any(|unit| unit == "count_distinct_set")
                );
            }
        }
    }
}

struct ScanFaultGuard;
impl Drop for ScanFaultGuard {
    fn drop(&mut self) {
        SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(None));
    }
}

#[test]
fn exact_distinct_public_source_pressure_replays_once_and_preserves_corruption() {
    let fixture = Fixture::new();
    let _reset = ScanFaultGuard;
    let policy = VortexLocalPrimitiveExecutionPolicy::new(1).unwrap();
    SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(Some(SourceScanTestFault::OwnedDenial)));
    let report = execute_vortex_local_primitive_with_policy(&fixture.query(), policy).unwrap();
    assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
    assert!(!report.fallback_execution_allowed);
    let result = payload(&report);
    assert_eq!(result["values"], fixture.expected());
    assert_eq!(result["aggregate_workers_partition_source_replays"], 1);
    SOURCE_SCAN_TEST_FAULT
        .with(|fault| fault.set(Some(SourceScanTestFault::CorruptionWithConcurrentDenial)));
    let error = execute_vortex_local_primitive_with_policy(&fixture.query(), policy).unwrap_err();
    assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
    assert!(error.to_string().contains("injected source corruption"));
}

#[test]
fn exact_distinct_finalized_counts_fail_retained_source_generation_validation() {
    for replace in [false, true] {
        let fixture = Fixture::new();
        let resident = ResidentVortexSession::for_external_cpu_pool(8 << 20, 1).unwrap();
        let prepared = resident.prepare_file(fixture.path()).unwrap();
        let mut policy = VortexLocalPrimitiveExecutionPolicy::new(1).unwrap();
        policy.resource_envelope.memory_budget_bytes = 8 << 20;
        let query = fixture.query();
        let result = prepared.with_native_execution(|file, session, runtime| {
            let result = read_prepared_vortex_simple_aggregate_scan(
                &fixture.uri(),
                &query,
                policy,
                file,
                session,
                runtime,
                Some(resident.memory()),
                None,
            )?;
            let values: serde_json::Value = serde_json::from_str(&result.result_summary).unwrap();
            assert_eq!(values["values"], fixture.expected());
            assert_eq!(
                values["aggregate_update_strategy"],
                "complete_integer_pair_partition_distinct"
            );
            if replace {
                let original = fixture.directory.join("original.vortex");
                std::fs::rename(fixture.path(), &original).unwrap();
                std::fs::copy(original, fixture.path()).unwrap();
            } else {
                let mut source = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(fixture.path())
                    .unwrap();
                let mut byte = [0];
                source.read_exact(&mut byte).unwrap();
                source.rewind().unwrap();
                byte[0] ^= 1;
                source.write_all(&byte).unwrap();
            }
            Ok(result)
        });
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("prepared source changed")
        );
        drop(prepared);
        drop(query);
        assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn exact_distinct_source_shape_preflight_restores_nonadmitted_provider_lanes() {
    let fixture = Fixture::new();
    let query = fixture.query();
    assert!(aggregate_count_workers::request_may_be_admitted(&query));
    let resident = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let prepared = resident.prepare_file(fixture.path()).unwrap();
    assert!(!aggregate_count_workers::restore_provider_drivers(
        &query,
        prepared.dtype()
    ));
    let nullable = StructArray::try_new(
        FieldNames::from(["cohort_alias", "member_alias"]),
        vec![
            PrimitiveArray::new(vec![1_i16], Validity::NonNullable).into_array(),
            PrimitiveArray::from_option_iter([Some(1_u64)]).into_array(),
        ],
        1,
        Validity::NonNullable,
    )
    .unwrap();
    assert!(aggregate_count_workers::restore_provider_drivers(
        &query,
        nullable.dtype()
    ));
    let mut unbounded = query.clone();
    unbounded.source_order_limit = None;
    assert!(!aggregate_count_workers::request_may_be_admitted(
        &unbounded
    ));
}
