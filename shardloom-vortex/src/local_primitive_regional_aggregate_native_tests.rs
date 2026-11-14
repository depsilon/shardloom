//! Real independently zoned files, complete scalar references and source ownership.

use super::*;
use crate::{
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    local_primitives::{VortexLocalPrimitiveExecutionPolicy, prepared_aggregate},
    resident_session::ResidentVortexSession,
};
use serde_json::{Value, json};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri};
use std::{
    fs,
    io::Write as _,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _, VortexSessionExecute as _,
        aggregate_fn::{
            AggregateFnVTableExt as _, NumericalAggregateOpts,
            fns::{max::Max, min::Min, null_count::NullCount},
        },
        arrays::{PrimitiveArray, StructArray, struct_::StructArrayExt as _},
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{runtime::current::CurrentThreadRuntime, session::RuntimeSessionExt as _},
    layout::{
        LayoutStrategy,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy,
            flat::writer::FlatLayoutStrategy,
            repartition::{RepartitionStrategy, RepartitionWriterOptions},
            struct_::StructStrategy,
            zoned::writer::{ZonedLayoutOptions, ZonedStrategy},
        },
    },
};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    directory: PathBuf,
    predicate: Vec<Option<i64>>,
    signed: Vec<Option<i64>>,
    unsigned: Vec<Option<u64>>,
}

fn zoned(length: usize, missing_min: bool) -> Arc<dyn LayoutStrategy> {
    let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
    let mut functions = vec![
        NullCount.bind(vortex::array::aggregate_fn::EmptyOptions),
        Max.bind(NumericalAggregateOpts::skip_nans()),
    ];
    if !missing_min {
        functions.push(Min.bind(NumericalAggregateOpts::skip_nans()));
    }
    Arc::new(RepartitionStrategy::new(
        ZonedStrategy::new(
            ChunkedLayoutStrategy::new(flat.clone()),
            flat,
            ZonedLayoutOptions {
                block_size: NonZeroUsize::new(length).unwrap(),
                concurrency: NonZeroUsize::new(1).unwrap(),
                aggregate_fns: Some(functions.into()),
            },
        ),
        RepartitionWriterOptions {
            block_size_minimum: 0,
            block_len_multiple: length,
            block_size_target: None,
            canonicalize: false,
        },
    ))
}

impl Fixture {
    fn new(
        rows: usize,
        lengths: [usize; 3],
        all_null: bool,
        predicate_nulls: bool,
        missing_min: bool,
    ) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-regional-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let fixture = Self {
            directory,
            predicate: (0..rows)
                .map(|row| {
                    if predicate_nulls && row < 8 {
                        None
                    } else {
                        Some(i64::try_from(row).unwrap())
                    }
                })
                .collect(),
            signed: (0..rows)
                .map(|row| {
                    if all_null || row % 7 == 0 {
                        None
                    } else {
                        Some(match row % 4 {
                            0 => i64::MIN,
                            1 => i64::MAX,
                            2 => 9_007_199_254_740_993,
                            _ => -i64::try_from(row).unwrap(),
                        })
                    }
                })
                .collect(),
            unsigned: (0..rows)
                .map(|row| {
                    if row % 5 == 0 {
                        None
                    } else {
                        Some(u64::MAX - row as u64)
                    }
                })
                .collect(),
        };
        let array = StructArray::try_new(
            ["p", "a", "b"].into(),
            vec![
                PrimitiveArray::from_option_iter(fixture.predicate.iter().copied()).into_array(),
                PrimitiveArray::from_option_iter(fixture.signed.iter().copied()).into_array(),
                PrimitiveArray::from_option_iter(fixture.unsigned.iter().copied()).into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
        let strategy = StructStrategy::new(flat.clone(), flat)
            .with_field_writer("p", zoned(lengths[0], false))
            .with_field_writer("a", zoned(lengths[1], missing_min))
            .with_field_writer("b", zoned(lengths[2], false));
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(fixture.path())
            .unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(Arc::new(strategy))
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut output, array.dtype().clone());
        writer.push(array).unwrap();
        assert_eq!(writer.finish().unwrap().row_count(), rows as u64);
        output.sync_all().unwrap();
        fixture
    }

    fn path(&self) -> PathBuf {
        self.directory.join("input.vortex")
    }

    fn request(&self, bounds: Option<(i64, i64)>) -> VortexQueryPrimitiveRequest {
        let measure = |function: &str, name: Option<&str>, alias: &str| {
            VortexSimpleAggregateMeasure::new(
                function,
                name.map(|name: &str| ColumnRef::new(name).unwrap()),
                alias.into(),
            )
        };
        let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.path().display().to_string()).unwrap(),
            VortexSimpleAggregateRequest::new(vec![
                measure("count", None, "rows"),
                measure("count", Some("a"), "present_a"),
                measure("min", Some("a"), "lo"),
                measure("max", Some("b"), "hi"),
            ]),
        );
        if let Some((low, high)) = bounds {
            request.predicate = Some(PredicateExpr::And(vec![
                PredicateExpr::Compare {
                    column: ColumnRef::new("p").unwrap(),
                    op: ComparisonOp::Gt,
                    value: StatValue::Int64(low),
                },
                PredicateExpr::Compare {
                    column: ColumnRef::new("p").unwrap(),
                    op: ComparisonOp::Lt,
                    value: StatValue::Int64(high),
                },
            ]));
        }
        request
    }

    fn oracle(&self, bounds: Option<(i64, i64)>) -> Value {
        let selected = (0..self.predicate.len())
            .filter(|&index| {
                bounds.is_none_or(|(lo, hi)| {
                    self.predicate[index].is_some_and(|value| value > lo && value < hi)
                })
            })
            .collect::<Vec<_>>();
        json!({"rows":selected.len(), "present_a":selected.iter().filter(|&&index| self.signed[index].is_some()).count(),
            "lo":selected.iter().filter_map(|&index| self.signed[index]).min(),
            "hi":selected.iter().filter_map(|&index| self.unsigned[index]).max()})
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn policy() -> VortexLocalPrimitiveExecutionPolicy {
    VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 1).unwrap()
}

fn payload(result: &prepared_aggregate::ExecutedVortexAggregate) -> Value {
    serde_json::from_str(
        result
            .report
            .result_summary
            .as_deref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap()
}

#[test]
fn regional_aggregate_native_shared_and_independent_zones_keep_complete_exact_answers() {
    for (lengths, rows, bounds, metadata_rows, excluded_rows, residual_rows) in [
        ([8, 4, 8], 49, Some((10, 33)), 16, 17, 16),
        ([8, 4, 6], 71, Some((10, 60)), 24, 0, 47),
        ([8, 4, 8], 49, None, 49, 0, 0),
    ] {
        let fixture = Fixture::new(rows, lengths, false, false, false);
        let request = fixture.request(bounds);
        let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
        let memory = session.memory().clone();
        let prepared =
            prepared_aggregate::prepare_aggregate_in_session(&request, policy(), &session).unwrap();
        for _ in 0..2 {
            let result = prepared.execute().unwrap();
            assert!(result.native_io_certificate.is_certified());
            assert!(
                !result.report.fallback_execution_allowed
                    && !result.report.arrow_converted
                    && !result.report.spill_io_performed
            );
            let payload = payload(&result);
            assert_eq!(payload["values"], fixture.oracle(bounds));
            let proof = &payload["regional_aggregate"];
            assert_eq!(proof["metadata_rows"], metadata_rows);
            assert_eq!(proof["excluded_rows"], excluded_rows);
            assert_eq!(proof["residual_source_rows"], residual_rows);
            assert_eq!(proof["source_rows"], rows);
            assert_eq!(proof["whole_zones_only"], true);
            assert!(proof["metadata_provider_scans"].as_u64().unwrap() >= 2);
            assert!(proof["metadata_arrays_delivered"].as_u64().unwrap() >= 2);
            assert_eq!(
                proof["payload_arrays_delivered"],
                result.report.arrays_read_count
            );
            if residual_rows == 0 {
                assert_eq!(result.report.arrays_read_count, 0);
            } else {
                assert!(result.report.arrays_read_count > 0);
            }
        }
        drop(prepared);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn regional_aggregate_native_nulls_empty_and_missing_statistics_keep_result_semantics() {
    for (rows, all_null, predicate_nulls, missing_min, expect_regional) in [
        (49, true, false, false, true),
        (49, false, true, false, true),
        (49, false, false, true, true),
        (0, false, false, false, false),
    ] {
        let fixture = Fixture::new(rows, [8, 4, 8], all_null, predicate_nulls, missing_min);
        let bounds = Some((10, 33));
        let request = fixture.request(bounds);
        let prepared = prepared_aggregate::prepare_aggregate(&request, policy()).unwrap();
        let result = prepared.execute().unwrap();
        let payload = payload(&result);
        assert_eq!(payload["values"], fixture.oracle(bounds));
        assert_eq!(payload.get("regional_aggregate").is_some(), expect_regional);
        if missing_min {
            assert_eq!(payload["regional_aggregate"]["metadata_rows"], 0);
            assert!(
                payload["regional_aggregate"]["metadata_arrays_delivered"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
            assert!(result.report.arrays_read_count > 0);
        }
        assert!(result.native_io_certificate.is_certified());
    }
}

#[test]
fn regional_aggregate_native_retains_final_owned_values_after_source_session_drop() {
    let fixture = Fixture::new(49, [8, 4, 8], false, false, false);
    let request = fixture.request(Some((10, 33)));
    let prepared = prepared_aggregate::prepare_aggregate(&request, policy()).unwrap();
    let result = prepared.execute_owned().unwrap();
    assert_eq!(result.result.row_count(), 1);
    drop(prepared);
    assert_eq!(result.result.row_count(), 1);
    let columns = ["rows", "present_a", "lo", "hi"].map(str::to_owned);
    let values: Value = serde_json::from_str(
        result
            .result
            .to_bounded_json(&columns, 65536)
            .unwrap()
            .value(),
    )
    .unwrap();
    assert_eq!(values, json!([fixture.oracle(Some((10, 33)))]));
    assert!(result.execution.native_io_certificate.is_certified());
}

#[test]
fn regional_aggregate_native_optional_budget_refusal_does_not_mutate_states() {
    let fixture = Fixture::new(49, [8, 4, 8], false, false, false);
    let request = fixture.request(Some((10, 33)));
    let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    source
        .with_native_execution(|file, native, runtime| {
            let lowering =
                AggregateLowering::new(&request, file.dtype(), policy().resource_envelope())
                    .unwrap();
            let mut states = SimpleAggregateStates::new(
                &lowering.rewrite.aggregate,
                &lowering.plan.projected_columns,
            )
            .unwrap();
            let before = states.result_summary(&[]).unwrap();
            let available = session.memory().snapshot().limit_bytes
                - session.memory().snapshot().reserved_bytes;
            let peer = session.memory().reserve(available - 1024).unwrap();
            assert!(
                admit(
                    file,
                    &request,
                    &lowering,
                    &mut states,
                    native,
                    runtime,
                    None
                )
                .unwrap()
                .is_none()
            );
            assert_eq!(states.result_summary(&[]).unwrap(), before);
            drop(peer);
            Ok(())
        })
        .unwrap();
    let prepared =
        prepared_aggregate::prepare_aggregate_in_session(&request, policy(), &session).unwrap();
    assert_eq!(
        payload(&prepared.execute().unwrap())["values"],
        fixture.oracle(Some((10, 33)))
    );
}

#[test]
fn regional_aggregate_native_cancellation_and_changed_generation_never_return_partial_answers() {
    let fixture = Fixture::new(49, [8, 4, 8], false, false, false);
    let request = fixture.request(Some((10, 33)));
    let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
    let memory = session.memory().clone();
    let prepared =
        prepared_aggregate::prepare_aggregate_in_session(&request, policy(), &session).unwrap();
    let token = CancellationToken::default();
    token.cancel();
    assert!(prepared.execute_cancellable(&token).is_err());
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(fixture.path())
        .unwrap();
    file.write_all(&[0]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(prepared.execute().is_err());
    drop(prepared);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn regional_aggregate_native_exact_partials_reduce_completed_reads_on_identical_requests() {
    // More than footer prefetch, with an ordinary scan reading the same native
    // file and complete request. The test-only switch is consumed by one call.
    let fixture = Fixture::new(65_537, [2048, 1024, 2048], false, false, false);
    for bounds in [None, Some((5000, 60000))] {
        let request = fixture.request(bounds);
        let mut observations = Vec::new();
        for scan_only in [true, false] {
            let session = ResidentVortexSession::new(64 << 20, 2).unwrap();
            let memory = session.memory().clone();
            let prepared =
                prepared_aggregate::prepare_aggregate_in_session(&request, policy(), &session)
                    .unwrap();
            SCAN_ONLY_TEST.set(scan_only);
            let result = prepared.execute().unwrap();
            assert!(!SCAN_ONLY_TEST.get());
            assert!(result.native_io_certificate.is_certified());
            let values = payload(&result);
            assert_eq!(values["values"], fixture.oracle(bounds));
            assert_eq!(values.get("regional_aggregate").is_none(), scan_only);
            let sources = &result.native_io_certificate.file_scans.sources;
            assert_eq!(sources.len(), 1);
            assert!(sources[0].completed_read_requests > 0);
            assert!(sources[0].completed_read_bytes > 0);
            observations.push((
                sources[0].completed_read_bytes,
                result.report.arrays_read_count,
            ));
            drop(result);
            drop(prepared);
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
        assert!(observations[1].0 < observations[0].0, "{observations:?}");
        assert!(observations[1].1 < observations[0].1, "{observations:?}");
    }
}

#[test]
fn regional_aggregate_native_unsupported_measure_and_group_shapes_decline_before_reads() {
    let fixture = Fixture::new(49, [8, 4, 8], false, false, false);
    let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
    let (source, observer) = session
        .prepare_observed_file(
            &fixture.path(),
            crate::resident_session::read_observer::ReadObservationLimits::default(),
        )
        .unwrap();
    let before = observer.snapshot().unwrap();
    for shape in ["sum", "avg", "count_distinct", "grouped"] {
        let mut request = fixture.request(None);
        let aggregate = request.simple_aggregate.as_mut().unwrap();
        if shape == "grouped" {
            aggregate.group_by.push(ColumnRef::new("p").unwrap());
        } else {
            aggregate.measures[0] = VortexSimpleAggregateMeasure::new(
                shape,
                Some(ColumnRef::new("p").unwrap()),
                "rows".into(),
            );
        }
        source
            .with_native_execution(|file, native, runtime| {
                let lowering =
                    AggregateLowering::new(&request, file.dtype(), policy().resource_envelope())
                        .unwrap();
                let mut states = SimpleAggregateStates::new(
                    &lowering.rewrite.aggregate,
                    &lowering.plan.projected_columns,
                )
                .unwrap();
                let original = states.result_summary(&[]).unwrap();
                assert!(
                    admit(
                        file,
                        &request,
                        &lowering,
                        &mut states,
                        native,
                        runtime,
                        None
                    )
                    .unwrap()
                    .is_none()
                );
                assert_eq!(states.result_summary(&[]).unwrap(), original);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(observer.snapshot().unwrap(), before);
}

#[test]
#[allow(clippy::single_range_in_vec_init)] // Each item is a complete range request, not a row vector.
fn regional_aggregate_native_ranges_keep_projection_filter_order_and_reject_bad_bounds() {
    use crate::local_primitives::aggregate_scan_source::AggregateScanSource;
    use vortex::array::expr::{root, select};

    let fixture = Fixture::new(49, [8, 4, 8], false, false, false);
    let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    source
        .with_native_execution(|file, native, runtime| {
            let request = fixture.request(Some((10, 45)));
            let lowering =
                AggregateLowering::new(&request, file.dtype(), policy().resource_envelope())
                    .unwrap();
            let source = AggregateScanSource::File(file);
            let projection = source.bind(&select(vec!["p"], root())).unwrap();
            let filter = source.bind(lowering.plan.filter.as_ref().unwrap()).unwrap();
            let ranges = [0..9, 12..20, 40..49];
            let mut iter = source
                .scan(native)
                .unwrap()
                .with_projection(projection)
                .with_filter(filter)
                .with_concurrency(1)
                .into_array_iter_for_ranges(runtime, native, Some(&ranges))
                .unwrap();
            let mut values = Vec::new();
            let mut ctx = native.create_execution_ctx();
            for chunk in iter.by_ref() {
                let chunk = chunk.unwrap().execute::<StructArray>(&mut ctx).unwrap();
                assert_eq!(chunk.names().len(), 1);
                let column = chunk.unmasked_field_by_name("p").unwrap();
                for row in 0..column.len() {
                    values.push(
                        column
                            .execute_scalar(row, &mut ctx)
                            .unwrap()
                            .as_primitive()
                            .as_::<i64>()
                            .unwrap(),
                    );
                }
            }
            assert_eq!(values, (12..20).chain(40..45).collect::<Vec<_>>());
            assert!(iter.next().is_none());
            let mut empty = source
                .scan(native)
                .unwrap()
                .into_array_iter_for_ranges(runtime, native, Some(&[]))
                .unwrap();
            assert!(empty.next().is_none());
            for invalid in [
                vec![0..0],
                vec![0..50],
                vec![10..20, 5..9],
                vec![0..20, 19..30],
            ] {
                assert!(
                    source
                        .scan(native)
                        .unwrap()
                        .into_array_iter_for_ranges(runtime, native, Some(&invalid))
                        .is_err()
                );
            }
            Ok(())
        })
        .unwrap();
}
