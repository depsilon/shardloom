use super::*;
use crate::{VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::{ColumnRef, ComparisonOp};
use std::{
    fs::OpenOptions,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{ConstantArray, PrimitiveArray, StructArray},
        dtype::{FieldNames, StructFields},
        scalar::Scalar,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{runtime::single::SingleThreadRuntime, session::RuntimeSessionExt as _},
};

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn fixture(chunks: &[usize], all_null: bool, encoded: bool) -> Fixture {
    write_fixture(chunks, |ordinal, rows| {
        let x = if encoded {
            ConstantArray::new(Scalar::from(Some(-3_i16)), rows).into_array()
        } else {
            PrimitiveArray::from_option_iter((0..rows).map(|row| {
                if all_null {
                    None
                } else {
                    [Some(-3_i16), None, Some(5), Some(0)][row % 4]
                }
            }))
            .into_array()
        };
        let p = PrimitiveArray::new(
            vec![i8::try_from(ordinal % 2).unwrap(); rows],
            Validity::NonNullable,
        )
        .into_array();
        StructArray::try_new(
            FieldNames::from(["x", "p"]),
            vec![x, p],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    })
}

fn write_fixture(chunks: &[usize], make_chunk: impl Fn(usize, usize) -> ArrayRef) -> Fixture {
    write_with_strategy(
        chunks,
        make_chunk,
        super::super::native_flat_layout::SequentialNativeFlatLayout::strategy(chunks.len()),
    )
}

fn write_with_strategy(
    chunks: &[usize],
    make_chunk: impl Fn(usize, usize) -> ArrayRef,
    strategy: Arc<dyn vortex::layout::LayoutStrategy>,
) -> Fixture {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let output = Fixture(std::env::temp_dir().join(format!(
        "shardloom-scalar-regions-{}-{}.vortex",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let first = make_chunk(0, chunks[0]);
    let dtype = first.dtype().clone();
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output.0)
        .unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(strategy)
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, dtype);
    writer.push(first).unwrap();
    for (ordinal, &rows) in chunks.iter().enumerate().skip(1) {
        writer.push(make_chunk(ordinal, rows)).unwrap();
    }
    assert_eq!(
        writer.finish().unwrap().row_count(),
        chunks.iter().sum::<usize>() as u64
    );
    output
}

fn measure(function: &str, column: Option<&str>, alias: &str) -> VortexSimpleAggregateMeasure {
    VortexSimpleAggregateMeasure::new(
        function,
        column.map(|c| ColumnRef::new(c).unwrap()),
        alias.into(),
    )
}

fn request(path: &Fixture, filtered: bool) -> VortexQueryPrimitiveRequest {
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.0.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::new(vec![
            measure("sum", Some("x"), "sx"),
            measure("avg", Some("x"), "ax"),
            measure("min", Some("x"), "nx"),
            measure("max", Some("x"), "mx"),
            measure("count", Some("x"), "cx"),
            measure("count", None, "n"),
        ]),
    );
    if filtered {
        request.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("p").unwrap(),
            op: ComparisonOp::Eq,
            value: StatValue::Int64(1),
        });
    }
    request
}

fn expected(rows: usize, all_null: bool, encoded: bool) -> serde_json::Value {
    if all_null || rows == 0 {
        serde_json::json!({"sx":null,"ax":null,"nx":null,"mx":null,"cx":0,"n":rows})
    } else if encoded {
        serde_json::json!({"sx":-3.0 * f64::from(u32::try_from(rows).unwrap()),"ax":-3.0,"nx":-3,"mx":-3,"cx":rows,"n":rows})
    } else {
        serde_json::json!({"sx":f64::from(u32::try_from(rows / 2).unwrap()),"ax":2.0/3.0,"nx":-3,"mx":5,"cx":rows/4*3,"n":rows})
    }
}

fn payload(summary: &str) -> serde_json::Value {
    serde_json::from_str(
        summary
            .rsplit_once(" values=")
            .map_or(summary, |(_, value)| value),
    )
    .unwrap()
}

#[test]
fn scalar_regions_preserve_original_batches_nullable_values_and_both_entrypoints() {
    let chunks = [65_536, 32_768, 32_768, 65_536];
    let source = fixture(&chunks, false, false);
    for parallelism in [1, 4] {
        for filtered in [false, true] {
            let request = request(&source, filtered);
            let policy =
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 1).unwrap();
            let rows = if filtered {
                chunks[1] + chunks[3]
            } else {
                chunks.iter().sum()
            };
            let scan = super::super::read_local_vortex_simple_aggregate_scan(
                request.source_uri.as_ref().unwrap(),
                &source.0,
                &request,
                policy,
            )
            .unwrap();
            let observed = payload(&scan.result_summary);
            assert_eq!(observed["values"], expected(rows, false, false));
            assert_eq!(observed["aggregate_source_region_execution"], true);
            assert_eq!(observed["aggregate_source_region_coverage_complete"], true);
            assert_eq!(observed["aggregate_source_region_submitted"], 4);
            assert_eq!(observed["aggregate_source_region_completed"], 4);
            assert_eq!(observed["aggregate_source_region_outstanding"], 0);
            assert_eq!(observed["aggregate_provider_background_workers"], 0);
            assert_eq!(scan.scan.pre_limit_result_row_count, rows);
            assert!(scan.scan.max_chunk_rows <= 65_536);
            let prepared =
                super::super::prepared_aggregate::prepare_aggregate(&request, policy).unwrap();
            let result = prepared.execute().unwrap();
            let repeated = payload(result.report.result_summary.as_ref().unwrap());
            assert_eq!(repeated["values"], observed["values"]);
            assert_eq!(repeated["aggregate_source_region_execution"], true);
            assert!(result.native_io_certificate.is_certified());
            assert!(!result.native_io_certificate.fallback_attempted);
        }
    }
}

#[test]
fn scalar_regions_preserve_all_null_encoded_empty_and_metadata_controls() {
    for (all_null, encoded) in [(true, false), (false, true)] {
        let source = fixture(&[65_536, 65_536], all_null, encoded);
        let request = request(&source, false);
        let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 1).unwrap();
        let scan = super::super::read_local_vortex_simple_aggregate_scan(
            request.source_uri.as_ref().unwrap(),
            &source.0,
            &request,
            policy,
        )
        .unwrap();
        let observed = payload(&scan.result_summary);
        assert_eq!(observed["values"], expected(131_072, all_null, encoded));
        assert_eq!(observed["aggregate_source_region_execution"], true);
        if encoded {
            assert!(
                observed
                    .get("aggregate_encoded_numeric_reduction")
                    .is_some()
            );
        }
        let metadata = VortexQueryPrimitiveRequest::simple_aggregate(
            request.source_uri.clone().unwrap(),
            VortexSimpleAggregateRequest::new(vec![
                measure("count", None, "n"),
                measure("count", Some("p"), "cp"),
            ]),
        );
        let scan = super::super::read_local_vortex_simple_aggregate_scan(
            metadata.source_uri.as_ref().unwrap(),
            &source.0,
            &metadata,
            policy,
        )
        .unwrap();
        assert!(scan.metadata_completed);
        assert_eq!(
            payload(&scan.result_summary)["values"],
            serde_json::json!({"n":131_072,"cp":131_072})
        );
        assert!(
            payload(&scan.result_summary)
                .get("aggregate_source_region_execution")
                .is_none()
        );
    }
    let source = fixture(&[0], false, false);
    let request = request(&source, false);
    let scan = super::super::read_local_vortex_simple_aggregate_scan(
        request.source_uri.as_ref().unwrap(),
        &source.0,
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap(),
    )
    .unwrap();
    assert_eq!(
        payload(&scan.result_summary)["values"],
        expected(0, false, false)
    );
    assert!(
        payload(&scan.result_summary)
            .get("aggregate_source_region_execution")
            .is_none()
    );
}

#[test]
fn scalar_regions_preserve_run_end_reductions_and_filtered_rows() {
    use vortex::{array::VortexSessionExecute as _, encodings::runend::RunEnd};
    let source = write_fixture(&[65_536; 2], |_, rows| {
        let ends = PrimitiveArray::new(
            vec![16_384_u32, 32_768, 49_152, 65_536],
            Validity::NonNullable,
        )
        .into_array();
        let values =
            PrimitiveArray::from_option_iter([Some(-3_i16), None, Some(5), Some(0)]).into_array();
        let x = RunEnd::try_new(
            ends,
            values,
            &mut VortexSession::default().create_execution_ctx(),
        )
        .unwrap()
        .into_array();
        StructArray::try_new(
            FieldNames::from(["x"]),
            vec![x],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    });
    for parallelism in [1, 4] {
        for filtered in [false, true] {
            let mut request = request(&source, false);
            if filtered {
                request.predicate = Some(PredicateExpr::Compare {
                    column: ColumnRef::new("x").unwrap(),
                    op: ComparisonOp::GtEq,
                    value: StatValue::Int64(0),
                });
            }
            let scan = super::super::read_local_vortex_simple_aggregate_scan(
                request.source_uri.as_ref().unwrap(),
                &source.0,
                &request,
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 1).unwrap(),
            )
            .unwrap();
            let observed = payload(&scan.result_summary);
            assert_eq!(observed["aggregate_source_region_execution"], true);
            assert_eq!(
                observed["values"],
                if filtered {
                    serde_json::json!({"sx":163_840.0,"ax":2.5,"nx":0,"mx":5,"cx":65_536,"n":65_536})
                } else {
                    expected(131_072, false, false)
                }
            );
        }
    }
}

#[test]
fn scalar_region_arithmetic_admission_has_exact_width_bound_and_exclusions() {
    let aggregate = VortexSimpleAggregateRequest::new(vec![
        measure("sum", Some("x"), "s"),
        measure("avg", Some("x"), "a"),
    ]);
    let columns = vec!["x".into()];
    let states = SimpleAggregateStates::new(&aggregate, &columns).unwrap();
    for ptype in [
        PType::I8,
        PType::I16,
        PType::I32,
        PType::I64,
        PType::U8,
        PType::U16,
        PType::U32,
        PType::U64,
    ] {
        let field = DType::Primitive(ptype, Nullability::Nullable);
        let dtype = DType::Struct(
            StructFields::new(FieldNames::from(["x"]), vec![field.clone()]),
            Nullability::NonNullable,
        );
        let boundary = u64::try_from((1_u128 << 53) / integer_magnitude(&field).unwrap()).unwrap();
        assert!(states_admitted(&states, &dtype, &columns, boundary));
        assert!(!states_admitted(&states, &dtype, &columns, boundary + 1));
        let extrema = VortexSimpleAggregateRequest::new(vec![
            measure("min", Some("x"), "n"),
            measure("max", Some("x"), "m"),
            measure("count", Some("x"), "c"),
        ]);
        assert!(states_admitted(
            &SimpleAggregateStates::new(&extrema, &columns).unwrap(),
            &dtype,
            &columns,
            u64::MAX
        ));
    }
    let dtype = DType::Struct(
        StructFields::new(
            FieldNames::from(["x"]),
            vec![DType::Primitive(PType::F64, Nullability::NonNullable)],
        ),
        Nullability::NonNullable,
    );
    assert!(!states_admitted(&states, &dtype, &columns, 1));
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("unit.vortex").unwrap(),
        aggregate,
    );
    assert!(!request_schema_may_be_admitted(&request, &dtype));
    let mut changed = request.clone();
    changed.simple_aggregate.as_mut().unwrap().measures[0].argument_offset = Some(1);
    assert!(!request_may_be_admitted(&changed));
    changed = request.clone();
    changed.source_order_limit = Some(1);
    assert!(!request_may_be_admitted(&changed));
    changed = request;
    changed.predicate = Some(PredicateExpr::And(vec![]));
    assert!(!request_may_be_admitted(&changed));
}

#[test]
fn scalar_region_merge_checks_overflow_identity_and_evidence_flags() {
    let aggregate = VortexSimpleAggregateRequest::new(vec![measure("count", None, "n")]);
    let mut target = SimpleAggregateStates::new(&aggregate, &[]).unwrap();
    let mut source = target.clone();
    source.states[0].count = 1;
    source.direct_scalar_updates = true;
    source.fused_numeric_additive_updates = true;
    merge_states(&mut target, &source).unwrap();
    assert!(target.direct_scalar_updates && target.fused_numeric_additive_updates);
    target.states[0].count = u64::MAX;
    assert!(merge_states(&mut target, &source).is_err());
    target.states[0].count = 0;
    source.states[0].alias = "different".into();
    assert!(merge_states(&mut target, &source).is_err());
    source.states[0].alias = "n".into();
    source.partition_distinct_completed = true;
    assert!(merge_states(&mut target, &source).is_err());
}

#[test]
fn scalar_region_directory_bound_covers_original_split_capacity_and_releases_credit() {
    let source = fixture(&[1_048_576, 32_768, 32_768], false, false);
    let resident =
        crate::resident_session::ResidentVortexSession::for_external_cpu_pool(32 << 20, 4).unwrap();
    let memory = resident.memory().clone();
    let prepared = resident.prepare_file(&source.0).unwrap();
    crate::native_provider_memory::materialize_file_metadata_for_test(prepared.file());
    let retained = memory.snapshot().reserved_bytes;
    prepared
        .with_native_execution(|file, session, runtime| {
            let nodes = layout_node_bound(file, &memory)?.unwrap();
            let (bytes, max_len) = directory_bound(nodes, file.row_count())?;
            let scan = file.scan().map_err(vortex_error)?;
            let boundaries = scan.full_file_splits().map_err(vortex_error)?;
            assert!(boundaries.len() as u64 <= max_len);
            assert!(boundaries.capacity() as u64 <= 2 * max_len);
            assert!(
                boundaries
                    .windows(2)
                    .all(|w| w[1] - w[0] <= PROVIDER_MAX_SPLIT_ROWS)
            );
            assert!(boundaries.contains(&1_048_576));
            let request = request(&source, false);
            let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(4, 1).unwrap();
            let lowering =
                AggregateLowering::new(&request, file.dtype(), policy.resource_envelope())?;
            let mut states = SimpleAggregateStates::new(
                &lowering.rewrite.aggregate,
                &lowering.plan.projected_columns,
            )?;
            let regions = Regions::admit(
                file,
                request.source_uri.as_ref().unwrap(),
                &request,
                &lowering,
                &states,
                &lowering.plan.projected_columns,
                policy,
                session,
                runtime.clone(),
                &memory,
                None,
            )?
            .unwrap();
            assert_eq!(regions.directory_peak_bytes, bytes);
            let report = regions.run(
                &mut states,
                &mut NativeNumericAccessorWork::default(),
                &mut Vec::new(),
                &mut 0,
            )?;
            assert_eq!(report.source_rows, 1_114_112);
            assert_eq!(report.submitted, boundaries.len() as u64 - 1);
            assert_eq!(report.completed, report.submitted);
            assert!(report.peak_outstanding <= 8);
            assert_eq!(
                payload(&states.result_summary(&[])?)["values"],
                expected(1_114_112, false, false)
            );
            // The enclosing operation still owns its deferred I/O metadata.
            // Its file view and I/O scope drain when with_native_execution returns.
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(prepared);
    drop(resident);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[path = "scalar_region_workers_lifecycle_tests.rs"]
mod lifecycle;

#[path = "scalar_region_workers_layout_tests.rs"]
mod layouts;

fn assert_width_result(
    source: &Fixture,
    minimum: &serde_json::Value,
    maximum: &serde_json::Value,
    sum: Option<f64>,
) {
    let mut measures = vec![
        measure("count", Some("x"), "c"),
        measure("min", Some("x"), "lo"),
        measure("max", Some("x"), "hi"),
    ];
    let mut expected = serde_json::json!({"c":98_304,"lo":minimum,"hi":maximum});
    if let Some(sum) = sum {
        measures.extend([
            measure("sum", Some("x"), "s"),
            measure("avg", Some("x"), "a"),
        ]);
        expected["s"] = sum.into();
        expected["a"] = (sum / 98_304.0).into();
    }
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(source.0.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::new(measures),
    );
    for parallelism in [1, 4] {
        let scan = super::super::read_local_vortex_simple_aggregate_scan(
            request.source_uri.as_ref().unwrap(),
            &source.0,
            &request,
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 1).unwrap(),
        )
        .unwrap();
        let value = payload(&scan.result_summary);
        assert_eq!(value["values"], expected);
        assert_eq!(value["aggregate_source_region_execution"], true);
        assert_eq!(value["aggregate_source_region_coverage_complete"], true);
    }
}

#[test]
fn scalar_region_actual_integer_widths_preserve_extrema_nulls_and_exact_additive_values() {
    macro_rules! width {
        ($type:ty, $fourth:expr, $sum:expr) => {{
            let source = write_fixture(&[65_536; 2], |_, rows| {
                let values = [Some(<$type>::MIN), None, Some(<$type>::MAX), Some($fourth)];
                let x = PrimitiveArray::from_option_iter((0..rows).map(|row| values[row % 4]))
                    .into_array();
                StructArray::try_new(
                    FieldNames::from(["x"]),
                    vec![x],
                    rows,
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array()
            });
            assert_width_result(
                &source,
                &serde_json::json!(<$type>::MIN),
                &serde_json::json!(<$type>::MAX),
                $sum,
            );
        }};
    }
    width!(i8, 0_i8, Some(-32_768.0));
    width!(i16, 0_i16, Some(-32_768.0));
    width!(i32, 0_i32, Some(-32_768.0));
    width!(i64, 0_i64, None);
    width!(u8, 1_u8, Some(256.0 * 32_768.0));
    width!(u16, 1_u16, Some(65_536.0 * 32_768.0));
    width!(u32, 1_u32, Some(4_294_967_296.0 * 32_768.0));
    width!(u64, 1_u64, None);
}

#[test]
fn scalar_region_float_large_integer_and_source_limit_keep_existing_execution() {
    for kind in ["float", "large_integer", "source_limit"] {
        let source = write_fixture(&[65_536; 2], |_, rows| {
            let x = match kind {
                "float" => PrimitiveArray::from_option_iter(
                    (0..rows).map(|row| [Some(1.0_f64), None, Some(2.0), Some(-1.0)][row % 4]),
                )
                .into_array(),
                "large_integer" => PrimitiveArray::from_option_iter(
                    (0..rows).map(|row| [Some(i64::MAX), None, Some(-i64::MAX), Some(0)][row % 4]),
                )
                .into_array(),
                _ => PrimitiveArray::from_option_iter(
                    (0..rows).map(|row| [Some(-3_i16), None, Some(5), Some(0)][row % 4]),
                )
                .into_array(),
            };
            StructArray::try_new(
                FieldNames::from(["x"]),
                vec![x],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        });
        let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(source.0.display().to_string()).unwrap(),
            VortexSimpleAggregateRequest::new(vec![
                measure("sum", Some("x"), "s"),
                measure("avg", Some("x"), "a"),
            ]),
        );
        if kind == "source_limit" {
            request.source_order_limit = Some(2);
        }
        let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(4, 1).unwrap();
        let scan = super::super::read_local_vortex_simple_aggregate_scan(
            request.source_uri.as_ref().unwrap(),
            &source.0,
            &request,
            policy,
        )
        .unwrap();
        let value = payload(&scan.result_summary);
        assert!(value.get("aggregate_source_region_execution").is_none());
        let prepared =
            super::super::prepared_aggregate::prepare_aggregate(&request, policy).unwrap();
        let result = prepared.execute().unwrap();
        let repeated = payload(result.report.result_summary.as_ref().unwrap());
        assert_eq!(repeated["values"], value["values"]);
        assert!(repeated.get("aggregate_source_region_execution").is_none());
        if kind == "source_limit" {
            // Aggregate limits bound output rows; they must not truncate the
            // rows contributing to the scalar result.
            assert_eq!(
                value["values"],
                serde_json::json!({"s":65_536.0,"a":2.0/3.0})
            );
        }
    }
}

#[test]
fn scalar_region_offsets_distinct_and_owned_arrays_stay_outside_admission() {
    use crate::{
        owned_array_source::{OwnedArraySource, OwnedArraySourceBounds},
        resident_session::ResidentVortexSession,
    };
    let source = fixture(&[65_536; 2], false, false);
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(4, 1).unwrap();
    for distinct in [false, true] {
        let mut aggregate = VortexSimpleAggregateRequest::new(vec![measure(
            if distinct { "count_distinct" } else { "sum" },
            Some("x"),
            "n",
        )]);
        if !distinct {
            aggregate.measures[0].argument_offset = Some(1);
        }
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(source.0.display().to_string()).unwrap(),
            aggregate,
        );
        assert!(!request_may_be_admitted(&request));
        let scan = super::super::read_local_vortex_simple_aggregate_scan(
            request.source_uri.as_ref().unwrap(),
            &source.0,
            &request,
            policy,
        )
        .unwrap();
        let value = payload(&scan.result_summary);
        assert!(value.get("aggregate_source_region_execution").is_none());
        assert_eq!(
            value["values"],
            if distinct {
                serde_json::json!({"n":3})
            } else {
                serde_json::json!({"n":163_840.0})
            }
        );
    }
    let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 4).unwrap();
    let memory = resident.memory().clone();
    // File projection can return more than the memory-column intake's 65,536
    // row cap. Keep the owned source above the region row threshold so this
    // proves source-kind exclusion independently of the small-input gate.
    let native = resident.prepare_file(&source.0).unwrap();
    let result = native
        .prepare_projection(&["x"], 131_072, 16 << 20)
        .unwrap()
        .execute()
        .unwrap();
    let owned = OwnedArraySource::from_owned(
        result,
        OwnedArraySourceBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(owned.row_count(), MIN_ROWS);
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        owned.source_uri().clone(),
        VortexSimpleAggregateRequest::new(vec![
            measure("count", Some("x"), "n"),
            measure("min", Some("x"), "lo"),
            measure("max", Some("x"), "hi"),
        ]),
    );
    assert!(request_schema_may_be_admitted(&request, owned.dtype()));
    let prepared = owned.prepare_aggregate(&request, policy).unwrap();
    let executed = prepared.execute().unwrap();
    let value = payload(executed.report.result_summary.as_ref().unwrap());
    assert_eq!(
        value["values"],
        serde_json::json!({"n":98_304,"lo":-3,"hi":5})
    );
    assert!(value.get("aggregate_source_region_execution").is_none());
    assert!(!executed.native_io_certificate.fallback_attempted);
    drop((executed, prepared, owned, native, resident));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
