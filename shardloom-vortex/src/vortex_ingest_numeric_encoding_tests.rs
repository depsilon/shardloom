use super::*;
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{Constant, PrimitiveArray, StructArray, VarBinArray},
        dtype::FieldNames,
        validity::Validity,
    },
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    layout::layouts::{
        dict::writer::{DictLayoutOptions, DictStrategy},
        flat::writer::FlatLayoutStrategy,
    },
};

use crate::physical_encoding_inventory::{
    PhysicalEncodingInspectionLimits, inspect_physical_encodings,
};

fn write(
    array: &ArrayRef,
    strategy: Arc<dyn LayoutStrategy>,
    session: &VortexSession,
    runtime: &CurrentThreadRuntime,
) -> vortex::file::VortexFile {
    let mut bytes = Vec::new();
    session
        .write_options()
        .with_strategy(strategy)
        .blocking(runtime)
        .write(&mut bytes, array.to_array_iterator())
        .unwrap();
    session.open_options().open_buffer(bytes).unwrap()
}

fn round_trip(array: &ArrayRef, file: &vortex::file::VortexFile, runtime: &CurrentThreadRuntime) {
    let mut ctx = file.session().create_execution_ctx();
    let mut offset = 0;
    for actual in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(runtime)
        .unwrap()
    {
        let actual = actual.unwrap();
        assert_eq!(actual.dtype(), array.dtype());
        for row in 0..actual.len() {
            assert_eq!(
                actual.execute_scalar(row, &mut ctx).unwrap(),
                array.execute_scalar(offset + row, &mut ctx).unwrap()
            );
        }
        offset += actual.len();
    }
    assert_eq!(offset, array.len());
}

#[test]
fn pinned_dictionary_probe_discards_a_non_dict_result_but_storage_adapter_retains_it() {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let probe_encodings =
        vortex::array::legacy_session().enabled_component_ids(ComponentKind::Array);
    assert!(probe_encodings.is_empty());
    assert!(
        session
            .enabled_component_ids(ComponentKind::Array)
            .contains(&Constant.id())
    );
    let array = PrimitiveArray::new(vec![73_i64; 4096], Validity::NonNullable).into_array();
    let compressor = NumericCompressor::new(IngestStageTimings::default(), &session);
    let compressed = compressor
        .compress_chunk(&array, &mut session.create_execution_ctx())
        .unwrap();
    assert!(compressed.is::<Constant>(), "{}", compressed.encoding_id());
    for candidate in [false, true] {
        let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
        let fallback: Arc<dyn LayoutStrategy> = if candidate {
            Arc::new(NumericDataStrategy::new(
                Arc::clone(&flat),
                IngestStageTimings::default(),
                &session,
            ))
        } else {
            Arc::clone(&flat)
        };
        let strategy = DictStrategy::new(
            Arc::clone(&flat),
            flat,
            fallback,
            DictLayoutOptions::default(),
            measured_probe(
                BtrBlocksCompressorBuilder::default()
                    .retain_allowed_encodings(&probe_encodings.iter().copied().collect())
                    .build(),
                IngestStageTimings::default(),
            ),
        );
        let file = write(&array, Arc::new(strategy), &session, &runtime);
        round_trip(&array, &file, &runtime);
        let evidence = runtime
            .block_on(inspect_physical_encodings(
                &file,
                PhysicalEncodingInspectionLimits::default(),
            ))
            .unwrap();
        let actual = evidence["flat_references"][0]["stored_array_nodes"][0]["encoding_id"]
            .as_str()
            .unwrap();
        assert_eq!(
            actual,
            if candidate {
                compressed.encoding_id()
            } else {
                array.encoding_id()
            }
            .to_string()
        );
    }
}

#[test]
fn post_coalescing_numeric_codec_preserves_exact_nulls_extrema_and_admitted_encodings() {
    let session = VortexSession::default();
    let timings = IngestStageTimings::default();
    let codec = NumericCompressor::new(timings.clone(), &session);
    let inputs = [
        PrimitiveArray::from_option_iter([
            Some(i64::MIN),
            None,
            Some(i64::MAX),
            Some(9_007_199_254_740_993),
        ])
        .into_array(),
        PrimitiveArray::from_option_iter([
            Some(0_u64),
            Some(u64::MAX),
            None,
            Some(9_007_199_254_740_993),
        ])
        .into_array(),
        PrimitiveArray::from_option_iter([Some(f64::MIN), None, Some(f64::MAX), Some(-0.5)])
            .into_array(),
        PrimitiveArray::from_option_iter([Some(f32::MIN), None, Some(f32::MAX), Some(0.25)])
            .into_array(),
        PrimitiveArray::from_option_iter([None::<i64>; 16]).into_array(),
        PrimitiveArray::new(Vec::<u64>::new(), Validity::NonNullable).into_array(),
        PrimitiveArray::new((0_i64..8192).collect::<Vec<_>>(), Validity::NonNullable).into_array(),
        PrimitiveArray::new(
            (0..8192)
                .map(|index| {
                    if index % 3 == 0 {
                        1.234_567_f64
                    } else {
                        7.891_011
                    }
                })
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array(),
    ];
    let mut ctx = session.create_execution_ctx();
    for input in inputs {
        let output = codec.compress_chunk(&input, &mut ctx).unwrap();
        assert_eq!(output.len(), input.len());
        assert_eq!(output.dtype(), input.dtype());
        assert!(
            !output
                .depth_first_traversal()
                .any(|node| node.is::<vortex::array::arrays::Dict>())
        );
        for row in 0..input.len() {
            assert_eq!(
                input.execute_scalar(row, &mut ctx).unwrap(),
                output.execute_scalar(row, &mut ctx).unwrap()
            );
        }
        if !output.is::<Primitive>() {
            let preserved = codec.compress_chunk(&output, &mut ctx).unwrap();
            assert!(ArrayRef::ptr_eq(&preserved, &output));
        }
    }
    let fields = timings
        .snapshot()
        .evidence_fields()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(fields["vortex_ingest_numeric_compress_calls"], "8");
    assert!(
        fields["vortex_ingest_numeric_preserve_calls"]
            .parse::<u64>()
            .unwrap()
            > 0
    );
    assert_eq!(fields["vortex_ingest_numeric_probe_calls"], "0");
}

#[test]
#[allow(clippy::too_many_lines)] // Persisted encoding, attribution and limits share one file fixture.
fn fast_load_table_persists_numeric_encoding_after_coalescing_and_attributes_real_fields() {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let rows = 8192;
    let array = StructArray::try_new(
        FieldNames::from(["renamed_constant", "renamed_sequence", "renamed_text"]),
        vec![
            PrimitiveArray::new(vec![i64::MAX; rows], Validity::NonNullable).into_array(),
            PrimitiveArray::new(
                (0_u64..rows as u64).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            VarBinArray::from_iter_nonnull(
                (0..rows).map(|index| format!("λ{index:05}")),
                DType::Utf8(vortex::array::dtype::Nullability::NonNullable),
            )
            .into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let timings = super::super::VortexWriterStageTiming::default();
    let strategy = super::super::large_source_fast_load_vortex_write_strategy(
        1024,
        1 << 20,
        1,
        &timings,
        &session,
    );
    let file = write(&array, strategy, &session, &runtime);
    round_trip(&array, &file, &runtime);
    let inventory = runtime
        .block_on(inspect_physical_encodings(
            &file,
            PhysicalEncodingInspectionLimits::default(),
        ))
        .unwrap();
    let columns = inventory["columns"].as_array().unwrap();
    let column = |name| {
        columns
            .iter()
            .find(|value| {
                value["column_path"] == serde_json::json!([name])
                    && value["auxiliary_path"] == serde_json::json!([])
            })
            .unwrap()
    };
    let constant = column("renamed_constant")["encoding_ids"]
        .as_array()
        .unwrap();
    assert!(
        constant
            .iter()
            .any(|id| id.as_str().unwrap().contains("constant")),
        "{inventory}"
    );
    assert!(
        !constant
            .iter()
            .any(|id| id.as_str().unwrap().contains("varbin"))
    );
    assert!(
        column("renamed_text")["encoding_ids"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id.as_str().unwrap().contains("varbin"))
    );
    assert!(
        inventory["flat_references"]
            .as_array()
            .unwrap()
            .iter()
            .any(|leaf| !leaf["auxiliary_path"].as_array().unwrap().is_empty())
    );
    let fields = timings
        .stages
        .snapshot()
        .evidence_fields()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    assert!(
        fields["vortex_ingest_numeric_compress_rows"]
            .parse::<u64>()
            .unwrap()
            >= (rows * 2) as u64
    );
    assert!(
        fields["vortex_ingest_numeric_probe_calls"]
            .parse::<u64>()
            .unwrap()
            > 0
    );
    assert_eq!(fields["vortex_ingest_text_zstd_calls"], "0");
    for limits in [
        PhysicalEncodingInspectionLimits {
            max_segment_bytes: 0,
            ..Default::default()
        },
        PhysicalEncodingInspectionLimits {
            max_layout_nodes: 1,
            ..Default::default()
        },
        PhysicalEncodingInspectionLimits {
            max_array_nodes: 0,
            ..Default::default()
        },
    ] {
        assert!(
            runtime
                .block_on(inspect_physical_encodings(&file, limits))
                .is_err()
        );
    }
}

use std::collections::BTreeMap;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
#[allow(clippy::too_many_lines)] // One persisted fixture exercises all admitted consumer boundaries.
fn encoded_numeric_file_serves_exact_public_projection_filter_count_and_sort() {
    use crate::{
        VortexAggregateOrderExpr, VortexSortRowsRequest,
        local_primitives::{
            VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionStatus,
            collect::{prepare_count_in_session, prepare_rows_in_session},
            execute_vortex_local_primitive_with_policy,
        },
        query_primitive::VortexQueryPrimitiveRequest,
        resident_session::ResidentVortexSession,
    };
    use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    use shardloom_plan::ProjectionRequest;
    struct Temporary(std::path::PathBuf);
    impl Drop for Temporary {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let path = Temporary(std::env::temp_dir().join(format!(
            "shardloom-numeric-consumers-{}-{}.vortex",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let array = StructArray::try_new(
        FieldNames::from(["renamed_key", "signed", "unsigned", "metric", "constant"]),
        vec![
            PrimitiveArray::new(vec![-2_i64, 5, 5, 0, i64::MAX], Validity::NonNullable)
                .into_array(),
            PrimitiveArray::from_option_iter([
                Some(i64::MIN),
                None,
                Some(i64::MAX),
                Some(9_007_199_254_740_993),
                Some(-1),
            ])
            .into_array(),
            PrimitiveArray::from_option_iter([
                Some(u64::MAX),
                Some(0),
                None,
                Some(9_007_199_254_740_993),
                Some(3),
            ])
            .into_array(),
            PrimitiveArray::from_option_iter([
                Some(-0.5_f64),
                Some(0.25),
                None,
                Some(1.5),
                Some(-2.0),
            ])
            .into_array(),
            PrimitiveArray::new(vec![42_i64; 5], Validity::NonNullable).into_array(),
        ],
        5,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let timing = super::super::VortexWriterStageTiming::default();
    let strategy = super::super::large_source_fast_load_vortex_write_strategy(
        2,
        1 << 20,
        1,
        &timing,
        &session,
    );
    let mut output = std::fs::File::create(&path.0).unwrap();
    session
        .write_options()
        .with_strategy(strategy)
        .blocking(&runtime)
        .write(&mut output, array.to_array_iterator())
        .unwrap();
    drop(output);
    let uri = DatasetUri::new(path.0.display().to_string()).unwrap();
    let resident = ResidentVortexSession::new(32 << 20, 2).unwrap();
    assert_eq!(
        prepare_count_in_session(
            &VortexQueryPrimitiveRequest::count_all(uri.clone()),
            &resident
        )
        .unwrap()
        .execute()
        .unwrap(),
        5
    );
    let expected = serde_json::json!([
        {"renamed_key": -2, "signed": i64::MIN, "unsigned": u64::MAX, "metric": -0.5, "constant": 42},
        {"renamed_key": 5, "signed": null, "unsigned": 0, "metric": 0.25, "constant": 42},
        {"renamed_key": 5, "signed": i64::MAX, "unsigned": null, "metric": null, "constant": 42},
        {"renamed_key": 0, "signed": 9_007_199_254_740_993_i64, "unsigned": 9_007_199_254_740_993_u64, "metric": 1.5, "constant": 42},
        {"renamed_key": i64::MAX, "signed": -1, "unsigned": 3, "metric": -2.0, "constant": 42},
    ]);
    let projection = prepare_rows_in_session(
        &VortexQueryPrimitiveRequest::project(uri.clone(), ProjectionRequest::All),
        &resident,
    )
    .unwrap();
    for _ in 0..3 {
        let result = projection.execute().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(result.values_json.value()).unwrap(),
            expected
        );
    }
    let filter = VortexQueryPrimitiveRequest::filter_and_project(
        uri.clone(),
        PredicateExpr::Compare {
            column: ColumnRef::new("renamed_key").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        },
        ProjectionRequest::All,
    )
    .with_source_order_limit(2);
    let filtered = prepare_rows_in_session(&filter, &resident)
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(filtered.values_json.value()).unwrap(),
        serde_json::json!([expected[1], expected[2]])
    );
    let sort = VortexQueryPrimitiveRequest::sort_rows(
        uri,
        ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new("renamed_key", true)]),
        3,
    );
    let sorted = execute_vortex_local_primitive_with_policy(
        &sort,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert_eq!(sorted.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(!sorted.fallback_execution_allowed);
    let actual = sorted
        .result_summary
        .as_ref()
        .unwrap()
        .split_once(" values=")
        .unwrap()
        .1;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(actual).unwrap()["values"],
        serde_json::json!([expected[4], expected[1], expected[2]])
    );
}
