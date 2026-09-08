use super::*;
use crate::{VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use vortex::VortexSessionDefault as _;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{ConstantArray, StructArray},
    dtype::Nullability,
    scalar::Scalar,
    validity::Validity,
};

fn states(functions: &[&str], columns: &[String]) -> SimpleAggregateStates {
    let measures = functions
        .iter()
        .enumerate()
        .map(|(index, function)| {
            VortexSimpleAggregateMeasure::new(
                *function,
                Some(ColumnRef::new(&columns[0]).unwrap()),
                format!("m{index}"),
            )
        })
        .collect();
    SimpleAggregateStates::new(&VortexSimpleAggregateRequest::new(measures), columns).unwrap()
}

fn assert_equal(actual: &SimpleAggregateStates, reference: &SimpleAggregateStates) {
    for (actual, reference) in actual.states.iter().zip(&reference.states) {
        assert_eq!(
            actual.result_json().unwrap(),
            reference.result_json().unwrap()
        );
        assert_eq!(actual.count, reference.count);
        assert_eq!(actual.sum.to_bits(), reference.sum.to_bits());
    }
}

fn check(encoded: &ArrayRef, selection: Option<&[usize]>, functions: &[&str], offset: Option<i64>) {
    let columns = vec!["renamed_measure".to_owned()];
    let session = vortex::session::VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let canonical = encoded
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap()
        .into_array();
    let mut actual = states(functions, &columns);
    for state in &mut actual.states {
        if matches!(
            state.function,
            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
        ) {
            state.argument_offset = offset;
            // Nonzero carried state proves that a weighted multiply or new
            // chunk boundary cannot silently alter accumulation across calls.
            state.sum = 7.0;
            state.count = 2;
        }
    }
    let mut reference = actual.clone();
    let mut work = NativeNumericAccessorWork::default();
    assert!(
        actual
            .update_direct_from_chunk(encoded, &columns, selection, &mut work, &mut ctx)
            .unwrap()
    );
    assert!(
        reference
            .update_direct_from_chunk(
                &canonical,
                &columns,
                selection,
                &mut NativeNumericAccessorWork::default(),
                &mut ctx
            )
            .unwrap()
    );
    assert_equal(&actual, &reference);
    assert_eq!(work.encoded_reduction.calls, 1);
    let mut summary = "{}".to_owned();
    work.annotate(&mut summary).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert!(summary["aggregate_encoded_numeric_reduction"].is_object());
    assert_eq!(
        summary["aggregate_native_numeric_accessor"]["native_decode_calls"],
        0
    );
}

#[test]
fn encoded_numeric_reduction_all_widths_match_native_values_and_selected_duplicates() {
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    macro_rules! integer {
        ($ty:ty) => {{
            let values = PrimitiveArray::from_option_iter([
                Some(<$ty>::MIN),
                None,
                Some(<$ty>::MAX),
                Some(3 as $ty),
            ])
            .into_array();
            let runs = RunEnd::try_new(
                PrimitiveArray::new(vec![2_u32, 4, 7, 11], Validity::NonNullable).into_array(),
                values,
                &mut ctx,
            )
            .unwrap()
            .into_array();
            for rows in [
                None,
                Some([10, 0, 6, 2, 6, 1].as_slice()),
                Some([].as_slice()),
            ] {
                check(
                    &runs,
                    rows,
                    &["count", "count_distinct", "min", "max", "sum", "avg"],
                    Some(-1),
                );
                check(&runs, rows, &["sum"], Some(2));
            }
        }};
    }
    integer!(u8);
    integer!(u16);
    integer!(u32);
    integer!(u64);
    integer!(i8);
    integer!(i16);
    integer!(i32);
    integer!(i64);
    for values in [
        PrimitiveArray::from_option_iter([Some(0.1_f32), Some(-0.0), None, Some(-0.1)])
            .into_array(),
        PrimitiveArray::from_option_iter([Some(1e16_f64), Some(1.0), None, Some(-1e16)])
            .into_array(),
    ] {
        let runs = RunEnd::try_new(
            PrimitiveArray::new(vec![2_u16, 101, 104, 107], Validity::NonNullable).into_array(),
            values,
            &mut ctx,
        )
        .unwrap()
        .into_array();
        check(&runs, None, &["sum"], None);
        check(
            &runs,
            None,
            &["sum", "avg", "count", "min", "max", "count_distinct"],
            None,
        );
        check(&runs, Some(&[105, 0, 4, 105, 4]), &["sum", "avg"], Some(3));
    }
}

#[test]
fn encoded_numeric_reduction_wide_fusion_preserves_offsets_and_mixed_measures() {
    let columns = vec!["measure".to_owned()];
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let array = RunEnd::try_new(
        PrimitiveArray::new(vec![3_u32, 7, 9, 15], Validity::NonNullable).into_array(),
        PrimitiveArray::from_option_iter([Some(1e16_f64), Some(1.0), None, Some(-1e16)])
            .into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    let canonical = array
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap()
        .into_array();
    for mixed in [false, true] {
        let mut functions = vec!["sum"; 90];
        if mixed {
            functions.extend(["avg", "min", "max", "count", "count_distinct"]);
        }
        let mut actual = states(&functions, &columns);
        for (index, state) in actual.states.iter_mut().enumerate() {
            if matches!(
                state.function,
                SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
            ) {
                state.argument_offset = Some(i64::try_from(index).unwrap() - 45);
                state.sum = 7.0;
                state.count = 2;
            }
        }
        let mut count_all = states(&["count"], &columns).states.remove(0);
        count_all.column_index = None;
        count_all.count = 3;
        actual.states.push(count_all);
        let mut reference = actual.clone();
        if !mixed {
            let mut work = NativeNumericAccessorWork::default();
            assert!(!update(&mut actual, &array, &columns, None, &mut work, &mut ctx).unwrap());
            assert_equal(&actual, &reference);
            assert_eq!(work.encoded_reduction.calls, 0);
            assert_eq!(work.encoded_reduction.child_primitive_executions, 0);
        }
        // Successive calls also check that fusion remains per source array,
        // including unordered selections and duplicate rows around null runs.
        for selection in [None, Some([14, 3, 8, 0, 3, 14].as_slice())] {
            assert!(
                actual
                    .update_direct_from_chunk(
                        &array,
                        &columns,
                        selection,
                        &mut NativeNumericAccessorWork::default(),
                        &mut ctx,
                    )
                    .unwrap()
            );
            assert!(
                reference
                    .update_direct_from_chunk(
                        &canonical,
                        &columns,
                        selection,
                        &mut NativeNumericAccessorWork::default(),
                        &mut ctx,
                    )
                    .unwrap()
            );
            assert_equal(&actual, &reference);
        }
    }
}

#[test]
fn encoded_numeric_reduction_constants_preserve_null_empty_and_floating_order() {
    for array in [
        ConstantArray::new(0.1_f64, 100_001).into_array(),
        ConstantArray::new(i64::MIN, 11).into_array(),
        ConstantArray::new(u64::MAX, 11).into_array(),
        ConstantArray::new(
            Scalar::null(DType::Primitive(PType::I16, Nullability::Nullable)),
            17,
        )
        .into_array(),
        ConstantArray::new(5_u8, 0).into_array(),
    ] {
        check(&array, None, &["sum"], None);
        check(
            &array,
            None,
            &["sum", "avg", "min", "max", "count_distinct", "count"],
            Some(1),
        );
    }
}

#[test]
fn encoded_numeric_reduction_sliced_runs_clip_boundaries_and_keep_row_order() {
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let runs = RunEnd::try_new(
        PrimitiveArray::new(vec![2_u64, 7, 9, 20], Validity::NonNullable).into_array(),
        PrimitiveArray::from_option_iter([Some(1_i32), Some(9), None, Some(-5)]).into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    for range in [1..12, 3..7, 19..20] {
        let sliced = runs.slice(range).unwrap();
        assert!(sliced.is::<RunEnd>() || sliced.is::<Constant>() || sliced.is::<Slice>());
        check(
            &sliced,
            None,
            &["sum", "avg", "count", "min", "max", "count_distinct"],
            None,
        );
        if sliced.len() > 1 {
            check(
                &sliced,
                Some(&[sliced.len() - 1, 0, 0]),
                &["sum", "avg", "count_distinct"],
                None,
            );
        }
    }
}

#[test]
fn encoded_numeric_reduction_complete_multi_column_states_include_count_all() {
    let columns = vec!["right".to_owned(), "left".to_owned()];
    let chunk = StructArray::try_new(
        vortex::array::dtype::FieldNames::from(["left", "right"]),
        vec![
            ConstantArray::new(7_i16, 31).into_array(),
            ConstantArray::new(-2_i64, 31).into_array(),
        ],
        31,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let request = VortexSimpleAggregateRequest::new(vec![
        VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("left").unwrap()),
            "total".into(),
        ),
        VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new("right").unwrap()),
            "minimum".into(),
        ),
        VortexSimpleAggregateMeasure::new("count", None, "rows".into()),
    ]);
    let mut actual = SimpleAggregateStates::new(&request, &columns).unwrap();
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let mut work = NativeNumericAccessorWork::default();
    assert!(
        actual
            .update_direct_from_chunk(&chunk, &columns, Some(&[30, 0, 30]), &mut work, &mut ctx)
            .unwrap()
    );
    let values = actual
        .states
        .iter()
        .map(|state| state.result_json().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            serde_json::json!(21.0),
            serde_json::json!(-2),
            serde_json::json!(3)
        ]
    );
    assert_eq!(work.encoded_reduction.constant_arrays, 2);
    assert_eq!(work.encoded_reduction.logical_rows, 6);
    assert_eq!(work.encoded_reduction.child_primitive_executions, 0);
}

#[test]
fn encoded_numeric_reduction_avoids_large_logical_expansion() {
    let columns = vec!["value".to_owned()];
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let array = RunEnd::try_new(
        PrimitiveArray::new(vec![50_000_000_u64, 100_000_000], Validity::NonNullable).into_array(),
        PrimitiveArray::new(vec![u64::MAX, 7], Validity::NonNullable).into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    let mut actual = states(&["count", "count_distinct", "min", "max"], &columns);
    let mut work = NativeNumericAccessorWork::default();
    assert!(update(&mut actual, &array, &columns, None, &mut work, &mut ctx).unwrap());
    assert_eq!(actual.states[0].count, 100_000_000);
    assert_eq!(actual.states[1].distinct_values.len(), 2);
    assert_eq!(actual.states[2].result_json().unwrap(), 7);
    assert_eq!(
        actual.states[3].result_json().unwrap(),
        serde_json::json!(u64::MAX)
    );
    assert_eq!(work.encoded_reduction.logical_rows, 100_000_000);
    assert_eq!(work.encoded_reduction.child_rows, 4);
    assert_eq!(work.encoded_reduction.max_child_rows, 2);
    assert_eq!(work.encoded_reduction.weighted_value_visits, 2);
}

#[test]
fn encoded_numeric_reduction_rejects_bad_selection_and_propagates_numeric_errors() {
    let columns = vec!["value".to_owned()];
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let array = ConstantArray::new(1_u8, 3).into_array();
    let mut actual = states(&["count"], &columns);
    assert!(
        update(
            &mut actual,
            &array,
            &columns,
            Some(&[3]),
            &mut NativeNumericAccessorWork::default(),
            &mut ctx
        )
        .is_err()
    );
    assert_eq!(actual.states[0].count, 0);
    actual.states[0].count = u64::MAX - 1;
    assert!(
        update(
            &mut actual,
            &array,
            &columns,
            None,
            &mut NativeNumericAccessorWork::default(),
            &mut ctx
        )
        .unwrap_err()
        .to_string()
        .contains("overflowed u64")
    );
    for value in [f64::INFINITY, f64::NAN, f64::MAX] {
        let array = ConstantArray::new(value, 3).into_array();
        let mut actual = states(&["sum"], &columns);
        assert!(
            update(
                &mut actual,
                &array,
                &columns,
                None,
                &mut NativeNumericAccessorWork::default(),
                &mut ctx
            )
            .unwrap_err()
            .to_string()
            .contains("non-finite")
        );
    }
}

#[test]
fn encoded_numeric_reduction_unadmitted_shapes_leave_state_and_work_unchanged() {
    let columns = vec!["value".to_owned()];
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let array = PrimitiveArray::new(vec![1_u64, 2, 3], Validity::NonNullable).into_array();
    let mut actual = states(&["sum"], &columns);
    let mut work = NativeNumericAccessorWork::default();
    assert!(!update(&mut actual, &array, &columns, None, &mut work, &mut ctx).unwrap());
    actual.states[0].value_transform = AggregateValueTransform::Length;
    let array = ConstantArray::new(2_u64, 3).into_array();
    assert!(!update(&mut actual, &array, &columns, None, &mut work, &mut ctx).unwrap());
    assert_eq!(actual.states[0].count, 0);
    assert_eq!(work.encoded_reduction.calls, 0);
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
#[allow(clippy::too_many_lines)] // One complete persisted-source execution contract.
fn encoded_numeric_reduction_public_native_file_preserves_complete_values() {
    use super::super::{
        VortexLocalPrimitiveExecutionPolicy, execute_vortex_local_primitive_with_policy,
        native_flat_layout::SequentialNativeFlatLayout,
    };
    use crate::VortexQueryPrimitiveRequest;
    use shardloom_core::DatasetUri;
    use vortex::{
        VortexSessionDefault as _,
        file::WriteOptionsSessionExt as _,
        io::{
            runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
            session::RuntimeSessionExt as _,
        },
        session::VortexSession,
    };
    struct FileGuard(std::path::PathBuf);
    impl Drop for FileGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let path = FileGuard(std::env::temp_dir().join(format!(
        "shardloom-encoded-reductions-{}-{}.vortex", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
    )));
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut ctx = session.create_execution_ctx();
    let values = RunEnd::try_new(
        PrimitiveArray::new(vec![4_u32, 12, 32], Validity::NonNullable).into_array(),
        PrimitiveArray::from_option_iter([Some(7_i64), None, Some(-2)]).into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    let chunk = StructArray::try_new(
        vortex::array::dtype::FieldNames::from(["renamed"]),
        vec![values],
        32,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path.0)
        .unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(SequentialNativeFlatLayout::strategy(1))
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, chunk.dtype().clone());
    writer.push(chunk).unwrap();
    assert_eq!(writer.finish().unwrap().row_count(), 32);
    drop(output);
    let mut measures = ["sum", "avg", "min", "max", "count", "count_distinct"]
        .into_iter()
        .map(|function| {
            VortexSimpleAggregateMeasure::new(
                function,
                Some(ColumnRef::new("renamed").unwrap()),
                function.to_owned(),
            )
        })
        .collect::<Vec<_>>();
    measures.push(VortexSimpleAggregateMeasure::new(
        "count",
        None,
        "rows".into(),
    ));
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.0.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::new(measures),
    );
    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
    )
    .unwrap();
    assert!(!report.fallback_execution_allowed);
    let (_, json) = report
        .result_summary
        .as_deref()
        .unwrap()
        .rsplit_once(" values=")
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(json).unwrap();
    assert_eq!(
        payload["values"],
        serde_json::json!({"sum":-12.0,"avg":-0.5,"min":-2,"max":7,"count":24,"count_distinct":2,"rows":32})
    );
    assert!(
        payload["aggregate_encoded_numeric_reduction"]["work"]["calls"]
            .as_u64()
            .is_some_and(|calls| calls > 0)
    );
    assert_eq!(
        payload["aggregate_native_numeric_accessor"]["native_decode_calls"],
        0
    );
}

#[test]
#[ignore = "manual release-mode scalar-consumer benchmark; emits paired complete-value evidence"]
fn encoded_numeric_reduction_measured_native_scalar_consumers() {
    let columns = vec!["measure".to_owned()];
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let rows = 8_000_000_u32;
    let array = RunEnd::try_new(
        PrimitiveArray::new(vec![rows / 2, rows], Validity::NonNullable).into_array(),
        PrimitiveArray::new(vec![i64::MIN, i64::MAX], Validity::NonNullable).into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    let mut trials = Vec::new();
    for trial in 0..7 {
        let mut pair = [0_u128; 2];
        let mut output = None;
        for encoded in if trial % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let mut states = states(&["count", "count_distinct", "min", "max"], &columns);
            let mut work = NativeNumericAccessorWork::default();
            let started = Instant::now();
            let input = if encoded {
                array.clone()
            } else {
                array
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)
                    .unwrap()
                    .into_array()
            };
            assert!(
                states
                    .update_direct_from_chunk(&input, &columns, None, &mut work, &mut ctx)
                    .unwrap()
            );
            pair[usize::from(encoded)] = started.elapsed().as_nanos();
            let values = states
                .states
                .iter()
                .map(|state| state.result_json().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                values,
                vec![
                    serde_json::json!(rows),
                    serde_json::json!(2),
                    serde_json::json!(i64::MIN),
                    serde_json::json!(i64::MAX)
                ]
            );
            if let Some(reference) = &output {
                assert_eq!(&values, reference);
            } else {
                output = Some(values);
            }
            if encoded {
                assert_eq!(work.encoded_reduction.child_rows, 4);
            }
        }
        trials.push(serde_json::json!({"trial":trial,"expanded_native_nanos":pair[0],"encoded_native_nanos":pair[1],"complete_values":output}));
    }
    println!(
        "{}",
        serde_json::json!({"rows":rows,"logical_width_bytes":8,"run_values":2,"trials":trials,"scope":"native_scalar_consumer_plus_array_canonicalization_only;alternating_order;not_file_or_process_time;no_zero_decode_or_RSS_claim"})
    );
}
