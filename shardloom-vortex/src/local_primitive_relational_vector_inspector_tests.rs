use super::*;
use vortex::array::arrays::{BoolArray, ExtensionArray, FixedSizeListArray};

#[test]
fn native_vector_inspectors_read_dimensions_without_observing_unused_values() {
    use crate::native_vector::{Kind as VectorKind, Metadata};
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let sparse = |size: u64, uniform: u64| {
        let storage = batch(
            &[
                ("size", PrimitiveArray::from_iter([size]).into_array()),
                (
                    "indices",
                    list(
                        PrimitiveArray::from_iter([u64::MAX]).into_array(),
                        &[(0, 1)],
                        Validity::NonNullable,
                    ),
                ),
                (
                    "values",
                    list(
                        PrimitiveArray::from_iter([f64::NAN]).into_array(),
                        &[(0, 1)],
                        Validity::NonNullable,
                    ),
                ),
            ],
            1,
        );
        let metadata = Metadata::parse(
            VectorKind::Sparse,
            &format!(r#"{{"version":1,"index_policy":"preserve","uniform_size":{uniform}}}"#),
        )
        .unwrap();
        let DType::Extension(dtype) =
            crate::native_vector::dtype(metadata, storage.dtype().clone()).unwrap()
        else {
            panic!("extension")
        };
        ExtensionArray::new(dtype, storage).into_array()
    };
    let quantized = {
        let storage = batch(
            &[
                (
                    "codes",
                    list(
                        PrimitiveArray::from_iter([1i8, 2, 3]).into_array(),
                        &[(0, 3)],
                        Validity::NonNullable,
                    ),
                ),
                ("scale", PrimitiveArray::from_iter([f64::NAN]).into_array()),
                ("zero_point", PrimitiveArray::from_iter([0i8]).into_array()),
            ],
            1,
        );
        let metadata = Metadata::parse(
            VectorKind::Quantized,
            r#"{"version":1,"parameter_mode":"per_vector","uniform_size":3}"#,
        )
        .unwrap();
        let DType::Extension(dtype) =
            crate::native_vector::dtype(metadata, storage.dtype().clone()).unwrap()
        else {
            panic!("extension")
        };
        ExtensionArray::new(dtype, storage).into_array()
    };
    let input = batch(&[("sparse", sparse(7, 7)), ("quantized", quantized)], 1);
    for (name, expected) in [("sparse", 7.0), ("quantized", 3.0)] {
        let output =
            evaluate(&session, &call("vector_dimension", vec![col(name)]), &input).unwrap();
        assert_eq!(numbers(&output), vec![Some(expected)]);
        assert!(evaluate(&session, &call("vector_norm", vec![col(name)]), &input).is_err());
    }
    let invalid = batch(&[("sparse", sparse(7, 8))], 1);
    assert!(
        evaluate(
            &session,
            &call("vector_dimension", vec![col("sparse")]),
            &invalid
        )
        .unwrap_err()
        .to_string()
        .contains("uniform_size")
    );
    let dense = list(
        PrimitiveArray::from_iter([f64::INFINITY]).into_array(),
        &[(0, 1), (0, 0), (0, 1)],
        Validity::from_iter([true, true, false]),
    );
    let input = batch(&[("v", dense)], 3);
    assert_eq!(
        numbers(&evaluate(&session, &call("vector_dimension", vec![col("v")]), &input).unwrap()),
        vec![Some(1.0), Some(0.0), None]
    );
}

#[test]
fn native_tensor_shape_keeps_order_rank_zero_and_nonfloat_components() {
    use crate::{native_tensor as fixed, native_variable_tensor as variable};
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let fixed_storage = FixedSizeListArray::try_new(
        BoolArray::from_iter([true; 12]).into_array(),
        6,
        Validity::from_iter([true, false]),
        2,
    )
    .unwrap()
    .into_array();
    let fixed_dtype = ExtDType::<fixed::FixedShapeTensor>::try_new(
        fixed::Metadata::parse(r#"{"shape":[2,3],"permutation":[1,0]}"#).unwrap(),
        fixed_storage.dtype().clone(),
    )
    .unwrap()
    .erased();
    let fixed = ExtensionArray::new(fixed_dtype, fixed_storage).into_array();
    let variable_storage = batch(
        &[
            (
                "data",
                list(
                    PrimitiveArray::from_iter([1i16; 6]).into_array(),
                    &[(0, 6), (0, 0)],
                    Validity::NonNullable,
                ),
            ),
            (
                "shape",
                FixedSizeListArray::try_new(
                    PrimitiveArray::from_iter([2i32, 3, 0, 3]).into_array(),
                    2,
                    Validity::NonNullable,
                    2,
                )
                .unwrap()
                .into_array(),
            ),
        ],
        2,
    );
    let variable_dtype = ExtDType::<variable::VariableShapeTensor>::try_new(
        variable::Metadata::parse(r#"{"permutation":[1,0],"uniform_shape":[null,3]}"#).unwrap(),
        variable_storage.dtype().clone(),
    )
    .unwrap()
    .erased();
    let variable = ExtensionArray::new(variable_dtype, variable_storage).into_array();
    let input = batch(&[("fixed", fixed), ("variable", variable)], 2);
    for (name, expected) in [
        ("fixed", vec![Some(vec![Some(3.0), Some(2.0)]), None]),
        (
            "variable",
            vec![
                Some(vec![Some(3.0), Some(2.0)]),
                Some(vec![Some(3.0), Some(0.0)]),
            ],
        ),
    ] {
        assert_eq!(
            list_numbers(
                &evaluate(&session, &call("tensor_shape", vec![col(name)]), &input).unwrap()
            ),
            expected
        );
        let physical = evaluate(
            &session,
            &call(
                "tensor_shape",
                vec![col(name), lit(ScalarValue::Utf8("physical".into()))],
            ),
            &input,
        )
        .unwrap();
        let mut expected = expected;
        for dimensions in expected.iter_mut().flatten() {
            dimensions.reverse();
        }
        assert_eq!(list_numbers(&physical), expected);
        assert!(evaluate(&session, &call("vector_norm", vec![col(name)]), &input).is_err());
    }
}

#[test]
fn native_tensor_shape_rank_zero_is_an_empty_fixed_list() {
    use crate::native_tensor as fixed;
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let storage = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter([42i32]).into_array(),
        1,
        Validity::NonNullable,
        1,
    )
    .unwrap()
    .into_array();
    let dtype = ExtDType::<fixed::FixedShapeTensor>::try_new(
        fixed::Metadata::parse(r#"{"shape":[]}"#).unwrap(),
        storage.dtype().clone(),
    )
    .unwrap()
    .erased();
    let input = batch(
        &[("v", ExtensionArray::new(dtype, storage).into_array())],
        1,
    );
    assert_eq!(
        list_numbers(&evaluate(&session, &call("tensor_shape", vec![col("v")]), &input).unwrap()),
        vec![Some(vec![])]
    );
}

#[test]
fn native_vector_index_extremes_null_controls_and_fixed_normalization_are_exact() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let vectors = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter([3.0f64, 4.0].repeat(7)).into_array(),
        2,
        Validity::from_iter([true, true, true, true, true, true, false]),
        7,
    )
    .unwrap()
    .into_array();
    let input = batch(
        &[
            ("v", vectors),
            (
                "i",
                PrimitiveArray::from_option_iter([
                    Some(i64::MIN),
                    Some(-2),
                    Some(-1),
                    Some(2),
                    Some(i64::MAX),
                    None,
                    Some(0),
                ])
                .into_array(),
            ),
        ],
        7,
    );
    assert_eq!(
        numbers(
            &evaluate(
                &session,
                &call("vector_element", vec![col("v"), col("i")]),
                &input
            )
            .unwrap()
        ),
        vec![None, Some(3.0), Some(4.0), None, None, None, None]
    );
    let slices = evaluate(
        &session,
        &call(
            "vector_slice",
            vec![col("v"), col("i"), lit(ScalarValue::UInt64(u64::MAX))],
        ),
        &input,
    )
    .unwrap();
    assert_eq!(
        list_numbers(&slices),
        vec![
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(4.0)]),
            Some(vec![]),
            Some(vec![]),
            None,
            None
        ]
    );
    let normalized = evaluate(&session, &call("vector_normalize", vec![col("v")]), &input).unwrap();
    assert!(
        matches!(normalized.dtype(),DType::FixedSizeList(child,2,Nullability::Nullable) if child.as_ref()==&DType::Primitive(PType::F64,Nullability::NonNullable))
    );
    assert_eq!(list_numbers(&normalized)[6], None);
    assert_eq!(
        list_numbers(&normalized)[0],
        Some(vec![Some(0.6), Some(0.8)])
    );
    let input = batch(&[], 2);
    for literal in [list_literal(&[]), list_literal(&[None, None])] {
        assert!(
            evaluate(
                &session,
                &call("vector_dimension", vec![literal.clone()]),
                &input
            )
            .is_ok()
        );
        assert_eq!(
            list_numbers(
                &evaluate(&session, &call("vector_normalize", vec![literal]), &input).unwrap()
            ),
            vec![None, None]
        );
    }
}

#[test]
fn native_vector_empty_output_and_child_buffers_keep_credit_after_all_producers_drop() {
    for rows in [0, 2] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let result = evaluate(
            &session,
            &call("vector_normalize", vec![list_literal(&[])]),
            &batch(&[], rows),
        )
        .unwrap();
        let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
        let column =
            crate::local_primitives::native_list::Column::new(result.clone(), &mut execution)
                .unwrap();
        let child = column
            .elements
            .clone()
            .execute::<PrimitiveArray>(&mut execution)
            .unwrap();
        let bytes = child.to_buffer::<f64>();
        let retained = column.elements.clone();
        drop((column, child, result, session, execution));
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(retained);
        assert!(
            memory.snapshot().reserved_bytes > 0,
            "empty bare buffer must retain its credit"
        );
        drop(bytes);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
