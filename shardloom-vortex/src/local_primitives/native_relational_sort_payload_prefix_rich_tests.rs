//! Rich payload prefixes reuse native validation and retain independent owners.

use super::*;
use parquet_variant::{Variant as PqValue, VariantBuilder};
use vortex::{
    array::{
        arrays::{ExtensionArray, FixedSizeListArray, VarBinArray},
        dtype::extension::ExtDType,
    },
    encodings::{
        parquet_variant::ParquetVariant,
        uuid::{Uuid, UuidMetadata},
    },
};

fn fixed(values: ArrayRef, width: u32) -> ArrayRef {
    FixedSizeListArray::try_new(values, width, Validity::from_iter([true, false, true]), 3)
        .unwrap()
        .into_array()
}

fn variable_tensor(invalid: bool, masked: bool) -> ArrayRef {
    let data = list(
        PrimitiveArray::from_iter([10_i32, 20, 30, 40]).into_array(),
        &[0, 1, 3, 4],
        Validity::NonNullable,
    );
    let shape = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter([1_i32, 1, 1, 2, if invalid { -1 } else { 1 }, 1]).into_array(),
        2,
        Validity::NonNullable,
        3,
    )
    .unwrap()
    .into_array();
    let storage = StructArray::try_new(
        FieldNames::from(["data", "shape"]),
        [data, shape],
        3,
        Validity::from_iter([true, true, !masked]),
    )
    .unwrap()
    .into_array();
    let dtype = ExtDType::<crate::native_variable_tensor::VariableShapeTensor>::try_new(
        crate::native_variable_tensor::Metadata::parse("{}").unwrap(),
        storage.dtype().clone(),
    )
    .unwrap()
    .erased();
    ExtensionArray::new(dtype, storage).into_array()
}

fn vector(quantized: bool, invalid: bool) -> ArrayRef {
    use crate::native_vector::{Kind, Metadata};
    let (kind, metadata, storage) = if quantized {
        (
            Kind::Quantized,
            r#"{"version":1,"parameter_mode":"per_vector"}"#,
            record(vec![
                (
                    "codes",
                    list(
                        PrimitiveArray::from_iter([-2_i8, 3, 7]).into_array(),
                        &[0, 1, 2, 3],
                        Validity::NonNullable,
                    ),
                ),
                (
                    "scale",
                    PrimitiveArray::from_iter([0.5_f64, 1.0, if invalid { 0.0 } else { 2.0 }])
                        .into_array(),
                ),
                (
                    "zero_point",
                    PrimitiveArray::from_iter([0_i8, 1, -1]).into_array(),
                ),
            ]),
        )
    } else {
        (
            Kind::Sparse,
            r#"{"version":1,"index_policy":"preserve"}"#,
            record(vec![
                (
                    "size",
                    PrimitiveArray::from_iter([2_u64, 2, 2]).into_array(),
                ),
                (
                    "indices",
                    list(
                        PrimitiveArray::from_iter([0_u64, 1, if invalid { 4 } else { 0 }])
                            .into_array(),
                        &[0, 1, 2, 3],
                        Validity::NonNullable,
                    ),
                ),
                (
                    "values",
                    list(
                        PrimitiveArray::from_iter([1.0_f64, 2.0, 3.0]).into_array(),
                        &[0, 1, 2, 3],
                        Validity::NonNullable,
                    ),
                ),
            ]),
        )
    };
    let DType::Extension(dtype) = crate::native_vector::dtype(
        Metadata::parse(kind, metadata).unwrap(),
        storage.dtype().clone(),
    )
    .unwrap() else {
        panic!("extension")
    };
    ExtensionArray::new(dtype, storage).into_array()
}

fn variant(invalid: bool, masked: bool) -> ArrayRef {
    let (metadata, mut values): (Vec<_>, Vec<_>) = [
        PqValue::Int64(i64::MIN),
        PqValue::Null,
        PqValue::from("λ\0rich"),
    ]
    .iter()
    .map(|value| VariantBuilder::new().with_value(value.clone()).finish())
    .unzip();
    if invalid {
        values[2] = vec![255];
    }
    ParquetVariant::try_new(
        Validity::from_iter([true, true, !masked]),
        VarBinArray::from_vec(metadata, DType::Binary(Nullability::NonNullable)).into_array(),
        Some(VarBinArray::from_vec(values, DType::Binary(Nullability::NonNullable)).into_array()),
        None,
    )
    .unwrap()
    .into_array()
}

fn fixtures() -> Vec<ArrayRef> {
    let embedding = fixed(
        PrimitiveArray::from_iter([1.0_f32, 2.0, 3.0, 4.0, 5.0, 6.0]).into_array(),
        2,
    );
    let embedding_dtype = ExtDType::<crate::native_embedding::Embedding>::try_new(
        crate::native_embedding::Metadata::parse(r#"{"version":1,"space_id":"prefix"}"#).unwrap(),
        embedding.dtype().clone(),
    )
    .unwrap()
    .erased();
    let tensor = fixed(PrimitiveArray::from_iter(0_i16..12).into_array(), 4);
    let tensor_dtype = ExtDType::<crate::native_tensor::FixedShapeTensor>::try_new(
        crate::native_tensor::Metadata::parse(r#"{"shape":[2,2],"permutation":[1,0]}"#).unwrap(),
        tensor.dtype().clone(),
    )
    .unwrap()
    .erased();
    let uuid = fixed(
        PrimitiveArray::from_iter(
            [0_u128, 7, u128::MAX]
                .into_iter()
                .flat_map(u128::to_be_bytes),
        )
        .into_array(),
        16,
    );
    let uuid_dtype = ExtDType::try_with_vtable(Uuid, UuidMetadata::default(), uuid.dtype().clone())
        .unwrap()
        .erased();
    let arrays = vec![
        ExtensionArray::new(embedding_dtype, embedding).into_array(),
        ExtensionArray::new(tensor_dtype, tensor).into_array(),
        ExtensionArray::new(uuid_dtype, uuid).into_array(),
        variable_tensor(false, false),
        variable_tensor(true, true),
        vector(false, false),
        vector(true, false),
        variant(false, false),
        variant(true, true),
    ];
    #[cfg(feature = "native-binary-objects")]
    let arrays = {
        let mut arrays = arrays;
        let DType::Extension(dtype) = crate::native_binary_type::dtype(true).unwrap() else {
            panic!("extension")
        };
        // Movement preserves opaque descriptor bytes. Resolving a managed
        // object is a different, explicit boundary and must not occur here.
        arrays.push(
            ExtensionArray::new(
                dtype,
                VarBinViewArray::from_iter_nullable_bin([
                    Some(b"opaque descriptor one".as_slice()),
                    None,
                    Some(b"opaque descriptor three".as_slice()),
                ])
                .into_array(),
            )
            .into_array(),
        );
        arrays
    };
    arrays
}

#[test]
fn native_relational_sort_prefix_keeps_rich_types_values_masks_and_last_owner_credit() {
    for payload in fixtures() {
        let array = record(vec![
            ("key", PrimitiveArray::from_iter([2_i64, 0, 1]).into_array()),
            ("payload", payload.clone()),
        ]);
        let binding = spec(&array, &["key"]);
        for cap in [0, 1, 2, 3, 7] {
            let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let outputs = session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut sort = Sort::new(&binding, context.memory())?;
                    for range in [0..1, 1..3] {
                        sort.build(array.slice(range).map_err(vortex_error)?, context)?;
                    }
                    #[cfg(debug_assertions)]
                    let observation =
                        super::super::super::super::native_relational_observation::Session::start();
                    let mut outputs = Vec::new();
                    sort.finish_prefix(context, 2, Some(cap), false, &mut |batch| {
                        outputs.push(super::super::super::super::logical_field_from_native_array(
                            &batch, "payload",
                        )?);
                        Ok(())
                    })?;
                    #[cfg(debug_assertions)]
                    {
                        let counts = observation.finish();
                        assert_eq!(counts.sort_output_rows_constructed, cap.min(3) as u64);
                        assert_eq!(counts.sort_payload_rows_validated, 3);
                    }
                    Ok(outputs)
                })
                .unwrap();
            drop(session);
            if cap != 0 {
                assert!(memory.snapshot().reserved_bytes > 0);
            }
            // A new execution context reads the retained child after the
            // producing session, Sort, source table and record owners drop.
            let reader = ResidentVortexSession::new(4 << 20, 1).unwrap();
            reader
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut execution = context.native_session().create_execution_ctx();
                    let mut row = 0;
                    for output in &outputs {
                        assert_eq!(output.dtype(), payload.dtype());
                        for index in 0..output.len() {
                            assert_eq!(
                                output
                                    .execute_scalar(index, &mut execution)
                                    .map_err(vortex_error)?,
                                payload
                                    .execute_scalar([1, 2, 0][row], &mut execution)
                                    .map_err(vortex_error)?
                            );
                            row += 1;
                        }
                    }
                    assert_eq!(row, cap.min(3));
                    Ok(())
                })
                .unwrap();
            drop(outputs);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_relational_sort_prefix_keeps_discarded_extension_and_variant_errors() {
    for payload in [
        variable_tensor(true, false),
        vector(false, true),
        vector(true, true),
        variant(true, false),
    ] {
        let array = record(vec![
            ("key", PrimitiveArray::from_iter([0_i64, 1, 2]).into_array()),
            ("payload", payload),
        ]);
        let binding = spec(&array, &["key"]);
        for batch_rows in [1, 3] {
            let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let mut expected = None;
            for cap in [None, Some(0), Some(1), Some(2), Some(3)] {
                session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut sort = Sort::new(&binding, context.memory())?;
                        sort.build(array.clone(), context)?;
                        let error = sort
                            .finish_prefix(context, batch_rows, cap, false, &mut |_| Ok(()))
                            .unwrap_err()
                            .to_string();
                        assert_eq!(expected.get_or_insert(error.clone()), &error);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn native_relational_sort_prefix_accepts_empty_rich_inputs_without_output() {
    for payload in fixtures() {
        let array = record(vec![
            ("key", PrimitiveArray::from_iter([2_i64, 0, 1]).into_array()),
            ("payload", payload),
        ]);
        let binding = spec(&array, &["key"]);
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        let memory = session.memory().clone();
        for cap in [0, 1, 7] {
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut sort = Sort::new(&binding, context.memory())?;
                    // Vortex's generic empty Variant slice is not admitted;
                    // empty sources use the existing native payload builder.
                    let empty = super::super::super::super::native_payload::empty_record(
                        array.dtype(),
                        context,
                    )?;
                    sort.build(empty, context)?;
                    sort.finish_prefix(context, 2, Some(cap), false, &mut |_| {
                        panic!("an empty sort must not construct an output batch")
                    })
                })
                .unwrap();
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}
