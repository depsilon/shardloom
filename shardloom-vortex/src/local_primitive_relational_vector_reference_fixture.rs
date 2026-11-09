//! Native raw-storage construction for the preimplementation semantic corpus.

use super::*;

pub(super) fn array(value: &Json) -> ArrayRef {
    match value["kind"].as_str().unwrap() {
        "dense_storage" => dense(value),
        "sparse_storage" | "quantized_storage" => vector(value),
        "fixed_tensor_storage" => fixed(value),
        "variable_tensor_storage" => variable(value),
        kind => panic!("unknown frozen input {kind}"),
    }
}

fn dense(value: &Json) -> ArrayRef {
    let values = value["values"].as_array().cloned().unwrap_or_default();
    let bits = values
        .iter()
        .map(|v| v.as_str().map(|s| u64::from_str_radix(s, 16).unwrap()))
        .collect::<Vec<_>>();
    let child = match value["component"].as_str().unwrap() {
        "f16" => PrimitiveArray::from_option_iter(bits.iter().map(|v| {
            v.map(|v| vortex::array::dtype::half::f16::from_bits(u16::try_from(v).unwrap()))
        }))
        .into_array(),
        "f32" => PrimitiveArray::from_option_iter(
            bits.iter()
                .map(|v| v.map(|v| f32::from_bits(u32::try_from(v).unwrap()))),
        )
        .into_array(),
        "f64" => PrimitiveArray::from_option_iter(bits.iter().map(|v| v.map(f64::from_bits)))
            .into_array(),
        _ => panic!("unadmitted float width"),
    };
    let valid = Validity::from_iter([!value["values"].is_null()]);
    let storage = if let Some(size) = value["fixed"].as_u64() {
        FixedSizeListArray::try_new(child, u32::try_from(size).unwrap(), valid, 1)
            .unwrap()
            .into_array()
    } else {
        list(child, &[(0, values.len() as u64)], valid)
    };
    if value["embedding"].is_null() {
        storage
    } else {
        let dtype = ExtDType::<crate::native_embedding::Embedding>::try_new(
            crate::native_embedding::Metadata::parse(&value["embedding"].to_string()).unwrap(),
            storage.dtype().clone(),
        )
        .unwrap()
        .erased();
        ExtensionArray::new(dtype, storage).into_array()
    }
}

fn vector(value: &Json) -> ArrayRef {
    use crate::native_vector::{Kind, Metadata};
    let (kind, storage, mut metadata) = if value["kind"] == "sparse_storage" {
        (
            Kind::Sparse,
            batch(
                &[
                    (
                        "size",
                        PrimitiveArray::from_iter([value["size"].as_u64().unwrap()]).into_array(),
                    ),
                    (
                        "indices",
                        list(
                            PrimitiveArray::from_iter(Vec::<u64>::new()).into_array(),
                            &[(0, 0)],
                            Validity::NonNullable,
                        ),
                    ),
                    (
                        "values",
                        list(
                            PrimitiveArray::from_iter(Vec::<f64>::new()).into_array(),
                            &[(0, 0)],
                            Validity::NonNullable,
                        ),
                    ),
                ],
                1,
            ),
            json!({"version":1,"index_policy":"preserve"}),
        )
    } else {
        let codes = value["codes"].as_array().unwrap();
        (
            Kind::Quantized,
            batch(
                &[
                    (
                        "codes",
                        list(
                            PrimitiveArray::from_option_iter(
                                codes
                                    .iter()
                                    .map(|v| v.as_i64().map(|v| i8::try_from(v).unwrap())),
                            )
                            .into_array(),
                            &[(0, codes.len() as u64)],
                            Validity::NonNullable,
                        ),
                    ),
                    (
                        "scale",
                        PrimitiveArray::from_iter([value["scale"].as_f64().unwrap()]).into_array(),
                    ),
                    (
                        "zero_point",
                        PrimitiveArray::from_iter([i8::try_from(
                            value["zero_point"].as_i64().unwrap(),
                        )
                        .unwrap()])
                        .into_array(),
                    ),
                ],
                1,
            ),
            json!({"version":1,"parameter_mode":"per_vector"}),
        )
    };
    if let Some(size) = value.get("uniform_size") {
        metadata["uniform_size"] = size.clone();
    }
    let DType::Extension(dtype) = crate::native_vector::dtype(
        Metadata::parse(kind, &metadata.to_string()).unwrap(),
        storage.dtype().clone(),
    )
    .unwrap() else {
        panic!("extension");
    };
    ExtensionArray::new(dtype, storage).into_array()
}

fn fixed(value: &Json) -> ArrayRef {
    let dimensions = value["shape"].as_array().unwrap();
    let size = if dimensions.iter().any(|v| v.as_u64() == Some(0)) {
        0
    } else {
        dimensions
            .iter()
            .map(|v| v.as_u64().unwrap())
            .product::<u64>()
    };
    let size = u32::try_from(size).unwrap();
    let storage = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter(vec![7i16; size as usize]).into_array(),
        size,
        Validity::from_iter([value["present"] == true]),
        1,
    )
    .unwrap()
    .into_array();
    let mut metadata = json!({"shape":dimensions});
    if let Some(order) = value.get("permutation") {
        metadata["permutation"] = order.clone();
    }
    let dtype = ExtDType::<crate::native_tensor::FixedShapeTensor>::try_new(
        crate::native_tensor::Metadata::parse(&metadata.to_string()).unwrap(),
        storage.dtype().clone(),
    )
    .unwrap()
    .erased();
    ExtensionArray::new(dtype, storage).into_array()
}

fn variable(value: &Json) -> ArrayRef {
    let rank = u32::try_from(value["rank"].as_u64().unwrap()).unwrap();
    let shape = value["shape"].as_array().unwrap();
    let count = usize::try_from(value["data_length"].as_u64().unwrap()).unwrap();
    let child = PrimitiveArray::from_option_iter(
        shape
            .iter()
            .map(|v| v.as_i64().map(|v| i32::try_from(v).unwrap())),
    )
    .into_array();
    let shape = FixedSizeListArray::try_new(child, rank, Validity::NonNullable, 1)
        .unwrap()
        .into_array();
    let storage = StructArray::try_new(
        FieldNames::from(["data", "shape"]),
        [
            list(
                PrimitiveArray::from_iter(vec![false; count].into_iter().map(i8::from))
                    .into_array(),
                &[(0, count as u64)],
                Validity::NonNullable,
            ),
            shape,
        ],
        1,
        Validity::from_iter([value["present"] == true]),
    )
    .unwrap()
    .into_array();
    let mut metadata = json!({});
    for key in ["permutation", "uniform_shape"] {
        if let Some(v) = value.get(key) {
            metadata[key] = v.clone();
        }
    }
    let dtype = ExtDType::<crate::native_variable_tensor::VariableShapeTensor>::try_new(
        crate::native_variable_tensor::Metadata::parse(&metadata.to_string()).unwrap(),
        storage.dtype().clone(),
    )
    .unwrap()
    .erased();
    ExtensionArray::new(dtype, storage).into_array()
}
