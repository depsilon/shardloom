use super::*;
use vortex::array::dtype::{FieldNames, Nullability, StructFields};

fn physical(element: DType, rank: u32, flags: u8) -> DType {
    DType::Struct(
        StructFields::new(
            FieldNames::from(["data", "shape"].as_slice()),
            vec![
                DType::List(element.into(), (flags & 2 != 0).into()),
                DType::FixedSizeList(
                    DType::Primitive(PType::I32, (flags & 4 != 0).into()).into(),
                    rank,
                    (flags & 8 != 0).into(),
                ),
            ],
        ),
        (flags & 1 != 0).into(),
    )
}

#[test]
fn variable_tensor_metadata_preserves_layout_and_optional_presence() {
    let metadata = Metadata::parse(
        r#" {"uniform_shape":[2,null,4],"dim_names":["Ω","","Ω"],"permutation":[2,0,1]} "#,
    )
    .unwrap();
    assert_eq!(
        metadata.as_str(),
        r#"{"dim_names":["Ω","","Ω"],"permutation":[2,0,1],"uniform_shape":[2,null,4]}"#
    );
    metadata.validate_rank(3).unwrap();
    assert!(metadata.validate_rank(2).is_err());
    assert_eq!(
        VariableShapeTensor::default()
            .deserialize_metadata(metadata.as_str().as_bytes())
            .unwrap(),
        metadata
    );
    assert_eq!(Metadata::parse("").unwrap(), Metadata::parse("{}").unwrap());
    assert_ne!(
        Metadata::parse("{}").unwrap(),
        Metadata::parse(r#"{"dim_names":[],"permutation":[],"uniform_shape":[]}"#).unwrap()
    );
}

#[test]
fn variable_tensor_metadata_refuses_unknown_duplicate_or_invalid_arrays() {
    for raw in [
        "[]",
        "null",
        r#"{"ndim":2}"#,
        r#"{"shape":[2]}"#,
        r#"{"uniform_shape":null}"#,
        r#"{"uniform_shape":[-1]}"#,
        r#"{"uniform_shape":[2147483648]}"#,
        r#"{"uniform_shape":[true]}"#,
        r#"{"uniform_shape":[2.0]}"#,
        r#"{"uniform_shape":[null],"uniform_shape":[null]}"#,
        r#"{"dim_names":[1]}"#,
        r#"{"dim_names":["x"],"permutation":[]}"#,
        r#"{"permutation":[0,0]}"#,
        r#"{"permutation":[2,0]}"#,
        r#"{"permutations":[0]}"#,
        "{} true",
    ] {
        assert!(Metadata::parse(raw).is_err(), "{raw}");
    }
    for raw in [b"{ }".as_slice(), b"", &[255]] {
        assert!(
            VariableShapeTensor::default()
                .deserialize_metadata(raw)
                .is_err()
        );
    }
}

#[test]
fn variable_tensor_shape_products_validate_every_dimension_and_uniform_constraint() {
    let metadata = Metadata::parse("{}").unwrap();
    for (shape, elements) in [
        (vec![], 1),
        (vec![2, 3, 4], 24),
        (vec![0, 3], 0),
        (vec![i32::MAX, i32::MAX, i32::MAX, 0], 0),
    ] {
        metadata
            .validate_dimensions(shape.len(), shape.into_iter().map(Ok), elements)
            .unwrap();
    }
    for (rank, shape, elements) in [
        (0, vec![], 0),
        (2, vec![2, 3], 5),
        (1, vec![0], 1),
        (2, vec![0, -1], 0),
        (1, vec![2, 3], 6),
        (2, vec![2], 2),
        (3, vec![i32::MAX; 3], 0),
    ] {
        assert!(
            metadata
                .validate_dimensions(rank, shape.into_iter().map(Ok), elements)
                .is_err()
        );
    }
    assert!(
        metadata
            .validate_dimensions(2, [Ok(0), Err(vortex_err!("null dimension"))], 0)
            .is_err()
    );
    let uniform = Metadata::parse(r#"{"uniform_shape":[null,2]}"#).unwrap();
    uniform.validate_dimensions(2, [Ok(0), Ok(2)], 0).unwrap();
    assert!(uniform.validate_dimensions(2, [Ok(0), Ok(3)], 0).is_err());
    uniform.validate_dimensions(2, [Ok(3), Ok(2)], 6).unwrap();
}

#[test]
fn variable_tensor_storage_preserves_all_five_nullability_flags() {
    for flags in 0..32 {
        let component = Nullability::from(flags & 16 != 0);
        let mut elements = vec![DType::Bool(component)];
        elements.extend(
            [
                PType::I8,
                PType::I16,
                PType::I32,
                PType::I64,
                PType::U8,
                PType::U16,
                PType::U32,
                PType::U64,
                PType::F32,
                PType::F64,
                PType::F16,
            ]
            .map(|p| DType::Primitive(p, component)),
        );
        for element in elements {
            for rank in [0, 3, u32::MAX] {
                let storage = physical(element.clone(), rank, flags);
                let dtype = DType::Extension(
                    ExtDType::<VariableShapeTensor>::try_new(
                        Metadata::parse("{}").unwrap(),
                        storage.clone(),
                    )
                    .unwrap()
                    .erased(),
                );
                assert_eq!(super::storage(&dtype), Some(&storage));
                assert_eq!(dtype.nullability(), storage.nullability());
                assert!(!crate::native_payload_schema::admitted_scalar(&dtype));
                assert!(crate::native_payload_schema::metadata_bytes(&dtype).is_ok());
            }
        }
    }
    for element in [
        DType::Utf8(false.into()),
        DType::Binary(false.into()),
        DType::Null,
    ] {
        assert!(
            ExtDType::<VariableShapeTensor>::try_new(
                Metadata::parse("{}").unwrap(),
                physical(element, 2, 0)
            )
            .is_err()
        );
    }
    assert!(
        ExtDType::<VariableShapeTensor>::try_new(
            Metadata::parse(r#"{"permutation":[0]}"#).unwrap(),
            physical(DType::Bool(false.into()), 2, 0)
        )
        .is_err()
    );
}

#[test]
fn variable_tensor_metadata_reservations_follow_clones_and_reject_without_leaks() {
    use shardloom_exec::live_memory::LiveMemoryPool;
    let memory = LiveMemoryPool::new(64 << 10).unwrap();
    let provider = VariableShapeTensor {
        allocator: Some(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
            memory.clone(),
        ))),
    };
    let metadata = provider
        .deserialize_metadata(br#"{"uniform_shape":[null,2]}"#)
        .unwrap();
    let retained = memory.snapshot().reserved_bytes;
    assert!(retained >= 256);
    let clone = metadata.clone();
    drop(metadata);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(clone);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for raw in [
        br#"{"uniform_shape":[-1]}"#.to_vec(),
        format!(r#"{{"dim_names":["{}"]}}"#, "x".repeat(8192)).into_bytes(),
    ] {
        assert!(provider.deserialize_metadata(&raw).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn variable_tensor_arrow_schema_preserves_flags_and_refuses_rank_overflow() {
    for flags in 0..32 {
        let storage = physical(
            DType::Primitive(PType::F64, (flags & 16 != 0).into()),
            3,
            flags,
        );
        let dtype = DType::Extension(
            ExtDType::<VariableShapeTensor>::try_new(
                Metadata::parse(r#"{"permutation":[2,0,1],"uniform_shape":[null,2,null]}"#)
                    .unwrap(),
                storage,
            )
            .unwrap()
            .erased(),
        );
        let field = super::arrow::field("tensor", &dtype).unwrap();
        assert_eq!(super::arrow::dtype(&field).unwrap(), dtype);
    }
    let wide = DType::Extension(
        ExtDType::<VariableShapeTensor>::try_new(
            Metadata::parse("{}").unwrap(),
            physical(DType::Bool(false.into()), u32::MAX, 0),
        )
        .unwrap()
        .erased(),
    );
    assert!(
        super::arrow::field("tensor", &wide)
            .unwrap_err()
            .to_string()
            .contains("signed 32-bit")
    );
}
