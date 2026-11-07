use super::*;
use vortex::array::dtype::Nullability;

#[test]
fn fixed_tensor_metadata_preserves_physical_shape_order_and_optional_presence() {
    let metadata = Metadata::parse(
        r#" { "shape": [2,3,4], "permutation": [2,0,1], "dim_names": ["Ω","", "Ω"] } "#,
    )
    .unwrap();
    assert_eq!(metadata.elements, 24);
    assert_eq!(
        metadata.as_str(),
        r#"{"dim_names":["Ω","","Ω"],"permutation":[2,0,1],"shape":[2,3,4]}"#
    );
    assert_eq!(
        FixedShapeTensor::default()
            .deserialize_metadata(metadata.as_str().as_bytes())
            .unwrap(),
        metadata
    );
    let scalar = Metadata::parse(r#"{"shape":[]}"#).unwrap();
    assert_eq!(scalar.elements, 1);
    assert_ne!(
        scalar,
        Metadata::parse(r#"{"dim_names":[],"permutation":[],"shape":[]}"#).unwrap()
    );
    for shape in [
        "[0]",
        "[9223372036854775807,9223372036854775807,0]",
        "[0,3]",
    ] {
        assert_eq!(
            Metadata::parse(&format!(r#"{{"shape":{shape}}}"#))
                .unwrap()
                .elements,
            0
        );
    }
    assert_eq!(
        Metadata::parse(r#"{"shape":[4294967295]}"#)
            .unwrap()
            .elements,
        u32::MAX
    );
}

#[test]
fn fixed_tensor_metadata_rejects_malformed_shape_and_layout() {
    for raw in [
        "{}",
        "[]",
        r#"{"shape":null}"#,
        r#"{"shape":[true]}"#,
        r#"{"shape":[1.0]}"#,
        r#"{"shape":[-1]}"#,
        r#"{"shape":[9223372036854775808,0]}"#,
        r#"{"shape":[4294967296]}"#,
        r#"{"shape":[65536,65536]}"#,
        r#"{"shape":[1],"shape":[1]}"#,
        r#"{"shape":[1],"version":1}"#,
        r#"{"shape":[1],"dim_names":null}"#,
        r#"{"shape":[1],"dim_names":[]}"#,
        r#"{"shape":[1],"dim_names":[1]}"#,
        r#"{"shape":[1],"permutation":[]}"#,
        r#"{"shape":[1],"permutation":[1]}"#,
        r#"{"shape":[2,3],"permutation":[0,0]}"#,
        r#"{"shape":[],"permutation":[0]}"#,
        r#"{"shape":[],"permutation":null}"#,
        r#"{"shape":[]} true"#,
    ] {
        assert!(Metadata::parse(raw).is_err(), "{raw}");
    }
    assert!(
        FixedShapeTensor::default()
            .deserialize_metadata(b"{ \"shape\": [] }")
            .is_err()
    );
    assert!(
        FixedShapeTensor::default()
            .deserialize_metadata(&[0xff])
            .is_err()
    );
}

#[test]
fn fixed_tensor_storage_checks_element_width_shape_and_nullability() {
    for component in [Nullability::Nullable, Nullability::NonNullable] {
        for outer in [Nullability::Nullable, Nullability::NonNullable] {
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
                ]
                .map(|p| DType::Primitive(p, component)),
            );
            for child in elements {
                for (shape, size) in [("[]", 1), ("[0,2]", 0), ("[2,3,4]", 24)] {
                    let storage = DType::FixedSizeList(child.clone().into(), size, outer);
                    let metadata = Metadata::parse(&format!(r#"{{"shape":{shape}}}"#)).unwrap();
                    let dtype = DType::Extension(
                        ExtDType::<FixedShapeTensor>::try_new(metadata.clone(), storage.clone())
                            .unwrap()
                            .erased(),
                    );
                    assert_eq!(super::storage(&dtype), Some(&storage));
                    assert_eq!(dtype.nullability(), outer);
                    assert!(!crate::native_payload_schema::admitted_scalar(&dtype));
                    assert!(
                        ExtDType::<FixedShapeTensor>::try_new(
                            metadata,
                            DType::FixedSizeList(child.clone().into(), size + 1, outer)
                        )
                        .is_err()
                    );
                }
            }
        }
    }
    for child in [
        DType::Utf8(false.into()),
        DType::Primitive(PType::F16, false.into()),
        DType::Null,
    ] {
        assert!(
            ExtDType::<FixedShapeTensor>::try_new(
                Metadata::parse(r#"{"shape":[1]}"#).unwrap(),
                DType::FixedSizeList(child.into(), 1, false.into())
            )
            .is_err()
        );
    }
}

#[test]
fn fixed_tensor_metadata_reservation_survives_clones_and_rolls_back_errors() {
    use shardloom_exec::live_memory::LiveMemoryPool;
    let memory = LiveMemoryPool::new(64 << 10).unwrap();
    let provider = FixedShapeTensor {
        allocator: Some(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
            memory.clone(),
        ))),
    };
    let metadata = provider
        .deserialize_metadata(br#"{"shape":[2,3]}"#)
        .unwrap();
    let retained = memory.snapshot().reserved_bytes;
    assert!(retained > 0 && retained < 1024);
    let clone = metadata.clone();
    drop(metadata);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(clone);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for raw in [
        br#"{"shape":[-1]}"#.to_vec(),
        format!(r#"{{"dim_names":["{}"],"shape":[1]}}"#, "x".repeat(8192)).into_bytes(),
    ] {
        assert!(provider.deserialize_metadata(&raw).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
