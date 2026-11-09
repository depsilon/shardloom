use super::*;
use std::sync::Arc;
use vortex::array::dtype::{FieldNames, Nullability, StructFields};

fn parsed(kind: Kind, blocked: bool) -> Metadata {
    Metadata::parse(
        kind,
        match (kind, blocked) {
            (Kind::Sparse, _) => r#"{"version":1,"index_policy":"preserve"}"#,
            (Kind::Quantized, false) => r#"{"version":1,"parameter_mode":"per_vector"}"#,
            (Kind::Quantized, true) => r#"{"version":1,"parameter_mode":"blocked","block_size":2}"#,
        },
    )
    .unwrap()
}
fn physical(kind: Kind, integer: PType, real: PType, blocked: bool, flags: u8) -> DType {
    let null = |bit: u32| Nullability::from(flags & (1_u8 << bit) != 0_u8);
    let integer = DType::Primitive(integer, null(1));
    let real = DType::Primitive(real, null(2));
    let (names, fields) = match kind {
        Kind::Sparse => (
            vec!["size", "indices", "values"],
            vec![
                DType::Primitive(PType::U64, null(3)),
                DType::List(integer.into(), null(4)),
                DType::List(real.into(), null(5)),
            ],
        ),
        Kind::Quantized => {
            let code = DType::List(integer.clone().into(), null(3));
            let zero = integer.with_nullability(null(6));
            (
                vec!["codes", "scale", "zero_point"],
                if blocked {
                    vec![
                        code,
                        DType::List(real.into(), null(4)),
                        DType::List(zero.into(), null(5)),
                    ]
                } else {
                    vec![code, real, zero]
                },
            )
        }
    };
    DType::Struct(StructFields::new(FieldNames::from(names), fields), null(0))
}

#[test]
fn native_vector_metadata_is_versioned_canonical_and_preserves_identity_presence() {
    let raw =
        r#" {"version":1, "index_policy":"preserve", "space_id":"未知 Ω", "normalization":null} "#;
    let metadata = Metadata::parse(Kind::Sparse, raw).unwrap();
    assert_eq!(
        metadata.as_str(),
        r#"{"index_policy":"preserve","normalization":null,"space_id":"未知 Ω","version":1}"#
    );
    let provider = SparseVector::default();
    let bytes = provider.serialize_metadata(&metadata).unwrap();
    assert_eq!(provider.deserialize_metadata(&bytes).unwrap(), metadata);
    assert!(provider.deserialize_metadata(raw.as_bytes()).is_err());
    assert!(provider.deserialize_metadata(&[0xff]).is_err());
    assert!(
        QuantizedVector::default()
            .serialize_metadata(&metadata)
            .is_err()
    );
    for invalid in [
        "{}",
        r#"{"index_policy":"preserve"}"#,
        r#"{"version":2,"index_policy":"preserve"}"#,
        r#"{"version":1.0,"index_policy":"preserve"}"#,
        r#"{"version":true,"index_policy":"preserve"}"#,
        r#"{"version":1,"version":1,"index_policy":"preserve"}"#,
        r#"{"version":1,"index_policy":"sort"}"#,
        r#"{"version":1,"index_policy":"preserve","unknown":null}"#,
        r#"{"version":1,"index_policy":"preserve","uniform_size":null}"#,
        r#"{"version":1,"index_policy":"preserve","uniform_size":-1}"#,
        r#"{"version":1,"index_policy":"preserve","uniform_size":1.0}"#,
        r#"{"version":1,"index_policy":"preserve","space_id":""}"#,
        r#"{"version":1,"index_policy":"preserve","space_id":[]}"#,
        r#"{"version":1,"index_policy":"preserve","block_size":2}"#,
    ] {
        assert!(Metadata::parse(Kind::Sparse, invalid).is_err(), "{invalid}");
    }
    for invalid in [
        r#"{"version":1}"#,
        r#"{"version":1,"parameter_mode":"per_vector","block_size":2}"#,
        r#"{"version":1,"parameter_mode":"blocked"}"#,
        r#"{"version":1,"parameter_mode":"blocked","block_size":0}"#,
        r#"{"version":1,"parameter_mode":"per_vector","index_policy":"preserve"}"#,
        r#"{"version":1,"parameter_mode":"per_vector","parameter_mode":"blocked"}"#,
    ] {
        assert!(
            Metadata::parse(Kind::Quantized, invalid).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn native_vector_sparse_relations_are_exact_without_dimension_sized_state() {
    let metadata = parsed(Kind::Sparse, false);
    let max = i128::from(u64::MAX);
    metadata
        .validate_sparse(u64::MAX, 4, 4, [max - 1, 0, max - 1, 1_i128 << 63].map(Ok))
        .unwrap();
    metadata.validate_sparse(0, 0, 0, []).unwrap();
    assert!(metadata.validate_sparse(4, 1, 1, [Ok(4)]).is_err());
    assert!(metadata.validate_sparse(4, 1, 1, [Ok(-1)]).is_err());
    assert!(metadata.validate_sparse(4, 1, 0, [Ok(0)]).is_err());
    assert!(metadata.validate_sparse(4, 2, 2, [Ok(0)]).is_err());
    let ordered = Metadata::parse(
        Kind::Sparse,
        r#"{"version":1,"index_policy":"strictly_increasing","uniform_size":8}"#,
    )
    .unwrap();
    ordered.validate_sparse(8, 3, 3, [0, 3, 7].map(Ok)).unwrap();
    for values in [[0, 0, 7], [0, 7, 3], [0, 3, 8]] {
        assert!(ordered.validate_sparse(8, 3, 3, values.map(Ok)).is_err());
    }
    assert!(ordered.validate_sparse(9, 0, 0, []).is_err());
    metadata
        .validate_json(
            &serde_json::json!({"size":u64::MAX,"indices":[u64::MAX-1,0],"values":[null,-0.0]}),
        )
        .unwrap();
    assert!(
        metadata
            .validate_json(&serde_json::json!({"size":4,"indices":[null],"values":[1]}))
            .is_err()
    );
}

#[test]
fn native_vector_quantized_modes_keep_empty_parameters_and_partial_blocks() {
    let scalar = parsed(Kind::Quantized, false);
    let blocked = parsed(Kind::Quantized, true);
    for (codes, expected) in [(0, 0), (1, 1), (2, 1), (3, 2), (5, 3)] {
        assert_eq!(scalar.parameter_count(codes).unwrap(), 1);
        assert_eq!(blocked.parameter_count(codes).unwrap(), expected);
    }
    let huge = Metadata::parse(
        Kind::Quantized,
        r#"{"version":1,"parameter_mode":"blocked","block_size":18446744073709551615}"#,
    )
    .unwrap();
    assert_eq!(huge.parameter_count(usize::MAX).unwrap(), 1);
    scalar
        .validate_json(&serde_json::json!({"codes":[],"scale":0.5,"zero_point":u64::MAX}))
        .unwrap();
    blocked
        .validate_json(&serde_json::json!({"codes":[],"scale":[],"zero_point":[]}))
        .unwrap();
    blocked
        .validate_json(
            &serde_json::json!({"codes":[0,null,2],"scale":[0.5,2.0],"zero_point":[0,1]}),
        )
        .unwrap();
    for bad in [
        serde_json::json!({"codes":[],"scale":[0.5],"zero_point":[0]}),
        serde_json::json!({"codes":[1],"scale":[null],"zero_point":[0]}),
        serde_json::json!({"codes":[1],"scale":[0.5],"zero_point":[null]}),
    ] {
        assert!(blocked.validate_json(&bad).is_err());
    }
    for value in [
        None,
        Some(0.0),
        Some(-0.0),
        Some(-1.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
    ] {
        assert!(Metadata::validate_scale(value).is_err());
    }
    Metadata::validate_scale(Some(f64::MIN_POSITIVE)).unwrap();
}

#[test]
fn native_vector_storage_retains_every_primitive_width_and_nullability() {
    for integer in [
        PType::I8,
        PType::I16,
        PType::I32,
        PType::I64,
        PType::U8,
        PType::U16,
        PType::U32,
        PType::U64,
    ] {
        for real in [PType::F16, PType::F32, PType::F64] {
            for (kind, blocked) in [
                (Kind::Sparse, false),
                (Kind::Quantized, false),
                (Kind::Quantized, true),
            ] {
                for flags in 0..128 {
                    let physical = physical(kind, integer, real, blocked, flags);
                    let dtype = dtype(parsed(kind, blocked), physical.clone()).unwrap();
                    assert_eq!(storage(&dtype), Some(&physical));
                    let memory =
                        shardloom_exec::live_memory::LiveMemoryPool::new(32 << 20).unwrap();
                    assert!(crate::native_payload_schema::metadata_bytes(&dtype, &memory).is_ok());
                    assert!(!crate::native_payload_schema::admitted_scalar(&dtype));
                    assert!(contains(&DType::List(dtype.clone().into(), true.into())));
                    #[cfg(feature = "universal-format-io")]
                    assert_eq!(
                        arrow::dtype(&arrow::field("v", &dtype).unwrap()).unwrap(),
                        dtype
                    );
                }
            }
        }
    }
    assert!(
        dtype(
            parsed(Kind::Sparse, false),
            physical(Kind::Sparse, PType::F32, PType::F64, false, 0)
        )
        .is_err()
    );
    assert!(
        dtype(
            parsed(Kind::Sparse, false),
            physical(Kind::Sparse, PType::U32, PType::U8, false, 0)
        )
        .is_err()
    );
    assert!(
        dtype(
            parsed(Kind::Quantized, true),
            physical(Kind::Quantized, PType::U8, PType::F32, false, 0)
        )
        .is_err()
    );
}

#[test]
fn native_vector_received_metadata_is_released_with_its_last_owner() {
    use shardloom_exec::live_memory::LiveMemoryPool;
    let memory = LiveMemoryPool::new(64 << 10).unwrap();
    let provider = SparseVector {
        allocator: Some(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
            memory.clone(),
        ))),
    };
    let metadata = provider
        .deserialize_metadata(br#"{"index_policy":"preserve","version":1}"#)
        .unwrap();
    let reserved = memory.snapshot().reserved_bytes;
    assert!(reserved >= 256);
    let retained = metadata.clone();
    drop(metadata);
    assert_eq!(memory.snapshot().reserved_bytes, reserved);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(
        provider
            .deserialize_metadata(br#"{"index_policy":"bad","version":1}"#)
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let denied = format!(
        r#"{{"index_policy":"preserve","space_id":"{}","version":1}}"#,
        "x".repeat(8192)
    );
    assert!(provider.deserialize_metadata(denied.as_bytes()).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
