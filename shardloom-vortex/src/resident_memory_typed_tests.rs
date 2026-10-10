use super::*;
use serde_json::{Value as Json, json};
use shardloom_exec::compute_pool::CancellationToken;

fn source(
    session: &ResidentVortexSession,
    dtype: &Json,
    values: &[Json],
) -> Result<ResidentMemorySource> {
    let dtype = dtype.to_string();
    let values = values.iter().map(Json::to_string).collect::<Vec<_>>();
    let borrowed = values
        .iter()
        .map(|value| Some(value.as_str()))
        .collect::<Vec<_>>();
    ResidentMemorySource::from_batch_columns(
        session,
        &[MemoryColumn {
            name: "v",
            values: MemoryColumnValues::TypedJson {
                dtype_json: &dtype,
                values: &borrowed,
            },
        }],
    )
}

fn rows(source: &ResidentMemorySource) -> Json {
    let result = source
        .prepare_projection(&["v"], None, None)
        .unwrap()
        .execute()
        .unwrap();
    serde_json::from_str(result.values_json.value()).unwrap()
}

#[test]
fn typed_scalars_preserve_exact_native_types_and_all_values() {
    let session = ResidentVortexSession::new(16 * 1024 * 1024, 1).unwrap();
    let cases = [
        (json!({"Primitive":["i8",true]}), json!(-128)),
        (json!({"Primitive":["i16",true]}), json!(-32768)),
        (json!({"Primitive":["i32",true]}), json!(i32::MIN)),
        (json!({"Primitive":["i64",true]}), json!(i64::MIN)),
        (json!({"Primitive":["u8",true]}), json!(u8::MAX)),
        (json!({"Primitive":["u16",true]}), json!(u16::MAX)),
        (json!({"Primitive":["u32",true]}), json!(u32::MAX)),
        (json!({"Primitive":["u64",true]}), json!(u64::MAX)),
        (json!({"Primitive":["f32",true]}), json!(-0.0)),
        (json!({"Primitive":["f64",true]}), json!(1.25)),
        (json!({"Bool":true}), json!(true)),
        (json!({"Utf8":true}), json!("λ\0猫")),
        (json!({"Binary":true}), json!("00ff41")),
        (
            json!({"Decimal":[{"precision":38,"scale":2},true]}),
            json!("decimal128(38,2):12345678901234567890123456789012345678"),
        ),
        (
            json!({"Extension":{"id":"vortex.date","metadata":[4],"storage_dtype":{"Primitive":["i32",true]}}}),
            json!(i32::MIN),
        ),
        (
            json!({"Extension":{"id":"vortex.timestamp","metadata":[1,0,0],"storage_dtype":{"Primitive":["i64",true]}}}),
            json!(i64::MAX),
        ),
    ];
    for (dtype, value) in cases {
        let input = source(&session, &dtype, &[value.clone(), Json::Null]).unwrap();
        assert_eq!(
            serde_json::to_value(input.dtype().as_struct_fields().field("v").unwrap()).unwrap(),
            dtype
        );
        assert_eq!(rows(&input), json!([{"v":value},{"v":null}]));
        drop(input);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn nested_null_parents_preserve_nonnullable_children_and_empty_shapes() {
    let session = ResidentVortexSession::new(16 * 1024 * 1024, 1).unwrap();
    let dtype = json!({"Struct":[{"names":["points","label"],"dtypes":[
        {"List":[{"FixedSizeList":[{"Primitive":["i16",false]},2,true]},true]},
        {"Utf8":false}]},true]});
    let values = vec![
        json!({"points":[[1,-2],null,[32767,-32768]],"label":"λ"}),
        Json::Null,
        json!({"points":[],"label":""}),
    ];
    let input = source(&session, &dtype, &values).unwrap();
    assert_eq!(input.intake_payload_bytes_copied(), 10);
    assert_eq!(
        rows(&input),
        Json::Array(values.iter().map(|value| json!({"v":value})).collect())
    );
    assert_eq!(
        serde_json::to_value(input.dtype().as_struct_fields().field("v").unwrap()).unwrap(),
        dtype
    );
    drop(input);
    let empty = source(&session, &dtype, &[]).unwrap();
    assert_eq!(rows(&empty), json!([]));
    drop(empty);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn typed_empty_and_sliced_buffers_keep_schema_credit_and_release_witness() {
    for values in [vec![], vec![json!("00ff")]] {
        let session = ResidentVortexSession::new(1024 * 1024, 1).unwrap();
        let memory = session.memory().clone();
        let input = source(&session, &json!({"Binary":true}), &values).unwrap();
        let witness = input.batch_release_witness().unwrap();
        let field = input.0.array.slots()[1].as_ref().unwrap().clone();
        let hidden = field.buffers()[0].clone();
        let sliced = field.slice(0..field.len()).unwrap();
        drop(field);
        drop(input);
        drop(session);
        assert!(witness.upgrade().is_some());
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(sliced);
        assert!(witness.upgrade().is_some());
        drop(hidden);
        assert!(witness.upgrade().is_none());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn malformed_typed_schemas_and_values_fail_without_retained_credits() {
    let session = ResidentVortexSession::new(16 * 1024 * 1024, 1).unwrap();
    for (dtype, value) in [
        (r#"{"Struct":[{"names":["x"],"dtypes":[]},true]}"#, "null"),
        (
            r#"{"Struct":[{"names":["x","x"],"dtypes":[{"Bool":true},{"Bool":true}]},true]}"#,
            "null",
        ),
        (r#"{"Bool":true,"Bool":false}"#, "true"),
        (r#"{"Bool":1}"#, "true"),
        (r#"{"Primitive":["f16",true]}"#, "1.0"),
        (r#"{"Decimal":[{"precision":39,"scale":0},true]}"#, "null"),
        (r#"{"Decimal":[{"precision":2,"scale":3},true]}"#, "null"),
        (r#"{"List":[{"Bool":true},true,true]}"#, "[]"),
        (r#"{"Primitive":["i8",true]}"#, "128"),
        (r#"{"Primitive":["u64",true]}"#, "-1"),
        (r#"{"Primitive":["f32",true]}"#, "0.1"),
        (r#"{"Primitive":["f64",true]}"#, "18446744073709551615"),
        (r#"{"Primitive":["f64",true]}"#, "18446744073709551617"),
        (r#"{"Primitive":["f64",true]}"#, "-9223372036854775809"),
        (r#"{"Primitive":["i64",true]}"#, "1.0"),
        (r#"{"Bool":false}"#, "null"),
        (r#"{"Binary":true}"#, r#""0g""#),
        (r#"{"FixedSizeList":[{"Bool":true},2,true]}"#, "[true]"),
        (
            r#"{"Struct":[{"names":["x"],"dtypes":[{"Bool":true}]},true]}"#,
            r#"{"x":true,"x":false}"#,
        ),
        (
            r#"{"Extension":{"id":"vortex.date","metadata":[0],"storage_dtype":{"Primitive":["i32",true]}}}"#,
            "1",
        ),
    ] {
        let result = ResidentMemorySource::from_batch_columns(
            &session,
            &[MemoryColumn {
                name: "v",
                values: MemoryColumnValues::TypedJson {
                    dtype_json: dtype,
                    values: &[Some(value)],
                },
            }],
        );
        assert!(result.is_err(), "accepted {dtype}: {value}");
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn fixed_null_expansion_and_denied_json_workspace_stop_before_native_allocation() {
    let session = ResidentVortexSession::new(64 * 1024, 1).unwrap();
    let dtype = json!({"FixedSizeList":[{"Primitive":["i64",false]},u32::MAX,true]});
    assert!(source(&session, &dtype, &[Json::Null]).is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    assert!(session.snapshot().memory.peak_reserved_bytes < 64 * 1024);
    assert!(source(&session, &json!({"Utf8":true}), &[json!("x".repeat(4096))]).is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn buffered_typed_batches_preserve_empty_schema_and_reject_drift() {
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
    let mut builder =
        MemoryBatchSourceBuilder::new(&session, CancellationToken::default()).unwrap();
    let dtype = r#"{"Primitive":["u16",true]}"#;
    builder
        .push_columns(&[MemoryColumn {
            name: "v",
            values: MemoryColumnValues::TypedJson {
                dtype_json: dtype,
                values: &[],
            },
        }])
        .unwrap();
    builder
        .push_columns(&[MemoryColumn {
            name: "v",
            values: MemoryColumnValues::TypedJson {
                dtype_json: dtype,
                values: &[Some("65535"), None],
            },
        }])
        .unwrap();
    assert!(
        builder
            .push_columns(&[MemoryColumn {
                name: "v",
                values: MemoryColumnValues::TypedJson {
                    dtype_json: r#"{"Primitive":["u32",true]}"#,
                    values: &[],
                }
            }])
            .is_err()
    );
    let input = builder.finish().unwrap();
    assert_eq!(rows(&input), json!([{"v":65535},{"v":null}]));
    assert_eq!(input.intake_payload_bytes_copied(), 2);
    drop(input);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn typed_float_json_preserves_subnormal_and_boundary_bits() {
    use vortex::array::arrays::Primitive;
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let mut values = vec![
        -0.0,
        f64::from(f32::from_bits(1)),
        f64::from(f32::from_bits(0x007f_ffff)),
        f64::from(f32::MIN_POSITIVE),
        f64::from(f32::MAX),
        f64::from(f32::MIN),
    ];
    let mut wide = vec![
        -0.0,
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::MIN,
        1.000_000_000_000_000_2,
        2.225_073_858_507_201_4e-308,
    ];
    let mut bits = 0x972e_56f4_a319_bc07_u64;
    for _ in 0..128 {
        bits = bits.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let narrow = f32::from_bits(u32::try_from(bits >> 32).unwrap());
        if narrow.is_finite() {
            values.push(f64::from(narrow));
        }
        let value = f64::from_bits(bits);
        if value.is_finite() {
            wide.push(value);
        }
    }
    for (kind, values) in [("f32", values), ("f64", wide)] {
        let dtype = json!({"Primitive":[kind,true]});
        let expected = values
            .iter()
            .copied()
            .map(|value| json!(value))
            .collect::<Vec<_>>();
        let input = source(&session, &dtype, &expected).unwrap();
        let field = input.0.array.slots()[1].as_ref().unwrap();
        let field = field.as_opt::<Primitive>().unwrap();
        for (index, expected) in values.iter().enumerate() {
            let actual = if kind == "f32" {
                f64::from(field.as_slice::<f32>()[index])
            } else {
                field.as_slice::<f64>()[index]
            };
            assert_eq!(actual.to_bits(), expected.to_bits(), "{kind} at {index}");
        }
        drop(input);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn nested_schema_depth_matches_native_payload_policy() {
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let mut dtype = json!({"Extension":{"id":"vortex.date","metadata":[4],
        "storage_dtype":{"Primitive":["i32",true]}}});
    let mut value = json!(i32::MIN);
    for _ in 0..24 {
        dtype = json!({"List":[dtype,true]});
        value = json!([value]);
    }
    let input = source(&session, &dtype, &[value.clone()]).unwrap();
    assert_eq!(rows(&input), json!([{"v":value}]));
    drop(input);
    let Err(error) = source(&session, &json!({"List":[dtype,true]}), &[Json::Null]) else {
        panic!("25 nested input levels must be rejected");
    };
    assert!(error.to_string().contains("depth 24"));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn every_nested_input_buffer_and_retaining_slice_keeps_ownership_credit() {
    fn buffers(array: &ArrayRef, output: &mut Vec<vortex::buffer::ByteBuffer>) {
        output.extend(array.buffers().iter().cloned());
        for child in array.slots().iter().flatten() {
            buffers(child, output);
        }
    }
    let dtype = json!({"Struct":[{"names":["id","values"],"dtypes":[
        {"Primitive":["u64",false]},
        {"List":[{"FixedSizeList":[{"Binary":true},2,true]},true]},
    ]},true]});
    for values in [
        vec![],
        vec![
            json!({"id":u64::MAX,"values":[["00ff",null],null]}),
            Json::Null,
        ],
    ] {
        let mut index = 0;
        loop {
            let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
            let memory = session.memory().clone();
            let input = source(&session, &dtype, &values).unwrap();
            let witness = input.batch_release_witness().unwrap();
            let mut all = Vec::new();
            buffers(&input.0.array, &mut all);
            assert!(all.len() >= 8);
            if index == all.len() {
                break;
            }
            let held = all.swap_remove(index);
            // bytes::Bytes deliberately detaches a zero-length slice from its
            // allocation. An empty owned buffer clone still retains its owner.
            let slice = if held.is_empty() {
                held.clone()
            } else {
                held.slice(0..1)
            };
            drop(held);
            drop(all);
            drop(input);
            drop(session);
            assert!(
                witness.upgrade().is_some(),
                "buffer {index}, {} rows",
                values.len()
            );
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(slice);
            assert!(witness.upgrade().is_none());
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            index += 1;
        }
    }
}

#[test]
fn empty_nested_array_slice_retains_the_source_schema_and_releases_it() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let dtype =
        json!({"List":[{"Struct":[{"names":["v"],"dtypes":[{"Binary":false}]},true]},true]});
    let input = source(&session, &dtype, &[]).unwrap();
    let witness = input.batch_release_witness().unwrap();
    let slice = input.0.array.slice(0..0).unwrap();
    drop(input);
    drop(session);
    assert!(witness.upgrade().is_some());
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(
        serde_json::to_value(slice.dtype().as_struct_fields().field("v").unwrap()).unwrap(),
        dtype
    );
    drop(slice);
    assert!(witness.upgrade().is_none());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
