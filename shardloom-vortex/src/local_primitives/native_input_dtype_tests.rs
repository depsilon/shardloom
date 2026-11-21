//! Original-schema parity and private deep conversion/cleanup proof.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;

use serde_json::{Value, json};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::dtype::{DType, Nullability, PType};

use super::super::super::native_input_schema_reference as reference;
use super::super::{application_fields, json as parser, parse_dtype, reserve_json};
use super::{Frame, Pending, ReservedVec, convert, convert_with_depth};

fn parity(raw: &str) -> bool {
    let memory = LiveMemoryPool::new(1 << 28).unwrap();
    let expected_memory = LiveMemoryPool::new(1 << 28).unwrap();
    let actual = parse_dtype(raw, &memory).map_err(|error| error.to_string());
    let expected = reference::parse_dtype(raw, &expected_memory).map_err(|error| error.to_string());
    let valid = match (actual, expected) {
        (Ok((actual, credit)), Ok((expected, expected_credit))) => {
            assert_eq!(actual, expected, "{raw}");
            assert_eq!(
                serde_json::to_vec(&actual).unwrap(),
                serde_json::to_vec(&expected).unwrap(),
                "{raw}"
            );
            assert_eq!(credit.bytes(), expected_credit.bytes(), "{raw}");
            assert_eq!(memory.snapshot().reserved_bytes, credit.bytes());
            drop(actual);
            drop(credit);
            drop(expected);
            drop(expected_credit);
            true
        }
        (Err(actual), Err(expected)) => {
            assert_eq!(actual, expected, "{raw}");
            false
        }
        (Ok(_), Err(error)) => panic!("new schema accepted {raw}: expected {error}"),
        (Err(error), Ok(_)) => panic!("new schema rejected {raw}: {error}"),
    };
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(expected_memory.snapshot().reserved_bytes, 0);
    valid
}

fn record(names: &[&str], types: Vec<Value>) -> Value {
    let mut value = json!({"Struct":[{"names":names,"dtypes":[]},true]});
    value["Struct"][0]["dtypes"] = Value::Array(types);
    value
}

fn extensions() -> Vec<Value> {
    let primitive = |kind: &str| json!({"Primitive":[kind,true]});
    let list = |kind: &str| json!({"List":[primitive(kind),false]});
    let extension = |id: &str, bytes: &[u8], storage: Value| {
        json!({"Extension":{
        "id":id,"metadata":bytes,"storage_dtype":storage}})
    };
    let mut values = vec![
        extension("vortex.date", &[4], primitive("i32")),
        extension("vortex.time", &[1], primitive("i64")),
        extension("vortex.timestamp", &[1, 0, 0], primitive("i64")),
        extension("shardloom.duration.v1", &[1], primitive("i64")),
        extension(
            crate::native_embedding::ID,
            br#"{"version":1}"#,
            json!({"FixedSizeList":[primitive("f64"),2,true]}),
        ),
        extension(
            crate::native_tensor::ID,
            br#"{"shape":[2]}"#,
            json!({"FixedSizeList":[primitive("i32"),2,true]}),
        ),
        extension(
            crate::native_variable_tensor::ID,
            b"{}",
            record(
                &["data", "shape"],
                vec![
                    list("i32"),
                    json!({"FixedSizeList":[primitive("i32"),2,false]}),
                ],
            ),
        ),
        extension(
            "shardloom.sparse_vector.v1",
            br#"{"index_policy":"preserve","version":1}"#,
            record(
                &["size", "indices", "values"],
                vec![primitive("u64"), list("u64"), list("f16")],
            ),
        ),
        extension(
            "shardloom.quantized_vector.v1",
            br#"{"parameter_mode":"per_vector","version":1}"#,
            record(
                &["codes", "scale", "zero_point"],
                vec![list("u64"), primitive("f16"), primitive("u64")],
            ),
        ),
    ];
    for (id, levels) in [
        ("geoarrow.point", 0),
        ("geoarrow.linestring", 1),
        ("geoarrow.polygon", 2),
    ] {
        let mut storage = record(&["x", "y"], vec![primitive("f64"), primitive("f64")]);
        for _ in 0..levels {
            storage = json!({"List":[storage,true]});
        }
        values.push(extension(id, b"{}", storage));
    }
    #[cfg(feature = "native-binary-objects")]
    values.push(extension(
        crate::native_binary_type::ID,
        br#"{"version":1}"#,
        json!({"Binary":true}),
    ));
    values
}

#[test]
fn typed_dtype_continuations_match_all_native_leaf_and_extension_schemas() {
    let mut fixtures = extensions();
    for nullable in [false, true] {
        for name in ["Bool", "Utf8", "Binary"] {
            fixtures.push(json!({name:nullable}));
        }
        for kind in [
            "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f16", "f32", "f64",
        ] {
            fixtures.push(json!({"Primitive":[kind,nullable]}));
        }
        fixtures.push(json!({"Decimal":[{"precision":38,"scale":38},nullable]}));
    }
    for dtype in &fixtures {
        assert!(parity(&dtype.to_string()));
        assert!(parity(&json!({"List":[dtype,true]}).to_string()));
        assert!(parity(
            &json!({"FixedSizeList":[dtype,0,false]}).to_string()
        ));
        assert!(parity(
            &record(
                &["first", "second"],
                vec![dtype.clone(), json!({"List":[dtype,true]})]
            )
            .to_string()
        ));
    }
    let names: Vec<String> = (0..257).map(|index| format!("field_{index}")).collect();
    assert!(parity(
        &json!({"Struct":[{"names":names,"dtypes":vec![json!({"Bool":false});257]},false]})
            .to_string()
    ));
    let fields: Vec<_> = fixtures
        .iter()
        .enumerate()
        .map(|(i, dtype)| (format!("field_{i}"), dtype.to_string()))
        .collect();
    assert_eq!(
        serde_json::to_vec(&application_fields(&fields).unwrap()).unwrap(),
        serde_json::to_vec(&reference::application_fields(&fields).unwrap()).unwrap()
    );
}

#[test]
fn typed_dtype_continuations_preserve_validation_order_and_public_limits() {
    for value in [
        Value::Null,
        json!({}),
        json!({"Bool":false,"Utf8":false}),
        json!({"Bool":1}),
        json!({"Primitive":["bad",false]}),
        json!({"Primitive":["bad",1]}),
        json!({"List":[null,1]}),
        json!({"List":[null,false]}),
        json!({"FixedSizeList":[null,-1,false]}),
        json!({"FixedSizeList":[null,4_294_967_296_u64,false]}),
        json!({"Unknown":[null,false]}),
        json!({"Unknown":null}),
        json!({"Decimal":[{"precision":39,"scale":0},false]}),
        json!({"Decimal":[{"precision":10,"scale":11},false]}),
        json!({"Struct":[{"names":[],"dtypes":[]},false]}),
        json!({"Struct":[{"names":["x"],"dtypes":[]},false]}),
        json!({"Struct":[{"names":["x","x"],"dtypes":[null,null]},false]}),
        json!({"Struct":[{"names":["x",""],"dtypes":[null,null]},false]}),
        json!({"Struct":[{"names":[1],"dtypes":[null]},false]}),
        json!({"Struct":[{"names":["x"],"dtypes":[null],"extra":0},false]}),
        json!({"Extension":null}),
        json!({"Extension":{"id":"unknown","metadata":[],"storage_dtype":null}}),
    ] {
        assert!(!parity(&value.to_string()));
    }
    for dtype in extensions() {
        for metadata in [Value::Null, json!([]), json!([256]), json!([b'{', b'}'])] {
            let mut bad = dtype.clone();
            bad["Extension"]["metadata"] = metadata;
            parity(&bad.to_string());
            bad["Extension"]["storage_dtype"] = json!({"Primitive":["bad",false]});
            assert!(!parity(&bad.to_string()));
        }
        let mut bad = dtype;
        bad["Extension"]["extra"] = json!(0);
        assert!(!parity(&bad.to_string()));
    }
    for shape in ["list", "fixed", "struct", "mixed"] {
        for depth in [0, 1, 23, 24, 25, 26] {
            parity(&chain(shape, depth, false));
        }
    }
    for fields in [
        vec![],
        vec![("x".into(), "null".into())],
        vec![
            ("x".into(), "{\"Bool\":false}".into()),
            ("x".into(), "{\"Bool\":false}".into()),
        ],
    ] {
        let actual = application_fields(&fields)
            .map(|dtype| serde_json::to_vec(&dtype).unwrap())
            .map_err(|e| e.to_string());
        let expected = reference::application_fields(&fields)
            .map(|dtype| serde_json::to_vec(&dtype).unwrap())
            .map_err(|e| e.to_string());
        assert_eq!(actual, expected);
    }
}

fn kind(shape: &str, index: usize) -> &str {
    if shape == "mixed" {
        ["struct", "list", "fixed"][index % 3]
    } else {
        shape
    }
}

fn chain(shape: &str, depth: usize, bad: bool) -> String {
    let mut raw = String::new();
    for index in 0..depth {
        raw.push_str(match kind(shape, index) {
            "struct" => r#"{"Struct":[{"names":["x","tail"],"dtypes":["#,
            "fixed" => r#"{"FixedSizeList":["#,
            _ => r#"{"List":["#,
        });
    }
    raw.push_str(if bad {
        r#"{"Primitive":["bad",false]}"#
    } else {
        r#"{"Primitive":["i32",false]}"#
    });
    for index in (0..depth).rev() {
        raw.push_str(match kind(shape, index) {
            "struct" => r#",{"Bool":false}]},false]}"#,
            "fixed" => ",1,false]}",
            _ => ",false]}",
        });
    }
    raw
}

fn child(dtype: &DType) -> &DType {
    match dtype {
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => child,
        DType::Struct(fields, _) => fields.field_dtypes().next().unwrap(),
        _ => panic!("expected constructed container"),
    }
}

fn inspect(mut dtype: &DType, shape: &str, from: usize, depth: usize) {
    for index in from..depth {
        assert!(!dtype.is_nullable());
        match kind(shape, index) {
            "struct" => {
                let DType::Struct(fields, _) = dtype else {
                    panic!("expected record")
                };
                assert_eq!(fields.nfields(), 2);
                assert_eq!(fields.names()[0].as_ref(), "x");
                assert_eq!(fields.names()[1].as_ref(), "tail");
                assert_eq!(
                    fields.field_dtypes().nth(1).unwrap(),
                    &DType::Bool(Nullability::NonNullable)
                );
            }
            "fixed" => assert!(matches!(dtype, DType::FixedSizeList(_, 1, _))),
            _ => assert!(matches!(dtype, DType::List(_, _))),
        }
        dtype = child(dtype);
    }
    assert_eq!(
        dtype,
        &DType::Primitive(PType::I32, Nullability::NonNullable)
    );
}

fn deep_child() {
    const DEPTH: usize = 8192;
    let memory = LiveMemoryPool::new(1 << 30).unwrap();
    for shape in ["list", "fixed", "struct", "mixed"] {
        let raw = chain(shape, DEPTH, false);
        let workspace = reserve_json(&memory, raw.len()).unwrap();
        let parsed = parser::parse_with_depth(&raw, &memory, DEPTH * 4 + 8).unwrap();
        let initial = memory.snapshot().reserved_bytes;
        let mut nodes = 0;
        let dtype = convert_with_depth(parsed.value(), &memory, DEPTH, &mut nodes).unwrap();
        let tails = (0..DEPTH)
            .filter(|index| kind(shape, *index) == "struct")
            .count();
        assert_eq!(nodes, DEPTH + 1 + tails);
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        inspect(&dtype, shape, 0, DEPTH);
        let retained = child(&dtype).clone();
        if shape == "mixed" {
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    let mut types = ReservedVec::new(&memory).unwrap();
                    types.push(dtype).unwrap();
                    let mut pending = Pending(ReservedVec::new(&memory).unwrap());
                    pending
                        .0
                        .push(Frame::Struct {
                            names: vec!["held"],
                            inputs: &[],
                            types,
                            child_depth: 0,
                            nullable: Nullability::NonNullable,
                        })
                        .unwrap();
                    panic!("intentional typed schema partial-owner panic");
                }))
                .is_err()
            );
        } else {
            drop(dtype);
        }
        inspect(&retained, shape, 1, DEPTH);
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        drop(parsed);
        drop(workspace);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        println!("TYPED_DTYPE_SHAPE_COMPLETE {shape}");
    }
    for raw in [
        chain("mixed", DEPTH, true),
        format!(
            r#"{{"Struct":[{{"names":["complete","bad"],"dtypes":[{},{{"Bool":7}}]}},false]}}"#,
            chain("list", DEPTH, false)
        ),
    ] {
        let workspace = reserve_json(&memory, raw.len()).unwrap();
        let parsed = parser::parse_with_depth(&raw, &memory, DEPTH * 4 + 8).unwrap();
        let initial = memory.snapshot().reserved_bytes;
        assert!(convert_with_depth(parsed.value(), &memory, DEPTH + 1, &mut 0).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        drop(parsed);
        drop(workspace);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    println!("TYPED_DTYPE_DEEP_COMPLETE");
}

#[test]
fn typed_dtype_continuations_convert_and_release_deep_real_json_on_small_stack() {
    const ENV: &str = "SHARDLOOM_TYPED_DTYPE_DEEP_CHILD";
    if std::env::var_os(ENV).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output=Command::new(std::env::current_exe().unwrap()).args(["--exact","local_primitives::native_input_schema::dtype::tests::typed_dtype_continuations_convert_and_release_deep_real_json_on_small_stack","--nocapture"]).env(ENV,"1").output().unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("TYPED_DTYPE_DEEP_COMPLETE"));
}

#[test]
fn typed_dtype_continuations_refund_denial_and_preserve_count_and_depth_checks() {
    let scalar = json!({"Primitive":["i32",false]});
    let tiny = LiveMemoryPool::new(1).unwrap();
    assert_eq!(
        convert(&scalar, &tiny).unwrap(),
        DType::Primitive(PType::I32, Nullability::NonNullable)
    );
    let mut nodes = usize::MAX;
    assert!(
        convert_with_depth(&scalar, &tiny, 24, &mut nodes)
            .unwrap_err()
            .to_string()
            .contains("input dtype node count overflow")
    );
    assert_eq!(nodes, usize::MAX);
    assert_eq!(tiny.snapshot().reserved_bytes, 0);
    let input_memory = LiveMemoryPool::new(1 << 24).unwrap();
    for (raw, limit) in [
        (
            chain("list", 1, false),
            (8 * std::mem::size_of::<Frame<'_>>() - 1) as u64,
        ),
        (record(&["x"], vec![scalar.clone()]).to_string(), 1),
        (
            chain("list", 16, false),
            (24 * std::mem::size_of::<Frame<'_>>() - 1) as u64,
        ),
    ] {
        let _workspace = reserve_json(&input_memory, raw.len()).unwrap();
        let parsed = parser::parse(&raw, &input_memory).unwrap();
        let memory = LiveMemoryPool::new(limit).unwrap();
        assert!(
            convert(parsed.value(), &memory)
                .unwrap_err()
                .to_string()
                .contains("memory reservation denied")
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 1);
    }
    assert_eq!(input_memory.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut nodes = 0;
    let error =
        convert_with_depth(&json!({"List":[null,false]}), &memory, 0, &mut nodes).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("nested input dtype exceeds depth 0")
    );
    assert_eq!(nodes, 2);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
