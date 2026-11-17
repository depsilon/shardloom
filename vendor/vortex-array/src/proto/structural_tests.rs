// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::fmt::Debug;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::thread;

use prost::Message;

use super::dtype::{self as dt, d_type::DtypeType as Type};
use super::scalar::{self as sv, scalar_value::Kind};

// Unmodified pre-change generated models: no calls into the replacement traits.
#[allow(
    clippy::all,
    clippy::absolute_paths,
    clippy::nursery,
    missing_docs,
    dead_code
)]
mod reference {
    pub mod dtype {
        include!("reference/vortex.dtype.rs");
    }
    pub mod scalar {
        include!("reference/vortex.scalar.rs");
    }
}

fn dtype(kind: Type) -> dt::DType {
    dt::DType {
        dtype_type: Some(kind),
    }
}
fn value(kind: Kind) -> sv::ScalarValue {
    sv::ScalarValue { kind: Some(kind) }
}

fn wrap_dtype(child: dt::DType, kind: usize) -> dt::DType {
    dtype(match kind % 7 {
        0 => Type::List(Box::new(dt::List {
            element_type: Some(Box::new(child)),
            nullable: true,
        })),
        1 => Type::FixedSizeList(Box::new(dt::FixedSizeList {
            element_type: Some(Box::new(child)),
            size: 7,
            nullable: false,
        })),
        2 => Type::Struct(dt::Struct {
            names: vec!["λ\0🙂".into()],
            dtypes: vec![child],
            nullable: true,
        }),
        3 => Type::Union(dt::Union {
            names: vec!["union".into()],
            dtypes: vec![child],
            type_ids: vec![255],
            nullable: false,
        }),
        4 => Type::Map(Box::new(dt::Map {
            key_type: Some(Box::new(child)),
            value_type: Some(Box::default()),
            keys_sorted: true,
            nullable: false,
        })),
        5 => Type::Map(Box::new(dt::Map {
            key_type: Some(Box::default()),
            value_type: Some(Box::new(child)),
            keys_sorted: false,
            nullable: true,
        })),
        6 => Type::Extension(Box::new(dt::Extension {
            id: "test.extension".into(),
            storage_dtype: Some(Box::new(child)),
            metadata: Some(vec![0, 255, 17]),
        })),
        _ => unreachable!(),
    })
}

fn dtype_chain(kind: usize, depth: usize, nullable: bool) -> dt::DType {
    let mut result = dtype(Type::Bool(dt::Bool { nullable }));
    for level in 0..depth {
        result = wrap_dtype(result, if kind == 7 { level % 7 } else { kind });
    }
    result
}

fn dtype_fixtures() -> Vec<dt::DType> {
    let mut fixtures = vec![dt::DType::default(), dtype(Type::Null(dt::Null {}))];
    for nullable in [false, true] {
        fixtures.extend([
            dtype(Type::Bool(dt::Bool { nullable })),
            dtype(Type::Utf8(dt::Utf8 { nullable })),
            dtype(Type::Binary(dt::Binary { nullable })),
            dtype(Type::Variant(dt::Variant { nullable })),
        ]);
        for r#type in [i32::MIN, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, i32::MAX] {
            fixtures.push(dtype(Type::Primitive(dt::Primitive { r#type, nullable })));
        }
        for (precision, scale) in [(0, 0), (38, -5), (u32::MAX, i32::MIN), (1, i32::MAX)] {
            fixtures.push(dtype(Type::Decimal(dt::Decimal {
                precision,
                scale,
                nullable,
            })));
        }
    }
    for kind in 0..8 {
        fixtures.push(dtype_chain(kind, 3, false));
        fixtures.push(dtype_chain(kind, 3, true));
    }
    for present in [false, true] {
        let child = || present.then(|| Box::new(dtype(Type::Null(dt::Null {}))));
        fixtures.extend([
            dtype(Type::List(Box::new(dt::List {
                element_type: child(),
                nullable: true,
            }))),
            dtype(Type::FixedSizeList(Box::new(dt::FixedSizeList {
                element_type: child(),
                size: u32::MAX,
                nullable: false,
            }))),
        ]);
        for metadata in [None, Some(vec![]), Some(vec![0, 128, 255])] {
            fixtures.push(dtype(Type::Extension(Box::new(dt::Extension {
                id: "λ\0🙂".into(),
                storage_dtype: child(),
                metadata,
            }))));
        }
        for other in [false, true] {
            fixtures.push(dtype(Type::Map(Box::new(dt::Map {
                key_type: child(),
                value_type: other.then(Box::default),
                keys_sorted: present,
                nullable: other,
            }))));
        }
    }
    for names in [vec![], vec!["duplicate".into(), "duplicate".into()]] {
        for dtypes in [vec![], vec![dt::DType::default()]] {
            fixtures.push(dtype(Type::Struct(dt::Struct {
                names: names.clone(),
                dtypes: dtypes.clone(),
                nullable: true,
            })));
            for type_ids in [vec![], vec![-1, 256, 0, 0]] {
                fixtures.push(dtype(Type::Union(dt::Union {
                    names: names.clone(),
                    dtypes: dtypes.clone(),
                    type_ids,
                    nullable: false,
                })));
            }
        }
    }
    fixtures
}

fn wrap_value(child: sv::ScalarValue, kind: usize) -> sv::ScalarValue {
    value(match kind % 3 {
        0 => Kind::ListValue(sv::ListValue {
            values: vec![child],
        }),
        1 => Kind::UnionValue(Box::new(sv::UnionValue {
            type_id: u32::MAX,
            value: Some(Box::new(child)),
        })),
        2 => Kind::VariantValue(Box::new(sv::Scalar {
            dtype: Some(dtype_chain(6, 2, true)),
            value: Some(Box::new(child)),
        })),
        _ => unreachable!(),
    })
}

fn value_chain(kind: usize, depth: usize, different: bool) -> sv::ScalarValue {
    let mut result = value(Kind::BoolValue(different));
    for level in 0..depth {
        result = wrap_value(result, if kind == 3 { level % 3 } else { kind });
    }
    result
}

fn value_fixtures() -> Vec<sv::ScalarValue> {
    let mut fixtures = vec![
        sv::ScalarValue::default(),
        value(Kind::NullValue(0)),
        value(Kind::NullValue(i32::MIN)),
        value(Kind::NullValue(i32::MAX)),
    ];
    for flag in [false, true] {
        fixtures.push(value(Kind::BoolValue(flag)));
    }
    for number in [i64::MIN, -1, 0, 1, i64::MAX] {
        fixtures.push(value(Kind::Int64Value(number)));
    }
    for number in [0, 1, u64::MAX] {
        fixtures.push(value(Kind::Uint64Value(number)));
        fixtures.push(value(Kind::F16Value(number)));
    }
    for bits in [
        0,
        0x8000_0000,
        0x7f80_0000,
        0xff80_0000,
        0x7fc0_1234,
        0x7f80_0001,
        1,
        0x3f80_0000,
    ] {
        fixtures.push(value(Kind::F32Value(f32::from_bits(bits))));
    }
    for bits in [
        0,
        0x8000_0000_0000_0000,
        0x7ff0_0000_0000_0000,
        0xfff0_0000_0000_0000,
        0x7ff8_0000_0000_1234,
        0x7ff0_0000_0000_0001,
        1,
        0x3ff0_0000_0000_0000,
    ] {
        fixtures.push(value(Kind::F64Value(f64::from_bits(bits))));
    }
    fixtures.extend([
        value(Kind::StringValue(String::new())),
        value(Kind::StringValue("λ\0🙂".into())),
        value(Kind::BytesValue(vec![])),
        value(Kind::BytesValue(vec![0, 128, 255])),
        value(Kind::ListValue(sv::ListValue::default())),
    ]);
    let leaves = fixtures.clone();
    for leaf in leaves {
        for kind in 0..3 {
            fixtures.push(wrap_value(leaf.clone(), kind));
        }
    }
    for has_dtype in [false, true] {
        for has_value in [false, true] {
            fixtures.push(value(Kind::VariantValue(Box::new(sv::Scalar {
                dtype: has_dtype.then(dt::DType::default),
                value: has_value.then(Box::default),
            }))));
        }
        fixtures.push(value(Kind::UnionValue(Box::new(sv::UnionValue {
            type_id: 256,
            value: has_dtype.then(Box::default),
        }))));
    }
    fixtures.push(value(Kind::ListValue(sv::ListValue {
        values: vec![sv::ScalarValue::default(); 4096],
    })));
    fixtures
}

fn assert_debug(left: &impl Debug, right: &impl Debug) {
    assert_eq!(format!("{left:?}"), format!("{right:?}"));
    assert_eq!(format!("{left:#?}"), format!("{right:#?}"));
    assert_eq!(format!("{left:x?}"), format!("{right:x?}"));
    assert_eq!(format!("{left:X?}"), format!("{right:X?}"));
    assert_eq!(format!("{left:#x?}"), format!("{right:#x?}"));
    assert_eq!(format!("{left:+#032x?}"), format!("{right:+#032x?}"));
    assert_eq!(format!("{left:24.3?}"), format!("{right:24.3?}"));
}

#[test]
fn schema_traits_match_original_generated_models() {
    let fixtures = dtype_fixtures();
    let references = fixtures
        .iter()
        .map(|item| reference::dtype::DType::decode(item.encode_to_vec().as_slice()).unwrap())
        .collect::<Vec<_>>();
    for (item, original) in fixtures.iter().zip(&references) {
        assert_eq!(
            item.clone().encode_to_vec(),
            original.clone().encode_to_vec()
        );
        assert_debug(item, original);
        assert_debug(
            &item.dtype_type.as_ref().cloned(),
            &original.dtype_type.as_ref().cloned(),
        );
    }
    for (a, left) in fixtures.iter().enumerate() {
        for (b, right) in fixtures.iter().enumerate() {
            assert_eq!(left == right, references[a] == references[b], "{a}, {b}");
        }
    }
}

#[test]
fn scalar_traits_match_original_generated_models_and_float_semantics() {
    let fixtures = value_fixtures();
    let references = fixtures
        .iter()
        .map(|item| {
            reference::scalar::ScalarValue::decode(item.encode_to_vec().as_slice()).unwrap()
        })
        .collect::<Vec<_>>();
    for (item, original) in fixtures.iter().zip(&references) {
        assert_eq!(
            item.clone().encode_to_vec(),
            original.clone().encode_to_vec()
        );
        assert_debug(item, original);
        assert_debug(
            &item.kind.as_ref().cloned(),
            &original.kind.as_ref().cloned(),
        );
    }
    for (a, left) in fixtures.iter().enumerate() {
        for (b, right) in fixtures.iter().enumerate() {
            assert_eq!(left == right, references[a] == references[b], "{a}, {b}");
        }
    }
    let nan = wrap_value(value(Kind::F64Value(f64::NAN)), 0);
    assert_ne!(nan, nan);
    assert_eq!(
        wrap_value(value(Kind::F64Value(-0.0)), 0),
        wrap_value(value(Kind::F64Value(0.0)), 0)
    );
}

#[test]
fn diagnostic_boundary_preserves_all_shallow_flags() {
    for kind in 0..8 {
        let shallow = dtype_chain(kind, 32, false);
        let original = reference::dtype::DType::decode(shallow.encode_to_vec().as_slice()).unwrap();
        assert_debug(&shallow, &original);
        assert_eq!(
            format!("{:?}", dtype_chain(kind, 33, false)),
            "DType { .. }"
        );
    }
    for kind in 0..4 {
        // A Variant's schema is a separate owner path, with two extra levels.
        let depth = if kind == 2 { 30 } else { 32 };
        let shallow = value_chain(kind, depth, false);
        let original =
            reference::scalar::ScalarValue::decode(shallow.encode_to_vec().as_slice()).unwrap();
        assert_debug(&shallow, &original);
        assert_eq!(
            format!("{:?}", value_chain(kind, depth + 1, false)),
            "ScalarValue { .. }"
        );
    }
    // Empty and extremely wide containers do not consume depth per sibling.
    let wide = dtype(Type::Struct(dt::Struct {
        names: vec![],
        dtypes: vec![dt::DType::default(); 4096],
        nullable: false,
    }));
    let original = reference::dtype::DType::decode(wide.encode_to_vec().as_slice()).unwrap();
    assert_debug(&wide, &original);
}

fn deep_case(kind: usize) {
    assert!(format!("{:?}", dtype_chain(kind, 32, false)).contains("dtype_type"));
    let scalar_kind = kind % 4;
    let depth = if scalar_kind == 2 { 30 } else { 32 };
    assert!(format!("{:?}", value_chain(scalar_kind, depth, false)).contains("kind"));
    let dtype = dtype_chain(kind, 8192, false);
    let clone = dtype.clone();
    assert_eq!(dtype, clone);
    assert_ne!(clone, dtype_chain(kind, 8192, true));
    assert_eq!(format!("{clone:?}"), "DType { .. }");
    assert!(format!("{clone:#?}").len() < 80);
    let detached = dtype.dtype_type.as_ref().unwrap().clone();
    assert_eq!(&detached, dtype.dtype_type.as_ref().unwrap());
    assert!(format!("{detached:?}").len() < 800);
    drop((detached, clone, dtype));
    let scalar = value_chain(kind % 4, 8192, false);
    let clone = scalar.clone();
    assert_eq!(scalar, clone);
    assert_ne!(clone, value_chain(kind % 4, 8192, true));
    assert_eq!(format!("{clone:?}"), "ScalarValue { .. }");
    let detached = scalar.kind.as_ref().unwrap().clone();
    assert_eq!(&detached, scalar.kind.as_ref().unwrap());
    assert!(format!("{detached:#?}").len() < 4000);
    drop((detached, clone, scalar));
    // A value clone owns both a deeply nested Variant schema and a deep value.
    let owned = sv::Scalar {
        dtype: Some(dtype_chain(kind, 8192, false)),
        value: Some(Box::new(value_chain(kind % 4, 8192, false))),
    };
    let clone = owned.clone();
    assert_eq!(owned, clone);
    assert!(format!("{clone:#?}").len() < 250);
    drop((clone, owned));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let source = value_chain(kind % 4, 8192, false);
            let clone = source.clone();
            assert_eq!(source, clone);
            std::hint::black_box(&clone);
            panic!("test cloned protobuf cleanup");
        }))
        .is_err()
    );
}

#[test]
fn deep_protobuf_traits_and_cleanup_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DEEP_PROTOBUF_TRAITS";
    const COMPLETE: &str = "deep protobuf traits completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let index = kind.parse().unwrap();
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || deep_case(index))
            .unwrap()
            .join()
            .unwrap();
        println!("\n{COMPLETE} {kind}");
        return;
    }
    for kind in 0..8 {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "proto::structural_tests::deep_protobuf_traits_and_cleanup_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{kind}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETE} {kind}"))
        );
    }
}
