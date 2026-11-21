// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

// Original structural fixture builders from 250be8a3c, instantiated against the
// immutable pre-change generated model. Only imports and visibility are adapted.
use crate::proto::structural_tests::reference::{dtype as dt, scalar as sv};
use dt::d_type::DtypeType as Type;
use sv::scalar_value::Kind;

pub(super) fn dtype(kind: Type) -> dt::DType {
    dt::DType {
        dtype_type: Some(kind),
    }
}
pub(super) fn value(kind: Kind) -> sv::ScalarValue {
    sv::ScalarValue { kind: Some(kind) }
}

pub(super) fn wrap_dtype(child: dt::DType, kind: usize) -> dt::DType {
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

pub(super) fn dtype_chain(kind: usize, depth: usize, nullable: bool) -> dt::DType {
    let mut result = dtype(Type::Bool(dt::Bool { nullable }));
    for level in 0..depth {
        result = wrap_dtype(result, if kind == 7 { level % 7 } else { kind });
    }
    result
}

pub(super) fn dtype_fixtures() -> Vec<dt::DType> {
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

pub(super) fn wrap_value(child: sv::ScalarValue, kind: usize) -> sv::ScalarValue {
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

pub(super) fn value_chain(kind: usize, depth: usize, different: bool) -> sv::ScalarValue {
    let mut result = value(Kind::BoolValue(different));
    for level in 0..depth {
        result = wrap_value(result, if kind == 3 { level % 3 } else { kind });
    }
    result
}

pub(super) fn value_fixtures() -> Vec<sv::ScalarValue> {
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
