// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Intrinsic protobuf traits. All recursive model cycles cross these two owners.

use std::fmt;
use std::iter::Enumerate;
use std::slice;

use super::dtype::{self as dt, d_type::DtypeType as Type};
use super::scalar::{self as sv, scalar_value::Kind};

const DEBUG_DEPTH: usize = 32;

#[derive(Clone, Copy)]
enum Owner<'a> {
    DType(&'a dt::DType),
    Value(&'a sv::ScalarValue),
}

enum Children<'a> {
    Fixed(std::array::IntoIter<Option<(usize, Owner<'a>)>, 2>),
    DTypes(Enumerate<slice::Iter<'a, dt::DType>>),
    Values(Enumerate<slice::Iter<'a, sv::ScalarValue>>),
}

impl<'a> Iterator for Children<'a> {
    type Item = (usize, Owner<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Fixed(children) => children.flatten().next(),
            Self::DTypes(children) => children.next().map(|(i, child)| (i, Owner::DType(child))),
            Self::Values(children) => children.next().map(|(i, child)| (i, Owner::Value(child))),
        }
    }
}

enum Owned {
    DType(dt::DType),
    Value(sv::ScalarValue),
}

impl Owned {
    fn dtype(self) -> dt::DType {
        match self {
            Self::DType(dtype) => dtype,
            Self::Value(_) => unreachable!(),
        }
    }

    fn value(self) -> sv::ScalarValue {
        match self {
            Self::Value(value) => value,
            Self::DType(_) => unreachable!(),
        }
    }

    fn attach(&mut self, slot: usize, child: Self) {
        match self {
            Self::DType(dtype) => match dtype.dtype_type.as_mut() {
                Some(Type::List(list)) => list.element_type = Some(Box::new(child.dtype())),
                Some(Type::FixedSizeList(list)) => {
                    list.element_type = Some(Box::new(child.dtype()))
                }
                Some(Type::Extension(ext)) => ext.storage_dtype = Some(Box::new(child.dtype())),
                Some(Type::Struct(fields)) => fields.dtypes.push(child.dtype()),
                Some(Type::Union(fields)) => fields.dtypes.push(child.dtype()),
                Some(Type::Map(map)) => {
                    let target = if slot == 0 {
                        &mut map.key_type
                    } else {
                        &mut map.value_type
                    };
                    *target = Some(Box::new(child.dtype()));
                }
                _ => unreachable!(),
            },
            Self::Value(value) => match value.kind.as_mut() {
                Some(Kind::ListValue(list)) => list.values.push(child.value()),
                Some(Kind::UnionValue(union)) => union.value = Some(Box::new(child.value())),
                Some(Kind::VariantValue(scalar)) => match child {
                    Self::DType(dtype) => scalar.dtype = Some(dtype),
                    Self::Value(value) => scalar.value = Some(Box::new(value)),
                },
                _ => unreachable!(),
            },
        }
    }
}

impl<'a> Owner<'a> {
    fn children(self) -> Children<'a> {
        let children = match self {
            Self::DType(dtype) => match dtype.dtype_type.as_ref() {
                Some(Type::Struct(fields)) => {
                    return Children::DTypes(fields.dtypes.iter().enumerate());
                }
                Some(Type::Union(fields)) => {
                    return Children::DTypes(fields.dtypes.iter().enumerate());
                }
                Some(Type::List(list)) => [list.element_type.as_deref().map(Self::DType), None],
                Some(Type::FixedSizeList(list)) => {
                    [list.element_type.as_deref().map(Self::DType), None]
                }
                Some(Type::Extension(ext)) => [ext.storage_dtype.as_deref().map(Self::DType), None],
                Some(Type::Map(map)) => [
                    map.key_type.as_deref().map(Self::DType),
                    map.value_type.as_deref().map(Self::DType),
                ],
                None
                | Some(
                    Type::Null(_)
                    | Type::Bool(_)
                    | Type::Primitive(_)
                    | Type::Decimal(_)
                    | Type::Utf8(_)
                    | Type::Binary(_)
                    | Type::Variant(_),
                ) => [None, None],
            },
            Self::Value(value) => match value.kind.as_ref() {
                Some(Kind::ListValue(list)) => {
                    return Children::Values(list.values.iter().enumerate());
                }
                Some(Kind::UnionValue(union)) => [union.value.as_deref().map(Self::Value), None],
                Some(Kind::VariantValue(scalar)) => [
                    scalar.dtype.as_ref().map(Self::DType),
                    scalar.value.as_deref().map(Self::Value),
                ],
                None
                | Some(
                    Kind::NullValue(_)
                    | Kind::BoolValue(_)
                    | Kind::Int64Value(_)
                    | Kind::Uint64Value(_)
                    | Kind::F32Value(_)
                    | Kind::F64Value(_)
                    | Kind::F16Value(_)
                    | Kind::StringValue(_)
                    | Kind::BytesValue(_),
                ) => [None, None],
            },
        };
        let [first, second] = children;
        Children::Fixed(
            [
                first.map(|child| (0, child)),
                second.map(|child| (1, child)),
            ]
            .into_iter(),
        )
    }

    // Copy only payload/header fields. Recursive fields are populated after their
    // children complete; partial owners already have intrinsic iterative cleanup.
    fn header_clone(self) -> Owned {
        match self {
            Self::DType(dtype) => Owned::DType(dt::DType {
                dtype_type: dtype.dtype_type.as_ref().map(|kind| match kind {
                    Type::Struct(fields) => Type::Struct(dt::Struct {
                        names: fields.names.clone(),
                        dtypes: Vec::with_capacity(fields.dtypes.len()),
                        nullable: fields.nullable,
                    }),
                    Type::Union(fields) => Type::Union(dt::Union {
                        names: fields.names.clone(),
                        dtypes: Vec::with_capacity(fields.dtypes.len()),
                        type_ids: fields.type_ids.clone(),
                        nullable: fields.nullable,
                    }),
                    Type::List(list) => Type::List(Box::new(dt::List {
                        element_type: None,
                        nullable: list.nullable,
                    })),
                    Type::FixedSizeList(list) => Type::FixedSizeList(Box::new(dt::FixedSizeList {
                        element_type: None,
                        size: list.size,
                        nullable: list.nullable,
                    })),
                    Type::Map(map) => Type::Map(Box::new(dt::Map {
                        key_type: None,
                        value_type: None,
                        keys_sorted: map.keys_sorted,
                        nullable: map.nullable,
                    })),
                    Type::Extension(ext) => Type::Extension(Box::new(dt::Extension {
                        id: ext.id.clone(),
                        storage_dtype: None,
                        metadata: ext.metadata.clone(),
                    })),
                    leaf @ (Type::Null(_)
                    | Type::Bool(_)
                    | Type::Primitive(_)
                    | Type::Decimal(_)
                    | Type::Utf8(_)
                    | Type::Binary(_)
                    | Type::Variant(_)) => leaf.clone(),
                }),
            }),
            Self::Value(value) => Owned::Value(sv::ScalarValue {
                kind: value.kind.as_ref().map(|kind| match kind {
                    Kind::ListValue(list) => Kind::ListValue(sv::ListValue {
                        values: Vec::with_capacity(list.values.len()),
                    }),
                    Kind::UnionValue(union) => Kind::UnionValue(Box::new(sv::UnionValue {
                        type_id: union.type_id,
                        value: None,
                    })),
                    Kind::VariantValue(_) => Kind::VariantValue(Box::default()),
                    leaf @ (Kind::NullValue(_)
                    | Kind::BoolValue(_)
                    | Kind::Int64Value(_)
                    | Kind::Uint64Value(_)
                    | Kind::F32Value(_)
                    | Kind::F64Value(_)
                    | Kind::F16Value(_)
                    | Kind::StringValue(_)
                    | Kind::BytesValue(_)) => leaf.clone(),
                }),
            }),
        }
    }

    fn header_eq(self, other: Self) -> bool {
        match (self, other) {
            (Self::DType(left), Self::DType(right)) => dtype_header_eq(left, right),
            (Self::Value(left), Self::Value(right)) => value_header_eq(left, right),
            _ => false,
        }
    }
}

fn dtype_header_eq(left: &dt::DType, right: &dt::DType) -> bool {
    match (left.dtype_type.as_ref(), right.dtype_type.as_ref()) {
        (None, None) => true,
        (Some(left), Some(right)) => match (left, right) {
            (Type::Null(a), Type::Null(b)) => a == b,
            (Type::Bool(a), Type::Bool(b)) => a == b,
            (Type::Primitive(a), Type::Primitive(b)) => a == b,
            (Type::Decimal(a), Type::Decimal(b)) => a == b,
            (Type::Utf8(a), Type::Utf8(b)) => a == b,
            (Type::Binary(a), Type::Binary(b)) => a == b,
            (Type::Variant(a), Type::Variant(b)) => a == b,
            (Type::Struct(a), Type::Struct(b)) => {
                a.names == b.names && a.nullable == b.nullable && a.dtypes.len() == b.dtypes.len()
            }
            (Type::Union(a), Type::Union(b)) => {
                a.names == b.names
                    && a.nullable == b.nullable
                    && a.type_ids == b.type_ids
                    && a.dtypes.len() == b.dtypes.len()
            }
            (Type::List(a), Type::List(b)) => a.nullable == b.nullable,
            (Type::FixedSizeList(a), Type::FixedSizeList(b)) => {
                a.size == b.size && a.nullable == b.nullable
            }
            (Type::Map(a), Type::Map(b)) => {
                a.keys_sorted == b.keys_sorted && a.nullable == b.nullable
            }
            (Type::Extension(a), Type::Extension(b)) => a.id == b.id && a.metadata == b.metadata,
            _ => false,
        },
        _ => false,
    }
}

fn value_header_eq(left: &sv::ScalarValue, right: &sv::ScalarValue) -> bool {
    match (left.kind.as_ref(), right.kind.as_ref()) {
        (None, None) => true,
        (Some(left), Some(right)) => match (left, right) {
            (Kind::ListValue(a), Kind::ListValue(b)) => a.values.len() == b.values.len(),
            (Kind::UnionValue(a), Kind::UnionValue(b)) => a.type_id == b.type_id,
            (Kind::VariantValue(_), Kind::VariantValue(_)) => true,
            (Kind::NullValue(a), Kind::NullValue(b)) => a == b,
            (Kind::BoolValue(a), Kind::BoolValue(b)) => a == b,
            (Kind::Int64Value(a), Kind::Int64Value(b)) => a == b,
            (Kind::Uint64Value(a), Kind::Uint64Value(b)) => a == b,
            (Kind::F16Value(a), Kind::F16Value(b)) => a == b,
            (Kind::F32Value(a), Kind::F32Value(b)) => a == b,
            (Kind::F64Value(a), Kind::F64Value(b)) => a == b,
            (Kind::StringValue(a), Kind::StringValue(b)) => a == b,
            (Kind::BytesValue(a), Kind::BytesValue(b)) => a == b,
            _ => false,
        },
        _ => false,
    }
}

fn clone_owner(mut owner: Owner<'_>) -> Owned {
    let mut pending = Vec::new();
    'descend: loop {
        let mut result = owner.header_clone();
        let mut children = owner.children();
        if let Some((slot, child)) = children.next() {
            pending.push((result, children, slot));
            owner = child;
            continue;
        }
        loop {
            let Some((mut parent, mut children, slot)) = pending.pop() else {
                return result;
            };
            parent.attach(slot, result);
            if let Some((slot, child)) = children.next() {
                pending.push((parent, children, slot));
                owner = child;
                continue 'descend;
            }
            result = parent;
        }
    }
}

fn equal(left: Owner<'_>, right: Owner<'_>) -> bool {
    if !left.header_eq(right) {
        return false;
    }
    let mut pending = Vec::new();
    let mut children = (left.children(), right.children());
    loop {
        match (children.0.next(), children.1.next()) {
            (Some((a, left)), Some((b, right))) if a == b && left.header_eq(right) => {
                pending.push(children);
                children = (left.children(), right.children());
            }
            (None, None) => match pending.pop() {
                Some(parent) => children = parent,
                None => return true,
            },
            _ => return false,
        }
    }
}

fn debug_is_deep(owner: Owner<'_>) -> bool {
    let mut pending: [Option<Children<'_>>; DEBUG_DEPTH] = std::array::from_fn(|_| None);
    let mut children = owner.children();
    let mut depth = 0;
    loop {
        if let Some((_, child)) = children.next() {
            if depth == DEBUG_DEPTH {
                return true;
            }
            pending[depth] = Some(children);
            depth += 1;
            children = child.children();
        } else if depth == 0 {
            return false;
        } else {
            depth -= 1;
            children = pending[depth].take().unwrap_or_else(|| unreachable!());
        }
    }
}

impl Clone for dt::DType {
    fn clone(&self) -> Self {
        clone_owner(Owner::DType(self)).dtype()
    }
}

impl PartialEq for dt::DType {
    fn eq(&self, other: &Self) -> bool {
        equal(Owner::DType(self), Owner::DType(other))
    }
}

impl fmt::Debug for dt::DType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("DType");
        if debug_is_deep(Owner::DType(self)) {
            builder.finish_non_exhaustive()
        } else {
            builder.field("dtype_type", &self.dtype_type).finish()
        }
    }
}

impl Clone for sv::ScalarValue {
    fn clone(&self) -> Self {
        clone_owner(Owner::Value(self)).value()
    }
}

impl PartialEq for sv::ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        equal(Owner::Value(self), Owner::Value(other))
    }
}

impl fmt::Debug for sv::ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("ScalarValue");
        if debug_is_deep(Owner::Value(self)) {
            builder.finish_non_exhaustive()
        } else {
            builder.field("kind", &self.kind).finish()
        }
    }
}
