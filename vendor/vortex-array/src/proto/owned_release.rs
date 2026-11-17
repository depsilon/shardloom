// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Cleanup of the existing protobuf trees, including independently owned children.
//! Optional fields permit safe ownership transfer without generated-code changes.

use std::mem;
use std::vec;

use super::dtype::{DType, d_type::DtypeType};
use super::scalar::{ScalarValue, scalar_value::Kind};

enum DTypeFrame {
    Fields {
        children: vec::IntoIter<DType>,
        // Union tags follow children in the generated field declaration.
        _tags: Vec<i32>,
    },
    Tail {
        child: Option<Box<DType>>,
        // Extension metadata follows storage in the generated declaration.
        _metadata: Option<Vec<u8>>,
    },
}

impl DTypeFrame {
    fn next(&mut self) -> Option<DtypeType> {
        match self {
            Self::Fields { children, .. } => children.find_map(|mut child| child.dtype_type.take()),
            Self::Tail { child, .. } => child.take().and_then(|mut child| child.dtype_type.take()),
        }
    }
}

impl Drop for DType {
    fn drop(&mut self) {
        let mut current = self.dtype_type.take();
        let mut pending = Vec::new();
        loop {
            if let Some(kind) = current.take() {
                match kind {
                    DtypeType::List(mut list) => {
                        current = list
                            .element_type
                            .take()
                            .and_then(|mut child| child.dtype_type.take());
                    }
                    DtypeType::FixedSizeList(mut list) => {
                        current = list
                            .element_type
                            .take()
                            .and_then(|mut child| child.dtype_type.take());
                    }
                    DtypeType::Struct(mut fields) => {
                        drop(mem::take(&mut fields.names));
                        pending.push(DTypeFrame::Fields {
                            children: mem::take(&mut fields.dtypes).into_iter(),
                            _tags: Vec::new(),
                        });
                    }
                    DtypeType::Union(mut fields) => {
                        drop(mem::take(&mut fields.names));
                        pending.push(DTypeFrame::Fields {
                            children: mem::take(&mut fields.dtypes).into_iter(),
                            _tags: mem::take(&mut fields.type_ids),
                        });
                    }
                    DtypeType::Map(mut map) => {
                        current = map
                            .key_type
                            .take()
                            .and_then(|mut child| child.dtype_type.take());
                        pending.push(DTypeFrame::Tail {
                            child: map.value_type.take(),
                            _metadata: None,
                        });
                    }
                    DtypeType::Extension(mut extension) => {
                        drop(mem::take(&mut extension.id));
                        current = extension
                            .storage_dtype
                            .take()
                            .and_then(|mut child| child.dtype_type.take());
                        pending.push(DTypeFrame::Tail {
                            child: None,
                            _metadata: extension.metadata.take(),
                        });
                    }
                    DtypeType::Null(_)
                    | DtypeType::Bool(_)
                    | DtypeType::Primitive(_)
                    | DtypeType::Decimal(_)
                    | DtypeType::Utf8(_)
                    | DtypeType::Binary(_)
                    | DtypeType::Variant(_) => {}
                }
            } else if let Some(frame) = pending.last_mut() {
                current = frame.next();
                if current.is_none() {
                    pending.pop();
                }
            } else {
                return;
            }
        }
    }
}

impl Drop for ScalarValue {
    fn drop(&mut self) {
        let mut current = self.kind.take();
        let mut pending: Vec<vec::IntoIter<Self>> = Vec::new();
        loop {
            if let Some(kind) = current.take() {
                match kind {
                    Kind::ListValue(list) => pending.push(list.values.into_iter()),
                    Kind::UnionValue(mut union) => {
                        current = union.value.take().and_then(|mut child| child.kind.take());
                    }
                    Kind::VariantValue(mut scalar) => {
                        // Generated Scalar declares dtype before value. DType's
                        // independent cleanup loop finishes before this value.
                        drop(scalar.dtype.take());
                        current = scalar.value.take().and_then(|mut child| child.kind.take());
                    }
                    Kind::NullValue(_)
                    | Kind::BoolValue(_)
                    | Kind::Int64Value(_)
                    | Kind::Uint64Value(_)
                    | Kind::F32Value(_)
                    | Kind::F64Value(_)
                    | Kind::StringValue(_)
                    | Kind::BytesValue(_)
                    | Kind::F16Value(_) => {}
                }
            } else if let Some(children) = pending.last_mut() {
                current = children.find_map(|mut child| child.kind.take());
                if current.is_none() {
                    pending.pop();
                }
            } else {
                return;
            }
        }
    }
}
