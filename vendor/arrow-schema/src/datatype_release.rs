// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Intrinsic cleanup of the existing Arrow schema graph, in declaration order.
//! No schema is cloned or inspected and no execution credit is requested here.

use std::array;
use std::mem::{self, ManuallyDrop};
use std::ptr;
use std::sync::Arc;
use std::vec;

use crate::field::FieldTail;
use crate::retained::{FieldStorage, Retained};
use crate::{DataType, FieldRef, Fields, UnionFields};

enum Owned {
    Timezone(Option<Arc<str>>),
    Field(FieldRef),
    Fields(Fields),
    Union(UnionFields),
    Dictionary([Box<DataType>; 2]),
    RunEnd([FieldRef; 2]),
}

/// Transfer all owning fields once, leaving an inert node for automatic drop.
fn take(dtype: &mut DataType) -> Option<Owned> {
    let owned = ManuallyDrop::new(mem::replace(dtype, DataType::Null));
    // SAFETY: dtype exclusively borrows an initialized, unpinned enum. Its old
    // value now exists only in local manual-drop storage. This exhaustive match
    // reads every owning field exactly once from its valid, aligned reference:
    // timezone, field/container, both dictionary boxes, or both run-end fields.
    // All untransferred fields are Copy. Extraction performs no allocation,
    // callback, formatting or fallible operation; no pointer escapes. The local
    // wrapper is never used or destroyed afterwards, and dtype remains Null.
    unsafe {
        match &*owned {
            DataType::Timestamp(_, timezone) => Some(Owned::Timezone(ptr::read(timezone))),
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::LargeList(field)
            | DataType::LargeListView(field)
            | DataType::Map(field, _) => Some(Owned::Field(ptr::read(field))),
            DataType::Struct(fields) => Some(Owned::Fields(ptr::read(fields))),
            DataType::Union(fields, _) => Some(Owned::Union(ptr::read(fields))),
            DataType::Dictionary(key, value) => {
                Some(Owned::Dictionary([ptr::read(key), ptr::read(value)]))
            }
            DataType::RunEndEncoded(run_ends, values) => {
                Some(Owned::RunEnd([ptr::read(run_ends), ptr::read(values)]))
            }
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => None,
        }
    }
}

enum Child {
    Field(FieldRef),
    Boxed(Box<DataType>),
}

enum Frame {
    Fields {
        children: vec::IntoIter<FieldRef>,
        _retained: Retained,
    },
    Union {
        children: vec::IntoIter<(i8, FieldRef)>,
        _retained: Retained,
    },
    Dictionary(array::IntoIter<Box<DataType>, 2>),
    RunEnd(array::IntoIter<FieldRef, 2>),
    FieldTail {
        _tail: FieldTail,
    },
    BoxTail {
        _allocation: Box<DataType>,
    },
}

impl Frame {
    fn next(&mut self) -> Option<Child> {
        match self {
            Self::Fields { children, .. } => children.next().map(Child::Field),
            Self::Union { children, .. } => children.next().map(|(_, f)| Child::Field(f)),
            Self::Dictionary(children) => children.next().map(Child::Boxed),
            Self::RunEnd(children) => children.next().map(Child::Field),
            Self::FieldTail { .. } | Self::BoxTail { .. } => None,
        }
    }
}

#[derive(Default)]
struct Pending(Vec<Frame>);

impl Drop for Pending {
    fn drop(&mut self) {
        // Ordinary Vec destruction would release outer credit before the inner
        // work after a retained owner's panic. Drain innermost frames first.
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

fn field(field: FieldRef, pending: &mut Pending) -> Option<Owned> {
    let (mut dtype, tail) = Arc::into_inner(field)?.into_release_parts();
    if !tail.is_empty() {
        pending.0.push(Frame::FieldTail { _tail: tail });
    }
    take(&mut dtype)
}

pub(crate) fn release(dtype: &mut DataType) {
    let mut current = take(dtype);
    let mut pending = Pending::default();
    loop {
        if let Some(owned) = current.take() {
            match owned {
                Owned::Timezone(timezone) => drop(timezone),
                Owned::Field(child) => current = field(child, &mut pending),
                Owned::Fields(fields) => {
                    if let Some(FieldStorage { fields, retained }) = fields.into_release_parts() {
                        pending.0.push(Frame::Fields {
                            children: fields.into_iter(),
                            _retained: retained,
                        });
                    }
                }
                Owned::Union(fields) => {
                    if let Some(FieldStorage { fields, retained }) = fields.into_release_parts() {
                        pending.0.push(Frame::Union {
                            children: fields.into_iter(),
                            _retained: retained,
                        });
                    }
                }
                Owned::Dictionary(children) => {
                    pending.0.push(Frame::Dictionary(children.into_iter()));
                }
                Owned::RunEnd(children) => pending.0.push(Frame::RunEnd(children.into_iter())),
            }
        } else if let Some(frame) = pending.0.last_mut() {
            match frame.next() {
                Some(Child::Field(child)) => current = field(child, &mut pending),
                Some(Child::Boxed(mut child)) => {
                    current = take(&mut child);
                    if current.is_some() {
                        pending.0.push(Frame::BoxTail { _allocation: child });
                    }
                }
                None => {
                    pending.0.pop();
                }
            }
        } else {
            return;
        }
    }
}

#[cfg(test)]
#[path = "datatype_release_tests.rs"]
mod tests;
