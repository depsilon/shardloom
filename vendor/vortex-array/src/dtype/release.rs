// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Final-owner dtype cleanup. The loop consumes existing native ownership; it
//! does not inspect lazy fields, clone descendants, or request execution grants.

use std::array;
use std::mem::{self, ManuallyDrop};
use std::ptr;
use std::sync::Arc;
use std::vec;

use crate::memory::HostMemoryReservation;

use super::{DType, ExtDTypeRef, FieldDType, MapDType, StructFields, UnionVariants};

enum Owned {
    List(Arc<DType>),
    Struct(StructFields),
    Map(MapDType),
    Union(UnionVariants),
    Extension(ExtDTypeRef),
}

/// Transfer the single owning field, leaving an inert node for Rust's drop glue.
fn take(dtype: &mut DType) -> Option<Owned> {
    let owned = ManuallyDrop::new(mem::replace(dtype, DType::Null));
    // SAFETY: `dtype` is an exclusive reference to an initialized, unpinned enum.
    // Its previous value now lives only in `owned`, with automatic drop disabled.
    // The exhaustive match reads each compound variant's sole owning field once
    // from its valid, aligned reference. All remaining fields, and every field
    // in a leaf variant, are Copy. No allocation, callback or panicking operation
    // occurs between removal and returning the owned work item. No pointer escapes;
    // `owned` is never used again or manually dropped. The original node is Null.
    unsafe {
        match &*owned {
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                Some(Owned::List(ptr::read(child)))
            }
            DType::Struct(fields, _) => Some(Owned::Struct(ptr::read(fields))),
            DType::Map(fields, _) => Some(Owned::Map(ptr::read(fields))),
            DType::Union(fields, _) => Some(Owned::Union(ptr::read(fields))),
            DType::Extension(extension) => Some(Owned::Extension(ptr::read(extension))),
            DType::Null
            | DType::Bool(_)
            | DType::Primitive(_, _)
            | DType::Decimal(_, _)
            | DType::Utf8(_)
            | DType::Binary(_)
            | DType::Variant(_) => None,
        }
    }
}

enum Frame {
    Fields {
        fields: vec::IntoIter<FieldDType>,
        // Keep the frame, even after its last field is taken, until that child's
        // complete subtree has been released. Credit must outlive descendants.
        _metadata: Option<HostMemoryReservation>,
    },
    Map(array::IntoIter<FieldDType, 2>),
}

// A panicking opaque owner must still release innermost continuations first.
// Vec's ordinary element drop order would refund ancestors before descendants.
#[derive(Default)]
struct Pending(Vec<Frame>);

impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

impl Frame {
    fn next(&mut self) -> Option<FieldDType> {
        match self {
            Self::Fields { fields, .. } => fields.next(),
            Self::Map(fields) => fields.next(),
        }
    }
}

pub(super) fn release(dtype: &mut DType) {
    let mut current = take(dtype);
    let mut pending = Pending::default();
    loop {
        if let Some(owned) = current.take() {
            match owned {
                Owned::List(child) => {
                    // into_inner handles simultaneous last owners without a
                    // failed try_unwrap dropping a recursive value behind us.
                    current = Arc::into_inner(child).and_then(|mut child| take(&mut child));
                }
                Owned::Extension(extension) => {
                    current = extension
                        .into_storage_dtype()
                        .and_then(|mut child| take(&mut child));
                }
                Owned::Struct(fields) => {
                    if let Some((fields, metadata)) = fields.into_owned_fields() {
                        pending.0.push(Frame::Fields {
                            fields: fields.into_iter(),
                            _metadata: metadata,
                        });
                    }
                }
                Owned::Union(fields) => {
                    if let Some(fields) = fields.into_owned_fields() {
                        pending.0.push(Frame::Fields {
                            fields: fields.into_iter(),
                            _metadata: None,
                        });
                    }
                }
                Owned::Map(fields) => {
                    if let Some(fields) = fields.into_owned_fields() {
                        pending.0.push(Frame::Map(fields.into_iter()));
                    }
                }
            }
        } else if let Some(frame) = pending.0.last_mut() {
            if let Some(field) = frame.next() {
                current = field
                    .into_initialized()
                    .and_then(|mut child| take(&mut child));
            } else {
                pending.0.pop();
            }
        } else {
            return;
        }
    }
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
