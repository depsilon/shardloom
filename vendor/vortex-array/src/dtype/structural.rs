// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Borrowed structural operations. Keep lazy inspection and trailing work in
//! the original order without recursively entering child dtype trait methods.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use vortex_error::VortexExpect;

use super::{DType, FieldDType, Nullability};

#[derive(Clone, Copy)]
enum Access {
    Ordinary,
    Struct,
    Union,
    MapKey,
    MapValue,
}

impl Access {
    fn for_mode(self, ignore_nullability: bool) -> Self {
        if ignore_nullability {
            self
        } else {
            Self::Ordinary
        }
    }

    fn value(self, field: &FieldDType) -> &DType {
        field.value_ref().vortex_expect(match self {
            Self::Ordinary => "Failed to parse FieldDType into DType",
            Self::Struct => "field DType must be valid",
            Self::Union => "variant DType must be valid",
            Self::MapKey => "map key dtype must be valid",
            Self::MapValue => "map value dtype must be valid",
        })
    }
}

enum Compare<'a> {
    Node(&'a DType, &'a DType),
    Field(&'a FieldDType, &'a FieldDType, Access),
    Fields(&'a [FieldDType], &'a [FieldDType], Access),
    Sorted(bool, bool),
    Tags(&'a [u8], &'a [u8]),
}

pub(super) fn equal(lhs: &DType, rhs: &DType, ignore_nullability: bool) -> bool {
    let mut current = Compare::Node(lhs, rhs);
    let mut pending = Vec::new();
    loop {
        match current {
            Compare::Node(lhs, rhs) => match (lhs, rhs) {
                (DType::Null, DType::Null) => {}
                (DType::Bool(a), DType::Bool(b))
                | (DType::Utf8(a), DType::Utf8(b))
                | (DType::Binary(a), DType::Binary(b))
                | (DType::Variant(a), DType::Variant(b)) => {
                    if !ignore_nullability && a != b {
                        return false;
                    }
                }
                (DType::Primitive(a, na), DType::Primitive(b, nb)) => {
                    if a != b || (!ignore_nullability && na != nb) {
                        return false;
                    }
                }
                (DType::Decimal(a, na), DType::Decimal(b, nb)) => {
                    if a != b || (!ignore_nullability && na != nb) {
                        return false;
                    }
                }
                (DType::List(a, na), DType::List(b, nb)) => {
                    if !ignore_nullability && na != nb {
                        return false;
                    }
                    if ignore_nullability || !Arc::ptr_eq(a, b) {
                        current = Compare::Node(a, b);
                        continue;
                    }
                }
                (DType::FixedSizeList(a, sa, na), DType::FixedSizeList(b, sb, nb)) => {
                    if sa != sb || (!ignore_nullability && na != nb) {
                        return false;
                    }
                    if ignore_nullability || !Arc::ptr_eq(a, b) {
                        current = Compare::Node(a, b);
                        continue;
                    }
                }
                (DType::Struct(a, na), DType::Struct(b, nb)) => {
                    if !ignore_nullability && na != nb {
                        return false;
                    }
                    if !a.ptr_eq(b) {
                        if a.names() != b.names() {
                            return false;
                        }
                        current = Compare::Fields(
                            a.raw_dtypes(),
                            b.raw_dtypes(),
                            Access::Struct.for_mode(ignore_nullability),
                        );
                        continue;
                    }
                }
                (DType::Union(a, na), DType::Union(b, nb)) => {
                    if !ignore_nullability && na != nb {
                        return false;
                    }
                    if !a.ptr_eq(b) {
                        if a.names() != b.names() {
                            return false;
                        }
                        pending.push(Compare::Tags(a.type_ids(), b.type_ids()));
                        current = Compare::Fields(
                            a.raw_dtypes(),
                            b.raw_dtypes(),
                            Access::Union.for_mode(ignore_nullability),
                        );
                        continue;
                    }
                }
                (DType::Map(a, na), DType::Map(b, nb)) => {
                    if !ignore_nullability && na != nb {
                        return false;
                    }
                    if !a.ptr_eq(b) {
                        let (ak, av) = a.raw_fields();
                        let (bk, bv) = b.raw_fields();
                        pending.push(Compare::Sorted(a.keys_sorted(), b.keys_sorted()));
                        pending.push(Compare::Field(
                            av,
                            bv,
                            Access::MapValue.for_mode(ignore_nullability),
                        ));
                        current =
                            Compare::Field(ak, bk, Access::MapKey.for_mode(ignore_nullability));
                        continue;
                    }
                }
                (DType::Extension(a), DType::Extension(b)) => {
                    if !a.metadata_eq(b) {
                        return false;
                    }
                    current = Compare::Node(a.storage_dtype(), b.storage_dtype());
                    continue;
                }
                // Keep additions to the native enum visible to this match.
                (DType::Null, _)
                | (DType::Bool(_), _)
                | (DType::Primitive(..), _)
                | (DType::Decimal(..), _)
                | (DType::Utf8(_), _)
                | (DType::Binary(_), _)
                | (DType::List(..), _)
                | (DType::FixedSizeList(..), _)
                | (DType::Map(..), _)
                | (DType::Struct(..), _)
                | (DType::Union(..), _)
                | (DType::Variant(_), _)
                | (DType::Extension(_), _) => return false,
            },
            Compare::Field(a, b, access) => {
                let (a, b) = if ignore_nullability {
                    (access.value(a), access.value(b))
                } else {
                    a.equality_values(b)
                };
                current = Compare::Node(a, b);
                continue;
            }
            Compare::Fields(a, b, access) => {
                if a.len() != b.len() {
                    return false;
                }
                if let (Some((a, ar)), Some((b, br))) = (a.split_first(), b.split_first()) {
                    if !ar.is_empty() {
                        pending.push(Compare::Fields(ar, br, access));
                    }
                    current = Compare::Field(a, b, access);
                    continue;
                }
            }
            Compare::Sorted(a, b) => {
                if a != b {
                    return false;
                }
            }
            Compare::Tags(a, b) => {
                if a != b {
                    return false;
                }
            }
        }
        let Some(next) = pending.pop() else {
            return true;
        };
        current = next;
    }
}

enum Feed<'a> {
    Node(&'a DType),
    Field(&'a FieldDType, Access),
    Fields(&'a [FieldDType], Access),
    Nullability(Nullability),
    Size(u32),
    Sorted(bool),
    Tags(&'a [u8]),
}

pub(super) fn hash<H: Hasher>(dtype: &DType, state: &mut H, ignore_nullability: bool) {
    let mut current = Feed::Node(dtype);
    let mut pending = Vec::new();
    loop {
        match current {
            Feed::Node(dtype) => {
                std::mem::discriminant(dtype).hash(state);
                match dtype {
                    DType::Null => {}
                    DType::Bool(n) | DType::Utf8(n) | DType::Binary(n) | DType::Variant(n) => {
                        if !ignore_nullability {
                            n.hash(state);
                        }
                    }
                    DType::Primitive(p, n) => {
                        p.hash(state);
                        if !ignore_nullability {
                            n.hash(state);
                        }
                    }
                    DType::Decimal(d, n) => {
                        d.hash(state);
                        if !ignore_nullability {
                            n.hash(state);
                        }
                    }
                    DType::List(child, n) => {
                        if !ignore_nullability {
                            pending.push(Feed::Nullability(*n));
                        }
                        current = Feed::Node(child);
                        continue;
                    }
                    DType::FixedSizeList(child, size, n) => {
                        if !ignore_nullability {
                            pending.push(Feed::Nullability(*n));
                        }
                        pending.push(Feed::Size(*size));
                        current = Feed::Node(child);
                        continue;
                    }
                    DType::Struct(fields, n) => {
                        fields.names().hash(state);
                        if !ignore_nullability {
                            fields.raw_dtypes().len().hash(state);
                            pending.push(Feed::Nullability(*n));
                        }
                        current = Feed::Fields(
                            fields.raw_dtypes(),
                            Access::Struct.for_mode(ignore_nullability),
                        );
                        continue;
                    }
                    DType::Union(variants, n) => {
                        variants.names().hash(state);
                        if !ignore_nullability {
                            variants.raw_dtypes().len().hash(state);
                            pending.push(Feed::Nullability(*n));
                        }
                        pending.push(Feed::Tags(variants.type_ids()));
                        current = Feed::Fields(
                            variants.raw_dtypes(),
                            Access::Union.for_mode(ignore_nullability),
                        );
                        continue;
                    }
                    DType::Map(map, n) => {
                        let (key, value) = map.raw_fields();
                        if !ignore_nullability {
                            pending.push(Feed::Nullability(*n));
                        }
                        pending.push(Feed::Sorted(map.keys_sorted()));
                        pending.push(Feed::Field(
                            value,
                            Access::MapValue.for_mode(ignore_nullability),
                        ));
                        current = Feed::Field(key, Access::MapKey.for_mode(ignore_nullability));
                        continue;
                    }
                    DType::Extension(extension) => {
                        extension.hash_metadata(state);
                        current = Feed::Node(extension.storage_dtype());
                        continue;
                    }
                }
            }
            Feed::Field(field, access) => {
                current = Feed::Node(access.value(field));
                continue;
            }
            Feed::Fields(fields, access) => {
                if let Some((field, rest)) = fields.split_first() {
                    if !rest.is_empty() {
                        pending.push(Feed::Fields(rest, access));
                    }
                    current = Feed::Field(field, access);
                    continue;
                }
            }
            Feed::Nullability(n) => n.hash(state),
            Feed::Size(size) => size.hash(state),
            Feed::Sorted(sorted) => sorted.hash(state),
            Feed::Tags(tags) => tags.hash(state),
        }
        let Some(next) = pending.pop() else {
            return;
        };
        current = next;
    }
}

#[cfg(test)]
#[path = "structural_tests.rs"]
mod tests;
