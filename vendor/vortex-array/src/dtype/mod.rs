// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-11-17: expose fallible borrowed dtype children.
// Structural equality, hashing and final-owner cleanup now use iterative work.

//! A type system for Vortex.
//!
//! This crate contains the core logical type system for Vortex, including the definition of data types,
//! and (optionally) logic for their serialization and deserialization.
//!
//! Vortex dtypes are logical domains, not physical layouts. Encodings are tracked separately on
//! arrays, so the same dtype may be backed by canonical buffers, dictionary codes, compressed
//! children, or a lazy expression array.
//!
//! Every non-null logical dtype carries [`Nullability`] directly. This differs from Apache Arrow,
//! where nullability is usually field metadata.

#[cfg(feature = "arbitrary")]
mod arbitrary;
mod bigint;
mod children;
mod decimal;
mod dtype_impl;
pub mod extension;
mod f16;
mod field;
mod field_mask;
mod field_names;
mod map;
mod native_dtype;
mod nullability;
mod ptype;
mod release;
pub mod serde;
pub mod session;
mod struct_;
mod structural;
mod union;

use std::sync::Arc;

/// The logical types of elements in Vortex arrays.
///
/// `DType` represents the different logical data types that can be represented in a Vortex array.
///
/// This is different from physical types, which represent the actual layout of data (compressed or
/// uncompressed). The set of physical types/formats (or data layout) is surjective into the set of
/// logical types (or in other words, all physical types map to a single logical type).
///
/// Note that a `DType` represents the logical type of the elements in the `Array`s, **not** the
/// logical type of the `Array` itself.
///
/// For example, an array with [`DType::Primitive`]([`I32`], [`NonNullable`]) could be physically
/// encoded as any of the following:
///
/// - A flat array of `i32` values.
/// - A run-length encoded sequence.
/// - Dictionary encoded values with bitpacked codes.
///
/// All of these physical encodings preserve the same logical [`I32`] type, even if the physical
/// data is different.
///
/// [`I32`]: PType::I32
/// [`NonNullable`]: Nullability::NonNullable
#[derive(Debug, Clone, Eq)]
pub enum DType {
    /// A logical null type.
    ///
    /// `Null` only has a single value, `null`.
    Null,

    /// A logical boolean type.
    ///
    /// `Bool` can be `true` or `false` if non-nullable. It can be `true`, `false`, or `null` if
    /// nullable.
    Bool(Nullability),

    /// A logical fixed-width numeric type.
    ///
    /// This can be unsigned, signed, or floating point. See [`PType`] for more information.
    Primitive(PType, Nullability),

    /// Logical real numbers with fixed precision and scale.
    ///
    /// See [`DecimalDType`] for more information.
    Decimal(DecimalDType, Nullability),

    /// Logical UTF-8 strings.
    Utf8(Nullability),

    /// Logical binary data.
    Binary(Nullability),

    /// A logical variable-length list type.
    ///
    /// This is parameterized by a single `DType` that represents the element type of the inner
    /// lists.
    List(Arc<DType>, Nullability),

    /// A logical fixed-size list type.
    ///
    /// This is parameterized by a `DType` that represents the element type of the inner lists, as
    /// well as a `u32` size that determines the fixed length of each `FixedSizeList` scalar.
    FixedSizeList(Arc<DType>, u32, Nullability),

    /// A logical map type.
    ///
    /// Map keys are non-nullable, values may be nullable, and [`MapDType`] stores Arrow's
    /// `keys_sorted` assertion.
    Map(MapDType, Nullability),

    /// A logical struct type.
    ///
    /// A `Struct` type is composed of an ordered list of fields, each with a corresponding name and
    /// `DType`. See [`StructFields`] for more information.
    Struct(StructFields, Nullability),

    /// A logical union (sum) type.
    ///
    /// A `Union` is composed of one or more **variants**, each with a name and a `DType`. A per-row
    /// `u8` tag selects which variant is "live" at that row.
    ///
    /// A union has independent outer nullability in addition to the nullability of each variant.
    /// This distinguishes a null union from a non-null union whose selected child is null.
    ///
    /// See [`UnionVariants`] for the type-tag conventions and accessors.
    Union(UnionVariants, Nullability),

    /// Dynamically typed values stored as Vortex scalars.
    ///
    /// A variant value preserves its full logical value in variant storage and may also carry a
    /// typed, row-aligned shredded representation for selected paths when materialized as a
    /// [`VariantArray`](crate::arrays::VariantArray).
    Variant(Nullability),

    /// A user-defined extension type.
    ///
    /// See [`ExtDTypeRef`] for more information.
    Extension(ExtDTypeRef),
}

impl Drop for DType {
    fn drop(&mut self) {
        release::release(self);
    }
}

impl PartialEq for DType {
    fn eq(&self, other: &Self) -> bool {
        structural::equal(self, other, false)
    }
}

impl std::hash::Hash for DType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        structural::hash(self, state, false);
    }
}

pub use bigint::*;
pub use children::DTypeChildren;
pub use decimal::*;
pub use dtype_impl::NativeDType;
pub use f16::*;
pub use field::*;
pub use field_mask::*;
pub use field_names::*;
pub use half;
pub use map::*;
pub use nullability::*;
pub use ptype::*;
pub use struct_::*;
pub use union::*;

use crate::dtype::extension::ExtDTypeRef;

pub mod proto {
    //! Protocol buffer representations for DTypes
    //!
    //! This module contains the code to serialize and deserialize DTypes to and from protocol buffers.

    pub use crate::proto::dtype;
}

pub mod flatbuffers {
    //! Flatbuffer representations for DTypes
    //!
    //! This module contains the code to serialize and deserialize DTypes to and from flatbuffers.

    pub use crate::flatbuffers::dtype::*;
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use vortex_session::VortexSession;

    pub(crate) static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::empty);
}
