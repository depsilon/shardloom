// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scalar values and types for the Vortex system.
//!
//! This crate provides scalar types and values that can be used to represent individual data
//! elements in the Vortex array system. A [`Scalar`] pairs a logical data type ([`DType`]) with an
//! optional non-null value ([`ScalarValue`]); [`None`] represents a null scalar.
//!
//! Note that the implementations of `Scalar` are split into several different modules.
//!
//! `Scalar` is the single-row counterpart to [`ArrayRef`](crate::ArrayRef): it is logical, not tied
//! to any physical array encoding. A scalar always carries its [`DType`], and null scalars are
//! represented by `value == None`.

#[cfg(feature = "arbitrary")]
pub mod arbitrary;

mod cast;
mod constructor;
mod convert;
mod diagnostic;
mod display;
mod downcast;
mod proto;
mod release;
mod scalar_impl;
mod scalar_value;
mod structural;
mod truncation;
mod typed_view;
mod validate;
mod validate_walk;

pub use scalar_value::*;
pub use truncation::*;
pub use typed_view::*;

use crate::dtype::DType;

/// A typed scalar value.
///
/// Scalars represent a single value with an associated [`DType`]. The value can be null, in which
/// case the [`value`][Scalar::value] method returns `None`.
#[derive(Clone, Eq)]
pub struct Scalar {
    /// The value of the scalar. This is [`None`] if the value is null, otherwise it is [`Some`].
    ///
    /// Invariant: If the [`DType`] is non-nullable, then this value _cannot_ be [`None`].
    value: Option<ScalarValue>,

    /// The type of the scalar. Keep after value so schema credit survives cleanup.
    dtype: DType,
}

// Preserve the existing diagnostic field order despite lifetime-based drop order.
impl std::fmt::Debug for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scalar")
            .field("dtype", &self.dtype)
            .field("value", &self.value)
            .finish()
    }
}

#[cfg(test)]
mod tests;
