// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Core [`ScalarValue`] type definition.

use std::fmt::Display;
use std::fmt::Formatter;

use itertools::Itertools;
use vortex_buffer::BufferString;
use vortex_buffer::ByteBuffer;
use vortex_error::vortex_panic;

use crate::dtype::DType;
use crate::scalar::DecimalValue;
use crate::scalar::PValue;
use crate::scalar::Scalar;
use crate::scalar::UnionValue;

/// The value stored in a [`Scalar`].
///
/// This enum represents the possible non-null values that can be stored in a scalar. When the
/// scalar is null, the value is represented as `None` in the `Option<ScalarValue>` field.
#[derive(Eq)]
pub enum ScalarValue {
    /// A boolean value.
    Bool(bool),
    /// A primitive numeric value.
    Primitive(PValue),
    /// A decimal value.
    Decimal(DecimalValue),
    /// A UTF-8 encoded string value.
    Utf8(BufferString),
    /// A binary (byte array) value.
    Binary(ByteBuffer),
    /// A tuple of potentially null scalar values.
    ///
    /// Used as the underlying representation for list, fixed-size list, map, and struct scalars.
    Tuple(Vec<Option<ScalarValue>>),
    /// A present union value carrying its selected type ID and raw child value.
    Union(UnionValue),
    /// A row-specific scalar wrapped by `DType::Variant`.
    Variant(Box<Scalar>),
}

impl Drop for ScalarValue {
    fn drop(&mut self) {
        super::release::release(self);
    }
}

impl ScalarValue {
    /// Returns the zero / identity value for the given [`DType`].
    pub(super) fn zero_value(dtype: &DType) -> Self {
        Self::try_zero_value(dtype)
            .unwrap_or_else(|| vortex_panic!("{dtype} has no non-null zero value"))
    }

    /// Returns the non-null zero value for `dtype`, or [`None`] if no such value exists.
    pub(super) fn try_zero_value(dtype: &DType) -> Option<Self> {
        super::initialization::zero(dtype)
    }

    /// Returns a valid default value for `dtype`, or [`None`] if the dtype has no default.
    ///
    /// The outer [`Option`] distinguishes a dtype with no default from a nullable dtype, whose valid
    /// default is represented by the inner [`None`].
    pub(super) fn try_default_value(dtype: &DType) -> Option<Option<Self>> {
        super::initialization::default(dtype)
    }
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        super::formatting::display_raw(self, f)
    }
}

impl ScalarValue {
    pub(super) fn fmt_leaf(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::Bool(b) => write!(f, "{b}"),
            ScalarValue::Primitive(p) => write!(f, "{p}"),
            ScalarValue::Decimal(d) => write!(f, "{d}"),
            ScalarValue::Utf8(s) => {
                let bufstr = s.as_str();
                let str_len = bufstr.chars().count();

                if str_len > 10 {
                    let prefix = String::from_iter(bufstr.chars().take(5));
                    let suffix = String::from_iter(bufstr.chars().skip(str_len - 5));

                    write!(f, "\"{prefix}..{suffix}\"")
                } else {
                    write!(f, "\"{bufstr}\"")
                }
            }
            ScalarValue::Binary(b) => {
                if b.len() > 10 {
                    write!(
                        f,
                        "{}..{}",
                        to_hex(&b[0..5]),
                        to_hex(&b[b.len() - 5..b.len()]),
                    )
                } else {
                    write!(f, "{}", to_hex(b))
                }
            }
            _ => unreachable!("compound scalar formatting uses continuations"),
        }
    }
}

/// Formats a byte slice as a hexadecimal string.
fn to_hex(slice: &[u8]) -> String {
    slice
        .iter()
        .format_with("", |f, b| b(&format_args!("{f:02x}")))
        .to_string()
}
