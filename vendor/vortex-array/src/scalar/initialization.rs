// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered default and zero construction without recursive child calls.

use vortex_buffer::{BufferString, ByteBuffer};
use vortex_error::VortexExpect;

use crate::dtype::{DType, StructFields, UnionVariants};

use super::{DecimalValue, PValue, Scalar, ScalarValue, UnionValue};

enum Children<'a> {
    Fixed {
        dtype: &'a DType,
        remaining: u32,
    },
    Struct {
        fields: &'a StructFields,
        index: usize,
    },
}

impl<'a> Children<'a> {
    fn next(&mut self) -> Option<&'a DType> {
        match self {
            Self::Fixed { dtype, remaining } => {
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                Some(dtype)
            }
            Self::Struct { fields, index } => {
                let dtype = fields.try_field_dtypes().nth(*index)?;
                *index += 1;
                Some(dtype.vortex_expect("field DType must be valid"))
            }
        }
    }
}

enum Frame<'a> {
    Tuple(Children<'a>, Vec<Option<ScalarValue>>),
    Union(&'a UnionVariants),
    PresentExtension,
}

#[derive(Default)]
struct Pending<'a>(Vec<Frame<'a>>);

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        // Match recursive unwind order for partially constructed siblings.
        while self.0.pop().is_some() {}
    }
}

pub(super) fn zero(dtype: &DType) -> Option<ScalarValue> {
    initialize::<false>(dtype).flatten()
}

pub(super) fn default(dtype: &DType) -> Option<Option<ScalarValue>> {
    initialize::<true>(dtype)
}

fn initialize<const DEFAULT: bool>(mut dtype: &DType) -> Option<Option<ScalarValue>> {
    let mut pending = Pending::default();
    'descend: loop {
        let mut value = if DEFAULT && dtype.is_nullable() {
            None
        } else {
            Some(match dtype {
                DType::Null => return DEFAULT.then_some(None),
                DType::Bool(_) => ScalarValue::Bool(false),
                DType::Primitive(ptype, _) => ScalarValue::Primitive(PValue::zero(ptype)),
                DType::Decimal(decimal, _) => ScalarValue::Decimal(DecimalValue::zero(decimal)),
                DType::Utf8(_) => ScalarValue::Utf8(BufferString::empty()),
                DType::Binary(_) => ScalarValue::Binary(ByteBuffer::empty()),
                DType::List(..) | DType::Map(..) => ScalarValue::Tuple(Vec::new()),
                DType::FixedSizeList(..) | DType::Struct(..) => {
                    let mut children = match dtype {
                        DType::FixedSizeList(dtype, size, _) => Children::Fixed {
                            dtype,
                            remaining: *size,
                        },
                        DType::Struct(fields, _) => Children::Struct { fields, index: 0 },
                        _ => unreachable!("tuple constructor dtype"),
                    };
                    if let Some(child) = children.next() {
                        pending.0.push(Frame::Tuple(children, Vec::new()));
                        dtype = child;
                        continue;
                    }
                    ScalarValue::Tuple(Vec::new())
                }
                DType::Union(variants, _) => {
                    let child = variants
                        .try_variant_dtypes()
                        .next()
                        .vortex_expect("union must have at least one variant")
                        .vortex_expect("variant DType must be valid");
                    pending.0.push(Frame::Union(variants));
                    dtype = child;
                    continue;
                }
                DType::Variant(_) => ScalarValue::Variant(Box::new(Scalar::null(DType::Null))),
                DType::Extension(extension) => {
                    if DEFAULT {
                        pending.0.push(Frame::PresentExtension);
                    }
                    dtype = extension.storage_dtype();
                    continue;
                }
            })
        };
        loop {
            match pending.0.pop() {
                None => return Some(value),
                Some(Frame::Tuple(mut children, mut values)) => {
                    values.push(value);
                    if let Some(child) = children.next() {
                        pending.0.push(Frame::Tuple(children, values));
                        dtype = child;
                        continue 'descend;
                    }
                    value = Some(ScalarValue::Tuple(values));
                }
                Some(Frame::Union(variants)) => {
                    value = Some(ScalarValue::Union(UnionValue::new(
                        variants.child_index_to_tag(0),
                        value,
                    )));
                }
                Some(Frame::PresentExtension) => {
                    // A non-null extension default needs present storage.
                    value.as_ref()?;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "initialization_tests.rs"]
mod tests;
