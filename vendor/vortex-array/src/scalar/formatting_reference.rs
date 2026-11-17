// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Shallow recursive reference preserving the pre-c0b1a8c72 formatting algorithms.
//!
//! Child Scalar formatting is redirected to Typed/Owned, and raw children to Raw.
//! The original view constructors, validation, lazy access and itertools joins
//! remain independent of the new continuation formatter.

use std::fmt;

use itertools::Itertools;
use vortex_error::VortexExpect;

use crate::dtype::DType;
use crate::dtype::extension::ForeignExtDType;
use crate::scalar::{Scalar, ScalarValue};

pub(super) struct Typed<'a>(pub(super) &'a Scalar);
struct Owned(Scalar);
impl fmt::Display for Owned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Typed(&self.0).fmt(f)
    }
}

impl fmt::Display for Typed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scalar = self.0;
        match scalar.dtype() {
            DType::Null => write!(f, "null"),
            DType::Bool(_) => write!(f, "{}", scalar.as_bool()),
            DType::Primitive(..) => write!(f, "{}", scalar.as_primitive()),
            DType::Decimal(..) => write!(f, "{}", scalar.as_decimal()),
            DType::Utf8(_) => write!(f, "{}", scalar.as_utf8()),
            DType::Binary(_) => write!(f, "{}", scalar.as_binary()),
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                let Some(values) = scalar.value().map(ScalarValue::as_list) else {
                    return write!(f, "null");
                };
                let type_str: &dyn fmt::Display =
                    if let DType::FixedSizeList(_, size, _) = scalar.dtype() {
                        &format!("fixed_size<{size}>")
                    } else {
                        &""
                    };
                write!(
                    f,
                    "{type_str}[{}]",
                    values
                        .iter()
                        .map(|value| Owned(
                            Scalar::try_new(child.as_ref().clone(), value.clone())
                                .vortex_expect("`ListScalar` is already a valid `Scalar`")
                        ))
                        .format(", ")
                )
            }
            DType::Struct(fields, _) => {
                let Some(values) = scalar.value().map(ScalarValue::as_list) else {
                    return write!(f, "null");
                };
                write!(f, "{{")?;
                let formatted_fields = fields
                    .names()
                    .iter()
                    .zip_eq(fields.fields())
                    .zip_eq(values.iter())
                    .map(|((name, dtype), value)| {
                        let val = Scalar::try_new(dtype, value.clone())
                            .vortex_expect("unable to construct a struct `Scalar`");
                        format!("{name}: {}", Typed(&val))
                    })
                    .format(", ");
                write!(f, "{formatted_fields}")?;
                write!(f, "}}")
            }
            DType::Map(..) => {
                let map = scalar.as_map();
                if map.is_null() {
                    return write!(f, "null");
                }
                write!(
                    f,
                    "{{{}}}",
                    map.entries()
                        .map(|(key, value)| format!("{}: {}", Typed(&key), Typed(&value)))
                        .format(", ")
                )
            }
            DType::Union(..) => {
                let union = scalar.as_union();
                let Some(value) = union.child() else {
                    return write!(f, "null");
                };
                let name = union
                    .variant_name()
                    .vortex_expect("non-null union scalar must select a variant");
                write!(f, "{name}({})", Typed(&value))
            }
            DType::Variant(_) => match scalar.as_variant().value() {
                None => write!(f, "null"),
                Some(value) => write!(f, "variant({})", Typed(value)),
            },
            DType::Extension(extension) if extension.is::<ForeignExtDType>() => {
                match scalar.value() {
                    None => write!(f, "null"),
                    Some(value) => write!(f, "{}", Raw(value)),
                }
            }
            DType::Extension(_) => write!(f, "{}", scalar.as_extension()),
        }
    }
}

pub(super) struct Raw<'a>(pub(super) &'a ScalarValue);
impl fmt::Display for Raw<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ScalarValue::Tuple(elements) => {
                write!(f, "[")?;
                for (index, element) in elements.iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    match element {
                        None => write!(f, "null")?,
                        Some(value) => write!(f, "{}", Raw(value))?,
                    }
                }
                write!(f, "]")
            }
            ScalarValue::Union(value) => {
                write!(f, "union@{}(", value.type_id())?;
                match value.child_value() {
                    Some(value) => write!(f, "{}", Raw(value)),
                    None => write!(f, "null"),
                }?;
                write!(f, ")")
            }
            ScalarValue::Variant(value) => write!(f, "{}", Typed(value)),
            // This leaf body is mechanically unchanged from scalar_value.rs
            // at c0b1a8c72; only the nested cases above require a reference walk.
            value => value.fmt_leaf(f),
        }
    }
}
