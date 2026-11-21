// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Recursive Display reference adapted from b99a1299c. Child wrappers keep it
// independent of the continuation formatter while retaining the original calls.

use std::fmt;

use itertools::Itertools;

use super::super::*;

pub(super) struct DTypeDisplay<'a>(pub &'a DType);
pub(super) struct StructDisplay<'a>(pub &'a StructFields);
pub(super) struct MapDisplay<'a>(pub &'a MapDType);
pub(super) struct UnionDisplay<'a>(pub &'a UnionVariants);
pub(super) struct ExtensionDisplay<'a>(pub &'a ExtDTypeRef);

impl fmt::Display for DTypeDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            DType::Null => write!(f, "null"),
            DType::Bool(null) => write!(f, "bool{null}"),
            DType::Primitive(pdt, null) => write!(f, "{pdt}{null}"),
            DType::Decimal(ddt, null) => write!(f, "{ddt}{null}"),
            DType::Utf8(null) => write!(f, "utf8{null}"),
            DType::Binary(null) => write!(f, "binary{null}"),
            DType::List(edt, null) => write!(f, "list({}){null}", DTypeDisplay(edt)),
            DType::FixedSizeList(edt, size, null) => {
                write!(f, "fixed_size_list({})[{size}]{null}", DTypeDisplay(edt))
            }
            DType::Map(map, null) => write!(f, "{}{null}", MapDisplay(map)),
            DType::Struct(sf, null) => write!(
                f,
                "{{{}}}{null}",
                sf.names()
                    .iter()
                    .zip(sf.fields())
                    .map(|(name, dt)| format!("{name}={}", DTypeDisplay(&dt)))
                    .join(", ")
            ),
            DType::Union(uv, null) => write!(f, "union({}){null}", UnionDisplay(uv)),
            DType::Variant(null) => write!(f, "variant{null}"),
            DType::Extension(ext) => write!(f, "{}", ExtensionDisplay(ext)),
        }
    }
}

impl fmt::Display for StructDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            pretty_struct(self.0, f, 0)
        } else {
            write!(
                f,
                "{{{}}}",
                self.0
                    .names()
                    .iter()
                    .zip(self.0.fields())
                    .map(|(name, dt)| format!("{name}={}", DTypeDisplay(&dt)))
                    .join(", ")
            )
        }
    }
}

fn pretty_struct(fields: &StructFields, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
    let indent = "  ".repeat(depth);
    let inner_indent = "  ".repeat(depth + 1);
    writeln!(f, "{{")?;
    for (index, (name, dtype)) in fields.names().iter().zip(fields.fields()).enumerate() {
        if index > 0 {
            writeln!(f, ",")?;
        }
        write!(f, "{inner_indent}{name}=")?;
        pretty_dtype(f, &dtype, depth + 1)?;
    }
    if !fields.names().is_empty() {
        writeln!(f)?;
    }
    write!(f, "{indent}}}")
}

fn pretty_dtype(f: &mut fmt::Formatter<'_>, dtype: &DType, depth: usize) -> fmt::Result {
    match dtype {
        DType::Struct(fields, nullable) => {
            pretty_struct(fields, f, depth)?;
            write!(f, "{nullable}")
        }
        DType::List(inner, nullable) => {
            write!(f, "list(")?;
            pretty_dtype(f, inner, depth)?;
            write!(f, "){nullable}")
        }
        DType::FixedSizeList(inner, size, nullable) => {
            write!(f, "fixed_size_list(")?;
            pretty_dtype(f, inner, depth)?;
            write!(f, ")[{size}]{nullable}")
        }
        _ => write!(f, "{}", DTypeDisplay(dtype)),
    }
}

impl fmt::Display for MapDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "map({}, {}, keys_sorted={})",
            DTypeDisplay(&self.0.key_dtype()),
            DTypeDisplay(&self.0.value_dtype()),
            self.0.keys_sorted()
        )
    }
}

impl fmt::Display for UnionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let show_tags = !self.0.is_consecutive();
        write!(
            f,
            "{}",
            self.0
                .names()
                .iter()
                .zip(self.0.variants())
                .zip(self.0.type_ids().iter())
                .map(|((name, dtype), tag)| if show_tags {
                    format!("{name}@{tag}={}", DTypeDisplay(&dtype))
                } else {
                    format!("{name}={}", DTypeDisplay(&dtype))
                })
                .join(", ")
        )
    }
}

impl fmt::Display for ExtensionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let metadata = self.0.display_metadata().to_string();
        if metadata.is_empty() {
            write!(f, "{}", self.0.id())?;
        } else {
            write!(f, "{}[{}]", self.0.id(), metadata)?;
        }
        write!(f, "({})", DTypeDisplay(self.0.storage_dtype()))
    }
}
