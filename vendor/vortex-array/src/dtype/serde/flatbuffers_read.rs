// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Logical schema parsing over physically verified bytes. Only eager edges
//! enter the continuation stack; lazy fields retain their original views.

use std::sync::Arc;

use vortex_error::{VortexError, VortexResult, vortex_err};

use super::ViewedDType;
use crate::dtype::extension::{ExtId, ForeignExtDType};
use crate::dtype::session::DTypeSessionExt;
use crate::dtype::{
    DType, DecimalDType, FieldDType, MapDType, Nullability, StructFields, UnionVariants,
    flatbuffers as fb,
};

enum Parent<'a> {
    List(Nullability),
    FixedSizeList(u32, Nullability),
    MapKey {
        key: fb::DType<'a>,
        value: fb::DType<'a>,
        sorted: bool,
        nullable: Nullability,
    },
    Extension(ExtId, fb::Extension<'a>),
}

fn child_view(dtype: fb::DType<'_>, source: &ViewedDType) -> ViewedDType {
    ViewedDType::from_fb_loc(
        dtype._tab.loc(),
        source.buffer().clone(),
        source.session.clone(),
    )
}

pub(super) fn read_dtype(view: ViewedDType) -> VortexResult<DType> {
    let mut parents = Vec::new();
    read_inner(&view, &mut parents).map_err(|mut error| {
        // An extension frame is removed before its own metadata is decoded.
        // Only still-pending storage ancestors add the old recursive context.
        for parent in parents.iter().rev() {
            if matches!(parent, Parent::Extension(..)) {
                error = error.with_context("failed to create DType from fbs message");
            }
        }
        error
    })
}

fn read_inner<'a>(view: &'a ViewedDType, parents: &mut Vec<Parent<'a>>) -> VortexResult<DType> {
    let mut current = view.flatbuffer();
    loop {
        let mut completed = match current.type_type() {
            fb::Type::List => {
                let list = current
                    .type__as_list()
                    .ok_or_else(|| vortex_err!("failed to parse list from flatbuffer"))?;
                let child = list.element_type().ok_or_else(|| {
                    vortex_err!("failed to parse list element type from flatbuffer")
                })?;
                parents.push(Parent::List(list.nullable().into()));
                current = child;
                continue;
            }
            fb::Type::FixedSizeList => {
                let list = current.type__as_fixed_size_list().ok_or_else(|| {
                    vortex_err!("failed to parse fixed-size list from flatbuffer")
                })?;
                let child = list.element_type().ok_or_else(|| {
                    vortex_err!("failed to parse list element type from flatbuffer")
                })?;
                parents.push(Parent::FixedSizeList(list.size(), list.nullable().into()));
                current = child;
                continue;
            }
            fb::Type::Map => {
                let map = current
                    .type__as_map()
                    .ok_or_else(|| vortex_err!("failed to parse map from flatbuffer"))?;
                let key = map
                    .key_type()
                    .ok_or_else(|| vortex_err!("failed to parse map key type from flatbuffer"))?;
                let value = map
                    .value_type()
                    .ok_or_else(|| vortex_err!("failed to parse map value type from flatbuffer"))?;
                parents.push(Parent::MapKey {
                    key,
                    value,
                    sorted: map.keys_sorted(),
                    nullable: map.nullable().into(),
                });
                current = key;
                continue;
            }
            fb::Type::Extension => {
                let extension = current
                    .type__as_extension()
                    .ok_or_else(|| vortex_err!("failed to parse extension from flatbuffer"))?;
                #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
                let id =
                    ExtId::new(extension.id().ok_or_else(|| {
                        vortex_err!("failed to parse extension id from flatbuffer")
                    })?);
                let child = extension.storage_dtype().ok_or_else(|| {
                    vortex_err!("storage_dtype must be present on DType fbs message")
                })?;
                parents.push(Parent::Extension(id, extension));
                current = child;
                continue;
            }
            fb::Type::Struct_ => {
                let fields = current
                    .type__as_struct_()
                    .ok_or_else(|| vortex_err!("failed to parse struct from flatbuffer"))?;
                DType::Struct(
                    StructFields::from_fb(fields, view.buffer().clone(), view.session.clone())?,
                    fields.nullable().into(),
                )
            }
            fb::Type::Union => {
                let variants = current
                    .type__as_union()
                    .ok_or_else(|| vortex_err!("failed to parse union from flatbuffer"))?;
                DType::Union(
                    UnionVariants::from_fb(variants, view.buffer().clone(), view.session.clone())?,
                    variants.nullable().into(),
                )
            }
            _ => read_leaf(current)?,
        };
        loop {
            completed = match parents.pop() {
                None => return Ok(completed),
                Some(Parent::List(nullable)) => DType::List(Arc::new(completed), nullable),
                Some(Parent::FixedSizeList(size, nullable)) => {
                    DType::FixedSizeList(Arc::new(completed), size, nullable)
                }
                Some(Parent::MapKey {
                    key,
                    value,
                    sorted,
                    nullable,
                }) => DType::Map(
                    MapDType::try_from_fields(
                        FieldDType::from_view_initialized(child_view(key, view), completed),
                        FieldDType::from(child_view(value, view)),
                        sorted,
                    )?,
                    nullable,
                ),
                Some(Parent::Extension(id, extension)) => {
                    let metadata = extension
                        .metadata()
                        .ok_or_else(|| {
                            vortex_err!("failed to parse extension metadata from flatbuffer")
                        })?
                        .bytes();
                    let dtype = if let Some(vtable) = view.session.dtypes().registry().get(&id) {
                        vtable.deserialize(metadata, completed)?
                    } else if view.session.allows_unknown() {
                        ForeignExtDType::from_parts(id, metadata.to_vec(), completed)?
                    } else {
                        return Err(vortex_err!("No such DType extension ID: {}", id));
                    };
                    DType::Extension(dtype)
                }
            };
        }
    }
}

fn read_leaf(dtype: fb::DType<'_>) -> Result<DType, VortexError> {
    Ok(match dtype.type_type() {
        fb::Type::Null => DType::Null,
        fb::Type::Bool => DType::Bool(
            dtype
                .type__as_bool()
                .ok_or_else(|| vortex_err!("failed to parse bool from flatbuffer"))?
                .nullable()
                .into(),
        ),
        fb::Type::Primitive => {
            let primitive = dtype
                .type__as_primitive()
                .ok_or_else(|| vortex_err!("failed to parse primitive from flatbuffer"))?;
            DType::Primitive(primitive.ptype().try_into()?, primitive.nullable().into())
        }
        fb::Type::Decimal => {
            let decimal = dtype
                .type__as_decimal()
                .ok_or_else(|| vortex_err!("failed to parse decimal dtype from flatbuffer"))?;
            DType::Decimal(
                DecimalDType::try_new(decimal.precision(), decimal.scale())?,
                decimal.nullable().into(),
            )
        }
        fb::Type::Utf8 => DType::Utf8(
            dtype
                .type__as_utf_8()
                .ok_or_else(|| vortex_err!("failed to parse utf-8 from flatbuffer"))?
                .nullable()
                .into(),
        ),
        fb::Type::Binary => DType::Binary(
            dtype
                .type__as_binary()
                .ok_or_else(|| vortex_err!("failed to parse binary from flatbuffer"))?
                .nullable()
                .into(),
        ),
        fb::Type::Variant => DType::Variant(
            dtype
                .type__as_variant()
                .ok_or_else(|| vortex_err!("failed to parse variant from flatbuffer"))?
                .nullable()
                .into(),
        ),
        _ => return Err(vortex_err!("Unknown DType variant")),
    })
}

#[cfg(test)]
#[path = "flatbuffers_read_tests.rs"]
mod tests;
