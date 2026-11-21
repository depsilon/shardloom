// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Shallow reference from the pre-iterative logical reader at 62cf249a5.
// Map keys recurse explicitly here so the reference cannot enter the new parser.

use super::super::super::ViewedDType;
use crate::dtype::extension::{ExtId, ForeignExtDType};
use crate::dtype::session::DTypeSessionExt;
use crate::dtype::{DType, DecimalDType, FieldDType, MapDType, StructFields, UnionVariants};
use crate::flatbuffers::{FlatBuffer, dtype as fb};
use std::sync::Arc;
use vortex_error::{VortexError, VortexResult, vortex_err};
use vortex_session::VortexSession;

pub(super) fn read_reference(vfdt: ViewedDType) -> Result<DType, VortexError> {
    let fb = vfdt.flatbuffer();
    match fb.type_type() {
        fb::Type::Null => Ok(DType::Null),
        fb::Type::Bool => Ok(DType::Bool(
            fb.type__as_bool()
                .ok_or_else(|| vortex_err!("failed to parse bool from flatbuffer"))?
                .nullable()
                .into(),
        )),
        fb::Type::Primitive => {
            let fb_primitive = fb
                .type__as_primitive()
                .ok_or_else(|| vortex_err!("failed to parse primitive from flatbuffer"))?;
            Ok(DType::Primitive(
                fb_primitive.ptype().try_into()?,
                fb_primitive.nullable().into(),
            ))
        }
        fb::Type::Decimal => {
            let fb_decimal = fb
                .type__as_decimal()
                .ok_or_else(|| vortex_err!("failed to parse decimal dtype from flatbuffer"))?;
            Ok(DType::Decimal(
                DecimalDType::try_new(fb_decimal.precision(), fb_decimal.scale())?,
                fb_decimal.nullable().into(),
            ))
        }
        fb::Type::Utf8 => Ok(DType::Utf8(
            fb.type__as_utf_8()
                .ok_or_else(|| vortex_err!("failed to parse utf-8 from flatbuffer"))?
                .nullable()
                .into(),
        )),
        fb::Type::Binary => Ok(DType::Binary(
            fb.type__as_binary()
                .ok_or_else(|| vortex_err!("failed to parse binary from flatbuffer"))?
                .nullable()
                .into(),
        )),
        fb::Type::List => {
            let fb_list = fb
                .type__as_list()
                .ok_or_else(|| vortex_err!("failed to parse list from flatbuffer"))?;

            let list_element = fb_list
                .element_type()
                .ok_or_else(|| vortex_err!("failed to parse list element type from flatbuffer"))?;
            let element_dtype = read_reference(ViewedDType::from_fb_loc(
                list_element._tab.loc(),
                vfdt.buffer().clone(),
                vfdt.session.clone(),
            ))?;
            Ok(DType::List(
                Arc::new(element_dtype),
                fb_list.nullable().into(),
            ))
        }
        fb::Type::FixedSizeList => {
            let fb_fixed_size_list = fb
                .type__as_fixed_size_list()
                .ok_or_else(|| vortex_err!("failed to parse fixed-size list from flatbuffer"))?;

            let list_element = fb_fixed_size_list
                .element_type()
                .ok_or_else(|| vortex_err!("failed to parse list element type from flatbuffer"))?;
            let element_dtype = read_reference(ViewedDType::from_fb_loc(
                list_element._tab.loc(),
                vfdt.buffer().clone(),
                vfdt.session.clone(),
            ))?;
            Ok(DType::FixedSizeList(
                Arc::new(element_dtype),
                fb_fixed_size_list.size(),
                fb_fixed_size_list.nullable().into(),
            ))
        }
        fb::Type::Map => {
            let fb_map = fb
                .type__as_map()
                .ok_or_else(|| vortex_err!("failed to parse map from flatbuffer"))?;
            let map = read_map_reference(fb_map, vfdt.buffer().clone(), vfdt.session.clone())?;
            Ok(DType::Map(map, fb_map.nullable().into()))
        }
        fb::Type::Struct_ => {
            let fb_struct = fb
                .type__as_struct_()
                .ok_or_else(|| vortex_err!("failed to parse struct from flatbuffer"))?;
            let struct_dtype =
                StructFields::from_fb(fb_struct, vfdt.buffer().clone(), vfdt.session.clone())?;

            Ok(DType::Struct(struct_dtype, fb_struct.nullable().into()))
        }
        fb::Type::Union => {
            let fb_union = fb
                .type__as_union()
                .ok_or_else(|| vortex_err!("failed to parse union from flatbuffer"))?;
            let variants =
                UnionVariants::from_fb(fb_union, vfdt.buffer().clone(), vfdt.session.clone())?;
            Ok(DType::Union(variants, fb_union.nullable().into()))
        }
        fb::Type::Variant => {
            let fb_variant = fb
                .type__as_variant()
                .ok_or_else(|| vortex_err!("failed to parse variant from flatbuffer"))?;
            Ok(DType::Variant(fb_variant.nullable().into()))
        }
        fb::Type::Extension => {
            let fb_ext = fb
                .type__as_extension()
                .ok_or_else(|| vortex_err!("failed to parse extension from flatbuffer"))?;
            #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
            let id = ExtId::new(
                fb_ext
                    .id()
                    .ok_or_else(|| vortex_err!("failed to parse extension id from flatbuffer"))?,
            );
            let storage_dtype = fb_ext
                .storage_dtype()
                .ok_or_else(|| vortex_err!("storage_dtype must be present on DType fbs message"))?;
            let storage_view = ViewedDType::from_fb_loc(
                storage_dtype._tab.loc(),
                vfdt.buffer().clone(),
                vfdt.session.clone(),
            );
            let storage_dtype = read_reference(storage_view)
                .map_err(|e| e.with_context("failed to create DType from fbs message"))?;

            let metadata = fb_ext
                .metadata()
                .ok_or_else(|| vortex_err!("failed to parse extension metadata from flatbuffer"))?
                .bytes();
            let ext_dtype = if let Some(vtable) = vfdt.session.dtypes().registry().get(&id) {
                vtable.deserialize(metadata, storage_dtype)?
            } else if vfdt.session.allows_unknown() {
                ForeignExtDType::from_parts(id, metadata.to_vec(), storage_dtype)?
            } else {
                return Err(vortex_err!("No such DType extension ID: {}", id));
            };

            Ok(DType::Extension(ext_dtype))
        }
        _ => Err(vortex_err!("Unknown DType variant")),
    }
}

fn read_map_reference(
    fb_map: fb::Map<'_>,
    buffer: FlatBuffer,
    session: VortexSession,
) -> VortexResult<MapDType> {
    let key = fb_map
        .key_type()
        .ok_or_else(|| vortex_err!("failed to parse map key type from flatbuffer"))?;
    let value = fb_map
        .value_type()
        .ok_or_else(|| vortex_err!("failed to parse map value type from flatbuffer"))?;
    let key = ViewedDType::from_fb_loc(key._tab.loc(), buffer.clone(), session.clone());
    let value = ViewedDType::from_fb_loc(value._tab.loc(), buffer, session);
    let parsed = read_reference(key.clone())?;
    MapDType::try_from_fields(
        FieldDType::from_view_initialized(key, parsed),
        FieldDType::from(value),
        fb_map.keys_sorted(),
    )
}
