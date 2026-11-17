// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// ShardLoom test-only recursive reference, adapted from the pinned pre-iteration writer.

use crate::dtype::{DType, flatbuffers as fb};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex_error::VortexResult;

pub(super) fn write_reference<'fb>(
    dtype: &DType,
    fbb: &mut FlatBufferBuilder<'fb>,
) -> VortexResult<WIPOffset<fb::DType<'fb>>> {
    let dtype_union = match dtype {
        DType::Null => fb::Null::create(fbb, &fb::NullArgs {}).as_union_value(),
        DType::Bool(n) => fb::Bool::create(
            fbb,
            &fb::BoolArgs {
                nullable: (*n).into(),
            },
        )
        .as_union_value(),
        DType::Primitive(ptype, n) => fb::Primitive::create(
            fbb,
            &fb::PrimitiveArgs {
                ptype: (*ptype).into(),
                nullable: (*n).into(),
            },
        )
        .as_union_value(),
        DType::Decimal(dt, n) => fb::Decimal::create(
            fbb,
            &fb::DecimalArgs {
                precision: dt.precision(),
                scale: dt.scale(),
                nullable: (*n).into(),
            },
        )
        .as_union_value(),
        DType::Utf8(n) => fb::Utf8::create(
            fbb,
            &fb::Utf8Args {
                nullable: (*n).into(),
            },
        )
        .as_union_value(),
        DType::Binary(n) => fb::Binary::create(
            fbb,
            &fb::BinaryArgs {
                nullable: (*n).into(),
            },
        )
        .as_union_value(),
        DType::List(edt, n) => {
            let element_type = Some(write_reference(edt.as_ref(), fbb)?);
            fb::List::create(
                fbb,
                &fb::ListArgs {
                    element_type,
                    nullable: (*n).into(),
                },
            )
            .as_union_value()
        }
        DType::FixedSizeList(edt, size, n) => {
            let element_type = Some(write_reference(edt.as_ref(), fbb)?);
            fb::FixedSizeList::create(
                fbb,
                &fb::FixedSizeListArgs {
                    element_type,
                    size: *size,
                    nullable: (*n).into(),
                },
            )
            .as_union_value()
        }
        DType::Map(map, n) => {
            let key_type = Some(write_reference(&map.key_dtype(), fbb)?);
            let value_type = Some(write_reference(&map.value_dtype(), fbb)?);
            fb::Map::create(
                fbb,
                &fb::MapArgs {
                    key_type,
                    value_type,
                    keys_sorted: map.keys_sorted(),
                    nullable: (*n).into(),
                },
            )
            .as_union_value()
        }
        DType::Struct(st, n) => {
            let names = st
                .names()
                .iter()
                .map(|n| fbb.create_string(n.as_ref()))
                .collect_vec();
            let names = Some(fbb.create_vector(&names));

            let dtypes = st
                .fields()
                .map(|dtype| write_reference(&dtype, fbb))
                .collect::<VortexResult<Vec<_>>>()?;
            let dtypes = Some(fbb.create_vector(&dtypes));

            fb::Struct_::create(
                fbb,
                &fb::Struct_Args {
                    names,
                    dtypes,
                    nullable: (*n).into(),
                },
            )
            .as_union_value()
        }
        DType::Union(uv, n) => {
            let names = uv
                .names()
                .iter()
                .map(|name| fbb.create_string(name.as_ref()))
                .collect_vec();
            let names = Some(fbb.create_vector(&names));

            let dtypes = uv
                .variants()
                .map(|dtype| write_reference(&dtype, fbb))
                .collect::<VortexResult<Vec<_>>>()?;
            let dtypes = Some(fbb.create_vector(&dtypes));

            // The FlatBuffers schema retains its original signed byte wire type for backwards
            // compatibility. Reinterpreting the bits preserves the full u8 tag range.
            let type_ids = uv
                .type_ids()
                .iter()
                .copied()
                .map(u8::cast_signed)
                .collect_vec();
            let type_ids = Some(fbb.create_vector(&type_ids));

            fb::Union::create(
                fbb,
                &fb::UnionArgs {
                    names,
                    dtypes,
                    type_ids,
                    nullable: (*n).into(),
                },
            )
            .as_union_value()
        }
        DType::Variant(n) => fb::Variant::create(
            fbb,
            &fb::VariantArgs {
                nullable: (*n).into(),
            },
        )
        .as_union_value(),
        DType::Extension(ext) => {
            let id = Some(fbb.create_string(ext.id().as_ref()));
            let storage_dtype = Some(write_reference(ext.storage_dtype(), fbb)?);
            let metadata = Some(fbb.create_vector(&ext.serialize_metadata()?));
            fb::Extension::create(
                fbb,
                &fb::ExtensionArgs {
                    id,
                    storage_dtype,
                    metadata,
                },
            )
            .as_union_value()
        }
    };

    let dtype_type = match dtype {
        DType::Null => fb::Type::Null,
        DType::Bool(_) => fb::Type::Bool,
        DType::Primitive(..) => fb::Type::Primitive,
        DType::Decimal(..) => fb::Type::Decimal,
        DType::Utf8(_) => fb::Type::Utf8,
        DType::Binary(_) => fb::Type::Binary,
        DType::List(..) => fb::Type::List,
        DType::FixedSizeList(..) => fb::Type::FixedSizeList,
        DType::Map(..) => fb::Type::Map,
        DType::Struct(..) => fb::Type::Struct_,
        DType::Union(..) => fb::Type::Union,
        DType::Variant(_) => fb::Type::Variant,
        DType::Extension { .. } => fb::Type::Extension,
    };

    Ok(fb::DType::create(
        fbb,
        &fb::DTypeArgs {
            type_type: dtype_type,
            type_: Some(dtype_union),
        },
    ))
}
