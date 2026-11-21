// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

// Original recursive converters, adapted only to standalone test functions.
// Exact source adaptation is checked by check_dtype_proto_reference.py.

use crate::dtype::extension::{ExtId, ForeignExtDType};
use crate::dtype::proto::dtype::{self as pb, d_type::DtypeType};
use crate::dtype::session::DTypeSessionExt;
use crate::dtype::{DType, DecimalDType, MapDType, StructFields, UnionVariants};
use std::sync::Arc;
use vortex_error::{VortexResult, vortex_err};
use vortex_session::VortexSession;

pub(crate) fn read(value: &pb::DType, session: &VortexSession) -> VortexResult<DType> {
    match value
        .dtype_type
        .as_ref()
        .ok_or_else(|| vortex_err!("Unrecognized DType"))?
    {
        DtypeType::Null(_) => Ok(DType::Null),
        DtypeType::Bool(b) => Ok(DType::Bool(b.nullable.into())),
        DtypeType::Primitive(p) => {
            let ptype = pb::PType::try_from(p.r#type).map_err(|_| {
                vortex_err!(
                    "protobuf Primitive type must be a known PType, got {}",
                    p.r#type
                )
            })?;
            Ok(DType::Primitive(ptype.into(), p.nullable.into()))
        }
        DtypeType::Decimal(d) => Ok(DType::Decimal(
            DecimalDType::try_new(
                d.precision
                    .try_into()
                    .map_err(|_| vortex_err!("proto precision could not be downcast to u8"))?,
                d.scale
                    .try_into()
                    .map_err(|_| vortex_err!("proto scale could not be downcast to i8"))?,
            )?,
            d.nullable.into(),
        )),
        DtypeType::Utf8(u) => Ok(DType::Utf8(u.nullable.into())),
        DtypeType::Binary(b) => Ok(DType::Binary(b.nullable.into())),
        DtypeType::List(l) => {
            let nullable = l.nullable.into();
            Ok(DType::List(
                read(
                    l.element_type
                        .as_ref()
                        .ok_or_else(|| vortex_err!("Invalid list element type"))?
                        .as_ref(),
                    session,
                )
                .map(Arc::new)?,
                nullable,
            ))
        }
        DtypeType::FixedSizeList(fsl) => {
            let nullable = fsl.nullable.into();
            Ok(DType::FixedSizeList(
                read(
                    fsl.element_type
                        .as_ref()
                        .ok_or_else(|| vortex_err!("Invalid fixed-size list element type"))?
                        .as_ref(),
                    session,
                )
                .map(Arc::new)?,
                fsl.size,
                nullable,
            ))
        }
        DtypeType::Map(map) => Ok(DType::Map(
            MapDType::try_new(
                read(
                    map.key_type
                        .as_ref()
                        .ok_or_else(|| vortex_err!("Invalid map key type"))?
                        .as_ref(),
                    session,
                )?,
                read(
                    map.value_type
                        .as_ref()
                        .ok_or_else(|| vortex_err!("Invalid map value type"))?
                        .as_ref(),
                    session,
                )?,
                map.keys_sorted,
            )?,
            map.nullable.into(),
        )),
        DtypeType::Struct(s) => Ok(DType::Struct(
            StructFields::new(
                s.names.iter().map(|s| s.as_str()).collect(),
                s.dtypes
                    .iter()
                    .map(|dt| read(dt, session))
                    .collect::<VortexResult<Vec<_>>>()?,
            ),
            s.nullable.into(),
        )),
        DtypeType::Union(u) => {
            let names = u.names.iter().map(|s| s.as_str()).collect();
            let dtypes = u
                .dtypes
                .iter()
                .map(|dt| read(dt, session))
                .collect::<VortexResult<Vec<_>>>()?;
            let type_ids = u
                .type_ids
                .iter()
                .map(|t| {
                    u8::try_from(*t)
                        .map_err(|_| vortex_err!("Union type_id {t} somehow does not fit in u8"))
                })
                .collect::<VortexResult<Vec<_>>>()?;
            let variants = UnionVariants::try_new(names, dtypes, type_ids)?;
            Ok(DType::Union(variants, u.nullable.into()))
        }
        DtypeType::Variant(v) => Ok(DType::Variant(v.nullable.into())),
        DtypeType::Extension(e) => {
            #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
            let id = ExtId::new(e.id.as_str());
            let storage_dtype = read(
                e.storage_dtype
                    .as_ref()
                    .ok_or_else(|| vortex_err!("Extension DType missing storage proto"))?,
                session,
            )?;
            let ext_dtype = if let Some(vtable) = session.dtypes().registry().get(&id) {
                vtable.deserialize(e.metadata(), storage_dtype)?
            } else if session.allows_unknown() {
                ForeignExtDType::from_parts(id, e.metadata().to_vec(), storage_dtype)?
            } else {
                return Err(vortex_err!("Unregistered extension type ID: {}", e.id));
            };
            Ok(DType::Extension(ext_dtype))
        }
    }
}

pub(crate) fn write(value: &DType) -> VortexResult<pb::DType> {
    Ok(pb::DType {
        dtype_type: Some(match value {
            DType::Null => DtypeType::Null(pb::Null {}),
            DType::Bool(null) => DtypeType::Bool(pb::Bool {
                nullable: (*null).into(),
            }),
            DType::Primitive(ptype, null) => DtypeType::Primitive(pb::Primitive {
                r#type: pb::PType::from(*ptype).into(),
                nullable: (*null).into(),
            }),
            DType::Decimal(decimal, null) => DtypeType::Decimal(pb::Decimal {
                precision: decimal.precision() as u32,
                scale: decimal.scale() as i32,
                nullable: (*null).into(),
            }),
            DType::Utf8(null) => DtypeType::Utf8(pb::Utf8 {
                nullable: (*null).into(),
            }),
            DType::Binary(null) => DtypeType::Binary(pb::Binary {
                nullable: (*null).into(),
            }),
            DType::List(edt, null) => DtypeType::List(Box::new(pb::List {
                element_type: Some(Box::new(write(edt.as_ref())?)),
                nullable: (*null).into(),
            })),
            DType::FixedSizeList(edt, size, null) => {
                DtypeType::FixedSizeList(Box::new(pb::FixedSizeList {
                    element_type: Some(Box::new(write(edt.as_ref())?)),
                    size: *size,
                    nullable: (*null).into(),
                }))
            }
            DType::Map(map, null) => {
                let key_dtype = map.key_dtype();
                let value_dtype = map.value_dtype();
                DtypeType::Map(Box::new(pb::Map {
                    key_type: Some(Box::new(write(&key_dtype)?)),
                    value_type: Some(Box::new(write(&value_dtype)?)),
                    keys_sorted: map.keys_sorted(),
                    nullable: (*null).into(),
                }))
            }
            DType::Struct(s, null) => DtypeType::Struct(pb::Struct {
                names: s.names().iter().map(|s| s.as_ref().to_string()).collect(),
                dtypes: s
                    .fields()
                    .map(|d| write(&d))
                    .collect::<VortexResult<Vec<_>>>()?,
                nullable: (*null).into(),
            }),
            DType::Union(uv, null) => DtypeType::Union(pb::Union {
                names: uv.names().iter().map(|n| n.as_ref().to_string()).collect(),
                dtypes: uv
                    .variants()
                    .map(|d| write(&d))
                    .collect::<VortexResult<Vec<_>>>()?,
                type_ids: uv.type_ids().iter().map(|t| *t as i32).collect(),
                nullable: (*null).into(),
            }),
            DType::Variant(null) => DtypeType::Variant(pb::Variant {
                nullable: (*null).into(),
            }),
            DType::Extension(e) => DtypeType::Extension(Box::new(pb::Extension {
                id: e.id().as_ref().into(),
                storage_dtype: Some(Box::new(write(e.storage_dtype())?)),
                metadata: Some(e.serialize_metadata()?),
            })),
        }),
    })
}
