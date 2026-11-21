// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered native reconstruction from the existing protobuf object model.

use std::sync::Arc;

use vortex_error::{VortexResult, vortex_err};
use vortex_session::VortexSession;

use crate::dtype::extension::{ExtId, ForeignExtDType};
use crate::dtype::proto::dtype::{self as pb, d_type::DtypeType};
use crate::dtype::session::DTypeSessionExt;
use crate::dtype::{DType, DecimalDType, FieldNames, MapDType, StructFields, UnionVariants};

#[derive(Clone, Copy)]
enum Fields<'a> {
    Struct(&'a pb::Struct),
    Union(&'a pb::Union),
}

impl<'a> Fields<'a> {
    fn children(self) -> &'a [pb::DType] {
        match self {
            Self::Struct(fields) => &fields.dtypes,
            Self::Union(fields) => &fields.dtypes,
        }
    }

    fn names(self) -> FieldNames {
        match self {
            Self::Struct(fields) => &fields.names,
            Self::Union(fields) => &fields.names,
        }
        .iter()
        .map(String::as_str)
        .collect()
    }

    fn finish(self, names: FieldNames, dtypes: Vec<DType>) -> VortexResult<DType> {
        Ok(match self {
            Self::Struct(fields) => {
                DType::Struct(StructFields::new(names, dtypes), fields.nullable.into())
            }
            Self::Union(fields) => {
                let tags = fields
                    .type_ids
                    .iter()
                    .map(|tag| {
                        u8::try_from(*tag).map_err(|_| {
                            vortex_err!("Union type_id {tag} somehow does not fit in u8")
                        })
                    })
                    .collect::<VortexResult<Vec<_>>>()?;
                DType::Union(
                    UnionVariants::try_new(names, dtypes, tags)?,
                    fields.nullable.into(),
                )
            }
        })
    }
}

enum Parent<'a> {
    List(bool),
    FixedList(u32, bool),
    MapKey(&'a pb::Map),
    MapValue(&'a pb::Map, DType),
    Fields {
        fields: Fields<'a>,
        next: usize,
        names: FieldNames,
        dtypes: Vec<DType>,
    },
    Extension(&'a pb::Extension, ExtId),
}

// A partial native subtree can own extension metadata. Unwind inside-out,
// matching the recursive call chain rather than Vec's forward element order.
#[derive(Default)]
struct Pending<'a>(Vec<Parent<'a>>);

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        while let Some(parent) = self.0.pop() {
            drop(parent);
        }
    }
}

enum Step<'a> {
    Child(&'a pb::DType, Parent<'a>),
    Ready(DType),
}

fn fields(fields: Fields<'_>) -> VortexResult<Step<'_>> {
    let names = fields.names();
    if let Some(child) = fields.children().first() {
        Ok(Step::Child(
            child,
            Parent::Fields {
                fields,
                next: 1,
                names,
                dtypes: Vec::new(),
            },
        ))
    } else {
        Ok(Step::Ready(fields.finish(names, Vec::new())?))
    }
}

fn enter(dtype: &pb::DType) -> VortexResult<Step<'_>> {
    let kind = dtype
        .dtype_type
        .as_ref()
        .ok_or_else(|| vortex_err!("Unrecognized DType"))?;
    Ok(match kind {
        DtypeType::List(list) => Step::Child(
            list.element_type
                .as_deref()
                .ok_or_else(|| vortex_err!("Invalid list element type"))?,
            Parent::List(list.nullable),
        ),
        DtypeType::FixedSizeList(list) => Step::Child(
            list.element_type
                .as_deref()
                .ok_or_else(|| vortex_err!("Invalid fixed-size list element type"))?,
            Parent::FixedList(list.size, list.nullable),
        ),
        DtypeType::Map(map) => Step::Child(
            map.key_type
                .as_deref()
                .ok_or_else(|| vortex_err!("Invalid map key type"))?,
            Parent::MapKey(map),
        ),
        DtypeType::Struct(value) => fields(Fields::Struct(value))?,
        DtypeType::Union(value) => fields(Fields::Union(value))?,
        DtypeType::Extension(extension) => {
            #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
            let id = ExtId::new(extension.id.as_str());
            let child = extension
                .storage_dtype
                .as_deref()
                .ok_or_else(|| vortex_err!("Extension DType missing storage proto"))?;
            Step::Child(child, Parent::Extension(extension, id))
        }
        _ => Step::Ready(leaf(kind)?),
    })
}

fn leaf(kind: &DtypeType) -> VortexResult<DType> {
    Ok(match kind {
        DtypeType::Null(_) => DType::Null,
        DtypeType::Bool(value) => DType::Bool(value.nullable.into()),
        DtypeType::Primitive(value) => DType::Primitive(
            pb::PType::try_from(value.r#type)
                .map_err(|_| {
                    vortex_err!(
                        "protobuf Primitive type must be a known PType, got {}",
                        value.r#type
                    )
                })?
                .into(),
            value.nullable.into(),
        ),
        DtypeType::Decimal(value) => DType::Decimal(
            DecimalDType::try_new(
                value
                    .precision
                    .try_into()
                    .map_err(|_| vortex_err!("proto precision could not be downcast to u8"))?,
                value
                    .scale
                    .try_into()
                    .map_err(|_| vortex_err!("proto scale could not be downcast to i8"))?,
            )?,
            value.nullable.into(),
        ),
        DtypeType::Utf8(value) => DType::Utf8(value.nullable.into()),
        DtypeType::Binary(value) => DType::Binary(value.nullable.into()),
        DtypeType::Variant(value) => DType::Variant(value.nullable.into()),
        DtypeType::List(_)
        | DtypeType::FixedSizeList(_)
        | DtypeType::Map(_)
        | DtypeType::Struct(_)
        | DtypeType::Union(_)
        | DtypeType::Extension(_) => unreachable!(),
    })
}

fn complete<'a>(
    parent: Parent<'a>,
    child: DType,
    session: &VortexSession,
) -> VortexResult<Step<'a>> {
    Ok(match parent {
        Parent::List(nullable) => Step::Ready(DType::List(Arc::new(child), nullable.into())),
        Parent::FixedList(size, nullable) => {
            Step::Ready(DType::FixedSizeList(Arc::new(child), size, nullable.into()))
        }
        Parent::MapKey(map) => {
            // A missing value is checked only after the key has converted.
            let value = map
                .value_type
                .as_deref()
                .ok_or_else(|| vortex_err!("Invalid map value type"))?;
            Step::Child(value, Parent::MapValue(map, child))
        }
        Parent::MapValue(map, key) => Step::Ready(DType::Map(
            MapDType::try_new(key, child, map.keys_sorted)?,
            map.nullable.into(),
        )),
        Parent::Fields {
            fields,
            next,
            names,
            mut dtypes,
        } => {
            dtypes.push(child);
            if let Some(child) = fields.children().get(next) {
                Step::Child(
                    child,
                    Parent::Fields {
                        fields,
                        next: next + 1,
                        names,
                        dtypes,
                    },
                )
            } else {
                Step::Ready(fields.finish(names, dtypes)?)
            }
        }
        Parent::Extension(extension, id) => {
            let dtype = if let Some(vtable) = session.dtypes().registry().get(&id) {
                vtable.deserialize(extension.metadata(), child)?
            } else if session.allows_unknown() {
                ForeignExtDType::from_parts(id, extension.metadata().to_vec(), child)?
            } else {
                return Err(vortex_err!(
                    "Unregistered extension type ID: {}",
                    extension.id
                ));
            };
            Step::Ready(DType::Extension(dtype))
        }
    })
}

pub(super) fn read_dtype(dtype: &pb::DType, session: &VortexSession) -> VortexResult<DType> {
    let mut pending = Pending::default();
    let mut step = enter(dtype)?;
    loop {
        step = match step {
            Step::Child(child, parent) => {
                pending.0.push(parent);
                enter(child)?
            }
            Step::Ready(child) => match pending.0.pop() {
                Some(parent) => complete(parent, child, session)?,
                None => return Ok(child),
            },
        };
    }
}
