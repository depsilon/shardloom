// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered conversion into the existing protobuf model. Wire encoding is separate.

use vortex_error::{VortexExpect, VortexResult};

use crate::dtype::extension::ExtDTypeRef;
use crate::dtype::proto::dtype::{self as pb, d_type::DtypeType};
use crate::dtype::{DType, DTypeChildren, Nullability};

enum Parent<'a> {
    List(Nullability),
    FixedList(u32, Nullability),
    MapKey {
        value: &'a DType,
        sorted: bool,
        nullable: Nullability,
    },
    MapValue {
        key: pb::DType,
        sorted: bool,
        nullable: Nullability,
    },
    Fields {
        owner: &'a DType,
        children: DTypeChildren<'a>,
        names: Vec<String>,
        dtypes: Vec<pb::DType>,
    },
    Extension {
        extension: &'a ExtDTypeRef,
        id: String,
    },
}

enum Step<'a> {
    Child(&'a DType, Parent<'a>),
    Ready(pb::DType),
}

fn ready(kind: DtypeType) -> Step<'static> {
    Step::Ready(pb::DType {
        dtype_type: Some(kind),
    })
}

fn field<'a>(owner: &DType, children: &mut DTypeChildren<'a>) -> Option<&'a DType> {
    children.next().map(|child| match owner {
        DType::Struct(..) => child.vortex_expect("field DType must be valid"),
        DType::Union(..) => child.vortex_expect("variant DType must be valid"),
        _ => unreachable!(),
    })
}

fn fields(owner: &DType, names: Vec<String>, dtypes: Vec<pb::DType>) -> Step<'static> {
    ready(match owner {
        DType::Struct(_, nullable) => DtypeType::Struct(pb::Struct {
            names,
            dtypes,
            nullable: (*nullable).into(),
        }),
        DType::Union(variants, nullable) => DtypeType::Union(pb::Union {
            names,
            dtypes,
            type_ids: variants
                .type_ids()
                .iter()
                .map(|tag| i32::from(*tag))
                .collect(),
            nullable: (*nullable).into(),
        }),
        _ => unreachable!(),
    })
}

fn enter(dtype: &DType) -> Step<'_> {
    match dtype {
        DType::List(child, nullable) => Step::Child(child, Parent::List(*nullable)),
        DType::FixedSizeList(child, size, nullable) => {
            Step::Child(child, Parent::FixedList(*size, *nullable))
        }
        DType::Map(map, nullable) => {
            // The original resolves both lazy dtypes before converting the key.
            let key = map
                .try_key_dtype()
                .vortex_expect("map key dtype must be valid");
            let value = map
                .try_value_dtype()
                .vortex_expect("map value dtype must be valid");
            Step::Child(
                key,
                Parent::MapKey {
                    value,
                    sorted: map.keys_sorted(),
                    nullable: *nullable,
                },
            )
        }
        DType::Struct(..) | DType::Union(..) => {
            let names = match dtype {
                DType::Struct(fields, _) => fields.names(),
                DType::Union(variants, _) => variants.names(),
                _ => unreachable!(),
            }
            .iter()
            .map(|name| name.as_ref().to_string())
            .collect();
            let mut children = dtype.try_child_dtypes();
            if let Some(child) = field(dtype, &mut children) {
                Step::Child(
                    child,
                    Parent::Fields {
                        owner: dtype,
                        children,
                        names,
                        dtypes: Vec::new(),
                    },
                )
            } else {
                fields(dtype, names, Vec::new())
            }
        }
        DType::Extension(extension) => {
            let id = extension.id().as_ref().into();
            Step::Child(
                extension.storage_dtype(),
                Parent::Extension { extension, id },
            )
        }
        _ => ready(leaf(dtype)),
    }
}

fn leaf(dtype: &DType) -> DtypeType {
    match dtype {
        DType::Null => DtypeType::Null(pb::Null {}),
        DType::Bool(nullable) => DtypeType::Bool(pb::Bool {
            nullable: (*nullable).into(),
        }),
        DType::Primitive(ptype, nullable) => DtypeType::Primitive(pb::Primitive {
            r#type: pb::PType::from(*ptype).into(),
            nullable: (*nullable).into(),
        }),
        DType::Decimal(decimal, nullable) => DtypeType::Decimal(pb::Decimal {
            precision: u32::from(decimal.precision()),
            scale: i32::from(decimal.scale()),
            nullable: (*nullable).into(),
        }),
        DType::Utf8(nullable) => DtypeType::Utf8(pb::Utf8 {
            nullable: (*nullable).into(),
        }),
        DType::Binary(nullable) => DtypeType::Binary(pb::Binary {
            nullable: (*nullable).into(),
        }),
        DType::Variant(nullable) => DtypeType::Variant(pb::Variant {
            nullable: (*nullable).into(),
        }),
        DType::List(..)
        | DType::FixedSizeList(..)
        | DType::Map(..)
        | DType::Struct(..)
        | DType::Union(..)
        | DType::Extension(_) => unreachable!(),
    }
}

fn complete(parent: Parent<'_>, child: pb::DType) -> VortexResult<Step<'_>> {
    Ok(match parent {
        Parent::List(nullable) => ready(DtypeType::List(Box::new(pb::List {
            element_type: Some(Box::new(child)),
            nullable: nullable.into(),
        }))),
        Parent::FixedList(size, nullable) => {
            ready(DtypeType::FixedSizeList(Box::new(pb::FixedSizeList {
                element_type: Some(Box::new(child)),
                size,
                nullable: nullable.into(),
            })))
        }
        Parent::MapKey {
            value,
            sorted,
            nullable,
        } => Step::Child(
            value,
            Parent::MapValue {
                key: child,
                sorted,
                nullable,
            },
        ),
        Parent::MapValue {
            key,
            sorted,
            nullable,
        } => ready(DtypeType::Map(Box::new(pb::Map {
            key_type: Some(Box::new(key)),
            value_type: Some(Box::new(child)),
            keys_sorted: sorted,
            nullable: nullable.into(),
        }))),
        Parent::Fields {
            owner,
            mut children,
            names,
            mut dtypes,
        } => {
            dtypes.push(child);
            if let Some(child) = field(owner, &mut children) {
                Step::Child(
                    child,
                    Parent::Fields {
                        owner,
                        children,
                        names,
                        dtypes,
                    },
                )
            } else {
                fields(owner, names, dtypes)
            }
        }
        Parent::Extension { extension, id } => {
            ready(DtypeType::Extension(Box::new(pb::Extension {
                id,
                storage_dtype: Some(Box::new(child)),
                metadata: Some(extension.serialize_metadata()?),
            })))
        }
    })
}

pub(super) fn write_dtype(dtype: &DType) -> VortexResult<pb::DType> {
    let mut pending = Vec::new();
    let mut step = enter(dtype);
    loop {
        step = match step {
            Step::Child(child, parent) => {
                pending.push(parent);
                enter(child)
            }
            Step::Ready(child) => match pending.pop() {
                Some(parent) => complete(parent, child)?,
                None => return Ok(child),
            },
        };
    }
}
