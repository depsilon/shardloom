// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Native schema writing with borrowed parent continuations. Builder actions
//! retain their original order, including names before children and extension
//! metadata after storage. This does not change physical read verification.

use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use vortex_error::{VortexExpect, VortexResult};

use crate::dtype::extension::ExtDTypeRef;
use crate::dtype::{DType, FieldDType, FieldNames, MapDType, Nullability, flatbuffers as fb};

type Offset<'fb> = WIPOffset<fb::DType<'fb>>;
type Names<'fb> = WIPOffset<Vector<'fb, ForwardsUOffset<&'fb str>>>;

enum Parent<'a, 'fb> {
    List(Nullability),
    FixedSizeList(u32, Nullability),
    MapKey(&'a MapDType, Nullability),
    MapValue(&'a MapDType, Nullability, Offset<'fb>),
    Fields {
        dtype: &'a DType,
        names: Names<'fb>,
        remaining: &'a [FieldDType],
        offsets: Vec<Offset<'fb>>,
    },
    Extension(&'a ExtDTypeRef, WIPOffset<&'fb str>),
}

fn names<'fb>(names: &FieldNames, fbb: &mut FlatBufferBuilder<'fb>) -> Names<'fb> {
    let offsets: Vec<_> = names
        .iter()
        .map(|name| fbb.create_string(name.as_ref()))
        .collect();
    fbb.create_vector(&offsets)
}

fn field<'a>(dtype: &DType, value: &'a FieldDType) -> &'a DType {
    value.value_ref().vortex_expect(match dtype {
        DType::Struct(..) => "field DType must be valid",
        DType::Union(..) => "variant DType must be valid",
        _ => unreachable!("only field-bearing schema frames use this helper"),
    })
}

fn finish<'fb>(
    fbb: &mut FlatBufferBuilder<'fb>,
    tag: fb::Type,
    payload: WIPOffset<flatbuffers::UnionWIPOffset>,
) -> Offset<'fb> {
    fb::DType::create(
        fbb,
        &fb::DTypeArgs {
            type_type: tag,
            type_: Some(payload),
        },
    )
}

fn finish_fields<'fb>(
    dtype: &DType,
    names: Names<'fb>,
    offsets: &[Offset<'fb>],
    fbb: &mut FlatBufferBuilder<'fb>,
) -> Offset<'fb> {
    let dtypes = Some(fbb.create_vector(offsets));
    let (tag, payload) = match dtype {
        DType::Struct(_, nullability) => (
            fb::Type::Struct_,
            fb::Struct_::create(
                fbb,
                &fb::Struct_Args {
                    names: Some(names),
                    dtypes,
                    nullable: (*nullability).into(),
                },
            )
            .as_union_value(),
        ),
        DType::Union(variants, nullability) => {
            let tags: Vec<_> = variants
                .type_ids()
                .iter()
                .copied()
                .map(u8::cast_signed)
                .collect();
            let type_ids = Some(fbb.create_vector(&tags));
            (
                fb::Type::Union,
                fb::Union::create(
                    fbb,
                    &fb::UnionArgs {
                        names: Some(names),
                        dtypes,
                        type_ids,
                        nullable: (*nullability).into(),
                    },
                )
                .as_union_value(),
            )
        }
        _ => unreachable!("only struct/union frames contain field offsets"),
    };
    finish(fbb, tag, payload)
}

pub(super) fn write_dtype<'fb>(
    dtype: &DType,
    fbb: &mut FlatBufferBuilder<'fb>,
) -> VortexResult<Offset<'fb>> {
    let mut current = dtype;
    let mut parents = Vec::new();
    loop {
        let mut completed = match current {
            DType::List(child, nullability) => {
                parents.push(Parent::List(*nullability));
                current = child;
                continue;
            }
            DType::FixedSizeList(child, size, nullability) => {
                parents.push(Parent::FixedSizeList(*size, *nullability));
                current = child;
                continue;
            }
            DType::Map(map, nullability) => {
                parents.push(Parent::MapKey(map, *nullability));
                current = map
                    .try_key_dtype()
                    .vortex_expect("map key dtype must be valid");
                continue;
            }
            DType::Struct(fields, _) => {
                let names = names(fields.names(), fbb);
                if let Some((first, remaining)) = fields.raw_dtypes().split_first() {
                    parents.push(Parent::Fields {
                        dtype: current,
                        names,
                        remaining,
                        offsets: Vec::with_capacity(fields.nfields()),
                    });
                    current = field(current, first);
                    continue;
                }
                finish_fields(current, names, &[], fbb)
            }
            DType::Union(variants, _) => {
                let names = names(variants.names(), fbb);
                if let Some((first, remaining)) = variants.raw_dtypes().split_first() {
                    parents.push(Parent::Fields {
                        dtype: current,
                        names,
                        remaining,
                        offsets: Vec::with_capacity(variants.len()),
                    });
                    current = field(current, first);
                    continue;
                }
                finish_fields(current, names, &[], fbb)
            }
            DType::Extension(extension) => {
                let id = fbb.create_string(extension.id().as_ref());
                parents.push(Parent::Extension(extension, id));
                current = extension.storage_dtype();
                continue;
            }
            DType::Null => {
                let payload = fb::Null::create(fbb, &fb::NullArgs {}).as_union_value();
                finish(fbb, fb::Type::Null, payload)
            }
            DType::Bool(nullability) => {
                let payload = fb::Bool::create(
                    fbb,
                    &fb::BoolArgs {
                        nullable: (*nullability).into(),
                    },
                )
                .as_union_value();
                finish(fbb, fb::Type::Bool, payload)
            }
            DType::Primitive(ptype, nullability) => {
                let payload = fb::Primitive::create(
                    fbb,
                    &fb::PrimitiveArgs {
                        ptype: (*ptype).into(),
                        nullable: (*nullability).into(),
                    },
                )
                .as_union_value();
                finish(fbb, fb::Type::Primitive, payload)
            }
            DType::Decimal(decimal, nullability) => {
                let payload = fb::Decimal::create(
                    fbb,
                    &fb::DecimalArgs {
                        precision: decimal.precision(),
                        scale: decimal.scale(),
                        nullable: (*nullability).into(),
                    },
                )
                .as_union_value();
                finish(fbb, fb::Type::Decimal, payload)
            }
            DType::Utf8(nullability) => {
                let payload = fb::Utf8::create(
                    fbb,
                    &fb::Utf8Args {
                        nullable: (*nullability).into(),
                    },
                )
                .as_union_value();
                finish(fbb, fb::Type::Utf8, payload)
            }
            DType::Binary(nullability) => {
                let payload = fb::Binary::create(
                    fbb,
                    &fb::BinaryArgs {
                        nullable: (*nullability).into(),
                    },
                )
                .as_union_value();
                finish(fbb, fb::Type::Binary, payload)
            }
            DType::Variant(nullability) => {
                let payload = fb::Variant::create(
                    fbb,
                    &fb::VariantArgs {
                        nullable: (*nullability).into(),
                    },
                )
                .as_union_value();
                finish(fbb, fb::Type::Variant, payload)
            }
        };
        loop {
            completed = match parents.pop() {
                None => return Ok(completed),
                Some(Parent::List(nullability)) => {
                    let payload = fb::List::create(
                        fbb,
                        &fb::ListArgs {
                            element_type: Some(completed),
                            nullable: nullability.into(),
                        },
                    )
                    .as_union_value();
                    finish(fbb, fb::Type::List, payload)
                }
                Some(Parent::FixedSizeList(size, nullability)) => {
                    let payload = fb::FixedSizeList::create(
                        fbb,
                        &fb::FixedSizeListArgs {
                            element_type: Some(completed),
                            size,
                            nullable: nullability.into(),
                        },
                    )
                    .as_union_value();
                    finish(fbb, fb::Type::FixedSizeList, payload)
                }
                Some(Parent::MapKey(map, nullability)) => {
                    parents.push(Parent::MapValue(map, nullability, completed));
                    current = map
                        .try_value_dtype()
                        .vortex_expect("map value dtype must be valid");
                    break;
                }
                Some(Parent::MapValue(map, nullability, key_type)) => {
                    let payload = fb::Map::create(
                        fbb,
                        &fb::MapArgs {
                            key_type: Some(key_type),
                            value_type: Some(completed),
                            keys_sorted: map.keys_sorted(),
                            nullable: nullability.into(),
                        },
                    )
                    .as_union_value();
                    finish(fbb, fb::Type::Map, payload)
                }
                Some(Parent::Fields {
                    dtype,
                    names,
                    remaining,
                    mut offsets,
                }) => {
                    offsets.push(completed);
                    if let Some((first, remaining)) = remaining.split_first() {
                        parents.push(Parent::Fields {
                            dtype,
                            names,
                            remaining,
                            offsets,
                        });
                        current = field(dtype, first);
                        break;
                    }
                    finish_fields(dtype, names, &offsets, fbb)
                }
                Some(Parent::Extension(extension, id)) => {
                    let metadata = Some(fbb.create_vector(&extension.serialize_metadata()?));
                    let payload = fb::Extension::create(
                        fbb,
                        &fb::ExtensionArgs {
                            id: Some(id),
                            storage_dtype: Some(completed),
                            metadata,
                        },
                    )
                    .as_union_value();
                    finish(fbb, fb::Type::Extension, payload)
                }
            };
        }
    }
}

#[cfg(test)]
#[path = "flatbuffers_write_tests.rs"]
mod tests;
