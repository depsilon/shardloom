// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered physical DType verification through the pinned checked verifier.
//! Each suspended table stays active in the original depth/table/byte budget.

use std::borrow::Cow;
use std::ops::Range;

use flatbuffers::{
    ErrorTraceDetail, ForwardsUOffset, InvalidFlatbuffer, TableVerifier, TableVerifierContinuation,
    Vector, Verifier, append_trace, verify_vector_range,
};

use super::dtype as fb;

type Result<T> = std::result::Result<T, InvalidFlatbuffer>;

#[derive(Clone, Copy)]
enum Kind {
    DType,
    List,
    FixedSizeList,
    Map,
    Struct,
    Union,
    Extension,
}

enum Work {
    Table(Kind, usize),
    Offset(Kind, usize),
    Done,
}

enum After {
    DType,
    List,
    FixedSizeList,
    MapKey,
    MapValue,
    Extension,
    Fields {
        union: bool,
        remaining: Range<usize>,
        next_index: usize,
        field_position: usize,
    },
}

enum Context {
    Union(&'static str, usize),
    Field(&'static str, usize),
    Element {
        field_position: usize,
        position: usize,
        index: usize,
    },
}

impl Context {
    fn trace<T>(self, result: Result<T>) -> Result<T> {
        match self {
            Self::Union(variant, position) => {
                let result = append_trace(
                    result,
                    ErrorTraceDetail::UnionVariant {
                        variant: Cow::Borrowed(variant),
                        position,
                    },
                );
                Self::Field("type_", position).trace(result)
            }
            Self::Field(name, position) => append_trace(
                result,
                ErrorTraceDetail::TableField {
                    field_name: Cow::Borrowed(name),
                    position,
                },
            ),
            Self::Element {
                field_position,
                position,
                index,
            } => {
                let result =
                    append_trace(result, ErrorTraceDetail::VectorElement { index, position });
                Self::Field("dtypes", field_position).trace(result)
            }
        }
    }
}

struct Parent<'opts, 'buf> {
    table: TableVerifierContinuation<'opts, 'buf>,
    after: After,
    context: Context,
}

pub(super) fn verify<'opts, 'buf>(
    verifier: &mut Verifier<'opts, 'buf>,
    position: usize,
) -> Result<()> {
    let mut parents = Vec::new();
    let result = verify_inner(verifier, position, &mut parents);
    // As with the generated recursive verifier, errors abort without finishing
    // pending tables. Append the same child-to-parent contexts during unwind.
    parents
        .into_iter()
        .rev()
        .fold(result, |result, parent| parent.context.trace(result))
}

fn verify_inner<'opts, 'buf>(
    verifier: &mut Verifier<'opts, 'buf>,
    position: usize,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<()> {
    let mut work = Work::Table(Kind::DType, position);
    loop {
        work = match work {
            Work::Offset(kind, position) => {
                let offset = verifier.get_uoffset(position)? as usize;
                Work::Table(kind, position.saturating_add(offset))
            }
            Work::Table(kind, position) => enter(verifier, kind, position, parents)?,
            Work::Done => {
                let Some(parent) = parents.pop() else {
                    return Ok(());
                };
                continue_table(parent.table.resume(verifier), parent.after, parents)?
            }
        };
    }
}

fn suspend<'opts, 'buf>(
    table: TableVerifier<'_, 'opts, 'buf>,
    after: After,
    context: Context,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) {
    parents.push(Parent {
        table: table.suspend(),
        after,
        context,
    });
}

fn dtype_union<'opts, 'buf>(
    table: TableVerifier<'_, 'opts, 'buf>,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<Work> {
    let mut child = None;
    let table = table.visit_union::<fb::Type, _>(
        "type_type",
        fb::DType::VT_TYPE_TYPE,
        "type_",
        fb::DType::VT_TYPE_,
        false,
        |tag, verifier, position| {
            // Leaf tables retain the generated verifier. Their call depth is
            // constant; only compound payloads need a suspended parent.
            let (kind, name) = match tag {
                fb::Type::Null => {
                    return verifier
                        .verify_union_variant::<ForwardsUOffset<fb::Null>>("Type::Null", position);
                }
                fb::Type::Bool => {
                    return verifier
                        .verify_union_variant::<ForwardsUOffset<fb::Bool>>("Type::Bool", position);
                }
                fb::Type::Primitive => {
                    return verifier.verify_union_variant::<ForwardsUOffset<fb::Primitive>>(
                        "Type::Primitive",
                        position,
                    );
                }
                fb::Type::Decimal => {
                    return verifier.verify_union_variant::<ForwardsUOffset<fb::Decimal>>(
                        "Type::Decimal",
                        position,
                    );
                }
                fb::Type::Utf8 => {
                    return verifier
                        .verify_union_variant::<ForwardsUOffset<fb::Utf8>>("Type::Utf8", position);
                }
                fb::Type::Binary => {
                    return verifier.verify_union_variant::<ForwardsUOffset<fb::Binary>>(
                        "Type::Binary",
                        position,
                    );
                }
                fb::Type::Variant => {
                    return verifier.verify_union_variant::<ForwardsUOffset<fb::Variant>>(
                        "Type::Variant",
                        position,
                    );
                }
                fb::Type::List => (Kind::List, "Type::List"),
                fb::Type::FixedSizeList => (Kind::FixedSizeList, "Type::FixedSizeList"),
                fb::Type::Map => (Kind::Map, "Type::Map"),
                fb::Type::Struct_ => (Kind::Struct, "Type::Struct_"),
                fb::Type::Union => (Kind::Union, "Type::Union"),
                fb::Type::Extension => (Kind::Extension, "Type::Extension"),
                // Preserve generated behavior for NONE and unknown tags.
                _ => return Ok(()),
            };
            child = Some((kind, name, position));
            Ok(())
        },
    )?;
    match child {
        Some((kind, name, position)) => {
            suspend(table, After::DType, Context::Union(name, position), parents);
            Ok(Work::Offset(kind, position))
        }
        None => {
            table.finish();
            Ok(Work::Done)
        }
    }
}

fn child_field<'opts, 'buf>(
    mut table: TableVerifier<'_, 'opts, 'buf>,
    field: flatbuffers::VOffsetT,
    name: &'static str,
    after: After,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<Work> {
    if let Some(position) = table.deref(field)? {
        suspend(table, after, Context::Field(name, position), parents);
        Ok(Work::Offset(Kind::DType, position))
    } else {
        // All generated DType child fields are optional. Logical parsing is
        // responsible for declarations that require a present child.
        continue_table(table, after, parents)
    }
}

fn fields<'opts, 'buf>(
    mut table: TableVerifier<'_, 'opts, 'buf>,
    union: bool,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<Work> {
    let (names, dtypes) = if union {
        (fb::Union::VT_NAMES, fb::Union::VT_DTYPES)
    } else {
        (fb::Struct_::VT_NAMES, fb::Struct_::VT_DTYPES)
    };
    table = table
        .visit_field::<ForwardsUOffset<Vector<'_, ForwardsUOffset<&str>>>>("names", names, false)?;
    let field_position = table.deref(dtypes)?;
    let remaining = if let Some(position) = field_position {
        let range = (|| {
            let verifier = table.verifier();
            let offset = verifier.get_uoffset(position)? as usize;
            verify_vector_range::<ForwardsUOffset<fb::DType>>(
                verifier,
                position.saturating_add(offset),
            )
        })();
        Context::Field("dtypes", position).trace(range)?
    } else {
        0..0
    };
    continue_table(
        table,
        After::Fields {
            union,
            remaining,
            next_index: 0,
            // An absent vector never creates an element context.
            field_position: field_position.unwrap_or(0),
        },
        parents,
    )
}

fn enter<'opts, 'buf>(
    verifier: &mut Verifier<'opts, 'buf>,
    kind: Kind,
    position: usize,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<Work> {
    let table = verifier.visit_table(position)?;
    match kind {
        Kind::DType => dtype_union(table, parents),
        Kind::List => child_field(
            table,
            fb::List::VT_ELEMENT_TYPE,
            "element_type",
            After::List,
            parents,
        ),
        Kind::FixedSizeList => child_field(
            table,
            fb::FixedSizeList::VT_ELEMENT_TYPE,
            "element_type",
            After::FixedSizeList,
            parents,
        ),
        Kind::Map => child_field(
            table,
            fb::Map::VT_KEY_TYPE,
            "key_type",
            After::MapKey,
            parents,
        ),
        Kind::Struct => fields(table, false, parents),
        Kind::Union => fields(table, true, parents),
        Kind::Extension => {
            let table =
                table.visit_field::<ForwardsUOffset<&str>>("id", fb::Extension::VT_ID, false)?;
            child_field(
                table,
                fb::Extension::VT_STORAGE_DTYPE,
                "storage_dtype",
                After::Extension,
                parents,
            )
        }
    }
}

fn continue_table<'opts, 'buf>(
    mut table: TableVerifier<'_, 'opts, 'buf>,
    mut after: After,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<Work> {
    loop {
        match after {
            After::DType => {
                table.finish();
                return Ok(Work::Done);
            }
            After::List => {
                table
                    .visit_field::<bool>("nullable", fb::List::VT_NULLABLE, false)?
                    .finish();
                return Ok(Work::Done);
            }
            After::FixedSizeList => {
                table
                    .visit_field::<u32>("size", fb::FixedSizeList::VT_SIZE, false)?
                    .visit_field::<bool>("nullable", fb::FixedSizeList::VT_NULLABLE, false)?
                    .finish();
                return Ok(Work::Done);
            }
            After::MapKey => {
                if let Some(position) = table.deref(fb::Map::VT_VALUE_TYPE)? {
                    suspend(
                        table,
                        After::MapValue,
                        Context::Field("value_type", position),
                        parents,
                    );
                    return Ok(Work::Offset(Kind::DType, position));
                }
                after = After::MapValue;
            }
            After::MapValue => {
                table
                    .visit_field::<bool>("keys_sorted", fb::Map::VT_KEYS_SORTED, false)?
                    .visit_field::<bool>("nullable", fb::Map::VT_NULLABLE, false)?
                    .finish();
                return Ok(Work::Done);
            }
            After::Extension => {
                table
                    .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
                        "metadata",
                        fb::Extension::VT_METADATA,
                        false,
                    )?
                    .finish();
                return Ok(Work::Done);
            }
            After::Fields {
                union,
                mut remaining,
                next_index,
                field_position,
            } => {
                if remaining.start < remaining.end {
                    let position = remaining.start;
                    remaining.start =
                        position.saturating_add(size_of::<ForwardsUOffset<fb::DType>>());
                    suspend(
                        table,
                        After::Fields {
                            union,
                            remaining,
                            next_index: next_index + 1,
                            field_position,
                        },
                        Context::Element {
                            field_position,
                            position,
                            index: next_index,
                        },
                        parents,
                    );
                    return Ok(Work::Offset(Kind::DType, position));
                }
                if union {
                    table
                        .visit_field::<ForwardsUOffset<Vector<'_, i8>>>(
                            "type_ids",
                            fb::Union::VT_TYPE_IDS,
                            false,
                        )?
                        .visit_field::<bool>("nullable", fb::Union::VT_NULLABLE, false)?
                        .finish();
                } else {
                    table
                        .visit_field::<bool>("nullable", fb::Struct_::VT_NULLABLE, false)?
                        .finish();
                }
                return Ok(Work::Done);
            }
        }
    }
}

#[cfg(test)]
#[path = "dtype_verify_tests.rs"]
mod tests;
