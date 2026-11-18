// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! The two public compatibility relations have different field and union rules.
//! Only their borrowed traversal machinery is shared.

use crate::{DataType, Field, FieldRef, UnionFields};

#[derive(Clone, Copy)]
enum Relation {
    TypeOnly,
    Contains,
}

enum Work<'a> {
    Type(&'a DataType, &'a DataType),
    Field(&'a Field, &'a Field),
    FieldTail(&'a Field, &'a Field),
    Fields(&'a [FieldRef], &'a [FieldRef]),
    Union(&'a UnionFields, &'a UnionFields, usize),
    Sorted(bool, bool),
}

pub(crate) fn equals_datatype(left: &DataType, right: &DataType) -> bool {
    relate(Work::Type(left, right), Relation::TypeOnly)
}

pub(crate) fn contains(left: &DataType, right: &DataType) -> bool {
    relate(Work::Type(left, right), Relation::Contains)
}

fn relate(mut current: Work<'_>, relation: Relation) -> bool {
    let mut pending = Vec::new();
    loop {
        match current {
            Work::Type(a, b) => match (a, b) {
                (DataType::List(a), DataType::List(b))
                | (DataType::ListView(a), DataType::ListView(b))
                | (DataType::LargeList(a), DataType::LargeList(b))
                | (DataType::LargeListView(a), DataType::LargeListView(b)) => {
                    current = Work::Field(a, b);
                    continue;
                }
                (DataType::FixedSizeList(a, an), DataType::FixedSizeList(b, bn)) => {
                    if an != bn {
                        return false;
                    }
                    current = Work::Field(a, b);
                    continue;
                }
                (DataType::Map(a, an), DataType::Map(b, bn)) => {
                    match relation {
                        Relation::Contains if an != bn => return false,
                        Relation::TypeOnly => pending.push(Work::Sorted(*an, *bn)),
                        Relation::Contains => {}
                    }
                    current = Work::Field(a, b);
                    continue;
                }
                (DataType::Struct(a), DataType::Struct(b)) => {
                    if a.len() != b.len() {
                        return false;
                    }
                    if !a.same_owner(b) {
                        current = Work::Fields(a, b);
                        continue;
                    }
                }
                (DataType::Union(a, am), DataType::Union(b, bm)) => {
                    if am != bm || (matches!(relation, Relation::TypeOnly) && a.len() != b.len()) {
                        return false;
                    }
                    // Contains deliberately allows extra tags on the RIGHT.
                    current = Work::Union(a, b, 0);
                    continue;
                }
                (DataType::Dictionary(ak, av), DataType::Dictionary(bk, bv)) => {
                    pending.push(Work::Type(av, bv));
                    current = Work::Type(ak, bk);
                    continue;
                }
                (DataType::RunEndEncoded(ak, av), DataType::RunEndEncoded(bk, bv))
                    if matches!(relation, Relation::TypeOnly) =>
                {
                    pending.push(Work::Field(av, bv));
                    current = Work::Field(ak, bk);
                    continue;
                }
                // In particular, Contains uses ordinary equality for run-end
                // fields, not nullable/metadata superset semantics.
                _ if a == b => {}
                _ => return false,
            },
            Work::Field(a, b) => {
                match relation {
                    Relation::TypeOnly if a.is_nullable() != b.is_nullable() => return false,
                    Relation::Contains => {
                        if a.name() != b.name() {
                            return false;
                        }
                        pending.push(Work::FieldTail(a, b));
                    }
                    Relation::TypeOnly => {}
                }
                current = Work::Type(a.data_type(), b.data_type());
                continue;
            }
            Work::FieldTail(a, b) => {
                if a.dictionary_flags().1 != b.dictionary_flags().1
                    || (!a.is_nullable() && b.is_nullable())
                    || !b
                        .metadata()
                        .iter()
                        .all(|(k, v)| a.metadata().get(k) == Some(v))
                {
                    return false;
                }
            }
            Work::Fields(a, b) => {
                if let Some((first, tail)) = a.split_first() {
                    pending.push(Work::Fields(tail, &b[1..]));
                    // Preserve the shared field shortcut from Fields::contains.
                    if !matches!(relation, Relation::Contains)
                        || !std::sync::Arc::ptr_eq(first, &b[0])
                    {
                        current = Work::Field(first, &b[0]);
                        continue;
                    }
                }
            }
            Work::Union(a, b, index) => {
                if let Some((id, field)) = a.get(index) {
                    // All public construction and serde paths validate unique
                    // tags, so the original any() has at most one candidate.
                    let Some((_, other)) = b.iter().find(|(other_id, _)| other_id == id) else {
                        return false;
                    };
                    pending.push(Work::Union(a, b, index + 1));
                    current = Work::Field(field, other);
                    continue;
                }
            }
            Work::Sorted(a, b) if a != b => return false,
            Work::Sorted(_, _) => {}
        }
        match pending.pop() {
            Some(next) => current = next,
            None => return true,
        }
    }
}
