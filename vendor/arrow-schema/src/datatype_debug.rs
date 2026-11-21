// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! The pinned Debug grammar, walked without recursive Field/DataType calls.
//! Scalar Debug implementations receive the original formatter flags. Containers
//! supply indentation directly, without a recursive chain of padding writers.

use std::collections::{HashMap, hash_map};
use std::fmt::{self, Debug};
use std::sync::Arc;

use crate::{DataType, Field, FieldRef, UnionFields};

enum Node<'a> {
    Type(&'a DataType),
    Field(&'a Field),
    Fields(&'a [FieldRef]),
    Union(&'a UnionFields),
    Pair(&'a (i8, FieldRef)),
    Timezone(&'a Option<Arc<str>>),
    Metadata(&'a HashMap<String, String>),
    Scalar(&'a dyn Debug),
    DictionaryId(i64),
}

enum Sequence<'a> {
    Type(&'a DataType, usize),
    Field(&'a Field, usize),
    Fields(&'a [FieldRef], usize),
    Union(&'a UnionFields, usize),
    Pair(&'a (i8, FieldRef), usize),
    Timezone(&'a Arc<str>, bool),
    Metadata(hash_map::Iter<'a, String, String>),
}

enum Label<'a> {
    Field(&'static str),
    Key(&'a String),
}

struct Member<'a> {
    label: Option<Label<'a>>,
    value: Node<'a>,
}

impl<'a> Member<'a> {
    fn value(value: Node<'a>) -> Self {
        Self { label: None, value }
    }

    fn field(label: &'static str, value: Node<'a>) -> Self {
        Self {
            label: Some(Label::Field(label)),
            value,
        }
    }
}

impl<'a> Sequence<'a> {
    fn next(&mut self) -> Option<Member<'a>> {
        match self {
            Self::Type(dtype, index) => {
                let index = std::mem::replace(index, *index + 1);
                let value = match (*dtype, index) {
                    (DataType::Timestamp(unit, _), 0)
                    | (DataType::Time32(unit), 0)
                    | (DataType::Time64(unit), 0)
                    | (DataType::Duration(unit), 0) => Node::Scalar(unit),
                    (DataType::Timestamp(_, zone), 1) => Node::Timezone(zone),
                    (DataType::Interval(unit), 0) => Node::Scalar(unit),
                    (DataType::FixedSizeBinary(size), 0) => Node::Scalar(size),
                    (DataType::List(field), 0)
                    | (DataType::ListView(field), 0)
                    | (DataType::FixedSizeList(field, _), 0)
                    | (DataType::LargeList(field), 0)
                    | (DataType::LargeListView(field), 0)
                    | (DataType::Map(field, _), 0) => Node::Field(field),
                    (DataType::FixedSizeList(_, size), 1) => Node::Scalar(size),
                    (DataType::Map(_, sorted), 1) => Node::Scalar(sorted),
                    (DataType::Struct(fields), 0) => Node::Fields(fields),
                    (DataType::Union(fields, _), 0) => Node::Union(fields),
                    (DataType::Union(_, mode), 1) => Node::Scalar(mode),
                    (DataType::Dictionary(key, _), 0) => Node::Type(key),
                    (DataType::Dictionary(_, value), 1) => Node::Type(value),
                    (DataType::Decimal32(precision, _), 0)
                    | (DataType::Decimal64(precision, _), 0)
                    | (DataType::Decimal128(precision, _), 0)
                    | (DataType::Decimal256(precision, _), 0) => Node::Scalar(precision),
                    (DataType::Decimal32(_, scale), 1)
                    | (DataType::Decimal64(_, scale), 1)
                    | (DataType::Decimal128(_, scale), 1)
                    | (DataType::Decimal256(_, scale), 1) => Node::Scalar(scale),
                    (DataType::RunEndEncoded(ends, _), 0) => Node::Field(ends),
                    (DataType::RunEndEncoded(_, values), 1) => Node::Field(values),
                    _ => return None,
                };
                Some(Member::value(value))
            }
            Self::Field(field, index) => loop {
                let index = std::mem::replace(index, *index + 1);
                let (dict_id, ordered) = field.dictionary_flags();
                let member = match index {
                    0 if field.name() != "item" => {
                        Member::field("name", Node::Scalar(field.name()))
                    }
                    1 => Member::field("data_type", Node::Type(field.data_type())),
                    2 if field.is_nullable() => Member::field("nullable", Node::Scalar(&true)),
                    3 if dict_id != 0 => Member::field("dict_id", Node::DictionaryId(dict_id)),
                    4 if ordered => Member::field("dict_is_ordered", Node::Scalar(&true)),
                    5 if !field.metadata().is_empty() => {
                        Member::field("metadata", Node::Metadata(field.metadata()))
                    }
                    0..=5 => continue,
                    _ => return None,
                };
                return Some(member);
            },
            Self::Fields(fields, index) => {
                let field = fields.get(*index)?;
                *index += 1;
                Some(Member::value(Node::Field(field)))
            }
            Self::Union(fields, index) => {
                let pair = fields.get(*index)?;
                *index += 1;
                Some(Member::value(Node::Pair(pair)))
            }
            Self::Pair(pair, index) => {
                let value = match *index {
                    0 => Node::Scalar(&pair.0),
                    1 => Node::Field(&pair.1),
                    _ => return None,
                };
                *index += 1;
                Some(Member::value(value))
            }
            Self::Timezone(value, done) => {
                if std::mem::replace(done, true) {
                    None
                } else {
                    Some(Member::value(Node::Scalar(*value)))
                }
            }
            Self::Metadata(iter) => {
                let (key, value) = iter.next()?;
                Some(Member {
                    label: Some(Label::Key(key)),
                    value: Node::Scalar(value),
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Delimiter {
    Tuple,
    Struct,
    List,
    Map,
}

impl Delimiter {
    fn open(self) -> &'static str {
        match self {
            Self::Tuple => "(",
            Self::Struct | Self::Map => "{",
            Self::List => "[",
        }
    }
    fn close(self) -> &'static str {
        match self {
            Self::Tuple => ")",
            Self::Struct | Self::Map => "}",
            Self::List => "]",
        }
    }
}

enum Work<'a> {
    Node(Node<'a>, usize),
    Members(Sequence<'a>, Delimiter, usize, bool),
}

impl Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format(Node::Type(self), f)
    }
}

pub(crate) fn field(value: &Field, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    format(Node::Field(value), f)
}

pub(crate) fn fields(value: &[FieldRef], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    format(Node::Fields(value), f)
}

pub(crate) fn union(value: &UnionFields, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    format(Node::Union(value), f)
}

fn indent(f: &mut fmt::Formatter<'_>, mut spaces: usize) -> fmt::Result {
    const SPACES: &str = "                                                                ";
    while spaces >= SPACES.len() {
        f.write_str(SPACES)?;
        spaces -= SPACES.len();
    }
    f.write_str(&SPACES[..spaces])
}

fn format(value: Node<'_>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let pretty = f.alternate();
    let mut current = Work::Node(value, 0);
    let mut pending = Vec::new();
    loop {
        match current {
            Work::Node(node, level) => {
                let container = match node {
                    Node::Type(dtype) => {
                        let (name, members) = type_name(dtype);
                        f.write_str(name)?;
                        members.then_some((Sequence::Type(dtype, 0), Delimiter::Tuple))
                    }
                    Node::Field(field) => {
                        f.write_str("Field ")?;
                        Some((Sequence::Field(field, 0), Delimiter::Struct))
                    }
                    Node::Fields(fields) => Some((Sequence::Fields(fields, 0), Delimiter::List)),
                    Node::Union(fields) => Some((Sequence::Union(fields, 0), Delimiter::List)),
                    Node::Pair(pair) => Some((Sequence::Pair(pair, 0), Delimiter::Tuple)),
                    Node::Timezone(Some(zone)) => {
                        f.write_str("Some")?;
                        Some((Sequence::Timezone(zone, false), Delimiter::Tuple))
                    }
                    Node::Timezone(None) => {
                        f.write_str("None")?;
                        None
                    }
                    Node::Metadata(metadata) => {
                        Some((Sequence::Metadata(metadata.iter()), Delimiter::Map))
                    }
                    Node::Scalar(scalar) => {
                        Debug::fmt(scalar, f)?;
                        None
                    }
                    Node::DictionaryId(id) => {
                        Debug::fmt(&id, f)?;
                        None
                    }
                };
                if let Some((sequence, delimiter)) = container {
                    f.write_str(delimiter.open())?;
                    current = Work::Members(sequence, delimiter, level, true);
                    continue;
                }
            }
            Work::Members(mut sequence, delimiter, level, first) => {
                if let Some(member) = sequence.next() {
                    if !first {
                        f.write_str(",")?;
                    }
                    if pretty {
                        f.write_str("\n")?;
                        indent(f, level + 4)?;
                    } else if !first || matches!(delimiter, Delimiter::Struct) {
                        f.write_str(" ")?;
                    }
                    if let Some(label) = member.label {
                        match label {
                            Label::Field(name) => f.write_str(name)?,
                            Label::Key(key) => Debug::fmt(key, f)?,
                        }
                        f.write_str(": ")?;
                    }
                    pending.push(Work::Members(sequence, delimiter, level, false));
                    current = Work::Node(member.value, if pretty { level + 4 } else { 0 });
                    continue;
                }
                if !first {
                    if pretty {
                        f.write_str(",\n")?;
                        indent(f, level)?;
                    } else if matches!(delimiter, Delimiter::Struct) {
                        f.write_str(" ")?;
                    }
                }
                f.write_str(delimiter.close())?;
            }
        }
        match pending.pop() {
            Some(next) => current = next,
            None => return Ok(()),
        }
    }
}

fn type_name(value: &DataType) -> (&'static str, bool) {
    match value {
        DataType::Null => ("Null", false),
        DataType::Boolean => ("Boolean", false),
        DataType::Int8 => ("Int8", false),
        DataType::Int16 => ("Int16", false),
        DataType::Int32 => ("Int32", false),
        DataType::Int64 => ("Int64", false),
        DataType::UInt8 => ("UInt8", false),
        DataType::UInt16 => ("UInt16", false),
        DataType::UInt32 => ("UInt32", false),
        DataType::UInt64 => ("UInt64", false),
        DataType::Float16 => ("Float16", false),
        DataType::Float32 => ("Float32", false),
        DataType::Float64 => ("Float64", false),
        DataType::Timestamp(..) => ("Timestamp", true),
        DataType::Date32 => ("Date32", false),
        DataType::Date64 => ("Date64", false),
        DataType::Time32(..) => ("Time32", true),
        DataType::Time64(..) => ("Time64", true),
        DataType::Duration(..) => ("Duration", true),
        DataType::Interval(..) => ("Interval", true),
        DataType::Binary => ("Binary", false),
        DataType::FixedSizeBinary(..) => ("FixedSizeBinary", true),
        DataType::LargeBinary => ("LargeBinary", false),
        DataType::BinaryView => ("BinaryView", false),
        DataType::Utf8 => ("Utf8", false),
        DataType::LargeUtf8 => ("LargeUtf8", false),
        DataType::Utf8View => ("Utf8View", false),
        DataType::List(..) => ("List", true),
        DataType::ListView(..) => ("ListView", true),
        DataType::FixedSizeList(..) => ("FixedSizeList", true),
        DataType::LargeList(..) => ("LargeList", true),
        DataType::LargeListView(..) => ("LargeListView", true),
        DataType::Struct(..) => ("Struct", true),
        DataType::Union(..) => ("Union", true),
        DataType::Dictionary(..) => ("Dictionary", true),
        DataType::Decimal32(..) => ("Decimal32", true),
        DataType::Decimal64(..) => ("Decimal64", true),
        DataType::Decimal128(..) => ("Decimal128", true),
        DataType::Decimal256(..) => ("Decimal256", true),
        DataType::Map(..) => ("Map", true),
        DataType::RunEndEncoded(..) => ("RunEndEncoded", true),
    }
}
