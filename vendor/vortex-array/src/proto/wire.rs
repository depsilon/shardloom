// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Stack-safe protobuf writing. Ordered merge continuations live in `decode`.

use std::slice;

use prost::bytes::{Buf, BufMut};
use prost::encoding::{self, DecodeContext, WireType};
use prost::{DecodeError, Message};

use super::dtype::{self as dt, d_type::DtypeType as Type};
use super::scalar::{self as sv, scalar_value::Kind};

#[derive(Clone, Copy)]
enum Node<'a> {
    DType(&'a dt::DType),
    Type(&'a Type),
    Value(&'a sv::ScalarValue),
    Scalar(&'a sv::Scalar),
    List(&'a sv::ListValue),
    Union(&'a sv::UnionValue),
}

enum Children<'a> {
    Fixed(std::array::IntoIter<Option<(u32, Node<'a>)>, 2>),
    DTypes(slice::Iter<'a, dt::DType>),
    Values(slice::Iter<'a, sv::ScalarValue>),
}

impl<'a> Iterator for Children<'a> {
    type Item = (u32, Node<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Fixed(children) => children.flatten().next(),
            Self::DTypes(children) => children.next().map(|child| (2, Node::DType(child))),
            Self::Values(children) => children.next().map(|child| (1, Node::Value(child))),
        }
    }
}

// One local-field visitor supplies both lengths and bytes. The visitor applies
// omission rules; these outputs use unchanged Prost codecs, without child calls.
trait Output {
    fn leaf_message<M: Message>(&mut self, value: &M);
    fn leaf_value(&mut self, value: &Kind);
    fn boolean(&mut self, tag: u32);
    fn uint32(&mut self, tag: u32, value: u32);
    #[expect(clippy::ptr_arg, reason = "the pinned Prost string codec takes String")]
    fn string(&mut self, tag: u32, value: &String);
    #[expect(
        clippy::ptr_arg,
        reason = "the pinned Prost BytesAdapter is implemented for Vec, not slices"
    )]
    fn bytes(&mut self, tag: u32, value: &Vec<u8>);
    fn packed_int32(&mut self, tag: u32, values: &[i32]);
}

struct Measure(usize);

impl Output for Measure {
    fn leaf_message<M: Message>(&mut self, value: &M) {
        self.0 += value.encoded_len();
    }
    fn leaf_value(&mut self, value: &Kind) {
        self.0 += value.encoded_len();
    }
    fn boolean(&mut self, tag: u32) {
        self.0 += encoding::bool::encoded_len(tag, &true);
    }
    fn uint32(&mut self, tag: u32, value: u32) {
        self.0 += encoding::uint32::encoded_len(tag, &value);
    }
    fn string(&mut self, tag: u32, value: &String) {
        self.0 += encoding::string::encoded_len(tag, value);
    }
    fn bytes(&mut self, tag: u32, value: &Vec<u8>) {
        self.0 += encoding::bytes::encoded_len(tag, value);
    }
    fn packed_int32(&mut self, tag: u32, values: &[i32]) {
        self.0 += encoding::int32::encoded_len_packed(tag, values);
    }
}

struct Write<'a, B>(&'a mut B);

impl<B: BufMut> Output for Write<'_, B> {
    fn leaf_message<M: Message>(&mut self, value: &M) {
        value.encode_raw(self.0);
    }
    fn leaf_value(&mut self, value: &Kind) {
        value.encode(self.0);
    }
    fn boolean(&mut self, tag: u32) {
        encoding::bool::encode(tag, &true, self.0);
    }
    fn uint32(&mut self, tag: u32, value: u32) {
        encoding::uint32::encode(tag, &value, self.0);
    }
    fn string(&mut self, tag: u32, value: &String) {
        encoding::string::encode(tag, value, self.0);
    }
    fn bytes(&mut self, tag: u32, value: &Vec<u8>) {
        encoding::bytes::encode(tag, value, self.0);
    }
    fn packed_int32(&mut self, tag: u32, values: &[i32]) {
        encoding::int32::encode_packed(tag, values, self.0);
    }
}

impl<'a> Node<'a> {
    fn children(self) -> Children<'a> {
        let children = match self {
            Self::DType(dtype) => [
                dtype
                    .dtype_type
                    .as_ref()
                    .map(|kind| (type_tag(kind), Self::Type(kind))),
                None,
            ],
            Self::Type(kind) => match kind {
                Type::Struct(fields) => return Children::DTypes(fields.dtypes.iter()),
                Type::Union(fields) => return Children::DTypes(fields.dtypes.iter()),
                Type::List(list) => [
                    list.element_type
                        .as_deref()
                        .map(|child| (1, Self::DType(child))),
                    None,
                ],
                Type::FixedSizeList(list) => [
                    list.element_type
                        .as_deref()
                        .map(|child| (1, Self::DType(child))),
                    None,
                ],
                Type::Extension(ext) => [
                    ext.storage_dtype
                        .as_deref()
                        .map(|child| (2, Self::DType(child))),
                    None,
                ],
                Type::Map(map) => [
                    map.key_type.as_deref().map(|child| (1, Self::DType(child))),
                    map.value_type
                        .as_deref()
                        .map(|child| (2, Self::DType(child))),
                ],
                Type::Null(_)
                | Type::Bool(_)
                | Type::Primitive(_)
                | Type::Decimal(_)
                | Type::Utf8(_)
                | Type::Binary(_)
                | Type::Variant(_) => [None, None],
            },
            Self::Value(value) => [
                match value.kind.as_ref() {
                    Some(Kind::ListValue(list)) => Some((9, Self::List(list))),
                    Some(Kind::VariantValue(scalar)) => Some((11, Self::Scalar(scalar))),
                    Some(Kind::UnionValue(union)) => Some((12, Self::Union(union))),
                    None
                    | Some(
                        Kind::NullValue(_)
                        | Kind::BoolValue(_)
                        | Kind::Int64Value(_)
                        | Kind::Uint64Value(_)
                        | Kind::F32Value(_)
                        | Kind::F64Value(_)
                        | Kind::F16Value(_)
                        | Kind::StringValue(_)
                        | Kind::BytesValue(_),
                    ) => None,
                },
                None,
            ],
            Self::Scalar(scalar) => [
                scalar.dtype.as_ref().map(|child| (1, Self::DType(child))),
                scalar.value.as_deref().map(|child| (2, Self::Value(child))),
            ],
            Self::List(list) => return Children::Values(list.values.iter()),
            Self::Union(union) => [
                union.value.as_deref().map(|child| (2, Self::Value(child))),
                None,
            ],
        };
        Children::Fixed(children.into_iter())
    }

    fn prefix(self, output: &mut impl Output) {
        match self {
            Self::Type(kind) => match kind {
                Type::Null(value) => output.leaf_message(value),
                Type::Bool(value) => output.leaf_message(value),
                Type::Primitive(value) => output.leaf_message(value),
                Type::Decimal(value) => output.leaf_message(value),
                Type::Utf8(value) => output.leaf_message(value),
                Type::Binary(value) => output.leaf_message(value),
                Type::Variant(value) => output.leaf_message(value),
                Type::Struct(fields) => {
                    for name in &fields.names {
                        output.string(1, name);
                    }
                }
                Type::Union(fields) => {
                    for name in &fields.names {
                        output.string(1, name);
                    }
                }
                Type::Extension(ext) => {
                    if !ext.id.is_empty() {
                        output.string(1, &ext.id);
                    }
                }
                Type::List(_) | Type::FixedSizeList(_) | Type::Map(_) => {}
            },
            Self::Value(value) => match value.kind.as_ref() {
                Some(
                    leaf @ (Kind::NullValue(_)
                    | Kind::BoolValue(_)
                    | Kind::Int64Value(_)
                    | Kind::Uint64Value(_)
                    | Kind::F32Value(_)
                    | Kind::F64Value(_)
                    | Kind::F16Value(_)
                    | Kind::StringValue(_)
                    | Kind::BytesValue(_)),
                ) => output.leaf_value(leaf),
                None | Some(Kind::ListValue(_) | Kind::VariantValue(_) | Kind::UnionValue(_)) => {}
            },
            Self::Union(union) => {
                if union.type_id != 0 {
                    output.uint32(1, union.type_id);
                }
            }
            Self::DType(_) | Self::Scalar(_) | Self::List(_) => {}
        }
    }

    fn tail(self, output: &mut impl Output) {
        let Self::Type(kind) = self else {
            return;
        };
        let nullable = match kind {
            Type::Struct(fields) => Some((3, fields.nullable)),
            Type::List(list) => Some((2, list.nullable)),
            Type::FixedSizeList(list) => {
                if list.size != 0 {
                    output.uint32(2, list.size);
                }
                Some((3, list.nullable))
            }
            Type::Union(fields) => {
                output.packed_int32(3, &fields.type_ids);
                Some((4, fields.nullable))
            }
            Type::Map(map) => {
                if map.keys_sorted {
                    output.boolean(3);
                }
                Some((4, map.nullable))
            }
            Type::Extension(ext) => {
                if let Some(bytes) = &ext.metadata {
                    output.bytes(3, bytes);
                }
                None
            }
            Type::Null(_)
            | Type::Bool(_)
            | Type::Primitive(_)
            | Type::Decimal(_)
            | Type::Utf8(_)
            | Type::Binary(_)
            | Type::Variant(_) => None,
        };
        if let Some((tag, true)) = nullable {
            output.boolean(tag);
        }
    }

    fn local_len(self) -> usize {
        let mut output = Measure(0);
        self.prefix(&mut output);
        self.tail(&mut output);
        output.0
    }
}

fn type_tag(kind: &Type) -> u32 {
    match kind {
        Type::Null(_) => 1,
        Type::Bool(_) => 2,
        Type::Primitive(_) => 3,
        Type::Decimal(_) => 4,
        Type::Utf8(_) => 5,
        Type::Binary(_) => 6,
        Type::Struct(_) => 7,
        Type::List(_) => 8,
        Type::Extension(_) => 9,
        Type::FixedSizeList(_) => 10,
        Type::Variant(_) => 11,
        Type::Union(_) => 12,
        Type::Map(_) => 13,
    }
}

fn nested_len(tag: u32, len: usize) -> usize {
    encoding::key_len(tag) + encoding::encoded_len_varint(len as u64) + len
}

// Measurement needs only the active ancestors, not the full writing plan.
fn encoded_len(root: Node<'_>) -> usize {
    let mut children = root.children();
    let local_len = root.local_len();
    let Some((first_tag, first)) = children.next() else {
        return local_len;
    };
    if children.next().is_none() && first.children().next().is_none() {
        return local_len + nested_len(first_tag, first.local_len());
    }
    let mut pending = Vec::new();
    let mut children = root.children();
    let mut tag = 0;
    let mut len = local_len;
    loop {
        if let Some((child_tag, child)) = children.next() {
            pending.push((children, tag, len));
            children = child.children();
            tag = child_tag;
            len = child.local_len();
        } else if let Some((parent_children, parent_tag, parent_len)) = pending.pop() {
            len = parent_len + nested_len(tag, len);
            children = parent_children;
            tag = parent_tag;
        } else {
            return len;
        }
    }
}

struct Entry<'a> {
    node: Node<'a>,
    tag: u32,
    end: usize,
    len: usize,
}

fn plan(root: Node<'_>) -> Vec<Entry<'_>> {
    let mut entries = vec![Entry {
        node: root,
        tag: 0,
        end: 0,
        len: root.local_len(),
    }];
    let mut pending = vec![(0, root.children())];
    while let Some((index, children)) = pending.last_mut() {
        if let Some((tag, child)) = children.next() {
            let index = entries.len();
            entries.push(Entry {
                node: child,
                tag,
                end: 0,
                len: child.local_len(),
            });
            pending.push((index, child.children()));
        } else {
            let index = *index;
            entries[index].end = entries.len();
            pending.pop();
            if let Some((parent, _)) = pending.last() {
                entries[*parent].len += nested_len(entries[index].tag, entries[index].len);
            }
        }
    }
    entries
}

fn delimiter(tag: u32, len: usize, buf: &mut impl BufMut) {
    encoding::encode_key(tag, WireType::LengthDelimited, buf);
    encoding::encode_varint(len as u64, buf);
}

fn encode_raw(root: Node<'_>, buf: &mut impl BufMut) {
    let mut output = Write(buf);
    // Empty messages and leaf oneofs need no heap writing plan.
    let mut children = root.children();
    let first = children.next();
    if first.is_none() {
        root.prefix(&mut output);
        root.tail(&mut output);
        return;
    }
    if let Some((tag, child)) = first
        && children.next().is_none()
        && child.children().next().is_none()
    {
        root.prefix(&mut output);
        delimiter(tag, child.local_len(), output.0);
        child.prefix(&mut output);
        child.tail(&mut output);
        root.tail(&mut output);
        return;
    }
    let entries = plan(root);
    let mut pending: Vec<usize> = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        while let Some(&parent) = pending.last() {
            if entries[parent].end != index {
                break;
            }
            entries[parent].node.tail(&mut output);
            pending.pop();
        }
        if entry.tag != 0 {
            delimiter(entry.tag, entry.len, output.0);
        }
        entry.node.prefix(&mut output);
        if entry.end == index + 1 {
            entry.node.tail(&mut output);
        } else {
            pending.push(index);
        }
    }
    while let Some(index) = pending.pop() {
        entries[index].node.tail(&mut output);
    }
}

impl Message for dt::DType {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        encode_raw(Node::DType(self), buf);
    }

    fn encoded_len(&self) -> usize {
        encoded_len(Node::DType(self))
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        super::decode::dtype_field(self, tag, wire_type, buf, ctx)
    }

    fn clear(&mut self) {
        self.dtype_type = None;
    }
}

impl Message for sv::ScalarValue {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        encode_raw(Node::Value(self), buf);
    }

    fn encoded_len(&self) -> usize {
        encoded_len(Node::Value(self))
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        super::decode::value_field(self, tag, wire_type, buf, ctx)
    }

    fn clear(&mut self) {
        self.kind = None;
    }
}
