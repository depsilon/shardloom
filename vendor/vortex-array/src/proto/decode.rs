// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered protobuf merge continuations with the original partial-update rules.

use std::mem;

use prost::bytes::Buf;
use prost::encoding::message::DecodeContinuation;
use prost::encoding::{self, DecodeContext, WireType};
use prost::{DecodeError, Message};

use super::dtype::{self as dt, d_type::DtypeType as Type};
use super::scalar::{self as sv, scalar_value::Kind};

struct Field {
    tag: u32,
    wire: WireType,
    context: DecodeContext,
}

enum Fields {
    One(Option<Field>),
    Start(Option<(WireType, DecodeContext)>),
    Delimited(DecodeContinuation),
}

impl Fields {
    fn next(&mut self, buf: &mut impl Buf) -> Result<Option<Field>, DecodeError> {
        loop {
            match self {
                Self::One(field) => return Ok(field.take()),
                Self::Start(start) => {
                    let (wire, context) = start.take().unwrap_or_else(|| unreachable!());
                    *self = Self::Delimited(DecodeContinuation::new(wire, buf, context)?);
                }
                Self::Delimited(frame) => {
                    return frame.next_field(buf).map(|next| {
                        next.map(|(tag, wire)| Field {
                            tag,
                            wire,
                            context: frame.context(),
                        })
                    });
                }
            }
        }
    }
}

enum Node {
    DType(dt::DType),
    Type(Type),
    Value(sv::ScalarValue),
    Kind(Kind),
}

#[derive(Clone, Copy)]
enum Slot {
    DTypeOneof { same: bool },
    ValueOneof { same: bool },
    OptionalDType(u32),
    OptionalValue(u32),
    RepeatedDType,
    RepeatedValue,
}

struct Attachment {
    slot: Slot,
    message: &'static str,
    field: &'static str,
}

impl Attachment {
    fn attach(self, parent: &mut Node, child: Node, success: bool) {
        match (self.slot, child) {
            (Slot::DTypeOneof { same }, Node::Type(kind)) => {
                let Node::DType(parent) = parent else {
                    unreachable!()
                };
                if same || success {
                    parent.dtype_type = Some(kind);
                }
            }
            (Slot::ValueOneof { same }, Node::Kind(kind)) => {
                let Node::Value(parent) = parent else {
                    unreachable!()
                };
                if same || success {
                    parent.kind = Some(kind);
                }
            }
            (Slot::OptionalDType(tag), Node::DType(child)) => {
                *parent.dtype_slot(tag) = child;
            }
            (Slot::OptionalValue(tag), Node::Value(child)) => {
                *parent.value_slot(tag) = child;
            }
            (Slot::RepeatedDType, Node::DType(child)) => {
                if success {
                    match parent {
                        Node::Type(Type::Struct(fields)) => fields.dtypes.push(child),
                        Node::Type(Type::Union(fields)) => fields.dtypes.push(child),
                        _ => unreachable!(),
                    }
                }
            }
            (Slot::RepeatedValue, Node::Value(child)) => {
                if success {
                    let Node::Kind(Kind::ListValue(list)) = parent else {
                        unreachable!()
                    };
                    list.values.push(child);
                }
            }
            _ => unreachable!(),
        }
    }
}

struct Frame {
    node: Node,
    fields: Fields,
    incoming: Option<Attachment>,
}

impl Frame {
    fn child(node: Node, incoming: Attachment, field: Field) -> Self {
        Self {
            node,
            fields: Fields::Start(Some((field.wire, field.context))),
            incoming: Some(incoming),
        }
    }
}

enum Root<'a> {
    DType(&'a mut dt::DType),
    Value(&'a mut sv::ScalarValue),
}

// The caller's root is restored even if a custom Buf unwinds during a read.
// All unfinished child edges have exactly the same rollback/partial-commit
// policy as an ordinary DecodeError. Restoration never reads the input.
struct Work<'a> {
    root: Root<'a>,
    frames: Vec<Frame>,
}

impl<'a> Work<'a> {
    fn new(mut root: Root<'a>, field: Field) -> Self {
        let node = match &mut root {
            Root::DType(dtype) => Node::DType(mem::take(*dtype)),
            Root::Value(value) => Node::Value(mem::take(*value)),
        };
        Self {
            root,
            frames: vec![Frame {
                node,
                fields: Fields::One(Some(field)),
                incoming: None,
            }],
        }
    }

    fn finish_child(&mut self, success: bool) {
        let child = self.frames.pop().unwrap_or_else(|| unreachable!());
        let parent = self.frames.last_mut().unwrap_or_else(|| unreachable!());
        child.incoming.unwrap_or_else(|| unreachable!()).attach(
            &mut parent.node,
            child.node,
            success,
        );
    }

    fn trace(&self, mut error: DecodeError) -> DecodeError {
        for frame in self.frames.iter().rev() {
            if let Some(edge) = &frame.incoming {
                error.push(edge.message, edge.field);
            }
        }
        error
    }

    fn merge(mut self, buf: &mut impl Buf) -> Result<(), DecodeError> {
        loop {
            let next = self
                .frames
                .last_mut()
                .unwrap_or_else(|| unreachable!())
                .fields
                .next(buf);
            let field = match next {
                Ok(Some(field)) => field,
                Ok(None) => {
                    if self.frames.len() == 1 {
                        return Ok(());
                    }
                    self.finish_child(true);
                    continue;
                }
                Err(error) => return Err(self.trace(error)),
            };
            let result = self
                .frames
                .last_mut()
                .unwrap_or_else(|| unreachable!())
                .node
                .merge_field(field, buf);
            match result {
                // Push before delimiter entry: every input read has an owned
                // restoration edge, including optional default initialization.
                Ok(Some(child)) => self.frames.push(child),
                Ok(None) => {}
                Err(error) => return Err(self.trace(error)),
            }
        }
    }
}

impl Drop for Work<'_> {
    fn drop(&mut self) {
        while self.frames.len() > 1 {
            self.finish_child(false);
        }
        let frame = self.frames.pop().unwrap_or_else(|| unreachable!());
        match (&mut self.root, frame.node) {
            (Root::DType(root), Node::DType(value)) => **root = value,
            (Root::Value(root), Node::Value(value)) => **root = value,
            _ => unreachable!(),
        }
    }
}

fn compound_dtype(tag: u32) -> bool {
    matches!(tag, 7..=10 | 12..=13)
}

fn compound_value(tag: u32) -> bool {
    matches!(tag, 9 | 11..=12)
}

fn dtype_leaf(dtype: &mut dt::DType, field: Field, buf: &mut impl Buf) -> Result<(), DecodeError> {
    match field.tag {
        1..=13 => Type::merge(
            &mut dtype.dtype_type,
            field.tag,
            field.wire,
            buf,
            field.context,
        )
        .map_err(|mut error| {
            error.push("DType", "dtype_type");
            error
        }),
        _ => encoding::skip_field(field.wire, field.tag, buf, field.context),
    }
}

fn value_leaf(
    value: &mut sv::ScalarValue,
    field: Field,
    buf: &mut impl Buf,
) -> Result<(), DecodeError> {
    match field.tag {
        1..=12 => Kind::merge(&mut value.kind, field.tag, field.wire, buf, field.context).map_err(
            |mut error| {
                error.push("ScalarValue", "kind");
                error
            },
        ),
        _ => encoding::skip_field(field.wire, field.tag, buf, field.context),
    }
}

impl Node {
    fn dtype_slot(&mut self, tag: u32) -> &mut dt::DType {
        let slot = match (self, tag) {
            (Self::Type(Type::List(list)), 1) => &mut list.element_type,
            (Self::Type(Type::FixedSizeList(list)), 1) => &mut list.element_type,
            (Self::Type(Type::Extension(ext)), 2) => &mut ext.storage_dtype,
            (Self::Type(Type::Map(map)), 1) => &mut map.key_type,
            (Self::Type(Type::Map(map)), 2) => &mut map.value_type,
            (Self::Kind(Kind::VariantValue(scalar)), 1) => {
                return scalar.dtype.get_or_insert_with(Default::default);
            }
            _ => unreachable!(),
        };
        // Leave the original Box in its optional slot while visiting the root.
        slot.get_or_insert_with(Default::default).as_mut()
    }

    fn value_slot(&mut self, tag: u32) -> &mut sv::ScalarValue {
        let slot = match (self, tag) {
            (Self::Kind(Kind::VariantValue(scalar)), 2) => &mut scalar.value,
            (Self::Kind(Kind::UnionValue(union)), 2) => &mut union.value,
            _ => unreachable!(),
        };
        slot.get_or_insert_with(Default::default).as_mut()
    }

    fn merge_field(
        &mut self,
        field: Field,
        buf: &mut impl Buf,
    ) -> Result<Option<Frame>, DecodeError> {
        match self {
            Self::DType(dtype) => {
                if !compound_dtype(field.tag) {
                    return dtype_leaf(dtype, field, buf).map(|()| None);
                }
                let same = matches!(
                    (dtype.dtype_type.as_ref(), field.tag),
                    (Some(Type::Struct(_)), 7)
                        | (Some(Type::List(_)), 8)
                        | (Some(Type::Extension(_)), 9)
                        | (Some(Type::FixedSizeList(_)), 10)
                        | (Some(Type::Union(_)), 12)
                        | (Some(Type::Map(_)), 13)
                );
                let kind = if same {
                    dtype.dtype_type.take().unwrap_or_else(|| unreachable!())
                } else {
                    match field.tag {
                        7 => Type::Struct(Default::default()),
                        8 => Type::List(Default::default()),
                        9 => Type::Extension(Default::default()),
                        10 => Type::FixedSizeList(Default::default()),
                        12 => Type::Union(Default::default()),
                        13 => Type::Map(Default::default()),
                        _ => unreachable!(),
                    }
                };
                return Ok(Some(Frame::child(
                    Self::Type(kind),
                    Attachment {
                        slot: Slot::DTypeOneof { same },
                        message: "DType",
                        field: "dtype_type",
                    },
                    field,
                )));
            }
            Self::Value(value) => {
                if !compound_value(field.tag) {
                    return value_leaf(value, field, buf).map(|()| None);
                }
                let same = matches!(
                    (value.kind.as_ref(), field.tag),
                    (Some(Kind::ListValue(_)), 9)
                        | (Some(Kind::VariantValue(_)), 11)
                        | (Some(Kind::UnionValue(_)), 12)
                );
                let kind = if same {
                    value.kind.take().unwrap_or_else(|| unreachable!())
                } else {
                    match field.tag {
                        9 => Kind::ListValue(Default::default()),
                        11 => Kind::VariantValue(Default::default()),
                        12 => Kind::UnionValue(Default::default()),
                        _ => unreachable!(),
                    }
                };
                return Ok(Some(Frame::child(
                    Self::Kind(kind),
                    Attachment {
                        slot: Slot::ValueOneof { same },
                        message: "ScalarValue",
                        field: "kind",
                    },
                    field,
                )));
            }
            Self::Type(_) | Self::Kind(_) => {}
        }
        let edge = match (&*self, field.tag) {
            (Self::Type(Type::Struct(_)), 2) => Some((Slot::RepeatedDType, "Struct", "dtypes")),
            (Self::Type(Type::Union(_)), 2) => Some((Slot::RepeatedDType, "Union", "dtypes")),
            (Self::Type(Type::List(_)), 1) => {
                Some((Slot::OptionalDType(1), "List", "element_type"))
            }
            (Self::Type(Type::FixedSizeList(_)), 1) => {
                Some((Slot::OptionalDType(1), "FixedSizeList", "element_type"))
            }
            (Self::Type(Type::Extension(_)), 2) => {
                Some((Slot::OptionalDType(2), "Extension", "storage_dtype"))
            }
            (Self::Type(Type::Map(_)), 1) => Some((Slot::OptionalDType(1), "Map", "key_type")),
            (Self::Type(Type::Map(_)), 2) => Some((Slot::OptionalDType(2), "Map", "value_type")),
            (Self::Kind(Kind::ListValue(_)), 1) => {
                Some((Slot::RepeatedValue, "ListValue", "values"))
            }
            (Self::Kind(Kind::VariantValue(_)), 1) => {
                Some((Slot::OptionalDType(1), "Scalar", "dtype"))
            }
            (Self::Kind(Kind::VariantValue(_)), 2) => {
                Some((Slot::OptionalValue(2), "Scalar", "value"))
            }
            (Self::Kind(Kind::UnionValue(_)), 2) => {
                Some((Slot::OptionalValue(2), "UnionValue", "value"))
            }
            _ => None,
        };
        if let Some((slot, message, name)) = edge {
            let node = match slot {
                Slot::OptionalDType(tag) => Self::DType(mem::take(self.dtype_slot(tag))),
                Slot::OptionalValue(tag) => Self::Value(mem::take(self.value_slot(tag))),
                Slot::RepeatedDType => Self::DType(Default::default()),
                Slot::RepeatedValue => Self::Value(Default::default()),
                Slot::DTypeOneof { .. } | Slot::ValueOneof { .. } => unreachable!(),
            };
            return Ok(Some(Frame::child(
                node,
                Attachment {
                    slot,
                    message,
                    field: name,
                },
                field,
            )));
        }
        // Recursive fields have all been intercepted. The original generated
        // helpers retain every primitive codec, optional update and error label.
        let result = match self {
            Self::Type(kind) => match kind {
                Type::Null(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Bool(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Primitive(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Decimal(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Utf8(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Binary(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Struct(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::List(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::FixedSizeList(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Extension(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Variant(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Union(v) => v.merge_field(field.tag, field.wire, buf, field.context),
                Type::Map(v) => v.merge_field(field.tag, field.wire, buf, field.context),
            },
            Self::Kind(Kind::ListValue(v)) => {
                v.merge_field(field.tag, field.wire, buf, field.context)
            }
            Self::Kind(Kind::VariantValue(v)) => {
                v.merge_field(field.tag, field.wire, buf, field.context)
            }
            Self::Kind(Kind::UnionValue(v)) => {
                v.merge_field(field.tag, field.wire, buf, field.context)
            }
            _ => unreachable!(),
        };
        result.map(|()| None)
    }
}

pub(super) fn dtype_field(
    dtype: &mut dt::DType,
    tag: u32,
    wire: WireType,
    buf: &mut impl Buf,
    context: DecodeContext,
) -> Result<(), DecodeError> {
    let field = Field { tag, wire, context };
    if compound_dtype(tag) {
        Work::new(Root::DType(dtype), field).merge(buf)
    } else {
        dtype_leaf(dtype, field, buf)
    }
}

pub(super) fn value_field(
    value: &mut sv::ScalarValue,
    tag: u32,
    wire: WireType,
    buf: &mut impl Buf,
    context: DecodeContext,
) -> Result<(), DecodeError> {
    let field = Field { tag, wire, context };
    if compound_value(tag) {
        Work::new(Root::Value(value), field).merge(buf)
    } else {
        value_leaf(value, field, buf)
    }
}
