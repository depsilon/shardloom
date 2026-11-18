// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered built-in Array -> ArrayData conversion. Children retain their exact
//! source owners until the parent descriptor is complete.

use arrow_buffer::Buffer;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{DataType, UnionFields};

use super::release::{Child, Children, UnionChildren};
use super::{Array, StructArray};

#[cfg(test)]
#[path = "data_export_tests.rs"]
pub(super) mod tests;

enum Sources {
    One(Child),
    Many(Children),
    Union(UnionFields, UnionChildren),
    Struct(StructArray),
}

impl Sources {
    fn len(&self) -> usize {
        match self {
            Self::One(_) | Self::Struct(_) => 1,
            Self::Many(children) => children.len(),
            Self::Union(fields, _) => fields.len(),
        }
    }

    fn get(&self, index: usize) -> Option<&dyn Array> {
        match self {
            Self::One(child) if index == 0 => Some(child.as_ref()),
            Self::Struct(child) if index == 0 => Some(child),
            Self::Many(children) => children.get(index).map(|child| child.as_ref()),
            Self::Union(fields, children) => fields
                .get(index)
                .map(|(id, _)| children[*id as usize].as_ref().unwrap().as_ref()),
            _ => None,
        }
    }
}

enum Plan {
    Builder(ArrayDataBuilder),
    // The original union conversion formed buffers before traversing children,
    // but moved its dtype into the builder only afterwards. Keep unwind order.
    Union {
        buffers: Vec<Buffer>,
        dtype: DataType,
        len: usize,
    },
}

/// Opaque continuation for the pinned provider's typed descriptor conversion.
/// It does not expose a new public array or input representation.
#[doc(hidden)]
pub struct ArrayDataNode {
    output: Vec<ArrayData>,
    plan: Plan,
    sources: Sources,
    next: usize,
}

impl ArrayDataNode {
    fn new(plan: Plan, sources: Sources, prefix: Option<ArrayData>) -> Self {
        let mut output = Vec::with_capacity(sources.len() + usize::from(prefix.is_some()));
        if let Some(prefix) = prefix {
            output.push(prefix);
        }
        Self {
            output,
            plan,
            sources,
            next: 0,
        }
    }

    pub(super) fn one(builder: ArrayDataBuilder, child: Child) -> Self {
        Self::new(Plan::Builder(builder), Sources::One(child), None)
    }

    pub(super) fn many(builder: ArrayDataBuilder, children: Children) -> Self {
        Self::new(Plan::Builder(builder), Sources::Many(children), None)
    }

    pub(super) fn structure(builder: ArrayDataBuilder, child: StructArray) -> Self {
        Self::new(Plan::Builder(builder), Sources::Struct(child), None)
    }

    pub(super) fn run(builder: ArrayDataBuilder, ends: ArrayData, values: Child) -> Self {
        Self::new(Plan::Builder(builder), Sources::One(values), Some(ends))
    }

    pub(super) fn union(
        dtype: DataType,
        len: usize,
        buffers: Vec<Buffer>,
        children: UnionChildren,
    ) -> Self {
        let fields = match &dtype {
            DataType::Union(fields, _) => fields.clone(),
            _ => unreachable!(),
        };
        Self::new(
            Plan::Union {
                buffers,
                dtype,
                len,
            },
            Sources::Union(fields, children),
            None,
        )
    }

    fn advance(&mut self) -> Option<Next> {
        let index = self.next;
        self.next += 1;
        let child = self.sources.get(index)?;
        Some(match child.data_node() {
            Some(node) => Next::Node(node),
            None => Next::Data(child.to_data()),
        })
    }

    fn finish(self) -> ArrayData {
        let Self {
            output,
            plan,
            sources,
            ..
        } = self;
        let builder = match plan {
            Plan::Builder(builder) => builder,
            Plan::Union {
                buffers,
                dtype,
                len,
            } => ArrayDataBuilder::new(dtype).len(len).buffers(buffers),
        }
        .child_data(output);
        // SAFETY: These private nodes contain precisely the dtype, buffers,
        // nulls, length and ordered child descriptors of an existing valid
        // built-in Array. This is the same build_unchecked contract formerly
        // applied by each From<ConcreteArray> implementation. Custom children
        // keep Array::to_data's existing unsafe-trait validity contract.
        let data = unsafe { builder.build_unchecked() };
        drop(sources);
        data
    }
}

enum Next {
    Node(ArrayDataNode),
    Data(ArrayData),
}

struct Pending(Vec<ArrayDataNode>);

impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(node) = self.0.pop() {
            drop(node);
        }
    }
}

pub(super) fn build(node: ArrayDataNode) -> ArrayData {
    // An in-flight child and its completed prefix release before outer frames.
    let mut pending = Pending(Vec::new());
    let mut current = node;
    loop {
        match current.advance() {
            Some(Next::Node(child)) => {
                pending.0.push(current);
                current = child;
            }
            Some(Next::Data(data)) => current.output.push(data),
            None => {
                let data = current.finish();
                match pending.0.pop() {
                    Some(mut parent) => {
                        parent.output.push(data);
                        current = parent;
                    }
                    None => return data,
                }
            }
        }
    }
}
