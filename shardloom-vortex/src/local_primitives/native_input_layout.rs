//! Flat counters preserve typed input validation without recursive ownership.

use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::dtype::{DType, StructFields};

use super::super::native_capacity::ReservedVec;
use super::{add, result_batch};

#[path = "native_input_layout_observe.rs"]
mod observe;

#[derive(Default)]
struct Node {
    rows: usize,
    variable: usize,
    copied: usize,
    // Exclusive preorder subtree end; the next sibling starts at this index.
    end: usize,
}

pub(in crate::local_primitives) struct Layout {
    root: Node,
    nodes: ReservedVec<Node>,
    memory: LiveMemoryPool,
}

enum Children<'a> {
    Struct {
        fields: &'a StructFields,
        next: usize,
    },
    One(Option<&'a DType>),
    Empty,
}

impl<'a> Children<'a> {
    fn new(dtype: &'a DType) -> Self {
        match dtype {
            DType::Struct(fields, _) => Self::Struct { fields, next: 0 },
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => Self::One(Some(child)),
            _ => Self::Empty,
        }
    }

    fn next(&mut self) -> Option<&'a DType> {
        match self {
            Self::Struct { fields, next } => {
                let child = fields.field_dtypes().nth(*next);
                *next += usize::from(child.is_some());
                child
            }
            Self::One(child) => child.take(),
            Self::Empty => None,
        }
    }
}

fn storage(dtype: &DType) -> &DType {
    crate::native_extensions::structured_payload_storage(dtype).unwrap_or(dtype)
}

fn first_child(index: Option<usize>) -> usize {
    index.map_or(0, |index| index + 1)
}

impl Layout {
    pub(in crate::local_primitives) fn new(dtype: &DType, memory: &LiveMemoryPool) -> Result<Self> {
        let mut layout = Self {
            root: Node::default(),
            nodes: ReservedVec::new(memory)?,
            memory: memory.clone(),
        };
        let mut pending = ReservedVec::new(memory)?;
        let mut current = (None, dtype);
        loop {
            let (index, dtype) = current;
            let mut children = Children::new(storage(dtype));
            if let Some(child) = children.next() {
                pending.push((index, children))?;
                current = (Some(layout.append()?), child);
                continue;
            }
            layout.finish(index);
            loop {
                let Some((index, mut children)) = pending.values.pop() else {
                    return Ok(layout);
                };
                if let Some(child) = children.next() {
                    pending.values.push((index, children));
                    current = (Some(layout.append()?), child);
                    break;
                }
                layout.finish(index);
            }
        }
    }

    fn append(&mut self) -> Result<usize> {
        let index = self.nodes.values.len();
        self.nodes.push(Node::default())?;
        Ok(index)
    }

    fn finish(&mut self, index: Option<usize>) {
        let end = self.nodes.values.len();
        self.node_mut(index).end = end;
    }

    fn node(&self, index: Option<usize>) -> &Node {
        index.map_or(&self.root, |index| &self.nodes.values[index])
    }

    fn node_mut(&mut self, index: Option<usize>) -> &mut Node {
        index.map_or(&mut self.root, |index| &mut self.nodes.values[index])
    }

    pub(in crate::local_primitives) fn copied(&self) -> Result<usize> {
        self.nodes
            .values
            .iter()
            .try_fold(self.root.copied, |bytes, node| add(bytes, node.copied))
    }

    pub(in crate::local_primitives) fn bytes(&self, dtype: &DType) -> Result<usize> {
        struct Frame<'a> {
            children: Children<'a>,
            next: usize,
            validity: usize,
            bytes: usize,
        }
        let mut pending = ReservedVec::new(&self.memory)?;
        let mut current = (None, dtype);
        loop {
            let (index, dtype) = current;
            let dtype = storage(dtype);
            let node = self.node(index);
            let validity = if dtype.is_nullable() {
                node.rows.div_ceil(8)
            } else {
                0
            };
            let bytes = match dtype {
                DType::Struct(..) | DType::FixedSizeList(..) => 0,
                DType::List(..) => super::multiply(add(node.rows, 1)?, 8)?,
                DType::Bool(_) => node.rows.div_ceil(8),
                DType::Utf8(_) | DType::Binary(_) => {
                    add(super::multiply(add(node.rows, 1)?, 8)?, node.variable)?
                }
                _ => super::multiply(node.rows, result_batch::width(dtype)?)?,
            };
            let mut children = Children::new(dtype);
            if let Some(child) = children.next() {
                let child_index = first_child(index);
                pending.push(Frame {
                    children,
                    next: self.nodes.values[child_index].end,
                    validity,
                    bytes,
                })?;
                current = (Some(child_index), child);
                continue;
            }
            let mut complete = add(validity, bytes)?;
            loop {
                let Some(mut frame) = pending.values.pop() else {
                    return Ok(complete);
                };
                frame.bytes = add(frame.bytes, complete)?;
                if let Some(child) = frame.children.next() {
                    let child_index = frame.next;
                    frame.next = self.nodes.values[child_index].end;
                    pending.values.push(frame);
                    current = (Some(child_index), child);
                    break;
                }
                complete = add(frame.validity, frame.bytes)?;
            }
        }
    }
}
