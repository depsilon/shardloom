//! Prepare native key nodes in child-before-parent order.

use super::{Children, Column, Container, Frames, Node, Value, is_container};
use crate::local_primitives::{
    native_capacity::ReservedVec,
    native_list::{self, Coordinates},
    native_relational_keys::{KeyColumn, Preparation, failed},
    vortex_error,
};
use shardloom_core::{FloatPolicy, Result};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx,
        arrays::{StructArray, struct_::StructArrayExt as _},
        dtype::DType,
    },
    mask::Mask,
};

struct Binding {
    // Completed children drop before the current and suspended native parents.
    nodes: ReservedVec<Node>,
    current: Option<Parent>,
    suspended: Frames<Parent>,
}

struct Parent {
    children: Pending,
    source: ArrayRef,
}

enum Pending {
    Struct {
        structure: StructArray,
        valid: Mask,
        field: usize,
        first: Option<usize>,
        last: Option<usize>,
    },
    List {
        coordinates: Coordinates,
        elements: ArrayRef,
        child: Option<usize>,
    },
}

impl Column {
    pub(in crate::local_primitives) fn new(
        array: &ArrayRef,
        context: &mut ExecutionCtx,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        schema: Option<MemoryLease>,
        purpose: Preparation,
        float_policy: FloatPolicy,
    ) -> Result<Self> {
        cancellation.check()?;
        let owner = memory.reserve(size_of::<Self>() as u64)?;
        let mut binding = Binding {
            nodes: ReservedVec::new(memory)?,
            current: None,
            suspended: Frames::new(memory)?,
        };
        binding.nodes.reserve_one()?;
        binding.current = Some(Parent::new(array.clone(), context)?);
        loop {
            let current = binding.current.as_mut().expect("an open native key parent");
            if let Some(child) = current.next(cancellation)? {
                KeyColumn::begin_preparation(cancellation)?;
                let nested = is_container(child.dtype());
                if nested {
                    cancellation.check()?;
                }
                // Reserve for the new node and for every still-open parent.
                binding.nodes.reserve(binding.suspended.len() + 2)?;
                if nested {
                    binding.suspended.reserve_one()?;
                    let next = Parent::new(child, context)?;
                    binding.suspended.push_admitted(
                        binding
                            .current
                            .replace(next)
                            .expect("suspended native key parent"),
                    );
                } else {
                    let leaf = KeyColumn::bind_leaf(
                        &child,
                        context,
                        memory,
                        cancellation,
                        purpose,
                        float_policy,
                    )?;
                    let node = push_node(&mut binding.nodes, Value::Leaf(leaf));
                    current.attach(node, &mut binding.nodes.values);
                }
                continue;
            }
            cancellation.check()?;
            let done = binding.current.take().expect("completed native key parent");
            let node = push_node(&mut binding.nodes, Value::Container(done.finish()));
            let Some(parent) = binding.suspended.pop() else {
                return Ok(Self {
                    nodes: binding.nodes,
                    root: node,
                    cancellation: cancellation.clone(),
                    owner,
                    _schema: schema,
                });
            };
            binding.current = Some(parent);
            cancellation.check()?;
            binding
                .current
                .as_mut()
                .expect("resumed native key parent")
                .attach(node, &mut binding.nodes.values);
        }
    }
}

fn push_node(nodes: &mut ReservedVec<Node>, value: Value) -> usize {
    let position = nodes.values.len();
    debug_assert!(position < nodes.values.capacity());
    nodes.values.push(Node {
        value,
        next_sibling: None,
    });
    position
}

impl Parent {
    fn new(source: ArrayRef, context: &mut ExecutionCtx) -> Result<Self> {
        let children = match source.dtype() {
            DType::Struct(..) => {
                let structure = source
                    .clone()
                    .execute::<StructArray>(context)
                    .map_err(vortex_error)?;
                if structure.len() != source.len() || structure.dtype() != source.dtype() {
                    return Err(failed("struct key execution changed dtype or row count"));
                }
                let valid = structure
                    .validity()
                    .map_err(vortex_error)?
                    .execute_mask(structure.len(), context)
                    .map_err(vortex_error)?;
                Pending::Struct {
                    structure,
                    valid,
                    field: 0,
                    first: None,
                    last: None,
                }
            }
            DType::List(..) | DType::FixedSizeList(..) => {
                let (elements, coordinates) =
                    native_list::Column::new(source.clone(), context)?.into_indexed(context)?;
                Pending::List {
                    coordinates,
                    elements,
                    child: None,
                }
            }
            _ => return Err(failed("nested key has a non-nested dtype")),
        };
        Ok(Self { children, source })
    }

    fn next(&mut self, cancellation: &CancellationToken) -> Result<Option<ArrayRef>> {
        match &mut self.children {
            Pending::Struct {
                structure, field, ..
            } => {
                let Some(child) = structure.unmasked_field_opt(*field) else {
                    return Ok(None);
                };
                cancellation.check()?;
                *field += 1;
                Ok(Some(child.clone()))
            }
            Pending::List {
                elements, child, ..
            } => Ok(child.is_none().then(|| elements.clone())),
        }
    }

    fn attach(&mut self, position: usize, nodes: &mut [Node]) {
        match &mut self.children {
            Pending::Struct { first, last, .. } => {
                if let Some(previous) = last.replace(position) {
                    nodes[previous].next_sibling = Some(position);
                } else {
                    *first = Some(position);
                }
            }
            Pending::List { child, .. } => *child = Some(position),
        }
    }

    fn finish(self) -> Container {
        let children = match self.children {
            Pending::Struct {
                valid,
                field,
                first,
                ..
            } => Children::Struct {
                valid,
                first,
                count: field,
            },
            Pending::List {
                coordinates, child, ..
            } => Children::List {
                coordinates,
                elements: child.expect("completed native list element key"),
            },
        };
        Container {
            children,
            dtype: self.source.dtype().clone(),
            rows: self.source.len(),
        }
    }
}
