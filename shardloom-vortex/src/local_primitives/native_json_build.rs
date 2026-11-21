//! Ordered provider execution into a flat postorder view arena.

use super::{Node, ReservedVec, Tree, native_list, vortex_error};
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::array::{
    ArrayRef, Columnar, ExecutionCtx, IntoArray as _,
    arrays::{
        ExtensionArray, StructArray, extension::ExtensionArrayExt as _,
        struct_::StructArrayExt as _,
    },
    dtype::{DType, FieldNames},
};

enum Frame {
    Struct {
        array: StructArray,
        names: FieldNames,
        fields: ReservedVec<usize>,
    },
    List(native_list::Column),
    // The old recursive call retained the extension view until its storage view
    // finished. Preserve that ownership even though no JSON node is necessary.
    Extension(ExtensionArray),
}

struct Pending(ReservedVec<Frame>);

impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(frame) = self.0.values.pop() {
            drop(frame);
        }
    }
}

pub(super) fn build(
    array: &ArrayRef,
    execution: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Tree> {
    let mut nodes = ReservedVec::new(memory)?;
    let mut pending = Pending(ReservedVec::new(memory)?);
    let mut current = array.clone();
    loop {
        cancellation.check()?;
        let mut complete = match enter(current, execution, memory, &mut pending)? {
            Entry::Node(node) => node,
            Entry::Child(child) => {
                current = child;
                continue;
            }
        };
        loop {
            let Some(frame) = pending.0.values.pop() else {
                return Ok(Tree {
                    nodes,
                    root: complete,
                    memory: memory.clone(),
                });
            };
            match frame {
                Frame::Extension(array) => drop(array),
                Frame::List(column) => {
                    let child = append(&mut nodes, complete)?;
                    complete = Node::List { column, child };
                }
                Frame::Struct {
                    array,
                    names,
                    mut fields,
                } => {
                    // The whole child-index capacity was admitted before its
                    // first child; no sibling is executed while growing it.
                    fields.values.push(append(&mut nodes, complete)?);
                    if fields.values.len() < names.len() {
                        current = array.unmasked_field(fields.values.len()).clone();
                        pending.0.values.push(Frame::Struct {
                            array,
                            names,
                            fields,
                        });
                        break;
                    }
                    complete = Node::Struct {
                        names,
                        fields,
                        validity: array.struct_validity(),
                    };
                }
            }
        }
    }
}

fn append(nodes: &mut ReservedVec<Node>, node: Node) -> Result<usize> {
    let index = nodes.values.len();
    nodes.push(node)?;
    Ok(index)
}

enum Entry {
    Node(Node),
    Child(ArrayRef),
}

fn enter(
    mut current: ArrayRef,
    execution: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
    pending: &mut Pending,
) -> Result<Entry> {
    let node = match current.dtype() {
        DType::Extension(_) if crate::native_uuid::is_uuid(current.dtype()) => {
            Node::Uuid(crate::native_uuid::Values::new(&current, execution)?)
        }
        DType::Extension(_)
            if crate::native_extensions::structured_payload_storage(current.dtype()).is_some() =>
        {
            pending.0.reserve_one()?;
            let array = current
                .execute::<ExtensionArray>(execution)
                .map_err(vortex_error)?;
            current = array.storage_array().clone();
            pending.0.values.push(Frame::Extension(array));
            return Ok(Entry::Child(current));
        }
        DType::Struct(..) => {
            pending.0.reserve_one()?;
            let array = current
                .execute::<StructArray>(execution)
                .map_err(vortex_error)?;
            let names = array.names().clone();
            let mut fields = ReservedVec::new(memory)?;
            fields.reserve(names.len())?;
            if names.is_empty() {
                Node::Struct {
                    names,
                    fields,
                    validity: array.struct_validity(),
                }
            } else {
                current = array.unmasked_field(0).clone();
                pending.0.values.push(Frame::Struct {
                    array,
                    names,
                    fields,
                });
                return Ok(Entry::Child(current));
            }
        }
        DType::List(..) | DType::FixedSizeList(..) => {
            pending.0.reserve_one()?;
            let column = native_list::Column::new(current, execution)?;
            current = column.elements.clone();
            pending.0.values.push(Frame::List(column));
            return Ok(Entry::Child(current));
        }
        DType::Variant(_) => Node::Scalar(current),
        DType::Extension(_) => Node::Scalar(super::super::result_batch::scalar_storage(
            &current, execution,
        )?),
        _ => Node::Scalar(
            current
                .execute::<Columnar>(execution)
                .map_err(vortex_error)?
                .into_array(),
        ),
    };
    Ok(Entry::Node(node))
}
