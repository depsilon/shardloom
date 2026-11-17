// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Native serialization owners and ordered FlatBuffer writing without recursion.

use flatbuffers::{FlatBufferBuilder, Vector, WIPOffset};
use vortex_buffer::ByteBuffer;
use vortex_error::{VortexResult, vortex_bail, vortex_err};
use vortex_session::VortexSession;

use crate::array::ArrayId;
use crate::flatbuffers::{WriteFlatBuffer, array as fba};
use crate::session::ArraySessionExt;
use crate::{ArrayContext, ArrayRef};

struct Node {
    source: ArrayRef,
    serialized_id: ArrayId,
    metadata: Vec<u8>,
    buffers: Vec<ByteBuffer>,
    // Nodes are in preorder. The next sibling starts after this subtree.
    end: usize,
    buffer_start: usize,
}

pub(super) struct ArraySerializationTree {
    nodes: Vec<Node>,
    nbuffers: usize,
}

impl ArraySerializationTree {
    fn try_new(session: &VortexSession, source: &ArrayRef) -> VortexResult<Self> {
        let mut nodes = Vec::<Node>::new();
        let mut pending = Vec::<(usize, std::vec::IntoIter<ArrayRef>)>::new();
        let mut source = source.clone();
        'descend: loop {
            let Some(serialization) = session.array_serialize(&source)? else {
                vortex_bail!(
                    "Array {} does not support serialization",
                    source.encoding_id()
                );
            };
            let index = nodes.len();
            nodes.push(Node {
                source,
                serialized_id: serialization.serialized_id,
                metadata: serialization.metadata,
                buffers: serialization.buffers,
                end: 0,
                buffer_start: 0,
            });
            let mut children = serialization.children.into_iter();
            if let Some(child) = children.next() {
                pending.push((index, children));
                source = child;
                continue;
            }
            nodes[index].end = nodes.len();
            while let Some((index, mut children)) = pending.pop() {
                if let Some(child) = children.next() {
                    pending.push((index, children));
                    source = child;
                    continue 'descend;
                }
                nodes[index].end = nodes.len();
            }
            break;
        }
        // The original tree finishes every serializer before counting or
        // rejecting buffers. Keep that callback/error boundary.
        let mut nbuffers = 0;
        for node in &mut nodes {
            node.buffer_start = nbuffers;
            nbuffers += node.buffers.len();
        }
        Ok(Self { nodes, nbuffers })
    }

    fn subtree_nbuffers(&self, index: usize) -> usize {
        let node = &self.nodes[index];
        self.nodes
            .get(node.end)
            .map_or(self.nbuffers, |next| next.buffer_start)
            - node.buffer_start
    }

    pub(super) fn buffers(&self) -> Vec<ByteBuffer> {
        let mut buffers = Vec::with_capacity(self.nbuffers);
        for node in &self.nodes {
            buffers.extend(node.buffers.iter().cloned());
        }
        buffers
    }
}

/// A utility struct for creating an [`fba::ArrayNode`] flatbuffer.
pub struct ArrayNodeFlatBuffer<'a> {
    ctx: &'a ArrayContext,
    pub(super) array: ArraySerializationTree,
}

struct Frame<'fb> {
    node: usize,
    next_child: usize,
    encoding: u16,
    metadata: WIPOffset<Vector<'fb, u8>>,
    buffer_idx: u16,
    nbuffers: u16,
    child_buffer_idx: u16,
    children: Vec<WIPOffset<fba::ArrayNode<'fb>>>,
}

impl<'a> ArrayNodeFlatBuffer<'a> {
    pub fn try_new(
        ctx: &'a ArrayContext,
        session: &'a VortexSession,
        array: &ArrayRef,
    ) -> VortexResult<Self> {
        let array = ArraySerializationTree::try_new(session, array)?;
        if array.nbuffers > u16::MAX as usize {
            vortex_bail!(
                "Array and all descendent arrays can have at most u16::MAX buffers: {}",
                array.nbuffers
            );
        }
        Ok(Self { ctx, array })
    }

    fn enter<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
        index: usize,
        buffer_idx: u16,
    ) -> VortexResult<Frame<'fb>> {
        let node = &self.array.nodes[index];
        let encoding = self.ctx.intern(&node.serialized_id).ok_or_else(|| {
            vortex_err!(
                "Serialized array ID {} not permitted by ctx",
                node.serialized_id
            )
        })?;
        let metadata = fbb.create_vector(node.metadata.as_slice());
        let nbuffers = u16::try_from(node.buffers.len())
            .map_err(|_| vortex_err!("Array can have at most u16::MAX buffers"))?;
        Ok(Frame {
            node: index,
            next_child: index + 1,
            encoding,
            metadata,
            buffer_idx,
            nbuffers,
            child_buffer_idx: buffer_idx + nbuffers,
            children: Vec::new(),
        })
    }

    pub fn try_write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<fba::ArrayNode<'fb>>> {
        let mut pending = Vec::new();
        let mut frame = self.enter(fbb, 0, 0)?;
        loop {
            let node = &self.array.nodes[frame.node];
            if frame.next_child < node.end {
                let child = frame.next_child;
                frame.next_child = self.array.nodes[child].end;
                let buffer_idx = frame.child_buffer_idx;
                pending.push(frame);
                frame = self.enter(fbb, child, buffer_idx)?;
                continue;
            }
            let children = Some(fbb.create_vector(&frame.children));
            let buffers = Some(
                fbb.create_vector_from_iter((0..frame.nbuffers).map(|i| i + frame.buffer_idx)),
            );
            let stats = Some(node.source.statistics().write_flatbuffer(fbb)?);
            let result = fba::ArrayNode::create(
                fbb,
                &fba::ArrayNodeArgs {
                    encoding: frame.encoding,
                    metadata: Some(frame.metadata),
                    children,
                    buffers,
                    stats,
                },
            );
            let Some(mut parent) = pending.pop() else {
                return Ok(result);
            };
            parent.child_buffer_idx = u16::try_from(self.array.subtree_nbuffers(frame.node))
                .ok()
                .and_then(|nbuffers| nbuffers.checked_add(parent.child_buffer_idx))
                .ok_or_else(|| vortex_err!("Too many buffers (u16) for Array"))?;
            parent.children.push(result);
            frame = parent;
        }
    }
}

#[cfg(test)]
#[path = "serialization_tests.rs"]
mod tests;
