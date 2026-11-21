// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Original 6d84c2749 recursive array serialization for shallow reference tests.

use super::*;
use crate::flatbuffers::FlatBuffer;
use crate::flatbuffers::array::Compression;
use crate::serde::SerializeOptions;
use std::iter;
use vortex_buffer::Alignment;
use vortex_error::VortexExpect;

pub(super) fn serialize(
    array: &ArrayRef,
    ctx: &ArrayContext,
    session: &VortexSession,
    options: &SerializeOptions,
) -> VortexResult<Vec<ByteBuffer>> {
    // Resolve the wire representation once. Serializers may choose historical IDs and may
    // provide downgraded buffers or children that differ from the in-memory array tree.
    let root = ArrayNodeFlatBuffer::try_new(ctx, session, array)?;
    let array_buffers = root.array.buffers();

    // Allocate result buffers, including a possible padding buffer for each.
    let mut buffers = vec![];
    let mut fb_buffers = Vec::with_capacity(buffers.capacity());

    // If we're including padding, we need to find the maximum required buffer alignment.
    let max_alignment = array_buffers
        .iter()
        .map(|buf| buf.alignment())
        .chain(iter::once(FlatBuffer::alignment()))
        .max()
        .unwrap_or_else(FlatBuffer::alignment);

    // Create a shared buffer of zeros we can use for padding
    let zeros = ByteBuffer::zeroed(max_alignment.as_usize());

    // We push an empty buffer with the maximum alignment, so then subsequent buffers
    // will be aligned. For subsequent buffers, we always push a 1-byte alignment.
    buffers.push(ByteBuffer::zeroed_aligned(0, max_alignment));

    // Keep track of where we are in the "file" to calculate padding.
    let mut pos = options.offset;

    // Push all the array buffers with padding as necessary.
    for buffer in array_buffers {
        let padding = if options.include_padding {
            let padding = pos.next_multiple_of(buffer.alignment().as_usize()) - pos;
            if padding > 0 {
                pos += padding;
                buffers.push(zeros.slice(0..padding));
            }
            padding
        } else {
            0
        };

        fb_buffers.push(fba::Buffer::new(
            u16::try_from(padding).vortex_expect("padding fits into u16"),
            buffer.alignment().exponent(),
            Compression::None,
            u32::try_from(buffer.len())
                .map_err(|_| vortex_err!("All buffers must fit into u32 for serialization"))?,
        ));

        pos += buffer.len();
        buffers.push(buffer.aligned(Alignment::none()));
    }

    // Set up the flatbuffer builder
    let mut fbb = FlatBufferBuilder::new();

    let fb_root = root.try_write_flatbuffer(&mut fbb)?;

    let fb_buffers = fbb.create_vector(&fb_buffers);
    let fb_array = fba::Array::create(
        &mut fbb,
        &fba::ArrayArgs {
            root: Some(fb_root),
            buffers: Some(fb_buffers),
        },
    );
    fbb.finish_minimal(fb_array);
    let (fb_vec, fb_start) = fbb.collapse();
    let fb_end = fb_vec.len();
    let fb_buffer = ByteBuffer::from(fb_vec).slice(fb_start..fb_end);
    let fb_length = fb_buffer.len();

    if options.include_padding {
        let padding = pos.next_multiple_of(FlatBuffer::alignment().as_usize()) - pos;
        if padding > 0 {
            buffers.push(zeros.slice(0..padding));
        }
    }
    buffers.push(fb_buffer);

    // Finally, we write down the u32 length for the flatbuffer.
    buffers.push(ByteBuffer::from(
        u32::try_from(fb_length)
            .map_err(|_| vortex_err!("Array metadata flatbuffer must fit into u32 for serialization. Array encoding tree is too large."))?
            .to_le_bytes()
            .to_vec(),
    ));

    Ok(buffers)
}

#[derive(Clone, Debug)]
struct ArraySerializationTree {
    source: ArrayRef,
    serialized_id: ArrayId,
    metadata: Vec<u8>,
    buffers: Vec<ByteBuffer>,
    children: Vec<ArraySerializationTree>,
}

impl ArraySerializationTree {
    fn try_new(session: &VortexSession, source: &ArrayRef) -> VortexResult<Self> {
        let Some(serialization) = session.array_serialize(source)? else {
            vortex_bail!(
                "Array {} does not support serialization",
                source.encoding_id()
            );
        };
        let children = serialization
            .children
            .iter()
            .map(|child| Self::try_new(session, child))
            .collect::<VortexResult<Vec<_>>>()?;

        Ok(Self {
            source: source.clone(),
            serialized_id: serialization.serialized_id,
            metadata: serialization.metadata,
            buffers: serialization.buffers,
            children,
        })
    }

    fn nbuffers_recursive(&self) -> usize {
        self.buffers.len()
            + self
                .children
                .iter()
                .map(Self::nbuffers_recursive)
                .sum::<usize>()
    }

    fn buffers(&self) -> Vec<ByteBuffer> {
        let mut buffers = Vec::with_capacity(self.nbuffers_recursive());
        self.append_buffers(&mut buffers);
        buffers
    }

    fn append_buffers(&self, buffers: &mut Vec<ByteBuffer>) {
        buffers.extend(self.buffers.iter().cloned());
        for child in &self.children {
            child.append_buffers(buffers);
        }
    }
}

/// A utility struct for creating an [`fba::ArrayNode`] flatbuffer.
pub struct ArrayNodeFlatBuffer<'a> {
    ctx: &'a ArrayContext,
    array: ArraySerializationTree,
}

impl<'a> ArrayNodeFlatBuffer<'a> {
    pub fn try_new(
        ctx: &'a ArrayContext,
        session: &'a VortexSession,
        array: &ArrayRef,
    ) -> VortexResult<Self> {
        let array = ArraySerializationTree::try_new(session, array)?;
        let n_buffers_recursive = array.nbuffers_recursive();
        if n_buffers_recursive > u16::MAX as usize {
            vortex_bail!(
                "Array and all descendent arrays can have at most u16::MAX buffers: {}",
                n_buffers_recursive
            );
        };
        Ok(Self { ctx, array })
    }

    pub fn try_write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<fba::ArrayNode<'fb>>> {
        self.try_write_node(fbb, &self.array, 0)
    }

    fn try_write_node<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
        array: &ArraySerializationTree,
        buffer_idx: u16,
    ) -> VortexResult<WIPOffset<fba::ArrayNode<'fb>>> {
        let encoding_idx = self.ctx.intern(&array.serialized_id).ok_or_else(|| {
            vortex_err!(
                "Serialized array ID {} not permitted by ctx",
                array.serialized_id
            )
        })?;

        let metadata = Some(fbb.create_vector(array.metadata.as_slice()));

        // Assign buffer indices for all child arrays.
        let nbuffers = u16::try_from(array.buffers.len())
            .map_err(|_| vortex_err!("Array can have at most u16::MAX buffers"))?;
        let mut child_buffer_idx = buffer_idx + nbuffers;

        let children = array
            .children
            .iter()
            .map(|child| {
                // Update the number of buffers required.
                let msg = self.try_write_node(fbb, child, child_buffer_idx)?;

                child_buffer_idx = u16::try_from(child.nbuffers_recursive())
                    .ok()
                    .and_then(|nbuffers| nbuffers.checked_add(child_buffer_idx))
                    .ok_or_else(|| vortex_err!("Too many buffers (u16) for Array"))?;

                Ok(msg)
            })
            .collect::<VortexResult<Vec<_>>>()?;
        let children = Some(fbb.create_vector(&children));

        let buffers = Some(fbb.create_vector_from_iter((0..nbuffers).map(|i| i + buffer_idx)));
        let stats = Some(array.source.statistics().write_flatbuffer(fbb)?);

        Ok(fba::ArrayNode::create(
            fbb,
            &fba::ArrayNodeArgs {
                encoding: encoding_idx,
                metadata,
                children,
                buffers,
                stats,
            },
        ))
    }
}
