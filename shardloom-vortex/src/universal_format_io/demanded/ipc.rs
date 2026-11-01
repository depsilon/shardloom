//! IPC physical messages keep independent byte, schema and decoder owners.
//! Messages remain indivisible; output slicing never changes provider work.

use super::{OwnedLocalRead, Result, failed, generic::decoder::source_error};
use crate::universal_format_io::owned_batch::OwnedBatch;
use arrow_buffer::Buffer;
use arrow_ipc::{
    Block, MessageHeader, MetadataVersion,
    reader::{OwnedFileDecoder, OwnedSchema},
};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, LiveMemoryPool, MemoryLease},
};
use std::{mem::size_of, sync::Arc};

#[path = "ipc_memory.rs"]
mod memory;
#[cfg(test)]
#[path = "ipc_tests.rs"]
mod tests;

pub(super) struct Metadata {
    pub(super) schema: OwnedSchema,
    version: MetadataVersion,
    layout: Layout,
    _credit: MemoryLease,
}

enum Layout {
    File {
        dictionaries: Vec<Block>,
        batches: Vec<Block>,
        boundary: u64,
    },
    Stream {
        start: u64,
    },
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| failed("IPC source extent overflow"))
}

fn exact(input: &OwnedLocalRead, start: u64, output: &mut [u8]) -> Result<()> {
    let mut consumed = 0;
    while consumed < output.len() {
        let read = input.read_at(&mut output[consumed..], add(start, consumed as u64)?)?;
        if read == 0 {
            return Err(failed("truncated IPC source framing"));
        }
        consumed += read;
    }
    Ok(())
}

// OwnedLocalRead credits the exact Vec payload. Admit its Bytes owner record,
// this extra owner record, Arrow's shared Bytes record and their Arc headers
// separately. These are named structures, not allocator bookkeeping or RSS.
fn wire(
    input: &OwnedLocalRead,
    start: u64,
    length: usize,
    memory: &LiveMemoryPool,
) -> Result<Buffer> {
    let controls = size_of::<Budgeted<Vec<u8>>>()
        + size_of::<OwnedBytes>()
        + size_of::<bytes::Bytes>()
        + size_of::<Buffer>()
        + 12 * size_of::<usize>();
    let credit = memory.reserve(u64::try_from(controls).map_err(failed)?)?;
    let bytes = input.bytes(start, length)?;
    Ok(Buffer::from(bytes::Bytes::from_owner(OwnedBytes {
        bytes,
        _credit: credit,
    })))
}

struct OwnedBytes {
    bytes: bytes::Bytes,
    _credit: MemoryLease,
}
impl AsRef<[u8]> for OwnedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Return the metadata start/length and body start, without heap allocation.
fn header(input: &OwnedLocalRead, position: u64) -> Result<Option<(u64, usize, u64)>> {
    if position == input.len() {
        return Ok(None);
    }
    let mut length = [0; 4];
    exact(input, position, &mut length)?;
    let start = if length == [255; 4] {
        exact(input, add(position, 4)?, &mut length)?;
        add(position, 8)?
    } else {
        add(position, 4)?
    };
    let length = usize::try_from(i32::from_le_bytes(length)).map_err(failed)?;
    if length == 0 {
        return Ok(None);
    }
    let body = add(start, length as u64)?;
    if body > input.len() {
        return Err(failed("truncated IPC message metadata"));
    }
    Ok(Some((start, length, body)))
}

pub(super) fn discover(
    input: &OwnedLocalRead,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Arc<Metadata>> {
    cancellation.check()?;
    let mut magic = [0; 6];
    if input.len() >= 6 {
        exact(input, 0, &mut magic)?;
    }
    let mut credit = memory.reserve((size_of::<Metadata>() + 2 * size_of::<usize>()) as u64)?;
    let admission = memory::new(memory, cancellation)?;
    let (schema, version, layout) = if &magic == b"ARROW1" {
        if input.len() < 16 {
            return Err(failed("IPC file is too short"));
        }
        let mut tail = [0; 10];
        exact(input, input.len() - 10, &mut tail)?;
        let length = arrow_ipc::reader::read_footer_length(tail).map_err(source_error)?;
        if length == 0 || length > 16 << 20 || length as u64 > input.len() - 16 {
            return Err(failed("IPC footer is malformed or exceeds admission"));
        }
        let boundary = input.len() - 10 - length as u64;
        let bytes = wire(input, boundary, length, memory)?;
        let footer = arrow_ipc::root_as_footer(&bytes).map_err(failed)?;
        let schema = footer
            .schema()
            .ok_or_else(|| failed("IPC footer has no schema"))?;
        let schema = arrow_ipc::reader::read_schema_with_memory(schema, admission.as_ref())
            .map_err(source_error)?;
        let dictionaries = footer.dictionaries();
        let batches = footer
            .recordBatches()
            .ok_or_else(|| failed("IPC footer has no batch index"))?;
        let count = dictionaries
            .map_or(0, |blocks| blocks.len())
            .checked_add(batches.len())
            .ok_or_else(|| failed("IPC block index count overflow"))?;
        let capacity = count
            .checked_mul(size_of::<Block>())
            .ok_or_else(|| failed("IPC block index capacity overflow"))?;
        credit.resize(add(
            credit.bytes(),
            u64::try_from(capacity).map_err(failed)?,
        )?)?;
        let dictionaries = copy_blocks(dictionaries, boundary)?;
        let batches = copy_blocks(Some(batches), boundary)?;
        (
            schema,
            footer.version(),
            Layout::File {
                dictionaries,
                batches,
                boundary,
            },
        )
    } else {
        let (start, length, body) =
            header(input, 0)?.ok_or_else(|| failed("IPC stream has no schema message"))?;
        if length > 16 << 20 {
            return Err(failed("IPC stream schema exceeds metadata admission"));
        }
        let bytes = wire(input, start, length, memory)?;
        let message = arrow_ipc::root_as_message(&bytes).map_err(failed)?;
        if message.bodyLength() != 0 {
            return Err(failed("IPC schema message has a body"));
        }
        let schema = message
            .header_as_schema()
            .ok_or_else(|| failed("IPC first stream message is not a schema"))?;
        let schema = arrow_ipc::reader::read_schema_with_memory(schema, admission.as_ref())
            .map_err(source_error)?;
        (schema, message.version(), Layout::Stream { start: body })
    };
    cancellation.check()?;
    Ok(Arc::new(Metadata {
        schema,
        version,
        layout,
        _credit: credit,
    }))
}

fn block_extent(block: &Block, boundary: u64) -> Result<(u64, usize)> {
    let start = u64::try_from(block.offset()).map_err(failed)?;
    let metadata = u64::try_from(block.metaDataLength()).map_err(failed)?;
    let body = u64::try_from(block.bodyLength()).map_err(failed)?;
    let length = add(metadata, body)?;
    if start < 8 || metadata < 4 || add(start, length)? > boundary {
        return Err(failed("IPC block exceeds admitted file body"));
    }
    Ok((start, usize::try_from(length).map_err(failed)?))
}

fn copy_blocks(
    blocks: Option<flatbuffers::Vector<'_, Block>>,
    boundary: u64,
) -> Result<Vec<Block>> {
    let count = blocks.map_or(0, |blocks| blocks.len());
    let mut output = Vec::new();
    output.try_reserve_exact(count).map_err(failed)?;
    if output.capacity() > count {
        return Err(failed("IPC block index exceeds admitted capacity"));
    }
    if let Some(blocks) = blocks {
        for block in blocks {
            block_extent(block, boundary)?;
            output.push(*block);
        }
    }
    Ok(output)
}

pub(super) struct Reader {
    decoder: Option<OwnedFileDecoder>,
    input: OwnedLocalRead,
    metadata: Arc<Metadata>,
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
    dictionary: usize,
    batch: usize,
    position: u64,
    _credit: MemoryLease,
}

impl Reader {
    pub(super) fn new(
        input: OwnedLocalRead,
        metadata: Arc<Metadata>,
        projection: &[usize],
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let credit = memory.reserve(size_of::<Self>() as u64)?;
        let decoder = OwnedFileDecoder::new(
            metadata.schema.clone(),
            metadata.version,
            Some(projection),
            memory::new(memory, cancellation)?,
        )
        .map_err(source_error)?;
        let position = match metadata.layout {
            Layout::Stream { start } => start,
            Layout::File { .. } => 0,
        };
        Ok(Self {
            decoder: Some(decoder),
            input,
            metadata,
            memory: memory.clone(),
            cancellation: cancellation.clone(),
            dictionary: 0,
            batch: 0,
            position,
            _credit: credit,
        })
    }

    pub(super) fn next(&mut self) -> Result<Option<OwnedBatch>> {
        if self.decoder.is_none() {
            return Ok(None);
        }
        let output = self.read_next();
        if !matches!(output, Ok(Some(_))) {
            self.decoder.take();
        }
        output
    }

    fn read_next(&mut self) -> Result<Option<OwnedBatch>> {
        loop {
            self.cancellation.check()?;
            let decoder = self
                .decoder
                .as_mut()
                .ok_or_else(|| failed("IPC decoder owner missing"))?;
            let output = match &self.metadata.layout {
                Layout::File {
                    dictionaries,
                    batches,
                    boundary,
                } => {
                    if let Some(block) = dictionaries.get(self.dictionary) {
                        let (start, length) = block_extent(block, *boundary)?;
                        let input = wire(&self.input, start, length, &self.memory)?;
                        decoder
                            .read_dictionary(block, &input)
                            .map_err(source_error)?;
                        self.dictionary += 1;
                        continue;
                    }
                    let Some(block) = batches.get(self.batch) else {
                        return Ok(None);
                    };
                    let (start, length) = block_extent(block, *boundary)?;
                    let input = wire(&self.input, start, length, &self.memory)?;
                    let output = decoder
                        .read_record_batch(block, &input)
                        .map_err(source_error)?
                        .ok_or_else(|| {
                            failed("IPC record batch index contains a non-batch message")
                        })?;
                    self.batch += 1;
                    output
                }
                Layout::Stream { .. } => {
                    let Some((start, length, body)) = header(&self.input, self.position)? else {
                        return Ok(None);
                    };
                    let metadata = wire(&self.input, start, length, &self.memory)?;
                    let message = arrow_ipc::root_as_message(&metadata).map_err(failed)?;
                    let length = usize::try_from(message.bodyLength()).map_err(failed)?;
                    let next = add(body, length as u64)?;
                    if next > self.input.len() {
                        return Err(failed("truncated IPC message body"));
                    }
                    let input = wire(&self.input, body, length, &self.memory)?;
                    match message.header_type() {
                        MessageHeader::DictionaryBatch => {
                            decoder
                                .read_dictionary_message(&message, &input)
                                .map_err(source_error)?;
                            self.position = next;
                            continue;
                        }
                        MessageHeader::RecordBatch => {
                            let output = decoder
                                .read_record_batch_message(&message, &input)
                                .map_err(source_error)?
                                .ok_or_else(|| failed("IPC stream batch message missing"))?;
                            self.position = next;
                            output
                        }
                        _ => return Err(failed("unexpected IPC stream message type")),
                    }
                }
            };
            self.cancellation.check()?;
            let (batch, owner) = output.into_parts();
            return Ok(Some(OwnedBatch::with_owner(batch, owner)));
        }
    }
}
