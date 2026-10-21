// SPDX-License-Identifier: Apache-2.0
//! Admit only the prevalidated finite Arrow stream before allocating decode.
//! The declared IPC size is not a generic decompression allocation guarantee.

use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Metadata {
    pub(crate) decoded_bytes: u64,
    pub(crate) nodes: usize,
    pub(crate) buffers: usize,
    pub(crate) kind: String,
}

#[derive(Clone)]
pub(crate) struct ExpectedFrame {
    pub(crate) header_bytes: usize,
    pub(crate) header_sha256: Bytes,
    pub(crate) body_bytes: usize,
    pub(crate) body_sha256: Bytes,
    pub(crate) metadata: Metadata,
}

#[derive(Clone)]
pub(crate) struct Expected {
    pub(crate) frames: Vec<ExpectedFrame>,
    pub(crate) ordered_sha256: Bytes,
}

pub(crate) fn validate_data(data: &FlightData, expected: &ExpectedFrame, mode: Mode) -> Result<()> {
    if data.data_header.len() != expected.header_bytes
        || data.data_body.len() != expected.body_bytes
        || hash(&data.data_header) != expected.header_sha256
        || hash(&data.data_body) != expected.body_sha256
    {
        return Err(mismatch(
            "Arrow frame differs from prevalidated authorized bytes",
        ));
    }
    if inspect(data, mode)? != expected.metadata {
        return Err(mismatch(
            "Arrow frame metadata differs from frozen declaration",
        ));
    }
    Ok(())
}

pub(crate) fn inspect(data: &FlightData, mode: Mode) -> Result<Metadata> {
    if data.data_header.len() > 64 << 10 {
        return Err(limit("Arrow IPC header exceeds its byte limit"));
    }
    let message = arrow_ipc::root_as_message(&data.data_header)
        .map_err(|_| invalid("invalid Arrow IPC header"))?;
    if message.version() != arrow_ipc::MetadataVersion::V5
        || u64::try_from(message.bodyLength()).ok() != Some(data.data_body.len() as u64)
    {
        return Err(invalid("Arrow IPC version or body length differs"));
    }
    let expected_codec = match mode {
        Mode::ArrowNone => None,
        Mode::ArrowLz4 => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        Mode::ArrowZstd => Some(arrow_ipc::CompressionType::ZSTD),
        _ => return Err(mismatch("Arrow inspection requires an explicit Arrow mode")),
    };
    let mut metadata = Metadata {
        decoded_bytes: 0,
        nodes: 0,
        buffers: 0,
        kind: message
            .header_type()
            .variant_name()
            .unwrap_or("unknown")
            .to_owned(),
    };
    let batch = match message.header_type() {
        arrow_ipc::MessageHeader::Schema => {
            if !data.data_body.is_empty() || message.header_as_schema().is_none() {
                return Err(invalid("invalid Arrow schema message"));
            }
            return Ok(metadata);
        }
        arrow_ipc::MessageHeader::RecordBatch => message.header_as_record_batch(),
        arrow_ipc::MessageHeader::DictionaryBatch => {
            let dictionary = message
                .header_as_dictionary_batch()
                .ok_or_else(|| invalid("missing Arrow dictionary"))?;
            if dictionary.isDelta() {
                return Err(invalid("dictionary deltas are outside the frozen profile"));
            }
            dictionary.data()
        }
        _ => return Err(invalid("unsupported Arrow IPC message kind")),
    }
    .ok_or_else(|| invalid("missing Arrow record metadata"))?;
    if !(0..=65_536).contains(&batch.length()) {
        return Err(limit("Arrow record rows exceed their limit"));
    }
    if batch.compression().map(|codec| codec.codec()) != expected_codec
        || batch
            .compression()
            .is_some_and(|codec| codec.method() != arrow_ipc::BodyCompressionMethod::BUFFER)
    {
        return Err(mismatch("Arrow IPC compression differs from explicit mode"));
    }
    let nodes = batch
        .nodes()
        .ok_or_else(|| invalid("missing Arrow field nodes"))?;
    let buffers = batch
        .buffers()
        .ok_or_else(|| invalid("missing Arrow buffers"))?;
    metadata.nodes = nodes.len();
    metadata.buffers = buffers.len();
    if metadata.nodes > 4096 || metadata.buffers > 4096 {
        return Err(limit("Arrow field nodes or buffers exceed their limit"));
    }
    for node in nodes {
        if node.length() < 0 || !(0..=node.length()).contains(&node.null_count()) {
            return Err(invalid("invalid Arrow field-node lengths"));
        }
    }
    metadata.decoded_bytes =
        decoded_buffer_bytes(buffers, &data.data_body, expected_codec.is_some())?;
    Ok(metadata)
}

fn decoded_buffer_bytes(
    buffers: flatbuffers::Vector<'_, arrow_ipc::Buffer>,
    body: &[u8],
    compressed: bool,
) -> Result<u64> {
    let mut decoded_bytes = 0_u64;
    for buffer in buffers {
        let start = usize::try_from(buffer.offset())
            .map_err(|_| invalid("negative Arrow buffer offset"))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| invalid("negative Arrow buffer length"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("Arrow buffer range overflow"))?;
        let bytes = body
            .get(start..end)
            .ok_or_else(|| invalid("Arrow buffer range exceeds body"))?;
        let decoded = if compressed && !bytes.is_empty() {
            let prefix = bytes
                .get(..8)
                .ok_or_else(|| invalid("truncated Arrow compression prefix"))?;
            let declared = i64::from_le_bytes(
                prefix
                    .try_into()
                    .map_err(|_| invalid("invalid Arrow compression prefix"))?,
            );
            if declared == -1 {
                (bytes.len() - 8) as u64
            } else {
                u64::try_from(declared).map_err(|_| invalid("negative Arrow decoded length"))?
            }
        } else {
            bytes.len() as u64
        };
        decoded_bytes = decoded_bytes
            .checked_add(decoded)
            .filter(|total| *total <= MAX_PAYLOAD)
            .ok_or_else(|| limit("Arrow decoded declarations exceed their limit"))?;
    }
    Ok(decoded_bytes)
}

pub(crate) struct ArrowStream {
    binding: Binding,
    mode: Mode,
    expected: Expected,
    payload_bytes: u64,
    next_frame: usize,
    ordered_hash: Sha256,
    failed: bool,
    complete: bool,
}

impl ArrowStream {
    pub(crate) fn new(binding: Binding, mode: Mode, expected: Expected) -> Result<Self> {
        validate_binding(&binding, &binding)?;
        if !matches!(mode, Mode::ArrowNone | Mode::ArrowLz4 | Mode::ArrowZstd) {
            return Err(mismatch("Arrow stream requires an explicit Arrow mode"));
        }
        if expected.frames.is_empty()
            || expected.frames.len() as u64 > MAX_FRAMES
            || expected.ordered_sha256.len() != 32
        {
            return Err(limit("invalid frozen Arrow frame authority"));
        }
        let mut payload_bytes = 0_u64;
        let mut decoded_bytes = 0_u64;
        for (index, frame) in expected.frames.iter().enumerate() {
            if frame.header_bytes == 0
                || frame.header_bytes > 64 << 10
                || frame.header_sha256.len() != 32
                || frame.body_sha256.len() != 32
                || frame.metadata.nodes > 4096
                || frame.metadata.buffers > 4096
                || if index == 0 {
                    frame.metadata.kind != "Schema"
                } else {
                    !matches!(
                        frame.metadata.kind.as_str(),
                        "RecordBatch" | "DictionaryBatch"
                    )
                }
            {
                return Err(invalid("invalid frozen Arrow frame identity"));
            }
            payload_bytes = payload_bytes
                .checked_add(frame.header_bytes as u64)
                .and_then(|total| total.checked_add(frame.body_bytes as u64))
                .filter(|total| *total <= MAX_PAYLOAD)
                .ok_or_else(|| limit("frozen Arrow payload exceeds limit"))?;
            decoded_bytes = decoded_bytes
                .checked_add(frame.metadata.decoded_bytes)
                .filter(|total| *total <= MAX_PAYLOAD)
                .ok_or_else(|| limit("frozen Arrow decoded bytes exceed limit"))?;
        }
        Ok(Self {
            binding,
            mode,
            expected,
            payload_bytes,
            next_frame: 0,
            ordered_hash: Sha256::new(),
            failed: false,
            complete: false,
        })
    }

    pub(crate) fn accept(&mut self, data: &FlightData) -> Result<()> {
        if self.failed {
            return Err(invalid("failed Arrow attempt cannot resume"));
        }
        let result = self.accept_inner(data);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn accept_inner(&mut self, data: &FlightData) -> Result<()> {
        if self.complete {
            return Err(invalid("Arrow frame after completion"));
        }
        let frame = decode_frame(data)?;
        validate_binding(
            frame
                .binding
                .as_ref()
                .ok_or_else(|| invalid("missing Arrow frame binding"))?,
            &self.binding,
        )?;
        if frame.sequence != self.next_frame as u64 {
            return Err(Error {
                code: "SLX_SEQUENCE",
                detail: "duplicate, skipped or reordered Arrow frame",
            });
        }
        let Some(Body::Arrow(position)) = frame.body else {
            return Err(invalid("expected standard Arrow frame metadata"));
        };
        let expected = self
            .expected
            .frames
            .get(self.next_frame)
            .ok_or_else(|| invalid("extra Arrow frame"))?;
        // These exact identities precede both metadata admission and any call to
        // the standard allocating decoder. Sender-provided digests do not count.
        validate_data(data, expected, self.mode)?;
        if self.next_frame == 0 {
            let ack = position
                .ack
                .ok_or_else(|| invalid("Arrow Schema requires ACK"))?;
            if ack.profile.as_ref() != Some(&profile())
                || ack.mode != self.mode as i32
                || ack.expected_payload_sha256 != self.expected.ordered_sha256
                || ack.expected_payload_bytes != self.payload_bytes
                || ack.expected_messages as usize != self.expected.frames.len()
            {
                return Err(mismatch("Arrow ACK differs from frozen authority"));
            }
        } else if position.ack.is_some() {
            return Err(invalid("duplicate Arrow ACK"));
        }
        for bytes in [&data.data_header, &data.data_body] {
            self.ordered_hash.update((bytes.len() as u64).to_le_bytes());
            self.ordered_hash.update(bytes);
        }
        self.next_frame += 1;
        if self.next_frame == self.expected.frames.len() {
            let completion = position
                .complete
                .ok_or_else(|| invalid("last Arrow frame requires completion"))?;
            if completion.frames != self.next_frame as u64
                || completion.messages as usize != self.next_frame
                || completion.rows != self.binding.rows
                || completion.payload_bytes != self.payload_bytes
                || completion.ordered_payload_sha256 != self.expected.ordered_sha256
                || &self.ordered_hash.clone().finalize()[..]
                    != self.expected.ordered_sha256.as_ref()
            {
                return Err(mismatch("Arrow completion differs from frozen authority"));
            }
            self.complete = true;
        } else if position.complete.is_some() {
            return Err(invalid("premature Arrow completion"));
        }
        Ok(())
    }

    pub(crate) fn finish(self, rpc_succeeded: bool) -> Result<()> {
        if self.failed || !self.complete || !rpc_succeeded {
            return Err(Error {
                code: "SLX_INCOMPLETE",
                detail: "Arrow attempt requires completion, clean EOF and successful RPC status",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "native_exchange_arrow_protocol_tests.rs"]
mod tests;
