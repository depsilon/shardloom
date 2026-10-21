// SPDX-License-Identifier: Apache-2.0
//! Strict framing for the frozen finite exchange. No socket or payload decoder.
//! Callers must retain receiving credit and private payload ownership until
//! `finish` succeeds after clean transport EOF and successful RPC status.

#[path = "native_exchange_wire.rs"]
pub(crate) mod wire;

#[path = "native_exchange_arrow_protocol.rs"]
pub(crate) mod arrow;

#[path = "native_exchange_authority.rs"]
pub(crate) mod authority;

#[path = "native_exchange_attempts.rs"]
mod attempts;

#[path = "native_exchange_queue.rs"]
mod queue;

#[path = "native_exchange_transport.rs"]
mod transport;

#[path = "native_exchange_payload.rs"]
mod payload;

#[path = "native_exchange_service.rs"]
mod service;

#[path = "native_exchange_peer.rs"]
pub(crate) mod peer;

use arrow_flight::FlightData;
use bytes::Bytes;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use wire::{Ack, Binding, Complete, Frame, Limits, Mode, Open, Profile, frame::Body};

pub(crate) const MAX_RPC: usize = 2 << 20;
pub(crate) const MAX_CONTROL: usize = 16 << 10;
pub(crate) const MAX_PAYLOAD: u64 = 64 << 20;
pub(crate) const MAX_FRAGMENT: usize = 1 << 20;
pub(crate) const MAX_SEGMENTS: usize = 128;
pub(crate) const MAX_FRAMES: u64 = 256;

impl Mode {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::NativeFragments => "native_fragments",
            Self::NativeAssembled => "native_assembled",
            Self::ArrowNone => "arrow_none",
            Self::ArrowLz4 => "arrow_lz4",
            Self::ArrowZstd => "arrow_zstd",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Error {
    pub(crate) code: &'static str,
    pub(crate) detail: &'static str,
}

type Result<T> = std::result::Result<T, Error>;

fn invalid(detail: &'static str) -> Error {
    Error {
        code: "SLX_INVALID_WIRE",
        detail,
    }
}

fn mismatch(detail: &'static str) -> Error {
    Error {
        code: "SLX_IDENTITY",
        detail,
    }
}

fn limit(detail: &'static str) -> Error {
    Error {
        code: "SLX_RESOURCE_LIMIT",
        detail,
    }
}

pub(crate) fn hash(bytes: &[u8]) -> Bytes {
    Bytes::copy_from_slice(&Sha256::digest(bytes))
}

pub(crate) fn strict_decode<M: prost::Message + Default>(bytes: Bytes, max: usize) -> Result<M> {
    if bytes.len() > max {
        return Err(limit("extension message exceeds its byte limit"));
    }
    let original = bytes.clone();
    let message = M::decode(bytes).map_err(|_| invalid("malformed extension protobuf"))?;
    if message.encode_to_vec() != original {
        return Err(invalid(
            "extension protobuf must have one canonical encoding",
        ));
    }
    Ok(message)
}

pub(crate) fn profile() -> Profile {
    Profile {
        extension_version: 1,
        vortex_version: "0.85.0".to_owned(),
        vortex_message_version: 0,
        physical_registry: [
            "vortex.null",
            "vortex.bool",
            "vortex.primitive",
            "vortex.decimal",
            "vortex.varbin",
            "vortex.varbinview",
            "vortex.list",
            "vortex.listview",
            "vortex.fixed_size_list",
            "vortex.struct",
            "vortex.ext",
            "vortex.constant",
            "vortex.dict",
            "vortex.chunked",
            "vortex.runend",
            "fastlanes.bitpacked",
        ]
        .map(str::to_owned)
        .to_vec(),
        limits: Some(Limits {
            native_memory_bytes: 256 << 20,
            queued_bytes: 4 << 20,
            queued_frames: 4,
            fragment_payload_bytes: 1 << 20,
            encoded_rpc_bytes: 2 << 20,
            control_bytes: 16 << 10,
            complete_payload_bytes: MAX_PAYLOAD,
            frames: 256,
            fragment_segments: 128,
        }),
    }
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(crate) fn validate_binding(actual: &Binding, expected: &Binding) -> Result<()> {
    if !identifier(&actual.principal)
        || !identifier(&actual.source_id)
        || !identifier(&actual.query_id)
        || actual.server_epoch.len() != 16
        || actual.attempt_id.len() != 16
        || actual.server_epoch.iter().all(|byte| *byte == 0)
        || actual.attempt_id.iter().all(|byte| *byte == 0)
        || actual.source_sha256.len() != 32
        || actual.dtype_sha256.len() != 32
        || actual.operation_sha256.len() != 32
        || actual.policy_version == 0
        || actual.partition_id != 0
        || actual.first_row != 0
        || actual.rows > 65_536
    {
        return Err(invalid("invalid operation binding shape"));
    }
    if actual != expected {
        return Err(mismatch(
            "operation binding differs from authorized identity",
        ));
    }
    Ok(())
}

pub(crate) fn validate_open(open: &Open, binding: &Binding, mode: Mode) -> Result<()> {
    if mode == Mode::Unspecified || Mode::try_from(open.mode).ok() != Some(mode) {
        return Err(mismatch(
            "explicit exchange mode differs; native retry is forbidden",
        ));
    }
    validate_binding(
        open.binding
            .as_ref()
            .ok_or_else(|| invalid("missing operation binding"))?,
        binding,
    )?;
    if open.profile.as_ref() != Some(&profile()) {
        return Err(mismatch("exchange version, registry or limits differ"));
    }
    Ok(())
}

// Scan without allocation before Prost can construct a Vec for every repeated
// segment. The RPC byte ceiling alone would allow many empty byte fields.
fn varint(input: &mut &[u8]) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let (&byte, tail) = input
            .split_first()
            .ok_or_else(|| invalid("truncated protobuf varint"))?;
        *input = tail;
        if shift == 63 && byte > 1 {
            return Err(invalid("overflowing protobuf varint"));
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    Err(invalid("overflowing protobuf varint"))
}

fn delimited<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = usize::try_from(varint(input)?)
        .map_err(|_| invalid("protobuf length exceeds address space"))?;
    let (head, tail) = input
        .split_at_checked(len)
        .ok_or_else(|| invalid("truncated protobuf field"))?;
    *input = tail;
    Ok(head)
}

fn scan_fragment(mut input: &[u8]) -> Result<()> {
    let mut previous = 0;
    let mut segments = 0;
    let mut payload = 0;
    while !input.is_empty() {
        let key = varint(&mut input)?;
        let field = key >> 3;
        if field < previous || (field == previous && field != 4) {
            return Err(invalid("noncanonical native fragment fields"));
        }
        previous = field;
        match (field, key & 7) {
            (1..=3, 0) => {
                varint(&mut input)?;
            }
            (4, 2) => {
                let bytes = delimited(&mut input)?;
                if bytes.is_empty() {
                    return Err(invalid("empty native segment"));
                }
                segments += 1;
                payload += bytes.len();
                if segments > MAX_SEGMENTS || payload > MAX_FRAGMENT {
                    return Err(limit("native fragment exceeds segment or payload limit"));
                }
            }
            _ => return Err(invalid("unknown native fragment field")),
        }
    }
    if segments == 0 {
        return Err(invalid("native fragment has no segments"));
    }
    Ok(())
}

fn scan_frame(mut input: &[u8]) -> Result<()> {
    let mut previous = 0;
    let mut alternatives = 0;
    while !input.is_empty() {
        let key = varint(&mut input)?;
        let field = key >> 3;
        if field <= previous {
            return Err(invalid("duplicate or reordered frame fields"));
        }
        previous = field;
        match (field, key & 7) {
            (2, 0) => {
                varint(&mut input)?;
            }
            (1 | 3..=6, 2) => {
                let value = delimited(&mut input)?;
                if field == 4 {
                    scan_fragment(value)?;
                } else if value.len() > MAX_CONTROL {
                    return Err(limit("control submessage exceeds its byte limit"));
                }
                if field >= 3 {
                    alternatives += 1;
                }
            }
            _ => return Err(invalid("unknown frame field")),
        }
    }
    if alternatives != 1 {
        return Err(invalid("frame requires exactly one alternative"));
    }
    Ok(())
}

pub(crate) fn decode_frame(data: &FlightData) -> Result<Frame> {
    if data.flight_descriptor.is_some() {
        return Err(invalid("response frame cannot carry a descriptor"));
    }
    if data.encoded_len() > MAX_RPC {
        return Err(limit("Flight message exceeds encoded RPC ceiling"));
    }
    scan_frame(&data.app_metadata)?;
    let frame: Frame = strict_decode(data.app_metadata.clone(), MAX_RPC)?;
    let mut control = frame.clone();
    if let Some(Body::Native(fragment)) = &mut control.body {
        fragment.segments.clear();
    }
    if control.encoded_len() > MAX_CONTROL {
        return Err(limit("frame control envelope exceeds its byte limit"));
    }
    Ok(frame)
}

#[derive(Clone)]
pub(crate) struct NativeExpected {
    pub(crate) messages: [(u64, Bytes); 2],
    pub(crate) ordered_sha256: Bytes,
}

pub(crate) enum NativeStep {
    Ack,
    Payload {
        message: usize,
        segments: Vec<Bytes>,
        message_complete: bool,
    },
    Complete,
}

pub(crate) struct NativeStream {
    binding: Binding,
    mode: Mode,
    expected: NativeExpected,
    next_sequence: u64,
    next_message: usize,
    offset: u64,
    payload_bytes: u64,
    ordered_hash: Sha256,
    message_hash: Sha256,
    complete: bool,
    failed: bool,
}

impl NativeStream {
    pub(crate) fn new(binding: Binding, mode: Mode, expected: NativeExpected) -> Result<Self> {
        validate_binding(&binding, &binding)?;
        if !matches!(mode, Mode::NativeFragments | Mode::NativeAssembled) {
            return Err(mismatch("native stream requires an explicit native mode"));
        }
        let total = expected
            .messages
            .iter()
            .try_fold(0_u64, |sum, (len, sha)| {
                if *len == 0 || sha.len() != 32 {
                    return None;
                }
                sum.checked_add(*len)
            })
            .ok_or_else(|| invalid("invalid frozen native message authority"))?;
        if total > MAX_PAYLOAD || expected.ordered_sha256.len() != 32 {
            return Err(limit("invalid frozen native payload authority"));
        }
        Ok(Self {
            binding,
            mode,
            expected,
            next_sequence: 0,
            next_message: 0,
            offset: 0,
            payload_bytes: 0,
            ordered_hash: Sha256::new(),
            message_hash: Sha256::new(),
            complete: false,
            failed: false,
        })
    }

    fn expected_bytes(&self) -> u64 {
        self.expected.messages.iter().map(|(bytes, _)| bytes).sum()
    }

    fn ack(&self, ack: &Ack) -> Result<()> {
        if ack.profile.as_ref() != Some(&profile())
            || ack.mode != self.mode as i32
            || ack.expected_messages != 2
            || ack.expected_payload_bytes != self.expected_bytes()
            || ack.expected_payload_sha256 != self.expected.ordered_sha256
        {
            return Err(mismatch("ACK differs from the frozen native authority"));
        }
        Ok(())
    }

    pub(crate) fn accept(&mut self, data: &FlightData) -> Result<NativeStep> {
        if self.failed {
            return Err(invalid("failed native attempt cannot resume"));
        }
        let result = self.accept_inner(data);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn accept_inner(&mut self, data: &FlightData) -> Result<NativeStep> {
        if self.complete || self.next_sequence >= MAX_FRAMES {
            return Err(invalid("frame after completion or frame limit"));
        }
        if !data.data_header.is_empty() || !data.data_body.is_empty() {
            return Err(invalid(
                "native payload belongs only in application metadata",
            ));
        }
        let frame = decode_frame(data)?;
        validate_binding(
            frame
                .binding
                .as_ref()
                .ok_or_else(|| invalid("missing frame binding"))?,
            &self.binding,
        )?;
        if frame.sequence != self.next_sequence {
            return Err(Error {
                code: "SLX_SEQUENCE",
                detail: "duplicate, skipped or reordered frame",
            });
        }
        let step = match frame
            .body
            .ok_or_else(|| invalid("missing frame alternative"))?
        {
            Body::Ack(ack) if self.next_sequence == 0 => {
                self.ack(&ack)?;
                NativeStep::Ack
            }
            Body::Native(fragment) if self.next_sequence > 0 && self.next_message < 2 => {
                let index = self.next_message;
                let (expected_len, expected_sha) = &self.expected.messages[index];
                if fragment.message_index as usize != index
                    || fragment.message_offset != self.offset
                    || fragment.message_bytes != *expected_len
                {
                    return Err(mismatch("native message coordinates differ"));
                }
                let bytes = fragment.segments.iter().map(Bytes::len).sum::<usize>() as u64;
                let end = self
                    .offset
                    .checked_add(bytes)
                    .filter(|end| *end <= *expected_len)
                    .ok_or_else(|| invalid("native fragment exceeds its message"))?;
                if self.offset == 0 {
                    self.ordered_hash.update(expected_len.to_le_bytes());
                }
                for bytes in &fragment.segments {
                    self.ordered_hash.update(bytes);
                    self.message_hash.update(bytes);
                }
                self.offset = end;
                self.payload_bytes += bytes;
                let message_complete = end == *expected_len;
                if message_complete {
                    if &self.message_hash.clone().finalize()[..] != expected_sha.as_ref() {
                        return Err(mismatch(
                            "native message hash differs from authorized bytes",
                        ));
                    }
                    self.message_hash = Sha256::new();
                    self.next_message += 1;
                    self.offset = 0;
                }
                NativeStep::Payload {
                    message: index,
                    segments: fragment.segments,
                    message_complete,
                }
            }
            Body::Complete(complete) if self.next_message == 2 => {
                self.completion(&complete)?;
                self.complete = true;
                NativeStep::Complete
            }
            _ => return Err(invalid("unexpected native frame alternative or order")),
        };
        self.next_sequence += 1;
        Ok(step)
    }

    fn completion(&self, complete: &Complete) -> Result<()> {
        if complete.frames != self.next_sequence + 1
            || complete.messages != 2
            || complete.rows != self.binding.rows
            || complete.payload_bytes != self.payload_bytes
            || self.payload_bytes != self.expected_bytes()
            || complete.ordered_payload_sha256 != self.expected.ordered_sha256
            || &self.ordered_hash.clone().finalize()[..] != self.expected.ordered_sha256.as_ref()
        {
            return Err(mismatch("completion totals or ordered digest differ"));
        }
        Ok(())
    }

    pub(crate) fn finish(self, rpc_succeeded: bool) -> Result<()> {
        if self.failed || !self.complete || !rpc_succeeded {
            return Err(Error {
                code: "SLX_INCOMPLETE",
                detail: "native attempt requires completion, clean EOF and successful RPC status",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "native_exchange_protocol_tests.rs"]
mod tests;
