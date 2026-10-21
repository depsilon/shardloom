// SPDX-License-Identifier: Apache-2.0
//! Real provider payloads and private receiving owners for the finite experiment.

use super::{arrow, attempts::Attempt, queue::Queue, *};
use crate::{
    resident_memory_source::ResidentMemorySource,
    resident_memory_source::exchange_bridge::{
        self as bridge, ArrowEncoding, Fixture, NativeEncoding,
    },
    resident_session::ResidentVortexSession,
};
use serde::Serialize;
use serde_json::Value;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use std::{sync::Arc, time::Instant};
use wire::{ArrowPosition, NativeFragment};

pub(crate) fn native<T>(result: shardloom_core::Result<T>) -> Result<T> {
    result.map_err(|_| Error {
        code: "SLX_NATIVE",
        detail: "native source, provider or consumer refused the operation",
    })
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct SendReport {
    pub(crate) frames: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) application_wire_bytes: u64,
    pub(crate) application_envelope_copy_bytes: u64,
    pub(crate) complete_message_assembly_bytes: u64,
    pub(crate) provider_part_lengths: Vec<Vec<usize>>,
    pub(crate) encoding_seconds: f64,
    pub(crate) native_execution: Option<Value>,
}

pub(crate) struct Sending {
    binding: Binding,
    mode: Mode,
    state: SendState,
    payload_sha256: Bytes,
    payload_bytes: u64,
    message_count: u32,
    pub(crate) report: SendReport,
    finished: bool,
}

enum SendState {
    Native(NativeSending),
    Arrow {
        encoder: ArrowEncoding,
        expected: arrow::Expected,
    },
    Sql(ArrowEncoding),
}

struct NativeSending {
    messages: [std::vec::IntoIter<Bytes>; 2],
    lengths: [u64; 2],
    message: usize,
    offset: u64,
    part: Option<Bytes>,
    complete: bool,
}

fn verify_native(encoding: &NativeEncoding, expected: &NativeExpected) -> Result<()> {
    let mut ordered = Sha256::new();
    for (parts, (bytes, sha)) in encoding.messages.iter().zip(&expected.messages) {
        let actual = parts
            .iter()
            .try_fold(0_u64, |length, part| length.checked_add(part.len() as u64))
            .ok_or_else(|| limit("native provider length overflow"))?;
        if actual != *bytes {
            return Err(mismatch("native provider message length differs"));
        }
        ordered.update(bytes.to_le_bytes());
        let mut message = Sha256::new();
        for part in parts {
            message.update(part);
            ordered.update(part);
        }
        if &message.finalize()[..] != sha.as_ref() {
            return Err(mismatch(
                "native provider message differs from frozen authority",
            ));
        }
    }
    if &ordered.finalize()[..] != expected.ordered_sha256.as_ref() {
        return Err(mismatch(
            "native provider ordered payload differs from frozen authority",
        ));
    }
    Ok(())
}

impl Sending {
    pub(crate) fn sql(
        source: &ResidentMemorySource,
        fixture: &Fixture,
        binding: Binding,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let started = Instant::now();
        let (projected, execution) = native(fixture.execute_sql_source(source, cancellation))?;
        let encoder = native(bridge::encode_sql(&projected, cancellation))?;
        let report = SendReport {
            encoding_seconds: started.elapsed().as_secs_f64(),
            native_execution: Some(execution),
            ..SendReport::default()
        };
        Ok(Self {
            binding,
            mode: Mode::ArrowNone,
            state: SendState::Sql(encoder),
            payload_sha256: Bytes::new(),
            payload_bytes: 0,
            message_count: 0,
            report,
            finished: false,
        })
    }

    pub(crate) fn new(
        source: &ResidentMemorySource,
        fixture: &Fixture,
        binding: Binding,
        mode: Mode,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        validate_binding(&binding, &binding)?;
        let started = Instant::now();
        let mut report = SendReport::default();
        let (state, payload_bytes, payload_sha256, message_count) = match mode {
            Mode::NativeFragments | Mode::NativeAssembled => {
                let encoded = native(bridge::encode_native(source, mode, cancellation))?;
                verify_native(&encoded, &fixture.authority.native)?;
                report.complete_message_assembly_bytes = encoded.assembly_bytes;
                report.provider_part_lengths = encoded
                    .messages
                    .iter()
                    .map(|parts| parts.iter().map(Bytes::len).collect())
                    .collect();
                let lengths = fixture
                    .authority
                    .native
                    .messages
                    .each_ref()
                    .map(|(bytes, _)| *bytes);
                (
                    SendState::Native(NativeSending {
                        messages: encoded.messages.map(Vec::into_iter),
                        lengths,
                        message: 0,
                        offset: 0,
                        part: None,
                        complete: false,
                    }),
                    lengths.iter().sum(),
                    fixture.authority.native.ordered_sha256.clone(),
                    2,
                )
            }
            Mode::ArrowNone | Mode::ArrowLz4 | Mode::ArrowZstd => {
                let encoder = native(bridge::encode_arrow(source, fixture, mode, cancellation))?;
                let expected = fixture.authority.arrow_expected(mode)?;
                let payload = expected
                    .frames
                    .iter()
                    .map(|frame| (frame.header_bytes + frame.body_bytes) as u64)
                    .sum();
                let messages = u32::try_from(expected.frames.len())
                    .map_err(|_| limit("Arrow frame count overflow"))?;
                let sha = expected.ordered_sha256.clone();
                (
                    SendState::Arrow { encoder, expected },
                    payload,
                    sha,
                    messages,
                )
            }
            Mode::Unspecified => return Err(mismatch("an explicit transport mode is required")),
        };
        report.encoding_seconds = started.elapsed().as_secs_f64();
        report.payload_bytes = payload_bytes;
        Ok(Self {
            binding,
            mode,
            state,
            payload_sha256,
            payload_bytes,
            message_count,
            report,
            finished: false,
        })
    }

    fn ack(&self) -> Ack {
        Ack {
            profile: Some(profile()),
            mode: self.mode as i32,
            expected_payload_sha256: self.payload_sha256.clone(),
            expected_payload_bytes: self.payload_bytes,
            expected_messages: self.message_count,
        }
    }

    fn complete(&self, frames: u64) -> Complete {
        Complete {
            frames,
            messages: self.message_count,
            rows: self.binding.rows,
            payload_bytes: self.payload_bytes,
            ordered_payload_sha256: self.payload_sha256.clone(),
        }
    }

    pub(crate) async fn next(
        &mut self,
        queue: &Queue,
        attempt: &Arc<Attempt>,
    ) -> Result<Option<FlightData>> {
        attempt.check()?;
        if self.finished {
            return Ok(None);
        }
        if matches!(self.state, SendState::Sql(_)) {
            return self.next_sql(queue, attempt).await;
        }
        let started = Instant::now();
        let ack = self.ack();
        let completion = self.complete(self.report.frames + 1);
        let sequence = self.report.frames;
        let next = match &mut self.state {
            SendState::Native(state) => state
                .next(sequence, ack, completion)?
                .map(|body| (FlightData::default(), body)),
            SendState::Arrow { encoder, expected } => {
                if let Some(frame) = native(encoder.next().await)? {
                    let index =
                        usize::try_from(sequence).map_err(|_| limit("Arrow sequence overflow"))?;
                    let identity = expected
                        .frames
                        .get(index)
                        .ok_or_else(|| mismatch("extra provider Arrow frame"))?;
                    arrow::validate_data(&frame, identity, self.mode)?;
                    if !frame.app_metadata.is_empty() || frame.flight_descriptor.is_some() {
                        return Err(invalid(
                            "provider Arrow frame contains unregistered metadata",
                        ));
                    }
                    let position = ArrowPosition {
                        ack: (index == 0).then_some(ack),
                        complete: (index + 1 == expected.frames.len()).then_some(completion),
                    };
                    Some((frame, Body::Arrow(position)))
                } else {
                    if sequence != expected.frames.len() as u64 {
                        return Err(mismatch("truncated provider Arrow stream"));
                    }
                    None
                }
            }
            SendState::Sql(_) => return Err(invalid("SQL output cannot select a native envelope")),
        };
        self.report.encoding_seconds += started.elapsed().as_secs_f64();
        let Some((mut data, body)) = next else {
            self.finished = true;
            return Ok(None);
        };
        if sequence >= MAX_FRAMES {
            return Err(limit("provider frame count exceeds the fixed limit"));
        }
        let frame = Frame {
            binding: Some(self.binding.clone()),
            sequence,
            body: Some(body),
        };
        let metadata_bytes = frame.encoded_len();
        let encoded_bytes = data.encoded_len()
            + 1
            + prost::encoding::encoded_len_varint(metadata_bytes as u64)
            + metadata_bytes;
        let mut encode_seconds = 0.0;
        let data = queue
            .frame(attempt, encoded_bytes, || {
                let started = Instant::now();
                data.app_metadata = Bytes::from(frame.encode_to_vec());
                encode_seconds = started.elapsed().as_secs_f64();
                Ok(data)
            })
            .await?;
        self.report.encoding_seconds += encode_seconds;
        self.report.frames += 1;
        self.report.application_wire_bytes += (encoded_bytes + 5) as u64;
        self.report.application_envelope_copy_bytes += metadata_bytes as u64;
        Ok(Some(data))
    }

    async fn next_sql(
        &mut self,
        queue: &Queue,
        attempt: &Arc<Attempt>,
    ) -> Result<Option<FlightData>> {
        let SendState::Sql(encoder) = &mut self.state else {
            return Err(invalid("missing SQL Arrow encoder"));
        };
        let started = Instant::now();
        let next = native(encoder.next().await)?;
        self.report.encoding_seconds += started.elapsed().as_secs_f64();
        let Some(frame) = next else {
            self.finished = true;
            return Ok(None);
        };
        if !frame.app_metadata.is_empty()
            || frame.flight_descriptor.is_some()
            || self.report.frames >= MAX_FRAMES
        {
            return Err(invalid(
                "SQL results require bounded standard Arrow framing",
            ));
        }
        arrow::inspect(&frame, Mode::ArrowNone)?;
        self.report.payload_bytes = self
            .report
            .payload_bytes
            .checked_add((frame.data_header.len() + frame.data_body.len()) as u64)
            .filter(|total| *total <= MAX_PAYLOAD)
            .ok_or_else(|| limit("SQL Arrow payload exceeds fixed limit"))?;
        let bytes = frame.encoded_len();
        let frame = queue.frame(attempt, bytes, || Ok(frame)).await?;
        self.report.frames += 1;
        self.report.application_wire_bytes += (bytes + 5) as u64;
        Ok(Some(frame))
    }
}

impl NativeSending {
    fn next(&mut self, sequence: u64, ack: Ack, completion: Complete) -> Result<Option<Body>> {
        if sequence == 0 {
            return Ok(Some(Body::Ack(ack)));
        }
        if self.message == 2 {
            if self.complete {
                return Ok(None);
            }
            self.complete = true;
            return Ok(Some(Body::Complete(completion)));
        }
        let offset = self.offset;
        let message_index =
            u32::try_from(self.message).map_err(|_| invalid("invalid native message index"))?;
        let message_bytes = self.lengths[self.message];
        let mut segments = Vec::with_capacity(MAX_SEGMENTS);
        let mut available = MAX_FRAGMENT;
        while self.offset < message_bytes && available > 0 && segments.len() < MAX_SEGMENTS {
            let part = self
                .part
                .take()
                .or_else(|| self.messages[self.message].next())
                .ok_or_else(|| mismatch("native provider parts ended early"))?;
            if part.is_empty() {
                continue;
            }
            let len = available.min(part.len());
            segments.push(part.slice(..len));
            if len < part.len() {
                self.part = Some(part.slice(len..));
            }
            self.offset += len as u64;
            available -= len;
        }
        if segments.is_empty() || self.offset > message_bytes {
            return Err(invalid("native provider fragment coordinates differ"));
        }
        if self.offset == message_bytes {
            self.message += 1;
            self.offset = 0;
        }
        Ok(Some(Body::Native(NativeFragment {
            message_index,
            message_offset: offset,
            message_bytes,
            segments,
        })))
    }
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct ReceiveReport {
    pub(crate) frames: u64,
    pub(crate) application_wire_bytes: u64,
    pub(crate) private_native_assembly_bytes: u64,
    pub(crate) complete_native_messages: u64,
    pub(crate) encoding_preservation: Value,
    pub(crate) intake_copy_report: String,
}

pub(crate) struct Receiving {
    session: ResidentVortexSession,
    mode: Mode,
    state: Option<ReceiveState>,
    report: ReceiveReport,
}

enum ReceiveState {
    Native {
        messages: [Vec<u8>; 2],
        validator: NativeStream,
        _credit: MemoryLease,
    },
    Arrow {
        frames: Vec<FlightData>,
        validator: arrow::ArrowStream,
        _credit: MemoryLease,
    },
}

impl Receiving {
    pub(crate) fn new(
        session: &ResidentVortexSession,
        fixture: &Fixture,
        binding: Binding,
        mode: Mode,
    ) -> Result<Self> {
        let state = match mode {
            Mode::NativeFragments | Mode::NativeAssembled => {
                let expected = fixture.authority.native.clone();
                let validator = NativeStream::new(binding, mode, expected.clone())?;
                let bytes: u64 = expected.messages.iter().map(|part| part.0).sum();
                let credit = native(session.reserve_input_scratch(bytes))?;
                let mut messages = [Vec::new(), Vec::new()];
                for (message, (bytes, _)) in messages.iter_mut().zip(&expected.messages) {
                    message
                        .try_reserve_exact(
                            usize::try_from(*bytes)
                                .map_err(|_| limit("native receive capacity overflow"))?,
                        )
                        .map_err(|_| limit("native receiving allocation denied"))?;
                }
                ReceiveState::Native {
                    messages,
                    validator,
                    _credit: credit,
                }
            }
            Mode::ArrowNone | Mode::ArrowLz4 | Mode::ArrowZstd => {
                let expected = fixture.authority.arrow_expected(mode)?;
                let bytes: u64 = expected
                    .frames
                    .iter()
                    .map(|frame| {
                        (frame.header_bytes
                            + frame.body_bytes
                            + MAX_CONTROL
                            + std::mem::size_of::<FlightData>()) as u64
                    })
                    .sum();
                let validator = arrow::ArrowStream::new(binding, mode, expected.clone())?;
                let credit = native(session.reserve_input_scratch(bytes))?;
                ReceiveState::Arrow {
                    frames: Vec::with_capacity(expected.frames.len()),
                    validator,
                    _credit: credit,
                }
            }
            Mode::Unspecified => {
                return Err(mismatch("receiving requires an explicit transport mode"));
            }
        };
        Ok(Self {
            session: session.clone(),
            mode,
            state: Some(state),
            report: ReceiveReport::default(),
        })
    }

    pub(crate) fn accept(&mut self, data: FlightData) -> Result<()> {
        let result = self.accept_inner(data);
        if result.is_err() {
            self.state = None;
        }
        result
    }

    fn accept_inner(&mut self, data: FlightData) -> Result<()> {
        let state = self
            .state
            .as_mut()
            .ok_or_else(|| invalid("failed receiving attempt cannot resume"))?;
        let bytes = data.encoded_len();
        match state {
            ReceiveState::Native {
                messages,
                validator,
                ..
            } => {
                if let NativeStep::Payload {
                    message,
                    segments,
                    message_complete,
                } = validator.accept(&data)?
                {
                    for segment in segments {
                        let buffer = &mut messages[message];
                        if segment.len() > buffer.capacity() - buffer.len() {
                            return Err(limit("private native receive capacity exhausted"));
                        }
                        buffer.extend_from_slice(&segment);
                        self.report.private_native_assembly_bytes += segment.len() as u64;
                    }
                    self.report.complete_native_messages += u64::from(message_complete);
                }
            }
            ReceiveState::Arrow {
                frames, validator, ..
            } => {
                validator.accept(&data)?;
                frames.push(data);
            }
        }
        self.report.frames += 1;
        self.report.application_wire_bytes += (bytes + 5) as u64;
        Ok(())
    }

    pub(crate) async fn finish(
        mut self,
        rpc_succeeded: bool,
        fixture: &Fixture,
        cancellation: &CancellationToken,
    ) -> Result<(ResidentMemorySource, ReceiveReport)> {
        let state = self
            .state
            .take()
            .ok_or_else(|| invalid("failed receiving attempt cannot publish"))?;
        let source = match state {
            ReceiveState::Native {
                messages,
                validator,
                _credit: credit,
            } => {
                validator.finish(rpc_succeeded)?;
                let source = native(ResidentMemorySource::from_vortex_ipc(
                    &self.session,
                    &messages[0],
                    &messages[1],
                    cancellation,
                ))?;
                drop((messages, credit));
                source
            }
            ReceiveState::Arrow {
                frames,
                validator,
                _credit: credit,
            } => {
                validator.finish(rpc_succeeded)?;
                let source =
                    native(bridge::decode_arrow(&self.session, frames, cancellation).await)?;
                drop(credit);
                source
            }
        };
        self.report.encoding_preservation = native(fixture.check_source(&source, self.mode))?;
        self.report.intake_copy_report = format!("{:?}", source.intake_transfer_report());
        Ok((source, self.report))
    }
}

#[cfg(test)]
#[path = "native_exchange_payload_tests.rs"]
mod tests;
