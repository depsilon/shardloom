// SPDX-License-Identifier: Apache-2.0

use super::decoder_memory::{AdmittedBuffer, DecoderMemory, DecoderMemoryReservation, invalid};
use crate::compression::{CompressionCodec, DecompressionContext};
use arrow_buffer::Buffer;
use arrow_schema::ArrowError;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct State {
    current: usize,
    peak: usize,
    limit: usize,
    cancelled: bool,
    requests: Vec<usize>,
}

#[derive(Debug, Clone)]
pub(super) struct Memory(Arc<Mutex<State>>);

impl Memory {
    pub(super) fn new(limit: usize) -> Self {
        Self(Arc::new(Mutex::new(State {
            limit,
            ..State::default()
        })))
    }

    pub(super) fn current(&self) -> usize {
        self.0.lock().unwrap().current
    }

    pub(super) fn cancel(&self) {
        self.0.lock().unwrap().cancelled = true;
    }

    pub(super) fn peak(&self) -> usize {
        self.0.lock().unwrap().peak
    }

    pub(super) fn set_limit(&self, limit: usize) {
        self.0.lock().unwrap().limit = limit;
    }
}

impl DecoderMemory for Memory {
    fn reserve(&self, bytes: usize) -> Result<Box<dyn DecoderMemoryReservation>, ArrowError> {
        let mut state = self.0.lock().unwrap();
        state.requests.push(bytes);
        if state.cancelled {
            return Err(invalid("cancelled test decoder"));
        }
        let next = state
            .current
            .checked_add(bytes)
            .ok_or_else(|| invalid("test overflow"))?;
        if next > state.limit {
            return Err(invalid("denied test decoder"));
        }
        state.current = next;
        state.peak = state.peak.max(next);
        Ok(Box::new(Credit {
            memory: self.clone(),
            bytes,
        }))
    }
}

#[derive(Debug)]
struct Credit {
    memory: Memory,
    bytes: usize,
}

impl DecoderMemoryReservation for Credit {}

impl Drop for Credit {
    fn drop(&mut self) {
        self.memory.0.lock().unwrap().current -= self.bytes;
    }
}

fn decode(codec: CompressionCodec, input: Vec<u8>, memory: &Memory) -> Result<Buffer, ArrowError> {
    let mut context = DecompressionContext::new();
    context.memory = Some(Arc::new(memory.clone()));
    codec.decompress_to_buffer(&Buffer::from_vec(input), &mut context)
}

#[test]
fn owned_buffer_checks_capacity_before_allocation_and_retains_slices() {
    let memory = Memory::new(128);
    assert!(AdmittedBuffer::new(129, &memory).is_err());
    assert_eq!(memory.current(), 0);
    assert!(AdmittedBuffer::new(usize::MAX, &memory).is_err());
    let mut output = AdmittedBuffer::new(65, &memory).unwrap();
    output.as_mut_slice()[64] = 91;
    let buffer = output.finish();
    assert_eq!(buffer.as_ptr().addr() % 64, 0);
    let slice = buffer.slice(64);
    drop(buffer);
    assert_eq!(slice.as_slice(), &[91]);
    assert_eq!(memory.current(), 128);
    drop(slice);
    assert_eq!(memory.current(), 0);
}

#[test]
fn controlled_prefixes_and_missing_zstd_workspace_are_explicit() {
    let memory = Memory::new(1024);
    assert!(decode(CompressionCodec::Zstd(3), vec![0; 7], &memory).is_err());
    assert!(
        decode(
            CompressionCodec::Zstd(3),
            (-2_i64).to_le_bytes().to_vec(),
            &memory
        )
        .is_err()
    );
    assert!(
        decode(
            CompressionCodec::Zstd(3),
            0_i64.to_le_bytes().to_vec(),
            &memory
        )
        .unwrap()
        .is_empty()
    );
    let mut raw = (-1_i64).to_le_bytes().to_vec();
    raw.extend_from_slice(b"raw");
    assert_eq!(
        decode(CompressionCodec::Zstd(3), raw, &memory)
            .unwrap()
            .as_slice(),
        b"raw"
    );
    let mut compressed = 3_i64.to_le_bytes().to_vec();
    compressed.extend_from_slice(b"frame");
    let error = decode(CompressionCodec::Zstd(3), compressed, &memory).unwrap_err();
    assert!(error.to_string().contains("workspace is unavailable"));
    assert_eq!(memory.current(), 0);
}

#[cfg(feature = "lz4")]
fn frame(data: &[u8], info: lz4_flex::frame::FrameInfo) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

#[cfg(feature = "lz4")]
fn encoded(frame: &[u8], length: usize) -> Vec<u8> {
    let mut output = (length as i64).to_le_bytes().to_vec();
    output.extend_from_slice(frame);
    output
}

#[cfg(feature = "lz4")]
#[test]
fn owned_lz4_roundtrips_declared_block_capacities_and_linked_history() {
    use lz4_flex::frame::{BlockMode, BlockSize, FrameInfo};
    let data: Vec<u8> = (0..350_123).map(|i| (i * 31 % 251) as u8).collect();
    for (block_size, maximum) in [
        (BlockSize::Max64KB, 64 * 1024),
        (BlockSize::Max256KB, 256 * 1024),
        (BlockSize::Max1MB, 1024 * 1024),
        (BlockSize::Max4MB, 4 * 1024 * 1024),
    ] {
        for block_mode in [BlockMode::Independent, BlockMode::Linked] {
            let input = frame(
                &data,
                FrameInfo::new()
                    .block_size(block_size)
                    .block_mode(block_mode)
                    .block_checksums(true)
                    .content_checksum(true)
                    .content_size(Some(data.len() as u64)),
            );
            let memory = Memory::new(32 * 1024 * 1024);
            let output = decode(
                CompressionCodec::Lz4Frame,
                encoded(&input, data.len()),
                &memory,
            )
            .unwrap();
            assert_eq!(output.as_slice(), data);
            let capacity = (data.len() + 63) & !63;
            let workspace = if block_mode == BlockMode::Linked {
                maximum * 3 + 64 * 1024
            } else {
                maximum * 2
            };
            assert_eq!(memory.0.lock().unwrap().peak, capacity + workspace);
            assert_eq!(memory.current(), capacity);
            let retained = output.clone();
            drop(output);
            assert_eq!(retained.as_slice(), data);
            drop(retained);
            assert_eq!(memory.current(), 0);
        }
    }
}

#[cfg(feature = "lz4")]
#[test]
fn owned_lz4_denial_cancel_and_invalid_lengths_refund_all_credits() {
    use lz4_flex::frame::{BlockSize, FrameInfo};
    let data = vec![7; 1000];
    let input = frame(&data, FrameInfo::new().block_size(BlockSize::Max64KB));
    let memory = Memory::new(1024 + 128 * 1024 - 1);
    let error = decode(
        CompressionCodec::Lz4Frame,
        encoded(&input, data.len()),
        &memory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("denied"));
    assert_eq!(memory.current(), 0);
    memory.0.lock().unwrap().limit = 1024 * 1024;
    for declared in [0, 1, 999, 1001, 2000] {
        assert!(
            decode(
                CompressionCodec::Lz4Frame,
                encoded(&input, declared),
                &memory
            )
            .is_err()
        );
        assert_eq!(memory.current(), 0);
    }
    memory.0.lock().unwrap().cancelled = true;
    assert!(
        decode(
            CompressionCodec::Lz4Frame,
            encoded(&input, data.len()),
            &memory
        )
        .unwrap_err()
        .to_string()
        .contains("cancelled")
    );
    assert_eq!(memory.current(), 0);
}

#[cfg(feature = "lz4")]
#[test]
fn owned_lz4_checks_every_truncation_trailing_bytes_and_checksums() {
    use lz4_flex::frame::{BlockSize, FrameInfo};
    let data = b"complete values including frame end";
    let input = frame(
        data,
        FrameInfo::new()
            .block_size(BlockSize::Max64KB)
            .content_size(Some(data.len() as u64))
            .block_checksums(true)
            .content_checksum(true),
    );
    let memory = Memory::new(1024 * 1024);
    for end in 0..input.len() {
        assert!(
            decode(
                CompressionCodec::Lz4Frame,
                encoded(&input[..end], data.len()),
                &memory
            )
            .is_err(),
            "truncation {end}"
        );
        assert_eq!(memory.current(), 0);
    }
    let mut trailing = input.clone();
    trailing.extend_from_slice(&[0; 4]);
    assert!(
        decode(
            CompressionCodec::Lz4Frame,
            encoded(&trailing, data.len()),
            &memory
        )
        .is_err()
    );
    let mut concatenated = input.clone();
    concatenated.extend_from_slice(&input);
    assert!(
        decode(
            CompressionCodec::Lz4Frame,
            encoded(&concatenated, data.len()),
            &memory
        )
        .is_err()
    );
    for index in [14, input.len() - 1] {
        let mut damaged = input.clone();
        damaged[index] ^= 1;
        assert!(
            decode(
                CompressionCodec::Lz4Frame,
                encoded(&damaged, data.len()),
                &memory
            )
            .is_err()
        );
        assert_eq!(memory.current(), 0);
    }
}

#[cfg(feature = "lz4")]
#[test]
fn owned_lz4_legacy_complete_boundary_and_empty_frame() {
    use lz4_flex::frame::FrameInfo;
    let data = b"legacy values";
    // Legacy has just magic and length-prefixed compressed blocks. EOF is legal
    // at a complete block boundary; a partial block prefix/body is not.
    let compressed = lz4_flex::block::compress(data);
    let mut input = 0x184c_2102_u32.to_le_bytes().to_vec();
    input.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    input.extend_from_slice(&compressed);
    let memory = Memory::new(17 * 1024 * 1024);
    let output = decode(
        CompressionCodec::Lz4Frame,
        encoded(&input, data.len()),
        &memory,
    )
    .unwrap();
    assert_eq!(output.as_slice(), data);
    drop(output);
    assert_eq!(memory.current(), 0);
    input.push(0);
    assert!(
        decode(
            CompressionCodec::Lz4Frame,
            encoded(&input, data.len()),
            &memory
        )
        .is_err()
    );
    let empty = frame(&[], FrameInfo::new());
    let output = decode(CompressionCodec::Lz4Frame, encoded(&empty, 0), &memory).unwrap();
    assert!(output.is_empty());
    drop(output);
    assert_eq!(memory.current(), 0);
}
