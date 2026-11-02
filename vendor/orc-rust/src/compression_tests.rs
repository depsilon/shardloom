// SPDX-License-Identifier: Apache-2.0
//! Literal framing and retained-input controls for the pinned ORC decoder.

use super::*;
use crate::reader::memory::tests::{Counters, Memory};
use std::io::Read;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

struct Input {
    bytes: Vec<u8>,
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for Input {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Input {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[test]
fn retains_the_original_input_owner_without_copying() {
    let dropped = Arc::new(AtomicBool::new(false));
    let input = Bytes::from_owner(Input {
        bytes: b"owned bytes".to_vec(),
        dropped: Arc::clone(&dropped),
    });
    let pointer = input.as_ptr();
    let mut reader = Decompressor::new(input, None, Vec::new());
    assert_eq!(reader.decompressor.stream.as_ptr(), pointer);
    assert!(!dropped.load(Ordering::SeqCst));
    let mut prefix = [0; 3];
    reader.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, b"own");
    assert!(!dropped.load(Ordering::SeqCst));
    drop(reader);
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn rejects_truncated_compression_frames_without_panicking() {
    let compression = Compression::from_proto(CompressionKind::Zlib, Some(64));
    for bytes in [vec![1], vec![1, 0], vec![7, 0, 0, b'a'], vec![6, 0, 0, 0]] {
        let result = std::panic::catch_unwind(|| {
            let mut reader = Decompressor::new(Bytes::from(bytes), compression, Vec::new());
            reader.read_to_end(&mut Vec::new())
        });
        assert!(result.is_ok(), "malformed framing must return an error");
        assert!(result.unwrap().is_err());
    }
}

#[test]
fn empty_original_blocks_do_not_hide_later_values() {
    let bytes = Bytes::from_static(&[1, 0, 0, 1, 0, 0, 7, 0, 0, b'a', b'b', b'c']);
    let compression = Compression::from_proto(CompressionKind::Zlib, Some(64));
    let mut reader = Decompressor::new(bytes, compression, Vec::new());
    let mut values = Vec::new();
    reader.read_to_end(&mut values).unwrap();
    assert_eq!(values, b"abc");
}

fn owned_reader(
    kind: CompressionType,
    limit: usize,
) -> (Decompressor, Arc<Mutex<Counters>>, Vec<u8>) {
    let input = (0..20_000).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let mut compressor = Compressor::new(Some(kind), Some(32_768)).unwrap();
    let encoded = compressor.compress(&input).unwrap().into_owned();
    assert!(matches!(
        CompressionHeader::decode(encoded[..3].try_into().unwrap()),
        CompressionHeader::Compressed(_)
    ));
    let counters = Arc::new(Mutex::new(Counters::default()));
    let memory = Arc::new(Memory {
        counters: Arc::clone(&counters),
        limit,
    });
    let compression = Compression::from_proto(kind.to_proto(), Some(32_768));
    (
        Decompressor::new_with_memory(Bytes::from(encoded), compression, memory),
        counters,
        input,
    )
}

#[test]
fn owns_actual_decompression_buffers_until_consumed_or_dropped() {
    for kind in [
        CompressionType::Zlib,
        CompressionType::Snappy,
        CompressionType::Lz4,
    ] {
        let (mut reader, counters, expected) = owned_reader(kind, 100_000);
        let mut first = [0; 1];
        reader.read_exact(&mut first).unwrap();
        assert_eq!(first[0], expected[0]);
        assert_eq!(
            counters.lock().unwrap().live,
            reader.decompressor.scratch.bytes.capacity()
        );
        assert!(counters.lock().unwrap().live >= expected.len());
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, expected[1..]);
        assert_eq!(counters.lock().unwrap().live, 0);
        drop(reader);
        assert_eq!(counters.lock().unwrap().live, 0);

        let (mut reader, counters, _) = owned_reader(kind, 100_000);
        reader.read_exact(&mut first).unwrap();
        assert!(counters.lock().unwrap().live > 0);
        drop(reader);
        assert_eq!(counters.lock().unwrap().live, 0);
    }
}

#[test]
fn denied_decompression_is_terminal_and_refunds_every_credit() {
    for kind in [
        CompressionType::Zlib,
        CompressionType::Snappy,
        CompressionType::Lz4,
    ] {
        let (mut reader, counters, _) = owned_reader(kind, 10_000);
        assert!(reader
            .read(&mut [0; 1])
            .unwrap_err()
            .to_string()
            .contains("test shared memory denied"));
        let requests = counters.lock().unwrap().requests;
        assert!(reader
            .read(&mut [0; 1])
            .unwrap_err()
            .to_string()
            .contains("previously failed"));
        assert_eq!(counters.lock().unwrap().requests, requests);
        assert!(counters.lock().unwrap().peak <= 10_000);
        drop(reader);
        assert_eq!(counters.lock().unwrap().live, 0);
    }
}

#[test]
fn growth_admits_replacement_overlap_and_shares_one_budget() {
    let (mut first, counters, _) = owned_reader(CompressionType::Zlib, 100_000);
    first.read_exact(&mut [0; 1]).unwrap();
    assert_eq!(counters.lock().unwrap().peak, 32_768);
    first.decompressor.scratch.grow(40_000, 40_000).unwrap();
    assert_eq!(counters.lock().unwrap().peak, 32_768 + 40_000);
    let input = vec![0; 20_000];
    let mut compressor = Compressor::new(Some(CompressionType::Snappy), Some(32_768)).unwrap();
    let encoded = compressor.compress(&input).unwrap().into_owned();
    let memory = Arc::new(Memory {
        counters: Arc::clone(&counters),
        limit: 40_000,
    });
    let compression = Compression::from_proto(CompressionKind::Snappy, Some(32_768));
    let mut second = Decompressor::new_with_memory(Bytes::from(encoded), compression, memory);
    assert!(second.read(&mut [0; 1]).is_err());
    assert_eq!(counters.lock().unwrap().live, 40_000);
    drop(first);
    drop(second);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn enforces_declared_expansion_bound_for_supported_codecs() {
    for kind in [
        CompressionType::Zlib,
        CompressionType::Snappy,
        CompressionType::Lz4,
    ] {
        let (mut reader, counters, _) = owned_reader(kind, 100_000);
        reader.decompressor.maximum = 32;
        assert!(reader.read(&mut [0; 1]).is_err());
        assert!(counters.lock().unwrap().peak <= 32);
        drop(reader);
        assert_eq!(counters.lock().unwrap().live, 0);
    }
}

#[test]
fn controlled_zstd_requires_an_owned_workspace_provider() {
    let (mut reader, counters, _) = owned_reader(CompressionType::Zstd, 100_000);
    assert!(reader
        .read(&mut [0; 1])
        .unwrap_err()
        .to_string()
        .contains("requires a caller-owned decoder workspace"));
    drop(reader);
    assert_eq!(counters.lock().unwrap().live, 0);
    // Actual owned Zstd decoding is covered by the native provider integration;
    // this mock only supplies memory credits and cannot claim that capability.
}

#[test]
fn deflate_rejects_trailing_or_truncated_payloads() {
    let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(b"complete deflate").unwrap();
    let valid = encoder.finish().unwrap();
    let mut trailing = valid.clone();
    trailing.push(0);
    for input in [&trailing[..], &valid[..valid.len() - 1]] {
        let mut scratch = DecoderBuffer::new(Vec::new(), None);
        assert!(Zlib.decompress_block(input, &mut scratch, 64).is_err());
    }
    let mut scratch = DecoderBuffer::new(Vec::new(), None);
    Zlib.decompress_block(&valid, &mut scratch, 64).unwrap();
    assert_eq!(scratch.bytes, b"complete deflate");
}

#[test]
fn controlled_lzo_declines_before_unbounded_codec_allocation() {
    let counters = Arc::new(Mutex::new(Counters::default()));
    let memory = Arc::new(Memory {
        counters: Arc::clone(&counters),
        limit: 1_000_000,
    });
    let compression = Compression::from_proto(CompressionKind::Lzo, Some(32_768));
    let mut reader =
        Decompressor::new_with_memory(Bytes::from_static(&[2, 0, 0, 0]), compression, memory);
    assert!(reader
        .read(&mut [0; 1])
        .unwrap_err()
        .to_string()
        .contains("no bounded allocation interface"));
    assert_eq!(counters.lock().unwrap().peak, 0);
}
