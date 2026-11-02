// SPDX-License-Identifier: Apache-2.0
//! Independent raw-Deflate fixtures, growth/history and complete-input controls.

use super::*;
use arrow_schema::ArrowError;
use std::{io::Write, sync::Mutex};

#[derive(Debug)]
struct Credit {
    bytes: Mutex<usize>,
    peak: Mutex<usize>,
    limit: usize,
}
impl Credit {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Mutex::new(0),
            peak: Mutex::new(0),
            limit,
        }
    }
}
impl BlockMemoryReservation for Credit {
    fn resize(&self, bytes: usize) -> Result<(), ArrowError> {
        if bytes > self.limit {
            return Err(ArrowError::MemoryError("block control limit".to_string()));
        }
        *self.bytes.lock().unwrap() = bytes;
        let mut peak = self.peak.lock().unwrap();
        *peak = (*peak).max(bytes);
        Ok(())
    }
}

fn encoded(bytes: &[u8]) -> Vec<u8> {
    let mut writer = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap()
}

#[test]
fn credited_deflate_preserves_history_across_repeated_growth() {
    let prefix: Vec<u8> = (0_u32..32768)
        .map(|n| ((n.wrapping_mul(19873) ^ (n >> 3)) % 251) as u8)
        .collect();
    let mut values = prefix.clone();
    values.extend_from_slice(&prefix);
    values.extend_from_slice(&"large recurring 東京 payload".as_bytes().repeat(25000));
    let compressed = encoded(&values);
    assert!(values.len() > compressed.len() * 8);
    let owner = Credit::new(4 << 20);
    let decoded = deflate(&compressed, compressed.capacity(), &owner).unwrap();
    assert_eq!(decoded, values);
    assert_eq!(
        *owner.bytes.lock().unwrap(),
        compressed.capacity() + decoded.capacity()
    );
    assert!(*owner.peak.lock().unwrap() > compressed.capacity() + decoded.capacity());
}

#[test]
fn credited_deflate_rejects_truncation_trailing_data_and_invalid_streams() {
    let compressed = encoded(b"complete Deflate payload");
    for end in 0..compressed.len() {
        let owner = Credit::new(4096);
        assert!(
            deflate(&compressed[..end], compressed.len(), &owner).is_err(),
            "end={end}"
        );
    }
    for suffix in [vec![0], encoded(b"another stream")] {
        let mut trailing = compressed.clone();
        trailing.extend(suffix);
        assert!(deflate(&trailing, trailing.len(), &Credit::new(4096)).is_err());
    }
    assert!(deflate(&[0xff, 0xff], 2, &Credit::new(4096)).is_err());
    // RFC 1951 final empty stored block: no output allocation is necessary.
    let empty = [1, 0, 0, 255, 255];
    let owner = Credit::new(0);
    assert!(deflate(&empty, 0, &owner).unwrap().is_empty());
    assert_eq!(*owner.peak.lock().unwrap(), 0);
}

#[test]
fn credited_deflate_denies_output_growth_without_private_heap_fallback() {
    let compressed = encoded(&vec![b'x'; 100_000]);
    let owner = Credit::new(compressed.capacity() + 128);
    assert!(
        deflate(&compressed, compressed.capacity(), &owner)
            .unwrap_err()
            .to_string()
            .contains("block control limit")
    );
    assert!(*owner.peak.lock().unwrap() <= owner.limit);
}
