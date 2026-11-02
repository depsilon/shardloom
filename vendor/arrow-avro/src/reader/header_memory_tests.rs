// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom incremental header and lifetime controls.

use super::*;
use crate::reader::{DecoderMemory, DecoderMemoryReservation};
use arrow_schema::ArrowError;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct Pool {
    used: AtomicUsize,
    limit: AtomicUsize,
    cancelled: AtomicBool,
}

#[derive(Debug)]
struct Factory(Arc<Pool>);
#[derive(Debug)]
struct Credit(Arc<Pool>, usize);
impl DecoderMemoryReservation for Credit {}
impl Drop for Credit {
    fn drop(&mut self) {
        self.0.used.fetch_sub(self.1, Ordering::SeqCst);
    }
}
impl DecoderMemory for Factory {
    fn reserve(&self, bytes: usize) -> Result<Box<dyn DecoderMemoryReservation>, ArrowError> {
        if self.0.cancelled.load(Ordering::SeqCst) {
            return Err(ArrowError::MemoryError("header cancelled".into()));
        }
        let mut current = self.0.used.load(Ordering::SeqCst);
        loop {
            let next = current
                .checked_add(bytes)
                .filter(|total| *total <= self.0.limit.load(Ordering::SeqCst))
                .ok_or_else(|| ArrowError::MemoryError("header denied".into()))?;
            match self.0.used.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        Ok(Box::new(Credit(self.0.clone(), bytes)))
    }
}

fn controlled() -> (Arc<Pool>, HeaderDecoder) {
    let pool = Arc::new(Pool::default());
    pool.limit.store(usize::MAX, Ordering::SeqCst);
    let memory: Memory = Some(Arc::new(Factory(pool.clone())));
    (pool, HeaderDecoder::with_memory(&memory).unwrap())
}

fn assert_memory_error(error: AvroError, expected: &str) {
    match error {
        AvroError::ArrowError(error) => {
            assert!(matches!(*error, ArrowError::MemoryError(ref message) if message == expected));
        }
        other => panic!("expected original typed memory error, got {other:?}"),
    }
}

fn header_bytes() -> Vec<u8> {
    let mut bytes = b"Obj\x01\x02\x02k\x06val\x00".to_vec();
    bytes.extend(0..16);
    bytes
}

#[test]
fn header_decoder_reuse_preserves_fragmented_metadata_and_sync() {
    let bytes = header_bytes();
    let mut decoder = HeaderDecoder::default();
    for _ in 0..3 {
        for byte in &bytes {
            assert_eq!(decoder.decode(std::slice::from_ref(byte)).unwrap(), 1);
        }
        let header = decoder.flush().expect("complete header");
        assert_eq!(header.get("k"), Some(b"val".as_slice()));
        assert_eq!(header.metadata().count(), 1);
        assert_eq!(header.sync(), std::array::from_fn(|i| i as u8));
    }
}

#[test]
fn header_clones_share_storage_and_credit_after_reader_drop() {
    for fragment in [1, 2, 5, 100] {
        let (pool, mut decoder) = controlled();
        let bytes = header_bytes();
        for chunk in bytes.chunks(fragment) {
            assert_eq!(decoder.decode(chunk).unwrap(), chunk.len());
        }
        let header = decoder.flush().unwrap();
        assert!(pool.used.load(Ordering::SeqCst) >= bytes.len() - 4 - 16);
        let used = pool.used.load(Ordering::SeqCst);
        let copy = header.clone();
        assert!(Arc::ptr_eq(&header.data, &copy.data));
        drop(header);
        drop(decoder);
        assert_eq!(copy.get("k"), Some(b"val".as_slice()));
        assert_eq!(copy.sync(), std::array::from_fn(|i| i as u8));
        assert_eq!(pool.used.load(Ordering::SeqCst), used);
        drop(copy);
        assert_eq!(pool.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn header_growth_admits_old_and_new_buffers_together() {
    let (pool, mut decoder) = controlled();
    // One key byte, then a declared value of three bytes. Offsets remain live.
    decoder.decode(b"Obj\x01\x02\x02k\x06").unwrap();
    let before = pool.used.load(Ordering::SeqCst);
    // The replacement needs four bytes while the old one-byte allocation lives.
    // Net-only accounting would wrongly allow this with three bytes available.
    pool.limit.store(before + 3, Ordering::SeqCst);
    assert_memory_error(decoder.decode(b"val").unwrap_err(), "header denied");
    assert_eq!(pool.used.load(Ordering::SeqCst), before);
    assert_eq!(&*decoder.meta_buf, b"k");
    drop(decoder);
    assert_eq!(pool.used.load(Ordering::SeqCst), 0);
}

#[test]
fn header_failure_and_cancellation_refund_without_partial_header() {
    for cancelled in [false, true] {
        let (pool, mut decoder) = controlled();
        pool.limit.store(0, Ordering::SeqCst);
        pool.cancelled.store(cancelled, Ordering::SeqCst);
        assert_memory_error(
            decoder.decode(b"Obj\x01").unwrap_err(),
            if cancelled {
                "header cancelled"
            } else {
                "header denied"
            },
        );
        assert!(decoder.flush().is_none());
        assert_eq!(pool.used.load(Ordering::SeqCst), 0);
    }
    let (pool, mut decoder) = controlled();
    decoder.decode(b"Obj\x01\x02\x02k\x06").unwrap();
    pool.cancelled.store(true, Ordering::SeqCst);
    assert_memory_error(decoder.decode(b"val").unwrap_err(), "header cancelled");
    drop(decoder);
    assert_eq!(pool.used.load(Ordering::SeqCst), 0);
}

#[test]
fn header_reuse_preserves_the_factory_and_independent_owners() {
    let (pool, mut decoder) = controlled();
    decoder.decode(&header_bytes()).unwrap();
    let first = decoder.flush().unwrap();
    let before = pool.used.load(Ordering::SeqCst);
    decoder.decode(&header_bytes()).unwrap();
    let second = decoder.flush().unwrap();
    assert_eq!(pool.used.load(Ordering::SeqCst), before * 2);
    drop(decoder);
    drop(first);
    assert_eq!(pool.used.load(Ordering::SeqCst), before);
    assert_eq!(second.get("k"), Some(b"val".as_slice()));
    drop(second);
    assert_eq!(pool.used.load(Ordering::SeqCst), 0);
}

#[test]
fn negative_header_lengths_fail_before_payload_allocation() {
    for bytes in [
        b"Obj\x01\x02\x01".as_slice(),
        b"Obj\x01\x02\x02k\x01",
        b"Obj\x01\x01\x01",
    ] {
        let (pool, mut decoder) = controlled();
        assert!(matches!(
            decoder.decode(bytes),
            Err(AvroError::ParseError(_))
        ));
        drop(decoder);
        assert_eq!(pool.used.load(Ordering::SeqCst), 0);
    }
}
