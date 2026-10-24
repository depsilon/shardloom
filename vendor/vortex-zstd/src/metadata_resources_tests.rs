// SPDX-License-Identifier: Apache-2.0
// ShardLoom regression tests for admitted native frame tables.

use super::*;
use parking_lot::Mutex;
use vortex_array::memory::{DefaultHostAllocator, HostAllocator, WritableHostBuffer};
use vortex_buffer::Alignment;

#[derive(Debug, Default)]
struct Counts {
    live: usize,
    peak: usize,
    requests: Vec<usize>,
}

#[derive(Debug)]
struct Allocator {
    counts: Arc<Mutex<Counts>>,
    limit: usize,
}

struct Credit {
    counts: Arc<Mutex<Counts>>,
    bytes: usize,
}

impl Drop for Credit {
    fn drop(&mut self) {
        self.counts.lock().live -= self.bytes;
    }
}

impl HostAllocator for Allocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        DefaultHostAllocator.allocate(len, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        let mut counts = self.counts.lock();
        counts.requests.push(bytes);
        let live = counts
            .live
            .checked_add(bytes)
            .filter(|live| *live <= self.limit)
            .ok_or_else(|| vortex_err!("denied metadata reservation"))?;
        counts.live = live;
        counts.peak = counts.peak.max(live);
        Ok(HostMemoryReservation::new(Credit {
            counts: Arc::clone(&self.counts),
            bytes,
        }))
    }
}

fn allocator(limit: usize) -> (HostAllocatorRef, Arc<Mutex<Counts>>) {
    let counts = Arc::new(Mutex::new(Counts::default()));
    (
        Arc::new(Allocator {
            counts: Arc::clone(&counts),
            limit,
        }),
        counts,
    )
}

fn buffers(count: usize) -> Vec<BufferHandle> {
    (0..count)
        .map(|_| BufferHandle::new_host(ByteBuffer::empty()))
        .collect()
}

#[test]
fn frame_table_admission_is_exact_and_shared_until_the_last_owner() {
    let bytes = capacity_bytes(7).unwrap();
    let (allocator, counts) = allocator(bytes);
    let storage = Arc::new(FrameStorage::new_in(7, &allocator).unwrap());
    assert_eq!(storage.frames.capacity(), 7);
    assert_eq!(storage.metadata.frames.capacity(), 7);
    let retained = Arc::clone(&storage);
    drop(storage);
    assert_eq!(counts.lock().live, bytes);
    assert_eq!(counts.lock().requests, vec![bytes]);
    assert!(FrameStorage::new_in(1, &allocator).is_err());
    assert_eq!(counts.lock().peak, bytes);
    drop(retained);
    assert_eq!(counts.lock().live, 0);
    drop(FrameStorage::new_in(7, &allocator).unwrap());
    assert_eq!(counts.lock().live, 0);
}

#[test]
fn frame_table_denial_empty_and_overflow_do_not_allocate_a_table() {
    let bytes = capacity_bytes(1).unwrap();
    let (allocator, counts) = allocator(bytes - 1);
    let error = FrameStorage::new_in(1, &allocator).unwrap_err();
    assert!(error.to_string().contains("denied metadata reservation"));
    assert_eq!(counts.lock().peak, 0);
    assert_eq!(counts.lock().requests, vec![bytes]);
    let empty = FrameStorage::new_in(0, &allocator).unwrap();
    assert_eq!(empty.frames.capacity(), 0);
    assert_eq!(empty.metadata.frames.capacity(), 0);
    assert!(FrameStorage::new_in(usize::MAX, &allocator).is_err());
    assert_eq!(counts.lock().requests, vec![bytes]);
}

#[test]
fn frame_metadata_wire_scan_preserves_prost_unknown_and_repeated_field_semantics() {
    for dictionary_size in [0, 9] {
        let mut bytes = ZstdMetadata {
            dictionary_size: 3,
            frames: vec![ZstdFrameMetadata {
                uncompressed_size: 11,
                n_values: 2,
            }],
        }
        .encode_to_vec();
        bytes.extend_from_slice(&[
            0x48,
            0xac,
            0x02, // Unknown varint.
            0x53,
            0x12,
            0,
            0x54, // Unknown group containing a frame-like field.
            0x5a,
            2,
            0x12,
            0, // Unknown bytes containing a frame-like field.
            0x08,
            dictionary_size, // The last dictionary size wins.
            0x12,
            0, // A real empty repeated frame message retains legacy defaults.
        ]);
        let expected = ZstdMetadata::decode(bytes.as_slice()).unwrap();
        let (allocator, counts) = allocator(capacity_bytes(2).unwrap());
        let storage = FrameStorage::deserialize(
            &bytes,
            &buffers(2 + usize::from(dictionary_size != 0)),
            &allocator,
        )
        .unwrap();
        assert_eq!(storage.metadata.encode_to_vec(), expected.encode_to_vec());
        assert_eq!(storage.frames.len(), 2);
        assert_eq!(storage.frames.capacity(), 2);
        assert_eq!(storage.metadata.frames.capacity(), 2);
        assert_eq!(
            counts.lock().requests,
            vec![capacity_bytes(2).unwrap()]
        );
        drop(storage);
        assert_eq!(counts.lock().live, 0);
    }
}

#[test]
fn frame_metadata_rejects_top_level_wire_errors_before_reservation() {
    let malformed: &[&[u8]] = &[
        &[0x10, 0],    // Frame field with scalar wire type.
        &[0x0a, 0],    // Dictionary size with message wire type.
        &[0x12, 4, 0], // Truncated frame extent.
        &[0],          // Invalid tag.
        &[0x53, 0x5c], // Mismatched unknown group end.
        &[0x54],       // Unexpected group end.
        &[
            0x12, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1,
        ],
    ];
    let (allocator, counts) = allocator(0);
    for bytes in malformed {
        assert!(ZstdMetadata::decode(*bytes).is_err());
        assert!(FrameStorage::deserialize(bytes, &[], &allocator).is_err());
        assert!(counts.lock().requests.is_empty());
    }
    // Prost's bounded group recursion must apply to the preallocation scan too.
    let mut deep = vec![0x53; 150];
    deep.extend([0x54; 150]);
    assert!(FrameStorage::deserialize(&deep, &[], &allocator).is_err());
    assert!(counts.lock().requests.is_empty());
}

#[test]
fn frame_metadata_rejects_buffer_count_before_reservation_and_refunds_nested_errors() {
    let metadata = ZstdMetadata {
        dictionary_size: 5,
        frames: vec![ZstdFrameMetadata {
            uncompressed_size: 11,
            n_values: 2,
        }],
    }
    .encode_to_vec();
    let (allocator, counts) = allocator(capacity_bytes(1).unwrap());
    for count in [0, 1, 3] {
        assert!(FrameStorage::deserialize(&metadata, &buffers(count), &allocator).is_err());
        assert!(counts.lock().requests.is_empty());
    }
    // Top-level frame count is valid, but the nested frame has an invalid tag.
    assert!(FrameStorage::deserialize(&[0x12, 1, 0], &buffers(1), &allocator).is_err());
    assert_eq!(
        counts.lock().requests,
        vec![capacity_bytes(1).unwrap()]
    );
    assert_eq!(counts.lock().live, 0);
    // A valid parse still works after failure and releases the same credit.
    drop(FrameStorage::deserialize(&metadata, &buffers(2), &allocator).unwrap());
    assert_eq!(counts.lock().live, 0);
}

#[test]
fn frame_metadata_reservation_precedes_prost_message_construction() {
    let bytes: Vec<_> = (0..4096).flat_map(|_| [0x12, 0]).collect();
    let (allocator, counts) = allocator(1);
    let error = FrameStorage::deserialize(&bytes, &buffers(4096), &allocator).unwrap_err();
    assert!(error.to_string().contains("denied metadata reservation"));
    assert_eq!(
        counts.lock().requests,
        vec![capacity_bytes(4096).unwrap()]
    );
    assert_eq!(counts.lock().peak, 0);
}
