// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

#![expect(
    clippy::disallowed_types,
    reason = "The release-order test log uses the standard mutex without adding a provider dependency."
)]

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::{Arc, Barrier, Mutex};
use std::thread::Builder;

use bytes::Bytes;

use crate::{Alignment, Buffer, ByteBuffer};

const DEPTH: usize = 8192;
const STACK: usize = 128 * 1024;

struct Probe {
    id: usize,
    log: Arc<Mutex<Vec<usize>>>,
    panic_at: Option<usize>,
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.log.lock().unwrap().push(self.id);
        assert!(
            self.panic_at != Some(self.id),
            "retained owner probe {}",
            self.id
        );
    }
}

struct OriginalOwner {
    buffer: ByteBuffer,
    _owner: Arc<Probe>,
}

impl AsRef<[u8]> for OriginalOwner {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

fn attach(buffer: ByteBuffer, owner: Arc<Probe>, original: bool) -> ByteBuffer {
    if original {
        let alignment = buffer.alignment();
        ByteBuffer::from_bytes_aligned_with_owner(
            Bytes::from_owner(OriginalOwner {
                buffer,
                _owner: owner,
            }),
            alignment,
        )
    } else {
        buffer.with_retained_owner(owner)
    }
}

fn probe(id: usize, log: &Arc<Mutex<Vec<usize>>>, panic_at: Option<usize>) -> Arc<Probe> {
    Arc::new(Probe {
        id,
        log: Arc::clone(log),
        panic_at,
    })
}

#[test]
fn retained_owners_match_original_views_aliases_and_release_order() {
    for empty in [false, true] {
        for depth in [1, 3, 16] {
            let mut logs = Vec::new();
            for original in [true, false] {
                let log = Arc::new(Mutex::new(Vec::new()));
                let mut buffer =
                    ByteBuffer::zeroed_aligned(if empty { 0 } else { 128 }, Alignment::new(64));
                let pointer = buffer.as_ptr();
                let mut middle = None;
                for id in 0..depth {
                    buffer = attach(buffer, probe(id, &log, None), original);
                    if id == depth / 2 {
                        middle = Some(buffer.clone());
                    }
                }
                assert_eq!(buffer.as_ptr(), pointer);
                assert_eq!(buffer.alignment(), Alignment::new(64));
                let buffer = buffer.try_into_mut().unwrap_err();
                let typed = Buffer::<u64>::from_byte_buffer(buffer.clone());
                assert_eq!(typed.as_ptr().cast::<u8>(), pointer);
                let detached = buffer.slice(0..0);
                assert!(detached.backing.is_none());
                let slice = (!empty).then(|| buffer.slice(64..80));
                if let Some(slice) = &slice {
                    assert_eq!(slice.as_ptr().addr(), pointer.addr() + 64);
                    assert_eq!(slice.as_slice(), &[0; 16]);
                }
                let bytes = typed.into_byte_buffer().into_bytes();
                assert_eq!(bytes.as_ptr(), pointer);
                drop((buffer, slice));
                assert!(log.lock().unwrap().is_empty());
                drop(bytes);
                let split = log.lock().unwrap().clone();
                drop(middle);
                logs.push((split, log.lock().unwrap().clone()));
            }
            assert_eq!(logs[0], logs[1]);
        }
    }
}

#[test]
#[cfg(feature = "arrow")]
fn retained_owners_survive_empty_and_nonempty_arrow_conversions() {
    for len in [0, 16] {
        let log = Arc::new(Mutex::new(Vec::new()));
        let buffer = Buffer::<u64>::zeroed(len).with_retained_owner(probe(0, &log, None));
        let pointer = buffer.as_ptr();
        let arrow = buffer.into_arrow_scalar_buffer();
        assert_eq!(arrow.as_ptr(), pointer);
        let returned = Buffer::from_arrow_scalar_buffer(arrow);
        let bytes = returned.clone().into_byte_buffer().into_bytes();
        drop(returned);
        assert!(log.lock().unwrap().is_empty());
        assert_eq!(bytes.as_ptr(), pointer.cast::<u8>());
        drop(bytes);
        assert_eq!(*log.lock().unwrap(), [0]);
    }
}

fn deep_child() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut buffer = ByteBuffer::zeroed(8);
    let mut middle = None;
    let mut weak = None;
    for id in 0..DEPTH {
        buffer = buffer.with_retained_owner(probe(id, &log, None));
        if id == DEPTH / 2 - 1 {
            middle = Some(buffer.clone());
            weak = buffer.backing.as_ref().map(Arc::downgrade);
        }
    }
    drop(buffer);
    assert_eq!(*log.lock().unwrap(), (DEPTH / 2..DEPTH).collect::<Vec<_>>());
    assert!(weak.as_ref().unwrap().upgrade().is_some());
    drop(middle);
    assert!(weak.unwrap().upgrade().is_none());
    assert_eq!(
        *log.lock().unwrap(),
        (DEPTH / 2..DEPTH).chain(0..DEPTH / 2).collect::<Vec<_>>()
    );

    for _ in 0..16 {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut buffer = ByteBuffer::zeroed(8);
        for id in 0..DEPTH {
            buffer = buffer.with_retained_owner(probe(id, &log, None));
        }
        let weak = Arc::downgrade(buffer.backing.as_ref().unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let workers = [buffer.clone(), buffer].map(|buffer| {
            let barrier = Arc::clone(&barrier);
            Builder::new()
                .stack_size(STACK)
                .spawn(move || {
                    barrier.wait();
                    drop(buffer);
                })
                .unwrap()
        });
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(weak.upgrade().is_none());
        assert_eq!(*log.lock().unwrap(), (0..DEPTH).collect::<Vec<_>>());
    }

    for panic_at in [0, 1, DEPTH / 2, DEPTH - 1] {
        let log = Arc::new(Mutex::new(Vec::new()));
        // A Bytes-owned base exercises a foreign storage destructor panic;
        // later positions exercise attached-owner panic during explicit drain.
        let mut buffer = attach(ByteBuffer::zeroed(8), probe(0, &log, Some(panic_at)), true);
        for id in 1..DEPTH {
            buffer = buffer.with_retained_owner(probe(id, &log, Some(panic_at)));
        }
        assert!(catch_unwind(AssertUnwindSafe(|| drop(buffer))).is_err());
        assert_eq!(*log.lock().unwrap(), (0..DEPTH).collect::<Vec<_>>());
    }
    println!("BUFFER_RETAINED_OWNER_DEEP_COMPLETE");
}

#[test]
fn retained_owners_release_deep_shared_and_panicking_chains_on_small_stacks() {
    const ENV: &str = "SHARDLOOM_BUFFER_OWNER_DEEP_CHILD";
    if std::env::var_os(ENV).is_some() {
        Builder::new()
            .stack_size(STACK)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "retained_owner::tests::retained_owners_release_deep_shared_and_panicking_chains_on_small_stacks", "--nocapture"])
        .env(ENV, "1").output().unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("BUFFER_RETAINED_OWNER_DEEP_COMPLETE")
    );
}
