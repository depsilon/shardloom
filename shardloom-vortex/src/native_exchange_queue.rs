// SPDX-License-Identifier: Apache-2.0
//! Application queue credit follows every owning Flight byte field.
//! Provider encoding/TLS copies remain outside this application accounting.

use super::{attempts::Attempt, *};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const QUEUED_BYTES: usize = 4 << 20;
const QUEUED_FRAMES: usize = 4;
const IDLE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct Queue {
    memory: LiveMemoryPool,
    bytes: Arc<Semaphore>,
    frames: Arc<Semaphore>,
}

struct Credit {
    _memory: MemoryLease,
    _bytes: OwnedSemaphorePermit,
    _frame: OwnedSemaphorePermit,
    _attempt: Arc<Attempt>,
}

struct FieldOwner {
    bytes: Bytes,
    _credit: Arc<Credit>,
}

impl AsRef<[u8]> for FieldOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Queue {
    pub(crate) fn new(memory: LiveMemoryPool) -> Self {
        Self {
            memory,
            bytes: Arc::new(Semaphore::new(QUEUED_BYTES)),
            frames: Arc::new(Semaphore::new(QUEUED_FRAMES)),
        }
    }

    pub(crate) async fn frame(
        &self,
        attempt: &Arc<Attempt>,
        encoded_bytes: usize,
        produce: impl FnOnce() -> Result<FlightData>,
    ) -> Result<FlightData> {
        let result = self.frame_inner(attempt, encoded_bytes, produce).await;
        if result.is_err() {
            attempt.cancel();
        }
        result
    }

    async fn frame_inner(
        &self,
        attempt: &Arc<Attempt>,
        encoded_bytes: usize,
        produce: impl FnOnce() -> Result<FlightData>,
    ) -> Result<FlightData> {
        if encoded_bytes == 0 || encoded_bytes > MAX_RPC {
            return Err(limit("queued Flight message exceeds encoded RPC limit"));
        }
        // Include the gRPC length prefix conservatively in the queue allowance.
        let charge = encoded_bytes + 5;
        let credits = async {
            let frame = self
                .frames
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| limit("frame queue is closed"))?;
            let bytes = self
                .bytes
                .clone()
                .acquire_many_owned(
                    u32::try_from(charge).map_err(|_| limit("queued byte charge overflows"))?,
                )
                .await
                .map_err(|_| limit("byte queue is closed"))?;
            let memory = self
                .memory
                .reserve(charge as u64)
                .map_err(|_| limit("native grant denies queued frame"))?;
            Ok::<_, Error>(Credit {
                _memory: memory,
                _bytes: bytes,
                _frame: frame,
                _attempt: attempt.clone(),
            })
        };
        let credit = tokio::select! {
            () = attempt.cancelled() => return Err(Error { code: "SLX_CANCELLED", detail: "queue admission cancelled" }),
            result = tokio::time::timeout(IDLE, credits) => result
                .map_err(|_| Error { code: "SLX_IDLE", detail: "queue made no progress within idle deadline" })??,
        };
        attempt.check()?;
        let mut data = produce()?;
        attempt.check()?;
        if data.encoded_len() != encoded_bytes || data.flight_descriptor.is_some() {
            return Err(invalid(
                "queued response differs from its admitted size or shape",
            ));
        }
        let credit = Arc::new(credit);
        for field in [
            &mut data.data_header,
            &mut data.data_body,
            &mut data.app_metadata,
        ] {
            if !field.is_empty() {
                *field = Bytes::from_owner(FieldOwner {
                    bytes: std::mem::take(field),
                    _credit: credit.clone(),
                });
            }
        }
        Ok(data)
    }

    pub(crate) fn snapshot(&self) -> (usize, usize) {
        (
            QUEUED_BYTES - self.bytes.available_permits(),
            QUEUED_FRAMES - self.frames.available_permits(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_exchange_protocol::{
        attempts::{Connection, Principal, Registry},
        authority::{Authority, SUITE_QUERY},
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    fn fixture(budget: u64) -> (LiveMemoryPool, Queue, Registry, Arc<Attempt>) {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let queue = Queue::new(memory.clone());
        let authority = Arc::new(Authority::load().unwrap());
        let epoch = Bytes::from_static(&[1; 16]);
        let binding = authority
            .case("dictionary_large")
            .unwrap()
            .binding(
                "reader",
                &epoch,
                Bytes::from_static(&[2; 16]),
                1,
                SUITE_QUERY,
            )
            .unwrap();
        let registry = Registry::new(authority, epoch).unwrap();
        let attempt = registry
            .begin(Principal::Reader, &binding, &Connection::new(), false)
            .unwrap();
        (memory, queue, registry, attempt)
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    fn data() -> FlightData {
        FlightData {
            data_header: Bytes::from_static(b"header"),
            data_body: Bytes::from_static(b"body"),
            app_metadata: Bytes::from_static(b"envelope"),
            ..Default::default()
        }
    }

    #[test]
    fn each_cloned_field_retains_queue_memory_and_attempt_until_final_drop() {
        let (memory, queue, registry, attempt) = fixture(1 << 20);
        let expected = data().encoded_len() + 5;
        let sent = runtime()
            .block_on(queue.frame(&attempt, data().encoded_len(), || Ok(data())))
            .unwrap();
        let header = sent.data_header.clone();
        let body = sent.data_body.clone();
        let metadata = sent.app_metadata.clone();
        attempt.complete().unwrap();
        drop(attempt);
        drop(sent);
        assert_eq!(queue.snapshot(), (expected, 1));
        assert_eq!(memory.snapshot().reserved_bytes, expected as u64);
        assert_eq!(registry.snapshot().unwrap().0, 1);
        drop(header);
        drop(metadata);
        assert_eq!(memory.snapshot().reserved_bytes, expected as u64);
        drop(body);
        assert_eq!(queue.snapshot(), (0, 0));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(registry.snapshot().unwrap().0, 0);
    }

    #[test]
    fn grant_and_shape_refusals_release_permits_and_do_not_allocate_unadmitted_payload() {
        let (memory, queue, _, attempt) = fixture(1);
        let allocated = AtomicBool::new(false);
        let denied = runtime().block_on(queue.frame(&attempt, data().encoded_len(), || {
            allocated.store(true, Ordering::Relaxed);
            Ok(data())
        }));
        assert_eq!(denied.unwrap_err().code, "SLX_RESOURCE_LIMIT");
        assert!(!allocated.load(Ordering::Relaxed));
        assert!(attempt.check().is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(queue.snapshot(), (0, 0));
        let (_, queue, _, attempt) = fixture(1 << 20);
        for size in [0, MAX_RPC + 1] {
            assert!(
                runtime()
                    .block_on(queue.frame(&attempt, size, || {
                        allocated.store(true, Ordering::Relaxed);
                        Ok(data())
                    }))
                    .is_err()
            );
        }
        assert!(!allocated.load(Ordering::Relaxed));
        assert!(
            runtime()
                .block_on(queue.frame(&attempt, data().encoded_len() + 1, || Ok(data())))
                .is_err()
        );
        assert_eq!(queue.snapshot(), (0, 0));
    }

    #[test]
    fn full_queue_backpressures_then_cancellation_drains_every_waiter() {
        let (memory, queue, _, attempt) = fixture(8 << 20);
        runtime().block_on(async {
            let mut held = Vec::new();
            for _ in 0..QUEUED_FRAMES {
                held.push(
                    queue
                        .frame(&attempt, data().encoded_len(), || Ok(data()))
                        .await
                        .unwrap(),
                );
            }
            let allocated = Arc::new(AtomicBool::new(false));
            let task_queue = queue.clone();
            let task_attempt = attempt.clone();
            let task_allocated = allocated.clone();
            let blocked = tokio::spawn(async move {
                task_queue
                    .frame(&task_attempt, data().encoded_len(), || {
                        task_allocated.store(true, Ordering::Relaxed);
                        Ok(data())
                    })
                    .await
            });
            tokio::task::yield_now().await;
            assert!(!blocked.is_finished());
            assert!(!allocated.load(Ordering::Relaxed));
            attempt.cancel();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), blocked)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err()
                    .code,
                "SLX_CANCELLED"
            );
            drop(held);
        });
        assert_eq!(queue.snapshot(), (0, 0));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn queue_byte_ceiling_applies_before_frame_count_and_releases_on_buffer_drop() {
        let (memory, queue, _, attempt) = fixture(8 << 20);
        let maximum_frame = || FlightData {
            // Field 1000 has a two-byte tag and this length has three varint bytes.
            data_body: Bytes::from(vec![1; MAX_RPC - 5]),
            ..Default::default()
        };
        runtime().block_on(async {
            let first = queue
                .frame(&attempt, MAX_RPC, || Ok(maximum_frame()))
                .await
                .unwrap();
            assert_eq!(queue.snapshot(), (MAX_RPC + 5, 1));
            let other_queue = queue.clone();
            let other_attempt = attempt.clone();
            let second = tokio::spawn(async move {
                other_queue
                    .frame(&other_attempt, MAX_RPC, || Ok(maximum_frame()))
                    .await
            });
            tokio::task::yield_now().await;
            assert!(!second.is_finished());
            assert_eq!(memory.snapshot().reserved_bytes, (MAX_RPC + 5) as u64);
            drop(first);
            let second = tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(queue.snapshot(), (MAX_RPC + 5, 1));
            drop(second);
        });
        assert_eq!(queue.snapshot(), (0, 0));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
