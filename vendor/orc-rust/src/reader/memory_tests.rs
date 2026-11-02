// SPDX-License-Identifier: Apache-2.0
//! Shared deterministic test budget for provider allocation/lifetime controls.
use super::{DecoderMemory, DecoderMemoryReservation};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) live: usize,
    pub(crate) peak: usize,
    pub(crate) requests: usize,
}

#[derive(Debug)]
pub(crate) struct Memory {
    pub(crate) counters: Arc<Mutex<Counters>>,
    pub(crate) limit: usize,
}

#[derive(Debug)]
struct Credit {
    pub(crate) counters: Arc<Mutex<Counters>>,
    pub(crate) limit: usize,
    bytes: usize,
}

impl DecoderMemory for Memory {
    fn reserve(&self, bytes: usize) -> std::io::Result<Box<dyn DecoderMemoryReservation>> {
        let mut credit = Credit {
            counters: Arc::clone(&self.counters),
            limit: self.limit,
            bytes: 0,
        };
        credit.resize(bytes)?;
        Ok(Box::new(credit))
    }
}

impl DecoderMemoryReservation for Credit {
    fn resize(&mut self, bytes: usize) -> std::io::Result<()> {
        let mut counters = self.counters.lock().unwrap();
        counters.requests += 1;
        let live = counters.live - self.bytes + bytes;
        if live > self.limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "test shared memory denied",
            ));
        }
        counters.live = live;
        counters.peak = counters.peak.max(live);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for Credit {
    fn drop(&mut self) {
        self.counters.lock().unwrap().live -= self.bytes;
    }
}
