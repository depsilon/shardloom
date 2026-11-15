//! Operation-scoped evidence at the held native reader and scan boundaries.

use super::{SourceIdentity, resident_error};
use shardloom_core::{
    Result,
    native_file_scan::{
        NativeFileScanContract, NativeFileScanEvidence, NativeFileSourceObservation,
        NativeSourceSegmentReuseWork,
    },
};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::fmt::Write as _;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
pub(super) struct ReadWork {
    segments: AtomicU64,
    requests: AtomicU64,
    bytes: AtomicU64,
    cache_lookups: AtomicU64,
    cache_hits: AtomicU64,
    cache_cross_call_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_hit_bytes: AtomicU64,
    cache_cross_call_bytes: AtomicU64,
    cache_stores: AtomicU64,
    cache_copied_bytes: AtomicU64,
    cache_declines: AtomicU64,
}

impl ReadWork {
    pub(super) fn segment_requested(&self) -> Result<()> {
        increment(&self.segments, 1)
    }

    pub(super) fn read_completed(&self, bytes: usize) -> Result<()> {
        increment(&self.requests, 1)?;
        increment(&self.bytes, bytes as u64)
    }

    pub(super) fn cache_lookup(&self, hit_bytes: Option<(usize, bool)>) -> Result<()> {
        increment(&self.cache_lookups, 1)?;
        match hit_bytes {
            Some((bytes, cross_call)) => {
                increment(&self.cache_hits, 1)?;
                if cross_call {
                    increment(&self.cache_cross_call_hits, 1)?;
                    increment(&self.cache_cross_call_bytes, bytes as u64)?;
                }
                increment(&self.cache_hit_bytes, bytes as u64)
            }
            None => increment(&self.cache_misses, 1),
        }
    }

    pub(super) fn cache_copy(&self, copied_bytes: u64, stored: bool) -> Result<()> {
        increment(&self.cache_copied_bytes, copied_bytes)?;
        increment(
            if stored {
                &self.cache_stores
            } else {
                &self.cache_declines
            },
            1,
        )
    }

    fn reuse_snapshot(&self) -> shardloom_core::native_file_scan::NativeSourceSegmentReuseWork {
        shardloom_core::native_file_scan::NativeSourceSegmentReuseWork {
            lookups: self.cache_lookups.load(Ordering::Relaxed),
            hits: self.cache_hits.load(Ordering::Relaxed),
            cross_call_hits: self.cache_cross_call_hits.load(Ordering::Relaxed),
            misses: self.cache_misses.load(Ordering::Relaxed),
            hit_segment_bytes: self.cache_hit_bytes.load(Ordering::Relaxed),
            cross_call_hit_segment_bytes: self.cache_cross_call_bytes.load(Ordering::Relaxed),
            stored_segments: self.cache_stores.load(Ordering::Relaxed),
            copied_segment_bytes: self.cache_copied_bytes.load(Ordering::Relaxed),
            retention_declines: self.cache_declines.load(Ordering::Relaxed),
        }
    }
}

fn increment(counter: &AtomicU64, amount: u64) -> Result<()> {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(amount)
        })
        .map(|_| ())
        .map_err(|_| resident_error("native file read evidence counter overflow"))
}

struct Source {
    identity: Arc<SourceIdentity>,
    report: NativeFileSourceObservation,
    work: Arc<ReadWork>,
}

#[derive(Default)]
pub(super) struct Evidence {
    sources: Vec<Source>,
    contracts: Vec<NativeFileScanContract>,
    metadata: Option<MemoryLease>,
}

impl Evidence {
    fn reserve(&mut self, bytes: u64, memory: &LiveMemoryPool) -> Result<()> {
        let lease = self.metadata.get_or_insert(memory.reserve(0)?);
        lease.resize(
            lease
                .bytes()
                .checked_add(bytes)
                .ok_or_else(|| resident_error("native file evidence capacity overflow"))?,
        )
    }

    #[cfg(unix)]
    pub(super) fn source(
        &mut self,
        identity: &Arc<SourceIdentity>,
        memory: &LiveMemoryPool,
    ) -> Result<(usize, Arc<ReadWork>)> {
        if let Some((index, source)) = self
            .sources
            .iter()
            .enumerate()
            .find(|(_, source)| source.identity.same_generation(identity))
        {
            return Ok((index, Arc::clone(&source.work)));
        }
        let bytes = u64::try_from(identity.path().as_os_str().len())
            .ok()
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| resident_error("native file evidence source capacity overflow"))?;
        self.reserve(bytes, memory)?;
        self.sources
            .try_reserve_exact(1)
            .map_err(|error| resident_error(&error.to_string()))?;
        let index = self.sources.len();
        let work = Arc::new(ReadWork::default());
        let digest = identity.generation_fingerprint();
        let mut fingerprint = String::with_capacity(64);
        for byte in digest {
            write!(fingerprint, "{byte:02x}")
                .map_err(|error| resident_error(&error.to_string()))?;
        }
        self.sources.push(Source {
            identity: Arc::clone(identity),
            report: NativeFileSourceObservation {
                source_uri: identity.path().to_string_lossy().into_owned(),
                generation_fingerprint: fingerprint,
                segment_requests: 0,
                completed_read_requests: 0,
                completed_read_bytes: 0,
                segment_reuse: NativeSourceSegmentReuseWork::default(),
            },
            work: Arc::clone(&work),
        });
        Ok((index, work))
    }

    pub(super) fn record(
        &mut self,
        contract: NativeFileScanContract,
        memory: &LiveMemoryPool,
    ) -> Result<()> {
        self.begin(contract, memory).map(|_| ())
    }

    pub(super) fn begin(
        &mut self,
        contract: NativeFileScanContract,
        memory: &LiveMemoryPool,
    ) -> Result<usize> {
        if let Some((index, previous)) = self
            .contracts
            .iter_mut()
            .enumerate()
            .find(|(_, row)| row.same_contract(&contract))
        {
            previous.merge(&contract)?;
            return Ok(index);
        }
        self.reserve(contract_bytes(&contract)?, memory)?;
        self.contracts
            .try_reserve_exact(1)
            .map_err(|error| resident_error(&error.to_string()))?;
        let index = self.contracts.len();
        self.contracts.push(contract);
        Ok(index)
    }

    pub(super) fn add_rows(&mut self, index: usize, rows: u64) -> Result<()> {
        let contract = self
            .contracts
            .get_mut(index)
            .ok_or_else(|| resident_error("native scan evidence handle is absent"))?;
        contract.delivered_rows = contract
            .delivered_rows
            .checked_add(rows)
            .ok_or_else(|| resident_error("native scan evidence row counter overflow"))?;
        Ok(())
    }

    pub(super) fn record_path(
        &mut self,
        path: &std::path::Path,
        contract: NativeFileScanContract,
        memory: &LiveMemoryPool,
    ) -> Result<()> {
        self.begin_path(path, contract, memory).map(|_| ())
    }

    pub(super) fn begin_path(
        &mut self,
        path: &std::path::Path,
        mut contract: NativeFileScanContract,
        memory: &LiveMemoryPool,
    ) -> Result<usize> {
        let mut matches = self
            .sources
            .iter()
            .enumerate()
            .filter(|(_, source)| source.identity.path() == path);
        let (index, _) = matches
            .next()
            .ok_or_else(|| resident_error("scan evidence has no held source generation"))?;
        if matches.next().is_some() {
            return Err(resident_error(
                "scan evidence has ambiguous source generations",
            ));
        }
        contract.source = index;
        self.begin(contract, memory)
    }

    /// The operation must drain all reader work before taking this snapshot.
    pub(super) fn take(&mut self) -> (NativeFileScanEvidence, Option<MemoryLease>) {
        let sources = std::mem::take(&mut self.sources)
            .into_iter()
            .map(|mut source| {
                source.report.segment_requests = source.work.segments.load(Ordering::Relaxed);
                source.report.completed_read_requests =
                    source.work.requests.load(Ordering::Relaxed);
                source.report.completed_read_bytes = source.work.bytes.load(Ordering::Relaxed);
                source.report.segment_reuse = source.work.reuse_snapshot();
                source.report
            })
            .collect();
        (
            NativeFileScanEvidence {
                sources,
                contracts: std::mem::take(&mut self.contracts),
                session_segment_cache: None,
            },
            self.metadata.take(),
        )
    }
}

pub(crate) fn contract_bytes(contract: &NativeFileScanContract) -> Result<u64> {
    let mut bytes = 4096_u64;
    for columns in [
        &contract.projected_columns,
        &contract.output_columns,
        &contract.pushed_predicate_columns,
        &contract.residual_predicate_columns,
    ]
    .into_iter()
    .flatten()
    {
        for column in columns {
            bytes = bytes
                .checked_add(column.len() as u64)
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<String>() as u64))
                .ok_or_else(|| resident_error("native file scan contract capacity overflow"))?;
        }
    }
    bytes
        .checked_mul(2)
        .ok_or_else(|| resident_error("native file scan contract capacity overflow"))
}

#[cfg(test)]
#[path = "resident_file_evidence_tests.rs"]
mod tests;
