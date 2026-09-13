//! EOF-only unique-value counts over group-hash-partitioned complete pairs.
//! Pair owners remain unchanged until both local and global output admission.

use super::{
    CompoundPartitions, Partition, allocate, compound_count_partial, elapsed, failed, retain_best,
};
use crate::local_primitives::aggregate_chunk_jobs::ChunkWorkerContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use std::time::Instant;

pub(in super::super) struct DistinctSelection {
    partition: usize,
    selected: Vec<(usize, u64)>,
    pub complete_groups: usize,
    _lease: MemoryLease,
}
struct DomainCounts {
    values: Vec<u64>,
    _lease: MemoryLease,
}

impl CompoundPartitions {
    pub(in super::super) fn select_text_distinct(
        &self,
        index: usize,
        worker: &ChunkWorkerContext,
    ) -> Result<DistinctSelection> {
        worker.check_cancelled()?;
        if !self.text_distinct || self.pressured() {
            return Err(failed(
                "UTF8 DISTINCT selection requires complete unpressured pair state",
            ));
        }
        let started = Instant::now();
        let mut partition = self.partitions[index]
            .lock()
            .map_err(|_| failed("partition mutex poisoned"))?;
        let Some((values, lease)) = allocate::<u64>(partition.text.len(), &self.memory)? else {
            return Err(failed("UTF8 DISTINCT EOF domain-count reservation denied"));
        };
        let mut counts = DomainCounts {
            values,
            _lease: lease,
        };
        counts.values.resize(partition.text.len(), 0);
        // Weight can be arbitrarily large; each exact pair contributes ONE.
        for (ordinal, group) in partition.groups.iter().copied().enumerate() {
            if ordinal.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            if group.count == 0 {
                continue;
            }
            let domain = partition.domain_index(group.offset, group.len, worker)?;
            counts.values[domain] = counts.values[domain]
                .checked_add(1)
                .ok_or_else(|| failed("UTF8 DISTINCT count overflowed"))?;
        }
        let complete_groups = counts.values.iter().filter(|count| **count != 0).count();
        if complete_groups != partition.text_len {
            return Err(failed(
                "complete UTF8 DISTINCT domain includes uncommitted text",
            ));
        }
        let cap = self.retained.min(complete_groups);
        let lease = partition
            .selection_lease
            .take()
            .ok_or_else(|| failed("partition selected twice"))?;
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(cap)
            .map_err(|error| failed(&error.to_string()))?;
        if selected
            .capacity()
            .checked_mul(size_of::<(usize, u64)>())
            .is_none_or(|bytes| bytes as u64 > lease.bytes())
        {
            return Err(failed("UTF8 DISTINCT selection exceeds reservation"));
        }
        for (domain, count) in counts.values.iter().copied().enumerate() {
            if domain.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            if count == 0 {
                continue;
            }
            retain_best(&mut selected, (domain, count), cap, |left, right| {
                partition.worse_distinct(left, right)
            });
        }
        elapsed(&self.selection, started)?;
        Ok(DistinctSelection {
            partition: index,
            selected,
            complete_groups,
            _lease: lease,
        })
    }

    pub(in super::super) fn visit_text_distinct(
        &self,
        selection: &DistinctSelection,
        mut visit: impl FnMut(&str, u64) -> Result<()>,
    ) -> Result<()> {
        if !self.text_distinct {
            return Err(failed("UTF8 DISTINCT result lost partition mode"));
        }
        let partition = self.partitions[selection.partition]
            .lock()
            .map_err(|_| failed("partition mutex poisoned"))?;
        for &(index, count) in &selection.selected {
            let slot = partition.text[index];
            let value = std::str::from_utf8(&partition.bytes[slot.offset..slot.offset + slot.len])
                .map_err(|error| failed(&error.to_string()))?;
            visit(value, count)?;
        }
        Ok(())
    }
}

impl Partition {
    fn domain_index(
        &self,
        offset: usize,
        len: usize,
        worker: &ChunkWorkerContext,
    ) -> Result<usize> {
        let bytes = &self.bytes[offset..offset + len];
        let hash = compound_count_partial::string_hash(bytes);
        let mut index = compound_count_partial::bucket(hash, self.text.len());
        let mut probes = 0_usize;
        loop {
            if probes.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            probes += 1;
            let slot = self.text[index];
            if !slot.occupied {
                return Err(failed("complete pair lost its UTF8 domain"));
            }
            if slot.offset == offset && slot.len == len {
                return Ok(index);
            }
            index = (index + 1) & (self.text.len() - 1);
        }
    }
    fn worse_distinct(&self, left: (usize, u64), right: (usize, u64)) -> bool {
        if left.1 != right.1 {
            return left.1 < right.1;
        }
        let left = self.text[left.0];
        let right = self.text[right.0];
        self.bytes[left.offset..left.offset + left.len]
            > self.bytes[right.offset..right.offset + right.len]
    }
}
