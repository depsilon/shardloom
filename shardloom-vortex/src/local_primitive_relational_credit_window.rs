//! Bounded native admission and logical credits for immutable output.
//! Payload leases remain with their existing Vortex/Arrow owners.

use super::{BATCH_ROWS, NodeKind, PreparedRoot, PreparedVortexRelational, ScanSource, failed};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::dtype::{DType, PType};

const WINDOW_BYTES: usize = 4 << 20;
const BATCH_BYTES: usize = 2 << 20;
const METADATA_BYTES: u64 = 1024;

/// Two ordered, unacknowledged output slots under the original native grant.
/// Acknowledgment releases only logical credits, never a payload reservation.
pub struct OutputCreditWindow {
    expected_rows: usize,
    produced_rows: usize,
    produced_batches: usize,
    credits: [(usize, usize); 2],
    head: usize,
    count: usize,
    bytes: usize,
    metadata: MemoryLease,
}

impl OutputCreditWindow {
    fn new(rows: usize, pool: &LiveMemoryPool) -> Result<Self> {
        // Reserve before constructing the fixed ledger. No payload allowance
        // or replacement memory pool is created by this policy.
        let metadata = pool.reserve(METADATA_BYTES)?;
        Ok(Self {
            expected_rows: rows,
            produced_rows: 0,
            produced_batches: 0,
            credits: [(0, 0); 2],
            head: 0,
            count: 0,
            bytes: 0,
            metadata,
        })
    }

    #[must_use]
    pub fn expected_rows(&self) -> usize {
        self.expected_rows
    }

    #[must_use]
    pub fn metadata_bytes(&self) -> u64 {
        self.metadata.bytes()
    }

    /// Charge a batch before it can enter the channel. Returns its exact ordinal.
    /// # Errors
    /// Rejects extra/malformed batches, oversized references or exhausted slots.
    pub fn enqueue(&mut self, rows: usize, referenced_bytes: usize) -> Result<usize> {
        let expected_batches = self.expected_rows.div_ceil(BATCH_ROWS).max(1);
        let expected_rows = (self.expected_rows - self.produced_rows).min(BATCH_ROWS);
        if self.produced_batches >= expected_batches || rows != expected_rows {
            return Err(failed("output window changed the admitted batch partition"));
        }
        let bytes = self
            .bytes
            .checked_add(referenced_bytes)
            .ok_or_else(|| failed("output window referenced-byte count overflowed"))?;
        if self.count == 2 || referenced_bytes > BATCH_BYTES || bytes > WINDOW_BYTES {
            return Err(failed(
                "output window exceeds two batches, 2 MiB per batch or 4 MiB total",
            ));
        }
        let ordinal = self.produced_batches;
        self.credits[(self.head + self.count) % 2] = (ordinal, referenced_bytes);
        self.count += 1;
        self.bytes = bytes;
        self.produced_rows += rows;
        self.produced_batches += 1;
        Ok(ordinal)
    }

    /// The second send and every remaining final acknowledgment form barriers.
    #[must_use]
    pub fn requires_acknowledgment(&self) -> bool {
        self.count == 2
            || (self.produced_batches == self.expected_rows.div_ceil(BATCH_ROWS).max(1)
                && self.count != 0)
    }

    #[must_use]
    pub fn oldest_ordinal(&self) -> Option<usize> {
        (self.count != 0).then(|| self.credits[self.head].0)
    }

    /// Release a logical slot only after receiving its actual acknowledgment.
    /// # Errors
    /// Rejects out-of-order, duplicate or unsolicited acknowledgments unchanged.
    pub fn acknowledge(&mut self, ordinal: usize) -> Result<()> {
        if self.oldest_ordinal() != Some(ordinal) {
            return Err(failed(
                "output window acknowledgment is duplicate or out of order",
            ));
        }
        self.bytes -= self.credits[self.head].1;
        self.credits[self.head] = (0, 0);
        self.head = (self.head + 1) % 2;
        self.count -= 1;
        Ok(())
    }

    /// Check the final barrier before accepting the native completion evidence.
    /// # Errors
    /// Rejects incomplete output or a remaining logical credit.
    pub fn finish(&self) -> Result<()> {
        if self.produced_rows != self.expected_rows
            || self.produced_batches != self.expected_rows.div_ceil(BATCH_ROWS).max(1)
            || self.count != 0
            || self.bytes != 0
        {
            return Err(failed(
                "output window finished before all output acknowledgments",
            ));
        }
        Ok(())
    }
}

impl PreparedVortexRelational {
    pub(super) fn output_credit_window(
        &self,
        requested: bool,
    ) -> Result<Option<OutputCreditWindow>> {
        if !requested
            || !self.sources.is_empty()
            || !self.source_paths.is_empty()
            || !self.batch_sources.is_empty()
            || self.memory_sources.len() != 1
            || self.spill.is_some()
        {
            return Ok(None);
        }
        #[cfg(feature = "universal-format-io")]
        if !self.preparation_sources.is_empty() {
            return Ok(None);
        }
        let PreparedRoot::Bound(root) = &self.root else {
            return Ok(None);
        };
        if root.fields.is_empty()
            || root.fields.len() > 64
            || root
                .fields
                .iter()
                .any(|(_, dtype)| !matches!(dtype, DType::Primitive(PType::I64, _)))
        {
            return Ok(None);
        }
        let scan = match &root.kind {
            NodeKind::Project { input, .. } => input.as_ref(),
            NodeKind::Scan { .. } => root.as_ref(),
            _ => return Ok(None),
        };
        let NodeKind::Scan {
            source: ScanSource::Memory(projection),
            plan,
            residual,
            ..
        } = &scan.kind
        else {
            return Ok(None);
        };
        if plan.filter.is_some()
            || plan.residual_predicate.is_some()
            || plan.source_order_limit.is_some()
            || residual.is_some()
            || projection.source_rows() > 65_536
        {
            return Ok(None);
        }
        OutputCreditWindow::new(projection.source_rows(), self.session.memory()).map(Some)
    }
}

#[cfg(test)]
#[path = "local_primitive_relational_credit_window_tests.rs"]
mod tests;
