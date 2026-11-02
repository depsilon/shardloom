//! Applied row geometry at compatibility decoder continuation boundaries.
//! Held chunks, dictionaries and stripe streams retain their separate owners.

use super::{Result, failed};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::{arrow::ProjectionMask, file::metadata::RowGroupMetaData};
use shardloom_exec::{
    adaptive_work::{AdaptiveWorkReport, AdaptiveWorkSizer, WorkCost, WorkLimits, WorkShape},
    live_memory::LiveMemoryPool,
};

pub(super) const MAX_ROWS: usize = 8192;

#[derive(Default)]
pub(super) struct Sizing(AdaptiveWorkSizer);

impl Sizing {
    pub(super) fn select(
        &self,
        memory: &LiveMemoryPool,
        cost: WorkCost,
        remaining: u64,
        maximum_rows: usize,
    ) -> Result<Option<WorkShape>> {
        // A known empty extent still follows the provider's EOF/validation
        // path. It does not need or certify an invented positive work unit.
        if remaining == 0 {
            return Ok(None);
        }
        self.0
            .select(
                "columnar_decode",
                memory.snapshot(),
                cost,
                WorkLimits {
                    min_units: 1,
                    max_units: remaining.min(maximum_rows as u64),
                    max_in_flight: 1,
                },
            )
            .map(Some)
    }

    pub(super) fn complete(&mut self, selected: WorkShape, batch: &RecordBatch) {
        // Value spans guide future row geometry; retained page/dictionary
        // allocations and fixed array headers must not poison a short tail's
        // per-row observation. This observes no decoded values or predicates.
        let observed = super::super::work_sizing::observed_row_bytes(batch)
            .map(|bytes| bytes.saturating_mul(4).saturating_add(16));
        self.0.complete(selected, observed);
    }

    pub(super) fn report(&self) -> AdaptiveWorkReport {
        self.0.report()
    }
}

/// Additional decode, alignment and native-intake overlap. This is a sizing
/// estimate, not a new memory grant or an allocator/RSS bound. The existing
/// provider workspace and returned-buffer reservations remain authoritative.
pub(super) fn cost(
    physical: &Schema,
    logical: &Schema,
    partitions: &[(String, Option<String>)],
) -> Result<WorkCost> {
    let mut cost = super::super::work_sizing::schema_cost(physical, 8192, 2)?;
    let target = alignment_cost(logical, partitions)?;
    cost.fixed_bytes = cost
        .fixed_bytes
        .checked_add(target.fixed_bytes)
        .ok_or_else(|| failed("decoder batch metadata capacity overflow"))?;
    cost.bytes_per_unit = cost
        .bytes_per_unit
        .checked_add(target.bytes_per_unit)
        .ok_or_else(|| failed("decoder batch row capacity overflow"))?;
    Ok(cost)
}

pub(super) fn alignment_cost(
    logical: &Schema,
    partitions: &[(String, Option<String>)],
) -> Result<WorkCost> {
    let mut cost = super::super::work_sizing::schema_cost(logical, 8192, 2)?;
    cost.fixed_bytes = cost
        .fixed_bytes
        .checked_add(
            (logical.fields().len() as u64)
                .checked_mul(4096)
                .ok_or_else(|| failed("alignment schema width overflow"))?,
        )
        .ok_or_else(|| failed("alignment metadata capacity overflow"))?;
    for field in logical.fields() {
        if let Some((_, Some(value))) = partitions.iter().find(|(name, _)| name == field.name()) {
            // Hive constants are known values, not average string-width guesses.
            cost.bytes_per_unit = cost
                .bytes_per_unit
                .checked_add(
                    (value.len() as u64)
                        .checked_mul(2)
                        .ok_or_else(|| failed("partition width overflow"))?,
                )
                .ok_or_else(|| failed("decoder batch row capacity overflow"))?;
        }
    }
    Ok(cost)
}

/// Preserve the admitted Parquet page/decode envelope independently of the
/// next output-row target. Compressed buffers keep their own I/O leases.
pub(super) fn parquet_workspace(group: &RowGroupMetaData, mask: &ProjectionMask) -> Result<u64> {
    group
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| mask.leaf_included(*i))
        .try_fold(8192_u64, |sum, (_, column)| {
            let size = u64::try_from(column.uncompressed_size()).map_err(failed)?;
            sum.checked_add(size)
                .ok_or_else(|| failed("decoder workspace overflow"))
        })
}
