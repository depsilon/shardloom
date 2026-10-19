//! One credited positional block for exact replacement draws.

use super::{
    NativeBatch, NativeExecutionContext, Result, Schema, Value, add, failed, values::OwnedRow,
};
use crate::local_primitives::native_relational_spill::{StoredBlock, StoredOrder};

struct Cached {
    start: u64,
    batch: NativeBatch,
    _block: StoredBlock,
}

pub(super) struct Access<'p, 's> {
    population: StoredOrder<'s>,
    schema: &'p Schema,
    cached: Option<Cached>,
    blocks: u64,
}

impl<'p, 's> Access<'p, 's> {
    pub(super) fn new(population: StoredOrder<'s>, schema: &'p Schema) -> Self {
        Self {
            population,
            schema,
            cached: None,
            blocks: 0,
        }
    }

    fn at(
        &mut self,
        position: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<(&mut NativeBatch, usize)> {
        context.check_cancelled()?;
        if position >= self.population.rows() {
            return Err(failed("sampling draw position is outside its population"));
        }
        let width = self.population.block_rows() as u64;
        let start = position / width * width;
        #[cfg(test)]
        if self
            .cached
            .as_ref()
            .is_some_and(|cached| cached.start == start)
            && let Some(path) = self.population.run_path()
            && let Some(hook) =
                super::BEFORE_POPULATION_CACHE_HIT.with(|slot| slot.borrow_mut().take())
        {
            hook(path);
        }
        // A cached block is not authority to continue reading a changed run.
        self.population.validate()?;
        if self
            .cached
            .as_ref()
            .is_none_or(|cached| cached.start != start)
        {
            self.cached = None;
            let block = self
                .population
                .read_block_at(start, context)?
                .ok_or_else(|| failed("sampling population block is absent"))?;
            let batch = NativeBatch::new(block.array(), &self.schema.names, context)?;
            self.cached = Some(Cached {
                start,
                batch,
                _block: block,
            });
            add(&mut self.blocks, 1)?;
        }
        let cached = self
            .cached
            .as_mut()
            .ok_or_else(|| failed("sampling population cache is absent"))?;
        let offset = usize::try_from(position - start).map_err(super::super::vortex_error)?;
        if !matches!(cached.batch.value(0, offset)?, Value::UInt(ordinal) if ordinal == position) {
            return Err(failed("sampling population ordinal changed"));
        }
        Ok((&mut cached.batch, offset))
    }

    pub(super) fn weighted_index(
        &mut self,
        threshold: f64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<u64> {
        let mut low = 0;
        let mut high = self.population.rows();
        while low < high {
            let middle = low + (high - low) / 2;
            let (batch, offset) = self.at(middle, context)?;
            let Value::UInt(bits) = batch.value(super::records::CUMULATIVE, offset)? else {
                return Err(failed("sampling cumulative weight changed type"));
            };
            let cumulative = f64::from_bits(bits);
            if !cumulative.is_finite() || cumulative <= 0.0 {
                return Err(failed("sampling cumulative weight is invalid"));
            }
            if cumulative < threshold {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        self.population
            .rows()
            .checked_sub(1)
            .map(|last| low.min(last))
            .ok_or_else(|| failed("sampling draw requires a nonempty population"))
    }

    pub(super) fn row(
        &mut self,
        position: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<OwnedRow> {
        let columns = &self.schema.payload_indices;
        let (batch, offset) = self.at(position, context)?;
        batch.row(columns, offset, context)
    }

    pub(super) fn finish(mut self, context: &NativeExecutionContext<'_>) -> Result<u64> {
        self.cached = None;
        self.population.finish(context)?;
        Ok(self.blocks)
    }
}
