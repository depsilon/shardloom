//! Exact row selectors. Future-dependent policies retain at most one row per key.

use super::{
    BATCH_ROWS, BoundUnary, NativeBatch, NativeExecutionContext, ReservedVec, Result, UnaryOutput,
    Value, VortexDuplicateKeepPolicy as Keep, VortexQueryPrimitiveKind as Kind, failed,
    memory::KeyIndex, values::OwnedRow,
};
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool, MemoryLease};

struct Entry {
    first: usize,
    last: usize,
    count: usize,
    row: Option<OwnedRow>,
    _key: MemoryLease,
}

struct Keys {
    // Drop key strings before their payload credits in the entries.
    index: KeyIndex,
    entries: ReservedVec<Entry>,
}

impl Keys {
    fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            index: KeyIndex::new(memory)?,
            entries: ReservedVec::new(memory)?,
        })
    }

    fn observe(&mut self, key: Budgeted<String>, ordinal: usize) -> Result<(usize, bool)> {
        if let Some(&index) = self.index.values.get(key.value()) {
            let entry = &mut self.entries.values[index];
            entry.last = ordinal;
            entry.count = entry
                .count
                .checked_add(1)
                .ok_or_else(|| failed("key count overflow"))?;
            return Ok((index, false));
        }
        self.index.reserve_one()?;
        self.entries.reserve_one()?;
        let index = self.entries.values.len();
        let (key, lease) = key.into_parts();
        self.index.values.insert(key, index);
        self.entries.values.push(Entry {
            first: ordinal,
            last: ordinal,
            count: 1,
            row: None,
            _key: lease,
        });
        Ok((index, true))
    }
}

pub(super) enum State {
    Select(Selector),
    Tail(Tail),
    Sample(super::sample::Sample),
    Rolling(super::rolling::Rolling),
    Expression(super::expression::Expression),
    Melt(super::melt::Melt),
    Explode(super::explode::Explode),
    Pivot(super::pivot::Pivot),
}

impl State {
    pub(super) fn usage(&self) -> super::report::StateUsage {
        match self {
            Self::Select(state) => super::report::StateUsage {
                items: state
                    .keys
                    .entries
                    .values
                    .len()
                    .saturating_add(state.identities.values.len()),
                all_input_retained: state.ordinal > 0
                    && state
                        .keys
                        .entries
                        .values
                        .iter()
                        .filter(|entry| entry.row.is_some())
                        .count()
                        == state.ordinal,
            },
            Self::Sample(state) => state.usage(),
            Self::Tail(state) => super::report::StateUsage {
                items: state.rows.values.len(),
                all_input_retained: state.seen > 0 && state.seen == state.rows.values.len(),
            },
            Self::Rolling(state) => state.usage(),
            Self::Expression(state) => state.usage(),
            Self::Melt(_) | Self::Explode(_) => super::report::StateUsage::default(),
            Self::Pivot(state) => state.usage(),
        }
    }
    pub(super) fn new(
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        rows: Option<u64>,
        suffix_selected: bool,
    ) -> Result<Self> {
        if plan.request.kind == Kind::TailRows && !suffix_selected {
            return Ok(Self::Tail(Tail {
                rows: ReservedVec::new(context.memory())?,
                next: 0,
                seen: 0,
            }));
        }
        if plan.request.kind == Kind::SampleRows {
            return super::sample::Sample::new(plan, context, rows).map(Self::Sample);
        }
        if plan.request.kind == Kind::RollingWindowRows {
            return super::rolling::Rolling::new(plan, context, rows).map(Self::Rolling);
        }
        if plan.request.kind == Kind::ExpressionProjectRows {
            return super::expression::Expression::new(plan, context).map(Self::Expression);
        }
        if plan.request.kind == Kind::MeltRows {
            return Ok(Self::Melt(super::melt::Melt::default()));
        }
        if plan.request.kind == Kind::ExplodeRows {
            return Ok(Self::Explode(super::explode::Explode::default()));
        }
        if plan.request.kind == Kind::PivotRows {
            return super::pivot::Pivot::new(context).map(Self::Pivot);
        }
        Ok(Self::Select(Selector {
            keys: Keys::new(context.memory())?,
            ordinal: 0,
            visited: 0,
            source_rows: rows
                .map(usize::try_from)
                .transpose()
                .map_err(super::vortex_error)?,
            identities: ReservedVec::new(context.memory())?,
        }))
    }

    pub(super) fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<bool> {
        match self {
            Self::Select(state) => state.consume(plan, batch, rows, context, output),
            Self::Tail(state) => state.consume(plan, batch, rows, context),
            Self::Rolling(state) => state.consume(plan, batch, rows, context, output),
            Self::Expression(state) => state.consume(plan, batch, rows, context, output),
            Self::Melt(state) => state.consume(plan, batch, rows, context, output),
            Self::Explode(state) => state.consume(plan, batch, rows, context, output),
            Self::Pivot(state) => {
                state.consume(plan, batch, rows, context)?;
                Ok(false)
            }
            Self::Sample(state) => {
                state.consume(plan, batch, rows, context, output.payload.is_some())?;
                Ok(false)
            }
        }
    }

    pub(super) fn finish(
        self,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<usize> {
        match self {
            Self::Select(state) => state.finish(plan, context, output),
            Self::Tail(state) => state.finish(context, output),
            Self::Sample(state) => state.finish(plan, context, output),
            Self::Rolling(state) => state.finish(plan, context, output),
            Self::Expression(state) => state.finish(),
            Self::Melt(state) => Ok(state.produced),
            Self::Explode(state) => Ok(state.produced),
            Self::Pivot(_) => Err(failed(
                "pivot requires its discovered schema before delivery",
            )),
        }
    }
}

/// A produced relation has no file suffix range. Retain exactly the requested
/// suffix, with each payload and vector capacity charged to the same context.
pub(super) struct Tail {
    rows: ReservedVec<OwnedRow>,
    next: usize,
    seen: usize,
}

impl Tail {
    fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        let limit = plan
            .request
            .source_order_limit
            .ok_or_else(|| failed("tail limit is absent"))?;
        self.seen = self
            .seen
            .checked_add(rows)
            .ok_or_else(|| failed("tail row count overflow"))?;
        for row in 0..rows {
            if row % 256 == 0 {
                context.check_cancelled()?;
            }
            if self.rows.values.len() < limit {
                self.rows.reserve_one()?;
                self.rows.values.push(batch.row(&plan.output_indices, row)?);
            } else {
                self.rows.values[self.next] = batch.row(&plan.output_indices, row)?;
                self.next = (self.next + 1) % limit;
            }
        }
        Ok(false)
    }

    fn finish(
        self,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<usize> {
        let count = self.rows.values.len();
        for start in (0..count).step_by(BATCH_ROWS) {
            context.check_cancelled()?;
            output.emit((count - start).min(BATCH_ROWS), |row, column| {
                let index = (self.next + start + row) % count;
                super::values::borrowed(&self.rows.values[index].values()[column])
            })?;
        }
        Ok(self.seen)
    }
}

pub(super) struct Selector {
    keys: Keys,
    ordinal: usize,
    visited: usize,
    source_rows: Option<usize>,
    identities: ReservedVec<(usize, usize)>,
}

impl Selector {
    fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<bool> {
        let kind = plan.request.kind;
        let keep = plan.request.duplicate_keep;
        let limit = plan.request.source_order_limit.unwrap_or(usize::MAX);
        self.visited = self
            .visited
            .checked_add(rows)
            .ok_or_else(|| failed("visited row count overflow"))?;
        if kind == Kind::TailRows {
            output.emit(rows, |row, column| {
                batch.value(plan.output_indices[column], row)
            })?;
            return Ok(false);
        }
        let mut selected = ReservedVec::new(context.memory())?;
        let mut mask = ReservedVec::new(context.memory())?;
        let mut complete = false;
        for row in 0..rows {
            if row % 256 == 0 {
                context.check_cancelled()?;
            }
            if let Some(predicate) = &plan.predicate
                && !predicate.matches_with(&mut |column| batch.stat(column, row))?
            {
                continue;
            }
            let (index, first) = self
                .keys
                .observe(batch.key(&plan.key_indices, row)?, self.ordinal)?;
            match kind {
                Kind::DistinctRows => {
                    if first {
                        selected.push(row)?;
                    }
                    if output.rows.saturating_add(selected.values.len()) >= limit {
                        complete = true;
                    }
                }
                Kind::DropDuplicateRows => match keep {
                    Keep::First => {
                        if first && output.rows.saturating_add(selected.values.len()) < limit {
                            selected.push(row)?;
                        }
                        // Later rows cannot change the first selected occurrence.
                        complete = output.rows.saturating_add(selected.values.len()) >= limit;
                    }
                    Keep::Last => {
                        if output.payload.is_some() {
                            self.keys.entries.values[index].row =
                                Some(batch.row(&plan.output_indices, row)?);
                        }
                    }
                    Keep::AllDuplicates => {
                        if first && output.payload.is_some() {
                            self.keys.entries.values[index].row =
                                Some(batch.row(&plan.output_indices, row)?);
                        } else {
                            self.keys.entries.values[index].row = None;
                        }
                    }
                },
                Kind::DuplicateMaskRows if keep == Keep::First => {
                    mask.push(!first)?;
                    if output.rows.saturating_add(mask.values.len()) >= limit {
                        complete = true;
                    }
                }
                Kind::DuplicateMaskRows => {
                    if self.identities.values.len() < limit {
                        self.identities.push((self.ordinal, index))?;
                    }
                }
                _ => return Err(failed("row selector kind is not admitted")),
            }
            self.ordinal = self
                .ordinal
                .checked_add(1)
                .ok_or_else(|| failed("row ordinal overflow"))?;
            if complete {
                break;
            }
        }
        if kind == Kind::DuplicateMaskRows {
            output.emit(mask.values.len(), |row, _| {
                Ok(Value::Bool(mask.values[row]))
            })?;
        } else {
            output.emit(selected.values.len(), |row, column| {
                batch.value(plan.output_indices[column], selected.values[row])
            })?;
        }
        Ok(complete)
    }

    fn finish(
        self,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<usize> {
        let keep = plan.request.duplicate_keep;
        let limit = plan.request.source_order_limit.unwrap_or(usize::MAX);
        match plan.request.kind {
            Kind::DistinctRows => Ok(self.keys.entries.values.len()),
            Kind::TailRows => self
                .source_rows
                .ok_or_else(|| failed("file tail source count is absent")),
            Kind::DuplicateMaskRows => {
                for batch in self.identities.values.chunks(BATCH_ROWS) {
                    context.check_cancelled()?;
                    output.emit(batch.len(), |row, _| {
                        let (ordinal, index) = batch[row];
                        let entry = &self.keys.entries.values[index];
                        Ok(Value::Bool(if keep == Keep::Last {
                            entry.last != ordinal
                        } else {
                            entry.count > 1
                        }))
                    })?;
                }
                Ok(self.visited)
            }
            Kind::DropDuplicateRows if keep == Keep::First => Ok(self.keys.entries.values.len()),
            Kind::DropDuplicateRows => {
                let mut retained = ReservedVec::new(context.memory())?;
                for (index, entry) in self.keys.entries.values.iter().enumerate() {
                    if keep != Keep::AllDuplicates || entry.count == 1 {
                        retained.push(index)?;
                    }
                }
                retained.values.sort_unstable_by_key(|&index| {
                    let entry = &self.keys.entries.values[index];
                    if keep == Keep::Last {
                        entry.last
                    } else {
                        entry.first
                    }
                });
                let pre_limit = retained.values.len();
                for batch in retained.values[..pre_limit.min(limit)].chunks(BATCH_ROWS) {
                    context.check_cancelled()?;
                    output.emit(batch.len(), |row, column| {
                        let values = self.keys.entries.values[batch[row]]
                            .row
                            .as_ref()
                            .ok_or_else(|| failed("selected retained row is absent"))?;
                        super::values::borrowed(&values.values()[column])
                    })?;
                }
                Ok(pre_limit)
            }
            _ => Err(failed("selector finalization is not admitted")),
        }
    }
}
