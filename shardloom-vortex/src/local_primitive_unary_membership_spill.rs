//! Exact online unary observation with bounded native replacement state.

use super::{
    BoundUnary, NativeBatch, NativeExecutionContext, ReservedVec, Result, UnaryOutput, Value,
    VortexDuplicateKeepPolicy as Keep, VortexQueryPrimitiveKind as Kind, failed,
    report::StateUsage, values::OwnedRow,
};
use crate::local_primitives::{
    completed_result::CompletedRows,
    native_relational_record_output::Results,
    native_relational_spill::{
        State,
        replacement::{self, Key},
    },
};
use shardloom_exec::live_memory::{Budgeted, MemoryLease};
use std::collections::BTreeMap;
use vortex::array::ArrayRef;

#[path = "local_primitive_unary_membership_records.rs"]
mod records;
use records::{Identities, Schema};

const BLOCK_ROWS: usize = 256;
// A conservative node/entry credit, separate from the owned exact key bytes.
const ENTRY_BYTES: u64 = 512;

#[derive(Default)]
pub(in crate::local_primitives) struct Report {
    pub(in crate::local_primitives) input_rows: u64,
    pub(in crate::local_primitives) observed_rows: u64,
    pub(in crate::local_primitives) groups: u64,
    pub(in crate::local_primitives) identity_rows: u64,
    pub(in crate::local_primitives) output_rows: u64,
    pub(in crate::local_primitives) lookup_blocks: u64,
    pub(in crate::local_primitives) reader_opens: u64,
}

#[derive(Clone, Copy)]
struct Counts {
    first: u64,
    last: u64,
    count: u64,
}

struct Entry {
    counts: Counts,
    row: Option<OwnedRow>,
    key_credit: MemoryLease,
}

impl Entry {
    fn weight(&self) -> Result<u64> {
        self.key_credit
            .bytes()
            .checked_add(
                self.row
                    .as_ref()
                    .map(OwnedRow::retained_bytes)
                    .transpose()?
                    .unwrap_or(0),
            )
            .ok_or_else(|| failed("membership buffer weight overflow"))
    }
}

struct Builder<'p, 's> {
    schema: &'p Schema,
    store: replacement::Store<'p, 's>,
    buffer: BTreeMap<Key, Entry>,
    weight: u64,
    block_rows: usize,
    groups: u64,
    max_items: usize,
    flushed: bool,
}

impl<'p, 's> Builder<'p, 's> {
    fn new(
        schema: &'p Schema,
        state: &'s State,
        block_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            store: replacement::Store::new(&schema.state, state, block_rows, context)?,
            buffer: BTreeMap::new(),
            weight: 0,
            block_rows,
            groups: 0,
            max_items: 0,
            flushed: false,
        })
    }

    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(self.buffer.len())?;
        rows.values.extend(self.buffer.iter());
        let first = rows
            .values
            .first()
            .ok_or_else(|| failed("membership first key is absent"))?
            .0;
        let last = rows
            .values
            .last()
            .ok_or_else(|| failed("membership last key is absent"))?
            .0;
        let blocks = rows
            .values
            .chunks(self.block_rows)
            .map(|rows| self.schema.build(rows, context));
        self.store.append(
            rows.values.len(),
            first,
            last,
            Some((first, last)),
            blocks,
            context,
        )?;
        drop(rows);
        self.buffer.clear();
        self.weight = 0;
        self.flushed = true;
        self.store.compact(context)
    }

    fn insert(
        &mut self,
        key: Key,
        entry: Entry,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let weight = entry.weight()?;
        if weight > self.store.buffer_bytes().saturating_sub(self.weight) {
            self.flush(context)?;
        }
        self.weight = self
            .weight
            .checked_add(weight)
            .ok_or_else(|| failed("membership buffer weight overflow"))?;
        self.buffer.insert(key, entry);
        self.max_items = self.max_items.max(self.buffer.len());
        // The byte threshold shapes work; one large admitted record is not an
        // input-size error. Native array construction and I/O still reserve.
        if self.weight >= self.store.buffer_bytes() {
            self.flush(context)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn observe(
        &mut self,
        plan: &BoundUnary,
        key: Budgeted<String>,
        ordinal: u64,
        batch: &mut NativeBatch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        let (index, mut credit) = key.into_parts();
        let key = Key {
            index,
            kind: 0,
            domain: String::new(),
        };
        if prefix(plan)
            && (self.buffer.contains_key(&key) || self.store.find(&key, context)?.is_some())
        {
            return Ok(false);
        }
        let (key, mut entry, first) = if let Some((stored, entry)) = self.buffer.remove_entry(&key)
        {
            self.weight = self
                .weight
                .checked_sub(entry.weight()?)
                .ok_or_else(|| failed("membership buffer accounting underflow"))?;
            drop((key, credit));
            (stored, entry, false)
        } else {
            let previous = if prefix(plan) {
                None
            } else {
                self.store
                    .find(&key, context)?
                    .map(|row| self.schema.counts(&row, context))
                    .transpose()?
            };
            credit.resize(
                credit
                    .bytes()
                    .checked_add(ENTRY_BYTES)
                    .ok_or_else(|| failed("membership entry credit overflow"))?,
            )?;
            let first = previous.is_none();
            let counts = previous.unwrap_or(Counts {
                first: ordinal,
                last: ordinal,
                count: 0,
            });
            (
                key,
                Entry {
                    counts,
                    row: None,
                    key_credit: credit,
                },
                first,
            )
        };
        entry.counts.last = ordinal;
        entry.counts.count = entry
            .counts
            .count
            .checked_add(1)
            .ok_or_else(|| failed("membership key count overflow"))?;
        if plan.request.kind == Kind::DropDuplicateRows {
            match plan.request.duplicate_keep {
                Keep::Last => entry.row = Some(batch.row(&plan.output_indices, row, context)?),
                Keep::AllDuplicates if first => {
                    entry.row = Some(batch.row(&plan.output_indices, row, context)?);
                }
                Keep::AllDuplicates => entry.row = None,
                Keep::First => {}
            }
        }
        self.insert(key, entry, context)?;
        if first {
            add(&mut self.groups, 1)?;
        }
        Ok(first)
    }

    fn find(&mut self, key: &Key, context: &NativeExecutionContext<'_>) -> Result<Counts> {
        if let Some(entry) = self.buffer.get(key) {
            return Ok(entry.counts);
        }
        self.store
            .find(key, context)?
            .map(|row| self.schema.counts(&row, context))
            .transpose()?
            .ok_or_else(|| failed("duplicate mask identity has no final membership state"))
    }

    fn usage(&self, observed: u64) -> StateUsage {
        StateUsage {
            items: self.max_items,
            all_input_retained: observed != 0
                && !self.flushed
                && self
                    .buffer
                    .values()
                    .filter(|entry| entry.row.is_some())
                    .count() as u64
                    == observed,
        }
    }

    fn finish_rows(
        mut self,
        plan: &BoundUnary,
        state: &State,
        context: &NativeExecutionContext<'_>,
        results: &mut Results<'_>,
    ) -> Result<(u64, u64)> {
        if self.store.has_runs() {
            self.flush(context)?;
            let (run, mut blocks, mut opens) = self.store.complete(context)?;
            if let Some(run) = run {
                {
                    let mut access = run.open(state, &self.schema.state, context)?;
                    for position in 0..access.rows() {
                        context.check_cancelled()?;
                        let row = access.row(position, context)?;
                        self.schema.push_stored(&row, plan, context, results)?;
                    }
                    access.validate()?;
                    add(&mut blocks, access.blocks())?;
                    add(&mut opens, 1)?;
                }
                run.finish(state, context)?;
            }
            Ok((blocks, opens))
        } else {
            for (_, entry) in self.buffer {
                context.check_cancelled()?;
                self.schema.push_entry(&entry, plan, context, results)?;
            }
            Ok(self.store.observations())
        }
    }
}

fn prefix(plan: &BoundUnary) -> bool {
    plan.request.kind == Kind::DistinctRows || plan.request.duplicate_keep == Keep::First
}

fn add(value: &mut u64, count: u64) -> Result<()> {
    *value = value
        .checked_add(count)
        .ok_or_else(|| failed("unary membership counter overflow"))?;
    Ok(())
}

pub(super) fn admitted(kind: Kind) -> bool {
    matches!(
        kind,
        Kind::DistinctRows | Kind::DropDuplicateRows | Kind::DuplicateMaskRows
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // Keep one producer pass and its policy-specific finalization under the same owner.
pub(super) fn run(
    plan: &BoundUnary,
    state: &State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    produce: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<(StateUsage, Report)> {
    if !admitted(plan.request.kind) || batch_rows == 0 {
        return Err(failed(
            "membership spill requires an admitted selector and positive batch bound",
        ));
    }
    let block_rows = batch_rows.min(BLOCK_ROWS);
    let schema = Schema::new(plan, context)?;
    let mut builder = Builder::new(&schema, state, block_rows, context)?;
    let mut identities = if plan.request.kind == Kind::DuplicateMaskRows && !prefix(plan) {
        Some(Identities::new(
            &schema.identities,
            state,
            block_rows,
            context,
        )?)
    } else {
        None
    };
    let payload = CompletedRows::streaming_native(
        plan.fields.clone(),
        context.memory(),
        batch_rows,
        context.cancellation().clone(),
        consume,
    )?;
    let mut output = UnaryOutput {
        columns: &plan.output_columns,
        rows: 0,
        payload: Some(payload),
    };
    let mut report = Report::default();
    let limit = plan.request.source_order_limit.unwrap_or(usize::MAX);
    let mut stopped = false;
    produce(&mut |array| {
        context.check_cancelled()?;
        add(&mut report.input_rows, array.len() as u64)?;
        for start in (0..array.len()).step_by(block_rows) {
            if stopped {
                break;
            }
            let end = start.saturating_add(block_rows).min(array.len());
            let array = array.slice(start..end).map_err(super::vortex_error)?;
            let mut batch = NativeBatch::new(&array, &plan.columns, context)?;
            let mut selected = ReservedVec::new(context.memory())?;
            let mut masks = ReservedVec::new(context.memory())?;
            for row in 0..array.len() {
                context.check_cancelled()?;
                if let Some(predicate) = &plan.predicate
                    && !predicate.matches_with(&mut |column| batch.stat(column, row))?
                {
                    continue;
                }
                let ordinal = report.observed_rows;
                let key = batch.key(&plan.key_indices, row)?;
                if let Some(identities) = &mut identities
                    && ordinal < limit as u64
                {
                    identities.push(key.value(), ordinal, context)?;
                    add(&mut report.identity_rows, 1)?;
                }
                let first = builder.observe(plan, key, ordinal, &mut batch, row, context)?;
                add(&mut report.observed_rows, 1)?;
                if prefix(plan) {
                    if plan.request.kind == Kind::DuplicateMaskRows {
                        masks.push(!first)?;
                    } else if first {
                        selected.push(row)?;
                    }
                    stopped = output
                        .rows
                        .saturating_add(selected.values.len())
                        .saturating_add(masks.values.len())
                        >= limit;
                    if stopped {
                        break;
                    }
                }
            }
            if plan.request.kind == Kind::DuplicateMaskRows {
                output.emit(masks.values.len(), |row, _| {
                    Ok(Value::Bool(masks.values[row]))
                })?;
            } else {
                output.emit_selected(plan, &mut batch, selected.values.len(), context, |row| {
                    selected.values[row]
                })?;
            }
        }
        context.check_cancelled()
    })?;
    report.groups = builder.groups;
    let mut usage = builder.usage(report.observed_rows);
    if let Some(identities) = identities {
        usage.items = usage.items.saturating_add(identities.max_items());
        identities.finish(
            &schema,
            &mut builder,
            plan.request.duplicate_keep,
            context,
            &mut output,
        )?;
        (report.lookup_blocks, report.reader_opens) = builder.store.observations();
    } else if !prefix(plan) {
        let mut results = Results::new(&schema.output, state, block_rows, context)?;
        (report.lookup_blocks, report.reader_opens) =
            builder.finish_rows(plan, state, context, &mut results)?;
        results.finish(context, block_rows, &mut |array| {
            let rows = array.len().min(limit.saturating_sub(output.rows));
            output.emit_native(rows, context, || {
                schema.deliver(&array, rows, plan, context)
            })
        })?;
    } else {
        (report.lookup_blocks, report.reader_opens) = builder.store.observations();
    }
    output.finish(context)?;
    report.output_rows = output.rows as u64;
    context.check_cancelled()?;
    Ok((usage, report))
}
