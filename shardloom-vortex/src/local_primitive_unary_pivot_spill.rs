//! Online sparse pivot updates under the existing relational spill policy.

use super::{
    BoundUnary, Datum, MemoryLease, NativeBatch, NativeExecutionContext, Plan, Result, SpillReport,
    VortexPivotProjectionRequest, bytes,
    cells::{Cell, Kind, Margin},
    failed,
};
use crate::local_primitives::{
    PivotValue, ensure_pivot_output_column_name,
    native_capacity::ReservedVec,
    native_relational_spill::{
        State,
        pivot::{self, Key},
    },
    pivot_first_is_new,
};
use std::collections::BTreeMap;
use vortex::array::{ArrayRef, dtype::DType};

#[path = "local_primitive_unary_pivot_records.rs"]
mod records;
use records::Schema;
#[path = "local_primitive_unary_pivot_spill_output.rs"]
mod output;

#[derive(Clone)]
enum StoredValue {
    Index(Datum),
    First(Datum),
    Aggregate(Cell),
}

#[derive(Clone)]
struct Entry {
    ordinal: u64,
    value: StoredValue,
}

impl Entry {
    fn estimate(&self, key: &Key) -> Result<(u64, u64)> {
        let charge = bytes(
            512,
            key.index
                .capacity()
                .checked_add(key.domain.capacity())
                .ok_or_else(|| failed("pivot key capacity overflow"))?,
        )?;
        let datum = match &self.value {
            StoredValue::Index(value) | StoredValue::First(value) => Some(value),
            StoredValue::Aggregate(Cell::Nested(value)) => value.as_ref(),
            StoredValue::Aggregate(_) => None,
        };
        let payload = match datum {
            Some(Datum::Nested { array, .. }) => array.nbytes(),
            Some(value) => value.scalar_bytes() as u64,
            None => 0,
        };
        let weight = charge
            .checked_add(payload)
            .ok_or_else(|| failed("pivot buffer capacity overflow"))?;
        Ok((charge, weight))
    }
}

struct Builder<'p, 's> {
    schema: &'p Schema,
    store: pivot::Store<'p, 's>,
    buffer: BTreeMap<Key, Entry>,
    buffer_credit: MemoryLease,
    buffer_weight: u64,
    domains: BTreeMap<String, String>,
    domain_credit: MemoryLease,
    scratch: MemoryLease,
    input_rows: u64,
    index_count: usize,
    max_items: usize,
}

impl<'p, 's> Builder<'p, 's> {
    fn new(
        schema: &'p Schema,
        state: &'s State,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            store: pivot::Store::new(&schema.spec, state, context)?,
            buffer: BTreeMap::new(),
            buffer_credit: context.memory().reserve(0)?,
            buffer_weight: 0,
            domains: BTreeMap::new(),
            domain_credit: context.memory().reserve(0)?,
            scratch: context.memory().reserve(65_536)?,
            input_rows: 0,
            index_count: 0,
            max_items: 0,
        })
    }

    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut values = ReservedVec::new(context.memory())?;
        values.reserve(self.buffer.len())?;
        values.values.extend(self.buffer.iter());
        let first = values
            .values
            .first()
            .ok_or_else(|| failed("pivot flush lost its first key"))?
            .0;
        let last = values
            .values
            .last()
            .ok_or_else(|| failed("pivot flush lost its last key"))?
            .0;
        let mut indices = values
            .values
            .iter()
            .map(|(key, _)| *key)
            .filter(|key| key.kind == 0);
        let index_bounds = indices
            .next()
            .map(|first| (first, indices.next_back().unwrap_or(first)));
        let blocks = values
            .values
            .chunks(pivot::BLOCK_ROWS)
            .map(|rows| self.schema.build(rows, context));
        self.store.append(
            values.values.len(),
            first,
            last,
            index_bounds,
            blocks,
            context,
        )?;
        drop(values);
        self.buffer.clear();
        self.buffer_credit.resize(0)?;
        self.buffer_weight = 0;
        // Release buffered payloads before admitting merge input/output overlap.
        self.store.compact(context)
    }

    fn insert(
        &mut self,
        key: Key,
        value: Entry,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let (charge, weight) = value.estimate(&key)?;
        if weight > self.store.buffer_bytes() {
            return Err(failed(
                "one sparse pivot record exceeds the configured spill buffer threshold",
            ));
        }
        let (mut old_charge, mut old_weight) = self
            .buffer
            .get_key_value(&key)
            .map(|(key, value)| value.estimate(key))
            .transpose()?
            .unwrap_or((0, 0));
        if weight
            > self
                .store
                .buffer_bytes()
                .saturating_sub(self.buffer_weight - old_weight)
        {
            self.flush(context)?;
            old_charge = 0;
            old_weight = 0;
        }
        let bytes = self
            .buffer_credit
            .bytes()
            .checked_sub(old_charge)
            .and_then(|bytes| bytes.checked_add(charge))
            .ok_or_else(|| failed("pivot buffer credit overflow"))?;
        self.buffer_credit.resize(bytes)?;
        self.buffer.insert(key, value);
        self.buffer_weight = self
            .buffer_weight
            .checked_sub(old_weight)
            .and_then(|bytes| bytes.checked_add(weight))
            .ok_or_else(|| failed("pivot buffer weight overflow"))?;
        self.max_items = self
            .max_items
            .max(self.buffer.len().saturating_add(self.domains.len()));
        Ok(())
    }

    fn cell(&mut self, key: &Key, context: &NativeExecutionContext<'_>) -> Result<Option<Entry>> {
        if let Some(value) = self.buffer.get(key) {
            return Ok(Some(value.clone()));
        }
        self.store
            .find(key, context)?
            .map(|row| self.schema.read(&row, context))
            .transpose()
    }

    fn consume(
        &mut self,
        bound: &BoundUnary,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let plan = bound
            .pivot
            .as_ref()
            .ok_or_else(|| failed("pivot is not bound"))?;
        let projection = crate::local_primitives::required_pivot_projection(&bound.request)?;
        let mut batch = NativeBatch::new(array, &bound.columns, context)?;
        let count_value = Datum::null(context.memory())?;
        for row in 0..array.len() {
            context.check_cancelled()?;
            let ordinal = self.input_rows;
            self.input_rows = self
                .input_rows
                .checked_add(1)
                .ok_or_else(|| failed("pivot input ordinal overflow"))?;
            if let Some(predicate) = &bound.predicate
                && !predicate.matches_with(&mut |column| batch.stat(column, row))?
            {
                continue;
            }
            let index = Datum::from_batch(&mut batch, plan.indices[0], row, context)?;
            let pivot = Datum::from_batch(&mut batch, plan.indices[1], row, context)?;
            let value = if plan.aggregate == "count" {
                count_value.clone()
            } else {
                Datum::from_batch(&mut batch, plan.indices[2], row, context)?
            };
            self.update(plan, projection, &index, &pivot, &value, ordinal, context)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn update(
        &mut self,
        plan: &Plan,
        projection: &VortexPivotProjectionRequest,
        index: &Datum,
        pivot: &Datum,
        value: &Datum,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let payload = index
            .key_bytes()?
            .checked_add(pivot.key_bytes()?)
            .and_then(|bytes| bytes.checked_add(value.scalar_bytes()))
            .ok_or_else(|| failed("pivot key size overflow"))?;
        let scratch = bytes(65_536, payload)?;
        if scratch > self.scratch.bytes() {
            self.scratch.resize(scratch)?;
        }
        let index_key = index.pivot_key()?;
        let domain_key = pivot.pivot_key()?;
        Plan::check_domain(&self.domains, &domain_key, pivot, projection)?;
        let marker = Key {
            index: index_key.clone(),
            kind: 0,
            domain: String::new(),
        };
        if !self.buffer.contains_key(&marker) && self.store.find(&marker, context)?.is_none() {
            let value = index.retain(context)?;
            self.insert(
                marker,
                Entry {
                    ordinal,
                    value: StoredValue::Index(value),
                },
                context,
            )?;
            self.index_count = self
                .index_count
                .checked_add(1)
                .ok_or_else(|| failed("pivot index count overflow"))?;
        }
        if !self.domains.contains_key(&domain_key) {
            self.domain_credit.resize(
                self.domain_credit
                    .bytes()
                    .checked_add(bytes(2048, payload)?)
                    .ok_or_else(|| failed("pivot domain capacity overflow"))?,
            )?;
            ensure_pivot_output_column_name(&mut self.domains, &domain_key, pivot)?;
        }
        let key = Key {
            index: index_key,
            kind: 1,
            domain: domain_key,
        };
        let previous = self.cell(&key, context)?;
        let value = if matches!(plan.aggregate.as_str(), "first" | "first_unique") {
            let previous = previous
                .as_ref()
                .map(|entry| match &entry.value {
                    StoredValue::First(value) => Ok(value),
                    _ => Err(failed("pivot first cell changed its private kind")),
                })
                .transpose()?;
            if !pivot_first_is_new(previous, value, &plan.aggregate, projection)? {
                return Ok(());
            }
            StoredValue::First(value.retain(context)?)
        } else {
            let mut cell = match previous {
                Some(Entry {
                    value: StoredValue::Aggregate(cell),
                    ..
                }) => cell,
                None => Kind::new(plan.decimal_source, plan.nested_extrema).empty(),
                _ => return Err(failed("pivot aggregate cell changed its private kind")),
            };
            cell.update(value, &plan.aggregate, context)?;
            StoredValue::Aggregate(cell)
        };
        self.insert(key, Entry { ordinal, value }, context)
    }
}

pub(in crate::local_primitives) struct Completed {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) columns: Vec<String>,
    pub(super) rows: usize,
    pub(super) pre_limit_rows: usize,
    index_rows: usize,
    domains: Vec<String>,
    column_margins: ReservedVec<Margin>,
    grand_margin: Margin,
    run: Option<pivot::Owned>,
    schema: Schema,
    max_items: usize,
    report: SpillReport,
    _metadata: MemoryLease,
}

#[allow(clippy::too_many_lines)] // Preserve domain/schema completion and ordered margin observation together.
pub(super) fn complete(
    bound: &BoundUnary,
    state: &State,
    context: &NativeExecutionContext<'_>,
    produce: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
) -> Result<Completed> {
    let plan = bound
        .pivot
        .as_ref()
        .ok_or_else(|| failed("pivot is not bound"))?;
    let projection = crate::local_primitives::required_pivot_projection(&bound.request)?;
    let schema = Schema::new(plan, context)?;
    let mut builder = Builder::new(&schema, state, context)?;
    produce(&mut |array| builder.consume(bound, &array, context))?;
    context.check_cancelled()?;
    builder.flush(context)?;
    let Builder {
        store,
        domains,
        domain_credit,
        index_count,
        max_items,
        input_rows,
        ..
    } = builder;
    let (run, lookup_blocks, reader_opens) = store.complete(context)?;
    let mut report = SpillReport {
        stages: 1,
        input_rows,
        index_rows: index_count as u64,
        domains: domains.len() as u64,
        cells: run
            .as_ref()
            .map_or(0, pivot::Owned::rows)
            .checked_sub(index_count as u64)
            .ok_or_else(|| failed("pivot index count exceeds its completed records"))?,
        lookup_blocks,
        reader_opens,
    };
    let has_margin = projection.margins && index_count != 0;
    let index_rows = index_count.min(
        bound
            .request
            .source_order_limit
            .unwrap_or(usize::MAX)
            .saturating_sub(usize::from(has_margin)),
    );
    let key_bytes = domains.keys().try_fold(0usize, |bytes, key| {
        bytes
            .checked_add(key.len())
            .ok_or_else(|| failed("pivot domain size overflow"))
    })?;
    let metadata = context.memory().reserve(bytes(65_536, key_bytes)?)?;
    let mut fields = vec![plan.index_field.clone()];
    let mut ordered_domains = Vec::with_capacity(domains.len());
    for (key, name) in &domains {
        ordered_domains.push(key.clone());
        fields.push((name.clone(), plan.cell_dtype.clone()));
    }
    if projection.margins {
        fields.push((
            crate::local_primitives::pivot_output_column_name(&shardloom_core::StatValue::Utf8(
                projection.margins_name.clone(),
            )),
            plan.cell_dtype.clone(),
        ));
    }
    drop(
        crate::local_primitives::completed_result::CompletedRows::new_native(
            fields.clone(),
            context.memory(),
        )?,
    );
    let columns = fields.iter().map(|(name, _)| name.clone()).collect();
    drop((domains, domain_credit));
    let kind = Kind::new(plan.decimal_source, plan.nested_extrema);
    let mut column_margins = ReservedVec::new(context.memory())?;
    let mut grand_margin = Margin::for_kind(kind);
    if has_margin {
        let run = run
            .as_ref()
            .ok_or_else(|| failed("nonempty pivot lost its native state"))?;
        let mut access = run.open(state, &schema.spec, context)?;
        for domain in &ordered_domains {
            let mut margin = Margin::for_kind(kind);
            let mut position = 0;
            for _ in 0..index_rows {
                let index = access
                    .next_index(&mut position, context)?
                    .ok_or_else(|| failed("pivot index prefix is truncated"))?;
                let cell = aggregate_at(&schema, &mut access, index, domain, context)?;
                margin.push_cell(cell.as_ref(), &plan.aggregate)?;
            }
            column_margins.push(margin)?;
        }
        let mut position = 0;
        for _ in 0..index_rows {
            let index = access
                .next_index(&mut position, context)?
                .ok_or_else(|| failed("pivot index prefix is truncated"))?;
            for domain in &ordered_domains {
                let cell = aggregate_at(&schema, &mut access, index, domain, context)?;
                grand_margin.push_cell(cell.as_ref(), &plan.aggregate)?;
            }
        }
        access.validate()?;
        report.lookup_blocks = report
            .lookup_blocks
            .checked_add(access.blocks())
            .ok_or_else(|| failed("pivot lookup block count overflow"))?;
        report.reader_opens = report
            .reader_opens
            .checked_add(1)
            .ok_or_else(|| failed("pivot reader count overflow"))?;
    }
    Ok(Completed {
        fields,
        columns,
        rows: index_rows
            .checked_add(usize::from(has_margin))
            .ok_or_else(|| failed("pivot row count overflow"))?,
        pre_limit_rows: index_count
            .checked_add(usize::from(has_margin))
            .ok_or_else(|| failed("pivot row count overflow"))?,
        index_rows,
        domains: ordered_domains,
        column_margins,
        grand_margin,
        run,
        schema,
        max_items,
        report,
        _metadata: metadata,
    })
}

fn cell_at(
    access: &mut pivot::Access<'_, '_>,
    index: u64,
    domain: &str,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<pivot::Row>> {
    let marker = access.row(index, context)?;
    if marker.kind()? != 0 {
        return Err(failed("pivot output index is not a marker"));
    }
    let index = marker.index()?;
    access.find(
        pivot::KeyRef {
            index: index.as_ref(),
            kind: 1,
            domain: domain.as_bytes(),
        },
        context,
    )
}

fn aggregate_at(
    schema: &Schema,
    access: &mut pivot::Access<'_, '_>,
    index: u64,
    domain: &str,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<Cell>> {
    cell_at(access, index, domain, context)?
        .map(|row| match schema.read(&row, context)?.value {
            StoredValue::Aggregate(cell) => Ok(cell),
            _ => Err(failed("pivot margin selected a non-aggregate cell")),
        })
        .transpose()
}

impl Completed {
    pub(super) const fn spill_report(&self) -> SpillReport {
        self.report
    }

    pub(super) fn usage(&self) -> super::super::report::StateUsage {
        super::super::report::StateUsage {
            items: self.max_items,
            all_input_retained: false,
        }
    }
}
