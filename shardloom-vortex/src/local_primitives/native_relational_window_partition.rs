//! Preserve partition, ranking-pass and framed-function evaluation order.

use super::{
    Group, Layout, NativeExecutionContext, Report, Result, Spec, State, StoredOrder, add, bounds,
    distinct, extrema, failed, frame,
    lookup::{Lookup, Partition},
    records::{self, Kind, Values},
};
use crate::local_primitives::{
    SimpleAggregateFunction as Aggregate,
    native_capacity::ReservedVec,
    native_relational_records as private, native_relational_sort,
    native_relational_spill::Ordering,
    native_relational_window::{PeerRow, ranking_value},
    native_relational_window_frame::Input as _,
    result_batch::{self, Value},
    vortex_error,
};
use vortex::array::{
    VortexSessionExecute as _, arrays::PrimitiveArray, memory::MemorySessionExt as _,
};

pub(super) struct Peers<'a, 's> {
    pub(super) lookup: Lookup<'a, 's>,
    rows: usize,
    peer: usize,
    start: usize,
    end: usize,
}

impl<'a, 's> Peers<'a, 's> {
    pub(super) fn new(
        store: &'a StoredOrder<'s>,
        names: &'a [String],
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let mut lookup = Lookup::new(store, names, context)?;
        if lookup.rows()? < 2
            || lookup.unsigned(0, 0, context)? != 0
            || lookup.unsigned(lookup.rows()? - 1, 0, context)? != rows
        {
            return Err(failed("window peer index has invalid partition sentinels"));
        }
        let end = lookup.unsigned(1, 0, context)?;
        if end == 0 || end > rows {
            return Err(failed("window peer boundary exceeds its partition"));
        }
        Ok(Self {
            lookup,
            rows,
            peer: 0,
            start: 0,
            end,
        })
    }

    pub(super) fn at(
        &mut self,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<frame::Position> {
        if row < self.start || row >= self.rows {
            return Err(failed(
                "window peer position moved backwards or exceeded its partition",
            ));
        }
        while row >= self.end {
            self.start = self.end;
            self.peer += 1;
            self.end = self.lookup.unsigned(self.peer + 1, 0, context)?;
            if self.end <= self.start || self.end > self.rows {
                return Err(failed("window peer boundaries are not increasing"));
            }
        }
        Ok(frame::Position {
            rows: self.rows,
            peer_start: self.start,
            peer_end: self.end,
            peer: self.peer,
            row,
        })
    }

    pub(super) fn edge(
        &mut self,
        index: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<usize> {
        if index >= self.lookup.rows()? {
            return Ok(self.rows);
        }
        self.lookup.unsigned(index, 0, context)
    }
}

#[allow(clippy::too_many_arguments)] // One partition shares completed native input, schemas, store and result order.
pub(super) fn run<'s>(
    spec: &Spec,
    group: &Group,
    layout: &Layout,
    input: &mut Partition<'_, 's>,
    spill: &'s State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    result_spec: &native_relational_sort::Spec,
    results: &mut Ordering<'_, 's>,
    report: &mut Report,
) -> Result<()> {
    let rows = input.range.len();
    let mut stored = ReservedVec::new(context.memory())?;
    stored.reserve(group.functions.len())?;
    stored.values.resize_with(group.functions.len(), || None);
    let peers = rank(
        spec,
        group,
        layout,
        input,
        spill,
        context,
        limit,
        &mut stored.values,
        report,
    )?;
    for (slot, &function) in group.functions.iter().enumerate() {
        if let Some(frame) = &spec.frames[function] {
            let mut output = Values::new(&layout.results[function], spill, limit, context)?;
            let mut peer = Peers::new(&peers, &layout.peers.names, rows, context)?;
            match frame.function {
                frame::Function::Aggregate(
                    Aggregate::CountDistinct | Aggregate::Min | Aggregate::Max,
                ) => {
                    let bounds =
                        bounds::build(frame, input, &mut peer, spill, context, limit, report)?;
                    if frame.function == frame::Function::Aggregate(Aggregate::CountDistinct) {
                        distinct::run(
                            frame,
                            layout,
                            input,
                            &bounds,
                            spill,
                            context,
                            limit,
                            &mut output,
                            report,
                        )?;
                    } else {
                        extrema::run(
                            frame,
                            input,
                            &bounds,
                            spill,
                            context,
                            limit,
                            &mut output,
                            report,
                        )?;
                    }
                    bounds.finish(context)?;
                }
                _ => fixed(frame, input, &mut peer, &mut output, context)?,
            }
            add(&mut report.lookup_blocks, peer.lookup.blocks)?;
            stored.values[slot] = Some(output.finish(rows, context)?);
        }
    }
    assemble(
        spec,
        group,
        layout,
        input,
        &stored.values,
        result_spec,
        results,
        context,
        limit,
        report,
    )?;
    for store in stored.values {
        store
            .ok_or_else(|| failed("window partition result is absent"))?
            .finish(context)?;
    }
    peers.finish(context)
}

#[allow(clippy::too_many_arguments)] // The ranking pass fills function records and peer boundaries before evaluating frames.
fn rank<'s>(
    spec: &Spec,
    group: &Group,
    layout: &Layout,
    input: &mut Partition<'_, 's>,
    spill: &'s State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    stored: &mut [Option<StoredOrder<'s>>],
    report: &mut Report,
) -> Result<StoredOrder<'s>> {
    let rows = input.range.len();
    let mut ranking = ReservedVec::new(context.memory())?;
    ranking.reserve(group.functions.len())?;
    for (slot, &function) in group.functions.iter().enumerate() {
        if spec.frames[function].is_none() {
            ranking.values.push((
                slot,
                Values::new(&layout.results[function], spill, limit, context)?,
            ));
        }
    }
    let mut peers = records::Ordinals::new(&layout.peers, spill, limit, context)?;
    let mut start = 0;
    let mut dense_rank = 0u64;
    while start < rows {
        context.check_cancelled()?;
        let mut end = start + 1;
        while end < rows && same_order(input, group, start, end, context)? {
            if end.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            end += 1;
        }
        dense_rank = dense_rank
            .checked_add(1)
            .ok_or_else(|| failed("window dense rank overflow"))?;
        peers.push(start, context)?;
        add(&mut report.peer_records, 1)?;
        for position in start..end {
            if position.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            for (slot, output) in &mut ranking.values {
                output.push(
                    ranking_value(
                        &spec.functions[group.functions[*slot]],
                        PeerRow {
                            rows,
                            position,
                            peer_start: start,
                            peer_end: end,
                            dense_rank,
                        },
                    )?,
                    context,
                )?;
            }
        }
        start = end;
    }
    peers.push(rows, context)?;
    add(&mut report.peer_records, 1)?;
    let peers = peers.finish(context)?;
    for (slot, output) in ranking.values {
        stored[slot] = Some(output.finish(rows, context)?);
    }
    Ok(peers)
}

fn same_order(
    input: &mut Partition<'_, '_>,
    group: &Group,
    left: usize,
    right: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    for key in &group.order {
        if !input.compare_key(left, right, key.key, context)?.is_eq() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn fixed(
    spec: &frame::Spec,
    input: &mut Partition<'_, '_>,
    peers: &mut Peers<'_, '_>,
    output: &mut Values<'_, '_>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let mut cursor = frame::Cursor::default();
    let mut state = frame::State::new(spec, context.memory())?;
    for position in 0..input.range.len() {
        if position.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let at = peers.at(position, context)?;
        let range = cursor.advance(
            &spec.frame,
            &at,
            input,
            &mut |edge| peers.edge(edge, context),
            context,
        )?;
        let ranges = frame::intervals(range, &at, spec.frame.exclusion);
        output.push(
            state.advance(ranges, spec, input.range.len(), input, context)?,
            context,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Assemblies consume every function for this one partition before releasing its records.
fn assemble(
    spec: &Spec,
    group: &Group,
    layout: &Layout,
    input: &mut Partition<'_, '_>,
    stores: &[Option<StoredOrder<'_>>],
    result_spec: &native_relational_sort::Spec,
    results: &mut Ordering<'_, '_>,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    report: &mut Report,
) -> Result<()> {
    let rows = input.range.len();
    let mut positions = ReservedVec::new(context.memory())?;
    positions.reserve(limit)?;
    for start in (0..rows).step_by(limit) {
        let end = start.saturating_add(limit).min(rows);
        positions.values.clear();
        positions.values.extend((start..end).map(Some));
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(result_spec.fields.len())?;
        columns
            .values
            .push(private::unsigned(end - start, context, |row| {
                u64::try_from(input.lookup.unsigned(
                    input.range.start + start + row,
                    spec.keys.len(),
                    context,
                )?)
                .map_err(vortex_error)
            })?);
        for (slot, &index) in group.functions.iter().enumerate() {
            let store = stores[slot]
                .as_ref()
                .ok_or_else(|| failed("window function result is absent"))?;
            if store.rows() != rows as u64 {
                return Err(failed("window function result changed partition length"));
            }
            let mut lookup = Lookup::<2>::new(store, &layout.results[index].order.names, context)?;
            for row in start..end {
                if lookup.unsigned(row, 0, context)? != row {
                    return Err(failed("window partition result ordinal changed"));
                }
            }
            let kind = layout.results[index].kind;
            let values = lookup.gather(
                &positions.values,
                None,
                records::VALUE,
                &kind.dtype(),
                context,
            )?;
            let values = if matches!(kind, Kind::Source) {
                let mut execution = context.native_session().create_execution_ctx();
                let values = values
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                let valid = values
                    .validity()
                    .map_err(vortex_error)?
                    .execute_mask(values.len(), &mut execution)
                    .map_err(vortex_error)?;
                let ordinals = values.to_buffer::<u64>();
                result_batch::build_column(
                    &kind.dtype(),
                    values.len(),
                    &context.native_session().allocator(),
                    |row| {
                        if !valid.value(row) {
                            return Ok(Value::Null);
                        }
                        let position = usize::try_from(ordinals[row]).map_err(vortex_error)?;
                        if position >= rows {
                            return Err(failed("selected window position exceeds its partition"));
                        }
                        Ok(Value::UInt(
                            u64::try_from(input.lookup.unsigned(
                                input.range.start + position,
                                spec.keys.len(),
                                context,
                            )?)
                            .map_err(vortex_error)?,
                        ))
                    },
                )?
            } else {
                values
            };
            columns.values.push(values);
            add(&mut report.lookup_blocks, lookup.blocks)?;
        }
        results.build(
            private::structure(&result_spec.fields, columns, end - start)?,
            context,
        )?;
    }
    Ok(())
}
