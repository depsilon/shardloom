//! Stored monotonic frame bounds and their inverse membership intervals.

use super::{
    NativeExecutionContext, Report, Result, State, StoredOrder, add, failed, frame,
    lookup::{Lookup, Partition},
    partition::Peers,
};
use crate::local_primitives::{
    native_capacity::ReservedVec,
    native_relational_records::{self as records, ORDINAL},
    native_relational_sort,
    native_relational_spill::Ordering,
    native_relational_window_frame::Input as _,
    vortex_error,
};
use shardloom_exec::live_memory::MemoryLease;
use std::ops::Range;

pub(super) struct Bounds<'s> {
    pub(super) store: StoredOrder<'s>,
    pub(super) names: [String; 7],
    source_rows: usize,
    _metadata: MemoryLease,
}

impl Bounds<'_> {
    pub(super) fn finish(self, context: &NativeExecutionContext<'_>) -> Result<()> {
        self.store.finish(context)
    }
}

#[allow(clippy::too_many_arguments)] // Frame validation and persisted endpoints share one completed partition.
pub(super) fn build<'s>(
    spec: &frame::Spec,
    input: &mut Partition<'_, '_>,
    peers: &mut Peers<'_, '_>,
    spill: &'s State,
    context: &NativeExecutionContext<'_>,
    limit: usize,
    report: &mut Report,
) -> Result<Bounds<'s>> {
    let metadata = context.memory().reserve(4096)?;
    let names = [ORDINAL, "s0", "e0", "s1", "e1", "s2", "e2"].map(str::to_owned);
    let sort = records::order(
        names
            .iter()
            .map(|name| (name.clone(), records::u64_type()))
            .collect(),
        vec![ORDINAL.into()],
    )?;
    let mut order = Ordering::new(&sort, spill, limit, context)?;
    let mut buffer = ReservedVec::new(context.memory())?;
    buffer.reserve(limit)?;
    let mut written = 0;
    let mut cursor = frame::Cursor::default();
    let mut previous = [0..0, 0..0, 0..0];
    for position in 0..input.range.len() {
        if position.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let at = peers.at(position, context)?;
        let range = cursor.advance(
            &spec.frame,
            &at,
            input,
            &mut |index| peers.edge(index, context),
            context,
        )?;
        let ranges = frame::intervals(range, &at, spec.frame.exclusion);
        if spec.frame.can_rewind() {
            frame::validate_ranges(&ranges, input.range.len())?;
        } else {
            frame::validate_intervals(&previous, &ranges, input.range.len())?;
        }
        // Match the resident first-observation order before sorting values or
        // building summaries. Values absent from all frames are never evaluated.
        for (prior, next) in previous.iter().zip(&ranges) {
            for (offset, row) in frame::difference(next, prior)
                .into_iter()
                .flatten()
                .enumerate()
            {
                if offset.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                observe(spec, input, row, context)?;
            }
        }
        previous.clone_from(&ranges);
        buffer.values.push(ranges);
        if buffer.values.len() == limit {
            flush(&mut buffer, &mut written, &sort, &mut order, context)?;
            #[cfg(test)]
            super::progress(super::Progress::Bounds, written);
        }
    }
    flush(&mut buffer, &mut written, &sort, &mut order, context)?;
    if written != input.range.len() {
        return Err(failed("window bounds changed partition length"));
    }
    add(&mut report.bounds_rows, written as u64)?;
    Ok(Bounds {
        store: order.retain(context)?,
        names,
        source_rows: input.range.len(),
        _metadata: metadata,
    })
}

/// Find a maximal output range whose endpoints are monotone. No per-row run
/// index is retained: each stored bound is visited once plus a boundary reread.
pub(super) fn monotone_end(
    bounds: &Bounds<'_>,
    start: usize,
    context: &NativeExecutionContext<'_>,
    report: &mut Report,
) -> Result<usize> {
    let mut input = Lookup::<2>::new(&bounds.store, &bounds.names, context)?;
    let rows = input.rows()?;
    if start >= rows {
        return Err(failed("window monotone range starts outside output"));
    }
    let mut previous = ranges(&mut input, start, context)?;
    let mut end = start + 1;
    while end < rows {
        if end.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let next = ranges(&mut input, end, context)?;
        if !frame::monotone(&previous, &next) {
            break;
        }
        previous = next;
        end += 1;
    }
    add(&mut report.lookup_blocks, input.blocks)?;
    Ok(end)
}

fn observe(
    spec: &frame::Spec,
    input: &mut Partition<'_, '_>,
    row: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if !frame::admitted(row, spec, input, context)? {
        return Ok(());
    }
    if matches!(spec.function, frame::Function::Aggregate(function) if function.is_selector()) {
        // A selector observes its comparison keys and parent validity. Its
        // value remains payload until the final selected row is gathered.
        frame::selection::eligible(row, spec, input, context)?;
    } else {
        let key = spec
            .key
            .ok_or_else(|| failed("window interval function has no measure key"))?;
        if !input.key_is_null(row, key, context)? {
            input.hash_key(row, key, context)?;
        }
    }
    Ok(())
}

fn flush(
    buffer: &mut ReservedVec<[Range<usize>; 3]>,
    written: &mut usize,
    spec: &native_relational_sort::Spec,
    order: &mut Ordering<'_, '_>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if buffer.values.is_empty() {
        return Ok(());
    }
    let rows = buffer.values.len();
    let end = written
        .checked_add(rows)
        .ok_or_else(|| failed("window bound row count overflow"))?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(7)?;
    columns.values.push(records::unsigned(rows, context, |row| {
        u64::try_from(*written + row).map_err(vortex_error)
    })?);
    for interval in 0..3 {
        columns.values.push(records::unsigned(rows, context, |row| {
            u64::try_from(buffer.values[row][interval].start).map_err(vortex_error)
        })?);
        columns.values.push(records::unsigned(rows, context, |row| {
            u64::try_from(buffer.values[row][interval].end).map_err(vortex_error)
        })?);
    }
    order.build(records::structure(&spec.fields, columns, rows)?, context)?;
    *written = end;
    buffer.values.clear();
    Ok(())
}

pub(super) fn ranges(
    lookup: &mut Lookup<'_, '_>,
    row: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<[Range<usize>; 3]> {
    if lookup.unsigned(row, 0, context)? != row {
        return Err(failed("stored window frame ordinal changed"));
    }
    let mut ranges = [0..0, 0..0, 0..0];
    for (index, range) in ranges.iter_mut().enumerate() {
        *range = lookup.unsigned(row, 1 + 2 * index, context)?
            ..lookup.unsigned(row, 2 + 2 * index, context)?;
    }
    Ok(ranges)
}

pub(super) struct Inversion<'a, 's> {
    starts: [Lookup<'a, 's, 1>; 3],
    ends: [Lookup<'a, 's, 1>; 3],
    start_at: [usize; 3],
    end_at: [usize; 3],
    outputs: Range<usize>,
    source_rows: usize,
    previous: Option<usize>,
    _metadata: MemoryLease,
}

impl<'a, 's> Inversion<'a, 's> {
    pub(super) fn new(
        bounds: &'a Bounds<'s>,
        outputs: Range<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let rows = usize::try_from(bounds.store.rows())
            .map_err(|_| failed("window inverse count overflow"))?;
        if outputs.start >= outputs.end || outputs.end > rows {
            return Err(failed("window inverse output range exceeds stored bounds"));
        }
        let metadata = context
            .memory()
            .reserve(std::mem::size_of::<Self>() as u64)?;
        let read = || Lookup::new(&bounds.store, &bounds.names, context);
        Ok(Self {
            starts: [read()?, read()?, read()?],
            ends: [read()?, read()?, read()?],
            start_at: [outputs.start; 3],
            end_at: [outputs.start; 3],
            outputs,
            source_rows: bounds.source_rows,
            previous: None,
            _metadata: metadata,
        })
    }

    pub(super) fn spans(
        &mut self,
        source: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<[Range<usize>; 3]> {
        if self.previous.is_some_and(|previous| source < previous) || source >= self.source_rows {
            return Err(failed(
                "window inverse source moved backwards or exceeded its partition",
            ));
        }
        self.previous = Some(source);
        let mut spans = [0..0, 0..0, 0..0];
        for (index, span) in spans.iter_mut().enumerate() {
            while self.end_at[index] < self.outputs.end
                && self.ends[index].unsigned(self.end_at[index], 2 + 2 * index, context)? <= source
            {
                self.end_at[index] += 1;
            }
            while self.start_at[index] < self.outputs.end
                && self.starts[index].unsigned(self.start_at[index], 1 + 2 * index, context)?
                    <= source
            {
                self.start_at[index] += 1;
            }
            if self.end_at[index] > self.start_at[index] {
                return Err(failed("window inverse endpoints are reversed"));
            }
            *span =
                self.end_at[index] - self.outputs.start..self.start_at[index] - self.outputs.start;
        }
        Ok(spans)
    }

    pub(super) fn record(&self, report: &mut Report) -> Result<()> {
        for lookup in self.starts.iter().chain(&self.ends) {
            add(&mut report.lookup_blocks, lookup.blocks)?;
        }
        Ok(())
    }
}
