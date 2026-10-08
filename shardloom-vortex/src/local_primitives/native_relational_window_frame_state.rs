//! Moving arithmetic, exact distinct membership and native extrema ordinals.

use super::{Function, Input, Spec};
use crate::{
    local_primitives::{
        SimpleAggregateFunction as Aggregate, native_capacity::ReservedVec, native_decimal_reduce,
        native_float_total, native_relational_aggregate::number, native_relational_batch::failed,
        native_relational_index::RowIndex, native_relational_keys::Cell,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{cmp::Ordering, ops::Range};

pub(in super::super) enum Value {
    Integer(u64),
    Count(u64),
    Float(Option<f64>),
    Decimal(Option<i128>),
    Source(Option<usize>),
}

impl Value {
    pub(in super::super) fn map_source(
        self,
        map: impl FnOnce(usize) -> Result<usize>,
    ) -> Result<Self> {
        match self {
            Self::Source(Some(position)) => Ok(Self::Source(Some(map(position)?))),
            value => Ok(value),
        }
    }
}

pub(in super::super) struct State {
    ranges: [Range<usize>; 3],
    counts: [u64; 3],
    floats: [native_float_total::Total; 3],
    decimals: [native_decimal_reduce::Total; 3],
    distinct: Option<Distinct>,
    extrema: Option<[Candidates; 3]>,
    _ownership: MemoryLease,
}

impl State {
    pub(in super::super) fn new(spec: &Spec, memory: &LiveMemoryPool) -> Result<Self> {
        let ownership = memory.reserve(
            u64::try_from(std::mem::size_of::<Self>())
                .map_err(|_| failed("window state size overflow"))?,
        )?;
        Ok(Self {
            ranges: [0..0, 0..0, 0..0],
            counts: [0; 3],
            floats: [native_float_total::Total::default(); 3],
            decimals: [native_decimal_reduce::Total::default(); 3],
            distinct: if spec.function == Function::Aggregate(Aggregate::CountDistinct) {
                Some(Distinct {
                    index: RowIndex::new(memory)?,
                    counts: ReservedVec::new(memory)?,
                    active: 0,
                })
            } else {
                None
            },
            extrema: if matches!(
                spec.function,
                Function::Aggregate(Aggregate::Min | Aggregate::Max)
            ) {
                Some([
                    Candidates::new(memory)?,
                    Candidates::new(memory)?,
                    Candidates::new(memory)?,
                ])
            } else {
                None
            },
            _ownership: ownership,
        })
    }

    pub(in super::super) fn advance(
        &mut self,
        next: [Range<usize>; 3],
        spec: &Spec,
        rows: usize,
        input: &mut impl Input,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Value> {
        super::validate_intervals(&self.ranges, &next, rows)?;
        if !matches!(spec.function, Function::Aggregate(_)) {
            let selected = select(&next, spec.function)?;
            self.ranges = next;
            return Ok(Value::Source(selected));
        }
        if spec.function == Function::Aggregate(Aggregate::Count) && spec.key.is_none() {
            let count = next.iter().try_fold(0u64, |count, range| {
                let len =
                    u64::try_from(range.len()).map_err(|_| failed("window count overflow"))?;
                count
                    .checked_add(len)
                    .ok_or_else(|| failed("window count overflow"))
            })?;
            self.ranges = next;
            return Ok(Value::Count(count));
        }
        let key = spec
            .key
            .ok_or_else(|| failed("window measure has no bound key"))?;
        if let Some(extrema) = &mut self.extrema {
            let maximum = spec.function == Function::Aggregate(Aggregate::Max);
            let mut selected = None;
            for (index, candidates) in extrema.iter_mut().enumerate() {
                candidates.advance(&self.ranges[index], &next[index], spec, input, context)?;
                if let Some(candidate) = candidates.first() {
                    let better = if let Some(old) = selected {
                        let order = input.compare_key(candidate, old, key, context)?;
                        order
                            == if maximum {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                    } else {
                        true
                    };
                    if better {
                        selected = Some(candidate);
                    }
                }
            }
            self.ranges = next;
            return Ok(Value::Source(selected));
        }
        // Remove all departed observations before admitting arrivals. Each
        // interval advances monotonically even when an exclusion splits a frame.
        for (index, range) in next.iter().enumerate() {
            let departed = self.ranges[index].start..range.start.min(self.ranges[index].end);
            for (offset, position) in departed.enumerate() {
                if offset.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                self.observe(index, position, false, spec, input, context)?;
            }
        }
        for (index, range) in next.iter().enumerate() {
            let arrived = self.ranges[index].end.max(range.start)..range.end;
            for (offset, position) in arrived.enumerate() {
                if offset.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                self.observe(index, position, true, spec, input, context)?;
            }
        }
        self.ranges = next;
        self.finish(spec)
    }

    fn finish(&self, spec: &Spec) -> Result<Value> {
        match spec.function {
            Function::Aggregate(Aggregate::Count) => Ok(Value::Count(
                self.counts.iter().try_fold(0u64, |sum, count| {
                    sum.checked_add(*count)
                        .ok_or_else(|| failed("window count overflow"))
                })?,
            )),
            Function::Aggregate(Aggregate::CountDistinct) => Ok(Value::Count(
                self.distinct
                    .as_ref()
                    .ok_or_else(|| failed("window distinct state is absent"))?
                    .active,
            )),
            Function::Aggregate(Aggregate::Sum | Aggregate::Avg) => {
                let average = spec.function == Function::Aggregate(Aggregate::Avg);
                if let Some(dtype) = spec.decimal {
                    let mut total = native_decimal_reduce::Total::default();
                    for part in &self.decimals {
                        total.merge(part)?;
                    }
                    Ok(Value::Decimal(total.finish(dtype, average)?))
                } else {
                    let mut total = native_float_total::Total::default();
                    for part in &self.floats {
                        total.merge(part)?;
                    }
                    Ok(Value::Float(total.finish(average)?))
                }
            }
            _ => Err(failed("window state does not match its bound function")),
        }
    }

    fn observe(
        &mut self,
        interval: usize,
        row: usize,
        add: bool,
        spec: &Spec,
        input: &mut impl Input,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let key = spec
            .key
            .ok_or_else(|| failed("window observation has no bound key"))?;
        if input.key_is_null(row, key, context)? {
            return Ok(());
        }
        match spec.function {
            Function::Aggregate(Aggregate::Count) => {
                self.counts[interval] = if add {
                    self.counts[interval].checked_add(1)
                } else {
                    self.counts[interval].checked_sub(1)
                }
                .ok_or_else(|| failed("window observation count overflow or underflow"))?;
            }
            Function::Aggregate(Aggregate::CountDistinct) => {
                self.distinct
                    .as_mut()
                    .ok_or_else(|| failed("window distinct state is absent"))?
                    .observe(row, key, add, input, context)?;
            }
            Function::Aggregate(Aggregate::Sum | Aggregate::Avg) => {
                let value = input.raw_cell(row, key, context)?;
                if let Some(dtype) = spec.decimal {
                    let Cell::Decimal(value, actual) = value else {
                        return Err(failed("window decimal observation changed type"));
                    };
                    if actual != dtype {
                        return Err(failed("window decimal observation changed metadata"));
                    }
                    if add {
                        self.decimals[interval].add(value, dtype)?;
                    } else {
                        self.decimals[interval].remove(value, dtype)?;
                    }
                } else {
                    let value = number(&value)?;
                    if add {
                        self.floats[interval].add(value)?;
                    } else {
                        self.floats[interval].remove(value)?;
                    }
                }
            }
            _ => return Err(failed("window observation used an incompatible state")),
        }
        Ok(())
    }
}

fn select(ranges: &[Range<usize>; 3], function: Function) -> Result<Option<usize>> {
    if function == Function::Last {
        return Ok(ranges
            .iter()
            .rev()
            .find(|range| !range.is_empty())
            .map(|range| range.end - 1));
    }
    let mut offset = match function {
        Function::First => 0,
        Function::Nth(index) => index
            .checked_sub(1)
            .ok_or_else(|| failed("NTH_VALUE position is zero"))?,
        _ => {
            return Err(failed(
                "window positional selection used a non-positional function",
            ));
        }
    };
    for range in ranges {
        if offset < range.len() {
            return Ok(Some(range.start + offset));
        }
        offset -= range.len();
    }
    Ok(None)
}

struct Distinct {
    index: RowIndex,
    counts: ReservedVec<u64>,
    active: u64,
}

impl Distinct {
    fn observe(
        &mut self,
        row: usize,
        key: usize,
        add: bool,
        input: &mut impl Input,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let hash = input
            .hash_key(row, key, context)?
            .ok_or_else(|| failed("nonnull distinct key has no hash"))?;
        let mut equal = |other| Ok(input.compare_key(row, other, key, context)? == Ordering::Equal);
        let index =
            if let Some(index) = self.index.find(hash, context.cancellation(), &mut equal)? {
                index
            } else {
                if !add {
                    return Err(failed("departed distinct value was never admitted"));
                }
                self.counts.reserve_one()?;
                let index = self
                    .index
                    .insert(hash, row, context.cancellation(), &mut equal)?;
                if index != self.counts.values.len() {
                    return Err(failed("window distinct identity lost insertion order"));
                }
                self.counts.values.push(0);
                index
            };
        let count = self.counts.values[index];
        let next = if add {
            count.checked_add(1)
        } else {
            count.checked_sub(1)
        }
        .ok_or_else(|| failed("window distinct occurrence count overflow or underflow"))?;
        if count == 0 {
            self.active = self
                .active
                .checked_add(1)
                .ok_or_else(|| failed("window distinct count overflow"))?;
        }
        if next == 0 {
            self.active = self
                .active
                .checked_sub(1)
                .ok_or_else(|| failed("window distinct count underflow"))?;
        }
        self.counts.values[index] = next;
        Ok(())
    }
}

struct Candidates {
    positions: ReservedVec<usize>,
    head: usize,
}

impl Candidates {
    fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            positions: ReservedVec::new(memory)?,
            head: 0,
        })
    }

    fn first(&self) -> Option<usize> {
        self.positions.values.get(self.head).copied()
    }

    fn advance(
        &mut self,
        prior: &Range<usize>,
        next: &Range<usize>,
        spec: &Spec,
        input: &mut impl Input,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let key = spec
            .key
            .ok_or_else(|| failed("window extrema has no bound key"))?;
        let maximum = spec.function == Function::Aggregate(Aggregate::Max);
        while self.first().is_some_and(|position| position < next.start) {
            if self.head.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            self.head += 1;
        }
        if self.head >= self.positions.values.len() / 2 {
            context.check_cancelled()?;
            self.positions.values.drain(..self.head);
            self.head = 0;
        }
        for position in prior.end.max(next.start)..next.end {
            if position.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            // This also validates singleton/non-comparing finite key domains.
            if input.hash_key(position, key, context)?.is_none() {
                continue;
            }
            let mut compared = 0usize;
            while self.positions.values.len() > self.head {
                if compared.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let previous = *self
                    .positions
                    .values
                    .last()
                    .expect("nonempty candidate queue");
                let order = input.compare_key(previous, position, key, context)?;
                if order
                    != if maximum {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                {
                    break;
                }
                self.positions.values.pop();
                compared += 1;
            }
            self.positions.push(position)?;
        }
        Ok(())
    }
}
