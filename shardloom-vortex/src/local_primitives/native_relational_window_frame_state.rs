//! Moving arithmetic, exact distinct membership and native extrema ordinals.

use super::{Function, Input, Spec};
use crate::{
    local_primitives::{
        native_capacity::ReservedVec,
        native_decimal_reduce, native_float_total,
        native_order_statistic::{Arena, Root},
        native_quantile::Position,
        native_relational_aggregate::Function as Aggregate,
        native_relational_aggregate::number,
        native_relational_batch::failed,
        native_relational_index::RowIndex,
        native_relational_keys::Cell,
        result_batch,
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
    Sources(ReservedVec<usize>),
}

impl Value {
    pub(in super::super) fn map_source(
        self,
        mut map: impl FnMut(usize) -> Result<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        match self {
            Self::Source(Some(position)) => Ok(Self::Source(Some(map(position)?))),
            Self::Sources(mut positions) => {
                for (index, position) in positions.values.iter_mut().enumerate() {
                    if index.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    *position = map(*position)?;
                }
                Ok(Self::Sources(positions))
            }
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
    moments: Option<Box<super::moments::Ranges>>,
    quantiles: Option<(Arena, Root)>,
    collections: Option<(Arena, Root)>,
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
            distinct: if matches!(spec.function, Function::Aggregate(function) if function.is_distinct())
            {
                Some(Distinct {
                    index: RowIndex::new(memory)?,
                    counts: ReservedVec::new(memory)?,
                    active: 0,
                })
            } else {
                None
            },
            extrema: if matches!(spec.function, Function::Aggregate(function)
                if matches!(function, Aggregate::Min | Aggregate::Max) || function.is_selector())
            {
                Some([
                    Candidates::new(memory)?,
                    Candidates::new(memory)?,
                    Candidates::new(memory)?,
                ])
            } else {
                None
            },
            moments: if matches!(spec.function, Function::Aggregate(function) if function.is_moment())
            {
                Some(super::moments::Ranges::new(memory)?)
            } else {
                None
            },
            quantiles: if matches!(spec.function, Function::Aggregate(function) if function.is_quantile())
            {
                Some((Arena::new(memory)?, None))
            } else {
                None
            },
            collections: if matches!(spec.function, Function::Aggregate(function) if function.is_collection())
            {
                Some((Arena::new(memory)?, None))
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
        if spec.frame.can_rewind() {
            super::validate_ranges(&next, rows)?;
        } else {
            super::validate_intervals(&self.ranges, &next, rows)?;
        }
        if let Some(moments) = &mut self.moments {
            let result = moments.advance(&next, spec, input, context)?;
            self.ranges = next;
            return Ok(Value::Float(result));
        }
        if !matches!(spec.function, Function::Aggregate(_)) {
            let selected = select(&next, spec.function)?;
            self.ranges = next;
            return Ok(Value::Source(selected));
        }
        if spec.function == Function::Aggregate(Aggregate::Count)
            && spec.key.is_none()
            && spec.filter_key.is_none()
        {
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
        if let Some(extrema) = &mut self.extrema {
            let mut selected = None;
            for (index, candidates) in extrema.iter_mut().enumerate() {
                candidates.advance(&self.ranges[index], &next[index], spec, input, context)?;
                selected =
                    super::selection::choose(selected, candidates.first(), spec, input, context)?;
            }
            self.ranges = next;
            return Ok(Value::Source(selected));
        }
        // Remove all departed observations before admitting arrivals. Calendar
        // shifts may retreat, so both sides of an interval can change membership.
        for (index, range) in next.iter().enumerate() {
            let departed = super::difference(&self.ranges[index], range);
            for (offset, position) in departed.into_iter().flatten().enumerate() {
                if offset.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                self.observe(index, position, false, spec, input, context)?;
            }
        }
        for (index, range) in next.iter().enumerate() {
            let arrived = super::difference(range, &self.ranges[index]);
            for (offset, position) in arrived.into_iter().flatten().enumerate() {
                if offset.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                self.observe(index, position, true, spec, input, context)?;
            }
        }
        self.ranges = next;
        self.finish(spec, context)
    }

    fn finish(&self, spec: &Spec, context: &NativeExecutionContext<'_>) -> Result<Value> {
        match spec.function {
            Function::Aggregate(function) if function.is_collection() => {
                let (arena, root) = self
                    .collections
                    .as_ref()
                    .ok_or_else(|| failed("window collection state is absent"))?;
                let mut rows = ReservedVec::new(context.memory())?;
                rows.reserve(
                    usize::try_from(arena.count(*root))
                        .map_err(|_| failed("window collection count exceeds addressable rows"))?,
                )?;
                arena.visit(*root, context.cancellation(), &mut |row| {
                    rows.values.push(row);
                    Ok(())
                })?;
                Ok(Value::Sources(rows))
            }
            Function::Aggregate(function) if function.is_quantile() => {
                let (arena, root) = self
                    .quantiles
                    .as_ref()
                    .ok_or_else(|| failed("window quantile state is absent"))?;
                let count = arena.count(*root);
                let discrete = function == Aggregate::QuantileDisc;
                if count == 0 {
                    return Ok(if discrete {
                        Value::Source(None)
                    } else if spec.decimal.is_some() {
                        Value::Decimal(None)
                    } else {
                        Value::Float(None)
                    });
                }
                let position = Position::new(
                    spec.quantile_level
                        .ok_or_else(|| failed("window quantile level is absent"))?,
                    count,
                    discrete,
                )?;
                let (lower, ordinal) =
                    arena.select(*root, position.lower, context.cancellation())?;
                if discrete {
                    return Ok(Value::Source(Some(ordinal)));
                }
                let (upper, _) = arena.select(*root, position.upper, context.cancellation())?;
                let dtype = if let Some(source) = spec.decimal {
                    vortex::array::dtype::DType::Decimal(
                        native_decimal_reduce::output_dtype(source, true)?,
                        vortex::array::dtype::Nullability::Nullable,
                    )
                } else {
                    vortex::array::dtype::DType::Primitive(
                        vortex::array::dtype::PType::F64,
                        vortex::array::dtype::Nullability::Nullable,
                    )
                };
                match position.interpolate(lower, upper, &dtype)? {
                    result_batch::Value::Float(value) => Ok(Value::Float(Some(value))),
                    result_batch::Value::Decimal(value, _) => Ok(Value::Decimal(Some(value))),
                    _ => Err(failed("window quantile interpolation changed output type")),
                }
            }
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
            Function::Aggregate(
                Aggregate::Sum | Aggregate::Avg | Aggregate::SumDistinct | Aggregate::AvgDistinct,
            ) => {
                let average =
                    matches!(spec.function, Function::Aggregate(function) if function.is_average());
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
        if !super::admitted(row, spec, input, context)? {
            return Ok(());
        }
        if spec.function == Function::Aggregate(Aggregate::Count) && spec.key.is_none() {
            self.counts[interval] = if add {
                self.counts[interval].checked_add(1)
            } else {
                self.counts[interval].checked_sub(1)
            }
            .ok_or_else(|| failed("window observation count overflow or underflow"))?;
            return Ok(());
        }
        if let Some((arena, root)) = &mut self.collections {
            if !super::selection::eligible(row, spec, input, context)? {
                return Ok(());
            }
            let compare =
                |left, right| super::selection::compare(left, right, spec, input, context);
            *root = if add {
                arena.insert_by(*root, row, context.cancellation(), compare)?
            } else {
                arena.remove_by(*root, row, context.cancellation(), compare)?
            };
            return Ok(());
        }
        let key = spec
            .key
            .ok_or_else(|| failed("window observation has no bound key"))?;
        if input.key_is_null(row, key, context)? {
            return Ok(());
        }
        if let Some((arena, root)) = &mut self.quantiles {
            let value = input.raw_cell(row, key, context)?;
            *root = if add {
                arena.insert(*root, value, row, context.cancellation())?
            } else {
                arena.remove(*root, &value, row, context.cancellation())?
            };
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
            Function::Aggregate(
                Aggregate::CountDistinct | Aggregate::SumDistinct | Aggregate::AvgDistinct,
            ) => {
                let changed = self
                    .distinct
                    .as_mut()
                    .ok_or_else(|| failed("window distinct state is absent"))?
                    .observe(row, key, add, input, context)?;
                if changed && spec.function != Function::Aggregate(Aggregate::CountDistinct) {
                    // Membership belongs to the union of the frame intervals.
                    // One shared total observes the first/last active occurrence.
                    self.observe_additive(0, row, key, add, spec, input, context)?;
                }
            }
            Function::Aggregate(Aggregate::Sum | Aggregate::Avg) => {
                self.observe_additive(interval, row, key, add, spec, input, context)?;
            }
            _ => return Err(failed("window observation used an incompatible state")),
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // Updates the existing interval total using its selected native row.
    fn observe_additive(
        &mut self,
        interval: usize,
        row: usize,
        key: usize,
        add: bool,
        spec: &Spec,
        input: &mut impl Input,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
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
    ) -> Result<bool> {
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
        Ok(count == 0 || next == 0)
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
        let mut prior = prior.clone();
        if next.start < prior.start || next.end < prior.end {
            // Reuse the admitted queue capacity. Its discarded dominance
            // history cannot be revived when a calendar boundary retreats.
            self.positions.values.clear();
            self.head = 0;
            prior = 0..0;
        }
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
            if !super::selection::eligible(position, spec, input, context)? {
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
                if !super::selection::compare(previous, position, spec, input, context)?.is_gt() {
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
