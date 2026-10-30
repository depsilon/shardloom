//! Measure-specific payloads in the existing compact grouped-state owner.
//! Binding fixes numeric type and policy; only retained state is specialized.
//! Unknown or changed domains keep the ordinary ordered reducer's contract.

use super::super::{
    Cell, Function, GroupOrdinals, Measure, NativeExecutionContext, NumericAggregateSum, Spec,
    State, Value, failed, numeric_reuse,
};
use super::{DensePages, ReservedVec, try_grow};
use crate::local_primitives::{
    aggregate_dense_pages::allocate, native_float_total::Specials,
    native_relational_keys::KeyColumn,
};
use shardloom_core::{FloatPolicy, Result};
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        ArrayRef,
        dtype::{DType, Nullability, PType},
    },
    mask::Mask,
};

#[path = "native_relational_aggregate_typed_pair.rs"]
mod pair;

#[cfg(test)]
#[path = "native_relational_aggregate_typed_state_tests.rs"]
mod tests;
#[path = "native_relational_aggregate_typed_updates.rs"]
mod updates;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Count,
    Integer,
    StrictFloat,
    ExtendedFloat,
    SignedExtreme,
    UnsignedExtreme,
    FloatExtreme,
}

#[derive(Clone, Copy)]
struct Slot {
    kind: Kind,
    index: usize,
}

#[derive(Clone, Copy, Default)]
struct Integer {
    count: u64,
    sum: f64,
    correction: f64,
}

#[derive(Clone, Copy, Default)]
struct Floating {
    count: u64,
    sum: f64,
}

#[derive(Clone, Copy, Default)]
struct Extended {
    numeric: Floating,
    specials: Specials,
}

#[derive(Clone, Copy, Default)]
struct Extreme<T> {
    count: u64,
    value: T,
}

pub(super) struct Table {
    slots: Vec<Slot>,
    _slot_lease: MemoryLease,
    widths: [usize; 7],
    groups: usize,
    policy: FloatPolicy,
    counts: DensePages<u64>,
    integers: DensePages<Integer>,
    strict: DensePages<Floating>,
    extended: DensePages<Extended>,
    signed_extrema: DensePages<Extreme<i64>>,
    unsigned_extrema: DensePages<Extreme<u64>>,
    float_extrema: DensePages<Extreme<f64>>,
}

fn kind(measure: &Measure, policy: FloatPolicy) -> Option<Kind> {
    if measure.secondary_column.is_some()
        || measure.quantile_level.is_some()
        || measure.separator.is_some()
        || measure.selection.is_some()
        || measure.decimal_source.is_some()
        || measure.decimal_output.is_some()
        || !measure.distinct_fields.is_empty()
    {
        return None;
    }
    if measure.function == Function::Count {
        return (measure.dtype == DType::Primitive(PType::U64, Nullability::NonNullable))
            .then_some(Kind::Count);
    }
    let input = measure.input_ptype?;
    if input == PType::F16 || measure.column.is_none() {
        return None;
    }
    match measure.function {
        Function::Sum | Function::Avg
            if measure.dtype == DType::Primitive(PType::F64, Nullability::Nullable) =>
        {
            Some(if input.is_int() {
                Kind::Integer
            } else if policy == FloatPolicy::Strict {
                Kind::StrictFloat
            } else {
                Kind::ExtendedFloat
            })
        }
        Function::Min | Function::Max
            if measure.dtype == DType::Primitive(input, Nullability::Nullable) =>
        {
            Some(if input.is_signed_int() {
                Kind::SignedExtreme
            } else if input.is_int() {
                Kind::UnsignedExtreme
            } else {
                Kind::FloatExtreme
            })
        }
        _ => None,
    }
}

/// Inspect only the existing logical schema. Optional admission must not
/// execute, mask, allocate, or diagnose a future measure's source.
fn input_matches(measure: &Measure, array: &ArrayRef) -> bool {
    measure.function == Function::Count
        || measure.column.as_deref().is_some_and(|name| {
            matches!(array.dtype().as_struct_fields_opt().and_then(|fields| fields.field(name)),
            Some(DType::Primitive(ptype, _)) if Some(ptype) == measure.input_ptype)
        })
}

impl Table {
    pub(super) fn new(
        spec: &Spec,
        array: &ArrayRef,
        groups: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        let policy = context.float_policy();
        if groups == 0
            || spec.groups.is_empty()
            || spec.measures.is_empty()
            || spec
                .measures
                .iter()
                .any(|measure| kind(measure, policy).is_none() || !input_matches(measure, array))
        {
            return Ok(None);
        }
        context.check_cancelled()?;
        let memory = context.memory();
        // Optional state allocation has not observed a measure. Decline only
        // allocation refusal, retaining the ordinary owner's admission path;
        // cancellation and checked-cardinality errors remain explicit.
        let Some((mut slots, slot_lease)) = allocate::<Slot>(spec.measures.len(), memory, failed)?
        else {
            context.check_cancelled()?;
            return Ok(None);
        };
        let mut widths = [0; 7];
        for measure in &spec.measures {
            let kind = kind(measure, policy).expect("typed aggregate spec admitted");
            let width = &mut widths[kind as usize];
            slots.push(Slot {
                kind,
                index: *width,
            });
            *width += 1;
        }
        let mut table = Self {
            slots,
            _slot_lease: slot_lease,
            widths,
            groups: 0,
            policy,
            counts: DensePages::new(memory)?,
            integers: DensePages::new(memory)?,
            strict: DensePages::new(memory)?,
            extended: DensePages::new(memory)?,
            signed_extrema: DensePages::new(memory)?,
            unsigned_extrema: DensePages::new(memory)?,
            float_extrema: DensePages::new(memory)?,
        };
        if !table.try_reserve_groups(groups, context)? {
            context.check_cancelled()?;
            return Ok(None);
        }
        Ok(Some(table))
    }

    pub(super) fn accepts(
        &self,
        spec: &Spec,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> bool {
        self.policy == context.float_policy()
            && spec
                .measures
                .iter()
                .all(|measure| input_matches(measure, array))
    }

    pub(super) fn reserve_groups(
        &mut self,
        groups: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if !self.try_reserve_groups(groups, context)? {
            return Err(failed(
                "compact grouped aggregate state capacity could not be admitted",
            ));
        }
        Ok(())
    }

    fn try_reserve_groups(
        &mut self,
        groups: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        if groups < self.groups {
            return Err(failed("compact aggregate group count decreased"));
        }
        if !try_grow(
            &mut self.counts,
            groups,
            self.widths[Kind::Count as usize],
            context,
        )? || !try_grow(
            &mut self.integers,
            groups,
            self.widths[Kind::Integer as usize],
            context,
        )? || !try_grow(
            &mut self.strict,
            groups,
            self.widths[Kind::StrictFloat as usize],
            context,
        )? || !try_grow(
            &mut self.extended,
            groups,
            self.widths[Kind::ExtendedFloat as usize],
            context,
        )? || !try_grow(
            &mut self.signed_extrema,
            groups,
            self.widths[Kind::SignedExtreme as usize],
            context,
        )? || !try_grow(
            &mut self.unsigned_extrema,
            groups,
            self.widths[Kind::UnsignedExtreme as usize],
            context,
        )? || !try_grow(
            &mut self.float_extrema,
            groups,
            self.widths[Kind::FloatExtreme as usize],
            context,
        )? {
            return Ok(false);
        }
        self.groups = groups;
        Ok(true)
    }

    fn state(&self, index: usize, group: usize) -> State {
        let slot = self.slots[index];
        let position = group * self.widths[slot.kind as usize] + slot.index;
        let mut state = State::default();
        match slot.kind {
            Kind::Count => state.count = self.counts[position],
            Kind::Integer => {
                let value = self.integers[position];
                state.count = value.count;
                if value.count != 0 {
                    state.sum = NumericAggregateSum::Integer {
                        sum: value.sum,
                        correction: value.correction,
                    };
                }
            }
            Kind::StrictFloat => {
                let value = self.strict[position];
                state.count = value.count;
                state.sum = NumericAggregateSum::Ordered(value.sum);
            }
            Kind::ExtendedFloat => {
                let value = self.extended[position];
                state.count = value.numeric.count;
                state.sum = NumericAggregateSum::Ordered(value.numeric.sum);
                state.specials = value.specials;
            }
            Kind::SignedExtreme => {
                let value = self.signed_extrema[position];
                state.count = value.count;
                state.extreme = (value.count != 0).then(|| {
                    if value.value < 0 {
                        Cell::NegativeInteger(value.value)
                    } else {
                        Cell::NonnegativeInteger(value.value.cast_unsigned())
                    }
                });
            }
            Kind::UnsignedExtreme => {
                let value = self.unsigned_extrema[position];
                state.count = value.count;
                state.extreme = (value.count != 0).then_some(Cell::NonnegativeInteger(value.value));
            }
            Kind::FloatExtreme => {
                let value = self.float_extrema[position];
                state.count = value.count;
                state.extreme = (value.count != 0).then_some(Cell::Float(value.value.to_bits()));
            }
        }
        state
    }

    pub(super) fn restore(
        &self,
        spec: &Spec,
        states: &mut ReservedVec<State>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let count = self
            .groups
            .checked_mul(spec.measures.len())
            .ok_or_else(|| failed("aggregate state cardinality overflow"))?;
        let mut replacement = ReservedVec::new(context.memory())?;
        replacement.reserve(count)?;
        for group in 0..self.groups {
            if group.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            for index in 0..spec.measures.len() {
                replacement.values.push(self.state(index, group));
            }
        }
        context.check_cancelled()?;
        *states = replacement;
        Ok(())
    }

    pub(super) fn admit_next(
        &self,
        spec: &Spec,
        index: usize,
        array: &ArrayRef,
        column: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        if !numeric_reuse::same_input(spec, index, array, column, self.groups)
            || self.slots[index].kind != self.slots[index + 1].kind
        {
            return Ok(false);
        }
        for group in 0..self.groups {
            if group.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            #[cfg(all(test, debug_assertions))]
            crate::local_primitives::native_relational_observation::record(|counts| {
                counts.aggregate_numeric_reuse_comparison_groups += 1;
            });
            let (left, right) = (self.state(index, group), self.state(index + 1, group));
            if left.count != right.count
                || left.specials != right.specials
                || !numeric_reuse::same_sum(left.sum, right.sum)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn copy_completed(
        &mut self,
        source: usize,
        target: usize,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let (source, target) = (self.slots[source], self.slots[target]);
        if source.kind != target.kind {
            return Err(failed("compact numeric reuse has incompatible slots"));
        }
        let width = self.widths[source.kind as usize];
        match source.kind {
            Kind::Integer => copy(
                &mut self.integers,
                self.groups,
                width,
                source.index,
                target.index,
                context,
            )?,
            Kind::StrictFloat => copy(
                &mut self.strict,
                self.groups,
                width,
                source.index,
                target.index,
                context,
            )?,
            Kind::ExtendedFloat => copy(
                &mut self.extended,
                self.groups,
                width,
                source.index,
                target.index,
                context,
            )?,
            _ => return Err(failed("compact numeric reuse has incompatible slots")),
        }
        numeric_reuse::record_copy(rows, context)
    }

    pub(super) fn consume(
        &mut self,
        index: usize,
        measure: &Measure,
        ordinals: &GroupOrdinals,
        values: Option<&KeyColumn>,
        validity: Option<&Mask>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        updates::consume(self, index, measure, ordinals, values, validity, context)
    }

    #[allow(clippy::too_many_arguments)] // Preserve the existing prepared owner/proof and group domain.
    pub(super) fn consume_pair(
        &mut self,
        spec: &Spec,
        index: usize,
        ordinals: &GroupOrdinals,
        source: &ArrayRef,
        owner: &super::super::NativeNumericOwner,
        completed_policy: Option<FloatPolicy>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<usize>> {
        pair::accumulate(
            self,
            spec,
            index,
            ordinals,
            source,
            owner,
            completed_policy,
            context,
        )
    }

    pub(super) fn finish(
        &self,
        index: usize,
        group: usize,
        function: Function,
    ) -> Result<Value<'static>> {
        let state = self.state(index, group);
        if function == Function::Count {
            return Ok(Value::UInt(state.count));
        }
        if state.count == 0 {
            return Ok(Value::Null);
        }
        Ok(match function {
            Function::Sum => {
                Value::Float(state.specials.result().unwrap_or_else(|| state.sum.total()))
            }
            Function::Avg => Value::Float(state.specials.result().unwrap_or_else(|| {
                crate::local_primitives::simple_average_value(state.sum.total(), state.count)
            })),
            Function::Min | Function::Max => match state.extreme {
                Some(Cell::NegativeInteger(value)) => Value::Int(value),
                Some(Cell::NonnegativeInteger(value))
                    if self.slots[index].kind == Kind::SignedExtreme =>
                {
                    Value::Int(i64::try_from(value).map_err(super::super::vortex_error)?)
                }
                Some(Cell::NonnegativeInteger(value)) => Value::UInt(value),
                Some(Cell::Float(bits)) => Value::Float(f64::from_bits(bits)),
                _ => return Err(failed("compact numeric extremum state is absent")),
            },
            _ => return Err(failed("compact aggregate result has incompatible measure")),
        })
    }
}

fn copy<T: Copy>(
    pages: &mut DensePages<T>,
    groups: usize,
    width: usize,
    source: usize,
    target: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    for group in 0..groups {
        if group.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        pages[group * width + target] = pages[group * width + source];
    }
    Ok(())
}
