//! Exact grouping and distinct membership over native keys. Additive measures
//! retain ordered floating inputs and compensate integer-input additions, with
//! separate exact decimal totals.

#[cfg(test)]
#[path = "native_relational_aggregate_reuse_tests.rs"]
mod reuse_tests;

#[cfg(test)]
#[path = "native_relational_aggregate_numeric_tests.rs"]
mod numeric_tests;

#[cfg(test)]
#[path = "native_relational_aggregate_numeric_reuse_tests.rs"]
mod numeric_reuse_tests;

#[path = "native_relational_aggregate_numeric_reuse.rs"]
mod numeric_reuse;

#[path = "native_relational_aggregate_compact.rs"]
mod compact;

#[cfg(test)]
#[path = "native_relational_aggregate_compact_tests.rs"]
mod compact_tests;

#[path = "native_relational_aggregate_extrema.rs"]
mod extrema;

#[path = "native_relational_aggregate_extrema_pair.rs"]
mod extrema_pair;

#[path = "native_relational_collections.rs"]
mod collections;
#[path = "native_relational_aggregate_filter.rs"]
pub(super) mod filter;
#[path = "native_relational_quantiles.rs"]
mod quantiles;
#[path = "native_relational_selections.rs"]
mod selections;
#[path = "native_relational_statistics.rs"]
mod statistics;

pub(super) use super::native_relational_aggregate_kind::Function;
pub(super) use selections::Spec as SelectionSpec;

use super::{
    NumericAggregateSum, logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_decimal_reduce,
    native_numeric_owner::NativeNumericOwner,
    native_payload,
    native_relational_batch::{failed, index_array},
    native_relational_expression::{keys, parent_validity},
    native_relational_keys::{Cell, KeyColumn, compare_cells, validate_finite_scalar},
    native_relational_set::RowSet,
    result_batch::{self, Value},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::borrow::Cow;
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::StructArray,
        dtype::{DType, DecimalDType, FieldNames, PType},
        memory::HostMemorySessionExt as _,
        validity::Validity,
    },
    buffer::Alignment,
    mask::Mask,
};

pub(super) struct Measure {
    pub(super) function: Function,
    pub(super) column: Option<String>,
    pub(super) secondary_column: Option<String>,
    pub(super) quantile_level: Option<crate::query_primitive::VortexQuantileLevel>,
    pub(super) separator: Option<String>,
    pub(super) filter_column: Option<String>,
    pub(super) selection: Option<SelectionSpec>,
    pub(super) dtype: DType,
    /// Physical input proven by binding; absent for unconstrained internal specs.
    pub(super) input_ptype: Option<PType>,
    pub(super) decimal_source: Option<DecimalDType>,
    pub(super) decimal_output: Option<shardloom_core::decimal_output::DecimalOutput>,
    pub(super) distinct_fields: Vec<(String, DType)>,
    pub(super) distinct_names: Vec<String>,
}

impl Measure {
    pub(super) fn input_columns(&self) -> impl Iterator<Item = &str> {
        self.column
            .iter()
            .chain(self.secondary_column.iter())
            .chain(self.filter_column.iter())
            .map(String::as_str)
            .chain(
                self.selection
                    .iter()
                    .flat_map(|spec| spec.order.iter().map(|key| key.column.as_str())),
            )
    }
}
pub(super) struct Spec {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) groups: Vec<(String, DType)>,
    pub(super) group_names: Vec<String>,
    pub(super) measures: Vec<Measure>,
}

#[derive(Default)]
struct State {
    count: u64,
    sum: NumericAggregateSum,
    specials: super::native_float_total::Specials,
    extreme: Option<Cell>,
}

struct NativeExtreme {
    array: ArrayRef,
    key: KeyColumn,
}

/// Scalar aggregates share one group without allocating an ordinal per row.
enum GroupOrdinals {
    Scalar(usize),
    Grouped(ReservedVec<usize>),
}

impl GroupOrdinals {
    fn len(&self) -> usize {
        match self {
            Self::Scalar(rows) => *rows,
            Self::Grouped(groups) => groups.values.len(),
        }
    }

    fn at(&self, row: usize) -> usize {
        debug_assert!(row < self.len());
        match self {
            Self::Scalar(_) => 0,
            Self::Grouped(groups) => groups.values[row],
        }
    }

    fn iter(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        (0..self.len()).map(|row| (row, self.at(row)))
    }
}

/// Only adjacent compatible measures share this owner, including exact native
/// aliases of a completed projection result. A different column or
/// validity-only COUNT mode releases it before the next preparation, preserving the
/// sequential ownership bound and the original measure/error order.
struct PreparedColumn<'a> {
    name: &'a str,
    filter: Option<&'a str>,
    source: ArrayRef,
    values: Option<KeyColumn>,
    validity: Option<Mask>,
    completed_numeric_policy: Option<shardloom_core::FloatPolicy>,
}

impl PreparedColumn<'_> {
    fn matches(&self, measure: &Measure, column: &ArrayRef, count_validity: bool) -> bool {
        (measure.column.as_deref() == Some(self.name) || ArrayRef::ptr_eq(&self.source, column))
            && measure.filter_column.as_deref() == self.filter
            && self.validity.is_some() == count_validity
    }
}

pub(super) struct Aggregate<'a> {
    spec: &'a Spec,
    groups: Option<RowSet<'a>>,
    states: ReservedVec<State>,
    compact: Option<compact::Table>,
    specialize: bool,
    distinct: ReservedVec<Option<RowSet<'a>>>,
    nested_extrema: Option<ReservedVec<Option<NativeExtreme>>>,
    decimal_totals: Option<ReservedVec<native_decimal_reduce::Total>>,
    decimal_width: usize,
    statistics: Option<statistics::Table>,
    quantiles: Option<quantiles::Table>,
    selections: Option<selections::Table>,
    collections: Option<collections::Table>,
}

impl<'a> Aggregate<'a> {
    pub(super) fn new(spec: &'a Spec, memory: &LiveMemoryPool) -> Result<Self> {
        Self::with_distinct_sets(spec, memory, true)
    }

    /// The ordered executor supplies exact distinct counts and one scalar group.
    #[cfg(feature = "vortex-write")]
    pub(super) fn new_ordered(spec: &'a Spec, memory: &LiveMemoryPool) -> Result<Self> {
        if !spec.group_names.is_empty() || !spec.groups.is_empty() {
            return Err(failed("ordered reducer requires one scalar group"));
        }
        Self::with_distinct_sets(spec, memory, false)
    }

    fn with_distinct_sets(
        spec: &'a Spec,
        memory: &LiveMemoryPool,
        track_distinct: bool,
    ) -> Result<Self> {
        let mut states = ReservedVec::new(memory)?;
        let groups = if spec.group_names.is_empty() {
            states.reserve(spec.measures.len())?;
            for _ in &spec.measures {
                states.values.push(State::default());
            }
            None
        } else {
            Some(RowSet::new(&spec.groups, &spec.group_names, memory)?)
        };
        let mut distinct = ReservedVec::new(memory)?;
        distinct.reserve(spec.measures.len())?;
        for measure in &spec.measures {
            distinct
                .values
                .push(if track_distinct && measure.function.is_distinct() {
                    Some(RowSet::new(
                        &measure.distinct_fields,
                        &measure.distinct_names,
                        memory,
                    )?)
                } else {
                    None
                });
        }
        let nested_extrema = if spec.measures.iter().any(|measure| {
            native_payload::is_nested(&measure.dtype)
                && matches!(measure.function, Function::Min | Function::Max)
        }) {
            let mut values = ReservedVec::new(memory)?;
            values.reserve(states.values.len())?;
            values.values.resize_with(states.values.len(), || None);
            Some(values)
        } else {
            None
        };
        let decimal_width = spec
            .measures
            .iter()
            .filter(|measure| measure.decimal_source.is_some())
            .count();
        let decimal_totals = if decimal_width == 0 {
            None
        } else {
            let mut totals = ReservedVec::new(memory)?;
            if groups.is_none() {
                totals.reserve(decimal_width)?;
                totals
                    .values
                    .resize(decimal_width, native_decimal_reduce::Total::default());
            }
            Some(totals)
        };
        Ok(Self {
            spec,
            groups,
            states,
            compact: None,
            specialize: true,
            distinct,
            nested_extrema,
            decimal_totals,
            decimal_width,
            statistics: statistics::Table::new(spec, memory)?,
            quantiles: quantiles::Table::new(spec, memory, !track_distinct)?,
            selections: selections::Table::new(spec, memory)?,
            collections: collections::Table::new(spec, memory, !track_distinct)?,
        })
    }

    fn reserve_states(
        &mut self,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        #[cfg(test)]
        let _cost = super::native_relational_grouped_cost::scope(
            super::native_relational_grouped_cost::Phase::StateReservation,
        );
        let memory = context.memory();
        if let Some(groups) = &self.groups {
            let count = groups
                .rows()
                .checked_mul(self.spec.measures.len())
                .ok_or_else(|| failed("aggregate state cardinality overflow"))?;
            if let Some(table) = &mut self.compact {
                table.reserve_groups(groups.rows(), context)?;
            } else if self.specialize
                && self.states.values.is_empty()
                && let Some(table) =
                    compact::Table::specialize(self.spec, array, groups.rows(), context)?
            {
                self.compact = Some(table);
            } else if let Some(table) =
                compact::Table::promote(self.spec, &self.states.values, groups.rows(), context)?
            {
                self.compact = Some(table);
                // The new pages own complete old totals before these credits drop.
                self.states = ReservedVec::new(memory)?;
            } else {
                self.states.reserve(count - self.states.values.len())?;
                self.states.values.resize_with(count, State::default);
            }
        }
        if let Some(extrema) = &mut self.nested_extrema {
            extrema.reserve(self.states.values.len() - extrema.values.len())?;
            extrema
                .values
                .resize_with(self.states.values.len(), || None);
        }
        if let Some(totals) = &mut self.decimal_totals {
            let count = self
                .groups
                .as_ref()
                .map_or(1, RowSet::rows)
                .checked_mul(self.decimal_width)
                .ok_or_else(|| failed("decimal aggregate state cardinality overflow"))?;
            totals.reserve(count - totals.values.len())?;
            totals
                .values
                .resize(count, native_decimal_reduce::Total::default());
        }
        if let Some(statistics) = &mut self.statistics {
            statistics.reserve_groups(self.groups.as_ref().map_or(1, RowSet::rows))?;
        }
        if let Some(quantiles) = &mut self.quantiles {
            quantiles.reserve_groups(self.groups.as_ref().map_or(1, RowSet::rows))?;
        }
        if let Some(selections) = &mut self.selections {
            selections.reserve_groups(self.groups.as_ref().map_or(1, RowSet::rows))?;
        }
        if let Some(collections) = &mut self.collections {
            collections.reserve_groups(self.groups.as_ref().map_or(1, RowSet::rows), memory)?;
        }
        Ok(())
    }

    pub(super) fn consume(
        &mut self,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<()> {
        self.consume_inner(array, context, batch_rows, true)
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn consume_ordered_base(
        &mut self,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<()> {
        self.consume_inner(array, context, batch_rows, false)
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn add_ordered_distinct(&mut self, measure: usize, count: u64) -> Result<()> {
        if self.groups.is_some()
            || self.spec.measures.get(measure).map(|value| value.function)
                != Some(Function::CountDistinct)
            || self
                .distinct
                .values
                .get(measure)
                .is_none_or(Option::is_some)
        {
            return Err(failed("ordered distinct counter has incompatible state"));
        }
        let state = self
            .states
            .values
            .get_mut(measure)
            .ok_or_else(|| failed("ordered distinct accumulator is absent"))?;
        state.count = state
            .count
            .checked_add(count)
            .ok_or_else(|| failed("distinct count overflow"))?;
        Ok(())
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn add_ordered_quantiles(
        &mut self,
        measures: &[usize],
        values: &KeyColumn,
        rows: std::ops::Range<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if measures
            .iter()
            .any(|&index| self.spec.measures[index].function.is_quantile())
        {
            self.quantiles
                .as_mut()
                .ok_or_else(|| failed("ordered quantile state is absent"))?
                .consume_ordered(self.spec, measures, values, rows, context)?;
        }
        Ok(())
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn add_ordered_collection(
        &mut self,
        measure: usize,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let specification = self
            .spec
            .measures
            .get(measure)
            .ok_or_else(|| failed("ordered collection measure is absent"))?;
        self.collections
            .as_mut()
            .ok_or_else(|| failed("ordered collection state is absent"))?
            .consume(
                measure,
                specification,
                &GroupOrdinals::Scalar(array.len()),
                array,
                context,
            )
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn add_ordered_distinct_values(
        &mut self,
        measure: usize,
        values: &KeyColumn,
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let specification = self
            .spec
            .measures
            .get(measure)
            .ok_or_else(|| failed("ordered distinct measure is absent"))?;
        if self.groups.is_some()
            || !matches!(
                specification.function,
                Function::SumDistinct | Function::AvgDistinct
            )
            || self
                .distinct
                .values
                .get(measure)
                .is_none_or(Option::is_some)
        {
            return Err(failed("ordered distinct values have incompatible state"));
        }
        let decimal_index = self.spec.measures[..measure]
            .iter()
            .filter(|measure| measure.decimal_source.is_some())
            .count();
        for (offset, &row) in rows.iter().enumerate() {
            if offset.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let decimal = if specification.decimal_source.is_some() {
                Some(
                    self.decimal_totals
                        .as_mut()
                        .ok_or_else(|| failed("ordered distinct decimal state absent"))?
                        .values
                        .get_mut(decimal_index)
                        .ok_or_else(|| failed("ordered distinct decimal slot absent"))?,
                )
            } else {
                None
            };
            update_distinct(
                &mut self.states.values[measure],
                decimal,
                specification,
                Some(values.raw_cell(row)?),
                context,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)] // Keep the shared typed measure update loop and its ordering contract together.
    fn consume_inner(
        &mut self,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        accumulate_distinct_values: bool,
    ) -> Result<()> {
        #[cfg(test)]
        let _cost = super::native_relational_grouped_cost::scope(
            super::native_relational_grouped_cost::Phase::AggregateRemainder,
        );
        context.check_cancelled()?;
        if self.spec.measures.is_empty()
            && let Some(groups) = &mut self.groups
        {
            groups.insert_batch(array.clone(), None, context, batch_rows, None)?;
            return context.check_cancelled();
        }
        if let Some(table) = &self.compact
            && !table.accepts(self.spec, array, context)
        {
            // Internal callers may change representation across batches. Keep
            // the complete old state and lease until ordinary storage is ready.
            table.restore(self.spec, &mut self.states, context)?;
            self.compact = None;
            self.specialize = false;
        }
        let all_ordinals = if let Some(groups) = &mut self.groups {
            GroupOrdinals::Grouped(groups.intern_batch(array.clone(), context, batch_rows)?)
        } else {
            GroupOrdinals::Scalar(array.len())
        };
        self.reserve_states(array, context)?;
        let mut decimal_index = 0;
        let mut prepared: Option<PreparedColumn<'_>> = None;
        let mut selection_groups = None;
        let mut selected_filter = None;
        let mut selected = None;
        let mut numeric_reuse_from = None;
        let mut completed_extrema_end = 0;
        for (measure_index, measure) in self.spec.measures.iter().enumerate() {
            context.check_cancelled()?;
            if measure_index < completed_extrema_end {
                continue;
            }
            let reuse_from = numeric_reuse_from.take();
            let filter = measure.filter_column.as_deref();
            if selected_filter != Some(filter) {
                // Release the old domain and its value owners before preparing
                // a different selection. Only completed identical domains are reused.
                prepared = None;
                drop(selected.take());
                selected = filter::select(array, &all_ordinals, measure, context)?;
                selected_filter = Some(filter);
            }
            let (array, ordinals) = selected.as_ref().map_or((array, &all_ordinals), |input| {
                (&input.array, &input.groups)
            });
            if measure.function.is_collection() {
                if accumulate_distinct_values {
                    self.collections
                        .as_mut()
                        .ok_or_else(|| failed("collection aggregate state is absent"))?
                        .consume(measure_index, measure, ordinals, array, context)?;
                }
                prepared = None;
                continue;
            }
            if measure.function.is_selector() {
                let filtered_groups;
                let groups = if selected.is_some() {
                    filtered_groups = selections::Groups::new(ordinals, context)?;
                    &filtered_groups
                } else {
                    if selection_groups.is_none() {
                        selection_groups = Some(selections::Groups::new(&all_ordinals, context)?);
                    }
                    selection_groups.as_ref().expect("bound selector groups")
                };
                self.selections
                    .as_mut()
                    .ok_or_else(|| failed("selector aggregate state is absent"))?
                    .consume(measure_index, measure, groups, array, context)?;
                prepared = None;
                continue;
            }
            let decimal_slot = measure.decimal_source.map(|_| {
                let slot = decimal_index;
                decimal_index += 1;
                slot
            });
            if measure.function.is_moment()
                && self
                    .statistics
                    .as_ref()
                    .is_some_and(|table| !table.needs_input(measure_index))
            {
                continue;
            }
            if measure.function.is_quantile()
                && self
                    .quantiles
                    .as_ref()
                    .is_some_and(|table| !table.needs_input(measure_index))
            {
                continue;
            }
            if !accumulate_distinct_values && measure.function.is_distinct() {
                prepared = None;
                continue;
            }
            #[cfg(test)]
            let preparation_cost = super::native_relational_grouped_cost::scope(
                super::native_relational_grouped_cost::Phase::MeasurePreparation,
            );
            let column = measure
                .column
                .as_ref()
                .map(|name| logical_field_from_native_array(array, name))
                .transpose()?;
            let count_validity = measure.function == Function::Count && column.is_some();
            if !prepared.as_ref().is_some_and(|prior| {
                column
                    .as_ref()
                    .is_some_and(|column| prior.matches(measure, column, count_validity))
            }) {
                prepared = None;
                if let (Some(name), Some(array)) = (measure.column.as_deref(), &column) {
                    let (validity, values) = if count_validity {
                        (Some(parent_validity(array, context)?), None)
                    } else {
                        #[cfg(all(test, debug_assertions))]
                        super::native_relational_observation::record(|counts| {
                            counts.aggregate_value_preparations += 1;
                        });
                        (None, Some(keys(array, context)?))
                    };
                    prepared = Some(PreparedColumn {
                        name,
                        filter: measure.filter_column.as_deref(),
                        source: array.clone(),
                        values,
                        validity,
                        completed_numeric_policy: None,
                    });
                }
            }
            let values = prepared.as_ref().and_then(|column| column.values.as_ref());
            let validity = prepared
                .as_ref()
                .and_then(|column| column.validity.as_ref());
            #[cfg(test)]
            drop(preparation_cost);
            if let Some(table) = &mut self.compact {
                #[cfg(test)]
                let _cost = super::native_relational_grouped_cost::scope(
                    super::native_relational_grouped_cost::Phase::TypedUpdates,
                );
                if matches!(measure.function, Function::Min | Function::Max)
                    && let Some(KeyColumn::Numeric(owner)) = values
                {
                    let input = prepared.as_ref().expect("numeric column is prepared");
                    if let Some(end) = table.consume_pair(
                        self.spec,
                        measure_index,
                        ordinals,
                        &input.source,
                        owner,
                        input.completed_numeric_policy,
                        context,
                    )? {
                        completed_extrema_end = end;
                        if end == measure_index + 3 {
                            let valid = owner.validity().clone();
                            let input = prepared.as_mut().expect("numeric column is prepared");
                            input.values = None;
                            input.validity = Some(valid);
                            input.completed_numeric_policy = None;
                        }
                        continue;
                    }
                }
                let reuse_next = matches!(measure.function, Function::Sum | Function::Avg)
                    && matches!(values, Some(KeyColumn::Numeric(owner)) if owner.len() != 0)
                    && table.admit_next(
                        self.spec,
                        measure_index,
                        array,
                        column.as_ref().expect("numeric input column is bound"),
                        context,
                    )?;
                if let Some(source) = reuse_from {
                    table.copy_completed(source, measure_index, ordinals.len(), context)?;
                } else {
                    table.consume(measure_index, measure, ordinals, values, validity, context)?;
                }
                numeric_reuse_from = reuse_next.then_some(measure_index);
                if matches!(measure.function, Function::Sum | Function::Avg)
                    && matches!(values, Some(KeyColumn::Numeric(_)))
                {
                    prepared
                        .as_mut()
                        .expect("numeric column is prepared")
                        .completed_numeric_policy = Some(context.float_policy());
                }
                continue;
            }
            #[cfg(test)]
            let _cost = super::native_relational_grouped_cost::scope(
                super::native_relational_grouped_cost::Phase::OtherUpdates,
            );
            if measure.function.is_quantile() {
                self.quantiles
                    .as_mut()
                    .ok_or_else(|| failed("quantile aggregate state is absent"))?
                    .consume(
                        measure_index,
                        ordinals,
                        values.ok_or_else(|| failed("quantile input is absent"))?,
                        context,
                    )?;
                continue;
            }
            if measure.function.is_moment() {
                let secondary = measure
                    .secondary_column
                    .as_deref()
                    .map(|name| keys(&logical_field_from_native_array(array, name)?, context))
                    .transpose()?;
                self.statistics
                    .as_mut()
                    .ok_or_else(|| failed("statistical aggregate state is absent"))?
                    .consume(
                        measure_index,
                        ordinals,
                        values.ok_or_else(|| failed("statistical input column is absent"))?,
                        secondary.as_ref(),
                        context,
                    )?;
                continue;
            }
            if measure.function == Function::Count && column.is_none() && self.groups.is_none() {
                add_count(
                    &mut self.states.values[measure_index],
                    u64::try_from(array.len()).map_err(vortex_error)?,
                )?;
                continue;
            }
            if let Some(distinct) = self.distinct.values[measure_index].as_mut() {
                let states = &mut self.states.values;
                let decimal_totals = &mut self.decimal_totals;
                let width = self.spec.measures.len();
                let decimal_width = self.decimal_width;
                accumulate_distinct(
                    distinct,
                    ordinals,
                    column.as_ref().expect("bound distinct column"),
                    values.expect("bound distinct values"),
                    measure.function != Function::CountDistinct,
                    context,
                    batch_rows,
                    &mut |group, value| {
                        let index = group
                            .checked_mul(width)
                            .and_then(|index| index.checked_add(measure_index))
                            .ok_or_else(|| failed("distinct state index overflow"))?;
                        let state = states
                            .get_mut(index)
                            .ok_or_else(|| failed("distinct group has no accumulator"))?;
                        let decimal = decimal_slot
                            .map(|slot| {
                                let index = group
                                    .checked_mul(decimal_width)
                                    .and_then(|index| index.checked_add(slot))
                                    .ok_or_else(|| {
                                        failed("distinct decimal state index overflow")
                                    })?;
                                decimal_totals
                                    .as_mut()
                                    .ok_or_else(|| failed("distinct decimal state absent"))?
                                    .values
                                    .get_mut(index)
                                    .ok_or_else(|| failed("distinct decimal slot absent"))
                            })
                            .transpose()?;
                        update_distinct(state, decimal, measure, value, context)
                    },
                )?;
                continue;
            }
            if let Some(source) = measure.decimal_source {
                let totals = self
                    .decimal_totals
                    .as_mut()
                    .ok_or_else(|| failed("decimal aggregate state absent"))?;
                accumulate_decimal(
                    &mut totals.values,
                    ordinals,
                    values.ok_or_else(|| failed("decimal aggregate input absent"))?,
                    self.decimal_width,
                    decimal_slot.ok_or_else(|| failed("decimal aggregate slot absent"))?,
                    source,
                    context,
                )?;
                continue;
            }
            if matches!(measure.function, Function::Sum | Function::Avg)
                && let Some(KeyColumn::Numeric(owner)) = values
            {
                let reuse_next = owner.len() != 0
                    && numeric_reuse::admit_next(
                        self.spec,
                        measure_index,
                        array,
                        column.as_ref().expect("numeric input column is bound"),
                        &self.states.values,
                        context,
                    )?;
                if let Some(source) = reuse_from {
                    numeric_reuse::copy_completed(
                        &mut self.states.values,
                        self.spec.measures.len(),
                        source,
                        measure_index,
                        owner.len(),
                        context,
                    )?;
                } else if self.groups.is_none() {
                    accumulate_numeric(&mut self.states.values[measure_index], owner, context)?;
                } else {
                    accumulate_grouped_numeric(
                        &mut self.states.values,
                        ordinals,
                        self.spec.measures.len(),
                        measure_index,
                        owner,
                        context,
                    )?;
                }
                numeric_reuse_from = reuse_next.then_some(measure_index);
                prepared
                    .as_mut()
                    .expect("numeric column is prepared")
                    .completed_numeric_policy = Some(context.float_policy());
                continue;
            }
            if matches!(measure.function, Function::Min | Function::Max)
                && let Some(KeyColumn::Numeric(owner)) = values
            {
                if let Some(end) = extrema_pair::accumulate(
                    self.spec,
                    measure_index,
                    &mut self.states.values,
                    ordinals,
                    &prepared
                        .as_ref()
                        .expect("numeric column is prepared")
                        .source,
                    owner,
                    context,
                )? {
                    completed_extrema_end = end;
                    if end == measure_index + 3 {
                        // COUNT would replace value preparation with this same
                        // source's executed mask. Keep that ownership/reuse state.
                        let valid = owner.validity().clone();
                        let prepared = prepared.as_mut().expect("numeric column is prepared");
                        prepared.values = None;
                        prepared.validity = Some(valid);
                    }
                    continue;
                }
                extrema::accumulate(
                    &mut self.states.values,
                    ordinals,
                    self.spec.measures.len(),
                    measure_index,
                    measure.function == Function::Min,
                    owner,
                    context,
                )?;
                continue;
            }
            for (row, group) in ordinals.iter() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let state_index = group * self.spec.measures.len() + measure_index;
                if let Some(extrema) = &mut self.nested_extrema
                    && native_payload::is_nested(&measure.dtype)
                {
                    update_native_extreme(
                        &mut extrema.values[state_index],
                        measure,
                        column
                            .as_ref()
                            .ok_or_else(|| failed("nested extremum column absent"))?,
                        values.ok_or_else(|| failed("nested extremum keys absent"))?,
                        row,
                        context,
                    )?;
                    continue;
                }
                let state = &mut self.states.values[state_index];
                if count_validity {
                    if validity
                        .ok_or_else(|| failed("COUNT validity absent"))?
                        .value(row)
                    {
                        update(state, measure, None, context)?;
                    }
                    continue;
                }
                let value = values.map(|values| values.raw_cell(row)).transpose()?;
                #[cfg(all(test, debug_assertions))]
                super::native_relational_observation::record(|counts| {
                    counts.aggregate_row_updates += 1;
                });
                update(state, measure, value, context)?;
            }
        }
        context.check_cancelled()
    }

    #[allow(clippy::too_many_lines)] // One bounded output batch shares the group, collection and numeric state owners.
    pub(super) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        #[cfg(test)]
        let _cost = super::native_relational_grouped_cost::scope(
            super::native_relational_grouped_cost::Phase::Finalization,
        );
        let count = self.groups.as_ref().map_or(1, RowSet::rows);
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(count.min(batch_rows))?;
        for start in (0..count).step_by(batch_rows) {
            context.check_cancelled()?;
            let end = count.min(start.saturating_add(batch_rows));
            rows.values.clear();
            rows.values.extend((start..end).map(Some));
            let mut columns = ReservedVec::new(context.memory())?;
            columns.reserve(self.spec.fields.len())?;
            if let Some(groups) = &self.groups {
                let gather = groups.gather(&rows.values, context)?;
                for (name, dtype) in &self.spec.groups {
                    columns.values.push(gather.column(name, dtype, context)?);
                }
            }
            let mut decimal_index = 0;
            for (index, measure) in self.spec.measures.iter().enumerate() {
                if measure.function.is_ordered_value() {
                    columns.values.push(self.finish_ordered(
                        index,
                        measure,
                        start..end,
                        context,
                    )?);
                    continue;
                }
                if native_payload::is_nested(&measure.dtype) {
                    let extrema = self
                        .nested_extrema
                        .as_ref()
                        .ok_or_else(|| failed("nested aggregate state absent"))?;
                    columns.values.push(native_payload::retained_column(
                        &measure.dtype,
                        end - start,
                        context,
                        |row| {
                            let state = extrema
                                .values
                                .get((start + row) * self.spec.measures.len() + index)
                                .ok_or_else(|| failed("nested aggregate state index absent"))?;
                            Ok(state.as_ref().map(|extreme| extreme.array.clone()))
                        },
                    )?);
                    continue;
                }
                let decimal = measure.decimal_source.map(|source| {
                    let index = decimal_index;
                    decimal_index += 1;
                    (index, source)
                });
                columns
                    .values
                    .push(result_batch::build_column_with_float_policy(
                        &measure.dtype,
                        end - start,
                        &context.native_session().host_allocator(),
                        context.float_policy(),
                        |row| {
                            if row.is_multiple_of(1024) {
                                context.check_cancelled()?;
                            }
                            if let Some((index, source)) = decimal {
                                let totals = self
                                    .decimal_totals
                                    .as_ref()
                                    .ok_or_else(|| failed("decimal aggregate state absent"))?;
                                let DType::Decimal(output, _) = measure.dtype else {
                                    return Err(failed("decimal aggregate output type changed"));
                                };
                                return totals.values[(start + row) * self.decimal_width + index]
                                    .finish_with(
                                        source,
                                        measure.function.is_average(),
                                        measure.decimal_output,
                                    )
                                    .map(|value| {
                                        value.map_or(Value::Null, |value| {
                                            Value::Decimal(value, output)
                                        })
                                    });
                            }
                            self.finish_numeric(index, start + row, measure, context)
                        },
                    )?);
            }
            let (columns, _ownership) = columns.into_parts();
            let output = StructArray::try_new(
                self.spec
                    .fields
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<FieldNames>(),
                columns,
                end - start,
                Validity::NonNullable,
            )
            .map_err(vortex_error)?
            .into_array();
            #[cfg(test)]
            let downstream_cost = super::native_relational_grouped_cost::scope(
                super::native_relational_grouped_cost::Phase::DownstreamExcluded,
            );
            consume(output)?;
            #[cfg(test)]
            drop(downstream_cost);
            context.check_cancelled()?;
        }
        Ok(())
    }

    fn finish_ordered(
        &self,
        index: usize,
        measure: &Measure,
        groups: std::ops::Range<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if measure.function.is_collection() {
            self.collections
                .as_ref()
                .ok_or_else(|| failed("collection aggregate state is absent"))?
                .finish(index, measure, groups, context)
        } else {
            self.selections
                .as_ref()
                .ok_or_else(|| failed("selector aggregate state is absent"))?
                .finish(index, measure, groups, context)
        }
    }

    fn finish_numeric(
        &self,
        index: usize,
        group: usize,
        measure: &Measure,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Value<'_>> {
        if let Some(table) = &self.compact {
            return table.finish(index, group, measure.function);
        }
        if measure.function.is_moment() {
            return self
                .statistics
                .as_ref()
                .ok_or_else(|| failed("statistical aggregate state is absent"))?
                .finish(index, group, measure.function)
                .map(|value| value.map_or(Value::Null, Value::Float));
        }
        if measure.function.is_quantile() {
            return self
                .quantiles
                .as_ref()
                .ok_or_else(|| failed("quantile aggregate state is absent"))?
                .finish(index, group, measure, context);
        }
        final_value(
            &self.states.values[group * self.spec.measures.len() + index],
            measure,
        )
    }
}

fn accumulate_numeric(
    state: &mut State,
    owner: &NativeNumericOwner,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if owner.len() == 0 {
        return Ok(());
    }
    #[cfg(all(test, debug_assertions))]
    super::native_relational_observation::record(|counts| {
        counts.aggregate_numeric_bound_batches += 1;
        counts.aggregate_numeric_bound_rows += owner.len() as u64;
    });
    owner.visit_numeric(|row, value, integer| {
        // NULL rows still participate in the original cancellation cadence.
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if let Some(value) = value {
            update_numeric(state, value, integer, context.float_policy())?;
        }
        Ok(())
    })
}

fn update_numeric(
    state: &mut State,
    value: f64,
    integer: bool,
    policy: shardloom_core::FloatPolicy,
) -> Result<()> {
    update_numeric_parts(
        &mut state.count,
        &mut state.sum,
        &mut state.specials,
        value,
        integer,
        policy,
    )
}

fn update_numeric_parts(
    count: &mut u64,
    sum: &mut NumericAggregateSum,
    specials: &mut super::native_float_total::Specials,
    value: f64,
    integer: bool,
    policy: shardloom_core::FloatPolicy,
) -> Result<()> {
    // raw_cell validated a nonfinite input before update changed the count.
    if !policy.admits(value) {
        validate_finite_scalar(value)?;
    }
    update_count(count, 1)?;
    if specials.add(value)? {
        return Ok(());
    }
    sum.add(value, integer);
    if !sum.is_finite() {
        return Err(failed("aggregate sum became nonfinite"));
    }
    Ok(())
}

fn accumulate_grouped_numeric(
    states: &mut [State],
    ordinals: &GroupOrdinals,
    width: usize,
    measure_index: usize,
    owner: &NativeNumericOwner,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if owner.len() != ordinals.len() {
        return Err(failed("numeric group ordinals changed row count"));
    }
    if owner.len() == 0 {
        return Ok(());
    }
    // Dispatch once around the whole borrowed batch, retaining the same
    // per-measure, per-row update sequence without a boxed call for every row.
    #[cfg(all(test, debug_assertions))]
    super::native_relational_observation::record(|counts| {
        counts.aggregate_numeric_bound_batches += 1;
        counts.aggregate_numeric_bound_rows += owner.len() as u64;
    });
    owner.visit_numeric(|row, value, integer| {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if let Some(value) = value {
            update_numeric(
                &mut states[ordinals.at(row) * width + measure_index],
                value,
                integer,
                context.float_policy(),
            )?;
        }
        Ok(())
    })
}

fn accumulate_decimal(
    totals: &mut [native_decimal_reduce::Total],
    ordinals: &GroupOrdinals,
    values: &KeyColumn,
    width: usize,
    index: usize,
    source: DecimalDType,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    for (row, group) in ordinals.iter() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        match values.raw_cell(row)? {
            Cell::Null => {}
            Cell::Decimal(value, dtype) if dtype == source => {
                totals[group * width + index].add(value, source)?;
            }
            _ => return Err(failed("decimal aggregate input domain changed")),
        }
    }
    Ok(())
}

fn update_native_extreme(
    state: &mut Option<NativeExtreme>,
    measure: &Measure,
    array: &ArrayRef,
    values: &KeyColumn,
    row: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if values.is_null(row)? {
        return Ok(());
    }
    let replace = match state {
        None => true,
        Some(prior) => {
            let order = values.compare_at(row, &prior.key, 0)?;
            match measure.function {
                Function::Min => order.is_lt(),
                Function::Max => order.is_gt(),
                _ => return Err(failed("nested aggregate output requires MIN or MAX")),
            }
        }
    };
    if replace {
        let indices = index_array(1, false, context, |_| Ok(Some(row)))?;
        let selected = native_payload::take(array, &indices, &measure.dtype, context)?;
        let key = keys(&selected, context)?;
        // Construct while the previous owner is still credited. Only selected
        // child buffers survive the input batch, and replacement then drops it.
        *state = Some(NativeExtreme {
            array: selected,
            key,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // A bounded batch updates one measure's state through the shared exact set.
fn accumulate_distinct(
    distinct: &mut RowSet<'_>,
    groups: &GroupOrdinals,
    column: &ArrayRef,
    values: &KeyColumn,
    observe_values: bool,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    consume: &mut dyn FnMut(usize, Option<Cell>) -> Result<()>,
) -> Result<()> {
    let mut rows = ReservedVec::new(context.memory())?;
    rows.reserve(groups.len())?;
    for row in 0..groups.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if !values.is_null(row)? {
            rows.values.push(row);
        }
    }
    if rows.values.is_empty() {
        return Ok(());
    }
    let ordinals = index_array(rows.values.len(), false, context, |row| {
        Ok(Some(groups.at(rows.values[row])))
    })?;
    let selection = index_array(rows.values.len(), false, context, |row| {
        Ok(Some(rows.values[row]))
    })?;
    let column = column.take(selection).map_err(vortex_error)?;
    let input = StructArray::try_new(
        FieldNames::from(["group", "value"]),
        vec![ordinals, column],
        rows.values.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    distinct.insert_batch(
        input,
        None,
        context,
        batch_rows,
        Some(&mut |array| {
            let groups = keys(&logical_field_from_native_array(&array, "group")?, context)?;
            let values = observe_values
                .then(|| keys(&logical_field_from_native_array(&array, "value")?, context))
                .transpose()?;
            for row in 0..array.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let Cell::NonnegativeInteger(group) = groups.cell(row)? else {
                    return Err(failed("distinct group ordinal is invalid"));
                };
                consume(
                    usize::try_from(group).map_err(vortex_error)?,
                    values
                        .as_ref()
                        .map(|values| values.raw_cell(row))
                        .transpose()?,
                )?;
            }
            Ok(())
        }),
    )?;
    Ok(())
}

fn update_distinct(
    state: &mut State,
    decimal: Option<&mut native_decimal_reduce::Total>,
    measure: &Measure,
    value: Option<Cell>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if measure.function == Function::CountDistinct {
        return add_count(state, 1);
    }
    if let Some(source) = measure.decimal_source {
        let Some(Cell::Decimal(value, actual)) = value else {
            return Err(failed("distinct decimal observation changed type"));
        };
        if source != actual {
            return Err(failed("distinct decimal observation changed metadata"));
        }
        decimal
            .ok_or_else(|| failed("distinct decimal accumulator absent"))?
            .add(value, source)
    } else {
        update(state, measure, value, context)
    }
}

fn add_count(state: &mut State, count: u64) -> Result<()> {
    update_count(&mut state.count, count)
}

fn update_count(state: &mut u64, count: u64) -> Result<()> {
    *state = state
        .checked_add(count)
        .ok_or_else(|| failed("aggregate count overflow"))?;
    Ok(())
}

fn update(
    state: &mut State,
    measure: &Measure,
    value: Option<Cell>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if value == Some(Cell::Null) {
        return Ok(());
    }
    add_count(state, 1)?;
    match measure.function {
        Function::Count => Ok(()),
        Function::Sum | Function::Avg | Function::SumDistinct | Function::AvgDistinct => {
            update_sum_parts(
                &mut state.sum,
                &mut state.specials,
                value.as_ref(),
                context.float_policy(),
            )
        }
        Function::Min | Function::Max => {
            let value = value.ok_or_else(|| failed("extremum argument absent"))?;
            let replace = match &state.extreme {
                None => true,
                Some(prior) => {
                    let order = compare_cells(value.clone(), prior.clone())?;
                    if measure.function == Function::Min {
                        order.is_lt()
                    } else {
                        order.is_gt()
                    }
                }
            };
            if replace {
                state.extreme = Some(owned_extreme(value, context)?);
            }
            Ok(())
        }
        Function::CountDistinct => Err(failed("distinct state must use the exact native set")),
        Function::List | Function::StringAgg => {
            Err(failed("collection state requires its native value owner"))
        }
        Function::Median | Function::QuantileCont | Function::QuantileDisc => {
            Err(failed("quantile state requires its exact native owner"))
        }
        Function::First | Function::Last | Function::ArgMin | Function::ArgMax => Err(failed(
            "ordered selector state requires its native payload owner",
        )),
        Function::VarPop
        | Function::VarSamp
        | Function::StddevPop
        | Function::StddevSamp
        | Function::CovarPop
        | Function::CovarSamp
        | Function::Corr => Err(failed(
            "statistical state must use its centered native owner",
        )),
    }
}

fn update_sum_parts(
    sum: &mut NumericAggregateSum,
    specials: &mut super::native_float_total::Specials,
    value: Option<&Cell>,
    policy: shardloom_core::FloatPolicy,
) -> Result<()> {
    let value = value.ok_or_else(|| failed("numeric argument absent"))?;
    let numeric = number(value)?;
    if !policy.admits(numeric) {
        validate_finite_scalar(numeric)?;
    }
    if specials.add(numeric)? {
        return Ok(());
    }
    sum.add(
        numeric,
        matches!(
            value,
            Cell::NegativeInteger(_) | Cell::NonnegativeInteger(_)
        ),
    );
    if !sum.is_finite() {
        return Err(failed("aggregate sum became nonfinite"));
    }
    Ok(())
}

fn owned_extreme(value: Cell, context: &NativeExecutionContext<'_>) -> Result<Cell> {
    let (Cell::Utf8(bytes) | Cell::Binary(bytes)) = &value else {
        return Ok(value);
    };
    let mut owned = context
        .native_session()
        .host_allocator()
        .allocate(bytes.len(), Alignment::new(1))
        .map_err(vortex_error)?;
    owned.as_mut_slice().copy_from_slice(bytes.as_slice());
    Ok(if matches!(value, Cell::Binary(_)) {
        Cell::Binary(owned.freeze())
    } else {
        Cell::Utf8(owned.freeze())
    })
}

#[allow(clippy::cast_precision_loss)] // Preserve the shared primitive integer-to-F64 conversion before accumulation.
pub(super) fn number(value: &Cell) -> Result<f64> {
    match value {
        Cell::NegativeInteger(value) => Ok(*value as f64),
        Cell::NonnegativeInteger(value) => Ok(*value as f64),
        Cell::Float(bits) => Ok(f64::from_bits(*bits)),
        _ => Err(failed(
            "numeric aggregate requires an integer or floating value",
        )),
    }
}

fn final_value<'a>(state: &'a State, measure: &Measure) -> Result<Value<'a>> {
    Ok(match measure.function {
        Function::List | Function::StringAgg => {
            return Err(failed(
                "collection result requires its native payload owner",
            ));
        }
        Function::First | Function::Last | Function::ArgMin | Function::ArgMax => {
            return Err(failed(
                "ordered selector result requires its native payload owner",
            ));
        }
        Function::Median | Function::QuantileCont | Function::QuantileDisc => {
            return Err(failed("quantile result requires its exact native owner"));
        }
        Function::VarPop
        | Function::VarSamp
        | Function::StddevPop
        | Function::StddevSamp
        | Function::CovarPop
        | Function::CovarSamp
        | Function::Corr => {
            return Err(failed(
                "statistical result requires its centered native owner",
            ));
        }
        Function::Count | Function::CountDistinct => Value::UInt(state.count),
        Function::Sum | Function::Avg | Function::SumDistinct | Function::AvgDistinct
            if state.count == 0 =>
        {
            Value::Null
        }
        Function::Sum | Function::SumDistinct => {
            Value::Float(state.specials.result().unwrap_or_else(|| state.sum.total()))
        }
        Function::Avg | Function::AvgDistinct => Value::Float(
            state
                .specials
                .result()
                .unwrap_or_else(|| super::simple_average_value(state.sum.total(), state.count)),
        ),
        Function::Min | Function::Max => match &state.extreme {
            None | Some(Cell::Null) => Value::Null,
            Some(Cell::NegativeInteger(value) | Cell::Timestamp(value) | Cell::Temporal(value)) => {
                Value::Int(*value)
            }
            Some(Cell::NonnegativeInteger(value)) => {
                if matches!(
                    measure.dtype,
                    DType::Primitive(PType::I8 | PType::I16 | PType::I32 | PType::I64, _)
                ) {
                    Value::Int(i64::try_from(*value).map_err(vortex_error)?)
                } else {
                    Value::UInt(*value)
                }
            }
            Some(Cell::Float(bits)) => Value::Float(f64::from_bits(*bits)),
            Some(Cell::Boolean(value)) => Value::Bool(*value),
            Some(Cell::Utf8(bytes)) => Value::Text(Cow::Borrowed(
                std::str::from_utf8(bytes.as_slice()).map_err(vortex_error)?,
            )),
            Some(Cell::Binary(bytes)) => Value::Binary(Cow::Borrowed(bytes.as_slice())),
            Some(Cell::Decimal(value, dtype)) => Value::Decimal(*value, *dtype),
            Some(Cell::Date(value)) => Value::Int(i64::from(*value)),
        },
    })
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use crate::resident_session::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;
    use vortex::array::{
        VortexSessionExecute as _,
        arrays::{ListViewArray, PrimitiveArray, VarBinArray},
    };
    use vortex::buffer::ByteBuffer;

    #[test]
    fn native_typed_keys_binary_extremum_owns_only_selected_bytes_and_credits() {
        let source = ByteBuffer::from(vec![255u8; 2 << 20]);
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let owned = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                owned_extreme(Cell::Binary(source.slice(2..6)), context)
            })
            .unwrap();
        drop((source, session));
        assert!(memory.snapshot().reserved_bytes > 0);
        assert!(memory.snapshot().reserved_bytes < 1024);
        let Cell::Binary(bytes) = &owned else {
            panic!("binary logical identity changed")
        };
        assert_eq!(bytes.as_slice(), &[255; 4]);
        let retained = owned.clone();
        drop(owned);
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn native_nested_extremum_compacts_children_and_admits_replacement_before_releasing_old_state()
    {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let mut state = None;
        let list = |values: ArrayRef, offset: u64| {
            ListViewArray::try_new(
                values,
                PrimitiveArray::from_iter([offset]).into_array(),
                PrimitiveArray::from_iter([1u64]).into_array(),
                Validity::NonNullable,
                &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                    &vortex::array::array_session(),
                ),
            )
            .unwrap()
            .into_array()
        };
        let mut measure = Measure {
            secondary_column: None,
            filter_column: None,
            quantile_level: None,
            selection: None,
            separator: None,
            function: Function::Min,
            column: Some("items".into()),
            dtype: DType::Null,
            decimal_source: None,
            input_ptype: None,
            decimal_output: None,
            distinct_fields: vec![],
            distinct_names: vec![],
        };
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let huge = vec![255u8; 2 << 20];
                let values = result_batch::build_column(
                    &DType::Binary(vortex::array::dtype::Nullability::NonNullable),
                    2,
                    &context.native_session().host_allocator(),
                    |row| {
                        Ok(Value::Binary(Cow::Borrowed(if row == 0 {
                            &huge
                        } else {
                            b"\x02"
                        })))
                    },
                )?;
                let array = list(values, 1);
                measure.dtype = array.dtype().as_nullable();
                let key = keys(&array, context)?;
                update_native_extreme(&mut state, &measure, &array, &key, 0, context)?;
                assert!(memory.snapshot().reserved_bytes >= 2 << 20);
                drop((array, key));
                assert!(
                    memory.snapshot().reserved_bytes < 64 << 10,
                    "extremum retained the unused child domain"
                );
                let retained_bytes = memory.snapshot().reserved_bytes;
                let old = state.as_ref().unwrap().array.clone();
                let next = list(VarBinArray::from(vec![&b"\x01"[..]]).into_array(), 0);
                let next_key = keys(&next, context)?;
                let blocked = memory.reserve(
                    memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes - 1024,
                )?;
                let error =
                    update_native_extreme(&mut state, &measure, &next, &next_key, 0, context)
                        .unwrap_err();
                assert!(error.to_string().contains("reservation denied"), "{error}");
                assert!(ArrayRef::ptr_eq(&state.as_ref().unwrap().array, &old));
                drop(blocked);
                drop(next_key);
                assert_eq!(memory.snapshot().reserved_bytes, retained_bytes);
                let next_key = keys(&next, context)?;
                update_native_extreme(&mut state, &measure, &next, &next_key, 0, context)?;
                assert!(!ArrayRef::ptr_eq(&state.as_ref().unwrap().array, &old));
                drop((old, next, next_key));
                assert!(memory.snapshot().reserved_bytes < 64 << 10);
                Ok(())
            })
            .unwrap();
        let selected = state.as_ref().unwrap().array.clone();
        drop((state, session));
        assert!(memory.snapshot().reserved_bytes > 0);
        let mut execution = vortex::array::array_session().create_execution_ctx();
        assert_eq!(
            selected
                .execute_scalar(0, &mut execution)
                .unwrap()
                .as_list()
                .elements()
                .unwrap(),
            vec![vortex::array::scalar::Scalar::from(&b"\x01"[..])]
        );
        drop(selected);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
