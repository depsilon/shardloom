//! Group collections retain compact native segments and credited row selections.

use super::{Function, GroupOrdinals, Measure, Spec, failed};
use crate::{
    local_primitives::{
        logical_field_from_native_array,
        native_capacity::ReservedVec,
        native_collection_result, native_payload,
        native_relational_batch::{Batch, Table as Batches, take_batch},
        native_relational_expression::parent_validity,
        native_relational_order, native_relational_sort,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{cmp::Ordering, ops::Range};
use vortex::array::{ArrayRef, dtype::DType};

struct State {
    batches: Batches,
    groups: ReservedVec<ReservedVec<usize>>,
}

pub(super) struct Table {
    states: ReservedVec<Option<State>>,
    ordered: bool,
}

impl Table {
    pub(super) fn new(spec: &Spec, memory: &LiveMemoryPool, ordered: bool) -> Result<Option<Self>> {
        if !spec
            .measures
            .iter()
            .any(|measure| measure.function.is_collection())
        {
            return Ok(None);
        }
        let mut states = ReservedVec::new(memory)?;
        states.reserve(spec.measures.len())?;
        for measure in &spec.measures {
            states.values.push(if measure.function.is_collection() {
                Some(State {
                    batches: Batches::new(memory)?,
                    groups: ReservedVec::new(memory)?,
                })
            } else {
                None
            });
        }
        let mut table = Self { states, ordered };
        if spec.groups.is_empty() {
            table.reserve_groups(1, memory)?;
        }
        Ok(Some(table))
    }

    pub(super) fn reserve_groups(&mut self, count: usize, memory: &LiveMemoryPool) -> Result<()> {
        for state in self.states.values.iter_mut().flatten() {
            state
                .groups
                .reserve(count.saturating_sub(state.groups.values.len()))?;
            while state.groups.values.len() < count {
                state.groups.values.push(ReservedVec::new(memory)?);
            }
        }
        Ok(())
    }

    pub(super) fn consume(
        &mut self,
        index: usize,
        measure: &Measure,
        groups: &GroupOrdinals,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let spec = measure
            .selection
            .as_ref()
            .ok_or_else(|| failed("collection input is absent"))?;
        let state = self.states.values[index]
            .as_mut()
            .ok_or_else(|| failed("collection state is absent"))?;
        let validity = if measure.function == Function::StringAgg {
            Some(parent_validity(
                &logical_field_from_native_array(
                    array,
                    measure
                        .column
                        .as_deref()
                        .ok_or_else(|| failed("collection value is absent"))?,
                )?,
                context,
            )?)
        } else {
            None
        };
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(array.len())?;
        for row in 0..array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if validity.as_ref().is_none_or(|validity| validity.value(row)) {
                selected.values.push(row);
            }
        }
        if selected.values.is_empty() {
            return Ok(());
        }
        // Stored records have already been ordered. Retain only their values;
        // rebuilding comparison keys would defeat the bounded external sort.
        let fields = if self.ordered {
            &spec.fields[..1]
        } else {
            &spec.fields
        };
        let names = if self.ordered { &[][..] } else { &spec.names };
        let compact = take_batch(array, fields, &selected.values, context)?;
        let batch = Batch::for_ordering(compact, names, context)?;
        if !self.ordered {
            for row in 0..selected.values.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                batch.validate_keys(row)?;
                for (key, order) in spec.order.iter().enumerate() {
                    if order.nulls.is_none() && batch.key_is_null(row, key)? {
                        return Err(failed(
                            "aggregate ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
                        ));
                    }
                }
            }
        }
        let range = state.batches.push(batch)?;
        for (local, &row) in selected.values.iter().enumerate() {
            if local.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            state.groups.values[groups.at(row)].push(range.start + local)?;
        }
        Ok(())
    }

    pub(super) fn finish(
        &self,
        index: usize,
        measure: &Measure,
        groups: Range<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let state = self.states.values[index]
            .as_ref()
            .ok_or_else(|| failed("collection state is absent"))?;
        let spec = measure
            .selection
            .as_ref()
            .ok_or_else(|| failed("collection input is absent"))?;
        let column = measure
            .column
            .as_deref()
            .ok_or_else(|| failed("collection value is absent"))?;
        let source_dtype = match &measure.dtype {
            DType::List(child, _) => child.as_ref(),
            dtype => dtype,
        };
        // One-row results retain their own native buffers; the shared output
        // boundary compacts them into a single typed column under a new grant.
        let mut results = ReservedVec::new(context.memory())?;
        results.reserve(groups.len())?;
        for group in groups {
            context.check_cancelled()?;
            let mut rows = ReservedVec::new(context.memory())?;
            rows.reserve(state.groups.values[group].values.len())?;
            rows.values
                .extend_from_slice(&state.groups.values[group].values);
            if !self.ordered && !spec.order.is_empty() {
                native_relational_order::sort(
                    &mut rows.values,
                    context.cancellation(),
                    |left, right| {
                        let (a, arow) = state.batches.locate_batch(left)?;
                        let (b, brow) = state.batches.locate_batch(right)?;
                        for (key, order) in spec.order.iter().enumerate() {
                            let order = native_relational_sort::compare_key(
                                order,
                                a.key_is_null(arow, key)?,
                                b.key_is_null(brow, key)?,
                                || a.compare_key(arow, b, brow, key),
                            )?;
                            if order != Ordering::Equal {
                                return Ok(order);
                            }
                        }
                        Ok(left.cmp(&right))
                    },
                )?;
            }
            let mut selection = ReservedVec::new(context.memory())?;
            selection.reserve(rows.values.len())?;
            selection
                .values
                .extend(rows.values.iter().copied().map(Some));
            let values = state
                .batches
                .gather(&selection.values, false, context)?
                .column(column, source_dtype, context)?;
            results.values.push(native_collection_result::build(
                values,
                &measure.dtype,
                measure.separator.as_deref(),
                context,
            )?);
        }
        native_payload::retained_column(&measure.dtype, results.values.len(), context, |row| {
            Ok(Some(results.values[row].clone()))
        })
    }
}
