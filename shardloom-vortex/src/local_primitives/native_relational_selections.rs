//! Ordered selectors retain one compact native candidate per group. Batch
//! comparisons choose a winning ordinal before gathering its value and keys.

use super::{Function, GroupOrdinals, Measure, Spec as AggregateSpec, failed};
use crate::{
    local_primitives::{
        logical_field_from_native_array,
        native_capacity::ReservedVec,
        native_payload,
        native_relational_batch::{Batch, take_batch_with_policy},
        native_relational_expression::parent_validity,
        native_relational_order, native_relational_sort,
    },
    relational_query::VortexRelationalOrderKey,
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{cmp::Ordering, ops::Range};
use vortex::array::{ArrayRef, dtype::DType};

pub(in crate::local_primitives) struct Spec {
    pub(in crate::local_primitives) fields: Vec<(String, DType)>,
    /// The argument key comes first, followed by aggregate-local order keys.
    pub(in crate::local_primitives) names: Vec<String>,
    pub(in crate::local_primitives) order: Vec<VortexRelationalOrderKey>,
}

/// Group only the current batch's ordinals, preserving source order per group.
/// A scalar reduction allocates no row-to-group map.
pub(super) enum Groups<'a> {
    Scalar(usize),
    Grouped {
        rows: ReservedVec<usize>,
        groups: &'a GroupOrdinals,
    },
}

enum Rows<'a> {
    Scalar(usize),
    Grouped(&'a [usize]),
}

impl Rows<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Scalar(rows) => *rows,
            Self::Grouped(rows) => rows.len(),
        }
    }

    fn at(&self, index: usize) -> usize {
        match self {
            Self::Scalar(_) => index,
            Self::Grouped(rows) => rows[index],
        }
    }
}

impl<'a> Groups<'a> {
    pub(super) fn new(
        groups: &'a GroupOrdinals,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        if let GroupOrdinals::Scalar(rows) = groups {
            return Ok(Self::Scalar(*rows));
        }
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(groups.len())?;
        for row in 0..groups.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            rows.values.push(row);
        }
        native_relational_order::sort(&mut rows.values, context.cancellation(), |left, right| {
            Ok(groups
                .at(left)
                .cmp(&groups.at(right))
                .then_with(|| left.cmp(&right)))
        })?;
        Ok(Self::Grouped { rows, groups })
    }

    fn visit(&self, mut consume: impl FnMut(usize, Rows<'_>) -> Result<()>) -> Result<()> {
        match self {
            Self::Scalar(rows) => consume(0, Rows::Scalar(*rows)),
            Self::Grouped { rows, groups } => {
                let mut start = 0;
                while start < rows.values.len() {
                    let group = groups.at(rows.values[start]);
                    let mut end = start + 1;
                    while end < rows.values.len() && groups.at(rows.values[end]) == group {
                        end += 1;
                    }
                    consume(group, Rows::Grouped(&rows.values[start..end]))?;
                    start = end;
                }
                Ok(())
            }
        }
    }
}

pub(super) struct Table {
    slots: ReservedVec<Option<usize>>,
    states: ReservedVec<Option<Batch>>,
    width: usize,
}

impl Table {
    pub(super) fn new(spec: &AggregateSpec, memory: &LiveMemoryPool) -> Result<Option<Self>> {
        if !spec
            .measures
            .iter()
            .any(|measure| measure.function.is_selector())
        {
            return Ok(None);
        }
        let mut slots = ReservedVec::new(memory)?;
        slots.reserve(spec.measures.len())?;
        let mut width = 0;
        for measure in &spec.measures {
            slots.values.push(if measure.function.is_selector() {
                let slot = width;
                width += 1;
                Some(slot)
            } else {
                None
            });
        }
        let mut table = Self {
            slots,
            states: ReservedVec::new(memory)?,
            width,
        };
        if spec.groups.is_empty() {
            table.reserve_groups(1)?;
        }
        Ok(Some(table))
    }

    pub(super) fn reserve_groups(&mut self, groups: usize) -> Result<()> {
        let count = groups
            .checked_mul(self.width)
            .ok_or_else(|| failed("selector group state cardinality overflow"))?;
        self.states.reserve(count - self.states.values.len())?;
        self.states.values.resize_with(count, || None);
        Ok(())
    }

    pub(super) fn consume(
        &mut self,
        index: usize,
        measure: &Measure,
        groups: &Groups<'_>,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let spec = measure
            .selection
            .as_ref()
            .ok_or_else(|| failed("ordered selector specification is absent"))?;
        let slot =
            self.slots.values[index].ok_or_else(|| failed("ordered selector slot is absent"))?;
        let input = Batch::for_ordering(array.clone(), &spec.names, context)?;
        let paired = measure.function.requires_secondary();
        let validity = if paired {
            Some(parent_validity(
                &logical_field_from_native_array(
                    array,
                    measure
                        .column
                        .as_deref()
                        .ok_or_else(|| failed("selector value is absent"))?,
                )?,
                context,
            )?)
        } else {
            None
        };
        groups.visit(|group, rows| {
            context.check_cancelled()?;
            let state = &mut self.states.values[group * self.width + slot];
            let mut selected = None;
            for offset in 0..rows.len() {
                if offset.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let row = rows.at(offset);
                if paired
                    && (!validity.as_ref().expect("paired validity").value(row)
                        || input.key_is_null(row, 0)?)
                {
                    continue;
                }
                validate(spec, paired, &input, row)?;
                let prior = selected
                    .map(|row| (&input, row))
                    .or_else(|| state.as_ref().map(|batch| (batch, 0)));
                let replace = if let Some((prior, prior_row)) = prior {
                    let order = compare(measure.function, spec, &input, row, prior, prior_row)?;
                    if measure.function == Function::Last {
                        !order.is_lt()
                    } else {
                        order.is_lt()
                    }
                } else {
                    true
                };
                if replace {
                    selected = Some(row);
                }
            }
            if let Some(row) = selected {
                let retained = take_batch_with_policy(
                    array,
                    &spec.fields,
                    &[row],
                    native_payload::CopyPolicy::PreserveUnobserved,
                    context,
                )?;
                // Replacement stays charged until the new compact owner exists.
                *state = Some(Batch::for_ordering(retained, &spec.names, context)?);
            }
            Ok(())
        })
    }

    pub(super) fn finish(
        &self,
        index: usize,
        measure: &Measure,
        groups: Range<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let slot =
            self.slots.values[index].ok_or_else(|| failed("ordered selector slot is absent"))?;
        let column = measure
            .column
            .as_deref()
            .ok_or_else(|| failed("selector value is absent"))?;
        native_payload::retained_column(&measure.dtype, groups.len(), context, |row| {
            self.states.values[(groups.start + row) * self.width + slot]
                .as_ref()
                .map(|batch| logical_field_from_native_array(&batch.array, column))
                .transpose()
        })
    }
}

fn validate(spec: &Spec, paired: bool, batch: &Batch, row: usize) -> Result<()> {
    batch.validate_keys(row)?;
    for (index, key) in spec.order.iter().enumerate() {
        if key.nulls.is_none() && batch.key_is_null(row, index + usize::from(paired))? {
            return Err(failed(
                "aggregate ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
            ));
        }
    }
    Ok(())
}

fn compare(
    function: Function,
    spec: &Spec,
    left: &Batch,
    left_row: usize,
    right: &Batch,
    right_row: usize,
) -> Result<Ordering> {
    let paired = function.requires_secondary();
    if paired {
        let order = left.compare_key(left_row, right, right_row, 0)?;
        if order != Ordering::Equal {
            return Ok(if function == Function::ArgMax {
                order.reverse()
            } else {
                order
            });
        }
    }
    for (offset, key) in spec.order.iter().enumerate() {
        let index = offset + usize::from(paired);
        let order = native_relational_sort::compare_key(
            key,
            left.key_is_null(left_row, index)?,
            right.key_is_null(right_row, index)?,
            || left.compare_key(left_row, right, right_row, index),
        )?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
}
