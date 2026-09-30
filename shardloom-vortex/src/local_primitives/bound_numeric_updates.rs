//! Block-local measure kernels for exact pair preunion and compact grouping.
//! Each recipe borrows current native owners; no cross-block data or state is cached.

use super::{
    AggregateDirectColumnAccessor, AggregateValueTransform, CompactAggregateMeasureSpec,
    CompactAggregateMeasureValue, CompactAggregateMeasures, Result, ShardLoomError,
    SimpleAggregateFunction, SimpleAggregateState, SimpleAggregateStates,
};

pub(super) type RowKernel<'a, State = SimpleAggregateState> =
    Box<dyn Fn(&mut State, usize) -> Result<()> + 'a>;

pub(super) struct BoundNumericUpdates<'a> {
    kernels: Vec<(usize, RowKernel<'a>)>,
}

impl<'a> BoundNumericUpdates<'a> {
    pub(super) fn bind(
        template: &SimpleAggregateStates,
        accessors: &'a [AggregateDirectColumnAccessor],
        skipped_state_index: usize,
        chunk_rows: usize,
    ) -> Option<Self> {
        if template.states.get(skipped_state_index)?.function
            != SimpleAggregateFunction::CountDistinct
        {
            return None;
        }
        // Reject the complete shape before allocating any kernel: a late unsupported
        // measure must not add repeated allocation work to the original route.
        if !template.states.iter().enumerate().all(|(index, state)| {
            index == skipped_state_index
                || (state.argument_offset.is_none()
                    && matches!(state.value_transform, AggregateValueTransform::Identity)
                    && match (state.function, state.column_index) {
                        (SimpleAggregateFunction::Count, None) => true,
                        (SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg, Some(column)) => {
                            matches!(accessors.get(column), Some(AggregateDirectColumnAccessor::NativeNumeric(owner)) if owner.len() == chunk_rows)
                        }
                        _ => false,
                    })
        }) {
            return None;
        }
        let mut kernels = Vec::with_capacity(template.states.len());
        for (index, state) in template.states.iter().enumerate() {
            if index == skipped_state_index {
                continue;
            }
            if state.argument_offset.is_some()
                || !matches!(state.value_transform, AggregateValueTransform::Identity)
            {
                return None;
            }
            let kernel: RowKernel<'a> = match (state.function, state.column_index) {
                (SimpleAggregateFunction::Count, None) => {
                    Box::new(move |state, row| state.update_count_star_direct_row(row, chunk_rows))
                }
                (SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg, Some(column)) => {
                    let AggregateDirectColumnAccessor::NativeNumeric(owner) =
                        accessors.get(column)?
                    else {
                        return None;
                    };
                    if owner.len() != chunk_rows {
                        return None;
                    }
                    owner.bind_numeric_update()
                }
                _ => return None,
            };
            kernels.push((index, kernel));
        }
        (!kernels.is_empty()).then_some(Self { kernels })
    }

    /// The caller supplies a clone of the template used at binding. Measure order
    /// and partial state on an error match the ordinary row updater.
    pub(super) fn update(&self, states: &mut SimpleAggregateStates, row: usize) -> Result<()> {
        for (index, kernel) in &self.kernels {
            kernel(&mut states.states[*index], row)?;
        }
        Ok(())
    }
}

pub(super) struct BoundCompactNumericUpdates<'a> {
    kernels: Vec<RowKernel<'a, CompactAggregateMeasureValue>>,
}

impl<'a> BoundCompactNumericUpdates<'a> {
    pub(super) fn bind(
        specs: &[CompactAggregateMeasureSpec],
        accessors: &'a [AggregateDirectColumnAccessor],
        chunk_rows: usize,
    ) -> Option<Self> {
        // Validate the entire shape before allocating. Each recipe borrows only
        // this block's native owners, including their original-width validity.
        if specs.is_empty()
            || !specs.iter().all(|spec| {
                matches!(spec.value_transform, AggregateValueTransform::Identity)
                    && match (spec.function, spec.column_index) {
                        (SimpleAggregateFunction::Count, None) => true,
                        (
                            SimpleAggregateFunction::Count
                            | SimpleAggregateFunction::Sum
                            | SimpleAggregateFunction::Avg,
                            Some(column),
                        ) => matches!(accessors.get(column), Some(AggregateDirectColumnAccessor::NativeNumeric(owner)) if owner.len() == chunk_rows),
                        _ => false,
                    }
            })
        {
            return None;
        }
        let mut kernels: Vec<RowKernel<'a, CompactAggregateMeasureValue>> =
            Vec::with_capacity(specs.len());
        for spec in specs {
            let kernel: RowKernel<'a, CompactAggregateMeasureValue> = if let Some(column) =
                spec.column_index
            {
                let AggregateDirectColumnAccessor::NativeNumeric(owner) = &accessors[column] else {
                    unreachable!("complete native measure shape was admitted above")
                };
                owner.bind_compact_numeric_update(spec.function == SimpleAggregateFunction::Count)
            } else {
                Box::new(|state, _row| state.increment_count())
            };
            kernels.push(kernel);
        }
        Some(Self { kernels })
    }

    pub(super) fn update(&self, measures: &mut CompactAggregateMeasures, row: usize) -> Result<()> {
        debug_assert_eq!(self.kernels.len(), measures.values().len());
        for (kernel, value) in self.kernels.iter().zip(measures.values_mut()) {
            kernel(value, row)?;
        }
        Ok(())
    }
}

impl CompactAggregateMeasureValue {
    pub(super) fn increment_count(&mut self) -> Result<()> {
        self.count = self.count.checked_add(1).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex compact aggregate count overflowed u64".to_string(),
            )
        })?;
        Ok(())
    }

    pub(super) fn add_numeric(&mut self, numeric: f64) -> Result<()> {
        self.count = self.count.checked_add(1).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex compact numeric aggregate count overflowed u64".to_string(),
            )
        })?;
        self.sum += numeric;
        if !self.sum.is_finite() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex compact numeric aggregate sum became non-finite; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "bound_numeric_updates_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "bound_compact_numeric_updates_tests.rs"]
mod compact_tests;
