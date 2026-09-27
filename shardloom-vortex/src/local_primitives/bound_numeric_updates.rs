//! Block-local ordinary-measure kernels for the existing exact pair preunion.
//! Each recipe borrows current native owners; no cross-block data or state is cached.

use super::{
    AggregateDirectColumnAccessor, AggregateValueTransform, Result, SimpleAggregateFunction,
    SimpleAggregateState, SimpleAggregateStates,
};

pub(super) type RowKernel<'a> = Box<dyn Fn(&mut SimpleAggregateState, usize) -> Result<()> + 'a>;

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

#[cfg(test)]
#[path = "bound_numeric_updates_tests.rs"]
mod tests;
