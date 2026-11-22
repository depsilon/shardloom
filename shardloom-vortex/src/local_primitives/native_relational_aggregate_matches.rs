//! Matched source references enter the existing group, state and finalizer.

use super::{
    Aggregate, Function, GroupOrdinals, KeyColumn, NativeExecutionContext, Result, Spec, failed,
    update, update_integer_parts, update_numeric_parts,
};
use crate::local_primitives::{
    native_numeric_owner::NativeNumericOwner,
    native_relational_matches::{Input, Origin},
    native_relational_rows::Rows,
};
use vortex::array::dtype::{DType, PType};

impl Spec {
    pub(in crate::local_primitives) fn admits_matches(&self) -> bool {
        let [measure] = self.measures.as_slice() else {
            return false;
        };
        matches!(
            measure.function,
            Function::Count | Function::Sum | Function::Avg | Function::Min | Function::Max
        ) && measure.filter_column.is_none()
            && measure.secondary_column.is_none()
            && measure.selection.is_none()
            && measure.quantile_level.is_none()
            && measure.separator.is_none()
            && measure.decimal_source.is_none()
            && measure.decimal_output.is_none()
            && measure.distinct_fields.is_empty()
            && (measure.function == Function::Count
                || measure.input_ptype.is_some_and(|ptype| ptype != PType::F16))
    }
}

impl Aggregate<'_> {
    pub(in crate::local_primitives) fn consume_matches(
        &mut self,
        input: &Input<'_>,
        measure: Option<Origin>,
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<()> {
        context.check_cancelled()?;
        if !self.spec.admits_matches() || measure.is_some_and(|origin| origin.source != 0) {
            return Err(failed("direct join aggregate admission changed"));
        }
        self.prepare_compact_dtype(dtype, context)?;
        let ordinals = match &mut self.groups {
            Some(groups) => GroupOrdinals::Grouped(groups.intern_rows(input, context, batch_rows)?),
            None => GroupOrdinals::Scalar(input.len()),
        };
        self.reserve_states_dtype(dtype, context)?;
        let values = measure
            .map(|origin| {
                input
                    .column(origin)?
                    .single()
                    .ok_or_else(|| failed("fact measure is not one native owner"))
            })
            .transpose()?;
        let owner = match values {
            Some(KeyColumn::Numeric(owner)) => Some(owner),
            None => None,
            _ => return Err(failed("matched aggregate measure is not numeric")),
        };
        let rows = input.frame.source_rows(0)?;
        let measure = &self.spec.measures[0];
        if let Some(table) = &mut self.compact {
            table.consume_selection(
                0,
                measure,
                &ordinals,
                values,
                owner.map(NativeNumericOwner::validity),
                Some(rows),
                context,
            )?;
        } else if matches!(measure.function, Function::Sum | Function::Avg) {
            let owner = owner.ok_or_else(|| failed("matched numeric measure is absent"))?;
            if owner.is_integer() {
                owner.visit_selected_integer(rows, |row, value| {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if let Some(value) = value {
                        let state = &mut self.states.values[ordinals.at(row)];
                        update_integer_parts(&mut state.count, &mut state.sum, value)?;
                    }
                    Ok(())
                })?;
            } else {
                owner.visit_selected_numeric(rows, |row, value, integer| {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if let Some(value) = value {
                        let state = &mut self.states.values[ordinals.at(row)];
                        update_numeric_parts(
                            &mut state.count,
                            &mut state.sum,
                            &mut state.specials,
                            value,
                            integer,
                            context.float_policy(),
                        )?;
                    }
                    Ok(())
                })?;
            }
        } else {
            for (row, group) in ordinals.iter() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let value = values
                    .map(|values| values.raw_cell(rows[row]))
                    .transpose()?;
                update(&mut self.states.values[group], measure, value, context)?;
            }
        }
        context.check_cancelled()
    }
}
