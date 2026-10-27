//! Expand grouping sets over one producer, preserving original argument columns.

use super::{ArrayRef, DType, NativeExecutionContext, ReservedVec, Result, failed, vortex_error};
use crate::local_primitives::{logical_field_from_native_array, native_payload};
use vortex::array::{
    IntoArray as _,
    arrays::{ConstantArray, StructArray},
    builtins::ArrayBuiltins as _,
    dtype::FieldNames,
    scalar::Scalar,
    validity::Validity,
};

pub(super) struct Spec {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) originals: Vec<(String, DType)>,
    pub(super) group_sources: Vec<String>,
    pub(super) group_fields: Vec<(String, DType)>,
    pub(super) sets: Vec<Vec<usize>>,
    pub(super) indicators: Vec<Vec<usize>>,
    pub(super) filters: Vec<(String, Option<String>)>,
    pub(super) batch_metadata: u64,
}

impl Spec {
    pub(super) fn upper_rows(&self, input: Option<u64>) -> Option<u64> {
        input?
            .checked_mul(u64::try_from(self.sets.len()).ok()?)
            .map(|rows| rows.max(self.sets.iter().filter(|set| set.is_empty()).count() as u64))
    }

    fn expand(
        &self,
        input: Option<&ArrayRef>,
        set_index: usize,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        // These array/schema views live through the synchronous consumer. The
        // aggregate retains its own charged keys, values and native spill runs.
        let _metadata = context.memory().reserve(self.batch_metadata)?;
        let rows = input.map_or(1, ArrayRef::len);
        let set = &self.sets[set_index];
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.fields.len())?;
        for (name, dtype) in &self.originals {
            context.check_cancelled()?;
            columns.values.push(match input {
                Some(input) => logical_field_from_native_array(input, name)?,
                None if dtype == &DType::Null => {
                    ConstantArray::new(Scalar::null(DType::Null), 1).into_array()
                }
                None => native_payload::defaults(dtype, 1, context)?,
            });
        }
        for (index, ((_, dtype), source)) in self
            .group_fields
            .iter()
            .zip(&self.group_sources)
            .enumerate()
        {
            context.check_cancelled()?;
            columns.values.push(if set.binary_search(&index).is_ok() {
                logical_field_from_native_array(
                    input.ok_or_else(|| failed("nonempty grouping set cannot seed empty input"))?,
                    source,
                )?
                .cast(dtype.clone())
                .map_err(vortex_error)?
            } else {
                ConstantArray::new(Scalar::null(dtype.clone()), rows).into_array()
            });
        }
        columns.values.push(
            ConstantArray::new(
                Scalar::from(u64::try_from(set_index).map_err(vortex_error)?),
                rows,
            )
            .into_array(),
        );
        for indicator in &self.indicators {
            let mask = indicator.iter().fold(0u64, |mask, index| {
                (mask << 1) | u64::from(set.binary_search(index).is_err())
            });
            columns
                .values
                .push(ConstantArray::new(Scalar::from(mask), rows).into_array());
        }
        for (_, filter) in &self.filters {
            let dtype = &self.fields[columns.values.len()].1;
            let column = match (input, filter) {
                (Some(input), Some(name)) => logical_field_from_native_array(input, name)?,
                _ => ConstantArray::new(Scalar::from(input.is_some()), rows).into_array(),
            };
            columns
                .values
                .push(column.cast(dtype.clone()).map_err(vortex_error)?);
        }
        let (columns, _ownership) = columns.into_parts();
        let array = StructArray::try_new(
            self.fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<FieldNames>(),
            columns,
            rows,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?
        .into_array();
        consume(array)
    }
}

pub(super) fn produce(
    expansion: Option<&Spec>,
    context: &NativeExecutionContext<'_>,
    producer: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let Some(expansion) = expansion else {
        return producer(consume);
    };
    let mut rows = 0u64;
    producer(&mut |array| {
        context.check_cancelled()?;
        rows = rows
            .checked_add(u64::try_from(array.len()).map_err(vortex_error)?)
            .ok_or_else(|| failed("grouping-set input cardinality overflow"))?;
        rows.checked_mul(u64::try_from(expansion.sets.len()).map_err(vortex_error)?)
            .ok_or_else(|| failed("grouping-set expansion cardinality overflow"))?;
        if !array.is_empty() {
            for index in 0..expansion.sets.len() {
                expansion.expand(Some(&array), index, context, consume)?;
            }
        }
        Ok(())
    })?;
    // A failed/cancelled producer cannot manufacture a completed empty result.
    context.check_cancelled()?;
    if rows == 0 {
        for (index, set) in expansion.sets.iter().enumerate() {
            if set.is_empty() {
                expansion.expand(None, index, context, consume)?;
            }
        }
    }
    Ok(())
}
