//! Stable native sorting retains source payload and orders reserved ordinals.

use super::{
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Table, failed},
    native_relational_order, vortex_error,
};
use crate::{
    relational_query::{VortexRelationalNullOrder as NullOrder, VortexRelationalOrderKey},
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::cmp::Ordering;
use vortex::array::{
    ArrayRef,
    arrays::StructArray,
    dtype::{DType, FieldNames},
    validity::Validity,
};

pub(super) struct Spec {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) keys: Vec<VortexRelationalOrderKey>,
    pub(super) names: Vec<String>,
}

pub(super) struct Sort<'a> {
    spec: &'a Spec,
    table: Table,
}

impl<'a> Sort<'a> {
    pub(super) fn new(spec: &'a Spec, memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            spec,
            table: Table::new(memory)?,
        })
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn rows(&self) -> usize {
        self.table.rows()
    }

    /// Conservative flush estimate, separate from actual shared-pool accounting.
    #[cfg(feature = "vortex-write")]
    pub(super) fn incoming_bytes(&self, array: &ArrayRef) -> Result<u64> {
        let row_bytes = (self.spec.names.len() as u64)
            .checked_add(1)
            .and_then(|keys| keys.checked_mul(32))
            .and_then(|bytes| bytes.checked_mul(array.len() as u64))
            .ok_or_else(|| failed("sort retained input estimate overflow"))?;
        array
            .nbytes()
            .checked_add(row_bytes)
            .ok_or_else(|| failed("sort retained input estimate overflow"))
    }

    pub(super) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let batch = Batch::new(array, &self.spec.names, context)?;
        for row in 0..batch.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            batch.hash(row, true)?;
            for (index, key) in self.spec.keys.iter().enumerate() {
                if key.nulls.is_none() && batch.key_is_null(row, index)? {
                    return Err(failed(
                        "ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
                    ));
                }
            }
        }
        self.table.push(batch)?;
        Ok(())
    }

    pub(super) fn ordered_rows(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ReservedVec<usize>> {
        let mut ordinals = ReservedVec::new(context.memory())?;
        ordinals.reserve(self.table.rows())?;
        for row in 0..self.table.rows() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            ordinals.values.push(row);
        }
        native_relational_order::sort(
            &mut ordinals.values,
            context.cancellation(),
            |left, right| {
                for (index, key) in self.spec.keys.iter().enumerate() {
                    let order = compare_key(
                        key,
                        self.table.key_is_null(left, index)?,
                        self.table.key_is_null(right, index)?,
                        || self.table.compare_key(left, right, index),
                    )?;
                    if order != Ordering::Equal {
                        return Ok(order);
                    }
                }
                Ok(left.cmp(&right))
            },
        )?;
        Ok(ordinals)
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn gather_rows(
        &self,
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(rows.len())?;
        selected.values.extend(rows.iter().map(|row| Some(*row)));
        gather(&self.table, &selected.values, &self.spec.fields, context)
    }

    pub(super) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let ordinals = self.ordered_rows(context)?;
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(batch_rows.min(self.table.rows()))?;
        for part in ordinals.values.chunks(batch_rows) {
            context.check_cancelled()?;
            rows.values.clear();
            rows.values.extend(part.iter().map(|row| Some(*row)));
            consume(gather(
                &self.table,
                &rows.values,
                &self.spec.fields,
                context,
            )?)?;
            context.check_cancelled()?;
        }
        Ok(())
    }
}

pub(super) fn compare_key(
    key: &VortexRelationalOrderKey,
    left_null: bool,
    right_null: bool,
    compare: impl FnOnce() -> Result<Ordering>,
) -> Result<Ordering> {
    Ok(match (left_null, right_null) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if key.nulls == Some(NullOrder::First) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if key.nulls == Some(NullOrder::First) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let order = compare()?;
            if key.descending {
                order.reverse()
            } else {
                order
            }
        }
    })
}

pub(super) fn gather(
    table: &Table,
    rows: &[Option<usize>],
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let gather = table.gather(rows, false, context)?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(fields.len())?;
    for (name, dtype) in fields {
        columns.values.push(gather.column(name, dtype, context)?);
    }
    let (columns, _ownership) = columns.into_parts();
    let array = StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        rows.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    Ok(vortex::array::IntoArray::into_array(array))
}
