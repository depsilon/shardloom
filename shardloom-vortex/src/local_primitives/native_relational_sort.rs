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

    pub(super) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
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
                    let order = self.compare(left, right, index, key)?;
                    if order != Ordering::Equal {
                        return Ok(order);
                    }
                }
                Ok(left.cmp(&right))
            },
        )?;
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(batch_rows.min(self.table.rows()))?;
        for part in ordinals.values.chunks(batch_rows) {
            context.check_cancelled()?;
            rows.values.clear();
            rows.values.extend(part.iter().map(|row| Some(*row)));
            let gather = self.table.gather(&rows.values, false, context)?;
            let mut columns = ReservedVec::new(context.memory())?;
            columns.reserve(self.spec.fields.len())?;
            for (name, dtype) in &self.spec.fields {
                columns.values.push(gather.column(name, dtype, context)?);
            }
            let (columns, _ownership) = columns.into_parts();
            let array = StructArray::try_new(
                self.spec
                    .fields
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<FieldNames>(),
                columns,
                part.len(),
                Validity::NonNullable,
            )
            .map_err(vortex_error)?;
            consume(vortex::array::IntoArray::into_array(array))?;
            context.check_cancelled()?;
        }
        Ok(())
    }

    fn compare(
        &self,
        left: usize,
        right: usize,
        index: usize,
        key: &VortexRelationalOrderKey,
    ) -> Result<Ordering> {
        let nulls = (
            self.table.key_is_null(left, index)?,
            self.table.key_is_null(right, index)?,
        );
        Ok(match nulls {
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
                let order = self.table.compare_key(left, right, index)?;
                if key.descending {
                    order.reverse()
                } else {
                    order
                }
            }
        })
    }
}
