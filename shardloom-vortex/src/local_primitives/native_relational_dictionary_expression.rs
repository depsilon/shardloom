//! Preserve prepared dictionary codes while transforming only referenced values.

use crate::{
    local_primitives::{
        NativeNumericOwner,
        native_capacity::ReservedVec,
        native_relational_batch::failed,
        native_relational_expression::Expression,
        native_relational_keys::VariableColumn,
        result_batch::{self, Value},
        vortex_error,
    },
    native_dictionary_expression::Work,
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{
    ArrayRef, IntoArray as _, arrays::DictArray, dtype::DType, memory::HostMemorySessionExt as _,
};

const UNREFERENCED: usize = usize::MAX;
const DICTIONARY_METADATA_BYTES: u64 = 2048;

struct Dictionary<'a> {
    codes: &'a NativeNumericOwner,
    representatives: ReservedVec<usize>,
    metadata: MemoryLease,
}

pub(super) struct Output<'a> {
    dictionary: Option<Dictionary<'a>>,
    rows: usize,
    work: Work,
}

impl<'a> Output<'a> {
    pub(super) fn prepare(
        values: &'a VariableColumn,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let mut output = Self {
            dictionary: None,
            rows,
            work: Work {
                batches: 1,
                logical_rows: rows as u64,
                ..Work::default()
            },
        };
        let Some((codes, domain)) = values.dictionary_parts() else {
            output.work.no_dictionary_batches = 1;
            return Ok(output);
        };
        if domain >= rows {
            output.work.no_reduction_batches = 1;
            return Ok(output);
        }
        let Some(metadata) = context.memory().try_reserve(DICTIONARY_METADATA_BYTES)? else {
            output.work.capacity_declines = 1;
            return Ok(output);
        };
        let Some(mut representatives) = ReservedVec::try_with_capacity(context.memory(), domain)?
        else {
            output.work.capacity_declines = 1;
            return Ok(output);
        };
        representatives.values.resize(domain, UNREFERENCED);
        for row in 0..rows {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            // Use the same observed-code and null-value admission as the row
            // kernel. Invalid codes are errors, never optional strategy refusals.
            if let Some(index) = values.index(row)?
                && representatives.values[index] == UNREFERENCED
            {
                representatives.values[index] = row;
                output.work.referenced_values += 1;
            }
        }
        output.work.dictionary_batches = 1;
        output.work.domain_slots = domain as u64;
        output.work.code_rows_inspected = rows as u64;
        output.dictionary = Some(Dictionary {
            codes,
            representatives,
            metadata,
        });
        Ok(output)
    }

    pub(super) fn build<'v>(
        mut self,
        expression: &Expression,
        context: &NativeExecutionContext<'_>,
        mut value: impl FnMut(usize) -> Result<Value<'v>>,
    ) -> Result<ArrayRef> {
        let result = if let Some(dictionary) = self.dictionary {
            let allocator = crate::owned_buffers::with_credit(
                context.native_session().host_allocator(),
                dictionary.metadata,
            );
            let transformed = result_batch::build_expression_column(
                &expression.dtype,
                dictionary.representatives.values.len(),
                &allocator,
                context.float_policy(),
                |index| {
                    if index.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    let row = dictionary.representatives.values[index];
                    if row == UNREFERENCED {
                        return placeholder(&expression.dtype);
                    }
                    let result = value(row)?;
                    self.work.dictionary_kernel_calls += u64::from(!matches!(result, Value::Null));
                    Ok(result)
                },
            )?;
            // Reuse the existing original-width copier. No source allocation or
            // private provider child slot is retained by the derived dictionary.
            let codes = result_batch::copy_numeric_column(
                dictionary.codes.primitive().dtype(),
                dictionary.codes.primitive(),
                dictionary.codes.validity(),
                &allocator,
                context.float_policy(),
                || context.check_cancelled(),
            )?;
            self.work.code_rows_copied = self.rows as u64;
            self.work.retained_dictionary_rows = self.rows as u64;
            DictArray::try_new(codes, transformed)
                .map_err(vortex_error)?
                .into_array()
        } else {
            let result = expression.build(self.rows, context, |row| {
                let result = value(row)?;
                self.work.row_kernel_calls += u64::from(!matches!(result, Value::Null));
                Ok(result)
            })?;
            self.work.row_output_rows = self.rows as u64;
            result
        };
        context.check_cancelled()?;
        context.record_dictionary_expression(&self.work)?;
        Ok(result)
    }
}

fn placeholder(dtype: &DType) -> Result<Value<'static>> {
    if dtype.is_nullable() {
        return Ok(Value::Null);
    }
    match dtype {
        DType::Utf8(_) => Ok(Value::Text("".into())),
        DType::Bool(_) => Ok(Value::Bool(false)),
        DType::Primitive(vortex::array::dtype::PType::I64, _) => Ok(Value::Int(0)),
        _ => Err(failed("dictionary text output has an unadmitted dtype")),
    }
}
