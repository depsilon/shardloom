//! Compact typed build records; user payload names remain inside a private struct.

use super::{Batch, DType, NativeExecutionContext, ReservedVec, Result, Side, Spec, failed};
use crate::local_primitives::{
    native_payload,
    native_relational_batch::index_array,
    native_relational_records::{self as records, ORDINAL},
    native_relational_sort,
    result_batch::{self, Value},
    vortex_error,
};
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::StructArray,
    dtype::{FieldName, Nullability},
    memory::MemorySessionExt as _,
};

pub(super) const HASH: &str = "hash";
pub(super) const DATA: &str = "data";

pub(super) struct Layout {
    pub(super) build: native_relational_sort::Spec,
    pub(super) matches: native_relational_sort::Spec,
    pub(super) unmatched: native_relational_sort::Spec,
    payload_names: Vec<FieldName>,
    payload_dtype: DType,
    _metadata: MemoryLease,
}

impl Layout {
    pub(super) fn new(
        spec: &Spec,
        right_fields: &[(String, DType)],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let needed = |name: &String| {
            spec.right_keys.contains(name)
                || spec
                    .columns
                    .iter()
                    .any(|(side, column)| *side == Side::Right && column == name)
                || spec.condition.as_ref().is_some_and(|condition| {
                    condition
                        .columns
                        .iter()
                        .any(|(side, column)| *side == Side::Right && column == name)
                })
        };
        let bytes = right_fields
            .iter()
            .filter(|(name, _)| needed(name))
            .try_fold(16_384u64, |bytes, (name, dtype)| {
                native_payload::metadata_bytes(dtype)?
                    .checked_add(name.len() as u64 * 2)
                    .and_then(|field| field.checked_add(2048))
                    .and_then(|field| field.checked_mul(8))
                    .and_then(|field| bytes.checked_add(field))
                    .ok_or_else(|| failed("ordered join descriptor capacity overflow"))
            })?;
        let metadata = context.memory().reserve(bytes)?;
        let fields = right_fields
            .iter()
            .filter(|(name, _)| needed(name))
            .cloned()
            .collect::<Vec<_>>();
        let payload_names = fields
            .iter()
            .map(|(name, _)| FieldName::from(name.as_str()))
            .collect();
        // A predicate may need no right fields at all (for example ON TRUE in
        // a semi join). Keep only cardinality with an existing scalar dtype;
        // public native payloads deliberately reject zero-field nested structs.
        let payload_dtype = if fields.is_empty() {
            DType::Bool(Nullability::NonNullable)
        } else {
            DType::struct_(fields, Nullability::Nullable)
        };
        let build_fields = vec![
            (HASH.into(), records::u64_type().as_nullable()),
            (ORDINAL.into(), records::u64_type()),
            (DATA.into(), payload_dtype.clone()),
        ];
        Ok(Self {
            unmatched: records::order(build_fields.clone(), vec![ORDINAL.into()])?,
            build: records::order(build_fields, vec![HASH.into()])?,
            matches: records::order(
                vec![(ORDINAL.into(), records::u64_type())],
                vec![ORDINAL.into()],
            )?,
            payload_names,
            payload_dtype,
            _metadata: metadata,
        })
    }

    pub(super) fn record(
        &self,
        input: &Batch,
        range: std::ops::Range<usize>,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        context.check_cancelled()?;
        let rows = range.len();
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(3)?;
        columns.values.push(result_batch::build_column(
            &records::u64_type().as_nullable(),
            rows,
            &context.native_session().allocator(),
            |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                Ok(input
                    .hash(range.start + row, false)?
                    .map_or(Value::Null, Value::UInt))
            },
        )?);
        columns.values.push(records::unsigned(rows, context, |row| {
            ordinal
                .checked_add((range.start + row) as u64)
                .ok_or_else(|| failed("ordered join build ordinal overflow"))
        })?);
        let payload = if self.payload_names.is_empty() {
            result_batch::build_column(
                &self.payload_dtype,
                rows,
                &context.native_session().allocator(),
                |_| Ok(Value::Bool(false)),
            )?
        } else {
            let indices = index_array(rows, false, context, |row| Ok(Some(range.start + row)))?;
            let mut execution = context.native_session().create_execution_ctx();
            let source = input
                .array
                .clone()
                .execute::<StructArray>(&mut execution)
                .map_err(vortex_error)?;
            // Project unmasked children while retaining root validity. Hidden or
            // unreferenced children are never copied merely to retain a join row.
            let projected = source
                .project(&self.payload_names)
                .map_err(vortex_error)?
                .into_array();
            native_payload::take_record(&projected, &indices, &self.payload_dtype, context)?
        };
        columns.values.push(payload);
        records::structure(&self.build.fields, columns, rows)
    }
}
