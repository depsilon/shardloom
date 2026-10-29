//! Private projected keys, exact source ordinals and candidate lookup hashes.

use super::{ArrayRef, Batch, DType, Kind, NativeExecutionContext, Result, Spec, failed};
use crate::local_primitives::{
    native_capacity::ReservedVec,
    native_payload,
    native_relational_batch::index_array,
    native_relational_records::{self as records, ORDINAL},
    native_relational_sort,
    result_batch::{self, Value},
    vortex_error,
};
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::StructArray,
    dtype::{FieldName, Nullability},
    memory::HostMemorySessionExt as _,
};

pub(super) const GROUP: &str = "group";
pub(super) const HASH: &str = "hash";
pub(super) const DATA: &str = "data";

pub(super) struct Layout {
    pub(super) order: native_relational_sort::Spec,
    names: Vec<FieldName>,
    payload_dtype: DType,
    _metadata: MemoryLease,
}

impl Layout {
    pub(super) fn new(
        spec: &Spec,
        fields: &[(String, DType)],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let needed = |name: &String| spec.right_keys.contains(name);
        let bytes = fields.iter().filter(|(name, _)| needed(name)).try_fold(
            16_384u64,
            |bytes, (name, dtype)| {
                native_payload::metadata_bytes(dtype)?
                    .checked_add(name.len() as u64 * 2)
                    .and_then(|field| field.checked_add(2048))
                    .and_then(|field| field.checked_mul(8))
                    .and_then(|field| bytes.checked_add(field))
                    .ok_or_else(|| failed("subquery descriptor capacity overflow"))
            },
        )?;
        let metadata = context.memory().reserve(bytes)?;
        let fields = fields
            .iter()
            .filter(|(name, _)| needed(name))
            .cloned()
            .collect::<Vec<_>>();
        let names = fields
            .iter()
            .map(|(name, _)| FieldName::from(name.as_str()))
            .collect();
        let payload_dtype = if fields.is_empty() {
            // EXISTS without a correlation needs cardinality, not user payloads.
            DType::Bool(Nullability::NonNullable)
        } else {
            DType::struct_(fields, Nullability::Nullable)
        };
        let order = records::order(
            vec![
                (GROUP.into(), records::u64_type()),
                (HASH.into(), records::u64_type().as_nullable()),
                (ORDINAL.into(), records::u64_type()),
                (DATA.into(), payload_dtype.clone()),
            ],
            vec![GROUP.into(), HASH.into(), ORDINAL.into()],
        )?;
        Ok(Self {
            order,
            names,
            payload_dtype,
            _metadata: metadata,
        })
    }

    pub(super) fn record(
        &self,
        spec: &Spec,
        input: &Batch,
        range: std::ops::Range<usize>,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(range.len())?;
        for row in range {
            context.check_cancelled()?;
            if input.hash_prefix(row, spec.correlation, false)?.is_some() {
                selected.values.push(row);
            }
        }
        let rows = selected.values.len();
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(4)?;
        columns.values.push(records::unsigned(rows, context, |row| {
            input
                .hash_prefix(selected.values[row], spec.correlation, false)?
                .ok_or_else(|| failed("selected subquery correlation became null"))
        })?);
        columns
            .values
            .push(result_batch::build_column_with_float_policy(
                &records::u64_type().as_nullable(),
                rows,
                &context.native_session().host_allocator(),
                context.float_policy(),
                |row| {
                    context.check_cancelled()?;
                    let hash = if matches!(spec.kind, Kind::In) {
                        input.hash(selected.values[row], false)?
                    } else {
                        Some(0)
                    };
                    Ok(hash.map_or(Value::Null, Value::UInt))
                },
            )?);
        columns.values.push(records::unsigned(rows, context, |row| {
            ordinal
                .checked_add(selected.values[row] as u64)
                .ok_or_else(|| failed("subquery source ordinal overflow"))
        })?);
        let payload = if self.names.is_empty() {
            result_batch::build_column_with_float_policy(
                &self.payload_dtype,
                rows,
                &context.native_session().host_allocator(),
                context.float_policy(),
                |_| Ok(Value::Bool(false)),
            )?
        } else {
            let indices = index_array(rows, false, context, |row| Ok(Some(selected.values[row])))?;
            let mut execution = context.native_session().create_execution_ctx();
            let source = input
                .array
                .clone()
                .execute::<StructArray>(&mut execution)
                .map_err(vortex_error)?;
            let projected = source
                .project(&self.names)
                .map_err(vortex_error)?
                .into_array();
            native_payload::take_record(&projected, &indices, &self.payload_dtype, context)?
        };
        columns.values.push(payload);
        records::structure(&self.order.fields, columns, rows)
    }
}
