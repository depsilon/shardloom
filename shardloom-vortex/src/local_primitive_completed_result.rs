//! Bind completed native scalar values to the declared result schema before a sink.
//! No source replay, data-dependent schema inference, or serialized JSON round trip.

use super::{
    AggregateValueTransform, Result, ShardLoomError, SimpleAggregateFunction,
    VortexQueryPrimitiveRequest, required_simple_aggregate, vortex_error,
};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::StructArray,
    builders::builder_with_capacity,
    dtype::{DType, FieldNames, Nullability, PType},
    scalar::Scalar,
    validity::Validity,
};

const MAX_ROWS: usize = 65_536;
const MAX_BYTES: usize = 8 * 1024 * 1024;

pub(super) struct CompletedRows {
    fields: Vec<(String, DType)>,
    memory: LiveMemoryPool,
    array: Option<ArrayRef>,
    ownership: MemoryLease,
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "completed native result: {message}; no fallback execution was attempted"
    ))
}

pub(super) fn source_field(dtype: &DType, name: &str) -> Result<DType> {
    let field = dtype
        .as_struct_fields_opt()
        .and_then(|fields| fields.field(name))
        .ok_or_else(|| failed(&format!("source column '{name}' is absent")))?;
    if dtype.is_nullable() {
        Ok(field.as_nullable())
    } else {
        Ok(field)
    }
}

fn transformed_dtype(dtype: DType, transform: AggregateValueTransform) -> Result<DType> {
    let nullability = dtype.nullability();
    match transform {
        AggregateValueTransform::Identity => Ok(dtype),
        AggregateValueTransform::Length => Ok(DType::Primitive(PType::U64, nullability)),
        AggregateValueTransform::ConstantInt(_) => {
            Ok(DType::Primitive(PType::I64, Nullability::NonNullable))
        }
        AggregateValueTransform::UrlDomain
        | AggregateValueTransform::CaseSearchAdvZeroRefererElseEmpty => {
            Ok(DType::Utf8(nullability))
        }
        AggregateValueTransform::ExtractMinute if matches!(dtype, DType::Utf8(_)) => {
            Ok(DType::Primitive(PType::U64, nullability))
        }
        AggregateValueTransform::DateTruncMinute if matches!(dtype, DType::Utf8(_)) => Ok(dtype),
        AggregateValueTransform::AddOffset(_)
        | AggregateValueTransform::ExtractMinute
        | AggregateValueTransform::DateTruncMinute => match dtype {
            DType::Primitive(ptype, _) if ptype.is_signed_int() => {
                Ok(DType::Primitive(PType::I64, nullability))
            }
            DType::Primitive(ptype, _) if ptype.is_unsigned_int() => {
                Ok(DType::Primitive(PType::U64, nullability))
            }
            DType::Primitive(PType::F32 | PType::F64, _)
                if matches!(transform, AggregateValueTransform::AddOffset(_)) =>
            {
                Ok(DType::Primitive(PType::F64, nullability))
            }
            _ => Err(failed("unsupported derived result dtype")),
        },
    }
}

pub(super) fn aggregate_fields(
    request: &VortexQueryPrimitiveRequest,
    source: &DType,
) -> Result<Vec<(String, DType)>> {
    let aggregate = required_simple_aggregate(request)?;
    if aggregate.spill.is_some() {
        return Err(failed("owned aggregate spill output is not admitted"));
    }
    if (!aggregate.group_by.is_empty() || !aggregate.group_expressions.is_empty())
        && request.source_order_limit.is_none_or(|limit| {
            aggregate
                .offset
                .checked_add(limit)
                .is_none_or(|n| n > MAX_ROWS)
        })
    {
        return Err(failed("grouped output requires a limit at most 65536"));
    }
    let mut fields = Vec::new();
    for name in &aggregate.group_by {
        fields.push((
            name.as_str().to_owned(),
            source_field(source, name.as_str())?,
        ));
    }
    for expression in &aggregate.group_expressions {
        fields.push((
            expression.alias.clone(),
            transformed_dtype(
                source_field(source, expression.column.as_str())?,
                AggregateValueTransform::from_expression(expression)?,
            )?,
        ));
    }
    for measure in &aggregate.measures {
        let dtype = match SimpleAggregateFunction::parse(&measure.function)? {
            SimpleAggregateFunction::Count | SimpleAggregateFunction::CountDistinct => {
                DType::Primitive(PType::U64, Nullability::NonNullable)
            }
            // Preserve the existing native SUM/AVG f64 accumulation contract.
            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => {
                DType::Primitive(PType::F64, Nullability::Nullable)
            }
            SimpleAggregateFunction::Min | SimpleAggregateFunction::Max => {
                let name = measure
                    .column
                    .as_ref()
                    .ok_or_else(|| failed("MIN/MAX column is absent"))?;
                transformed_dtype(
                    source_field(source, name.as_str())?,
                    AggregateValueTransform::from_measure_transform(
                        measure.value_transform.as_deref(),
                    )?,
                )?
                .as_nullable()
            }
        };
        fields.push((measure.alias.clone(), dtype));
    }
    Ok(fields)
}

impl CompletedRows {
    pub(super) fn new(fields: Vec<(String, DType)>, memory: &LiveMemoryPool) -> Result<Self> {
        if fields.is_empty() || fields.len() > 128 {
            return Err(failed("requires 1..=128 flat scalar columns"));
        }
        let mut names = std::collections::BTreeSet::new();
        for (name, dtype) in &fields {
            if name.is_empty()
                || name.len() > 256
                || !names.insert(name)
                || !matches!(
                    dtype,
                    DType::Bool(_)
                        | DType::Utf8(_)
                        | DType::Primitive(
                            PType::I8
                                | PType::I16
                                | PType::I32
                                | PType::I64
                                | PType::U8
                                | PType::U16
                                | PType::U32
                                | PType::U64
                                | PType::F32
                                | PType::F64,
                            _
                        )
                )
            {
                return Err(failed("unsupported or duplicate output field"));
            }
        }
        let ownership = memory.reserve(64 * 1024)?;
        Ok(Self {
            fields,
            memory: memory.clone(),
            array: None,
            ownership,
        })
    }

    pub(super) fn finish_payload(
        &mut self,
        rows: usize,
        payload: &mut serde_json::Value,
    ) -> Result<()> {
        let values = payload
            .get("values")
            .ok_or_else(|| failed("completed values are absent"))?;
        let objects: Vec<_> = if rows == 0 {
            Vec::new()
        } else if let Some(object) = values.as_object() {
            vec![object]
        } else if let Some(values) = values.as_array() {
            values
                .iter()
                .map(|row| {
                    row.as_object()
                        .ok_or_else(|| failed("invalid completed row"))
                })
                .collect::<Result<_>>()?
        } else {
            return Err(failed("invalid completed values"));
        };
        if rows != objects.len() || rows > MAX_ROWS || self.array.is_some() {
            return Err(failed("completed row count exceeds admission or changed"));
        }
        // Builders use bounded opaque buffers. Reserve their growth overlap plus
        // metadata separately from the existing native scalar finalization stage.
        let mut bytes = self
            .fields
            .len()
            .checked_mul(rows)
            .and_then(|n| n.checked_mul(32))
            .ok_or_else(|| failed("result size overflow"))?;
        for object in &objects {
            for (name, _) in &self.fields {
                let value = object
                    .get(name)
                    .ok_or_else(|| failed("completed column is absent"))?;
                if let Some(text) = value.as_str() {
                    bytes = bytes
                        .checked_add(text.len())
                        .ok_or_else(|| failed("string size overflow"))?;
                }
            }
        }
        if bytes > MAX_BYTES {
            return Err(failed("completed values exceed 8 MiB output admission"));
        }
        let payload_owner = self.memory.reserve(
            u64::try_from(bytes.saturating_mul(4).saturating_add(64 * 1024))
                .map_err(vortex_error)?,
        )?;
        let mut arrays = Vec::with_capacity(self.fields.len());
        for (name, dtype) in &self.fields {
            let mut builder = builder_with_capacity(dtype, rows);
            for object in &objects {
                let value = object
                    .get(name)
                    .ok_or_else(|| failed("completed column is absent"))?;
                let scalar = scalar(value, dtype)?;
                builder.append_scalar(&scalar).map_err(vortex_error)?;
            }
            arrays.push(builder.finish());
        }
        let names: FieldNames = self.fields.iter().map(|(name, _)| name.as_str()).collect();
        let array = StructArray::try_new(names, arrays, rows, Validity::NonNullable)
            .map_err(vortex_error)?
            .into_array();
        if array.nbytes() > MAX_BYTES as u64 {
            return Err(failed("native buffers exceed output admission"));
        }
        // Transfer the opaque buffer grant to the result lifetime.
        self.ownership = payload_owner;
        self.array = Some(array);
        payload["values"] = serde_json::Value::Null;
        payload["aggregate_result_boundary"] =
            "owned_native_columns_from_completed_scalar_values".into();
        payload["aggregate_result_serialized_json_round_trip"] = false.into();
        Ok(())
    }

    pub(super) fn into_array(self) -> Result<(ArrayRef, MemoryLease)> {
        Ok((
            self.array
                .ok_or_else(|| failed("result was not completed"))?,
            self.ownership,
        ))
    }
}

fn scalar(value: &serde_json::Value, dtype: &DType) -> Result<Scalar> {
    if value.is_null() {
        return if dtype.is_nullable() {
            Ok(Scalar::null(dtype.clone()))
        } else {
            Err(failed("null in a nonnullable result column"))
        };
    }
    let nullability = dtype.nullability();
    let scalar = match value {
        serde_json::Value::Bool(value) => Scalar::bool(*value, nullability),
        serde_json::Value::String(value) => Scalar::utf8(value.clone(), nullability),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                Scalar::primitive(value, nullability)
            } else if let Some(value) = value.as_i64() {
                Scalar::primitive(value, nullability)
            } else {
                Scalar::primitive(
                    value
                        .as_f64()
                        .ok_or_else(|| failed("invalid numeric value"))?,
                    nullability,
                )
            }
        }
        _ => return Err(failed("non-scalar completed value")),
    };
    scalar.cast(dtype).map_err(vortex_error)
}

#[cfg(all(feature = "vortex-write", unix))]
pub(super) fn write(
    result: crate::resident_session::OwnedVortexResultBatch,
    request: &VortexQueryPrimitiveRequest,
    path: &std::path::Path,
    format: super::VortexLocalPrimitiveRowExportFormat,
    overwrite: bool,
    policy: super::VortexLocalPrimitiveExecutionPolicy,
) -> Result<super::VortexLocalPrimitiveRowExportReport> {
    let plan = super::native_sink::NativeSinkPlan::completed(result)?;
    if format == super::VortexLocalPrimitiveRowExportFormat::Vortex {
        return plan.write(request, path, overwrite, policy);
    }
    #[cfg(feature = "universal-format-io")]
    {
        super::columnar_compat_sink::prepare_plan(
            request,
            plan,
            format,
            policy,
            super::columnar_compat_sink::CompatibilityLimits::default(),
        )?
        .ok_or_else(|| failed("result exceeds compatibility sink admission"))?
        .write(path, overwrite)
        .map(|completed| completed.report)
    }
    #[cfg(not(feature = "universal-format-io"))]
    Err(failed("compatibility output requires universal-format-io"))
}

#[cfg(all(feature = "vortex-write", unix))]
pub(super) fn export_sort(
    request: &VortexQueryPrimitiveRequest,
    output: &std::path::Path,
    format: super::VortexLocalPrimitiveRowExportFormat,
    overwrite: bool,
    policy: super::VortexLocalPrimitiveExecutionPolicy,
) -> Result<super::VortexLocalPrimitiveRowExportReport> {
    if request
        .source_order_limit
        .is_none_or(|limit| limit > MAX_ROWS)
        || super::required_sort_rows(request)?.spill.is_some()
    {
        return Err(failed(
            "sort output requires a bounded result without explicit spill",
        ));
    }
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("sort source is absent"))?;
    let path = super::local_vortex_path(uri, request.kind)?
        .ok_or_else(|| failed("sort requires local Vortex"))?;
    let session = crate::resident_session::ResidentVortexSession::new(
        policy.resource_envelope().memory_budget_bytes,
        policy.max_parallelism,
    )?;
    let source = session.prepare_file(&path)?;
    let columns = super::projected_column_names(source.dtype(), &request.projection, request.kind)?;
    let fields = columns
        .iter()
        .map(|name| Ok((name.clone(), source_field(source.dtype(), name)?)))
        .collect::<Result<_>>()?;
    let mut result = CompletedRows::new(fields, session.memory())?;
    source.validate_generation()?;
    let scan = super::read_local_vortex_sort_rows_scan_with_output(
        uri,
        &path,
        request,
        policy,
        Some(&mut result),
    )?;
    source.validate_generation()?;
    let (array, ownership) = result.into_array()?;
    let result = session.own_completed_array(array, ownership)?;
    let mut report = write(result, request, output, format, overwrite, policy)?;
    report.rows_scanned = scan.scan.source_row_count;
    report.pre_limit_result_row_count = super::usize_to_u64(scan.scan.pre_limit_result_row_count)?;
    report.arrays_read_count = scan.scan.arrays_read_count;
    report.max_chunk_rows = scan.scan.max_chunk_rows;
    report.state_budget = super::sort_rows_state_budget_report(
        request,
        &scan.scan,
        report.pre_limit_result_row_count,
    )?;
    report.evidence.upstream_scan_called = scan.scan.arrays_read_count > 0;
    report.evidence.side_effects.data_read |= report.evidence.upstream_scan_called;
    report.evidence.side_effects.data_decoded |= report.evidence.upstream_scan_called;
    report.evidence.side_effects.data_materialized |= report.evidence.upstream_scan_called;
    report.evidence.pushdown = super::VortexLocalPrimitiveRowExportPushdownEvidence {
        filter_pushdown_applied: scan.scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.scan.projection_pushdown_applied,
        source_order_limit_applied: scan.scan.source_order_limit.is_some(),
    };
    Ok(report)
}
