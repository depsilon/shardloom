//! Bind completed native scalar values to the declared result schema before a sink.
//! No source replay, data-dependent schema inference, or serialized JSON round trip.

use super::{
    AggregateValueTransform, Result, ShardLoomError, SimpleAggregateFunction,
    VortexQueryPrimitiveRequest, required_simple_aggregate, vortex_error,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability, PType},
};

const MAX_ROWS: usize = 65_536;
const MAX_BYTES: usize = 8 * 1024 * 1024;

pub(super) struct CompletedRows<'a> {
    fields: Vec<(String, DType)>,
    memory: LiveMemoryPool,
    array: Option<ArrayRef>,
    ownership: MemoryLease,
    delivery: Delivery<'a>,
    finished: bool,
}

enum Delivery<'a> {
    Collect,
    #[cfg_attr(not(unix), allow(dead_code))]
    Stream {
        batch_rows: usize,
        cancellation: CancellationToken,
        consume: &'a mut dyn FnMut(ArrayRef) -> Result<()>,
    },
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
    aggregate_fields_with_bounds(request, source, true)
}

#[cfg(all(unix, feature = "vortex-write"))]
pub(super) fn aggregate_stream_fields(
    request: &VortexQueryPrimitiveRequest,
    source: &DType,
) -> Result<Vec<(String, DType)>> {
    aggregate_fields_with_bounds(request, source, false)
}

fn aggregate_fields_with_bounds(
    request: &VortexQueryPrimitiveRequest,
    source: &DType,
    collect: bool,
) -> Result<Vec<(String, DType)>> {
    let aggregate = required_simple_aggregate(request)?;
    if collect && aggregate.spill.is_some() {
        return Err(failed("owned aggregate spill output is not admitted"));
    }
    if collect
        && (!aggregate.group_by.is_empty() || !aggregate.group_expressions.is_empty())
        && request.source_order_limit.is_some_and(|limit| {
            aggregate
                .offset
                .checked_add(limit)
                .is_none_or(|n| n > MAX_ROWS)
        })
    {
        return Err(failed("grouped output limit plus offset exceeds 65536"));
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

#[cfg(unix)]
impl<'consumer> CompletedRows<'consumer> {
    pub(super) fn streaming(
        fields: Vec<(String, DType)>,
        memory: &LiveMemoryPool,
        batch_rows: usize,
        cancellation: CancellationToken,
        consume: &'consumer mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<Self> {
        if batch_rows == 0 || batch_rows > 8192 {
            return Err(failed("native stream batch rows must be in 1..=8192"));
        }
        let mut output = Self::new(fields, memory)?;
        output.delivery = Delivery::Stream {
            batch_rows,
            cancellation,
            consume,
        };
        Ok(output)
    }
}

impl CompletedRows<'_> {
    pub(super) fn is_streaming(&self) -> bool {
        matches!(self.delivery, Delivery::Stream { .. })
    }

    pub(super) fn finish_values<'a>(
        &mut self,
        columns: &[String],
        rows: usize,
        value: impl FnMut(usize, usize) -> Result<super::result_batch::Value<'a>>,
    ) -> Result<()> {
        self.push_values(columns, rows, value)?;
        self.finished = true;
        Ok(())
    }

    pub(super) fn finish_stream(&mut self) -> Result<()> {
        if !self.is_streaming() || self.finished {
            return Err(failed("stream already finished or not admitted"));
        }
        self.finished = true;
        Ok(())
    }

    pub(super) fn push_values<'a>(
        &mut self,
        columns: &[String],
        rows: usize,
        mut value: impl FnMut(usize, usize) -> Result<super::result_batch::Value<'a>>,
    ) -> Result<()> {
        if (!self.is_streaming() && rows > MAX_ROWS)
            || self.finished
            || !columns.iter().eq(self.fields.iter().map(|(name, _)| name))
        {
            return Err(failed(
                "completed native rows or schema differ from admission",
            ));
        }
        let allocator = std::sync::Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
            self.memory.clone(),
        )) as vortex::array::memory::HostAllocatorRef;
        match &mut self.delivery {
            Delivery::Collect => {
                self.array = Some(super::result_batch::build(
                    &self.fields,
                    rows,
                    MAX_BYTES,
                    &allocator,
                    &self.memory,
                    value,
                )?);
            }
            Delivery::Stream {
                batch_rows,
                cancellation,
                consume,
            } => {
                let mut start = 0;
                loop {
                    cancellation.check()?;
                    let mut count = (rows - start).min(*batch_rows);
                    while super::result_batch::buffer_bytes(
                        &self.fields,
                        count,
                        &mut |row, column| value(start + row, column),
                    )? > MAX_BYTES
                    {
                        if count <= 1 {
                            return Err(failed(
                                "one complete output row exceeds the native batch byte bound",
                            ));
                        }
                        count = count.div_ceil(2);
                    }
                    let array = super::result_batch::build(
                        &self.fields,
                        count,
                        MAX_BYTES,
                        &allocator,
                        &self.memory,
                        |row, column| value(start + row, column),
                    )?;
                    cancellation.check()?;
                    consume(array)?;
                    cancellation.check()?;
                    start += count;
                    if start == rows {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn admit_group_count(&self, limit: Option<usize>, groups: usize) -> Result<usize> {
        if self.is_streaming() {
            return Ok(limit.map_or(groups, |limit| groups.min(limit)));
        }
        if limit.is_none() && groups > MAX_ROWS {
            return Err(failed(
                "grouped output without a limit exceeds 65536 groups",
            ));
        }
        let rows = limit.map_or(groups, |limit| groups.min(limit));
        if rows > MAX_ROWS || self.fields.len().saturating_mul(rows).saturating_mul(32) > MAX_BYTES
        {
            return Err(failed("grouped output exceeds 8 MiB output admission"));
        }
        Ok(rows)
    }

    pub(super) fn has_utf8(&self) -> bool {
        self.fields
            .iter()
            .any(|(_, dtype)| matches!(dtype, DType::Utf8(_)))
    }

    /// Admit and reserve the scalar-row bridge before its JSON/StatValue clones.
    /// The string bound is conservative across retained candidates, before HAVING
    /// and final selection. Native builders acquire their own overlapping lease.
    pub(super) fn reserve_finalization(
        &self,
        rows: usize,
        max_utf8_len: usize,
    ) -> Result<MemoryLease> {
        let utf8_fields = self
            .fields
            .iter()
            .filter(|(_, dtype)| matches!(dtype, DType::Utf8(_)))
            .count();
        let string_bytes = rows
            .saturating_mul(utf8_fields)
            .saturating_mul(max_utf8_len);
        let cells = rows.saturating_mul(self.fields.len());
        if rows > MAX_ROWS || cells.saturating_mul(32).saturating_add(string_bytes) > MAX_BYTES {
            return Err(failed(
                "completed values exceed 8 MiB output admission before finalization",
            ));
        }
        // Per cell: two scalar/map representations plus BTreeMap node slack.
        // Names may be 256 bytes and are copied into each result row. Per row:
        // vector/map headers and growth; strings allow overlapping value clones.
        let name_bytes: usize = self.fields.iter().map(|(name, _)| name.len()).sum();
        let bytes = cells
            .saturating_mul(256)
            .saturating_add(rows.saturating_mul(name_bytes).saturating_mul(2))
            .saturating_add(rows.saturating_mul(1024))
            .saturating_add(string_bytes.saturating_mul(4))
            .saturating_add(64 * 1024);
        self.memory
            .reserve(u64::try_from(bytes).map_err(vortex_error)?)
    }

    /// Candidate identities, order values and selected references remain live
    /// while batches are delivered. They are separate from payload buffers.
    pub(super) fn reserve_selection(
        &self,
        groups: usize,
        order_fields: usize,
        max_utf8_len: usize,
    ) -> Result<MemoryLease> {
        let per_group = 256_usize
            .checked_add(
                order_fields
                    .checked_mul(128_usize.saturating_add(max_utf8_len))
                    .ok_or_else(|| failed("selection reservation overflow"))?,
            )
            .ok_or_else(|| failed("selection reservation overflow"))?;
        let bytes = groups
            .checked_mul(per_group)
            .and_then(|n| n.checked_add(64 * 1024))
            .ok_or_else(|| failed("selection reservation overflow"))?;
        self.memory
            .reserve(u64::try_from(bytes).map_err(vortex_error)?)
    }

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
                        | DType::Variant(_)
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
            delivery: Delivery::Collect,
            finished: false,
        })
    }

    pub(super) fn into_array(self) -> Result<(ArrayRef, MemoryLease)> {
        Ok((
            self.array
                .ok_or_else(|| failed("result was not completed"))?,
            self.ownership,
        ))
    }
}

#[cfg(all(feature = "vortex-write", unix))]
#[allow(clippy::too_many_arguments)]
pub(super) fn write_stream(
    plan: super::native_sink::NativeSinkPlan,
    request: &VortexQueryPrimitiveRequest,
    path: &std::path::Path,
    format: super::VortexLocalPrimitiveRowExportFormat,
    overwrite: bool,
    policy: super::VortexLocalPrimitiveExecutionPolicy,
    producer: &mut super::native_sink::ArrayProducer<'_>,
    cancellation: &CancellationToken,
) -> Result<super::VortexLocalPrimitiveRowExportReport> {
    write_stream_admitted(
        plan,
        request,
        path,
        format,
        overwrite,
        policy,
        producer,
        cancellation,
        None,
    )
}

#[cfg(all(feature = "vortex-write", unix))]
#[allow(clippy::too_many_arguments)]
pub(super) fn write_stream_admitted(
    plan: super::native_sink::NativeSinkPlan,
    request: &VortexQueryPrimitiveRequest,
    path: &std::path::Path,
    format: super::VortexLocalPrimitiveRowExportFormat,
    overwrite: bool,
    policy: super::VortexLocalPrimitiveExecutionPolicy,
    producer: &mut super::native_sink::ArrayProducer<'_>,
    cancellation: &CancellationToken,
    admitted: Option<&crate::resident_session::NativeExecutionContext<'_>>,
) -> Result<super::VortexLocalPrimitiveRowExportReport> {
    if format == super::VortexLocalPrimitiveRowExportFormat::Vortex {
        return plan.write_produced(
            request,
            path,
            overwrite,
            policy,
            Some(producer),
            cancellation,
            admitted,
        );
    }
    if matches!(
        format,
        super::VortexLocalPrimitiveRowExportFormat::Json
            | super::VortexLocalPrimitiveRowExportFormat::Jsonl
            | super::VortexLocalPrimitiveRowExportFormat::Csv
    ) {
        return super::native_text_sink::write(
            plan,
            request,
            path,
            format,
            overwrite,
            policy,
            Some(producer),
            cancellation,
            admitted,
        );
    }
    #[cfg(feature = "universal-format-io")]
    if format.is_compatibility_binary() {
        let limits = super::columnar_compat_sink::CompatibilityLimits::streaming(
            plan.row_count,
            cancellation,
        );
        return super::columnar_compat_sink::prepare_plan(request, plan, format, policy, limits)?
            .ok_or_else(|| failed("result schema is outside compatibility output admission"))?
            .write_produced(path, overwrite, producer, cancellation, admitted)
            .map(|completed| completed.report);
    }
    Err(failed("result stream format is not admitted"))
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
    if matches!(
        format,
        super::VortexLocalPrimitiveRowExportFormat::Json
            | super::VortexLocalPrimitiveRowExportFormat::Jsonl
            | super::VortexLocalPrimitiveRowExportFormat::Csv
    ) {
        return super::native_text_sink::write(
            plan,
            request,
            path,
            format,
            overwrite,
            policy,
            None,
            &CancellationToken::default(),
            None,
        );
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
    let sort = super::required_sort_rows(request)?;
    let cancellation = sort
        .spill
        .as_ref()
        .map_or_else(CancellationToken::default, |spill| {
            CancellationToken::from_shared_flag(std::sync::Arc::clone(&spill.cancellation))
        });
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
    let fields: Vec<(String, DType)> = columns
        .iter()
        .map(|name| Ok((name.clone(), source_field(source.dtype(), name)?)))
        .collect::<Result<_>>()?;
    // Validate shape before opening a staged output, without constructing results.
    drop(CompletedRows::new(fields.clone(), session.memory())?);
    let upper_rows = if sort.tie_policy == super::VortexSortTiePolicy::All {
        source.file().row_count()
    } else {
        source
            .file()
            .row_count()
            .min(request.source_order_limit.unwrap_or(usize::MAX) as u64)
    };
    let plan = super::native_sink::NativeSinkPlan::produced(
        session.clone(),
        DType::struct_(fields.clone(), Nullability::NonNullable),
        upper_rows,
        Some(std::fs::canonicalize(&path).map_err(vortex_error)?),
        Some(source.clone()),
    )?;
    let mut completed = None;
    let mut producer = |context: &crate::resident_session::NativeExecutionContext<'_>,
                        batch_rows,
                        consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
        source.with_admitted_native_execution(context, |file, context| {
            let generation = sort
                .spill
                .as_ref()
                .map(|_| {
                    super::sort_spill::SortSourceGeneration::capture(&path).map(std::sync::Arc::new)
                })
                .transpose()?;
            let mut accept = |array| {
                if !consume(array)? {
                    return Err(failed("ordered result consumer stopped before completion"));
                }
                Ok(())
            };
            let mut result = CompletedRows::streaming(
                fields.clone(),
                context.memory(),
                batch_rows,
                context.cancellation().clone(),
                &mut accept,
            )?;
            completed = Some(super::read_opened_local_vortex_sort_rows_scan_with_output(
                uri,
                request,
                policy,
                Some(&mut result),
                file,
                context.native_session(),
                context.runtime(),
                generation.as_ref(),
                Some(context),
            )?);
            Ok(())
        })
    };
    let report = write_stream(
        plan,
        request,
        output,
        format,
        overwrite,
        policy,
        &mut producer,
        &cancellation,
    )?;
    let scan = completed.ok_or_else(|| failed("ordered result did not complete"))?;
    finish_sort_report(request, &scan, report)
}

#[cfg(all(feature = "vortex-write", unix))]
fn finish_sort_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &super::LocalVortexRowsScan,
    mut report: super::VortexLocalPrimitiveRowExportReport,
) -> Result<super::VortexLocalPrimitiveRowExportReport> {
    report.rows_scanned = scan.scan.source_row_count;
    report.pre_limit_result_row_count = super::usize_to_u64(scan.scan.pre_limit_result_row_count)?;
    report.arrays_read_count = scan.scan.arrays_read_count;
    report.max_chunk_rows = scan.scan.max_chunk_rows;
    report.state_budget = super::sort_rows_report(request, scan)?.state_budget;
    report.evidence.upstream_scan_called = scan.scan.arrays_read_count > 0;
    report.evidence.side_effects.data_read |= report.evidence.upstream_scan_called;
    report.evidence.side_effects.data_decoded |= report.evidence.upstream_scan_called;
    report.evidence.side_effects.data_materialized |= report.evidence.upstream_scan_called;
    report.evidence.side_effects.row_read |= report.evidence.upstream_scan_called;
    if let Some(evidence) = report.evidence.native_array_sink.as_mut() {
        evidence.pre_limit_result_row_count = report.pre_limit_result_row_count;
    }
    report.evidence.pushdown = super::VortexLocalPrimitiveRowExportPushdownEvidence {
        filter_pushdown_applied: scan.scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.scan.projection_pushdown_applied,
        source_order_limit_applied: scan.scan.source_order_limit.is_some(),
    };
    Ok(report)
}
