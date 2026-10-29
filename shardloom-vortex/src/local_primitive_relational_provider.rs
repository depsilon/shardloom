//! Native file strategies shared by typed and SQL relational declarations.
//! Selection is metadata-only, after the ordinary complete-tree type binder.

use super::{
    Metrics, NativeExecutionContext, Node, Result, VortexLocalPrimitiveExecutionPolicy, add, failed,
};
use crate::{
    local_primitives::{
        self as native, VortexLocalPrimitiveExecutionReport as Report,
        VortexQueryPrimitiveKind as Kind, VortexQueryPrimitiveRequest as Request,
    },
    resident_session::{PreparedVortexProjection, PreparedVortexSource},
};
use shardloom_core::NativeIoCertificate;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{ConstantArray, StructArray},
    dtype::{DType, FieldNames, Nullability, PType},
    validity::Validity,
};

#[path = "local_primitive_relational_provider_memory.rs"]
mod memory;
#[path = "local_primitive_relational_provider_plan.rs"]
mod plan;
pub(super) use plan::scan_predicate;

pub(super) struct Prepared {
    strategy: Strategy,
    // Provider field -> public result name. This is an encoded-column view.
    output: Vec<(String, String)>,
    _metadata: MemoryLease,
}

pub(super) struct Selection {
    pub(super) provider: Option<Prepared>,
    pub(super) reason: &'static str,
}

impl Selection {
    pub(super) const fn declined(reason: &'static str) -> Self {
        Self {
            provider: None,
            reason,
        }
    }
}

enum Strategy {
    Count {
        request: Request,
        source: PreparedVortexSource,
        policy: VortexLocalPrimitiveExecutionPolicy,
    },
    CountWhere(Box<native::prepared_count::PreparedVortexCountWhere>),
    Aggregate(Box<native::prepared_aggregate::PreparedVortexAggregate>),
    Sort {
        request: Request,
        source: PreparedVortexSource,
        policy: VortexLocalPrimitiveExecutionPolicy,
    },
    Projection {
        source: PreparedVortexSource,
        operation: Box<PreparedVortexProjection>,
    },
}

pub(super) struct Executed {
    pub(super) report: Option<Report>,
    pub(super) certificate: Option<NativeIoCertificate>,
}

pub(super) fn prepare(
    root: &Node,
    source: &PreparedVortexSource,
    policy: VortexLocalPrimitiveExecutionPolicy,
    memory: &LiveMemoryPool,
    float_policy: shardloom_core::FloatPolicy,
) -> Result<Selection> {
    if float_policy == shardloom_core::FloatPolicy::Extended {
        return Ok(Selection::declined(
            "extended_float_policy_requires_shared_relational_execution",
        ));
    }
    let Some(bytes) = memory::scratch(root)? else {
        return Ok(Selection::declined(
            "bound_operation_requires_relational_execution",
        ));
    };
    let capacity = memory.snapshot();
    if bytes > capacity.limit_bytes.saturating_sub(capacity.reserved_bytes) {
        return Ok(Selection::declined(
            "provider_scratch_exceeds_available_credit",
        ));
    }
    let mut metadata = memory.reserve(bytes)?;
    let names = root
        .fields
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let Some(lowered) = plan::lower(root, source.dtype(), &names) else {
        return Ok(Selection::declined(
            "bound_semantics_have_no_equivalent_file_provider",
        ));
    };
    let bytes = memory::retained(&lowered.request, &lowered.output, &names)?;
    let capacity = memory.snapshot();
    if bytes
        > capacity
            .limit_bytes
            .saturating_sub(capacity.reserved_bytes)
            .saturating_add(metadata.bytes())
    {
        return Ok(Selection::declined(
            "provider_retained_metadata_exceeds_available_credit",
        ));
    }
    metadata.resize(bytes)?;
    let request = lowered.request;
    native::prepared_dispatch::source_session(source, &request, Some(policy))?;
    if native::prepared_dispatch::request_requires_relational(source, &request)? {
        return Ok(Selection::declined(
            "source_types_require_relational_execution",
        ));
    }
    if matches!(request.kind, Kind::ProjectColumns | Kind::FilterAndProject)
        && native::row_export_scan_plan(&request, source.dtype())?
            .residual_predicate
            .is_some()
    {
        return Ok(Selection::declined(
            "projection_residual_requires_relational_execution",
        ));
    }
    let fields = provider_fields(&request, source.dtype())?;
    if having_requires_typed_filter(&request, source.dtype(), &fields)? {
        return Ok(Selection::declined(
            "having_requires_typed_null_or_exact_numeric_semantics",
        ));
    }
    if ordering_requires_typed_execution(&request, source.dtype(), &fields)? {
        return Ok(Selection::declined(
            "ordering_requires_explicit_typed_null_semantics",
        ));
    }
    if lowered.output.len() != root.fields.len()
        || lowered
            .output
            .iter()
            .zip(&root.fields)
            .any(|(name, (_, dtype))| {
                fields
                    .iter()
                    .find(|(field, _)| field == name)
                    .is_none_or(|(_, candidate)| candidate != dtype)
            })
    {
        return Ok(Selection::declined(
            "provider_result_schema_is_not_equivalent",
        ));
    }
    let strategy = prepare_strategy(request, source, policy)?;
    Ok(Selection {
        provider: Some(Prepared {
            strategy,
            output: lowered.output.into_iter().zip(names).collect(),
            _metadata: metadata,
        }),
        reason: "equivalent_bound_native_file_provider_selected",
    })
}

fn prepare_strategy(
    request: Request,
    source: &PreparedVortexSource,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<Strategy> {
    Ok(match request.kind {
        Kind::CountAll => Strategy::Count {
            request,
            source: source.clone(),
            policy,
        },
        Kind::CountWhere => Strategy::CountWhere(Box::new(
            native::prepared_count::prepare_count_where_from_source(
                &request,
                policy,
                source.clone(),
            )?,
        )),
        Kind::SimpleAggregate => Strategy::Aggregate(Box::new(
            native::prepared_aggregate::prepare_aggregate_from_source(
                &request,
                policy,
                source.clone(),
            )?,
        )),
        Kind::SortRows => Strategy::Sort {
            request,
            source: source.clone(),
            policy,
        },
        Kind::ProjectColumns | Kind::FilterAndProject => {
            let plan = native::row_export_scan_plan(&request, source.dtype())?;
            let filter = plan
                .filter
                .as_ref()
                .map(|filter| native::bind_vortex_scan_expr(source.file(), filter))
                .transpose()?;
            let columns = plan
                .projected_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let max_rows = request.source_order_limit.map_or_else(
                || source.file().row_count().max(1),
                |count| u64::try_from(count).unwrap_or(u64::MAX),
            );
            let operation = source
                .prepare_projection(&columns, max_rows, u64::MAX)?
                .with_filter(filter);
            Strategy::Projection {
                source: source.clone(),
                operation: Box::new(operation),
            }
        }
        _ => return Err(failed("unrecognized admitted provider")),
    })
}

fn having_requires_typed_filter(
    request: &Request,
    source: &DType,
    fields: &[(String, DType)],
) -> Result<bool> {
    let Some(aggregate) = &request.simple_aggregate else {
        return Ok(false);
    };
    for expression in &aggregate.having {
        let dtype = fields
            .iter()
            .find(|(name, _)| name == &expression.column)
            .map(|(_, dtype)| dtype)
            .ok_or_else(|| failed("HAVING field is absent"))?;
        // Primitive HAVING uses total value ordering. The typed filter instead
        // rejects unknown comparisons, including fields hidden by projection.
        if (dtype.is_nullable()
            && !grouped_measure_is_nonnull(request, source, &expression.column)?)
            || having_has_lossy_numeric_comparison(dtype, &expression.value)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn having_has_lossy_numeric_comparison(dtype: &DType, literal: &str) -> bool {
    let serde_json::Value::Number(value) = native::aggregate_having_value_json(literal) else {
        return false;
    };
    match dtype {
        DType::Primitive(PType::F16 | PType::F32 | PType::F64, _) => {
            // Every integer through +/-2^53 is exactly representable as f64.
            // Outside that consecutive range retain the typed exact comparator.
            value.as_i64().is_some_and(|value| {
                !(-9_007_199_254_740_992..=9_007_199_254_740_992).contains(&value)
            }) || value
                .as_u64()
                .is_some_and(|value| value > 9_007_199_254_740_992)
        }
        DType::Primitive(ptype, _) if ptype.is_int() => {
            // At +/-2^53 itself, an adjacent integer can round onto the
            // floating threshold. Strictly interior thresholds are safe.
            value.is_f64()
                && value
                    .as_f64()
                    .is_some_and(|value| value.abs() >= 9_007_199_254_740_992.0)
        }
        _ => false,
    }
}

fn ordering_requires_typed_execution(
    request: &Request,
    source: &DType,
    fields: &[(String, DType)],
) -> Result<bool> {
    let ordering = match request.kind {
        Kind::SimpleAggregate => request
            .simple_aggregate
            .as_ref()
            .expect("lowered aggregate")
            .order_by
            .as_slice(),
        Kind::SortRows => request
            .sort_rows
            .as_ref()
            .expect("lowered sort")
            .order_by
            .as_slice(),
        _ => &[],
    };
    for key in ordering {
        let dtype = if request.kind == Kind::SortRows {
            native::completed_result::source_field(source, &key.column)?
        } else {
            fields
                .iter()
                .find(|(name, _)| name == &key.column)
                .map(|(_, dtype)| dtype.clone())
                .ok_or_else(|| failed("ordering field is absent"))?
        };
        if key.nulls.is_none()
            && dtype.is_nullable()
            && !grouped_measure_is_nonnull(request, source, &key.column)?
        {
            return Ok(true); // Retain the relational runtime's explicit NULL ordering check.
        }
    }
    Ok(false)
}

fn grouped_measure_is_nonnull(request: &Request, source: &DType, name: &str) -> Result<bool> {
    let Some(aggregate) = request.simple_aggregate.as_ref() else {
        return Ok(false);
    };
    // Every emitted group has at least one input row. A scalar reduction can
    // instead emit NULL on empty input. Keep the public nullable output schema;
    // this proof only admits consumers that cannot encounter a NULL value.
    if aggregate.group_by.is_empty() && aggregate.group_expressions.is_empty() {
        return Ok(false);
    }
    let Some(measure) = aggregate
        .measures
        .iter()
        .find(|measure| measure.alias == name)
    else {
        return Ok(false);
    };
    if !matches!(
        measure.function.to_ascii_lowercase().as_str(),
        "sum" | "avg" | "min" | "max"
    ) || !matches!(
        measure.value_transform.as_deref(),
        None | Some(
            "length" | "extract_minute" | "date_trunc_minute" | "url_domain" | "regex_domain"
        )
    ) {
        return Ok(false);
    }
    let Some(column) = &measure.column else {
        return Ok(false);
    };
    Ok(!native::completed_result::source_field(source, column.as_str())?.is_nullable())
}

fn provider_fields(request: &Request, source: &DType) -> Result<Vec<(String, DType)>> {
    match request.kind {
        Kind::CountAll | Kind::CountWhere => Ok(vec![(
            "count".into(),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        )]),
        Kind::SimpleAggregate => native::completed_result::aggregate_stream_fields(request, source),
        Kind::SortRows | Kind::ProjectColumns | Kind::FilterAndProject => {
            native::projected_column_names(source, &request.projection, request.kind)?
                .into_iter()
                .map(|name| {
                    native::completed_result::source_field(source, &name).map(|dtype| (name, dtype))
                })
                .collect()
        }
        _ => Err(failed("unrecognized lowered provider")),
    }
}

impl Prepared {
    pub(super) fn consume(
        &self,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<Executed> {
        let mut emit = |array: ArrayRef| {
            let mut columns = native::native_capacity::ReservedVec::new(context.memory())?;
            columns.reserve(self.output.len())?;
            for (source, _) in &self.output {
                columns
                    .values
                    .push(native::logical_field_from_native_array(&array, source)?);
            }
            let (columns, _ownership) = columns.into_parts();
            let output = StructArray::try_new(
                self.output
                    .iter()
                    .map(|(_, name)| name.as_str())
                    .collect::<FieldNames>(),
                columns,
                array.len(),
                Validity::NonNullable,
            )
            .map_err(native::vortex_error)?
            .into_array();
            if output.is_empty() {
                return consume(output);
            }
            for start in (0..output.len()).step_by(batch_rows) {
                context.check_cancelled()?;
                consume(
                    output
                        .slice(start..output.len().min(start + batch_rows))
                        .map_err(native::vortex_error)?,
                )?;
            }
            Ok(())
        };
        if let Strategy::Projection { source, operation } = &self.strategy {
            add(&metrics.scans_started, 1)?;
            if source.file().row_count() > 0 {
                add(&metrics.data_scans, 1)?;
            }
            operation.consume_in_context(context, &mut |array| {
                add(&metrics.scan_rows, array.len() as u64)?;
                add(&metrics.scan_batches, 1)?;
                emit(array)
            })?;
            // The ordinary relational certificate describes the actual native
            // batches and sink. Do not fabricate aggregate/decode measurements
            // or label this native producer as a JSON collection.
            return Ok(Executed {
                report: None,
                certificate: None,
            });
        }
        let (report, mut certificate) = match &self.strategy {
            Strategy::Count {
                request,
                source,
                policy,
            } => consume_count(request, source, *policy, context, &mut emit)?,
            Strategy::CountWhere(operation) => {
                let execution = operation.execute_in_context(context)?;
                emit_count(execution.count, context, &mut emit)?;
                (execution.report, execution.native_io_certificate)
            }
            Strategy::Aggregate(operation) => {
                let execution =
                    operation.consume_native_in_context(context, batch_rows, &mut emit)?;
                (execution.report, execution.native_io_certificate)
            }
            Strategy::Sort {
                request,
                source,
                policy,
            } => {
                let (report, _) = native::prepared_dispatch::execute_sort_in_context(
                    request,
                    *policy,
                    source,
                    Some(&mut |array, _| emit(array)),
                    context,
                )?;
                let certificate = native::local_primitive_native_io_certificate(request, &report)?;
                (report, certificate)
            }
            Strategy::Projection { .. } => unreachable!("projection consumed above"),
        };
        certificate.source_pushdown_report.proof_basis.push_str(
            ";complete_typed_plan_validated_before_provider_selection;shared_native_provider;encoded_output_alias_view;single_operation_resource_owner;source_generation_validated_after_final_consumer",
        );
        Ok(Executed {
            report: Some(report),
            certificate: Some(certificate),
        })
    }
}

fn consume_count(
    request: &Request,
    source: &PreparedVortexSource,
    policy: VortexLocalPrimitiveExecutionPolicy,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<(Report, NativeIoCertificate)> {
    // Footer metadata is already held by this generation. It needs the caller's
    // ownership and generation checks, but no scoped payload reader or I/O.
    source
        .retained_session()
        .validate_execution_context(context)?;
    context.check_cancelled()?;
    source.validate_generation()?;
    let file = source.file();
    let count = file.row_count();
    emit_count(count, context, consume)?;
    source.validate_generation()?;
    context.check_cancelled()?;
    let (policy, physical) = policy.with_physical_policy_for_request(request);
    let report =
        native::count_all_held_metadata_report(file, count, policy).with_physical_policy(physical);
    let certificate = native::local_primitive_native_io_certificate(request, &report)?;
    Ok((report, certificate))
}

fn emit_count(
    count: u64,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let _ownership = crate::native_payload_schema::reserve_names(context.memory(), 1, |_| "count")?;
    consume(
        StructArray::try_new(
            FieldNames::from(vec!["count"]),
            vec![ConstantArray::new(count, 1).into_array()],
            1,
            Validity::NonNullable,
        )
        .map_err(native::vortex_error)?
        .into_array(),
    )
}
