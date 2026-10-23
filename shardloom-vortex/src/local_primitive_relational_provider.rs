//! Native file strategies shared by typed and SQL relational declarations.
//! Selection is metadata-only, after the ordinary complete-tree type binder.

use super::{NativeExecutionContext, Node, Result, VortexLocalPrimitiveExecutionPolicy, failed};
use crate::{
    local_primitives::{
        self as native, VortexLocalPrimitiveExecutionReport as Report,
        VortexQueryPrimitiveKind as Kind, VortexQueryPrimitiveRequest as Request,
    },
    relational_query::VortexRelationalPlan,
    resident_session::PreparedVortexSource,
};
use shardloom_core::NativeIoCertificate;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{ConstantArray, StructArray},
    dtype::{DType, FieldNames, Nullability, PType},
    validity::Validity,
};

#[path = "local_primitive_relational_provider_plan.rs"]
mod plan;
pub(super) use plan::scan_predicate;

pub(super) struct Prepared {
    strategy: Strategy,
    // Provider field -> public result name. This is an encoded-column view.
    output: Vec<(String, String)>,
    _metadata: MemoryLease,
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
}

pub(super) struct Executed {
    pub(super) report: Report,
    pub(super) certificate: NativeIoCertificate,
}

pub(super) fn prepare(
    plan: &VortexRelationalPlan,
    root: &Node,
    source: &PreparedVortexSource,
    policy: VortexLocalPrimitiveExecutionPolicy,
    memory: &LiveMemoryPool,
    bound_metadata_bytes: u64,
) -> Result<Option<Prepared>> {
    // Binder accounting covers nodes, expressions, names and schemas. Reserve
    // a conservative second representation before cloning any lowered request.
    let bytes = bound_metadata_bytes
        .checked_mul(4)
        .ok_or_else(|| failed("provider declaration capacity overflow"))?;
    let capacity = memory.snapshot();
    if bytes > capacity.limit_bytes.saturating_sub(capacity.reserved_bytes) / 2 {
        return Ok(None); // Choose the already-admitted general provider before execution.
    }
    let metadata = memory.reserve(bytes)?;
    let names = root
        .fields
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let Some(lowered) = plan::lower(plan, source.dtype(), &names) else {
        return Ok(None);
    };
    let request = lowered.request;
    native::prepared_dispatch::source_session(source, &request, Some(policy))?;
    if native::prepared_dispatch::request_requires_relational(source, &request)? {
        return Ok(None);
    }
    let fields = provider_fields(&request, source.dtype())?;
    if having_requires_typed_filter(&request, source.dtype(), &fields)?
        || ordering_requires_typed_execution(&request, source.dtype(), &fields)?
    {
        return Ok(None);
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
        return Ok(None);
    }
    let strategy = match request.kind {
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
        _ => return Err(failed("unrecognized admitted provider")),
    };
    Ok(Some(Prepared {
        strategy,
        output: lowered.output.into_iter().zip(names).collect(),
        _metadata: metadata,
    }))
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
        if dtype.is_nullable() && !grouped_measure_is_nonnull(request, source, &expression.column)?
        {
            return Ok(true);
        }
    }
    Ok(false)
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
        Kind::SortRows => {
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
        let (report, mut certificate) = match &self.strategy {
            Strategy::Count {
                request,
                source,
                policy,
            } => source.with_admitted_native_execution(context, |file, context| {
                let count = file.row_count();
                emit_count(count, context, &mut emit)?;
                let (policy, physical) = policy.with_physical_policy_for_request(request);
                let report = native::count_all_held_metadata_report(file, count, policy)
                    .with_physical_policy(physical);
                let certificate = native::local_primitive_native_io_certificate(request, &report)?;
                Ok((report, certificate))
            })?,
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
        };
        certificate.source_pushdown_report.proof_basis.push_str(
            ";complete_typed_plan_validated_before_provider_selection;shared_native_provider;encoded_output_alias_view;single_operation_resource_owner;source_generation_validated_after_final_consumer",
        );
        Ok(Executed {
            report,
            certificate,
        })
    }
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
