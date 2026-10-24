//! Shared workflow grammar and metadata-based provider selection for embedding.

use super::{
    Arc, CancellationToken, ExecutionResources, PreparedKind, PreparedNativeOperation,
    PreparedPrimitive, PreparedVortexRelational, PreparedVortexSource, PrimitiveMetadata,
    PrimitiveProvider, ResidentVortexSession, Result, VortexLocalPrimitiveExecutionPolicy,
    VortexQueryPrimitiveRequest, failed, prepared_dispatch, workflow,
};
use shardloom_core::DatasetUri;
use shardloom_vortex::{
    VortexQueryPrimitiveKind as Kind,
    local_primitives::{
        collect, prepared_aggregate, prepared_count, prepared_relational, prepared_unary,
    },
    relational_query::{
        VortexRelationalAggregate, VortexRelationalLimit, VortexRelationalPlan,
        VortexRelationalScan, VortexRelationalSet, VortexRelationalSetKind, VortexRelationalUnary,
    },
};

/// Inert native workflow declaration parsed by the ordinary public command
/// grammar. Constructing a declaration never opens sources or destinations.
pub struct NativeWorkflowDeclaration {
    request: workflow::PublicWorkflowRouteRequest,
    control_bytes: u64,
    sql_preserves_operation: bool,
}

impl NativeWorkflowDeclaration {
    /// Select the same file SQL providers as the public workflow router. A
    /// nonmatching declaration remains on the shared relational binder; a
    /// selected provider's errors are never retried through another path.
    pub(in super::super) fn from_sql_request(
        mut request: workflow::PublicWorkflowRouteRequest,
    ) -> Result<Option<Self>> {
        request.infer_defaults();
        let mut request = workflow::effective_public_workflow_request(&request);
        if workflow::normalized_vortex_primitive(&request).is_none() {
            return Ok(None);
        }
        if !request.source_bindings.is_empty() {
            request.bind_flat_source()?;
        }
        validate_route(&request)?;
        let statement_bytes = request.sql_statement.as_ref().map_or(0, String::capacity);
        let bytes = request.source_bindings.iter().try_fold(
            u64::try_from(statement_bytes).map_err(|_| failed("SQL declaration size overflow"))?,
            |sum, (reference, binding)| {
                [
                    reference.capacity(),
                    binding.input_format.capacity(),
                    binding.source_schema.as_ref().map_or(0, String::capacity),
                    std::mem::size_of::<workflow::PublicSourceBinding>(),
                ]
                .into_iter()
                .try_fold(sum, |sum, size| {
                    sum.checked_add(
                        u64::try_from(size).map_err(|_| failed("SQL declaration size overflow"))?,
                    )
                    .ok_or_else(|| failed("SQL declaration size overflow"))
                })
            },
        )?;
        // As for the command declaration, reserve original text, inferred
        // payloads, request clones and provider preparation metadata together.
        let control_bytes = bytes
            .checked_mul(128)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| failed("SQL declaration metadata estimate overflow"))?;
        Ok(Some(Self {
            request,
            control_bytes,
            sql_preserves_operation: true,
        }))
    }

    /// Use the shared parser and route admission, including unused-option checks.
    /// Arguments begin with the surface (`python`, `dataframe`, `sql` or `cli`).
    /// # Errors
    /// Returns the same declaration/route errors as the command interface.
    pub fn new(arguments: Vec<String>) -> Result<Self> {
        let has_primitive_payload = arguments.iter().any(|argument| {
            argument.starts_with("--vortex-") || argument == "--native-vortex-right-input"
        });
        let bytes = arguments.iter().try_fold(0_u64, |sum, argument| {
            let bytes = u64::try_from(argument.capacity() + std::mem::size_of::<String>())
                .map_err(|_| failed("declaration size overflow"))?;
            sum.checked_add(bytes)
                .ok_or_else(|| failed("declaration size overflow"))
        })?;
        let control_bytes = bytes
            .checked_mul(128)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| failed("declaration metadata estimate overflow"))?;
        let request = workflow::PublicWorkflowRouteRequest::parse(arguments.into_iter())?;
        let request = workflow::effective_public_workflow_request(&request);
        if has_primitive_payload && workflow::normalized_vortex_primitive(&request).is_none() {
            return Err(failed(
                "native operation options require a matching complete primitive declaration",
            ));
        }
        validate_route(&request)?;
        Ok(Self {
            request,
            control_bytes,
            sql_preserves_operation: !has_primitive_payload,
        })
    }

    /// Prepare providers inside the supplied admitted owner. Metadata selection
    /// precedes execution and does not retry another provider after failure.
    /// # Errors
    /// Propagates invalid resources, source changes, schema and provider failures.
    #[allow(clippy::too_many_lines)] // Keep admission, source ownership and provider selection together.
    pub fn prepare(
        &self,
        resources: ExecutionResources,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<PreparedNativeOperation> {
        cancellation.check()?;
        let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(resources)?;
        prepared_relational::validate_session_policy(policy, session)?;
        let mut metadata = session.reserve_input_scratch(self.control_bytes)?;
        let mut request = self.request.clone();
        request.resources =
            crate::execution_resources::ResourceArguments::from_resources(resources);
        validate_route(&request)?;
        let primitive = workflow::normalized_vortex_primitive(&request);
        if primitive.is_none() {
            let statement = request.sql_statement.as_deref().ok_or_else(|| {
                failed("a compatibility input requires its complete shared SQL declaration")
            })?;
            let (operation, reusable) =
                workflow::resident_relational::prepare_in_session_with_reuse(
                    statement,
                    &request,
                    session,
                    |_| Ok(()),
                    cancellation,
                )?;
            return Ok(
                PreparedNativeOperation::relational(operation, resources).with_reuse(reusable)
            );
        }
        // Normalize compatibility sources through the same accounted preparation
        // used by CLI. Keep explicit primitive options on the rebound request;
        // dispatching only its SQL text would silently omit those options.
        let mut preparation_fields = Vec::new();
        let mut preparation_identity = None;
        if request.input_format.as_deref() != Some("vortex") {
            let route = workflow::plan_public_workflow_route(&request);
            let rebound = workflow::prepared_local_workflow_native_request(&request)
                .map_err(|plan| failed(plan.blocker_reason))?;
            let preparation = workflow::prepare_local_source_for_public_workflow_controlled(
                &rebound.left_source_uri,
                &rebound.left_source_format,
                &rebound.left_target,
                false,
                resources,
                Some(&session.shared_memory_pool()),
                request.source_fingerprint_policy.as_deref(),
                request.source_schema.as_deref(),
                cancellation,
            )
            .map_err(|error| match error {
                workflow::PreparationFacadeError::Runtime(error) => error,
                workflow::PreparationFacadeError::FeatureGated => {
                    failed("compatibility preparation is feature gated")
                }
            })?;
            preparation_fields = workflow::local_prepared_vortex_execution_attachment_fields(
                &request,
                &route,
                &preparation,
                false,
            );
            preparation_identity = Some(
                preparation
                    .identity
                    .ok_or_else(|| failed("preparation source generation proof is absent"))?,
            );
            request = rebound.request;
            validate_route(&request)?;
        }
        let primitive = workflow::normalized_vortex_primitive(&request)
            .ok_or_else(|| failed("native primitive declaration is absent"))?;
        let input = request
            .input_uri
            .as_deref()
            .ok_or_else(|| failed("native input is absent"))?;
        let binding = workflow::native_vortex_input_binding_for_uri(
            input,
            &session.shared_memory_pool(),
            cancellation,
        )?;
        let (primitive_request, argument, source_uris) =
            workflow::native_vortex_bound_request_and_arg(&request, primitive, &binding)?;
        // The binding owns source text. Reserve the additional retained provider
        // handles, request copies and vector capacity before preparing each part.
        let source_bytes = source_uris.iter().try_fold(0_u64, |sum, uri| {
            let bytes =
                u64::try_from(uri.as_str().len()).map_err(|_| failed("source size overflow"))?;
            sum.checked_add(
                bytes
                    .checked_mul(128)
                    .and_then(|n| n.checked_add(8192))
                    .ok_or_else(|| failed("source metadata estimate overflow"))?,
            )
            .ok_or_else(|| failed("source metadata estimate overflow"))
        })?;
        metadata.resize(
            metadata
                .bytes()
                .checked_add(source_bytes)
                .ok_or_else(|| failed("source metadata estimate overflow"))?,
        )?;
        let mut sources = Vec::with_capacity(source_uris.len());
        for uri in &source_uris {
            let mut part = primitive_request.clone();
            part.source_uri = Some(uri.clone());
            let mut source =
                prepared_dispatch::prepare_source_in_session(&part, policy, session, cancellation)?;
            if let Some(identity) = &preparation_identity {
                source = source.with_preparation_sources(vec![Arc::clone(identity)])?;
            }
            sources.push(source);
        }
        // Native-file SQL makes the same metadata-based choice as the CLI. The
        // primitive aggregate/sort/count paths cannot represent extended fields.
        // Preserve complete SQL (including grouping transforms, ordering and
        // HAVING), and hand already admitted sources to the shared typed binder.
        // Explicit primitive payloads are not replaced by their advisory SQL.
        let provider = if self.sql_preserves_operation
            && matches!(
                primitive_request.kind,
                Kind::SimpleAggregate | Kind::SortRows | Kind::CountWhere
            )
            && let Some(statement) = request.sql_statement.as_deref()
            && sources.iter().try_fold(false, |extended, source| {
                workflow::resident_sql::request_requires_relational(source, &primitive_request)
                    .map(|required| extended || required)
            })? {
            // Extended-field SQL already used typed output names. Preserve the
            // primitive names only for the HAVING path newly admitted here.
            let preserve_having_names = sources.iter().try_fold(true, |preserve, source| {
                prepared_dispatch::request_requires_relational(source, &primitive_request)
                    .map(|extended| preserve && !extended)
            })?;
            let statement =
                workflow::resident_sql::typed_statement(statement, &request, preserve_having_names);
            let operation = workflow::resident_relational::prepare_in_session(
                &statement,
                &request,
                session,
                |schemas| {
                    for (uri, source) in source_uris.iter().zip(&sources) {
                        schemas.register_prepared_source(uri, source.clone())?;
                    }
                    Ok(())
                },
                cancellation,
            )?;
            PrimitiveProvider::Relational(Box::new(operation))
        } else if primitive_request.kind == Kind::CountAll {
            let mut counts = Vec::with_capacity(sources.len());
            for (source, uri) in sources.iter().zip(&source_uris) {
                let mut part = primitive_request.clone();
                part.source_uri = Some(uri.clone());
                counts.push(collect::prepare_count_from_source(
                    &part,
                    source,
                    cancellation,
                )?);
            }
            PrimitiveProvider::Count(counts)
        } else if sources.len() > 1
            && matches!(
                primitive_request.kind,
                Kind::SimpleAggregate | Kind::SortRows
            )
        {
            // Use the complete shared partitioned kernels. Re-lowering these
            // declarations could change transformed grouping, HAVING or ties.
            super::PartitionedNativeOperation::new(
                &primitive_request,
                policy,
                &source_uris,
                &sources,
                cancellation,
            )?;
            PrimitiveProvider::Partitioned
        } else if sources.len() > 1 {
            PrimitiveProvider::Relational(Box::new(prepare_collection(
                &primitive_request,
                policy,
                session,
                &source_uris,
                &sources,
                cancellation,
            )?))
        } else {
            let source = sources
                .first()
                .ok_or_else(|| failed("native source binding is empty"))?
                .clone();
            prepare_single(&primitive_request, policy, source, cancellation)?
        };
        cancellation.check()?;
        Ok(PreparedNativeOperation {
            resources,
            reusable: binding.mode == "single_file",
            kind: PreparedKind::Primitive(Box::new(PreparedPrimitive {
                session: session.clone(),
                provider,
                shared: Arc::new(PrimitiveMetadata {
                    request: primitive_request,
                    argument,
                    binding,
                    sources,
                    source_uris,
                    preparation_fields,
                    policy,
                    _metadata: metadata,
                }),
            })),
        })
    }
}

fn validate_route(request: &workflow::PublicWorkflowRouteRequest) -> Result<()> {
    if request.materialization_policy == "zero_decode"
        && workflow::normalized_vortex_primitive(request)
            .is_some_and(workflow::native_vortex_primitive_materializes)
    {
        return Err(failed(
            "native row computation requires admitted materialization; use --materialization-policy bounded",
        ));
    }
    if let Some(primitive) = workflow::normalized_vortex_primitive(request)
        && let Some(plan) = workflow::native_vortex_primitive_payload_blocker(request, primitive)
    {
        return Err(failed(plan.blocker_reason));
    }
    let plan = workflow::plan_public_workflow_route(request);
    if plan.status != workflow::CommandStatus::Success {
        let message = plan
            .diagnostics
            .first()
            .map_or(plan.blocker_reason, |diagnostic| {
                diagnostic.message.as_str()
            });
        return Err(failed(message));
    }
    Ok(())
}

fn prepare_single(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: PreparedVortexSource,
    cancellation: &CancellationToken,
) -> Result<PrimitiveProvider> {
    cancellation.check()?;
    Ok(match request.kind {
        Kind::CountWhere => PrimitiveProvider::CountWhere(Box::new(
            prepared_count::prepare_count_where_from_source_controlled(
                request,
                policy,
                source,
                cancellation,
            )?,
        )),
        Kind::SimpleAggregate => PrimitiveProvider::Aggregate(Box::new(
            prepared_aggregate::prepare_aggregate_from_source(request, policy, source)?,
        )),
        Kind::SortRows => PrimitiveProvider::Sort,
        Kind::ProjectColumns | Kind::FilterPredicate | Kind::FilterAndProject => {
            PrimitiveProvider::Relational(Box::new(
                prepared_dispatch::prepare_projection_from_source(
                    request,
                    policy,
                    source,
                    cancellation,
                )?,
            ))
        }
        Kind::ExpressionProjectRows if request.structured_projection.is_some() => {
            PrimitiveProvider::Relational(Box::new(
                prepared_dispatch::prepare_projection_from_source(
                    request,
                    policy,
                    source,
                    cancellation,
                )?,
            ))
        }
        _ => PrimitiveProvider::Unary(Box::new(prepared_unary::prepare_unary_from_source(
            request, policy, source,
        )?)),
    })
}

fn prepare_collection(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    uris: &[DatasetUri],
    sources: &[PreparedVortexSource],
    cancellation: &CancellationToken,
) -> Result<PreparedVortexRelational> {
    prepared_relational::prepare_relational_with_schema_in_session_controlled(
        policy,
        session,
        |schemas| {
            for (uri, source) in uris.iter().zip(sources) {
                schemas.register_prepared_source(uri, source.clone())?;
            }
            let scan_projection = if matches!(
                request.kind,
                Kind::ProjectColumns | Kind::FilterPredicate | Kind::FilterAndProject
            ) {
                request.projection.clone()
            } else {
                shardloom_plan::ProjectionRequest::All
            };
            let scans = uris
                .iter()
                .map(|uri| {
                    VortexRelationalPlan::Scan(VortexRelationalScan {
                        source_uri: uri.clone(),
                        projection: scan_projection.clone(),
                        predicate: request.predicate.clone(),
                    })
                })
                .collect::<Vec<_>>();
            let input = union_all(scans)?;
            match request.kind {
                Kind::CountWhere => Ok(VortexRelationalPlan::Aggregate(Box::new(
                    VortexRelationalAggregate {
                        input,
                        group_by: Vec::new(),
                        measures: vec![shardloom_vortex::VortexSimpleAggregateMeasure::new(
                            "count",
                            None,
                            "count_all".into(),
                        )],
                    },
                ))),
                Kind::ProjectColumns | Kind::FilterPredicate | Kind::FilterAndProject => {
                    Ok(match request.source_order_limit {
                        None => input,
                        Some(count) => {
                            VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                                input,
                                offset: 0,
                                count,
                            }))
                        }
                    })
                }
                Kind::ExpressionProjectRows if request.structured_projection.is_some() => {
                    prepared_dispatch::structured_projection_plan(request, input)
                }
                _ => {
                    let mut operation = request.clone();
                    operation.source_uri = None;
                    operation.predicate = None; // Applied exactly once to the part scans above.
                    Ok(VortexRelationalPlan::Unary(Box::new(
                        VortexRelationalUnary {
                            input,
                            request: operation,
                        },
                    )))
                }
            }
        },
        cancellation,
    )
}

fn union_all(mut plans: Vec<VortexRelationalPlan>) -> Result<VortexRelationalPlan> {
    if plans.is_empty() {
        return Err(failed("native source collection is empty"));
    }
    // Preserve declared source order with a balanced tree; file count must not
    // become an accidental recursive-depth ceiling.
    while plans.len() > 1 {
        let mut joined = Vec::with_capacity(plans.len().div_ceil(2));
        let mut iter = plans.into_iter();
        while let Some(left) = iter.next() {
            joined.push(if let Some(right) = iter.next() {
                VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
                    left,
                    right,
                    kind: VortexRelationalSetKind::UnionAll,
                }))
            } else {
                left
            });
        }
        plans = joined;
    }
    plans
        .pop()
        .ok_or_else(|| failed("native source collection is empty"))
}
