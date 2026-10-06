//! Explicit bounded JSON result sink over resident, owned native arrays.

use super::{
    Result, ShardLoomError, VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveKind,
    VortexQueryPrimitiveRequest, bind_vortex_scan_expr, local_vortex_path,
    logical_field_from_native_array, row_export_scan_plan,
};
use crate::resident_session::{
    OwnedVortexResultBatch, PreparedVortexCount, PreparedVortexProjection, PreparedVortexSource,
    ResidentSessionSnapshot, ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool, MemoryLease};
use std::io::Write as _;
use vortex::array::{ArrayRef, ExecutionCtx, dtype::DType};

const MAX_COLLECT_ROWS: u64 = 65_536;
const MAX_JSON_BYTES: usize = 8 * 1024 * 1024;

/// Prepare the exact native footer count using the same local source admission
/// as bounded row collection. Each execution validates the source generation.
///
/// # Errors
/// Rejects non-count requests, additional operation payloads, diagnostic errors,
/// and nonlocal/native sources. Payload admission precedes opening the source.
pub fn prepare_count_in_session(
    request: &VortexQueryPrimitiveRequest,
    session: &ResidentVortexSession,
) -> Result<PreparedVortexCount> {
    if request.kind != VortexQueryPrimitiveKind::CountAll
        || request.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                shardloom_core::DiagnosticSeverity::Error
                    | shardloom_core::DiagnosticSeverity::Fatal
            ) || diagnostic.fallback.attempted
        })
    {
        return Err(collect_error(
            "only admitted exact count-all requests may use native footer count",
        ));
    }
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| collect_error("source URI is required"))?;
    let mut canonical = VortexQueryPrimitiveRequest::count_all(uri.clone());
    canonical.diagnostics.clone_from(&request.diagnostics);
    if request != &canonical {
        return Err(collect_error(
            "native footer count does not admit predicate, projection, limit, or other operation payloads",
        ));
    }
    let path = local_vortex_path(uri, request.kind)?
        .ok_or_else(|| collect_error("local Vortex source is required"))?;
    Ok(session.prepare_file(path)?.prepare_count())
}

pub struct CollectedVortexRows {
    pub rows: u64,
    pub projected_columns: Vec<String>,
    pub source_order_limit: Option<usize>,
    pub values_json: Budgeted<String>,
    /// Ordered native result dtype, serialized by the pinned Vortex provider.
    pub result_schema_json: Budgeted<String>,
    pub runtime: ResidentSessionSnapshot,
    pub native_io_certificate: shardloom_core::NativeIoCertificate,
}

/// The exact scalar count rendered at a requested collection boundary.
pub struct CollectedCountRows {
    pub result_jsonl: Budgeted<String>,
    pub result_schema_json: Budgeted<String>,
}

/// One explicitly materialized result batch. Both strings retain the native
/// session's memory credits independently of the producer's lifetime.
pub struct SerializedVortexResultBatch {
    pub rows: usize,
    pub values_json: Budgeted<String>,
    pub result_schema_json: Budgeted<String>,
}

/// Serialize an already computed count without reading or decoding input rows.
/// The ordered native schema matches the SQL binder's unaliased COUNT(*) name.
/// # Errors
/// Rejects insufficient capacity for the bounded scalar and schema payloads.
pub fn render_count_rows(
    count: u64,
    session: &ResidentVortexSession,
) -> Result<CollectedCountRows> {
    use vortex::array::dtype::{Nullability, PType};
    let memory = session.memory();
    let dtype = DType::struct_(
        [(
            "count_all",
            DType::Primitive(PType::U64, Nullability::NonNullable),
        )],
        Nullability::NonNullable,
    );
    let result_schema_json = serialize_result_schema(&dtype, memory)?;
    let mut output = BoundedJson::new(memory, 64)?;
    writeln!(&mut output, "{{\"count_all\":{count}}}").map_err(collect_io_error)?;
    let text = String::from_utf8(output.bytes).map_err(collect_io_error)?;
    Ok(CollectedCountRows {
        result_jsonl: Budgeted::new(text, output.lease),
        result_schema_json,
    })
}

/// Complete the requested local projection, retaining arrays until JSON rendering.
///
/// # Errors
/// Rejects unsupported residual predicates, source mutation, unsupported result
/// types, and results exceeding 65,536 rows or 8 MiB. Never returns a preview as
/// a completed collect; use an explicit streaming export for larger results.
pub fn collect_rows(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<CollectedVortexRows> {
    let session = ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )?;
    prepare_rows_in_session(request, &session)?.execute()
}

/// A source-generation-bound projection/filter and its explicit bounded JSON sink.
/// Repeated calls reuse source metadata, workers, and bound expressions, never answers.
pub struct PreparedVortexCollect {
    request: VortexQueryPrimitiveRequest,
    session: ResidentVortexSession,
    source: PreparedVortexSource,
    projection: PreparedVortexProjection,
    projected_columns: Vec<String>,
    filtered: bool,
}

/// Bind a local collect to a caller-owned resident session. Keep the returned
/// handle across calls to reuse the opened reader and prepared expressions.
///
/// # Errors
/// Rejects unsupported requests, residual predicates, source generations, and
/// projection schemas without invoking another execution engine.
pub fn prepare_rows_in_session(
    request: &VortexQueryPrimitiveRequest,
    session: &ResidentVortexSession,
) -> Result<PreparedVortexCollect> {
    prepare_rows(request, session, None)
}

/// Bind a projection to a source already admitted by native strategy selection.
/// # Errors
/// Rejects changed/mismatched sources and unsupported projection semantics.
pub fn prepare_rows_from_source(
    request: &VortexQueryPrimitiveRequest,
    source: PreparedVortexSource,
) -> Result<PreparedVortexCollect> {
    let session = super::prepared_dispatch::source_session(&source, request, None)?;
    prepare_rows(request, &session, Some(source))
}

fn prepare_rows(
    request: &VortexQueryPrimitiveRequest,
    session: &ResidentVortexSession,
    prepared: Option<PreparedVortexSource>,
) -> Result<PreparedVortexCollect> {
    if request.diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.severity,
            shardloom_core::DiagnosticSeverity::Error | shardloom_core::DiagnosticSeverity::Fatal
        ) || diagnostic.fallback.attempted
    }) {
        return Err(collect_error(
            "request diagnostics prohibit native execution",
        ));
    }
    if !matches!(
        request.kind,
        VortexQueryPrimitiveKind::ProjectColumns
            | VortexQueryPrimitiveKind::FilterAndProject
            | VortexQueryPrimitiveKind::FilterPredicate
    ) {
        return Err(collect_error("operation is not an admitted projection"));
    }
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| collect_error("source URI is required"))?;
    let path = local_vortex_path(uri, request.kind)?
        .ok_or_else(|| collect_error("local Vortex source is required"))?;
    let source = if let Some(source) = prepared {
        source
    } else {
        session.prepare_file(path)?
    };
    let plan = row_export_scan_plan(request, source.dtype())?;
    if plan.residual_predicate.is_some() {
        return Err(collect_error(
            "resident collect does not yet admit this residual predicate; use an explicit native row export",
        ));
    }
    let names = plan
        .projected_columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let rows_limit = request
        .source_order_limit
        .map_or(MAX_COLLECT_ROWS + 1, |limit| {
            u64::try_from(limit)
                .unwrap_or(u64::MAX)
                .min(MAX_COLLECT_ROWS + 1)
        });
    let filter = plan
        .filter
        .as_ref()
        .map(|filter| bind_vortex_scan_expr(source.file(), filter))
        .transpose()?;
    let projection = source
        .prepare_projection(&names, rows_limit, 32 * 1024 * 1024)?
        .with_filter(filter);
    Ok(PreparedVortexCollect {
        request: request.clone(),
        session: session.clone(),
        source,
        projection,
        projected_columns: plan.projected_columns,
        filtered: plan.filter.is_some(),
    })
}

impl PreparedVortexCollect {
    /// Validate file admission against the generation held by this projection.
    /// This does not collect rows or open another provider.
    ///
    /// # Errors
    /// Returns the source's metadata mismatch or generation validation error.
    pub fn validate_file_metadata(&self, expected: &std::fs::Metadata) -> Result<()> {
        self.source.validate_file_metadata(expected)
    }

    /// Execute the prepared projection/filter into owned native arrays without
    /// row rendering. The complete bounded result may outlive this handle.
    ///
    /// # Errors
    /// Rejects changed source generations and row/byte/memory bound violations.
    pub fn execute_arrays(&self) -> Result<OwnedVortexResultBatch> {
        let result = self.projection.execute()?;
        if result.row_count() > MAX_COLLECT_ROWS {
            return Err(collect_error(
                "collect exceeds 65,536 rows; use an explicit streaming export",
            ));
        }
        Ok(result)
    }

    /// Execute the prepared native scan and complete its bounded JSON sink.
    /// Output capacity is reserved as it grows and remains owned by the result.
    ///
    /// # Errors
    /// Rejects mutation, unsupported JSON types, and complete output bounds.
    pub fn execute(&self) -> Result<CollectedVortexRows> {
        let result = self.execute_arrays()?;
        self.complete_json(result)
    }

    #[cfg(test)]
    pub(crate) fn execute_with_after_scan(
        &self,
        after_scan: impl FnOnce(),
    ) -> Result<CollectedVortexRows> {
        let result = self.execute_arrays()?;
        after_scan();
        self.complete_json(result)
    }

    fn complete_json(&self, result: OwnedVortexResultBatch) -> Result<CollectedVortexRows> {
        let values_json = result.render_admitted_json(&self.projected_columns, MAX_JSON_BYTES)?;
        let result_schema_json = serialize_result_schema(result.dtype(), self.session.memory())?;
        self.source.validate_generation()?;
        let native_io_certificate = certificate(&self.request, result.row_count(), self.filtered)?;
        let rows = result.row_count();
        drop(result);
        Ok(CollectedVortexRows {
            rows,
            projected_columns: self.projected_columns.clone(),
            source_order_limit: self.request.source_order_limit,
            values_json,
            result_schema_json,
            runtime: self.session.snapshot(),
            native_io_certificate,
        })
    }
}

fn certificate(
    request: &VortexQueryPrimitiveRequest,
    rows: u64,
    filtered: bool,
) -> Result<shardloom_core::NativeIoCertificate> {
    certificate_for_origin(
        request.kind,
        request.diagnostics.clone(),
        rows,
        filtered,
        false,
    )
}

pub(crate) fn memory_certificate(
    rows: u64,
    filtered: bool,
) -> Result<shardloom_core::NativeIoCertificate> {
    let kind = if filtered {
        VortexQueryPrimitiveKind::FilterAndProject
    } else {
        VortexQueryPrimitiveKind::ProjectColumns
    };
    certificate_for_origin(kind, Vec::new(), rows, filtered, true)
}

fn certificate_for_origin(
    kind: VortexQueryPrimitiveKind,
    diagnostics: Vec<shardloom_core::Diagnostic>,
    rows: u64,
    filtered: bool,
    in_memory: bool,
) -> Result<shardloom_core::NativeIoCertificate> {
    use shardloom_core::{
        NativeIoCertificate, NativeIoMaterializationBoundaryReport,
        NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSourceCapabilityReport,
        NativeIoSourcePushdownReport, RepresentationState,
    };
    let mut accepted = vec!["projection".to_string()];
    if filtered {
        accepted.push("exact_filter".to_string());
    }
    let (source_kind, adapter_id, schema_status, statistics, proof, representation) = if in_memory {
        (
            "typed_memory_batch",
            "shardloom.resident_vortex.memory.v1",
            "validated_immutable_typed_snapshot",
            "exact_snapshot_row_count",
            "bound native array expressions over owned immutable typed buffers",
            RepresentationState::DecodedColumnar,
        )
    } else {
        (
            "local_vortex_file",
            "shardloom.resident_vortex.v1",
            "opened_validated_generation",
            "native_footer",
            "bound scan expressions; source generation checked before and after execution",
            RepresentationState::VortexEncoded,
        )
    };
    NativeIoCertificate::new(
        format!("resident.{}.bounded_collect.native_io", kind.as_str()),
        "resident_vortex_owned_arrays_to_bounded_json",
        NativeIoSourceCapabilityReport {
            source_kind: source_kind.into(), adapter_id: adapter_id.into(),
            schema_discovery_status: schema_status.into(),
            statistics_availability: statistics.into(),
            pushdown_capabilities: "projection,exact_filter".into(),
            encoded_representation_preserved: !in_memory, range_read_capability: !in_memory,
            streaming_capability: !in_memory, object_store_capability: false, fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: accepted, rejected_operations: Vec::new(),
            guarantee: "exact_provider_filter_then_ordered_result_limit".into(),
            proof_basis: format!("vortex {} {proof}", crate::UPSTREAM_VORTEX_PROVIDER_VERSION),
            residual_expression: None, conservative_false_positive_policy: false,
            unsafe_rejected_reason: None, fallback_attempted: false,
        },
        vec![NativeIoRepresentationTransition::new(representation, RepresentationState::MaterializedRows, true)],
        json_sink_requirements(),
        json_sink_fidelity(),
        vec![NativeIoMaterializationBoundaryReport {
            boundary_id: "resident_collect_json_sink".into(), from_state: representation,
            to_state: RepresentationState::MaterializedRows, required_by: "explicit_json_collect".into(),
            reason: "native scalar evaluation at requested sink; decoded byte volume is not instrumented".into(),
            bytes_decoded: 0, rows_materialized: rows,
            fidelity_loss: "physical dtype, encoding and statistics are not JSON values".into(), fallback_attempted: false,
        }],
        NativeIoSideEffectReport {
            data_read: true, data_decoded: rows > 0, data_materialized: rows > 0, row_read: rows > 0,
            arrow_converted: false, object_store_io: false, write_io: false, spill_io_performed: false,
            external_effects_executed: false, fallback_attempted: false, fallback_execution_allowed: false,
        },
        diagnostics,
    )
}

fn json_sink_requirements() -> shardloom_core::NativeIoSinkRequirementReport {
    shardloom_core::NativeIoSinkRequirementReport {
        target_format: "bounded_json_rows".into(),
        accepts_encoded: false,
        requires_decoded_columnar: false,
        requires_rows: true,
        preserves_metadata: false,
        requires_ordering: true,
        requires_partitioning: false,
        requires_commit: false,
        supports_streaming: false,
        max_chunk_size: Some(MAX_JSON_BYTES as u64),
        backpressure_policy:
            "65536_row_and_8mib_complete_result_bounds_with_owned_buffer_reservations".into(),
    }
}

fn json_sink_fidelity() -> shardloom_core::NativeIoAdapterFidelityReport {
    shardloom_core::NativeIoAdapterFidelityReport {
        adapter_id: "shardloom.resident_vortex.json_sink.v1".into(),
        source_kind: "vortex".into(),
        sink_kind: "json".into(),
        metadata_preserved: false,
        statistics_preserved: false,
        encoded_representation_preserved: false,
        materialization_required: true,
        fidelity_loss: "JSON preserves admitted values with binary hexadecimal text, decimal128(precision,scale):unscaled_integer text, Date32 day integers and timezone-free timestamp microsecond integers; logical dtype and physical encoding are not persisted"
            .into(),
        metadata_loss: "Vortex physical encodings, statistics and metadata not exported".into(),
        fallback_attempted: false,
    }
}

pub(crate) fn render_owned_json(
    result: &OwnedVortexResultBatch,
    names: &[String],
    memory: &LiveMemoryPool,
    max_bytes: usize,
) -> Result<Budgeted<String>> {
    if result.row_count() > MAX_COLLECT_ROWS || max_bytes == 0 {
        return Err(collect_error(
            "native JSON sink requires positive byte bounds and at most 65,536 rows",
        ));
    }
    let mut output = JsonRows::new(memory, max_bytes.min(MAX_JSON_BYTES), false)?;
    let mut context = result.create_execution_ctx();
    for array in result.arrays() {
        output.append(array, names, &mut context, &CancellationToken::default())?;
    }
    output.finish()
}

/// A terminal bounded text sink. Native batches are consumed synchronously;
/// neither a second row table nor a JSON value tree is constructed.
pub(super) struct JsonRows {
    output: BoundedJson,
    memory: LiveMemoryPool,
    rows: u64,
    lines: bool,
    schema: Option<(DType, MemoryLease)>,
}

impl JsonRows {
    pub(super) fn append_native(
        &mut self,
        array: &ArrayRef,
        context: &crate::resident_session::NativeExecutionContext<'_>,
    ) -> Result<()> {
        use vortex::array::VortexSessionExecute as _;
        if let Some((dtype, _)) = &self.schema {
            if dtype != array.dtype() {
                return Err(collect_error(
                    "native result schema changed between batches",
                ));
            }
        } else {
            let lease = self
                .memory
                .reserve(schema_scratch_bytes(array.dtype(), 0)?)?;
            self.schema = Some((array.dtype().clone(), lease));
        }
        let fields = array
            .dtype()
            .as_struct_fields_opt()
            .ok_or_else(|| collect_error("result requires a struct schema"))?;
        self.append(
            array,
            fields.names().as_ref(),
            &mut context.native_session().create_execution_ctx(),
            context.cancellation(),
        )
    }

    pub(super) fn new(memory: &LiveMemoryPool, max_bytes: usize, lines: bool) -> Result<Self> {
        if max_bytes == 0 || max_bytes > MAX_JSON_BYTES {
            return Err(collect_error("JSON collection requires a 1..=8 MiB bound"));
        }
        let mut output = BoundedJson::new(memory, max_bytes)?;
        if !lines {
            output.write_all(b"[").map_err(collect_io_error)?;
        }
        Ok(Self {
            output,
            memory: memory.clone(),
            rows: 0,
            lines,
            schema: None,
        })
    }

    pub(super) fn append(
        &mut self,
        array: &ArrayRef,
        names: &[impl AsRef<str>],
        context: &mut ExecutionCtx,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        let next_rows = self
            .rows
            .checked_add(array.len() as u64)
            .ok_or_else(|| collect_error("collection row overflow"))?;
        if next_rows > MAX_COLLECT_ROWS {
            return Err(collect_error(
                "collect exceeds 65,536 rows; use an explicit streaming export",
            ));
        }
        let bytes = names
            .len()
            .checked_mul(std::mem::size_of::<ArrayRef>() + 512)
            .ok_or_else(|| collect_error("JSON field metadata overflow"))?;
        let _metadata = self.memory.reserve(bytes as u64)?;
        // Logical fields need not be physical encoding child slots. Resolve
        // each requested field through the provider, including struct validity,
        // only at this explicit JSON materialization boundary. Preserve Variant
        // tags directly; the provider's general Columnar builder is not required
        // to support dynamic variants for this scalar serialization boundary.
        let children = names
            .iter()
            .map(|column| {
                let field = logical_field_from_native_array(array, column.as_ref())?;
                super::native_json::Column::new(&field, context, &self.memory)
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..array.len() {
            cancellation.check()?;
            if !self.lines && self.rows != 0 {
                self.output.write_all(b",").map_err(collect_io_error)?;
            }
            self.output.write_all(b"{").map_err(collect_io_error)?;
            for (index, column) in names.iter().enumerate() {
                if index != 0 {
                    self.output.write_all(b",").map_err(collect_io_error)?;
                }
                serde_json::to_writer(&mut self.output, column.as_ref())
                    .map_err(collect_io_error)?;
                self.output.write_all(b":").map_err(collect_io_error)?;
                let child = &children[index];
                child.write(row, &mut self.output, context, cancellation)?;
            }
            self.output
                .write_all(if self.lines { b"}\n" } else { b"}" })
                .map_err(collect_io_error)?;
            self.rows += 1;
        }
        Ok(())
    }

    /// Preserve execution/source evidence and record the actual terminal sink.
    pub(super) fn finish_certified(
        self,
        certificate: &mut shardloom_core::NativeIoCertificate,
    ) -> Result<(Budgeted<String>, Budgeted<String>)> {
        if !certificate.is_certified() {
            return Err(collect_error(
                "JSON collection requires certified native execution",
            ));
        }
        let rows = self.rows;
        let dtype = &self
            .schema
            .as_ref()
            .ok_or_else(|| {
                collect_error("native collection did not deliver its typed result schema")
            })?
            .0;
        let schema = serialize_result_schema(dtype, &self.memory)?;
        let output = self.finish()?;
        certify_json_delivery(certificate, rows, false)?;
        Ok((output, schema))
    }

    pub(super) fn finish(mut self) -> Result<Budgeted<String>> {
        if !self.lines {
            self.output.write_all(b"]").map_err(collect_io_error)?;
        }
        let text = String::from_utf8(self.output.bytes).map_err(collect_io_error)?;
        Ok(Budgeted::new(text, self.output.lease))
    }
}

pub(super) fn certify_json_delivery(
    certificate: &mut shardloom_core::NativeIoCertificate,
    rows: u64,
    streamed: bool,
) -> Result<()> {
    use shardloom_core::{
        NativeIoMaterializationBoundaryReport, NativeIoRepresentationTransition,
        RepresentationState,
    };
    if !certificate.is_certified() {
        return Err(collect_error(
            "JSON delivery requires certified native execution",
        ));
    }
    let from_state = certificate
        .representation_transitions
        .last()
        .map_or(RepresentationState::DecodedColumnar, |transition| {
            transition.to_state
        });
    certificate
        .representation_transitions
        .push(NativeIoRepresentationTransition::new(
            from_state,
            RepresentationState::MaterializedRows,
            true,
        ));
    certificate.sink_requirement_report = json_sink_requirements();
    if streamed {
        certificate.sink_requirement_report.supports_streaming = true;
        certificate.sink_requirement_report.max_chunk_size = Some(2048);
        certificate.sink_requirement_report.backpressure_policy =
            "synchronous_consumer_controls_next_batch".into();
    }
    certificate.adapter_fidelity_report = json_sink_fidelity();
    certificate
        .materialization_boundaries
        .push(NativeIoMaterializationBoundaryReport {
        boundary_id: if streamed {
            "resident_json_batch_sink"
        } else {
            "resident_collect_json_sink"
        }
        .into(),
        from_state,
        to_state: RepresentationState::MaterializedRows,
        required_by: if streamed {
            "explicit_json_batch_consumption"
        } else {
            "explicit_json_collect"
        }
        .into(),
        reason:
            "native scalar evaluation at requested sink; decoded byte volume is not instrumented"
                .into(),
        bytes_decoded: 0,
        rows_materialized: rows,
        fidelity_loss: "physical dtype, encoding and statistics are not JSON values".into(),
        fallback_attempted: false,
    });
    certificate.side_effects.data_decoded |= rows > 0;
    certificate.side_effects.data_materialized |= rows > 0;
    certificate.side_effects.row_read |= rows > 0;
    certificate
        .source_pushdown_report
        .proof_basis
        .push_str(if streamed {
            ";complete_native_batches_to_incremental_json;no_query_replay=true"
        } else {
            ";complete_native_batches_to_bounded_jsonl;no_query_replay=true"
        });
    Ok(())
}

/// Serialize the existing native dtype at the explicit result boundary. The
/// provider's ordered names and recursive types also describe typed empty rows.
pub(crate) fn serialize_result_schema(
    dtype: &DType,
    memory: &LiveMemoryPool,
) -> Result<Budgeted<String>> {
    let _scratch = memory.reserve(schema_scratch_bytes(dtype, 0)?)?;
    let mut output = BoundedJson::new(memory, MAX_JSON_BYTES)?;
    serde_json::to_writer(&mut output, dtype).map_err(collect_io_error)?;
    let text = String::from_utf8(output.bytes).map_err(collect_io_error)?;
    Ok(Budgeted::new(text, output.lease))
}

fn schema_scratch_bytes(dtype: &DType, depth: usize) -> Result<u64> {
    if depth > 64 {
        return Err(collect_error("result schema exceeds 64 nested levels"));
    }
    let mut bytes = 1024_u64;
    let mut add = |child: &DType| -> Result<()> {
        bytes = bytes
            .checked_add(schema_scratch_bytes(child, depth + 1)?)
            .ok_or_else(|| collect_error("result schema metadata overflow"))?;
        Ok(())
    };
    match dtype {
        DType::Struct(fields, _) => {
            for child in fields.fields() {
                add(&child)?;
            }
        }
        DType::Union(fields, _) => {
            for child in fields.variants() {
                add(&child)?;
            }
        }
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => add(child)?,
        DType::Map(map, _) => {
            add(&map.key_dtype())?;
            add(&map.value_dtype())?;
        }
        DType::Extension(extension) => add(extension.storage_dtype())?,
        _ => {}
    }
    if bytes > MAX_JSON_BYTES as u64 {
        return Err(collect_error("result schema scratch exceeds 8 MiB"));
    }
    Ok(bytes)
}

pub(super) fn write_scalar_json(
    writer: &mut impl std::io::Write,
    scalar: &vortex::array::scalar::Scalar,
) -> Result<u64> {
    use shardloom_core::StatValue;
    use vortex::array::scalar::ScalarValue;
    let mut scalar = scalar;
    let mut depth = 0;
    while let Some(ScalarValue::Variant(inner)) = scalar.value() {
        depth += 1;
        if depth > 64 {
            return Err(collect_error("JSON Variant nesting exceeds 64 levels"));
        }
        scalar = inner;
    }
    if matches!(scalar.dtype(), vortex::array::dtype::DType::Extension(_))
        && crate::native_payload_schema::temporal_storage(scalar.dtype()).is_none()
    {
        return Err(collect_error(
            "JSON extension payload requires Date32 or timezone-free TimestampMicros",
        ));
    }
    let mut utf8_bytes = match scalar.value() {
        Some(ScalarValue::Utf8(value)) => super::usize_to_u64(value.as_str().len())?,
        Some(ScalarValue::Binary(value)) => super::usize_to_u64(value.len())?
            .checked_mul(2)
            .ok_or_else(|| collect_error("binary JSON size overflow"))?,
        _ => 0,
    };
    match scalar.value() {
        None => writer.write_all(b"null").map_err(collect_io_error),
        Some(ScalarValue::Bool(value)) => {
            serde_json::to_writer(writer, value).map_err(collect_io_error)
        }
        Some(ScalarValue::Utf8(value)) => {
            serde_json::to_writer(writer, value.as_str()).map_err(collect_io_error)
        }
        Some(ScalarValue::Binary(value)) => {
            writer.write_all(b"\"").map_err(collect_io_error)?;
            for byte in value.iter() {
                write!(writer, "{byte:02x}").map_err(collect_io_error)?;
            }
            writer.write_all(b"\"").map_err(collect_io_error)
        }
        Some(ScalarValue::Decimal(value)) => {
            let vortex::array::dtype::DType::Decimal(dtype, _) = scalar.dtype() else {
                return Err(collect_error("JSON decimal requires a decimal dtype"));
            };
            if !crate::native_payload_schema::admitted_decimal(*dtype) {
                return Err(collect_error(
                    "JSON decimal exceeds admitted precision or scale",
                ));
            }
            let value = value
                .cast::<i128>()
                .ok_or_else(|| collect_error("JSON decimal exceeds signed 128-bit storage"))?;
            let text = format!(
                "decimal128({},{}):{value}",
                dtype.precision(),
                dtype.scale()
            );
            utf8_bytes = super::usize_to_u64(text.len())?;
            serde_json::to_writer(writer, &text).map_err(collect_io_error)
        }
        Some(ScalarValue::Primitive(value)) => match super::vortex_pvalue_to_stat_value(*value) {
            Some(StatValue::UInt64(value)) => {
                serde_json::to_writer(writer, &value).map_err(collect_io_error)
            }
            Some(StatValue::Int64(value)) => {
                serde_json::to_writer(writer, &value).map_err(collect_io_error)
            }
            Some(StatValue::Float64(value)) if value.is_finite() => {
                serde_json::to_writer(writer, &value).map_err(collect_io_error)
            }
            _ => Err(collect_error(
                "JSON numbers require supported finite values",
            )),
        },
        _ => Err(collect_error(
            "result type is not admitted for JSON collect",
        )),
    }?;
    Ok(utf8_bytes)
}

struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
    lease: MemoryLease,
}

impl BoundedJson {
    fn new(pool: &LiveMemoryPool, limit: usize) -> Result<Self> {
        Ok(Self {
            bytes: Vec::new(),
            limit,
            lease: pool.reserve(0)?,
        })
    }

    fn reserve_capacity(&mut self, required: usize) -> std::io::Result<()> {
        if required <= self.bytes.capacity() {
            return Ok(());
        }
        let capacity = required
            .max(self.bytes.capacity().saturating_mul(2))
            .max(256)
            .min(self.limit);
        let previous = self.lease.bytes();
        // Cover old and replacement buffers during reallocation, then retain
        // only the new allocation's credits. Try exact growth under pressure.
        self.lease
            .resize(previous + capacity as u64)
            .or_else(|_| self.lease.resize(previous + required as u64))
            .map_err(std::io::Error::other)?;
        let capacity =
            usize::try_from(self.lease.bytes() - previous).map_err(std::io::Error::other)?;
        if let Err(error) = self.bytes.try_reserve_exact(capacity - self.bytes.len()) {
            self.lease.resize(previous).map_err(std::io::Error::other)?;
            return Err(std::io::Error::other(error));
        }
        self.lease
            .resize(self.bytes.capacity() as u64)
            .map_err(std::io::Error::other)?;
        Ok(())
    }
}

impl std::io::Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other(if self.limit == MAX_JSON_BYTES {
                "collect exceeds 8 MiB; use an explicit streaming export".to_string()
            } else {
                format!("collect exceeds admitted {} byte JSON bound", self.limit)
            }));
        }
        self.reserve_capacity(self.bytes.len() + bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn collect_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; no fallback execution was attempted"))
}

fn collect_io_error(error: impl std::fmt::Display) -> ShardLoomError {
    collect_error(&error.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::vortex_scalar_to_stat_value;
    use super::*;
    use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    use shardloom_plan::ProjectionRequest;

    #[test]
    fn scalar_count_collection_is_exact_typed_and_releases_output_reservations() {
        let session = ResidentVortexSession::new(64 * 1024, 1).unwrap();
        let memory = session.memory();
        for count in [0, 1, u64::MAX] {
            let output = render_count_rows(count, &session).unwrap();
            let rows: serde_json::Value =
                serde_json::from_str(output.result_jsonl.value()).unwrap();
            assert_eq!(rows, serde_json::json!({"count_all": count}));
            let schema: serde_json::Value =
                serde_json::from_str(output.result_schema_json.value()).unwrap();
            assert_eq!(
                schema,
                serde_json::json!({"Struct": [{"names": ["count_all"],
                    "dtypes": [{"Primitive": ["u64", false]}]}, false]})
            );
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(output);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
        assert!(render_count_rows(1, &ResidentVortexSession::new(1, 1).unwrap()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn prepared_count_rejects_noncanonical_payloads_before_source_open() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex");
        let canonical = VortexQueryPrimitiveRequest::count_all(
            DatasetUri::new(path.display().to_string()).unwrap(),
        );
        let mut filtered = canonical.clone();
        filtered.predicate = Some(PredicateExpr::AlwaysFalse);
        let mut projected = canonical.clone();
        projected.projection = ProjectionRequest::columns(vec![ColumnRef::new("value").unwrap()]);
        let mut sorted = canonical.clone();
        sorted.sort_rows = Some(crate::VortexSortRowsRequest::new(Vec::new()));
        let mut structured = canonical.clone();
        structured.structured_projection =
            Some(crate::VortexStructuredProjectionRequest::new(Vec::new()));
        let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
        for request in [
            filtered,
            projected,
            canonical.clone().with_source_order_limit(1),
            canonical.clone().with_sample_seed(7),
            canonical.clone().with_sample_fraction(0.5),
            sorted,
            structured,
        ] {
            let error = prepare_count_in_session(&request, &session)
                .err()
                .expect("noncanonical count payload must be rejected");
            assert!(error.to_string().contains("does not admit"));
            assert_eq!(session.snapshot().prepared_source_opens, 0);
            assert_eq!(session.snapshot().completed_executions, 0);
            assert_eq!(session.snapshot().memory.peak_reserved_bytes, 0);
        }
        let prepared = prepare_count_in_session(&canonical, &session).unwrap();
        assert_eq!(prepared.execute().unwrap(), 5);
        assert_eq!(session.snapshot().prepared_source_opens, 1);
    }

    #[cfg(unix)]
    #[test]
    fn json_sink_uses_logical_aliased_fields_from_chunked_native_structs() {
        use crate::resident_memory_source::{
            MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
        };
        use vortex::array::arrays::ChunkedArray;
        let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
        let source = ResidentMemorySource::from_columns(
            &session,
            &[
                MemoryColumn {
                    name: "renamed_label",
                    values: MemoryColumnValues::Utf8(&[Some("港-λ"), None]),
                },
                MemoryColumn {
                    name: "shipment_sequence",
                    values: MemoryColumnValues::Int64(&[Some(i64::MIN), Some(i64::MAX)]),
                },
            ],
            MemorySourceBounds::default(),
        )
        .unwrap();
        let native = source
            .prepare_projection(&["renamed_label", "shipment_sequence"], None, None)
            .unwrap()
            .execute_arrays()
            .unwrap();
        let array = native.arrays()[0].clone();
        let dtype = array.dtype().clone();
        let chunked = session
            .execute_owned_array(4, 1024 * 1024, |_| {
                ChunkedArray::try_new(vec![array.clone(), array], dtype)
                    .map(vortex::array::IntoArray::into_array)
                    .map_err(collect_io_error)
            })
            .unwrap();
        assert_eq!(chunked.arrays()[0].encoding_id().as_ref(), "vortex.chunked");
        let names = vec!["shipment_sequence".to_string(), "renamed_label".to_string()];
        let json = render_owned_json(&chunked, &names, session.memory(), 4096).unwrap();
        let actual: serde_json::Value = serde_json::from_str(json.value()).unwrap();
        let pair = [
            serde_json::json!({"shipment_sequence":i64::MIN,"renamed_label":"港-λ"}),
            serde_json::json!({"shipment_sequence":i64::MAX,"renamed_label":null}),
        ];
        assert_eq!(
            actual,
            serde_json::json!([pair[0], pair[1], pair[0], pair[1]])
        );
        let complete =
            super::super::row_export_columns_from_chunk(&chunked.arrays()[0], &names).unwrap();
        assert_eq!(
            complete,
            vec![
                vec![
                    StatValue::Int64(i64::MIN),
                    StatValue::Int64(i64::MAX),
                    StatValue::Int64(i64::MIN),
                    StatValue::Int64(i64::MAX)
                ],
                vec![
                    StatValue::Utf8("港-λ".into()),
                    StatValue::Null,
                    StatValue::Utf8("港-λ".into()),
                    StatValue::Null
                ],
            ]
        );
        let selected = super::super::row_export_selected_columns_from_chunk(
            &chunked.arrays()[0],
            &names,
            &[3, 0],
        )
        .unwrap();
        assert_eq!(
            selected,
            vec![
                vec![StatValue::Int64(i64::MAX), StatValue::Int64(i64::MIN)],
                vec![StatValue::Null, StatValue::Utf8("港-λ".into())],
            ]
        );
        // JSON materialization must not replace the caller's original array.
        assert_eq!(chunked.arrays()[0].encoding_id().as_ref(), "vortex.chunked");
    }

    #[test]
    fn filtered_collect_returns_values_not_only_a_projection_descriptor() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            DatasetUri::new(path.display().to_string()).unwrap(),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").unwrap()]),
        );
        let result = collect_rows(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        assert_eq!(result.rows, 3);
        let values: serde_json::Value = serde_json::from_str(result.values_json.value()).unwrap();
        assert_eq!(
            values,
            serde_json::json!([{"metric": 30}, {"metric": 40}, {"metric": 50}])
        );
        assert_eq!(result.runtime.prepared_source_opens, 1);
        assert_eq!(result.runtime.completed_executions, 1);
    }

    #[test]
    fn json_sink_refuses_oversize_values_without_partial_success() {
        let pool = LiveMemoryPool::new(4).unwrap();
        let mut sink = BoundedJson::new(&pool, 4).unwrap();
        sink.write_all(b"1234").unwrap();
        assert!(sink.write_all(b"5").is_err());
        assert_eq!(sink.bytes, b"1234");
        assert_eq!(pool.snapshot().reserved_bytes, 4);
        drop(sink);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn json_sink_grows_under_shared_pressure_and_failed_growth_keeps_owned_bytes() {
        let pool = LiveMemoryPool::new(300).unwrap();
        let held = pool.reserve(280).unwrap();
        let mut sink = BoundedJson::new(&pool, 1024).unwrap();
        sink.write_all(b"1234567890").unwrap();
        assert_eq!(sink.lease.bytes(), 10);
        assert!(sink.write_all(b"01234567890").is_err());
        assert_eq!(sink.bytes, b"1234567890");
        assert_eq!(pool.snapshot().reserved_bytes, 290);
        drop(held);
        sink.write_all(b"01234567890").unwrap();
        assert_eq!(sink.bytes, b"123456789001234567890");
        drop(sink);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    fn filtered_request(path: &std::path::Path) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::filter_and_project(
            DatasetUri::new(path.display().to_string()).unwrap(),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").unwrap()]),
        )
        .with_source_order_limit(2)
    }

    #[test]
    fn repeated_prepared_collect_reuses_binding_and_returns_complete_values_under_small_budget() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex");
        let session = ResidentVortexSession::new(4 * 1024 * 1024, 2).unwrap();
        let memory = session.memory().clone();
        let prepared = prepare_rows_in_session(&filtered_request(&path), &session).unwrap();
        for execution in 1..=5 {
            let result = prepared.execute().unwrap();
            assert_eq!(result.rows, 2);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(result.values_json.value()).unwrap(),
                serde_json::json!([{"metric": 30}, {"metric": 40}]),
            );
            assert_eq!(result.runtime.prepared_source_opens, 1);
            assert_eq!(result.runtime.completed_executions, execution);
            assert!(result.values_json.reserved_bytes() < 1024);
        }
        let arrays = prepared.execute_arrays().unwrap();
        assert_eq!(arrays.row_count(), 2);
        assert_eq!(session.snapshot().completed_executions, 6);
        drop(prepared);
        drop(session);
        let mut context = arrays.create_execution_ctx();
        let values = arrays
            .arrays()
            .iter()
            .flat_map(|array| {
                let child = array
                    .named_children()
                    .into_iter()
                    .find(|(name, _)| name.as_str() == "metric")
                    .unwrap()
                    .1;
                (0..array.len())
                    .map(|row| {
                        vortex_scalar_to_stat_value(
                            &child.execute_scalar(row, &mut context).unwrap(),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![StatValue::Int64(30), StatValue::Int64(40)]);
        drop(context);
        drop(arrays);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn collected_json_retains_its_reservation_after_prepared_session_drop() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex");
        let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
        let memory = session.memory().clone();
        let prepared = prepare_rows_in_session(&filtered_request(&path), &session).unwrap();
        let result = prepared.execute().unwrap();
        drop(prepared);
        drop(session);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            result.values_json.reserved_bytes() + result.result_schema_json.reserved_bytes()
        );
        assert_eq!(
            result.values_json.value(),
            "[{\"metric\":30},{\"metric\":40}]"
        );
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[cfg(unix)]
    #[test]
    fn native_result_schema_preserves_order_nullability_and_empty_output_ownership() {
        use crate::resident_memory_source::{
            MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
        };
        for count in [0, 1] {
            let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
            let memory = session.memory().clone();
            let source = ResidentMemorySource::from_columns(
                &session,
                &[
                    MemoryColumn {
                        name: "n",
                        values: MemoryColumnValues::Int64(&[None][..count]),
                    },
                    MemoryColumn {
                        name: "text,λ",
                        values: MemoryColumnValues::Utf8(&[None][..count]),
                    },
                ],
                MemorySourceBounds::default(),
            )
            .unwrap();
            let prepared = source
                .prepare_projection(&["text,λ", "n"], None, None)
                .unwrap();
            let result = prepared.execute().unwrap();
            assert_eq!(result.rows, count as u64);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(result.result_schema_json.value())
                    .unwrap(),
                serde_json::json!({"Struct":[{"names":["text,λ","n"],
                    "dtypes":[{"Utf8":true},{"Primitive":["i64",true]}]},false]})
            );
            drop(prepared);
            drop(source);
            drop(session);
            assert_eq!(
                memory.snapshot().reserved_bytes,
                result.values_json.reserved_bytes() + result.result_schema_json.reserved_bytes()
            );
            drop(result);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }

    #[cfg(unix)]
    #[test]
    fn native_result_sink_rejects_schema_drift_and_keeps_empty_batch_metadata() {
        use vortex::array::{
            IntoArray as _,
            arrays::{PrimitiveArray, StructArray},
            dtype::FieldNames,
            validity::Validity,
        };
        let array = |name: &str| {
            StructArray::try_new(
                FieldNames::from([name]),
                vec![PrimitiveArray::from_option_iter([None::<i64>; 0]).into_array()],
                0,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        };
        let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
        let memory = session.memory().clone();
        let mut sink = JsonRows::new(&memory, 4096, true).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                sink.append_native(&array("n,λ"), context)?;
                let error = sink.append_native(&array("changed"), context).unwrap_err();
                assert!(error.to_string().contains("schema changed between batches"));
                Ok(())
            })
            .unwrap();
        let mut certificate = memory_certificate(0, false).unwrap();
        let (values, schema) = sink.finish_certified(&mut certificate).unwrap();
        assert_eq!(values.value(), "");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(schema.value()).unwrap(),
            serde_json::json!({"Struct":[{"names":["n,λ"],"dtypes":[{"Primitive":["i64",true]}]},false]})
        );
        drop(session);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            values.reserved_bytes() + schema.reserved_bytes()
        );
        drop((values, schema));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn prepared_collect_rejects_replacement_and_truncation_before_returning_cached_values() {
        static NEXT_INPUT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let original = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex");
        let directory = std::env::temp_dir().join(format!(
            "shardloom-prepared-collect-{}-{}",
            std::process::id(),
            NEXT_INPUT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        for change in ["replace", "truncate"] {
            let path = directory.join(format!("{change}.vortex"));
            std::fs::copy(&original, &path).unwrap();
            let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
            let prepared = prepare_rows_in_session(&filtered_request(&path), &session).unwrap();
            assert_eq!(prepared.execute().unwrap().rows, 2);
            if change == "replace" {
                let replacement = directory.join("replacement.vortex");
                std::fs::copy(&original, &replacement).unwrap();
                std::fs::rename(replacement, &path).unwrap();
            } else {
                std::fs::File::options()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(8)
                    .unwrap();
            }
            assert!(prepared.execute().is_err(), "{change}");
            assert!(prepared.execute_arrays().is_err(), "{change}");
            assert_eq!(session.snapshot().completed_executions, 1);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn filtered_limit_and_empty_selection_return_complete_source_order_values() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex");
        for (threshold, expected) in [
            (3, serde_json::json!([{"metric": 30}, {"metric": 40}])),
            (99, serde_json::json!([])),
        ] {
            let request = VortexQueryPrimitiveRequest::filter_and_project(
                DatasetUri::new(path.display().to_string()).unwrap(),
                PredicateExpr::Compare {
                    column: ColumnRef::new("value").unwrap(),
                    op: ComparisonOp::GtEq,
                    value: StatValue::Int64(threshold),
                },
                ProjectionRequest::columns(vec![ColumnRef::new("metric").unwrap()]),
            )
            .with_source_order_limit(2);
            let result = collect_rows(
                &request,
                VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
            )
            .unwrap();
            let values: serde_json::Value =
                serde_json::from_str(result.values_json.value()).unwrap();
            assert_eq!(values, expected);
            assert_eq!(result.rows, expected.as_array().unwrap().len() as u64);
        }
    }

    #[cfg(feature = "vortex-write")]
    #[test]
    fn nullable_utf8_and_large_integers_are_lossless_and_unbounded_collect_fails() {
        use vortex::{
            VortexSessionDefault as _,
            array::{
                IntoArray as _,
                arrays::{PrimitiveArray, StructArray, VarBinViewArray},
                dtype::FieldNames,
                validity::Validity,
            },
            file::WriteOptionsSessionExt as _,
            io::{
                runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
                session::RuntimeSessionExt as _,
            },
            session::VortexSession,
        };
        let path = std::env::temp_dir().join(format!(
            "shardloom-collect-values-{}.vortex",
            std::process::id()
        ));
        let ids =
            PrimitiveArray::from_option_iter([Some(i64::MAX), None, Some(i64::MIN)]).into_array();
        let text = VarBinViewArray::from_iter_nullable_str([Some("\u{03b1}\"\n"), None, Some("")])
            .into_array();
        let array = StructArray::try_new(
            FieldNames::from(["identifier", "text"]),
            vec![ids, text],
            3,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut bytes = Vec::new();
        runtime
            .block_on(
                session
                    .write_options()
                    .write(&mut bytes, array.to_array_stream()),
            )
            .unwrap();
        std::fs::write(&path, bytes).unwrap();
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new(path.display().to_string()).unwrap(),
            ProjectionRequest::columns(vec![
                ColumnRef::new("text").unwrap(),
                ColumnRef::new("identifier").unwrap(),
            ]),
        );
        let result = collect_rows(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
        )
        .unwrap();
        let values: serde_json::Value = serde_json::from_str(result.values_json.value()).unwrap();
        assert_eq!(
            values,
            serde_json::json!([
                {"identifier": i64::MAX, "text": "\u{03b1}\"\n"},
                {"identifier": null, "text": null},
                {"identifier": i64::MIN, "text": ""},
            ])
        );
        drop(result);
        let array = StructArray::try_new(
            FieldNames::from(["identifier"]),
            vec![PrimitiveArray::new(vec![7_i64; 65_537], Validity::NonNullable).into_array()],
            65_537,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let mut bytes = Vec::new();
        runtime
            .block_on(
                session
                    .write_options()
                    .write(&mut bytes, array.to_array_stream()),
            )
            .unwrap();
        std::fs::write(&path, bytes).unwrap();
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new(path.display().to_string()).unwrap(),
            ProjectionRequest::columns(vec![ColumnRef::new("identifier").unwrap()]),
        );
        let result = collect_rows(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
        );
        assert!(result.err().unwrap().to_string().contains("65,536 rows"));
        std::fs::remove_file(&path).unwrap();
    }
}
