//! Explicit bounded JSON result sink over resident, owned native arrays.

use super::{
    Result, ShardLoomError, VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveKind,
    VortexQueryPrimitiveRequest, bind_vortex_scan_expr, local_vortex_path, row_export_scan_plan,
    stat_value_to_json_value, vortex_scalar_to_stat_value,
};
use crate::resident_session::{
    OwnedVortexResultBatch, ResidentSessionSnapshot, ResidentVortexSession,
};
use shardloom_exec::live_memory::Budgeted;
use std::{collections::BTreeMap, io::Write as _};

pub struct CollectedVortexRows {
    pub rows: u64,
    pub projected_columns: Vec<String>,
    pub source_order_limit: Option<usize>,
    pub values_json: Budgeted<String>,
    pub runtime: ResidentSessionSnapshot,
    pub native_io_certificate: shardloom_core::NativeIoCertificate,
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
    let session = ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )?;
    let source = session.prepare_file(path)?;
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
    let rows_limit = request.source_order_limit.map_or(65_537, |limit| {
        u64::try_from(limit).unwrap_or(u64::MAX).min(65_537)
    });
    let filter = plan
        .filter
        .as_ref()
        .map(|filter| bind_vortex_scan_expr(source.file(), filter))
        .transpose()?;
    let result = source
        .prepare_projection(&names, rows_limit, 32 * 1024 * 1024)?
        .with_filter(filter)
        .execute()?;
    if result.row_count() > 65_536 {
        return Err(collect_error(
            "collect exceeds 65,536 rows; use an explicit streaming export",
        ));
    }
    let lease = session.memory().reserve(8 * 1024 * 1024)?;
    let mut output = BoundedJson {
        bytes: Vec::with_capacity(8 * 1024 * 1024),
        limit: 8 * 1024 * 1024,
    };
    render_rows(&result, &names, &mut output)?;
    let values_json = String::from_utf8(output.bytes).map_err(collect_io_error)?;
    source.validate_generation()?;
    let native_io_certificate = certificate(request, result.row_count(), plan.filter.is_some())?;
    Ok(CollectedVortexRows {
        rows: result.row_count(),
        projected_columns: plan.projected_columns,
        source_order_limit: request.source_order_limit,
        values_json: Budgeted::new(values_json, lease),
        runtime: session.snapshot(),
        native_io_certificate,
    })
}

fn certificate(
    request: &VortexQueryPrimitiveRequest,
    rows: u64,
    filtered: bool,
) -> Result<shardloom_core::NativeIoCertificate> {
    use shardloom_core::{
        NativeIoAdapterFidelityReport, NativeIoCertificate, NativeIoMaterializationBoundaryReport,
        NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
        NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, RepresentationState,
    };
    let mut accepted = vec!["projection".to_string()];
    if filtered {
        accepted.push("exact_filter".to_string());
    }
    NativeIoCertificate::new(
        format!("resident.{}.bounded_collect.native_io", request.kind.as_str()),
        "resident_vortex_owned_arrays_to_bounded_json",
        NativeIoSourceCapabilityReport {
            source_kind: "local_vortex_file".into(), adapter_id: "shardloom.resident_vortex.v1".into(),
            schema_discovery_status: "opened_validated_generation".into(),
            statistics_availability: "native_footer".into(),
            pushdown_capabilities: "projection,exact_filter".into(),
            encoded_representation_preserved: true, range_read_capability: true,
            streaming_capability: true, object_store_capability: false, fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: accepted, rejected_operations: Vec::new(),
            guarantee: "exact_provider_filter_then_ordered_result_limit".into(),
            proof_basis: format!("vortex {} bound scan expressions; source generation checked before and after execution", crate::UPSTREAM_VORTEX_PROVIDER_VERSION),
            residual_expression: None, conservative_false_positive_policy: false,
            unsafe_rejected_reason: None, fallback_attempted: false,
        },
        vec![NativeIoRepresentationTransition::new(RepresentationState::VortexEncoded, RepresentationState::MaterializedRows, true)],
        NativeIoSinkRequirementReport {
            target_format: "bounded_json_rows".into(), accepts_encoded: false,
            requires_decoded_columnar: false, requires_rows: true, preserves_metadata: false,
            requires_ordering: true, requires_partitioning: false, requires_commit: false,
            supports_streaming: false, max_chunk_size: Some(8 * 1024 * 1024),
            backpressure_policy: "65536_row_and_8mib_complete_result_bounds_with_owned_buffer_reservations".into(),
        },
        NativeIoAdapterFidelityReport {
            adapter_id: "shardloom.resident_vortex.json_sink.v1".into(), source_kind: "vortex".into(),
            sink_kind: "json".into(), metadata_preserved: false, statistics_preserved: false,
            encoded_representation_preserved: false, materialization_required: true,
            fidelity_loss: "JSON preserves admitted scalar values, not physical dtype or encoding".into(),
            metadata_loss: "Vortex physical encodings, statistics and metadata not exported".into(), fallback_attempted: false,
        },
        vec![NativeIoMaterializationBoundaryReport {
            boundary_id: "resident_collect_json_sink".into(), from_state: RepresentationState::VortexEncoded,
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
        request.diagnostics.clone(),
    )
}

fn render_rows(
    result: &OwnedVortexResultBatch,
    names: &[&str],
    output: &mut BoundedJson,
) -> Result<()> {
    let mut context = result.create_execution_ctx();
    output.write_all(b"[").map_err(collect_io_error)?;
    let mut emitted = 0;
    for array in result.arrays() {
        let children = array
            .named_children()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for row in 0..array.len() {
            if emitted != 0 {
                output.write_all(b",").map_err(collect_io_error)?;
            }
            output.write_all(b"{").map_err(collect_io_error)?;
            for (index, column) in names.iter().enumerate() {
                if index != 0 {
                    output.write_all(b",").map_err(collect_io_error)?;
                }
                serde_json::to_writer(&mut *output, column).map_err(collect_io_error)?;
                output.write_all(b":").map_err(collect_io_error)?;
                let child = children
                    .get(*column)
                    .ok_or_else(|| collect_error("projected field missing from native array"))?;
                let scalar = child
                    .execute_scalar(row, &mut context)
                    .map_err(collect_io_error)?;
                let value = if scalar.is_null() {
                    serde_json::Value::Null
                } else {
                    let value = vortex_scalar_to_stat_value(&scalar).ok_or_else(|| {
                        collect_error("result type is not admitted for JSON collect")
                    })?;
                    stat_value_to_json_value(&value)?
                };
                serde_json::to_writer(&mut *output, &value).map_err(collect_io_error)?;
            }
            output.write_all(b"}").map_err(collect_io_error)?;
            emitted += 1;
        }
    }
    output.write_all(b"]").map_err(collect_io_error)?;
    Ok(())
}

struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
}

impl std::io::Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other(
                "collect exceeds 8 MiB; use an explicit streaming export",
            ));
        }
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
    use super::*;
    use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    use shardloom_plan::ProjectionRequest;

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
        let mut sink = BoundedJson {
            bytes: Vec::with_capacity(4),
            limit: 4,
        };
        sink.write_all(b"1234").unwrap();
        assert!(sink.write_all(b"5").is_err());
        assert_eq!(sink.bytes, b"1234");
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
