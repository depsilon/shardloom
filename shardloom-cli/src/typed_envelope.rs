//! Shared typed-envelope routing for CLI command output.
//!
//! This module is deliberately protocol-only: it routes existing command fields
//! into typed envelope slots and refs without changing command behavior,
//! executing runtime work, probing datasets, or weakening no-fallback policy.

use shardloom_core::{OutputEnvelope, OutputTypedArtifact, OutputTypedRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypedEnvelopeFieldSlot {
    Result,
    Policy,
    Lifecycle,
    CapabilitySnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypedEnvelopeRefSlot {
    ResultRef,
    ArtifactRef,
    Certificate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InlineReportPayloadSpec {
    artifact_id_fallback: &'static str,
    artifact_kind: &'static str,
    status_key: &'static str,
    payload_keys: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InlinePrefixedPayloadSpec {
    key_prefix: &'static str,
    artifact_kind: &'static str,
    emitted_key: &'static str,
    id_key: &'static str,
    status_key: &'static str,
}

const INLINE_PREFIXED_PAYLOAD_SPECS: &[InlinePrefixedPayloadSpec] = &[
    InlinePrefixedPayloadSpec {
        key_prefix: "local_count_native_io_",
        artifact_kind: "native_io_certificate",
        emitted_key: "local_count_native_io_certificate_emitted",
        id_key: "local_count_native_io_certificate_id",
        status_key: "local_count_native_io_certificate_status",
    },
    InlinePrefixedPayloadSpec {
        key_prefix: "execution_certificate_",
        artifact_kind: "execution_certificate",
        emitted_key: "execution_certificate_emitted",
        id_key: "execution_certificate_id",
        status_key: "execution_certificate_status",
    },
    InlinePrefixedPayloadSpec {
        key_prefix: "local_primitive_native_io_",
        artifact_kind: "native_io_certificate",
        emitted_key: "local_primitive_native_io_certificate_emitted",
        id_key: "local_primitive_native_io_certificate_id",
        status_key: "local_primitive_native_io_certificate_status",
    },
    InlinePrefixedPayloadSpec {
        key_prefix: "local_primitive_execution_certificate_",
        artifact_kind: "execution_certificate",
        emitted_key: "local_primitive_execution_certificate_emitted",
        id_key: "local_primitive_execution_certificate_id",
        status_key: "local_primitive_execution_certificate_status",
    },
    InlinePrefixedPayloadSpec {
        key_prefix: "streaming_batch_runtime_",
        artifact_kind: "streaming_batch_runtime_report",
        emitted_key: "streaming_batch_runtime_report_emitted",
        id_key: "streaming_batch_runtime_report_id",
        status_key: "streaming_batch_runtime_status",
    },
];

const EXECUTION_CERTIFICATE_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "certificate_schema_version",
    "artifact_count",
    "required_artifact_count",
    "hash_required_count",
    "machine_readable_required_count",
    "artifact_order",
    "machine_readable_certificate_surface",
    "deterministic_field_order_required",
    "certificate_evaluation_performed",
    "runtime_execution",
    "data_read",
    "data_materialized",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const NATIVE_IO_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "contract_count",
    "representation_state_count",
    "transition_example_count",
    "certificate_path_requirement_count",
    "contract_kind_order",
    "representation_state_order",
    "transition_example_order",
    "certificate_path_order",
    "per_path_certificate_required",
    "aggregate_certificate_not_sufficient",
    "materialization_boundary_required_for_decoded_columnar",
    "materialization_boundary_required_for_rows",
    "source_pushdown_proof_required",
    "sink_requirement_propagation_required",
    "adapter_fidelity_report_required",
    "runtime_execution",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const BENCHMARK_PLAN_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "status",
    "benchmark_execution_implemented",
    "external_baselines",
    "scenario_count",
    "required_metric_count",
    "required_metric_order",
    "external_baseline_engine_order",
    "claim_gate_status",
    "claim_gate_correctness_evidence",
    "claim_gate_benchmark_evidence",
    "claim_gate_comparison_report",
    "claim_gate_reproducibility_evidence",
    "performance_claim_allowed",
    "fallback_execution_allowed",
];

const BENCHMARK_CLAIM_EVIDENCE_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "schema_version",
    "report_id",
    "scope",
    "claim_evidence_status",
    "surface_count",
    "blocked_surface_count",
    "blocked_surface_order",
    "scenario_count",
    "required_metric_count",
    "expected_result_count",
    "result_count",
    "missing_result_count",
    "run_manifest_status",
    "comparison_report_status",
    "claim_gate_status",
    "claim_gate_benchmark_evidence",
    "measured_benchmark_result_rows_required",
    "measured_benchmark_result_rows_present",
    "benchmark_execution_performed",
    "fallback_execution_allowed",
    "fallback_attempted",
];

const STREAMING_PLAN_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "status",
    "source_kind",
    "source_zero_decode",
    "sink_kind",
    "sink_accepts_encoded",
    "sink_requires_materialization",
    "sink_preserves_metadata",
    "materialization_required",
    "best_data_work_level",
    "runtime_execution",
    "fallback_execution_allowed",
];

const STREAMING_BATCH_PLAN_REPORT_PAYLOAD_KEYS: &[&str] = &[
    "mode",
    "encoded_streaming_batch_status",
    "streaming_mode",
    "source_kind",
    "source_capability",
    "sink_kind",
    "sink_capability",
    "representation",
    "zero_decode",
    "encoded_representation_preserved",
    "selection_vector_preserved",
    "materialization_required",
    "materialization_boundary",
    "bounded_parallelism",
    "bounded_memory",
    "backpressure_bounded",
    "estimated_batch_count",
    "estimated_batch_mib",
    "streams_executed",
    "tasks_executed",
    "data_read",
    "data_decoded",
    "data_materialized",
    "fallback_execution_allowed",
];

fn typed_envelope_field_slot(key: &str) -> TypedEnvelopeFieldSlot {
    let normalized = key.to_ascii_lowercase();
    if is_policy_envelope_field(&normalized) {
        TypedEnvelopeFieldSlot::Policy
    } else if is_capability_envelope_field(&normalized) {
        TypedEnvelopeFieldSlot::CapabilitySnapshot
    } else if is_lifecycle_envelope_field(&normalized) {
        TypedEnvelopeFieldSlot::Lifecycle
    } else {
        TypedEnvelopeFieldSlot::Result
    }
}

fn is_policy_envelope_field(key: &str) -> bool {
    key.contains("fallback")
        || key.contains("side_effect")
        || key.contains("external_engine")
        || key.contains("external_runtime")
        || key.contains("network_effect")
        || key.contains("network_probe")
        || key.contains("filesystem_probe")
        || key.contains("catalog_probe")
        || key.contains("adapter_probe")
        || key.contains("dataset_probe")
        || key.contains("write_effect")
        || key.contains("write_io")
        || key.contains("unsafe_effect")
        || key.contains("destructive")
        || key.contains("policy")
        || key.ends_with("_allowed")
        || key.ends_with("_prohibited")
}

fn is_lifecycle_envelope_field(key: &str) -> bool {
    matches!(
        key,
        "mode"
            | "schema_version"
            | "protocol_id"
            | "protocol_stability"
            | "output_envelope_schema_version"
            | "transport_protocol_id"
            | "invocation_model"
            | "command_status_values"
            | "output_formats"
    ) || key.ends_with("_mode")
        || key.ends_with("_version")
        || key.ends_with("_status")
        || key.ends_with("_phase")
        || key.ends_with("_protocol")
}

fn is_capability_envelope_field(key: &str) -> bool {
    key.contains("capability")
        || key.contains("certification")
        || key.contains("feature_gate")
        || key.contains("supported")
        || key.contains("unsupported")
        || key.contains("readiness")
        || key.contains("coverage")
        || key.ends_with("_ready")
}

fn inline_report_payload_spec(command: &str) -> Option<InlineReportPayloadSpec> {
    match command {
        "execution-certificate-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "execution-certificate-plan.report",
            artifact_kind: "execution_certificate_report",
            status_key: "certificate_surface_status",
            payload_keys: EXECUTION_CERTIFICATE_REPORT_PAYLOAD_KEYS,
        }),
        "native-io-envelope-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "native-io-envelope-plan.report",
            artifact_kind: "native_io_report",
            status_key: "native_io_envelope_status",
            payload_keys: NATIVE_IO_REPORT_PAYLOAD_KEYS,
        }),
        "benchmark-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "benchmark-plan.report",
            artifact_kind: "benchmark_plan_report",
            status_key: "claim_gate_status",
            payload_keys: BENCHMARK_PLAN_REPORT_PAYLOAD_KEYS,
        }),
        "benchmark-claim-evidence-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "benchmark-claim-evidence-plan.report",
            artifact_kind: "benchmark_claim_evidence_report",
            status_key: "claim_gate_status",
            payload_keys: BENCHMARK_CLAIM_EVIDENCE_REPORT_PAYLOAD_KEYS,
        }),
        "streaming-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "streaming-plan.materialization-boundary",
            artifact_kind: "materialization_boundary_report",
            status_key: "status",
            payload_keys: STREAMING_PLAN_REPORT_PAYLOAD_KEYS,
        }),
        "streaming-batch-plan" => Some(InlineReportPayloadSpec {
            artifact_id_fallback: "streaming-batch-plan.materialization-boundary",
            artifact_kind: "materialization_boundary_report",
            status_key: "encoded_streaming_batch_status",
            payload_keys: STREAMING_BATCH_PLAN_REPORT_PAYLOAD_KEYS,
        }),
        _ => None,
    }
}

fn field_value<'a>(fields: &'a [(String, String)], key: &str) -> Option<&'a str> {
    fields
        .iter()
        .find_map(|(field_key, value)| (field_key == key).then_some(value.as_str()))
}

fn inline_report_artifact(
    spec: InlineReportPayloadSpec,
    fields: &[(String, String)],
) -> OutputTypedArtifact {
    let artifact_id = field_value(fields, "report_id").unwrap_or(spec.artifact_id_fallback);
    let status = field_value(fields, spec.status_key).unwrap_or("available");
    let mut artifact = OutputTypedArtifact::new(artifact_id, spec.artifact_kind, status);
    for key in spec.payload_keys {
        if let Some(value) = field_value(fields, key) {
            artifact = artifact.with_field(*key, value);
        }
    }
    artifact
}

fn inline_report_payload(
    command: &str,
    fields: &[(String, String)],
) -> Option<OutputTypedArtifact> {
    let spec = inline_report_payload_spec(command)?;
    let artifact = inline_report_artifact(spec, fields);
    if artifact.payload.fields.is_empty() {
        None
    } else {
        Some(artifact)
    }
}

fn inline_payload_artifact_id(
    command: &str,
    artifact_kind: &str,
    field_value: Option<&str>,
) -> String {
    field_value
        .filter(|value| field_value_is_reference(value))
        .map_or_else(
            || format!("{command}.{artifact_kind}"),
            std::string::ToString::to_string,
        )
}

fn inline_prefixed_payload(
    command: &str,
    spec: InlinePrefixedPayloadSpec,
    fields: &[(String, String)],
) -> Option<OutputTypedArtifact> {
    if field_value(fields, spec.emitted_key) != Some("true") {
        return None;
    }
    let artifact_id = inline_payload_artifact_id(
        command,
        spec.artifact_kind,
        field_value(fields, spec.id_key),
    );
    let status = field_value(fields, spec.status_key).unwrap_or("available");
    let mut artifact = OutputTypedArtifact::new(artifact_id, spec.artifact_kind, status);
    for (key, value) in fields
        .iter()
        .filter(|(key, _)| key.starts_with(spec.key_prefix))
    {
        artifact = artifact.with_field(key, value);
    }
    if artifact.payload.fields.is_empty() {
        None
    } else {
        Some(artifact)
    }
}

fn inline_prefixed_payloads(
    command: &str,
    fields: &[(String, String)],
) -> Vec<OutputTypedArtifact> {
    INLINE_PREFIXED_PAYLOAD_SPECS
        .iter()
        .filter_map(|spec| inline_prefixed_payload(command, *spec, fields))
        .collect()
}

fn typed_envelope_ref_slot(key: &str) -> Option<(TypedEnvelopeRefSlot, &'static str)> {
    let normalized = key.to_ascii_lowercase();
    if !is_reference_field_key(&normalized) {
        return None;
    }

    if normalized.contains("native_io_certificate") {
        Some((TypedEnvelopeRefSlot::Certificate, "native_io_certificate"))
    } else if normalized.contains("execution_certificate") {
        Some((TypedEnvelopeRefSlot::Certificate, "execution_certificate"))
    } else if normalized.contains("certificate") {
        Some((TypedEnvelopeRefSlot::Certificate, "certificate"))
    } else if normalized.contains("evidence_artifact") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "evidence_artifact"))
    } else if normalized.contains("materialization_boundary") {
        Some((
            TypedEnvelopeRefSlot::ArtifactRef,
            "materialization_boundary_report",
        ))
    } else if normalized.contains("benchmark_row") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "benchmark_row"))
    } else if normalized.contains("foundry") && normalized.contains("report") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "foundry_boundary_report"))
    } else if normalized.contains("source") && normalized.contains("report") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "source_report"))
    } else if normalized.contains("sink") && normalized.contains("report") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "sink_report"))
    } else if normalized.contains("artifact") {
        Some((TypedEnvelopeRefSlot::ArtifactRef, "artifact"))
    } else if normalized.contains("result") {
        Some((TypedEnvelopeRefSlot::ResultRef, "result"))
    } else {
        None
    }
}

fn is_reference_field_key(key: &str) -> bool {
    key.ends_with("_ref")
        || key.ends_with("_refs")
        || key.ends_with("_id")
        || key.ends_with("_uri")
        || key.ends_with("_path")
}

fn field_value_is_reference(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    !normalized.is_empty()
        && !matches!(
            normalized.as_str(),
            "false" | "true" | "0" | "none" | "null" | "not_performed" | "not_available"
        )
}

fn typed_ref_status(value: &str) -> &'static str {
    let normalized = value.to_ascii_lowercase();
    if normalized.contains("blocked") {
        "blocked"
    } else if normalized.contains("missing") || normalized.contains("incomplete") {
        "incomplete"
    } else {
        "available"
    }
}

fn typed_ref_id(key: &str, value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() <= 128 && !trimmed.contains(',') {
        trimmed.to_string()
    } else {
        key.to_string()
    }
}

fn typed_ref_uri(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let extension_candidate = std::path::Path::new(trimmed)
        .extension()
        .and_then(std::ffi::OsStr::to_str);
    if trimmed.contains("://")
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || extension_candidate.is_some_and(|ext| {
            ["json", "vortex", "parquet"]
                .iter()
                .any(|known| ext.eq_ignore_ascii_case(known))
        })
    {
        Some(trimmed.to_string())
    } else {
        None
    }
}

fn add_typed_ref_if_present(envelope: OutputEnvelope, key: &str, value: &str) -> OutputEnvelope {
    let Some((slot, kind)) = typed_envelope_ref_slot(key) else {
        return envelope;
    };
    if !field_value_is_reference(value) {
        return envelope;
    }

    let mut reference =
        OutputTypedRef::new(typed_ref_id(key, value), kind, typed_ref_status(value));
    if let Some(uri) = typed_ref_uri(value) {
        reference = reference.with_uri(uri);
    }

    match slot {
        TypedEnvelopeRefSlot::ResultRef => envelope.with_result_ref(reference),
        TypedEnvelopeRefSlot::ArtifactRef => envelope.with_artifact_ref(reference),
        TypedEnvelopeRefSlot::Certificate => envelope.with_certificate(reference),
    }
}

pub(crate) fn apply_typed_envelope_field(
    envelope: OutputEnvelope,
    key: String,
    value: String,
) -> OutputEnvelope {
    let envelope = add_typed_ref_if_present(envelope, &key, &value);
    match typed_envelope_field_slot(&key) {
        TypedEnvelopeFieldSlot::Result => envelope.with_result_field(key, value),
        TypedEnvelopeFieldSlot::Policy => envelope
            .with_policy_field(key.clone(), value.clone())
            .with_legacy_field(key, value),
        TypedEnvelopeFieldSlot::Lifecycle => envelope
            .with_lifecycle_field(key.clone(), value.clone())
            .with_legacy_field(key, value),
        TypedEnvelopeFieldSlot::CapabilitySnapshot => envelope
            .with_capability_snapshot_field(key.clone(), value.clone())
            .with_legacy_field(key, value),
    }
}

pub(crate) fn apply_typed_envelope_fields(
    envelope: OutputEnvelope,
    command: &str,
    fields: Vec<(String, String)>,
) -> OutputEnvelope {
    let inline_report = inline_report_payload(command, &fields);
    let inline_prefixed_payloads = inline_prefixed_payloads(command, &fields);
    let mut envelope = envelope;
    for (key, value) in fields {
        envelope = apply_typed_envelope_field(envelope, key, value);
    }
    if let Some(artifact) = inline_report {
        envelope = envelope.with_artifact(artifact);
    }
    for artifact in inline_prefixed_payloads {
        envelope = envelope.with_artifact(artifact);
    }
    envelope
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_ref_fields_attach_typed_certificate_and_legacy_field() {
        let envelope = apply_typed_envelope_field(
            OutputEnvelope::success("test", "ok", "ok"),
            "execution_certificate_ref".to_string(),
            "cert.execution.local".to_string(),
        );

        assert_eq!(envelope.certificates.len(), 1);
        assert_eq!(envelope.certificates[0].id, "cert.execution.local");
        assert_eq!(envelope.certificates[0].kind, "execution_certificate");
        assert_eq!(envelope.certificates[0].status, "available");
        assert_eq!(
            envelope.fields,
            vec![(
                "execution_certificate_ref".to_string(),
                "cert.execution.local".to_string()
            )]
        );
    }

    #[test]
    fn artifact_ref_fields_attach_typed_artifact_ref() {
        let envelope = apply_typed_envelope_field(
            OutputEnvelope::success("test", "ok", "ok"),
            "materialization_boundary_report_ref".to_string(),
            "artifacts/materialization.json".to_string(),
        );

        assert_eq!(envelope.artifact_refs.len(), 1);
        assert_eq!(
            envelope.artifact_refs[0].kind,
            "materialization_boundary_report"
        );
        assert_eq!(
            envelope.artifact_refs[0].uri.as_deref(),
            Some("artifacts/materialization.json")
        );
    }

    #[test]
    fn command_fields_attach_inline_execution_certificate_report_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("execution-certificate-plan", "ok", "ok"),
            "execution-certificate-plan",
            vec![
                ("mode".to_string(), "execution_certificate_plan".to_string()),
                (
                    "schema_version".to_string(),
                    "shardloom.execution_certificate_evidence_surface.v1".to_string(),
                ),
                (
                    "report_id".to_string(),
                    "cg16.execution-certificate-evidence-surface".to_string(),
                ),
                (
                    "certificate_surface_status".to_string(),
                    "report_only_planned".to_string(),
                ),
                ("artifact_count".to_string(), "6".to_string()),
                (
                    "machine_readable_certificate_surface".to_string(),
                    "true".to_string(),
                ),
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(
            envelope.artifacts[0].artifact_id,
            "cg16.execution-certificate-evidence-surface"
        );
        assert_eq!(
            envelope.artifacts[0].artifact_kind,
            "execution_certificate_report"
        );
        assert_eq!(envelope.artifacts[0].status, "report_only_planned");
        assert!(
            envelope.artifacts[0]
                .payload
                .fields
                .contains(&("artifact_count".to_string(), "6".to_string()))
        );
        assert!(envelope.fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn command_fields_attach_inline_benchmark_claim_evidence_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("benchmark-claim-evidence-plan", "ok", "ok"),
            "benchmark-claim-evidence-plan",
            vec![
                ("mode".to_string(), "benchmark_claim_evidence".to_string()),
                (
                    "report_id".to_string(),
                    "cg6.benchmark_claim_evidence.aggregate".to_string(),
                ),
                (
                    "claim_gate_status".to_string(),
                    "evidence_missing".to_string(),
                ),
                (
                    "measured_benchmark_result_rows_present".to_string(),
                    "false".to_string(),
                ),
                ("fallback_attempted".to_string(), "false".to_string()),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(
            envelope.artifacts[0].artifact_kind,
            "benchmark_claim_evidence_report"
        );
        assert_eq!(envelope.artifacts[0].status, "evidence_missing");
        assert!(envelope.artifacts[0].payload.fields.contains(&(
            "measured_benchmark_result_rows_present".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn command_fields_attach_inline_emitted_runtime_certificate_payloads() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("vortex-run", "ok", "ok"),
            "vortex-run",
            vec![
                (
                    "local_primitive_native_io_certificate_emitted".to_string(),
                    "true".to_string(),
                ),
                (
                    "local_primitive_native_io_certificate_id".to_string(),
                    "native-io.local.fixture".to_string(),
                ),
                (
                    "local_primitive_native_io_certificate_status".to_string(),
                    "certified".to_string(),
                ),
                (
                    "local_primitive_native_io_source_kind".to_string(),
                    "vortex_file".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_emitted".to_string(),
                    "true".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_id".to_string(),
                    "execution.local.fixture".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_status".to_string(),
                    "certified".to_string(),
                ),
                (
                    "local_primitive_execution_certificate_fixture_id".to_string(),
                    "vortex-local-count-where-struct-five".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 2);
        assert!(envelope.artifacts.iter().any(|artifact| {
            artifact.artifact_id == "native-io.local.fixture"
                && artifact.artifact_kind == "native_io_certificate"
                && artifact.status == "certified"
                && artifact.payload.fields.contains(&(
                    "local_primitive_native_io_source_kind".to_string(),
                    "vortex_file".to_string(),
                ))
        }));
        assert!(envelope.artifacts.iter().any(|artifact| {
            artifact.artifact_id == "execution.local.fixture"
                && artifact.artifact_kind == "execution_certificate"
                && artifact.status == "certified"
                && artifact.payload.fields.contains(&(
                    "local_primitive_execution_certificate_fixture_id".to_string(),
                    "vortex-local-count-where-struct-five".to_string(),
                ))
        }));
    }

    #[test]
    fn command_fields_skip_inline_runtime_certificate_payload_when_not_emitted() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("vortex-count", "ok", "ok"),
            "vortex-count",
            vec![
                (
                    "execution_certificate_emitted".to_string(),
                    "false".to_string(),
                ),
                (
                    "execution_certificate_status".to_string(),
                    "evidence_unavailable".to_string(),
                ),
            ],
        );

        assert!(envelope.artifacts.is_empty());
        assert_eq!(
            envelope.lifecycle.fields,
            vec![(
                "execution_certificate_status".to_string(),
                "evidence_unavailable".to_string()
            )]
        );
    }

    #[test]
    fn command_fields_attach_inline_streaming_runtime_report_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("vortex-count", "ok", "ok"),
            "vortex-count",
            vec![
                (
                    "streaming_batch_runtime_report_emitted".to_string(),
                    "true".to_string(),
                ),
                (
                    "streaming_batch_runtime_status".to_string(),
                    "executed".to_string(),
                ),
                (
                    "streaming_batch_runtime_representation".to_string(),
                    "vortex_encoded".to_string(),
                ),
                (
                    "streaming_batch_runtime_zero_decode".to_string(),
                    "preserved".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(
            envelope.artifacts[0].artifact_id,
            "vortex-count.streaming_batch_runtime_report"
        );
        assert_eq!(
            envelope.artifacts[0].artifact_kind,
            "streaming_batch_runtime_report"
        );
        assert_eq!(envelope.artifacts[0].status, "executed");
        assert!(envelope.artifacts[0].payload.fields.contains(&(
            "streaming_batch_runtime_zero_decode".to_string(),
            "preserved".to_string()
        )));
    }

    #[test]
    fn command_fields_attach_inline_materialization_boundary_report_payload() {
        let envelope = apply_typed_envelope_fields(
            OutputEnvelope::success("streaming-batch-plan", "ok", "ok"),
            "streaming-batch-plan",
            vec![
                ("mode".to_string(), "streaming_batch_plan".to_string()),
                (
                    "encoded_streaming_batch_status".to_string(),
                    "requires_materialization".to_string(),
                ),
                (
                    "materialization_boundary".to_string(),
                    "full_materialization_boundary".to_string(),
                ),
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
            ],
        );

        assert_eq!(envelope.artifacts.len(), 1);
        assert_eq!(
            envelope.artifacts[0].artifact_id,
            "streaming-batch-plan.materialization-boundary"
        );
        assert_eq!(
            envelope.artifacts[0].artifact_kind,
            "materialization_boundary_report"
        );
        assert_eq!(envelope.artifacts[0].status, "requires_materialization");
        assert!(envelope.artifacts[0].payload.fields.contains(&(
            "materialization_boundary".to_string(),
            "full_materialization_boundary".to_string()
        )));
    }
}
