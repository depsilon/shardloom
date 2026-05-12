//! Shared typed-envelope routing for CLI command output.
//!
//! This module is deliberately protocol-only: it routes existing command fields
//! into typed envelope slots and refs without changing command behavior,
//! executing runtime work, probing datasets, or weakening no-fallback policy.

use shardloom_core::{OutputEnvelope, OutputTypedRef};

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
}
