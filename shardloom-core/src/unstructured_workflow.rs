//! Unstructured, media, model, and embedding workflow boundary contracts.
//!
//! This module is report-only. It models media and model-derived artifacts as
//! typed references plus explicit effect boundaries without implementing OCR,
//! transcription, media decoding, embedding generation, LLM calls, model
//! inference, provider retries, or runtime fallback behavior.

#![allow(clippy::struct_excessive_bools)]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaLocationKind {
    FoundryMediaSet,
    FoundryVirtualMediaSet,
    DatasetPath,
    ExternalUri,
    LocalPath,
}

impl MediaLocationKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FoundryMediaSet => "foundry_media_set",
            Self::FoundryVirtualMediaSet => "foundry_virtual_media_set",
            Self::DatasetPath => "dataset_path",
            Self::ExternalUri => "external_uri",
            Self::LocalPath => "local_path",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Document,
    Image,
    Audio,
    Video,
    Archive,
    BinaryBlob,
}

impl MediaKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Archive => "archive",
            Self::BinaryBlob => "binary_blob",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryExecutor {
    PipelineCode,
    FoundryMediaTransform,
    FoundryAipLogic,
    FoundryModelService,
    GovernedModelService,
    ShardLoomStructuredAnalytics,
}

impl BoundaryExecutor {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PipelineCode => "pipeline_code",
            Self::FoundryMediaTransform => "foundry_media_transform",
            Self::FoundryAipLogic => "foundry_aip_logic",
            Self::FoundryModelService => "foundry_model_service",
            Self::GovernedModelService => "governed_model_service",
            Self::ShardLoomStructuredAnalytics => "shardloom_structured_analytics",
        }
    }

    #[must_use]
    pub const fn shardloom_core_runtime_owner(self) -> bool {
        matches!(self, Self::ShardLoomStructuredAnalytics)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowBoundaryKind {
    MediaExtraction,
    Chunking,
    ModelCall,
    EmbeddingGeneration,
    Redaction,
    StructuredAnalytics,
}

impl WorkflowBoundaryKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MediaExtraction => "media_extraction",
            Self::Chunking => "chunking",
            Self::ModelCall => "model_call",
            Self::EmbeddingGeneration => "embedding_generation",
            Self::Redaction => "redaction",
            Self::StructuredAnalytics => "structured_analytics",
        }
    }

    #[must_use]
    pub const fn is_effectful(self) -> bool {
        matches!(
            self,
            Self::MediaExtraction | Self::ModelCall | Self::EmbeddingGeneration | Self::Redaction
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeterminismLevel {
    Deterministic,
    NonDeterministic,
    ProviderDeclared,
    Unknown,
}

impl DeterminismLevel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::NonDeterministic => "non_deterministic",
            Self::ProviderDeclared => "provider_declared",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnstructuredMaturity {
    U0DeclaredOnly,
    U1MediaReferenceDiscovery,
    U2ExtractionBoundaryRecorded,
    U3ChunkTableEmitted,
    U4EmbeddingOrModelBoundaryRecorded,
    U5StructuredOutputsValidated,
    U6DownstreamAnalyticsCertified,
    U7FoundryWorkflowCertified,
}

impl UnstructuredMaturity {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::U0DeclaredOnly => "u0_declared_only",
            Self::U1MediaReferenceDiscovery => "u1_media_reference_discovery",
            Self::U2ExtractionBoundaryRecorded => "u2_extraction_boundary_recorded",
            Self::U3ChunkTableEmitted => "u3_chunk_table_emitted",
            Self::U4EmbeddingOrModelBoundaryRecorded => "u4_embedding_or_model_boundary_recorded",
            Self::U5StructuredOutputsValidated => "u5_structured_outputs_validated",
            Self::U6DownstreamAnalyticsCertified => "u6_downstream_analytics_certified",
            Self::U7FoundryWorkflowCertified => "u7_foundry_workflow_certified",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaRef {
    pub media_ref_id: &'static str,
    pub media_kind: MediaKind,
    pub location_kind: MediaLocationKind,
    pub locator_ref: &'static str,
    pub mime_type: &'static str,
    pub checksum_status: &'static str,
    pub access_policy_ref: &'static str,
    pub extraction_status: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaManifest {
    pub manifest_id: &'static str,
    pub source_system: &'static str,
    pub media_ref_count: usize,
    pub virtual_or_external_status: &'static str,
    pub update_detection_policy: &'static str,
    pub known_limitations: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextChunkTable {
    pub table_id: &'static str,
    pub required_columns: Vec<&'static str>,
    pub provenance_required: bool,
    pub confidence_required: bool,
    pub redaction_status_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingTable {
    pub table_id: &'static str,
    pub required_columns: Vec<&'static str>,
    pub model_version_required: bool,
    pub vector_dimension_required: bool,
    pub input_hash_required: bool,
    pub vector_execution_claim: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionBoundaryReport {
    pub boundary_id: &'static str,
    pub operation: &'static str,
    pub executor: BoundaryExecutor,
    pub input_kind: MediaLocationKind,
    pub output_artifact: &'static str,
    pub determinism: DeterminismLevel,
    pub materialization_boundary: bool,
    pub shardloom_native_execution: bool,
    pub fallback_attempted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCallBoundaryReport {
    pub boundary_id: &'static str,
    pub model_kind: &'static str,
    pub task: &'static str,
    pub executor: BoundaryExecutor,
    pub prompt_template_hash_required: bool,
    pub token_budget_required: bool,
    pub cost_accounting_required: bool,
    pub human_review_policy_required: bool,
    pub output_validation_schema_required: bool,
    pub external_effect: bool,
    pub shardloom_native_execution: bool,
    pub fallback_attempted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingBoundaryReport {
    pub boundary_id: &'static str,
    pub model_ref_required: bool,
    pub model_version_required: bool,
    pub input_hash_required: bool,
    pub output_embedding_table_required: bool,
    pub executor: BoundaryExecutor,
    pub external_effect: bool,
    pub shardloom_native_execution: bool,
    pub fallback_attempted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundryMediaBoundaryPosture {
    pub surface_id: &'static str,
    pub maturity: UnstructuredMaturity,
    pub supported_handles: Vec<&'static str>,
    pub effect_boundaries: Vec<&'static str>,
    pub execution_owner: BoundaryExecutor,
    pub staging_or_materialization_report_required: bool,
    pub governance_refs_required: bool,
    pub fallback_attempted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundryAipLogicBoundaryReport {
    pub boundary_id: &'static str,
    pub exposed_resources: Vec<&'static str>,
    pub default_tools: Vec<&'static str>,
    pub execute_write_cancel_default: &'static str,
    pub policy_required_for_effects: bool,
    pub shardloom_native_execution: bool,
    pub fallback_attempted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnstructuredWorkflowCertificate {
    pub certificate_id: &'static str,
    pub maturity: UnstructuredMaturity,
    pub input_media_manifest_ref: &'static str,
    pub boundary_refs: Vec<&'static str>,
    pub structured_output_refs: Vec<&'static str>,
    pub redaction_policy_ref: &'static str,
    pub cost_effect_policy_ref: &'static str,
    pub downstream_analytics_status: &'static str,
    pub vector_execution_status: &'static str,
    pub fallback_attempted: bool,
}

impl UnstructuredWorkflowCertificate {
    #[must_use]
    pub fn boundary_complete(&self) -> bool {
        !self.boundary_refs.is_empty()
            && !self.structured_output_refs.is_empty()
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn keeps_vector_execution_separate(&self) -> bool {
        self.vector_execution_status == "separately_certified_extension_type_work"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnstructuredWorkflowBoundaryReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub media_refs: Vec<MediaRef>,
    pub media_manifest: MediaManifest,
    pub text_chunk_table: TextChunkTable,
    pub embedding_table: EmbeddingTable,
    pub extraction_boundaries: Vec<ExtractionBoundaryReport>,
    pub model_call_boundaries: Vec<ModelCallBoundaryReport>,
    pub embedding_boundaries: Vec<EmbeddingBoundaryReport>,
    pub foundry_media_posture: Vec<FoundryMediaBoundaryPosture>,
    pub foundry_aip_logic_boundary: FoundryAipLogicBoundaryReport,
    pub foundry_unstructured_surface_names: Vec<&'static str>,
    pub certificate: UnstructuredWorkflowCertificate,
    pub pipeline_owned_operations: Vec<&'static str>,
    pub shardloom_owned_operations: Vec<&'static str>,
    pub prohibited_core_operations: Vec<&'static str>,
    pub fallback_attempted: bool,
}

impl UnstructuredWorkflowBoundaryReport {
    #[must_use]
    pub fn planned() -> Self {
        Self {
            schema_version: "shardloom.unstructured_workflow_boundaries.v1",
            report_id: "cg21p_1_unstructured_media_model_boundaries",
            media_refs: vec![
                MediaRef {
                    media_ref_id: "media_ref.foundry_document",
                    media_kind: MediaKind::Document,
                    location_kind: MediaLocationKind::FoundryMediaSet,
                    locator_ref: "foundry_media_set_rid_or_path",
                    mime_type: "application/pdf",
                    checksum_status: "required_if_available",
                    access_policy_ref: "foundry_governance_ref",
                    extraction_status: "not_performed_by_shardloom_core",
                },
                MediaRef {
                    media_ref_id: "media_ref.virtual_media",
                    media_kind: MediaKind::Image,
                    location_kind: MediaLocationKind::FoundryVirtualMediaSet,
                    locator_ref: "foundry_virtual_media_set_ref",
                    mime_type: "image/*",
                    checksum_status: "external_handle_limited",
                    access_policy_ref: "foundry_external_source_policy",
                    extraction_status: "governed_external_handle",
                },
            ],
            media_manifest: MediaManifest {
                manifest_id: "media_manifest.v1",
                source_system: "foundry_or_external_media_source",
                media_ref_count: 2,
                virtual_or_external_status: "explicit_handle_not_native_execution",
                update_detection_policy: "recorded_if_platform_exposes_versions",
                known_limitations: vec![
                    "virtual media sets may have limited update/delete visibility",
                    "transformed media outputs may persist in platform backing storage",
                    "media bytes are not loaded by ShardLoom core by default",
                ],
            },
            text_chunk_table: TextChunkTable {
                table_id: "text_chunk_table.v1",
                required_columns: vec![
                    "document_id",
                    "chunk_id",
                    "text",
                    "start_offset",
                    "end_offset",
                    "page_number",
                    "section",
                    "extraction_method",
                    "extraction_version",
                    "confidence",
                    "provenance_ref",
                    "redaction_status",
                ],
                provenance_required: true,
                confidence_required: true,
                redaction_status_required: true,
            },
            embedding_table: EmbeddingTable {
                table_id: "embedding_table.v1",
                required_columns: vec![
                    "entity_id",
                    "document_id",
                    "chunk_id",
                    "embedding_model",
                    "model_version",
                    "vector",
                    "dimension",
                    "normalization",
                    "created_at",
                    "input_hash",
                    "redaction_policy",
                    "provider_boundary",
                ],
                model_version_required: true,
                vector_dimension_required: true,
                input_hash_required: true,
                vector_execution_claim: "not_claimed_by_boundary_contract",
            },
            extraction_boundaries: vec![
                ExtractionBoundaryReport {
                    boundary_id: "boundary.media_extraction.ocr",
                    operation: "ocr_or_document_text_extraction",
                    executor: BoundaryExecutor::FoundryMediaTransform,
                    input_kind: MediaLocationKind::FoundryMediaSet,
                    output_artifact: "text_chunk_table",
                    determinism: DeterminismLevel::ProviderDeclared,
                    materialization_boundary: true,
                    shardloom_native_execution: false,
                    fallback_attempted: false,
                },
                ExtractionBoundaryReport {
                    boundary_id: "boundary.chunking",
                    operation: "chunk_text",
                    executor: BoundaryExecutor::PipelineCode,
                    input_kind: MediaLocationKind::DatasetPath,
                    output_artifact: "text_chunk_table",
                    determinism: DeterminismLevel::Deterministic,
                    materialization_boundary: true,
                    shardloom_native_execution: false,
                    fallback_attempted: false,
                },
            ],
            model_call_boundaries: vec![ModelCallBoundaryReport {
                boundary_id: "boundary.model_call.llm_classification",
                model_kind: "llm",
                task: "classify_or_extract_structured_fields",
                executor: BoundaryExecutor::FoundryModelService,
                prompt_template_hash_required: true,
                token_budget_required: true,
                cost_accounting_required: true,
                human_review_policy_required: true,
                output_validation_schema_required: true,
                external_effect: true,
                shardloom_native_execution: false,
                fallback_attempted: false,
            }],
            embedding_boundaries: vec![EmbeddingBoundaryReport {
                boundary_id: "boundary.embedding_generation",
                model_ref_required: true,
                model_version_required: true,
                input_hash_required: true,
                output_embedding_table_required: true,
                executor: BoundaryExecutor::FoundryModelService,
                external_effect: true,
                shardloom_native_execution: false,
                fallback_attempted: false,
            }],
            foundry_media_posture: vec![
                FoundryMediaBoundaryPosture {
                    surface_id: "FoundryMediaSetSource",
                    maturity: UnstructuredMaturity::U1MediaReferenceDiscovery,
                    supported_handles: vec!["media_set_rid", "media_item_ref", "mime_type"],
                    effect_boundaries: vec![
                        "FoundryMediaExtractionBoundaryReport",
                        "FoundryModelCallBoundaryReport",
                        "FoundryEmbeddingBoundaryReport",
                    ],
                    execution_owner: BoundaryExecutor::FoundryMediaTransform,
                    staging_or_materialization_report_required: true,
                    governance_refs_required: true,
                    fallback_attempted: false,
                },
                FoundryMediaBoundaryPosture {
                    surface_id: "FoundryVirtualMediaSetSource",
                    maturity: UnstructuredMaturity::U1MediaReferenceDiscovery,
                    supported_handles: vec!["virtual_media_set_ref", "external_source_ref"],
                    effect_boundaries: vec!["FoundryMediaExtractionBoundaryReport"],
                    execution_owner: BoundaryExecutor::FoundryMediaTransform,
                    staging_or_materialization_report_required: true,
                    governance_refs_required: true,
                    fallback_attempted: false,
                },
                FoundryMediaBoundaryPosture {
                    surface_id: "FoundryMediaSetSink",
                    maturity: UnstructuredMaturity::U0DeclaredOnly,
                    supported_handles: vec!["media_set_output_ref", "certificate_sidecar"],
                    effect_boundaries: vec!["FoundryMediaMaterializationBoundaryReport"],
                    execution_owner: BoundaryExecutor::PipelineCode,
                    staging_or_materialization_report_required: true,
                    governance_refs_required: true,
                    fallback_attempted: false,
                },
            ],
            foundry_aip_logic_boundary: FoundryAipLogicBoundaryReport {
                boundary_id: "boundary.foundry_aip_logic",
                exposed_resources: vec![
                    "capability_snapshots",
                    "certificates",
                    "unsupported_diagnostics",
                    "benchmark_summaries",
                ],
                default_tools: vec!["inspect_capabilities", "validate_plan", "explain_plan"],
                execute_write_cancel_default: "disabled_until_explicit_policy",
                policy_required_for_effects: true,
                shardloom_native_execution: false,
                fallback_attempted: false,
            },
            foundry_unstructured_surface_names: vec![
                "FoundryMediaSetSource",
                "FoundryVirtualMediaSetSource",
                "FoundryMediaSetSink",
                "FoundryMediaExtractionBoundaryReport",
                "FoundryModelCallBoundaryReport",
                "FoundryEmbeddingBoundaryReport",
                "FoundryAipLogicBoundaryReport",
                "FoundryUnstructuredWorkflowCertificate",
            ],
            certificate: UnstructuredWorkflowCertificate {
                certificate_id: "unstructured_workflow_certificate.v1",
                maturity: UnstructuredMaturity::U4EmbeddingOrModelBoundaryRecorded,
                input_media_manifest_ref: "media_manifest.v1",
                boundary_refs: vec![
                    "boundary.media_extraction.ocr",
                    "boundary.chunking",
                    "boundary.model_call.llm_classification",
                    "boundary.embedding_generation",
                ],
                structured_output_refs: vec!["text_chunk_table.v1", "embedding_table.v1"],
                redaction_policy_ref: "required",
                cost_effect_policy_ref: "required_for_model_boundaries",
                downstream_analytics_status: "certified_only_after_structured_outputs_execute",
                vector_execution_status: "separately_certified_extension_type_work",
                fallback_attempted: false,
            },
            pipeline_owned_operations: vec![
                "ocr",
                "transcription",
                "media_conversion",
                "embedding_generation",
                "llm_call",
                "model_inference",
                "prompt_handling",
                "provider_retry_rate_limit",
                "human_review",
                "ontology_edit",
            ],
            shardloom_owned_operations: vec![
                "structured_output_validation",
                "joins_filters_aggregates_over_extracted_outputs",
                "certificate_emission",
                "lineage_and_policy_reporting",
                "downstream_structured_analytics_when_capability_certified",
            ],
            prohibited_core_operations: vec![
                "silent_ocr",
                "silent_transcription",
                "silent_media_decode",
                "silent_embedding_generation",
                "silent_llm_call",
                "silent_model_inference",
                "silent_ontology_edit",
            ],
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn media_model_effects_are_external_boundaries(&self) -> bool {
        self.extraction_boundaries
            .iter()
            .all(|boundary| !boundary.shardloom_native_execution && !boundary.fallback_attempted)
            && self.model_call_boundaries.iter().all(|boundary| {
                boundary.external_effect
                    && !boundary.shardloom_native_execution
                    && !boundary.fallback_attempted
            })
            && self.embedding_boundaries.iter().all(|boundary| {
                boundary.external_effect
                    && !boundary.shardloom_native_execution
                    && !boundary.fallback_attempted
            })
    }

    #[must_use]
    pub fn foundry_posture_is_report_first(&self) -> bool {
        self.foundry_media_posture.iter().all(|posture| {
            posture.staging_or_materialization_report_required
                && posture.governance_refs_required
                && !posture.fallback_attempted
        }) && self.foundry_aip_logic_boundary.policy_required_for_effects
            && !self.foundry_aip_logic_boundary.shardloom_native_execution
            && !self.foundry_aip_logic_boundary.fallback_attempted
    }

    #[must_use]
    pub fn covers_rfc0036_foundry_unstructured_surfaces(&self) -> bool {
        [
            "FoundryMediaSetSource",
            "FoundryVirtualMediaSetSource",
            "FoundryMediaSetSink",
            "FoundryMediaExtractionBoundaryReport",
            "FoundryModelCallBoundaryReport",
            "FoundryEmbeddingBoundaryReport",
            "FoundryAipLogicBoundaryReport",
            "FoundryUnstructuredWorkflowCertificate",
        ]
        .into_iter()
        .all(|surface| self.foundry_unstructured_surface_names.contains(&surface))
    }

    #[must_use]
    pub fn no_silent_core_model_or_media_runtime(&self) -> bool {
        [
            "silent_ocr",
            "silent_transcription",
            "silent_media_decode",
            "silent_embedding_generation",
            "silent_llm_call",
            "silent_model_inference",
        ]
        .into_iter()
        .all(|operation| self.prohibited_core_operations.contains(&operation))
    }
}

#[must_use]
pub fn plan_unstructured_workflow_boundaries() -> UnstructuredWorkflowBoundaryReport {
    UnstructuredWorkflowBoundaryReport::planned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unstructured_boundary_report_contains_media_chunk_and_embedding_contracts() {
        let report = plan_unstructured_workflow_boundaries();

        assert_eq!(
            report.schema_version,
            "shardloom.unstructured_workflow_boundaries.v1"
        );
        assert_eq!(report.media_refs.len(), 2);
        assert!(
            report
                .text_chunk_table
                .required_columns
                .contains(&"provenance_ref")
        );
        assert!(
            report
                .embedding_table
                .required_columns
                .contains(&"provider_boundary")
        );
        assert_eq!(
            report.embedding_table.vector_execution_claim,
            "not_claimed_by_boundary_contract"
        );
    }

    #[test]
    fn media_and_model_work_remains_explicit_external_boundary_work() {
        let report = plan_unstructured_workflow_boundaries();

        assert!(report.media_model_effects_are_external_boundaries());
        assert!(report.no_silent_core_model_or_media_runtime());
        assert!(report.pipeline_owned_operations.contains(&"llm_call"));
        assert!(
            report
                .pipeline_owned_operations
                .contains(&"embedding_generation")
        );
        assert!(
            report
                .shardloom_owned_operations
                .contains(&"structured_output_validation")
        );
    }

    #[test]
    fn foundry_media_and_aip_posture_is_report_first_and_policy_gated() {
        let report = plan_unstructured_workflow_boundaries();

        assert!(report.foundry_posture_is_report_first());
        assert!(report.covers_rfc0036_foundry_unstructured_surfaces());
        for surface in [
            "FoundryMediaSetSource",
            "FoundryVirtualMediaSetSource",
            "FoundryMediaSetSink",
            "FoundryMediaExtractionBoundaryReport",
            "FoundryModelCallBoundaryReport",
            "FoundryEmbeddingBoundaryReport",
            "FoundryAipLogicBoundaryReport",
            "FoundryUnstructuredWorkflowCertificate",
        ] {
            assert!(report.foundry_unstructured_surface_names.contains(&surface));
        }
        assert!(
            report
                .foundry_media_posture
                .iter()
                .any(|posture| posture.surface_id == "FoundryVirtualMediaSetSource")
        );
        assert_eq!(
            report
                .foundry_aip_logic_boundary
                .execute_write_cancel_default,
            "disabled_until_explicit_policy"
        );
    }

    #[test]
    fn unstructured_workflow_certificate_keeps_vector_execution_separate() {
        let report = plan_unstructured_workflow_boundaries();

        assert!(report.certificate.boundary_complete());
        assert!(report.certificate.keeps_vector_execution_separate());
        assert!(!report.fallback_attempted);
        assert!(!report.certificate.fallback_attempted);
    }
}
