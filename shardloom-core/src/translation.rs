//! Translation planning domain skeleton.
//!
//! This module models translation planning and reporting only.
//! It does not perform file writes, IO, or fallback execution.
//! Vortex remains the highest-fidelity native output target.

use crate::{
    DatasetFormat, DatasetUri, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    FallbackStatus,
};

/// Output target kind for translation planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputTargetKind {
    Vortex,
    ArrowIpc,
    Parquet,
    IcebergCompatible,
    DeltaCompatible,
    JsonLines,
    Csv,
    Unknown,
    Extension(String),
}

impl OutputTargetKind {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Vortex => "vortex",
            Self::ArrowIpc => "arrow_ipc",
            Self::Parquet => "parquet",
            Self::IcebergCompatible => "iceberg_compatible",
            Self::DeltaCompatible => "delta_compatible",
            Self::JsonLines => "json_lines",
            Self::Csv => "csv",
            Self::Unknown => "unknown",
            Self::Extension(value) => value,
        }
    }

    /// True only for Vortex-native output.
    #[must_use]
    pub fn is_native_vortex(&self) -> bool {
        matches!(self, Self::Vortex)
    }

    /// True for explicit compatibility export targets.
    #[must_use]
    pub fn is_compatibility_output(&self) -> bool {
        matches!(
            self,
            Self::ArrowIpc
                | Self::Parquet
                | Self::IcebergCompatible
                | Self::DeltaCompatible
                | Self::JsonLines
                | Self::Csv
        )
    }

    #[must_use]
    pub fn from_dataset_format(format: &DatasetFormat) -> Self {
        match format {
            DatasetFormat::Vortex => Self::Vortex,
            DatasetFormat::ArrowIpc => Self::ArrowIpc,
            DatasetFormat::Parquet => Self::Parquet,
            DatasetFormat::IcebergCompatible => Self::IcebergCompatible,
            DatasetFormat::DeltaCompatible => Self::DeltaCompatible,
            DatasetFormat::JsonLines => Self::JsonLines,
            DatasetFormat::Csv => Self::Csv,
            DatasetFormat::Unknown => Self::Unknown,
            DatasetFormat::Extension(value) => Self::Extension(value.clone()),
        }
    }
}

/// Output target address and kind for translation planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputTarget {
    pub uri: DatasetUri,
    pub kind: OutputTargetKind,
}
impl OutputTarget {
    #[must_use]
    pub fn new(uri: DatasetUri, kind: OutputTargetKind) -> Self {
        Self { uri, kind }
    }

    #[must_use]
    pub fn from_uri(uri: DatasetUri) -> Self {
        let lower = uri.as_str().to_ascii_lowercase();
        let delta_ext = std::path::Path::new(&lower)
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("delta"));
        let kind = if lower.contains("/_delta_log") || delta_ext {
            OutputTargetKind::DeltaCompatible
        } else if lower.contains("/metadata/")
            && (lower.ends_with(".metadata.json") || lower.ends_with("/v1.metadata.json"))
        {
            OutputTargetKind::IcebergCompatible
        } else {
            OutputTargetKind::from_dataset_format(&DatasetFormat::infer_from_uri(&uri))
        };
        Self { uri, kind }
    }

    #[must_use]
    pub fn is_native_vortex(&self) -> bool {
        self.kind.is_native_vortex()
    }

    #[must_use]
    pub fn summary(&self) -> String {
        let mode = if self.kind.is_native_vortex() {
            "native vortex"
        } else if self.kind.is_compatibility_output() {
            "compatibility output"
        } else {
            "unsupported output"
        };
        format!(
            "target_uri={} target_kind={} target_mode={mode}",
            self.uri.as_str(),
            self.kind.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FidelityLevel {
    NativeFullFidelity,
    NativePartialFidelity,
    CompatibilityHighFidelity,
    CompatibilityLossyPhysical,
    Unsupported,
}
impl FidelityLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeFullFidelity => "native_full_fidelity",
            Self::NativePartialFidelity => "native_partial_fidelity",
            Self::CompatibilityHighFidelity => "compatibility_high_fidelity",
            Self::CompatibilityLossyPhysical => "compatibility_lossy_physical",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub fn is_native(&self) -> bool {
        matches!(self, Self::NativeFullFidelity | Self::NativePartialFidelity)
    }
    #[must_use]
    pub fn is_lossy(&self) -> bool {
        matches!(self, Self::CompatibilityLossyPhysical | Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataKind {
    LogicalDType,
    Nullability,
    Validity,
    SegmentStatistics,
    Encoding,
    Layout,
    SegmentBoundaries,
    SortHints,
    SnapshotLinkage,
    ManifestLinkage,
    SelectionVector,
    MaterializationState,
}
impl MetadataKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LogicalDType => "logical_dtype",
            Self::Nullability => "nullability",
            Self::Validity => "validity",
            Self::SegmentStatistics => "segment_statistics",
            Self::Encoding => "encoding",
            Self::Layout => "layout",
            Self::SegmentBoundaries => "segment_boundaries",
            Self::SortHints => "sort_hints",
            Self::SnapshotLinkage => "snapshot_linkage",
            Self::ManifestLinkage => "manifest_linkage",
            Self::SelectionVector => "selection_vector",
            Self::MaterializationState => "materialization_state",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataPreservationStatus {
    Preserved,
    PartiallyPreserved,
    Recomputed,
    Dropped,
    NotApplicable,
    Unknown,
}
impl MetadataPreservationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Preserved => "preserved",
            Self::PartiallyPreserved => "partially_preserved",
            Self::Recomputed => "recomputed",
            Self::Dropped => "dropped",
            Self::NotApplicable => "not_applicable",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPreservation {
    pub kind: MetadataKind,
    pub status: MetadataPreservationStatus,
    pub note: Option<String>,
}
impl MetadataPreservation {
    #[must_use]
    pub fn new(kind: MetadataKind, status: MetadataPreservationStatus) -> Self {
        Self {
            kind,
            status,
            note: None,
        }
    }
    #[must_use]
    pub fn with_note(
        kind: MetadataKind,
        status: MetadataPreservationStatus,
        note: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            status,
            note: Some(note.into()),
        }
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        if let Some(note) = &self.note {
            format!("{}: {} ({note})", self.kind.as_str(), self.status.as_str())
        } else {
            format!("{}: {}", self.kind.as_str(), self.status.as_str())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializationRequirement {
    None,
    SelectionOnly,
    Partial { reason: String },
    Full { reason: String },
    Unknown { reason: String },
}
impl MaterializationRequirement {
    #[must_use]
    pub fn requires_materialization(&self) -> bool {
        matches!(
            self,
            Self::Partial { .. } | Self::Full { .. } | Self::Unknown { .. }
        )
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        match self {
            Self::None => "none".to_string(),
            Self::SelectionOnly => "selection_only".to_string(),
            Self::Partial { reason } => format!("partial ({reason})"),
            Self::Full { reason } => format!("full ({reason})"),
            Self::Unknown { reason } => format!("unknown ({reason})"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranslationPlanningStatus {
    Planned,
    NativeOutputPlanned,
    CompatibilityOutputPlanned,
    Unsupported,
}
impl TranslationPlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::NativeOutputPlanned => "native_output_planned",
            Self::CompatibilityOutputPlanned => "compatibility_output_planned",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    NotPlanned,
    BestEffort,
    AtomicRequired,
    AtomicIfAvailable,
}
impl CommitMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotPlanned => "not_planned",
            Self::BestEffort => "best_effort",
            Self::AtomicRequired => "atomic_required",
            Self::AtomicIfAvailable => "atomic_if_available",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TranslationPlan {
    pub target: OutputTarget,
    pub fidelity: FidelityLevel,
    pub materialization: MaterializationRequirement,
    pub metadata: Vec<MetadataPreservation>,
    pub commit_mode: CommitMode,
    pub status: TranslationPlanningStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl TranslationPlan {
    #[must_use]
    pub fn for_target(target: OutputTarget) -> Self {
        match target.kind {
            OutputTargetKind::Vortex => Self::native_vortex(target),
            OutputTargetKind::ArrowIpc
            | OutputTargetKind::Parquet
            | OutputTargetKind::IcebergCompatible
            | OutputTargetKind::DeltaCompatible
            | OutputTargetKind::JsonLines
            | OutputTargetKind::Csv => Self::compatibility_output(target),
            OutputTargetKind::Unknown | OutputTargetKind::Extension(_) => {
                let target_kind = target.kind.as_str().to_string();
                Self::unsupported(
                    target,
                    "translation_output_target",
                    format!("Output target kind '{target_kind}' is not supported yet."),
                )
            }
        }
    }
    #[must_use]
    pub fn native_vortex(target: OutputTarget) -> Self {
        Self {
            target,
            fidelity: FidelityLevel::NativeFullFidelity,
            materialization: MaterializationRequirement::SelectionOnly,
            metadata: vec![
                MetadataPreservation::new(
                    MetadataKind::LogicalDType,
                    MetadataPreservationStatus::Preserved,
                ),
                MetadataPreservation::new(
                    MetadataKind::Nullability,
                    MetadataPreservationStatus::Preserved,
                ),
                MetadataPreservation::new(
                    MetadataKind::Validity,
                    MetadataPreservationStatus::Preserved,
                ),
                MetadataPreservation::new(
                    MetadataKind::SegmentStatistics,
                    MetadataPreservationStatus::Preserved,
                ),
                MetadataPreservation::new(
                    MetadataKind::Encoding,
                    MetadataPreservationStatus::Preserved,
                ),
                MetadataPreservation::new(
                    MetadataKind::Layout,
                    MetadataPreservationStatus::Preserved,
                ),
                MetadataPreservation::new(
                    MetadataKind::SegmentBoundaries,
                    MetadataPreservationStatus::Preserved,
                ),
            ],
            commit_mode: CommitMode::NotPlanned,
            status: TranslationPlanningStatus::NativeOutputPlanned,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn compatibility_output(target: OutputTarget) -> Self {
        let mut plan = Self {
            target,
            fidelity: FidelityLevel::CompatibilityLossyPhysical,
            materialization: MaterializationRequirement::Partial {
                reason:
                    "Compatibility export may require decoding physical Vortex-specific encodings."
                        .to_string(),
            },
            metadata: vec![
                MetadataPreservation::new(
                    MetadataKind::LogicalDType,
                    MetadataPreservationStatus::Preserved,
                ),
                MetadataPreservation::new(
                    MetadataKind::Nullability,
                    MetadataPreservationStatus::PartiallyPreserved,
                ),
                MetadataPreservation::new(
                    MetadataKind::Encoding,
                    MetadataPreservationStatus::Dropped,
                ),
                MetadataPreservation::new(
                    MetadataKind::Layout,
                    MetadataPreservationStatus::Dropped,
                ),
                MetadataPreservation::new(
                    MetadataKind::SegmentBoundaries,
                    MetadataPreservationStatus::Dropped,
                ),
                MetadataPreservation::new(
                    MetadataKind::SegmentStatistics,
                    MetadataPreservationStatus::PartiallyPreserved,
                ),
            ],
            commit_mode: CommitMode::NotPlanned,
            status: TranslationPlanningStatus::CompatibilityOutputPlanned,
            diagnostics: vec![],
        };
        plan.add_diagnostic(Diagnostic::new(DiagnosticCode::MetadataLoss, DiagnosticSeverity::Warning, DiagnosticCategory::MetadataLoss, "Compatibility output may lose Vortex physical metadata fidelity.", Some("translation_output".to_string()), Some("Compatibility exports preserve logical interoperability but may drop encoding/layout metadata.".to_string()), Some("Use a Vortex target for highest-fidelity native persistence.".to_string()), FallbackStatus::disabled_by_policy()));
        plan
    }
    #[must_use]
    pub fn unsupported(
        target: OutputTarget,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let diagnostic = Diagnostic::new(
            DiagnosticCode::UnsupportedOutputFormat,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            format!(
                "Unsupported translation output target: {}",
                target.kind.as_str()
            ),
            Some(feature),
            Some(reason),
            Some(
                "Select a supported target (for example .vortex or .parquet) and retry."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        );
        Self {
            target,
            fidelity: FidelityLevel::Unsupported,
            materialization: MaterializationRequirement::Unknown {
                reason: "Planning cannot continue for unsupported output targets.".to_string(),
            },
            metadata: vec![],
            commit_mode: CommitMode::NotPlanned,
            status: TranslationPlanningStatus::Unsupported,
            diagnostics: vec![diagnostic],
        }
    }
    pub fn add_metadata_preservation(&mut self, metadata: MetadataPreservation) {
        self.metadata.push(metadata);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let metadata = if self.metadata.is_empty() {
            "none".to_string()
        } else {
            self.metadata
                .iter()
                .map(MetadataPreservation::to_human_text)
                .collect::<Vec<_>>()
                .join(", ")
        };
        let diagnostics = if self.diagnostics.is_empty() {
            "none".to_string()
        } else {
            self.diagnostics
                .iter()
                .map(Diagnostic::to_human_text)
                .collect::<Vec<_>>()
                .join(" | ")
        };
        format!(
            "translation_plan\n{}\nfidelity={}\nmaterialization={}\nstatus={}\ncommit_mode={}\nfallback_execution=disabled\nmetadata=[{}]\ndiagnostics=[{}]",
            self.target.summary(),
            self.fidelity.as_str(),
            self.materialization.to_human_text(),
            self.status.as_str(),
            self.commit_mode.as_str(),
            metadata,
            diagnostics
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TranslationReport {
    pub target: OutputTarget,
    pub fidelity: FidelityLevel,
    pub metadata: Vec<MetadataPreservation>,
    pub materialization: MaterializationRequirement,
    pub diagnostics: Vec<Diagnostic>,
    pub output_files: Vec<String>,
    pub committed: bool,
}
impl TranslationReport {
    #[must_use]
    pub fn from_plan(plan: &TranslationPlan) -> Self {
        Self {
            target: plan.target.clone(),
            fidelity: plan.fidelity,
            metadata: plan.metadata.clone(),
            materialization: plan.materialization.clone(),
            diagnostics: plan.diagnostics.clone(),
            output_files: vec![],
            committed: false,
        }
    }
    pub fn add_output_file(&mut self, output_file: impl Into<String>) {
        self.output_files.push(output_file.into());
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_metadata_loss(&self) -> bool {
        self.metadata.iter().any(|m| {
            matches!(
                m.status,
                MetadataPreservationStatus::Dropped
                    | MetadataPreservationStatus::PartiallyPreserved
            )
        })
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let diagnostics = if self.diagnostics.is_empty() {
            "none".to_string()
        } else {
            self.diagnostics
                .iter()
                .map(Diagnostic::to_human_text)
                .collect::<Vec<_>>()
                .join(" | ")
        };
        format!(
            "translation_report\n{}\nfidelity={}\ncommitted={}\nmaterialization={}\nmetadata_loss={}\ndiagnostics=[{}]",
            self.target.summary(),
            self.fidelity.as_str(),
            self.committed,
            self.materialization.to_human_text(),
            self.has_metadata_loss(),
            diagnostics
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_target_kind_vortex_is_native() {
        assert!(OutputTargetKind::Vortex.is_native_vortex());
    }
    #[test]
    fn output_target_kind_parquet_is_compatibility() {
        assert!(OutputTargetKind::Parquet.is_compatibility_output());
    }
    #[test]
    fn output_target_from_uri_infers_vortex() {
        let target = OutputTarget::from_uri(DatasetUri::new("out.vortex").expect("valid uri"));
        assert_eq!(target.kind, OutputTargetKind::Vortex);
    }
    #[test]
    fn output_target_from_uri_infers_parquet() {
        let target = OutputTarget::from_uri(DatasetUri::new("out.parquet").expect("valid uri"));
        assert_eq!(target.kind, OutputTargetKind::Parquet);
    }

    #[test]
    fn output_target_from_uri_infers_delta_compatible() {
        let target = OutputTarget::from_uri(
            DatasetUri::new("s3://bucket/table/_delta_log/00000000000000000000.json")
                .expect("valid uri"),
        );
        assert_eq!(target.kind, OutputTargetKind::DeltaCompatible);
    }

    #[test]
    fn output_target_from_uri_infers_iceberg_compatible() {
        let target = OutputTarget::from_uri(
            DatasetUri::new("s3://bucket/table/metadata/v1.metadata.json").expect("valid uri"),
        );
        assert_eq!(target.kind, OutputTargetKind::IcebergCompatible);
    }

    #[test]
    fn output_target_from_uri_iceberg_dir_parquet_stays_parquet() {
        let target = OutputTarget::from_uri(
            DatasetUri::new("s3://bucket/iceberg/table/out.parquet").expect("valid uri"),
        );
        assert_eq!(target.kind, OutputTargetKind::Parquet);
    }
    #[test]
    fn native_full_fidelity_is_native() {
        assert!(FidelityLevel::NativeFullFidelity.is_native());
    }
    #[test]
    fn compatibility_lossy_is_lossy() {
        assert!(FidelityLevel::CompatibilityLossyPhysical.is_lossy());
    }
    #[test]
    fn materialization_none_not_required() {
        assert!(!MaterializationRequirement::None.requires_materialization());
    }
    #[test]
    fn materialization_full_required() {
        assert!(
            MaterializationRequirement::Full {
                reason: "decode".into()
            }
            .requires_materialization()
        );
    }
    #[test]
    fn for_target_routes_vortex_native() {
        let p = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("out.vortex").expect("valid uri"),
        ));
        assert_eq!(p.status, TranslationPlanningStatus::NativeOutputPlanned);
    }
    #[test]
    fn for_target_routes_parquet_compatibility() {
        let p = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("out.parquet").expect("valid uri"),
        ));
        assert_eq!(
            p.status,
            TranslationPlanningStatus::CompatibilityOutputPlanned
        );
    }
    #[test]
    fn for_target_routes_unknown_unsupported() {
        let p = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("out.unknown").expect("valid uri"),
        ));
        assert_eq!(p.status, TranslationPlanningStatus::Unsupported);
    }
    #[test]
    fn native_vortex_plan_has_native_full_fidelity() {
        let p = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("out.vortex").expect("valid uri"),
        ));
        assert_eq!(p.fidelity, FidelityLevel::NativeFullFidelity);
    }
    #[test]
    fn parquet_plan_has_metadata_loss() {
        let p = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("out.parquet").expect("valid uri"),
        ));
        let report = TranslationReport::from_plan(&p);
        assert!(report.has_metadata_loss());
    }
    #[test]
    fn unsupported_plan_has_errors() {
        let p = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("out.unknown").expect("valid uri"),
        ));
        assert!(p.has_errors());
    }
    #[test]
    fn report_from_plan_preserves_target_and_fidelity() {
        let p = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("out.vortex").expect("valid uri"),
        ));
        let r = TranslationReport::from_plan(&p);
        assert_eq!(r.target, p.target);
        assert_eq!(r.fidelity, p.fidelity);
    }
    #[test]
    fn report_has_metadata_loss_works() {
        let mut r = TranslationReport::from_plan(&TranslationPlan::for_target(
            OutputTarget::from_uri(DatasetUri::new("out.vortex").expect("valid uri")),
        ));
        assert!(!r.has_metadata_loss());
        r.metadata.push(MetadataPreservation::new(
            MetadataKind::Layout,
            MetadataPreservationStatus::Dropped,
        ));
        assert!(r.has_metadata_loss());
    }
    #[test]
    fn human_text_includes_fallback_execution_disabled() {
        let p = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("out.vortex").expect("valid uri"),
        ));
        assert!(p.to_human_text().contains("fallback_execution=disabled"));
    }
}
