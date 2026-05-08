//! Native dataset manifest, snapshot, and incremental planning skeleton types.
//!
//! These domain types model planning metadata only. They do not parse persisted
//! manifest files, perform Vortex IO, or execute object-store operations.
//! Unsupported behavior must fail explicitly with deterministic diagnostics, and
//! fallback execution remains disabled by policy.

use std::collections::BTreeSet;

use crate::{
    CommitMode, DatasetFormat, DatasetRef, DatasetUri, Diagnostic, DiagnosticCategory,
    DiagnosticCode, DiagnosticSeverity, EncodedSegment, FallbackStatus, ManifestId, OutputTarget,
    OutputTargetKind, Result, SegmentId, ShardLoomError, SnapshotId,
};

/// Manifest schema version for planning metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestVersion {
    V1,
    Unknown,
    Extension(String),
}

impl ManifestVersion {
    /// Stable version string for explain and diagnostics output.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::V1 => "v1",
            Self::Unknown => "unknown",
            Self::Extension(_) => "extension",
        }
    }
}

/// Snapshot linkage for immutable planning units.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    pub id: SnapshotId,
    pub parent: Option<SnapshotId>,
}
impl SnapshotRef {
    #[must_use]
    pub fn new(id: SnapshotId) -> Self {
        Self { id, parent: None }
    }
    #[must_use]
    pub fn with_parent(mut self, parent: SnapshotId) -> Self {
        self.parent = Some(parent);
        self
    }
    #[must_use]
    pub fn has_parent(&self) -> bool {
        self.parent.is_some()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "snapshot={} has_parent={}",
            self.id.as_str(),
            self.has_parent()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileRole {
    NativeVortexData,
    Manifest,
    TemporaryOutput,
    CompatibilityOutput,
    CommitRecord,
    Unknown,
}
impl FileRole {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeVortexData => "native_vortex_data",
            Self::Manifest => "manifest",
            Self::TemporaryOutput => "temporary_output",
            Self::CompatibilityOutput => "compatibility_output",
            Self::CommitRecord => "commit_record",
            Self::Unknown => "unknown",
        }
    }
}

/// Planned file metadata descriptor used by manifests and commit records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDescriptor {
    pub uri: DatasetUri,
    pub format: DatasetFormat,
    pub role: FileRole,
    pub size_bytes: Option<u64>,
}
impl FileDescriptor {
    #[must_use]
    pub fn new(uri: DatasetUri, format: DatasetFormat, role: FileRole) -> Self {
        Self {
            uri,
            format,
            role,
            size_bytes: None,
        }
    }
    #[must_use]
    pub fn from_uri(uri: DatasetUri, role: FileRole) -> Self {
        let format = DatasetFormat::infer_from_uri(&uri);
        Self::new(uri, format, role)
    }
    #[must_use]
    pub fn with_size_bytes(mut self, size_bytes: u64) -> Self {
        self.size_bytes = Some(size_bytes);
        self
    }
    #[must_use]
    pub fn is_native_vortex_data(&self) -> bool {
        self.role == FileRole::NativeVortexData && self.format.is_native_vortex()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "uri={} format={} role={} size_bytes={:?} native_vortex_data={}",
            self.uri.as_str(),
            self.format.as_str(),
            self.role.as_str(),
            self.size_bytes,
            self.is_native_vortex_data()
        )
    }
}

/// Segment membership entry inside a planning manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestSegment {
    pub segment: EncodedSegment,
    pub file: FileDescriptor,
    pub snapshot: Option<SnapshotId>,
}
impl ManifestSegment {
    #[must_use]
    pub fn new(segment: EncodedSegment, file: FileDescriptor) -> Self {
        Self {
            segment,
            file,
            snapshot: None,
        }
    }
    #[must_use]
    pub fn with_snapshot(mut self, snapshot: SnapshotId) -> Self {
        self.snapshot = Some(snapshot);
        self
    }
    #[must_use]
    pub fn can_use_metadata(&self) -> bool {
        self.segment.can_use_metadata()
    }
    #[must_use]
    pub fn has_byte_ranges(&self) -> bool {
        self.segment.has_byte_ranges()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "segment={} snapshot={} metadata_capable={} has_byte_ranges={} file_role={}",
            self.segment.id.as_str(),
            self.snapshot.as_ref().map_or("none", SnapshotId::as_str),
            self.can_use_metadata(),
            self.has_byte_ranges(),
            self.file.role.as_str()
        )
    }
}

/// Dataset manifest planning model (not persisted manifest parsing).
#[derive(Debug, Clone, PartialEq)]
pub struct DatasetManifest {
    pub id: ManifestId,
    pub version: ManifestVersion,
    pub dataset: DatasetRef,
    pub snapshot: SnapshotRef,
    pub files: Vec<FileDescriptor>,
    pub segments: Vec<ManifestSegment>,
    pub diagnostics: Vec<Diagnostic>,
}
impl DatasetManifest {
    #[must_use]
    pub fn new(id: ManifestId, dataset: DatasetRef, snapshot: SnapshotRef) -> Self {
        Self {
            id,
            version: ManifestVersion::V1,
            dataset,
            snapshot,
            files: Vec::new(),
            segments: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    pub fn add_file(&mut self, file: FileDescriptor) {
        self.files.push(file);
    }
    pub fn add_segment(&mut self, segment: ManifestSegment) {
        self.segments.push(segment);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
    #[must_use]
    pub fn native_vortex_file_count(&self) -> usize {
        self.files
            .iter()
            .filter(|f| f.is_native_vortex_data())
            .count()
    }
    #[must_use]
    pub fn segments_with_metadata_count(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| s.can_use_metadata())
            .count()
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
    pub fn summary(&self) -> String {
        format!(
            "manifest={} dataset={} snapshot={} files={} segments={} native_vortex_files={} metadata_capable_segments={} fallback_execution=disabled",
            self.id.as_str(),
            self.dataset.id.as_str(),
            self.snapshot.id.as_str(),
            self.file_count(),
            self.segment_count(),
            self.native_vortex_file_count(),
            self.segments_with_metadata_count()
        )
    }
}

/// Heuristic thresholds for report-only layout-health planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutHealthPolicy {
    pub small_file_threshold_bytes: u64,
    pub small_segment_row_threshold: u64,
    pub small_segment_size_threshold_bytes: u64,
    pub target_segment_row_count: u64,
}

impl Default for LayoutHealthPolicy {
    fn default() -> Self {
        Self {
            small_file_threshold_bytes: 16 * 1024 * 1024,
            small_segment_row_threshold: 1_000,
            small_segment_size_threshold_bytes: 1024 * 1024,
            target_segment_row_count: 64_000,
        }
    }
}

/// Overall layout-health planning status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutHealthStatus {
    Healthy,
    NeedsAttention,
    CompactionRecommended,
    Unsupported,
}

impl LayoutHealthStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::NeedsAttention => "needs_attention",
            Self::CompactionRecommended => "compaction_recommended",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Machine-readable issue family for layout-health reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutHealthIssueKind {
    EmptyManifest,
    SmallFiles,
    SmallSegments,
    MissingStatistics,
    MissingByteRanges,
    MixedFormats,
    MixedEncodings,
    MixedLayouts,
    NonNativeDataFiles,
}

impl LayoutHealthIssueKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EmptyManifest => "empty_manifest",
            Self::SmallFiles => "small_files",
            Self::SmallSegments => "small_segments",
            Self::MissingStatistics => "missing_statistics",
            Self::MissingByteRanges => "missing_byte_ranges",
            Self::MixedFormats => "mixed_formats",
            Self::MixedEncodings => "mixed_encodings",
            Self::MixedLayouts => "mixed_layouts",
            Self::NonNativeDataFiles => "non_native_data_files",
        }
    }
}

/// Counted layout-health issue emitted by report-only planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayoutHealthIssue {
    pub kind: LayoutHealthIssueKind,
    pub affected_count: usize,
    pub message: String,
}

impl LayoutHealthIssue {
    #[must_use]
    pub fn new(
        kind: LayoutHealthIssueKind,
        affected_count: usize,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            affected_count,
            message: message.into(),
        }
    }
}

/// Machine-readable CG-9 layout-health planning evidence.
///
/// This report evaluates already-declared manifest/file/segment metadata only. It does not read
/// table metadata, inspect files, run compaction, write data, contact catalogs, or attempt fallback execution.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct LayoutHealthReport {
    pub manifest: DatasetManifest,
    pub policy: LayoutHealthPolicy,
    pub status: LayoutHealthStatus,
    pub issues: Vec<LayoutHealthIssue>,
    pub diagnostics: Vec<Diagnostic>,
    pub file_count: usize,
    pub segment_count: usize,
    pub native_vortex_file_count: usize,
    pub non_native_data_file_count: usize,
    pub small_file_count: usize,
    pub small_segment_count: usize,
    pub missing_statistics_segment_count: usize,
    pub missing_byte_range_segment_count: usize,
    pub unique_format_count: usize,
    pub unique_encoding_count: usize,
    pub unique_layout_count: usize,
    pub compaction_candidate_count: usize,
    pub requires_statistics_refresh: bool,
    pub requires_byte_range_index: bool,
    pub requires_layout_review: bool,
    pub recommends_compaction: bool,
    pub can_plan_without_io: bool,
    pub data_read: bool,
    pub write_io: bool,
    pub catalog_io: bool,
    pub object_store_io: bool,
    pub compaction_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
}

impl LayoutHealthReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.fallback_execution_allowed
            || self.compaction_execution_allowed
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.data_read
            && !self.write_io
            && !self.catalog_io
            && !self.object_store_io
            && !self.compaction_execution_allowed
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "layout_health(status={}, files={}, segments={}, small_files={}, small_segments={}, missing_stats={}, missing_byte_ranges={}, compaction_candidates={}, data_read=false, write_io=false, catalog_io=false, object_store_io=false, compaction_execution=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.file_count,
            self.segment_count,
            self.small_file_count,
            self.small_segment_count,
            self.missing_statistics_segment_count,
            self.missing_byte_range_segment_count,
            self.compaction_candidate_count
        )
    }
}

/// Evaluates manifest layout health without performing table, file, object-store, or catalog IO.
#[must_use]
pub fn evaluate_layout_health(
    manifest: DatasetManifest,
    policy: LayoutHealthPolicy,
) -> LayoutHealthReport {
    let counts = LayoutHealthCounts::from_manifest(&manifest, policy);
    let issues = layout_health_issues(counts);
    let diagnostics = layout_health_diagnostics(&issues);
    let status = layout_health_status(counts);

    LayoutHealthReport {
        file_count: counts.files,
        segment_count: counts.segments,
        native_vortex_file_count: counts.native_vortex_files,
        non_native_data_file_count: counts.non_native_data_files,
        small_file_count: counts.small_files,
        small_segment_count: counts.small_segments,
        missing_statistics_segment_count: counts.missing_statistics_segments,
        missing_byte_range_segment_count: counts.missing_byte_range_segments,
        unique_format_count: counts.unique_formats,
        unique_encoding_count: counts.unique_encodings,
        unique_layout_count: counts.unique_layouts,
        compaction_candidate_count: counts.small_files + counts.small_segments,
        requires_statistics_refresh: counts.missing_statistics_segments > 0,
        requires_byte_range_index: counts.missing_byte_range_segments > 0,
        requires_layout_review: counts.unique_formats > 1
            || counts.unique_encodings > 1
            || counts.unique_layouts > 1
            || counts.non_native_data_files > 0,
        recommends_compaction: counts.small_files > 0 || counts.small_segments > 0,
        can_plan_without_io: true,
        data_read: false,
        write_io: false,
        catalog_io: false,
        object_store_io: false,
        compaction_execution_allowed: false,
        fallback_execution_allowed: false,
        manifest,
        policy,
        status,
        issues,
        diagnostics,
    }
}

#[derive(Debug, Clone, Copy)]
struct LayoutHealthCounts {
    files: usize,
    segments: usize,
    native_vortex_files: usize,
    non_native_data_files: usize,
    small_files: usize,
    small_segments: usize,
    missing_statistics_segments: usize,
    missing_byte_range_segments: usize,
    unique_formats: usize,
    unique_encodings: usize,
    unique_layouts: usize,
}

impl LayoutHealthCounts {
    fn from_manifest(manifest: &DatasetManifest, policy: LayoutHealthPolicy) -> Self {
        Self {
            files: manifest.file_count(),
            segments: manifest.segment_count(),
            native_vortex_files: manifest.native_vortex_file_count(),
            non_native_data_files: layout_health_non_native_data_files(manifest),
            small_files: layout_health_small_file_count(manifest, policy),
            small_segments: layout_health_small_segment_count(manifest, policy),
            missing_statistics_segments: manifest
                .segments
                .iter()
                .filter(|segment| !segment.can_use_metadata())
                .count(),
            missing_byte_range_segments: manifest
                .segments
                .iter()
                .filter(|segment| !segment.has_byte_ranges())
                .count(),
            unique_formats: unique_file_format_count(manifest),
            unique_encodings: unique_segment_encoding_count(manifest),
            unique_layouts: unique_segment_layout_count(manifest),
        }
    }
}

fn layout_health_status(counts: LayoutHealthCounts) -> LayoutHealthStatus {
    if counts.segments == 0 {
        LayoutHealthStatus::Unsupported
    } else if counts.small_files > 0 || counts.small_segments > 0 {
        LayoutHealthStatus::CompactionRecommended
    } else if counts.missing_statistics_segments > 0
        || counts.missing_byte_range_segments > 0
        || counts.unique_formats > 1
        || counts.unique_encodings > 1
        || counts.unique_layouts > 1
        || counts.non_native_data_files > 0
    {
        LayoutHealthStatus::NeedsAttention
    } else {
        LayoutHealthStatus::Healthy
    }
}

fn layout_health_issues(counts: LayoutHealthCounts) -> Vec<LayoutHealthIssue> {
    let mut issues = Vec::new();
    push_layout_health_issue(
        &mut issues,
        counts.segments == 0,
        LayoutHealthIssueKind::EmptyManifest,
        1,
        "layout health requires declared segment metadata",
    );
    push_layout_health_issue(
        &mut issues,
        counts.small_files > 0,
        LayoutHealthIssueKind::SmallFiles,
        counts.small_files,
        "small files are compaction candidates",
    );
    push_layout_health_issue(
        &mut issues,
        counts.small_segments > 0,
        LayoutHealthIssueKind::SmallSegments,
        counts.small_segments,
        "small segments are compaction candidates",
    );
    push_layout_health_issue(
        &mut issues,
        counts.missing_statistics_segments > 0,
        LayoutHealthIssueKind::MissingStatistics,
        counts.missing_statistics_segments,
        "segments missing statistics limit pruning and incremental planning evidence",
    );
    push_layout_health_issue(
        &mut issues,
        counts.missing_byte_range_segments > 0,
        LayoutHealthIssueKind::MissingByteRanges,
        counts.missing_byte_range_segments,
        "segments missing byte ranges limit object-store range planning evidence",
    );
    push_layout_health_issue(
        &mut issues,
        counts.unique_formats > 1,
        LayoutHealthIssueKind::MixedFormats,
        counts.unique_formats,
        "mixed file formats require compatibility review",
    );
    push_layout_health_issue(
        &mut issues,
        counts.unique_encodings > 1,
        LayoutHealthIssueKind::MixedEncodings,
        counts.unique_encodings,
        "mixed encodings require native kernel capability review",
    );
    push_layout_health_issue(
        &mut issues,
        counts.unique_layouts > 1,
        LayoutHealthIssueKind::MixedLayouts,
        counts.unique_layouts,
        "mixed layouts require native layout capability review",
    );
    push_layout_health_issue(
        &mut issues,
        counts.non_native_data_files > 0,
        LayoutHealthIssueKind::NonNativeDataFiles,
        counts.non_native_data_files,
        "non-native data files require adapter fidelity evidence",
    );
    issues
}

fn push_layout_health_issue(
    issues: &mut Vec<LayoutHealthIssue>,
    present: bool,
    kind: LayoutHealthIssueKind,
    affected_count: usize,
    message: &'static str,
) {
    if present {
        issues.push(LayoutHealthIssue::new(kind, affected_count, message));
    }
}

fn layout_health_diagnostics(issues: &[LayoutHealthIssue]) -> Vec<Diagnostic> {
    issues
        .iter()
        .map(|issue| match issue.kind {
            LayoutHealthIssueKind::EmptyManifest => Diagnostic::invalid_input(
                issue.kind.as_str(),
                "layout health requires at least one declared segment",
                "Attach manifest segment metadata before evaluating layout health.",
            ),
            _ => layout_health_warning(issue),
        })
        .collect()
}

fn layout_health_warning(issue: &LayoutHealthIssue) -> Diagnostic {
    Diagnostic::new(
        layout_health_diagnostic_code(issue.kind),
        DiagnosticSeverity::Warning,
        DiagnosticCategory::Planning,
        issue.message.clone(),
        Some(issue.kind.as_str().to_string()),
        Some("Layout-health planning is report-only and did not inspect storage.".to_string()),
        Some("Use this evidence to schedule future native maintenance planning.".to_string()),
        FallbackStatus::disabled_by_policy(),
    )
}

fn layout_health_diagnostic_code(kind: LayoutHealthIssueKind) -> DiagnosticCode {
    match kind {
        LayoutHealthIssueKind::MissingStatistics => DiagnosticCode::MissingStatistics,
        LayoutHealthIssueKind::NonNativeDataFiles => DiagnosticCode::MetadataLoss,
        _ => DiagnosticCode::ResourceBudgetExceeded,
    }
}

fn layout_health_non_native_data_files(manifest: &DatasetManifest) -> usize {
    manifest
        .files
        .iter()
        .filter(|file| {
            (file.role == FileRole::NativeVortexData && !file.format.is_native_vortex())
                || file.role == FileRole::CompatibilityOutput
        })
        .count()
}

fn layout_health_small_file_count(manifest: &DatasetManifest, policy: LayoutHealthPolicy) -> usize {
    manifest
        .files
        .iter()
        .filter(|file| {
            file.size_bytes
                .is_some_and(|size| size > 0 && size < policy.small_file_threshold_bytes)
        })
        .count()
}

fn layout_health_small_segment_count(
    manifest: &DatasetManifest,
    policy: LayoutHealthPolicy,
) -> usize {
    manifest
        .segments
        .iter()
        .filter(|segment| {
            segment
                .segment
                .stats
                .row_count
                .is_some_and(|rows| rows > 0 && rows < policy.small_segment_row_threshold)
                || segment
                    .segment
                    .layout
                    .physical_size_bytes
                    .is_some_and(|size| {
                        size > 0 && size < policy.small_segment_size_threshold_bytes
                    })
        })
        .count()
}

fn unique_file_format_count(manifest: &DatasetManifest) -> usize {
    manifest
        .files
        .iter()
        .map(|file| file.format.as_str().to_string())
        .collect::<BTreeSet<_>>()
        .len()
}

fn unique_segment_encoding_count(manifest: &DatasetManifest) -> usize {
    manifest
        .segments
        .iter()
        .map(|segment| segment.segment.layout.encoding.as_str().to_string())
        .collect::<BTreeSet<_>>()
        .len()
}

fn unique_segment_layout_count(manifest: &DatasetManifest) -> usize {
    manifest
        .segments
        .iter()
        .map(|segment| segment.segment.layout.layout.as_str().to_string())
        .collect::<BTreeSet<_>>()
        .len()
}

/// Heuristic thresholds for report-only compaction planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionPlanningPolicy {
    pub max_candidates_per_group: usize,
    pub target_file_size_bytes: u64,
    pub target_segment_row_count: u64,
    pub require_native_vortex_inputs: bool,
    pub allow_mixed_layout_groups: bool,
}

impl Default for CompactionPlanningPolicy {
    fn default() -> Self {
        Self {
            max_candidates_per_group: 8,
            target_file_size_bytes: 128 * 1024 * 1024,
            target_segment_row_count: 64_000,
            require_native_vortex_inputs: true,
            allow_mixed_layout_groups: false,
        }
    }
}

/// Overall status for report-only compaction planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPlanningStatus {
    NotNeeded,
    PlanningReady,
    BlockedByMetadata,
    BlockedByLayoutReview,
    Unsupported,
}

impl CompactionPlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotNeeded => "not_needed",
            Self::PlanningReady => "planning_ready",
            Self::BlockedByMetadata => "blocked_by_metadata",
            Self::BlockedByLayoutReview => "blocked_by_layout_review",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Machine-readable action family for future compaction work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPlanningActionKind {
    MergeSmallFiles,
    MergeSmallSegments,
    RefreshStatistics,
    BuildByteRangeIndex,
    ReviewMixedFormats,
    ReviewMixedEncodings,
    ReviewMixedLayouts,
    ReviewNonNativeDataFiles,
    Unsupported,
}

impl CompactionPlanningActionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MergeSmallFiles => "merge_small_files",
            Self::MergeSmallSegments => "merge_small_segments",
            Self::RefreshStatistics => "refresh_statistics",
            Self::BuildByteRangeIndex => "build_byte_range_index",
            Self::ReviewMixedFormats => "review_mixed_formats",
            Self::ReviewMixedEncodings => "review_mixed_encodings",
            Self::ReviewMixedLayouts => "review_mixed_layouts",
            Self::ReviewNonNativeDataFiles => "review_non_native_data_files",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Report-only compaction planning action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlanningAction {
    pub kind: CompactionPlanningActionKind,
    pub affected_count: usize,
    pub message: String,
}

impl CompactionPlanningAction {
    #[must_use]
    pub fn new(
        kind: CompactionPlanningActionKind,
        affected_count: usize,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            affected_count,
            message: message.into(),
        }
    }
}

/// Machine-readable CG-9 compaction planning evidence.
///
/// This report derives recommendations from declared manifest and layout-health metadata only.
/// It does not read table metadata, inspect data files, write compaction outputs, contact object
/// stores or catalogs, execute maintenance, or attempt fallback execution.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CompactionPlanningReport {
    pub layout_health: LayoutHealthReport,
    pub policy: CompactionPlanningPolicy,
    pub status: CompactionPlanningStatus,
    pub actions: Vec<CompactionPlanningAction>,
    pub diagnostics: Vec<Diagnostic>,
    pub file_count: usize,
    pub segment_count: usize,
    pub candidate_file_count: usize,
    pub candidate_segment_count: usize,
    pub candidate_count: usize,
    pub blocked_candidate_count: usize,
    pub estimated_compaction_group_count: usize,
    pub missing_statistics_segment_count: usize,
    pub missing_byte_range_segment_count: usize,
    pub non_native_data_file_count: usize,
    pub requires_statistics_refresh: bool,
    pub requires_byte_range_index: bool,
    pub requires_layout_review: bool,
    pub requires_native_input_review: bool,
    pub compaction_recommended: bool,
    pub recommendation_emitted: bool,
    pub can_plan_without_io: bool,
    pub data_read: bool,
    pub write_io: bool,
    pub catalog_io: bool,
    pub object_store_io: bool,
    pub compaction_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
}

impl CompactionPlanningReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.fallback_execution_allowed
            || self.compaction_execution_allowed
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.data_read
            && !self.write_io
            && !self.catalog_io
            && !self.object_store_io
            && !self.compaction_execution_allowed
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "compaction_plan(status={}, candidates={}, candidate_files={}, candidate_segments={}, estimated_groups={}, data_read=false, write_io=false, catalog_io=false, object_store_io=false, compaction_execution=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.candidate_count,
            self.candidate_file_count,
            self.candidate_segment_count,
            self.estimated_compaction_group_count
        )
    }
}

/// Plans future compaction recommendations without performing table, file, object-store, or catalog IO.
#[must_use]
pub fn evaluate_compaction_planning(
    manifest: DatasetManifest,
    layout_policy: LayoutHealthPolicy,
    policy: CompactionPlanningPolicy,
) -> CompactionPlanningReport {
    let layout_health = evaluate_layout_health(manifest, layout_policy);
    let status = compaction_planning_status(&layout_health, policy);
    let actions = compaction_planning_actions(&layout_health, status);
    let candidate_count = layout_health.compaction_candidate_count;
    let estimated_compaction_group_count =
        estimated_compaction_group_count(candidate_count, status, policy);
    let recommendation_emitted =
        status == CompactionPlanningStatus::PlanningReady && candidate_count > 0;

    CompactionPlanningReport {
        file_count: layout_health.file_count,
        segment_count: layout_health.segment_count,
        candidate_file_count: layout_health.small_file_count,
        candidate_segment_count: layout_health.small_segment_count,
        candidate_count,
        blocked_candidate_count: if recommendation_emitted {
            0
        } else {
            candidate_count
        },
        estimated_compaction_group_count,
        missing_statistics_segment_count: layout_health.missing_statistics_segment_count,
        missing_byte_range_segment_count: layout_health.missing_byte_range_segment_count,
        non_native_data_file_count: layout_health.non_native_data_file_count,
        requires_statistics_refresh: layout_health.requires_statistics_refresh,
        requires_byte_range_index: layout_health.requires_byte_range_index,
        requires_layout_review: layout_health.requires_layout_review,
        requires_native_input_review: policy.require_native_vortex_inputs
            && layout_health.non_native_data_file_count > 0,
        compaction_recommended: layout_health.recommends_compaction,
        recommendation_emitted,
        can_plan_without_io: true,
        data_read: false,
        write_io: false,
        catalog_io: false,
        object_store_io: false,
        compaction_execution_allowed: false,
        fallback_execution_allowed: false,
        diagnostics: layout_health.diagnostics.clone(),
        layout_health,
        policy,
        status,
        actions,
    }
}

fn compaction_planning_status(
    layout_health: &LayoutHealthReport,
    policy: CompactionPlanningPolicy,
) -> CompactionPlanningStatus {
    if layout_health.status == LayoutHealthStatus::Unsupported {
        CompactionPlanningStatus::Unsupported
    } else if layout_health.requires_layout_review
        && (!policy.allow_mixed_layout_groups
            || (policy.require_native_vortex_inputs
                && layout_health.non_native_data_file_count > 0))
    {
        CompactionPlanningStatus::BlockedByLayoutReview
    } else if layout_health.requires_statistics_refresh || layout_health.requires_byte_range_index {
        CompactionPlanningStatus::BlockedByMetadata
    } else if layout_health.recommends_compaction {
        CompactionPlanningStatus::PlanningReady
    } else {
        CompactionPlanningStatus::NotNeeded
    }
}

fn estimated_compaction_group_count(
    candidate_count: usize,
    status: CompactionPlanningStatus,
    policy: CompactionPlanningPolicy,
) -> usize {
    if status != CompactionPlanningStatus::PlanningReady || candidate_count == 0 {
        0
    } else {
        candidate_count.div_ceil(policy.max_candidates_per_group.max(1))
    }
}

fn compaction_planning_actions(
    layout_health: &LayoutHealthReport,
    status: CompactionPlanningStatus,
) -> Vec<CompactionPlanningAction> {
    let mut actions = Vec::new();
    push_compaction_action(
        &mut actions,
        status == CompactionPlanningStatus::Unsupported,
        CompactionPlanningActionKind::Unsupported,
        1,
        "compaction planning requires declared segment metadata",
    );
    push_compaction_action(
        &mut actions,
        layout_health.small_file_count > 0,
        CompactionPlanningActionKind::MergeSmallFiles,
        layout_health.small_file_count,
        "small files are future compaction candidates",
    );
    push_compaction_action(
        &mut actions,
        layout_health.small_segment_count > 0,
        CompactionPlanningActionKind::MergeSmallSegments,
        layout_health.small_segment_count,
        "small segments are future compaction candidates",
    );
    push_compaction_action(
        &mut actions,
        layout_health.requires_statistics_refresh,
        CompactionPlanningActionKind::RefreshStatistics,
        layout_health.missing_statistics_segment_count,
        "statistics must be refreshed before safe compaction grouping",
    );
    push_compaction_action(
        &mut actions,
        layout_health.requires_byte_range_index,
        CompactionPlanningActionKind::BuildByteRangeIndex,
        layout_health.missing_byte_range_segment_count,
        "byte-range evidence is needed before object-store-aware compaction planning",
    );
    push_compaction_action(
        &mut actions,
        layout_health.unique_format_count > 1,
        CompactionPlanningActionKind::ReviewMixedFormats,
        layout_health.unique_format_count,
        "mixed file formats require adapter fidelity review before compaction",
    );
    push_compaction_action(
        &mut actions,
        layout_health.unique_encoding_count > 1,
        CompactionPlanningActionKind::ReviewMixedEncodings,
        layout_health.unique_encoding_count,
        "mixed encodings require native kernel review before compaction",
    );
    push_compaction_action(
        &mut actions,
        layout_health.unique_layout_count > 1,
        CompactionPlanningActionKind::ReviewMixedLayouts,
        layout_health.unique_layout_count,
        "mixed layouts require native layout review before compaction",
    );
    push_compaction_action(
        &mut actions,
        layout_health.non_native_data_file_count > 0,
        CompactionPlanningActionKind::ReviewNonNativeDataFiles,
        layout_health.non_native_data_file_count,
        "non-native data files require adapter fidelity evidence before compaction",
    );
    actions
}

fn push_compaction_action(
    actions: &mut Vec<CompactionPlanningAction>,
    present: bool,
    kind: CompactionPlanningActionKind,
    affected_count: usize,
    message: &'static str,
) {
    if present {
        actions.push(CompactionPlanningAction::new(kind, affected_count, message));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentChangeKind {
    Added,
    Removed,
    Replaced,
    Unchanged,
    MetadataOnly,
    Unknown,
}
impl SegmentChangeKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Removed => "removed",
            Self::Replaced => "replaced",
            Self::Unchanged => "unchanged",
            Self::MetadataOnly => "metadata_only",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentChange {
    pub kind: SegmentChangeKind,
    pub segment_id: SegmentId,
    pub reason: Option<String>,
}
impl SegmentChange {
    #[must_use]
    pub fn new(kind: SegmentChangeKind, segment_id: SegmentId) -> Self {
        Self {
            kind,
            segment_id,
            reason: None,
        }
    }
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "segment={} change_kind={} reason={}",
            self.segment_id.as_str(),
            self.kind.as_str(),
            self.reason.as_deref().unwrap_or("none")
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangeSet {
    pub from_snapshot: Option<SnapshotId>,
    pub to_snapshot: SnapshotId,
    pub changes: Vec<SegmentChange>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ChangeSet {
    #[must_use]
    pub fn new(to_snapshot: SnapshotId) -> Self {
        Self {
            from_snapshot: None,
            to_snapshot,
            changes: vec![],
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn between(from_snapshot: SnapshotId, to_snapshot: SnapshotId) -> Self {
        Self {
            from_snapshot: Some(from_snapshot),
            to_snapshot,
            changes: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_change(&mut self, change: SegmentChange) {
        self.changes.push(change);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn changed_segment_count(&self) -> usize {
        self.changes
            .iter()
            .filter(|c| c.kind != SegmentChangeKind::Unchanged)
            .count()
    }
    #[must_use]
    pub fn added_count(&self) -> usize {
        self.changes
            .iter()
            .filter(|c| c.kind == SegmentChangeKind::Added)
            .count()
    }
    #[must_use]
    pub fn removed_count(&self) -> usize {
        self.changes
            .iter()
            .filter(|c| c.kind == SegmentChangeKind::Removed)
            .count()
    }
    #[must_use]
    pub fn replaced_count(&self) -> usize {
        self.changes
            .iter()
            .filter(|c| c.kind == SegmentChangeKind::Replaced)
            .count()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.changed_segment_count() == 0
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
    pub fn summary(&self) -> String {
        format!(
            "from_snapshot={} to_snapshot={} changed_segments={} added={} removed={} replaced={} fallback_execution=disabled",
            self.from_snapshot
                .as_ref()
                .map_or("none", SnapshotId::as_str),
            self.to_snapshot.as_str(),
            self.changed_segment_count(),
            self.added_count(),
            self.removed_count(),
            self.replaced_count()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalPlanningDecision {
    ReuseUnchangedSegments { reason: String },
    ExecuteChangedSegmentsOnly { reason: String },
    PartialRecompute { reason: String },
    FullRecomputeRequired { reason: String },
    Unsupported { reason: String },
}
impl IncrementalPlanningDecision {
    #[must_use]
    pub fn reason(&self) -> &str {
        match self {
            Self::ReuseUnchangedSegments { reason }
            | Self::ExecuteChangedSegmentsOnly { reason }
            | Self::PartialRecompute { reason }
            | Self::FullRecomputeRequired { reason }
            | Self::Unsupported { reason } => reason,
        }
    }
    #[must_use]
    pub fn requires_full_recompute(&self) -> bool {
        matches!(self, Self::FullRecomputeRequired { .. })
    }
    #[must_use]
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Self::Unsupported { .. })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "decision={} reason={}",
            match self {
                Self::ReuseUnchangedSegments { .. } => "reuse_unchanged_segments",
                Self::ExecuteChangedSegmentsOnly { .. } => "execute_changed_segments_only",
                Self::PartialRecompute { .. } => "partial_recompute",
                Self::FullRecomputeRequired { .. } => "full_recompute_required",
                Self::Unsupported { .. } => "unsupported",
            },
            self.reason()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IncrementalPlanSkeleton {
    pub change_set: ChangeSet,
    pub decision: IncrementalPlanningDecision,
    pub diagnostics: Vec<Diagnostic>,
}
impl IncrementalPlanSkeleton {
    #[must_use]
    pub fn from_change_set(change_set: ChangeSet) -> Self {
        let decision = if change_set.is_empty() {
            IncrementalPlanningDecision::ReuseUnchangedSegments {
                reason: "no changed segments detected in planning metadata".to_string(),
            }
        } else if change_set.changes.iter().all(|c| {
            matches!(
                c.kind,
                SegmentChangeKind::Added
                    | SegmentChangeKind::Removed
                    | SegmentChangeKind::Replaced
                    | SegmentChangeKind::MetadataOnly
            )
        }) {
            IncrementalPlanningDecision::ExecuteChangedSegmentsOnly {
                reason: "changed-segment planning can execute only changed segments conservatively"
                    .to_string(),
            }
        } else {
            IncrementalPlanningDecision::PartialRecompute {
                reason: "change kinds require conservative partial recomputation".to_string(),
            }
        };
        Self {
            change_set,
            decision,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn unsupported(
        change_set: ChangeSet,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let mut s = Self {
            change_set,
            decision: IncrementalPlanningDecision::Unsupported {
                reason: format!("{feature}: {reason}"),
            },
            diagnostics: vec![],
        };
        s.add_diagnostic(Diagnostic::unsupported(DiagnosticCode::UnsupportedEffect, feature, format!("Incremental planning behavior is unsupported: {reason}. Fallback execution was not attempted. Spark, DataFusion, DuckDB, Polars, and Velox are not fallback engines."), Some("Use supported planning paths or wait for native ShardLoom support.".to_string())));
        s
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.change_set.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "{}; {}; fallback execution disabled",
            self.change_set.summary(),
            self.decision.summary()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcEventKind {
    Insert,
    Update,
    Delete,
    Tombstone,
    SchemaChange,
    PartitionChange,
    MetadataOnly,
    Unknown,
}
impl CdcEventKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Tombstone => "tombstone",
            Self::SchemaChange => "schema_change",
            Self::PartitionChange => "partition_change",
            Self::MetadataOnly => "metadata_only",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn requires_row_identity(&self) -> bool {
        matches!(self, Self::Update | Self::Delete)
    }

    #[must_use]
    pub const fn requires_delete_handling(&self) -> bool {
        matches!(self, Self::Delete | Self::Tombstone)
    }

    #[must_use]
    pub const fn requires_schema_compatibility(&self) -> bool {
        matches!(self, Self::SchemaChange)
    }

    #[must_use]
    pub const fn requires_partition_compatibility(&self) -> bool {
        matches!(self, Self::PartitionChange)
    }

    #[must_use]
    pub const fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcEventSummary {
    pub kind: CdcEventKind,
    pub count: usize,
}
impl CdcEventSummary {
    #[must_use]
    pub const fn new(kind: CdcEventKind, count: usize) -> Self {
        Self { kind, count }
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "cdc_event(kind={}, count={})",
            self.kind.as_str(),
            self.count
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcIncrementalPlanningStatus {
    ReuseUnchangedSegments,
    ExecuteChangedSegmentsOnly,
    PartialRecomputeRequired,
    FullRecomputeRequired,
    Unsupported,
}
impl CdcIncrementalPlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReuseUnchangedSegments => "reuse_unchanged_segments",
            Self::ExecuteChangedSegmentsOnly => "execute_changed_segments_only",
            Self::PartialRecomputeRequired => "partial_recompute_required",
            Self::FullRecomputeRequired => "full_recompute_required",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Machine-readable CG-9 CDC/incremental planning evidence.
///
/// This report evaluates declared change sets and CDC event summaries only. It does not read
/// manifests, scan data files, apply changes, write data, contact catalogs, or attempt fallback execution.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CdcIncrementalPlanningReport {
    pub change_set: ChangeSet,
    pub incremental_plan: IncrementalPlanSkeleton,
    pub cdc_events: Vec<CdcEventSummary>,
    pub status: CdcIncrementalPlanningStatus,
    pub diagnostics: Vec<Diagnostic>,
    pub insert_count: usize,
    pub update_count: usize,
    pub delete_count: usize,
    pub tombstone_count: usize,
    pub schema_change_count: usize,
    pub partition_change_count: usize,
    pub metadata_only_count: usize,
    pub unknown_event_count: usize,
    pub changed_segment_count: usize,
    pub metadata_only_segment_count: usize,
    pub unknown_segment_change_count: usize,
    pub requires_snapshot_pair: bool,
    pub requires_row_identity: bool,
    pub requires_delete_handling: bool,
    pub requires_schema_compatibility: bool,
    pub requires_partition_compatibility: bool,
    pub can_reuse_unchanged_segments: bool,
    pub can_execute_changed_segments_only: bool,
    pub requires_partial_recompute: bool,
    pub requires_full_recompute: bool,
    pub unsupported_change_count: usize,
    pub data_read: bool,
    pub write_io: bool,
    pub catalog_io: bool,
    pub object_store_io: bool,
    pub fallback_execution_allowed: bool,
}

impl CdcIncrementalPlanningReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.incremental_plan.has_errors()
            || self.unsupported_change_count > 0
            || self.fallback_execution_allowed
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.data_read
            && !self.write_io
            && !self.catalog_io
            && !self.object_store_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "cdc_incremental_plan(status={}, changed_segments={}, inserts={}, updates={}, deletes={}, tombstones={}, schema_changes={}, partition_changes={}, unsupported_changes={}, data_read=false, write_io=false, catalog_io=false, object_store_io=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.changed_segment_count,
            self.insert_count,
            self.update_count,
            self.delete_count,
            self.tombstone_count,
            self.schema_change_count,
            self.partition_change_count,
            self.unsupported_change_count
        )
    }
}

/// Evaluates CDC/incremental planning metadata without performing manifest, catalog, or data I/O.
#[must_use]
pub fn evaluate_cdc_incremental_planning(
    change_set: ChangeSet,
    cdc_events: Vec<CdcEventSummary>,
) -> CdcIncrementalPlanningReport {
    let incremental_plan = IncrementalPlanSkeleton::from_change_set(change_set.clone());
    let counts = CdcIncrementalCounts::from_change_set_and_events(&change_set, &cdc_events);
    let requirements = cdc_incremental_requirements(&change_set, counts);
    let unsupported_change_count =
        requirements.len() + counts.unknown_events + counts.unknown_segment_changes;
    let diagnostics = cdc_incremental_diagnostics(&requirements, counts);
    let status = if unsupported_change_count > 0 || diagnostics_are_errors(&diagnostics) {
        CdcIncrementalPlanningStatus::Unsupported
    } else {
        cdc_status_from_incremental_decision(&incremental_plan.decision)
    };

    CdcIncrementalPlanningReport {
        changed_segment_count: counts.changed_segments,
        metadata_only_segment_count: counts.metadata_only_segments,
        unknown_segment_change_count: counts.unknown_segment_changes,
        insert_count: counts.inserts,
        update_count: counts.updates,
        delete_count: counts.deletes,
        tombstone_count: counts.tombstones,
        schema_change_count: counts.schema_changes,
        partition_change_count: counts.partition_changes,
        metadata_only_count: counts.metadata_only_events,
        unknown_event_count: counts.unknown_events,
        requires_snapshot_pair: cdc_has_requirement(
            &requirements,
            CdcIncrementalRequirement::SnapshotPair,
        ),
        requires_row_identity: cdc_has_requirement(
            &requirements,
            CdcIncrementalRequirement::RowIdentity,
        ),
        requires_delete_handling: cdc_has_requirement(
            &requirements,
            CdcIncrementalRequirement::DeleteHandling,
        ),
        requires_schema_compatibility: cdc_has_requirement(
            &requirements,
            CdcIncrementalRequirement::SchemaCompatibility,
        ),
        requires_partition_compatibility: cdc_has_requirement(
            &requirements,
            CdcIncrementalRequirement::PartitionCompatibility,
        ),
        can_reuse_unchanged_segments: status
            == CdcIncrementalPlanningStatus::ReuseUnchangedSegments,
        can_execute_changed_segments_only: status
            == CdcIncrementalPlanningStatus::ExecuteChangedSegmentsOnly,
        requires_partial_recompute: status
            == CdcIncrementalPlanningStatus::PartialRecomputeRequired,
        requires_full_recompute: status == CdcIncrementalPlanningStatus::FullRecomputeRequired,
        unsupported_change_count,
        data_read: false,
        write_io: false,
        catalog_io: false,
        object_store_io: false,
        fallback_execution_allowed: false,
        change_set,
        incremental_plan,
        cdc_events,
        status,
        diagnostics,
    }
}

#[derive(Debug, Clone, Copy)]
struct CdcIncrementalCounts {
    inserts: usize,
    updates: usize,
    deletes: usize,
    tombstones: usize,
    schema_changes: usize,
    partition_changes: usize,
    metadata_only_events: usize,
    unknown_events: usize,
    changed_segments: usize,
    metadata_only_segments: usize,
    unknown_segment_changes: usize,
}

impl CdcIncrementalCounts {
    fn from_change_set_and_events(change_set: &ChangeSet, events: &[CdcEventSummary]) -> Self {
        Self {
            inserts: count_cdc_events(events, CdcEventKind::Insert),
            updates: count_cdc_events(events, CdcEventKind::Update),
            deletes: count_cdc_events(events, CdcEventKind::Delete),
            tombstones: count_cdc_events(events, CdcEventKind::Tombstone),
            schema_changes: count_cdc_events(events, CdcEventKind::SchemaChange),
            partition_changes: count_cdc_events(events, CdcEventKind::PartitionChange),
            metadata_only_events: count_cdc_events(events, CdcEventKind::MetadataOnly),
            unknown_events: count_cdc_events(events, CdcEventKind::Unknown),
            changed_segments: change_set.changed_segment_count(),
            metadata_only_segments: change_set
                .changes
                .iter()
                .filter(|change| change.kind == SegmentChangeKind::MetadataOnly)
                .count(),
            unknown_segment_changes: change_set
                .changes
                .iter()
                .filter(|change| change.kind == SegmentChangeKind::Unknown)
                .count(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CdcIncrementalRequirement {
    SnapshotPair,
    RowIdentity,
    DeleteHandling,
    SchemaCompatibility,
    PartitionCompatibility,
}

impl CdcIncrementalRequirement {
    fn diagnostic(self) -> Diagnostic {
        match self {
            Self::SnapshotPair => cdc_incremental_unsupported_diagnostic(
                "cdc_incremental_snapshot_pair",
                "changed-segment CDC planning requires both source and target snapshot ids",
            ),
            Self::RowIdentity => cdc_incremental_unsupported_diagnostic(
                "cdc_incremental_row_identity",
                "CDC updates/deletes require a native row-identity rule before incremental planning can be certified",
            ),
            Self::DeleteHandling => cdc_incremental_unsupported_diagnostic(
                "cdc_incremental_delete_handling",
                "CDC delete/tombstone events require native delete/tombstone handling before incremental planning can be certified",
            ),
            Self::SchemaCompatibility => cdc_incremental_unsupported_diagnostic(
                "cdc_incremental_schema_change",
                "CDC schema-change events require attached schema compatibility evidence",
            ),
            Self::PartitionCompatibility => cdc_incremental_unsupported_diagnostic(
                "cdc_incremental_partition_change",
                "CDC partition-change events require attached partition compatibility evidence",
            ),
        }
    }
}

fn cdc_incremental_requirements(
    change_set: &ChangeSet,
    counts: CdcIncrementalCounts,
) -> Vec<CdcIncrementalRequirement> {
    let mut requirements = Vec::new();
    if change_set.from_snapshot.is_none() && !change_set.is_empty() {
        requirements.push(CdcIncrementalRequirement::SnapshotPair);
    }
    if counts.updates > 0 || counts.deletes > 0 {
        requirements.push(CdcIncrementalRequirement::RowIdentity);
    }
    if counts.deletes > 0 || counts.tombstones > 0 {
        requirements.push(CdcIncrementalRequirement::DeleteHandling);
    }
    if counts.schema_changes > 0 {
        requirements.push(CdcIncrementalRequirement::SchemaCompatibility);
    }
    if counts.partition_changes > 0 {
        requirements.push(CdcIncrementalRequirement::PartitionCompatibility);
    }
    requirements
}

fn cdc_has_requirement(
    requirements: &[CdcIncrementalRequirement],
    requirement: CdcIncrementalRequirement,
) -> bool {
    requirements.contains(&requirement)
}

fn cdc_incremental_diagnostics(
    requirements: &[CdcIncrementalRequirement],
    counts: CdcIncrementalCounts,
) -> Vec<Diagnostic> {
    let mut diagnostics: Vec<_> = requirements
        .iter()
        .map(|requirement| requirement.diagnostic())
        .collect();
    if counts.unknown_events > 0 || counts.unknown_segment_changes > 0 {
        diagnostics.push(cdc_incremental_unsupported_diagnostic(
            "cdc_incremental_unknown_change",
            "unknown CDC events or segment changes cannot be planned safely",
        ));
    }
    diagnostics
}

fn count_cdc_events(events: &[CdcEventSummary], kind: CdcEventKind) -> usize {
    events
        .iter()
        .filter(|event| event.kind == kind)
        .map(|event| event.count)
        .sum()
}

fn cdc_status_from_incremental_decision(
    decision: &IncrementalPlanningDecision,
) -> CdcIncrementalPlanningStatus {
    match decision {
        IncrementalPlanningDecision::ReuseUnchangedSegments { .. } => {
            CdcIncrementalPlanningStatus::ReuseUnchangedSegments
        }
        IncrementalPlanningDecision::ExecuteChangedSegmentsOnly { .. } => {
            CdcIncrementalPlanningStatus::ExecuteChangedSegmentsOnly
        }
        IncrementalPlanningDecision::PartialRecompute { .. } => {
            CdcIncrementalPlanningStatus::PartialRecomputeRequired
        }
        IncrementalPlanningDecision::FullRecomputeRequired { .. } => {
            CdcIncrementalPlanningStatus::FullRecomputeRequired
        }
        IncrementalPlanningDecision::Unsupported { .. } => {
            CdcIncrementalPlanningStatus::Unsupported
        }
    }
}

fn diagnostics_are_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.severity,
            DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
        )
    })
}

fn cdc_incremental_unsupported_diagnostic(
    feature: impl Into<String>,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::unsupported(
        DiagnosticCode::UnsupportedEffect,
        feature,
        message,
        Some(
            "Attach native CDC compatibility evidence before enabling this incremental path."
                .to_string(),
        ),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteIntentStatus {
    Planned,
    Validated,
    WriteNotImplemented,
    Unsupported,
}
impl WriteIntentStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Validated => "validated",
            Self::WriteNotImplemented => "write_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriteIntent {
    pub target: OutputTarget,
    pub output_format: OutputTargetKind,
    pub commit_mode: CommitMode,
    pub idempotency_key: Option<String>,
    pub status: WriteIntentStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl WriteIntent {
    #[must_use]
    pub fn new(target: OutputTarget) -> Self {
        let output_format = target.kind.clone();
        Self {
            target,
            output_format,
            commit_mode: CommitMode::NotPlanned,
            idempotency_key: None,
            status: WriteIntentStatus::Planned,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn with_commit_mode(mut self, commit_mode: CommitMode) -> Self {
        self.commit_mode = commit_mode;
        self
    }
    #[must_use]
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
    #[must_use]
    pub fn write_not_implemented(target: OutputTarget) -> Self {
        let mut s = Self::new(target);
        s.status = WriteIntentStatus::WriteNotImplemented;
        s.add_diagnostic(Diagnostic::unsupported(DiagnosticCode::UnsupportedEffect, "write_intent", "Write intent is planning-only. Actual file writes are not implemented. Fallback execution was not attempted. Spark, DataFusion, DuckDB, Polars, and Velox are not fallback engines.", Some("Use this intent for planning metadata only.".to_string())));
        s
    }
    #[must_use]
    pub fn unsupported(
        target: OutputTarget,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let mut s = Self::new(target);
        s.status = WriteIntentStatus::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(DiagnosticCode::UnsupportedOutputFormat, feature, format!("Write intent is unsupported: {reason}. Fallback execution was not attempted. Spark, DataFusion, DuckDB, Polars, and Velox are not fallback engines."), Some("Select a supported output mode or wait for native support.".to_string())));
        s
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
    pub fn summary(&self) -> String {
        format!(
            "target_uri={} output_kind={} native_vortex_output={} commit_mode={} status={} diagnostics={} fallback_execution=disabled",
            self.target.uri.as_str(),
            self.output_format.as_str(),
            self.output_format.is_native_vortex(),
            self.commit_mode.as_str(),
            self.status.as_str(),
            self.diagnostics.len()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitStatus {
    NotStarted,
    Planned,
    Committed,
    Failed,
    Ambiguous,
    Unsupported,
}
impl CommitStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Planned => "planned",
            Self::Committed => "committed",
            Self::Failed => "failed",
            Self::Ambiguous => "ambiguous",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitRecord {
    pub commit_id: String,
    pub input_snapshots: Vec<SnapshotId>,
    pub output_snapshot: Option<SnapshotId>,
    pub added_segments: Vec<SegmentId>,
    pub removed_segments: Vec<SegmentId>,
    pub output_files: Vec<FileDescriptor>,
    pub status: CommitStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl CommitRecord {
    /// Creates a commit planning record with a validated commit identifier.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `commit_id` is empty or whitespace only.
    pub fn new(commit_id: impl Into<String>) -> Result<Self> {
        let commit_id = commit_id.into();
        if commit_id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "commit id must not be empty".to_string(),
            ));
        }
        Ok(Self {
            commit_id,
            input_snapshots: vec![],
            output_snapshot: None,
            added_segments: vec![],
            removed_segments: vec![],
            output_files: vec![],
            status: CommitStatus::NotStarted,
            diagnostics: vec![],
        })
    }
    pub fn add_input_snapshot(&mut self, snapshot: SnapshotId) {
        self.input_snapshots.push(snapshot);
    }
    pub fn set_output_snapshot(&mut self, snapshot: SnapshotId) {
        self.output_snapshot = Some(snapshot);
    }
    pub fn add_added_segment(&mut self, segment: SegmentId) {
        self.added_segments.push(segment);
    }
    pub fn add_removed_segment(&mut self, segment: SegmentId) {
        self.removed_segments.push(segment);
    }
    pub fn add_output_file(&mut self, file: FileDescriptor) {
        self.output_files.push(file);
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
    pub fn summary(&self) -> String {
        format!(
            "commit_id={} status={} input_snapshots={} output_snapshot={} added_segments={} removed_segments={} output_files={} fallback_execution=disabled",
            self.commit_id,
            self.status.as_str(),
            self.input_snapshots.len(),
            self.output_snapshot
                .as_ref()
                .map_or("none", SnapshotId::as_str),
            self.added_segments.len(),
            self.removed_segments.len(),
            self.output_files.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ByteRange, ColumnRef, EncodingKind, LayoutKind, LogicalDType, Nullability, SegmentLayout,
        SegmentStats,
    };
    fn mk_seg() -> EncodedSegment {
        EncodedSegment::new(
            SegmentId::new("s1").unwrap(),
            ColumnRef::new("c").unwrap(),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat)
                .with_byte_ranges(vec![ByteRange::new(0, 10)]),
            SegmentStats::with_row_count(10),
        )
    }
    fn mk_layout_manifest() -> DatasetManifest {
        DatasetManifest::new(
            ManifestId::new("m").unwrap(),
            DatasetRef::from_uri(DatasetUri::new("file://x/a.vortex").unwrap()).unwrap(),
            SnapshotRef::new(SnapshotId::new("s").unwrap()),
        )
    }
    fn mk_layout_seg(
        id: &str,
        rows: Option<u64>,
        physical_size_bytes: Option<u64>,
        has_byte_ranges: bool,
    ) -> EncodedSegment {
        let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
        layout.physical_size_bytes = physical_size_bytes;
        if has_byte_ranges {
            layout = layout.with_byte_ranges(vec![ByteRange::new(0, 10)]);
        }
        let stats = rows.map_or_else(SegmentStats::unknown, SegmentStats::with_row_count);
        EncodedSegment::new(
            SegmentId::new(id).unwrap(),
            ColumnRef::new("c").unwrap(),
            LogicalDType::Int64,
            Nullability::Nullable,
            layout,
            stats,
        )
    }
    #[test]
    fn snapshot_ref_without_parent_has_parent_false() {
        assert!(!SnapshotRef::new(SnapshotId::new("a").unwrap()).has_parent());
    }
    #[test]
    fn snapshot_ref_with_parent_has_parent_true() {
        assert!(
            SnapshotRef::new(SnapshotId::new("a").unwrap())
                .with_parent(SnapshotId::new("b").unwrap())
                .has_parent()
        );
    }
    #[test]
    fn file_descriptor_from_uri_infers_vortex() {
        let fd = FileDescriptor::from_uri(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            FileRole::NativeVortexData,
        );
        assert_eq!(fd.format, DatasetFormat::Vortex);
    }
    #[test]
    fn file_descriptor_native_vortex_role_true() {
        let fd = FileDescriptor::new(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            DatasetFormat::Vortex,
            FileRole::NativeVortexData,
        );
        assert!(fd.is_native_vortex_data());
    }
    #[test]
    fn file_descriptor_native_vortex_role_false_for_parquet() {
        let fd = FileDescriptor::new(
            DatasetUri::new("file://x/a.parquet").unwrap(),
            DatasetFormat::Parquet,
            FileRole::NativeVortexData,
        );
        assert!(!fd.is_native_vortex_data());
    }
    #[test]
    fn manifest_segment_can_use_metadata_delegates() {
        let ms = ManifestSegment::new(
            mk_seg(),
            FileDescriptor::from_uri(
                DatasetUri::new("file://x/a.vortex").unwrap(),
                FileRole::NativeVortexData,
            ),
        );
        assert!(ms.can_use_metadata());
    }
    #[test]
    fn dataset_manifest_new_has_version_v1() {
        let dm = DatasetManifest::new(
            ManifestId::new("m").unwrap(),
            DatasetRef::from_uri(DatasetUri::new("file://x/a.vortex").unwrap()).unwrap(),
            SnapshotRef::new(SnapshotId::new("s").unwrap()),
        );
        assert_eq!(dm.version, ManifestVersion::V1);
    }
    #[test]
    fn dataset_manifest_counts_files_and_segments() {
        let mut dm = DatasetManifest::new(
            ManifestId::new("m").unwrap(),
            DatasetRef::from_uri(DatasetUri::new("file://x/a.vortex").unwrap()).unwrap(),
            SnapshotRef::new(SnapshotId::new("s").unwrap()),
        );
        dm.add_file(FileDescriptor::from_uri(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            FileRole::NativeVortexData,
        ));
        dm.add_segment(ManifestSegment::new(
            mk_seg(),
            FileDescriptor::from_uri(
                DatasetUri::new("file://x/a.vortex").unwrap(),
                FileRole::NativeVortexData,
            ),
        ));
        assert_eq!(dm.file_count(), 1);
        assert_eq!(dm.segment_count(), 1);
    }
    #[test]
    fn layout_health_healthy_manifest_is_side_effect_free() {
        let mut manifest = mk_layout_manifest();
        let file = FileDescriptor::from_uri(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            FileRole::NativeVortexData,
        )
        .with_size_bytes(64 * 1024 * 1024);
        manifest.add_file(file.clone());
        manifest.add_segment(ManifestSegment::new(
            mk_layout_seg("s1", Some(64_000), Some(8 * 1024 * 1024), true),
            file,
        ));

        let report = evaluate_layout_health(manifest, LayoutHealthPolicy::default());

        assert_eq!(report.status, LayoutHealthStatus::Healthy);
        assert_eq!(report.issues.len(), 0);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }
    #[test]
    fn layout_health_recommends_compaction_for_small_files_and_segments() {
        let mut manifest = mk_layout_manifest();
        let file = FileDescriptor::from_uri(
            DatasetUri::new("file://x/small.vortex").unwrap(),
            FileRole::NativeVortexData,
        )
        .with_size_bytes(1024);
        manifest.add_file(file.clone());
        manifest.add_segment(ManifestSegment::new(
            mk_layout_seg("s1", Some(10), Some(512), true),
            file,
        ));

        let report = evaluate_layout_health(manifest, LayoutHealthPolicy::default());

        assert_eq!(report.status, LayoutHealthStatus::CompactionRecommended);
        assert_eq!(report.small_file_count, 1);
        assert_eq!(report.small_segment_count, 1);
        assert!(report.recommends_compaction);
        assert!(!report.compaction_execution_allowed);
    }
    #[test]
    fn layout_health_missing_stats_needs_attention_without_io() {
        let mut manifest = mk_layout_manifest();
        let file = FileDescriptor::from_uri(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            FileRole::NativeVortexData,
        )
        .with_size_bytes(64 * 1024 * 1024);
        manifest.add_file(file.clone());
        manifest.add_segment(ManifestSegment::new(
            mk_layout_seg("s1", None, Some(8 * 1024 * 1024), false),
            file,
        ));

        let report = evaluate_layout_health(manifest, LayoutHealthPolicy::default());

        assert_eq!(report.status, LayoutHealthStatus::NeedsAttention);
        assert_eq!(report.missing_statistics_segment_count, 1);
        assert_eq!(report.missing_byte_range_segment_count, 1);
        assert!(report.requires_statistics_refresh);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }
    #[test]
    fn layout_health_empty_manifest_is_unsupported() {
        let report = evaluate_layout_health(mk_layout_manifest(), LayoutHealthPolicy::default());

        assert_eq!(report.status, LayoutHealthStatus::Unsupported);
        assert!(report.has_errors());
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn compaction_planning_healthy_manifest_is_not_needed() {
        let mut manifest = mk_layout_manifest();
        let file = FileDescriptor::from_uri(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            FileRole::NativeVortexData,
        )
        .with_size_bytes(64 * 1024 * 1024);
        manifest.add_file(file.clone());
        manifest.add_segment(ManifestSegment::new(
            mk_layout_seg("s1", Some(64_000), Some(8 * 1024 * 1024), true),
            file,
        ));

        let report = evaluate_compaction_planning(
            manifest,
            LayoutHealthPolicy::default(),
            CompactionPlanningPolicy::default(),
        );

        assert_eq!(report.status, CompactionPlanningStatus::NotNeeded);
        assert_eq!(report.candidate_count, 0);
        assert_eq!(report.estimated_compaction_group_count, 0);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }
    #[test]
    fn compaction_planning_recommends_groups_for_small_candidates() {
        let mut manifest = mk_layout_manifest();
        let file = FileDescriptor::from_uri(
            DatasetUri::new("file://x/small.vortex").unwrap(),
            FileRole::NativeVortexData,
        )
        .with_size_bytes(1024);
        manifest.add_file(file.clone());
        manifest.add_segment(ManifestSegment::new(
            mk_layout_seg("s1", Some(10), Some(512), true),
            file,
        ));

        let report = evaluate_compaction_planning(
            manifest,
            LayoutHealthPolicy::default(),
            CompactionPlanningPolicy::default(),
        );

        assert_eq!(report.status, CompactionPlanningStatus::PlanningReady);
        assert_eq!(report.candidate_file_count, 1);
        assert_eq!(report.candidate_segment_count, 1);
        assert_eq!(report.candidate_count, 2);
        assert_eq!(report.estimated_compaction_group_count, 1);
        assert!(report.recommendation_emitted);
        assert!(!report.compaction_execution_allowed);
        assert!(
            report
                .actions
                .iter()
                .any(|action| { action.kind == CompactionPlanningActionKind::MergeSmallFiles })
        );
    }
    #[test]
    fn compaction_planning_blocks_on_missing_metadata_without_io() {
        let mut manifest = mk_layout_manifest();
        let file = FileDescriptor::from_uri(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            FileRole::NativeVortexData,
        )
        .with_size_bytes(64 * 1024 * 1024);
        manifest.add_file(file.clone());
        manifest.add_segment(ManifestSegment::new(
            mk_layout_seg("s1", None, Some(8 * 1024 * 1024), false),
            file,
        ));

        let report = evaluate_compaction_planning(
            manifest,
            LayoutHealthPolicy::default(),
            CompactionPlanningPolicy::default(),
        );

        assert_eq!(report.status, CompactionPlanningStatus::BlockedByMetadata);
        assert!(report.requires_statistics_refresh);
        assert!(report.requires_byte_range_index);
        assert!(!report.recommendation_emitted);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }
    #[test]
    fn compaction_planning_blocks_mixed_layout_review() {
        let mut manifest = mk_layout_manifest();
        let vortex_file = FileDescriptor::from_uri(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            FileRole::NativeVortexData,
        )
        .with_size_bytes(64 * 1024 * 1024);
        let parquet_file = FileDescriptor::new(
            DatasetUri::new("file://x/a.parquet").unwrap(),
            DatasetFormat::Parquet,
            FileRole::NativeVortexData,
        )
        .with_size_bytes(64 * 1024 * 1024);
        manifest.add_file(vortex_file.clone());
        manifest.add_file(parquet_file.clone());
        manifest.add_segment(ManifestSegment::new(
            mk_layout_seg("s1", Some(64_000), Some(8 * 1024 * 1024), true),
            vortex_file,
        ));
        manifest.add_segment(ManifestSegment::new(
            mk_layout_seg("s2", Some(64_000), Some(8 * 1024 * 1024), true),
            parquet_file,
        ));

        let report = evaluate_compaction_planning(
            manifest,
            LayoutHealthPolicy::default(),
            CompactionPlanningPolicy::default(),
        );

        assert_eq!(
            report.status,
            CompactionPlanningStatus::BlockedByLayoutReview
        );
        assert!(report.requires_layout_review);
        assert!(report.requires_native_input_review);
        assert!(report.actions.iter().any(|action| {
            action.kind == CompactionPlanningActionKind::ReviewNonNativeDataFiles
        }));
    }
    #[test]
    fn compaction_planning_empty_manifest_is_unsupported() {
        let report = evaluate_compaction_planning(
            mk_layout_manifest(),
            LayoutHealthPolicy::default(),
            CompactionPlanningPolicy::default(),
        );

        assert_eq!(report.status, CompactionPlanningStatus::Unsupported);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }
    #[test]
    fn dataset_manifest_native_vortex_file_count_works() {
        let mut dm = DatasetManifest::new(
            ManifestId::new("m").unwrap(),
            DatasetRef::from_uri(DatasetUri::new("file://x/a.vortex").unwrap()).unwrap(),
            SnapshotRef::new(SnapshotId::new("s").unwrap()),
        );
        dm.add_file(FileDescriptor::new(
            DatasetUri::new("file://x/a.vortex").unwrap(),
            DatasetFormat::Vortex,
            FileRole::NativeVortexData,
        ));
        dm.add_file(FileDescriptor::new(
            DatasetUri::new("file://x/a.parquet").unwrap(),
            DatasetFormat::Parquet,
            FileRole::CompatibilityOutput,
        ));
        assert_eq!(dm.native_vortex_file_count(), 1);
    }
    #[test]
    fn segment_change_summary_includes_change_kind() {
        let s =
            SegmentChange::new(SegmentChangeKind::Added, SegmentId::new("s").unwrap()).summary();
        assert!(s.contains("added"));
    }
    #[test]
    fn change_set_empty_is_empty() {
        assert!(ChangeSet::new(SnapshotId::new("s").unwrap()).is_empty());
    }
    #[test]
    fn change_set_with_added_change_not_empty() {
        let mut cs = ChangeSet::new(SnapshotId::new("s").unwrap());
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Added,
            SegmentId::new("x").unwrap(),
        ));
        assert!(!cs.is_empty());
    }
    #[test]
    fn change_set_added_removed_replaced_counts_work() {
        let mut cs = ChangeSet::new(SnapshotId::new("s").unwrap());
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Added,
            SegmentId::new("a").unwrap(),
        ));
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Removed,
            SegmentId::new("b").unwrap(),
        ));
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Replaced,
            SegmentId::new("c").unwrap(),
        ));
        assert_eq!(cs.added_count(), 1);
        assert_eq!(cs.removed_count(), 1);
        assert_eq!(cs.replaced_count(), 1);
    }
    #[test]
    fn incremental_plan_empty_chooses_reuse() {
        let p =
            IncrementalPlanSkeleton::from_change_set(ChangeSet::new(SnapshotId::new("s").unwrap()));
        assert!(matches!(
            p.decision,
            IncrementalPlanningDecision::ReuseUnchangedSegments { .. }
        ));
    }
    #[test]
    fn incremental_plan_changed_chooses_execute_changed() {
        let mut cs = ChangeSet::new(SnapshotId::new("s").unwrap());
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Added,
            SegmentId::new("a").unwrap(),
        ));
        let p = IncrementalPlanSkeleton::from_change_set(cs);
        assert!(matches!(
            p.decision,
            IncrementalPlanningDecision::ExecuteChangedSegmentsOnly { .. }
        ));
    }
    #[test]
    fn cdc_incremental_append_only_executes_changed_segments_without_io() {
        let mut cs = ChangeSet::between(
            SnapshotId::new("s1").unwrap(),
            SnapshotId::new("s2").unwrap(),
        );
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Added,
            SegmentId::new("a").unwrap(),
        ));

        let report = evaluate_cdc_incremental_planning(
            cs,
            vec![CdcEventSummary::new(CdcEventKind::Insert, 5)],
        );

        assert_eq!(
            report.status,
            CdcIncrementalPlanningStatus::ExecuteChangedSegmentsOnly
        );
        assert_eq!(report.insert_count, 5);
        assert!(report.can_execute_changed_segments_only);
        assert_eq!(report.unsupported_change_count, 0);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }
    #[test]
    fn cdc_incremental_delete_requires_native_delete_handling() {
        let mut cs = ChangeSet::between(
            SnapshotId::new("s1").unwrap(),
            SnapshotId::new("s2").unwrap(),
        );
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Removed,
            SegmentId::new("a").unwrap(),
        ));

        let report = evaluate_cdc_incremental_planning(
            cs,
            vec![CdcEventSummary::new(CdcEventKind::Delete, 1)],
        );

        assert_eq!(report.status, CdcIncrementalPlanningStatus::Unsupported);
        assert!(report.requires_row_identity);
        assert!(report.requires_delete_handling);
        assert!(report.has_errors());
        assert!(!report.diagnostics[0].fallback.attempted);
        assert!(report.side_effect_free());
    }
    #[test]
    fn cdc_incremental_missing_snapshot_pair_is_rejected_for_changes() {
        let mut cs = ChangeSet::new(SnapshotId::new("s2").unwrap());
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Added,
            SegmentId::new("a").unwrap(),
        ));

        let report = evaluate_cdc_incremental_planning(
            cs,
            vec![CdcEventSummary::new(CdcEventKind::Insert, 1)],
        );

        assert_eq!(report.status, CdcIncrementalPlanningStatus::Unsupported);
        assert!(report.requires_snapshot_pair);
        assert!(report.has_errors());
    }
    #[test]
    fn cdc_incremental_unknown_change_is_rejected() {
        let mut cs = ChangeSet::between(
            SnapshotId::new("s1").unwrap(),
            SnapshotId::new("s2").unwrap(),
        );
        cs.add_change(SegmentChange::new(
            SegmentChangeKind::Unknown,
            SegmentId::new("a").unwrap(),
        ));

        let report = evaluate_cdc_incremental_planning(
            cs,
            vec![CdcEventSummary::new(CdcEventKind::Unknown, 1)],
        );

        assert_eq!(report.status, CdcIncrementalPlanningStatus::Unsupported);
        assert_eq!(report.unknown_segment_change_count, 1);
        assert_eq!(report.unknown_event_count, 1);
        assert!(report.has_errors());
    }
    #[test]
    fn incremental_decision_requires_full_recompute_works() {
        assert!(
            IncrementalPlanningDecision::FullRecomputeRequired {
                reason: "r".to_string()
            }
            .requires_full_recompute()
        );
    }
    #[test]
    fn write_intent_new_preserves_target_kind() {
        let target = OutputTarget::from_uri(DatasetUri::new("file://x/a.vortex").unwrap());
        let wi = WriteIntent::new(target.clone());
        assert_eq!(wi.output_format, target.kind);
    }
    #[test]
    fn write_intent_not_implemented_has_errors() {
        let wi = WriteIntent::write_not_implemented(OutputTarget::from_uri(
            DatasetUri::new("file://x/a.vortex").unwrap(),
        ));
        assert!(wi.has_errors());
    }
    #[test]
    fn commit_record_rejects_empty_commit_id() {
        assert!(CommitRecord::new("  ").is_err());
    }
    #[test]
    fn commit_record_summary_includes_commit_id_and_status() {
        let cr = CommitRecord::new("c1").unwrap();
        let s = cr.summary();
        assert!(s.contains("commit_id=c1"));
        assert!(s.contains("status=not_started"));
    }
}
