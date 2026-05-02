//! Native dataset manifest, snapshot, and incremental planning skeleton types.
//!
//! These domain types model planning metadata only. They do not parse persisted
//! manifest files, perform Vortex IO, or execute object-store operations.
//! Unsupported behavior must fail explicitly with deterministic diagnostics, and
//! fallback execution remains disabled by policy.

use crate::{
    CommitMode, DatasetFormat, DatasetRef, DatasetUri, Diagnostic, DiagnosticCode,
    DiagnosticSeverity, EncodedSegment, ManifestId, OutputTarget, OutputTargetKind, Result,
    SegmentId, ShardLoomError, SnapshotId,
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
