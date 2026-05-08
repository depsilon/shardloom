//! Object-store range planning evidence.
//!
//! This module plans request shapes from already-declared manifest byte-range metadata.
//! It performs no object-store IO, no file reads, no data materialization, and no fallback execution.

use shardloom_core::{
    ByteRange, DatasetManifest, DatasetUri, Diagnostic, DiagnosticCategory, DiagnosticCode,
    DiagnosticSeverity, FallbackStatus, SegmentId, UriScheme,
};

/// Report-only policy for object-store range planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStoreRangePlanningPolicy {
    pub max_ranges_per_request: usize,
    pub max_request_bytes: u64,
    pub coalesce_adjacent_ranges: bool,
    pub max_coalesce_gap_bytes: u64,
}

impl Default for ObjectStoreRangePlanningPolicy {
    fn default() -> Self {
        Self {
            max_ranges_per_request: 8,
            max_request_bytes: 16 * 1024 * 1024,
            coalesce_adjacent_ranges: true,
            max_coalesce_gap_bytes: 4096,
        }
    }
}

/// Object-store range planning status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreRangePlanningStatus {
    Planned,
    BlockedMissingByteRanges,
    BlockedInvalidRanges,
    BlockedRequestBudget,
    BlockedNonObjectStore,
    Unsupported,
}

impl ObjectStoreRangePlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::BlockedMissingByteRanges => "blocked_missing_byte_ranges",
            Self::BlockedInvalidRanges => "blocked_invalid_ranges",
            Self::BlockedRequestBudget => "blocked_request_budget",
            Self::BlockedNonObjectStore => "blocked_non_object_store",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Planned)
    }
}

/// Planned object-store range request shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStoreRangeRequest {
    pub uri: DatasetUri,
    pub segment_ids: Vec<SegmentId>,
    pub range: ByteRange,
    pub source_range_count: usize,
}

impl ObjectStoreRangeRequest {
    #[must_use]
    pub fn new(
        uri: DatasetUri,
        segment_id: SegmentId,
        range: ByteRange,
    ) -> ObjectStoreRangeRequest {
        Self {
            uri,
            segment_ids: vec![segment_id],
            range,
            source_range_count: 1,
        }
    }

    #[must_use]
    pub fn estimated_bytes(&self) -> u64 {
        self.range.length
    }
}

/// Machine-readable CG-10 object-store range planning evidence.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStoreRangePlanningReport {
    pub manifest: DatasetManifest,
    pub policy: ObjectStoreRangePlanningPolicy,
    pub status: ObjectStoreRangePlanningStatus,
    pub requests: Vec<ObjectStoreRangeRequest>,
    pub diagnostics: Vec<Diagnostic>,
    pub file_count: usize,
    pub segment_count: usize,
    pub object_store_file_count: usize,
    pub non_object_store_file_count: usize,
    pub ranged_segment_count: usize,
    pub missing_byte_range_segment_count: usize,
    pub invalid_range_count: usize,
    pub oversized_range_count: usize,
    pub planned_request_count: usize,
    pub planned_range_count: usize,
    pub coalesced_range_count: usize,
    pub estimated_request_bytes: u64,
    pub requires_byte_ranges: bool,
    pub requires_request_budget_review: bool,
    pub full_file_read_required: bool,
    pub full_file_read_allowed: bool,
    pub can_plan_without_io: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
}

impl ObjectStoreRangePlanningReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.fallback_execution_allowed
            || self.object_store_io
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "object_store_range_plan(status={}, object_store_files={}, segments={}, planned_requests={}, planned_ranges={}, estimated_request_bytes={}, data_read=false, object_store_io=false, write_io=false, fallback_execution=disabled)",
            self.status.as_str(),
            self.object_store_file_count,
            self.segment_count,
            self.planned_request_count,
            self.planned_range_count,
            self.estimated_request_bytes
        )
    }
}

/// Plans object-store byte-range request shapes from declared manifest metadata only.
#[must_use]
pub fn plan_object_store_ranges(
    manifest: DatasetManifest,
    policy: ObjectStoreRangePlanningPolicy,
) -> ObjectStoreRangePlanningReport {
    let counts = ObjectStoreRangeCounts::from_manifest(&manifest, policy);
    let status = object_store_range_status(counts);
    let requests = if status == ObjectStoreRangePlanningStatus::Planned {
        coalesced_object_store_ranges(&manifest, policy)
    } else {
        Vec::new()
    };
    let estimated_request_bytes = requests
        .iter()
        .map(ObjectStoreRangeRequest::estimated_bytes)
        .sum();
    let planned_range_count = requests
        .iter()
        .map(|request| request.source_range_count)
        .sum();
    let diagnostics = object_store_range_diagnostics(counts, status);

    ObjectStoreRangePlanningReport {
        file_count: counts.files,
        segment_count: counts.segments,
        object_store_file_count: counts.object_store_files,
        non_object_store_file_count: counts.non_object_store_files,
        ranged_segment_count: counts.ranged_segments,
        missing_byte_range_segment_count: counts.missing_byte_range_segments,
        invalid_range_count: counts.invalid_ranges,
        oversized_range_count: counts.oversized_ranges,
        planned_request_count: requests.len(),
        planned_range_count,
        coalesced_range_count: planned_range_count.saturating_sub(requests.len()),
        estimated_request_bytes,
        requires_byte_ranges: counts.missing_byte_range_segments > 0,
        requires_request_budget_review: counts.oversized_ranges > 0,
        full_file_read_required: counts.missing_byte_range_segments > 0,
        full_file_read_allowed: false,
        can_plan_without_io: true,
        data_read: false,
        object_store_io: false,
        write_io: false,
        fallback_execution_allowed: false,
        manifest,
        policy,
        status,
        requests,
        diagnostics,
    }
}

#[derive(Debug, Clone, Copy)]
struct ObjectStoreRangeCounts {
    files: usize,
    segments: usize,
    object_store_files: usize,
    non_object_store_files: usize,
    ranged_segments: usize,
    missing_byte_range_segments: usize,
    invalid_ranges: usize,
    oversized_ranges: usize,
}

impl ObjectStoreRangeCounts {
    fn from_manifest(manifest: &DatasetManifest, policy: ObjectStoreRangePlanningPolicy) -> Self {
        Self {
            files: manifest.file_count(),
            segments: manifest.segment_count(),
            object_store_files: manifest
                .files
                .iter()
                .filter(|file| is_object_store_uri(&file.uri))
                .count(),
            non_object_store_files: manifest
                .files
                .iter()
                .filter(|file| !is_object_store_uri(&file.uri))
                .count(),
            ranged_segments: manifest
                .segments
                .iter()
                .filter(|segment| {
                    is_object_store_uri(&segment.file.uri) && segment.segment.has_byte_ranges()
                })
                .count(),
            missing_byte_range_segments: manifest
                .segments
                .iter()
                .filter(|segment| {
                    is_object_store_uri(&segment.file.uri) && !segment.segment.has_byte_ranges()
                })
                .count(),
            invalid_ranges: manifest
                .segments
                .iter()
                .filter(|segment| is_object_store_uri(&segment.file.uri))
                .flat_map(|segment| segment.segment.layout.byte_ranges.iter())
                .filter(|range| range.is_empty())
                .count(),
            oversized_ranges: manifest
                .segments
                .iter()
                .filter(|segment| is_object_store_uri(&segment.file.uri))
                .flat_map(|segment| segment.segment.layout.byte_ranges.iter())
                .filter(|range| range.length > policy.max_request_bytes)
                .count(),
        }
    }
}

fn object_store_range_status(counts: ObjectStoreRangeCounts) -> ObjectStoreRangePlanningStatus {
    if counts.segments == 0 {
        ObjectStoreRangePlanningStatus::Unsupported
    } else if counts.object_store_files == 0 {
        ObjectStoreRangePlanningStatus::BlockedNonObjectStore
    } else if counts.invalid_ranges > 0 {
        ObjectStoreRangePlanningStatus::BlockedInvalidRanges
    } else if counts.oversized_ranges > 0 {
        ObjectStoreRangePlanningStatus::BlockedRequestBudget
    } else if counts.missing_byte_range_segments > 0 {
        ObjectStoreRangePlanningStatus::BlockedMissingByteRanges
    } else {
        ObjectStoreRangePlanningStatus::Planned
    }
}

fn is_object_store_uri(uri: &DatasetUri) -> bool {
    matches!(
        uri.scheme(),
        UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls
    )
}

fn coalesced_object_store_ranges(
    manifest: &DatasetManifest,
    policy: ObjectStoreRangePlanningPolicy,
) -> Vec<ObjectStoreRangeRequest> {
    let mut requests = manifest
        .segments
        .iter()
        .filter(|segment| is_object_store_uri(&segment.file.uri))
        .flat_map(|segment| {
            segment
                .segment
                .layout
                .byte_ranges
                .iter()
                .copied()
                .map(|range| {
                    ObjectStoreRangeRequest::new(
                        segment.file.uri.clone(),
                        segment.segment.id.clone(),
                        range,
                    )
                })
        })
        .collect::<Vec<_>>();

    requests.sort_by(|left, right| {
        left.uri
            .as_str()
            .cmp(right.uri.as_str())
            .then(left.range.start.cmp(&right.range.start))
            .then(left.range.length.cmp(&right.range.length))
    });

    if !policy.coalesce_adjacent_ranges {
        return requests;
    }

    let mut coalesced: Vec<ObjectStoreRangeRequest> = Vec::new();
    for request in requests {
        if let Some(last) = coalesced.last_mut() {
            if can_coalesce_ranges(last, &request, policy) {
                let start = last.range.start.min(request.range.start);
                let end = last
                    .range
                    .end_exclusive()
                    .max(request.range.end_exclusive());
                last.range = ByteRange::new(start, end.saturating_sub(start));
                last.segment_ids.extend(request.segment_ids);
                last.source_range_count += request.source_range_count;
                continue;
            }
        }
        coalesced.push(request);
    }
    coalesced
}

fn can_coalesce_ranges(
    left: &ObjectStoreRangeRequest,
    right: &ObjectStoreRangeRequest,
    policy: ObjectStoreRangePlanningPolicy,
) -> bool {
    if left.uri != right.uri || left.source_range_count >= policy.max_ranges_per_request {
        return false;
    }
    let left_end = left.range.end_exclusive();
    let right_end = right.range.end_exclusive();
    let gap = right.range.start.saturating_sub(left_end);
    let merged_length = right_end
        .max(left_end)
        .saturating_sub(left.range.start.min(right.range.start));
    gap <= policy.max_coalesce_gap_bytes && merged_length <= policy.max_request_bytes
}

fn object_store_range_diagnostics(
    counts: ObjectStoreRangeCounts,
    status: ObjectStoreRangePlanningStatus,
) -> Vec<Diagnostic> {
    match status {
        ObjectStoreRangePlanningStatus::Planned => Vec::new(),
        ObjectStoreRangePlanningStatus::Unsupported => vec![object_store_range_error(
            DiagnosticCode::InvalidInput,
            "manifest_segments",
            "object-store range planning requires at least one declared segment",
            "Attach manifest segment metadata with byte ranges before planning object-store requests.",
        )],
        ObjectStoreRangePlanningStatus::BlockedNonObjectStore => vec![object_store_range_error(
            DiagnosticCode::ObjectStoreUnsupported,
            "object_store_uri",
            "no object-store input files were declared",
            "Declare S3, GCS, or ADLS file URIs before object-store range planning.",
        )],
        ObjectStoreRangePlanningStatus::BlockedInvalidRanges => vec![object_store_range_error(
            DiagnosticCode::InvalidInput,
            "byte_ranges",
            format!(
                "{} invalid empty byte ranges were declared",
                counts.invalid_ranges
            ),
            "Remove empty byte ranges before planning object-store requests.",
        )],
        ObjectStoreRangePlanningStatus::BlockedRequestBudget => vec![object_store_range_error(
            DiagnosticCode::ResourceBudgetExceeded,
            "request_budget",
            format!(
                "{} byte ranges exceed the per-request byte budget",
                counts.oversized_ranges
            ),
            "Split oversized byte ranges or raise the planning budget explicitly.",
        )],
        ObjectStoreRangePlanningStatus::BlockedMissingByteRanges => {
            vec![object_store_range_error(
                DiagnosticCode::ObjectStoreUnsupported,
                "byte_ranges",
                format!(
                    "{} object-store segments are missing byte ranges",
                    counts.missing_byte_range_segments
                ),
                "Attach segment byte-range metadata before object-store range planning.",
            )]
        }
    }
}

fn object_store_range_error(
    code: DiagnosticCode,
    feature: impl Into<String>,
    message: impl Into<String>,
    suggested_next_step: impl Into<String>,
) -> Diagnostic {
    Diagnostic::new(
        code,
        DiagnosticSeverity::Error,
        DiagnosticCategory::ObjectStore,
        message,
        Some(feature.into()),
        Some("Object-store range planning is report-only and did not read storage.".to_string()),
        Some(suggested_next_step.into()),
        FallbackStatus::disabled_by_policy(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ColumnRef, DatasetFormat, DatasetRef, EncodedSegment, EncodingKind, FileDescriptor,
        FileRole, LayoutKind, LogicalDType, ManifestId, ManifestSegment, Nullability,
        SegmentLayout, SegmentStats, SnapshotId, SnapshotRef,
    };

    fn manifest_with_uri(uri: &str, ranges: Vec<ByteRange>) -> DatasetManifest {
        let dataset_uri = DatasetUri::new(uri).expect("uri");
        let mut manifest = DatasetManifest::new(
            ManifestId::new("m").expect("manifest id"),
            DatasetRef::from_uri(dataset_uri.clone()).expect("dataset ref"),
            SnapshotRef::new(SnapshotId::new("s").expect("snapshot id")),
        );
        let file = FileDescriptor::new(
            dataset_uri,
            DatasetFormat::Vortex,
            FileRole::NativeVortexData,
        )
        .with_size_bytes(128 * 1024 * 1024);
        let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
        layout.byte_ranges = ranges;
        layout.physical_size_bytes = Some(8 * 1024 * 1024);
        let segment = EncodedSegment::new(
            SegmentId::new("s1").expect("segment id"),
            ColumnRef::new("c").expect("column"),
            LogicalDType::Int64,
            Nullability::Nullable,
            layout,
            SegmentStats::with_row_count(64_000),
        );
        manifest.add_file(file.clone());
        manifest.add_segment(ManifestSegment::new(segment, file));
        manifest
    }

    #[test]
    fn plans_s3_ranges_without_io() {
        let manifest = manifest_with_uri(
            "s3://bucket/table.vortex",
            vec![ByteRange::new(0, 1024), ByteRange::new(2048, 1024)],
        );

        let report = plan_object_store_ranges(
            manifest,
            ObjectStoreRangePlanningPolicy {
                max_coalesce_gap_bytes: 2048,
                ..ObjectStoreRangePlanningPolicy::default()
            },
        );

        assert_eq!(report.status, ObjectStoreRangePlanningStatus::Planned);
        assert_eq!(report.object_store_file_count, 1);
        assert_eq!(report.planned_range_count, 2);
        assert_eq!(report.planned_request_count, 1);
        assert_eq!(report.coalesced_range_count, 1);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn missing_byte_ranges_block_planning() {
        let report = plan_object_store_ranges(
            manifest_with_uri("s3://bucket/table.vortex", Vec::new()),
            ObjectStoreRangePlanningPolicy::default(),
        );

        assert_eq!(
            report.status,
            ObjectStoreRangePlanningStatus::BlockedMissingByteRanges
        );
        assert!(report.requires_byte_ranges);
        assert!(report.full_file_read_required);
        assert!(!report.full_file_read_allowed);
        assert!(report.has_errors());
        assert!(report.side_effect_free());
    }

    #[test]
    fn local_files_are_not_object_store_range_targets() {
        let report = plan_object_store_ranges(
            manifest_with_uri("file://tmp/table.vortex", vec![ByteRange::new(0, 1024)]),
            ObjectStoreRangePlanningPolicy::default(),
        );

        assert_eq!(
            report.status,
            ObjectStoreRangePlanningStatus::BlockedNonObjectStore
        );
        assert_eq!(report.object_store_file_count, 0);
        assert!(report.has_errors());
    }

    #[test]
    fn invalid_empty_ranges_block_planning() {
        let report = plan_object_store_ranges(
            manifest_with_uri("s3://bucket/table.vortex", vec![ByteRange::new(0, 0)]),
            ObjectStoreRangePlanningPolicy::default(),
        );

        assert_eq!(
            report.status,
            ObjectStoreRangePlanningStatus::BlockedInvalidRanges
        );
        assert_eq!(report.invalid_range_count, 1);
        assert!(report.has_errors());
    }
}
