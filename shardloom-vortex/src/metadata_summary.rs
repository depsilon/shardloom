//! `Vortex` metadata normalization for `ShardLoom` planning.
//!
//! This module converts metadata-only probe reports into stable `ShardLoom`
//! summaries. It does not run scan execution, does not decode or materialize
//! data, does not perform object-store IO, does not write files, and does not
//! enable fallback execution.

use std::fmt::Write as _;

use shardloom_core::{
    ColumnRef, DatasetUri, Diagnostic, DiagnosticCode, EncodingKind, LayoutKind, LogicalDType,
    Nullability, SegmentId, SegmentStats,
};

/// Normalization status for `Vortex` metadata probe summaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataSummaryStatus {
    Summarized,
    ProbeDeferred,
    MetadataUnavailable,
    Unsupported,
}
impl VortexMetadataSummaryStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Summarized => "summarized",
            Self::ProbeDeferred => "probe_deferred",
            Self::MetadataUnavailable => "metadata_unavailable",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

/// Availability state for `Vortex` metadata facets used in planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataAvailability {
    Available,
    PartiallyAvailable,
    Deferred,
    Unavailable,
    Unknown,
}
impl VortexMetadataAvailability {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::PartiallyAvailable => "partially_available",
            Self::Deferred => "deferred",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self, Self::Available | Self::PartiallyAvailable)
    }
}

/// Column-level `Vortex` metadata summary for planning-time `ShardLoom` logic.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexColumnMetadataSummary {
    pub column: Option<ColumnRef>,
    pub dtype: LogicalDType,
    pub nullability: Nullability,
    pub encoding: EncodingKind,
    pub layout: LayoutKind,
    pub stats: SegmentStats,
    pub statistics_available: bool,
    pub byte_ranges_available: bool,
}
impl VortexColumnMetadataSummary {
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            column: None,
            dtype: LogicalDType::Unknown,
            nullability: Nullability::Unknown,
            encoding: EncodingKind::Unknown,
            layout: LayoutKind::Unknown,
            stats: SegmentStats::unknown(),
            statistics_available: false,
            byte_ranges_available: false,
        }
    }
    #[must_use]
    pub fn new(column: ColumnRef) -> Self {
        Self {
            column: Some(column),
            ..Self::unknown()
        }
    }
    #[must_use]
    pub fn with_dtype(mut self, dtype: LogicalDType) -> Self {
        self.dtype = dtype;
        self
    }
    #[must_use]
    pub fn with_nullability(mut self, nullability: Nullability) -> Self {
        self.nullability = nullability;
        self
    }
    #[must_use]
    pub fn with_encoding(mut self, encoding: EncodingKind) -> Self {
        self.encoding = encoding;
        self
    }
    #[must_use]
    pub fn with_layout(mut self, layout: LayoutKind) -> Self {
        self.layout = layout;
        self
    }
    #[must_use]
    pub fn with_stats(mut self, stats: SegmentStats) -> Self {
        self.stats = stats;
        self
    }
    #[must_use]
    pub fn with_statistics_available(mut self, value: bool) -> Self {
        self.statistics_available = value;
        self
    }
    #[must_use]
    pub fn with_byte_ranges_available(mut self, value: bool) -> Self {
        self.byte_ranges_available = value;
        self
    }
    #[must_use]
    pub fn can_use_metadata(&self) -> bool {
        self.statistics_available || self.stats.row_count.is_some() || self.stats.has_min_max()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "column={} dtype={} nullability={} encoding={} layout={} stats_available={} byte_ranges_available={}",
            self.column.as_ref().map_or("<unknown>", ColumnRef::as_str),
            self.dtype.as_str(),
            self.nullability.as_str(),
            self.encoding.as_str(),
            self.layout.as_str(),
            self.statistics_available,
            self.byte_ranges_available
        )
    }
}

/// Segment-level `Vortex` metadata summary for planning-time pruning/explain input.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexSegmentMetadataSummary {
    pub segment_id: Option<SegmentId>,
    pub row_count: Option<u64>,
    pub columns: Vec<VortexColumnMetadataSummary>,
    pub statistics_available: bool,
    pub encoding_layout_available: bool,
    pub byte_ranges_available: bool,
}
impl VortexSegmentMetadataSummary {
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            segment_id: None,
            row_count: None,
            columns: vec![],
            statistics_available: false,
            encoding_layout_available: false,
            byte_ranges_available: false,
        }
    }
    #[must_use]
    pub fn with_segment_id(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }
    #[must_use]
    pub fn with_row_count(mut self, row_count: u64) -> Self {
        self.row_count = Some(row_count);
        self
    }
    pub fn add_column(&mut self, column: VortexColumnMetadataSummary) {
        self.columns.push(column);
    }
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
    #[must_use]
    pub fn can_use_metadata(&self) -> bool {
        self.statistics_available
            || self
                .columns
                .iter()
                .any(VortexColumnMetadataSummary::can_use_metadata)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "segment={} row_count={} columns={} statistics_available={} encoding_layout_available={} byte_ranges_available={}",
            self.segment_id
                .as_ref()
                .map_or("<unknown>", SegmentId::as_str),
            self.row_count
                .map_or_else(|| "unknown".to_string(), |v| v.to_string()),
            self.column_count(),
            self.statistics_available,
            self.encoding_layout_available,
            self.byte_ranges_available
        )
    }
}

/// File-level `Vortex` metadata summary for `ShardLoom` planning boundaries.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexFileMetadataSummary {
    pub uri: Option<DatasetUri>,
    pub metadata_available: VortexMetadataAvailability,
    pub schema_available: VortexMetadataAvailability,
    pub statistics_available: VortexMetadataAvailability,
    pub encoding_layout_available: VortexMetadataAvailability,
    pub row_count: Option<u64>,
    pub segments: Vec<VortexSegmentMetadataSummary>,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
}
impl VortexFileMetadataSummary {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            uri: None,
            metadata_available: VortexMetadataAvailability::Unknown,
            schema_available: VortexMetadataAvailability::Unknown,
            statistics_available: VortexMetadataAvailability::Unknown,
            encoding_layout_available: VortexMetadataAvailability::Unknown,
            row_count: None,
            segments: vec![],
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
        }
    }
    #[must_use]
    pub fn from_probe_report(report: &crate::VortexMetadataProbeReport) -> Self {
        let mut out = Self::empty();
        out.uri.clone_from(&report.target_uri);
        out.metadata_available = if report.metadata_available {
            VortexMetadataAvailability::Available
        } else if matches!(
            report.status,
            crate::VortexMetadataIoStatus::DeferredApiUnclear
                | crate::VortexMetadataIoStatus::DeferredApiUnstable
        ) {
            VortexMetadataAvailability::Deferred
        } else {
            VortexMetadataAvailability::Unavailable
        };
        out.schema_available = if report.schema_available {
            VortexMetadataAvailability::Available
        } else {
            VortexMetadataAvailability::Unavailable
        };
        out.statistics_available = if report.statistics_available {
            VortexMetadataAvailability::Available
        } else {
            VortexMetadataAvailability::Unavailable
        };
        out.encoding_layout_available = if report.encoding_layout_available {
            VortexMetadataAvailability::Available
        } else {
            VortexMetadataAvailability::Unavailable
        };
        out.data_materialized = report.data_materialized;
        out.object_store_io = report.object_store_io;
        out.write_io = report.write_io;
        out.fallback_execution_allowed = report.fallback_execution_allowed;
        out
    }
    pub fn add_segment(&mut self, segment: VortexSegmentMetadataSummary) {
        self.segments.push(segment);
    }
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
    #[must_use]
    pub fn column_count_estimate(&self) -> usize {
        self.segments
            .iter()
            .map(VortexSegmentMetadataSummary::column_count)
            .sum()
    }
    #[must_use]
    pub fn can_use_metadata(&self) -> bool {
        self.metadata_available.is_available()
            || self
                .segments
                .iter()
                .any(VortexSegmentMetadataSummary::can_use_metadata)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "uri={} metadata={} schema={} statistics={} encoding_layout={} row_count={} segments={} data_materialized={} object_store_io={} write_io={} fallback_execution_allowed={}",
            self.uri.as_ref().map_or("<unknown>", DatasetUri::as_str),
            self.metadata_available.as_str(),
            self.schema_available.as_str(),
            self.statistics_available.as_str(),
            self.encoding_layout_available.as_str(),
            self.row_count
                .map_or_else(|| "unknown".to_string(), |v| v.to_string()),
            self.segment_count(),
            self.data_materialized,
            self.object_store_io,
            self.write_io,
            self.fallback_execution_allowed
        )
    }
}

/// Normalized metadata summary report for `ShardLoom` planner integration.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexMetadataSummaryReport {
    pub status: VortexMetadataSummaryStatus,
    pub summary: VortexFileMetadataSummary,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataSummaryReport {
    #[must_use]
    pub fn from_probe_report(report: &crate::VortexMetadataProbeReport) -> Self {
        let status = if report
            .diagnostics
            .iter()
            .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
            && matches!(report.status, crate::VortexMetadataIoStatus::Unsupported)
        {
            VortexMetadataSummaryStatus::Unsupported
        } else if report.metadata_available {
            VortexMetadataSummaryStatus::Summarized
        } else if matches!(
            report.status,
            crate::VortexMetadataIoStatus::DeferredApiUnclear
                | crate::VortexMetadataIoStatus::DeferredApiUnstable
        ) {
            VortexMetadataSummaryStatus::ProbeDeferred
        } else {
            VortexMetadataSummaryStatus::MetadataUnavailable
        };
        Self {
            status,
            summary: VortexFileMetadataSummary::from_probe_report(report),
            diagnostics: report.diagnostics.clone(),
        }
    }
    #[must_use]
    pub fn probe_deferred(report: &crate::VortexMetadataProbeReport) -> Self {
        let mut out = Self::from_probe_report(report);
        out.status = VortexMetadataSummaryStatus::ProbeDeferred;
        out
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            status: VortexMetadataSummaryStatus::Unsupported,
            summary: VortexFileMetadataSummary::empty(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                None,
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
            || self.status.is_error()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "Vortex metadata summary\nsummary status: {}\nmetadata availability: {}\nschema availability: {}\nstatistics availability: {}\nencoding/layout availability: {}",
            self.status.as_str(),
            self.summary.metadata_available.as_str(),
            self.summary.schema_available.as_str(),
            self.summary.statistics_available.as_str(),
            self.summary.encoding_layout_available.as_str()
        );
        if let Some(row_count) = self.summary.row_count {
            let _ = write!(out, "\nrow count: {row_count}");
        }
        let _ = write!(
            out,
            "\nsegment count: {}\ndata materialized: {}\nobject-store IO: {}\nwrite IO: {}\nfallback execution allowed: {}",
            self.summary.segment_count(),
            self.summary.data_materialized,
            self.summary.object_store_io,
            self.summary.write_io,
            self.summary.fallback_execution_allowed
        );
        if self.diagnostics.is_empty() {
            out.push_str("\ndiagnostics: none");
        } else {
            out.push_str("\ndiagnostics:");
            for d in &self.diagnostics {
                let _ = write!(out, "\n- {}", d.to_human_text());
            }
        }
        out
    }
}

/// Summarizes a `Vortex` metadata probe without performing any IO.
#[must_use]
pub fn summarize_vortex_metadata_probe(
    report: &crate::VortexMetadataProbeReport,
) -> VortexMetadataSummaryReport {
    VortexMetadataSummaryReport::from_probe_report(report)
}

/// Returns whether a metadata summary report is plan-only and side-effect free.
#[must_use]
pub fn metadata_summary_is_plan_only(report: &VortexMetadataSummaryReport) -> bool {
    !report.summary.data_materialized
        && !report.summary.write_io
        && !report.summary.object_store_io
        && !report.summary.fallback_execution_allowed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn availability_flags() {
        assert!(VortexMetadataAvailability::Available.is_available());
        assert!(!VortexMetadataAvailability::Deferred.is_available());
    }
    #[test]
    fn unsupported_is_error() {
        assert!(VortexMetadataSummaryStatus::Unsupported.is_error());
    }
    #[test]
    fn column_unknown_defaults() {
        let c = VortexColumnMetadataSummary::unknown();
        assert_eq!(c.dtype, LogicalDType::Unknown);
        assert_eq!(c.encoding, EncodingKind::Unknown);
        assert_eq!(c.layout, LayoutKind::Unknown);
    }
    #[test]
    fn column_stats_enable_metadata() {
        let c = VortexColumnMetadataSummary::unknown().with_stats(SegmentStats::with_row_count(3));
        assert!(c.can_use_metadata());
    }
    #[test]
    fn segment_unknown_empty() {
        assert_eq!(VortexSegmentMetadataSummary::unknown().column_count(), 0);
    }
    #[test]
    fn add_column_increments() {
        let mut s = VortexSegmentMetadataSummary::unknown();
        let col = ColumnRef::new("a").expect("column");
        s.add_column(VortexColumnMetadataSummary::new(col));
        assert_eq!(s.column_count(), 1);
    }
    #[test]
    fn file_empty_flags_false() {
        let f = VortexFileMetadataSummary::empty();
        assert!(
            !f.data_materialized
                && !f.object_store_io
                && !f.write_io
                && !f.fallback_execution_allowed
        );
    }
    #[test]
    fn from_probe_preserves_io_booleans_false_deferred() {
        let p = crate::VortexMetadataProbeReport::deferred_api_unclear();
        let f = VortexFileMetadataSummary::from_probe_report(&p);
        assert!(
            !f.data_materialized
                && !f.object_store_io
                && !f.write_io
                && !f.fallback_execution_allowed
        );
    }
    #[test]
    fn deferred_probe_status() {
        let p = crate::VortexMetadataProbeReport::deferred_api_unclear();
        let r = VortexMetadataSummaryReport::from_probe_report(&p);
        assert_eq!(r.status, VortexMetadataSummaryStatus::ProbeDeferred);
    }
    #[test]
    fn human_text_fields() {
        let mut p = crate::VortexMetadataProbeReport::deferred_api_unclear();
        p.add_diagnostic(Diagnostic::no_fallback_execution("x"));
        let t = VortexMetadataSummaryReport::from_probe_report(&p).to_human_text();
        assert!(t.contains("data materialized: false"));
        assert!(t.contains("object-store IO: false"));
        assert!(t.contains("write IO: false"));
        assert!(t.contains("fallback execution allowed: false"));
        assert!(t.contains("diagnostics:"));
    }
    #[test]
    fn severity_errors() {
        let p = crate::VortexMetadataProbeReport::unsupported("f", "r");
        assert!(VortexMetadataSummaryReport::from_probe_report(&p).has_errors());
    }
    #[test]
    fn summarize_no_io_and_plan_only() {
        let p = crate::VortexMetadataProbeReport::deferred_api_unclear();
        let r = summarize_vortex_metadata_probe(&p);
        assert!(metadata_summary_is_plan_only(&r));
        assert_eq!(r.summary.segment_count(), 0);
    }
}
