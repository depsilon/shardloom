use std::fmt::Write as _;

use shardloom_core::{
    ComparisonOp, Diagnostic, DiagnosticCode, PredicateExpr, PredicateProof, PruningDecision,
    Result, SegmentId, StatValue,
};

/// Conservative status for `Vortex` metadata-driven pruning planning in `ShardLoom`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataPruningStatus {
    Planned,
    MetadataOnly,
    Pruned,
    NeedsEncodedRead,
    NeedsPartialDecode,
    StatisticsUnavailable,
    Unsupported,
}
impl VortexMetadataPruningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataOnly => "metadata_only",
            Self::Pruned => "pruned",
            Self::NeedsEncodedRead => "needs_encoded_read",
            Self::NeedsPartialDecode => "needs_partial_decode",
            Self::StatisticsUnavailable => "statistics_unavailable",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
    #[must_use]
    pub const fn requires_data_read(&self) -> bool {
        matches!(self, Self::NeedsEncodedRead | Self::NeedsPartialDecode)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataPruningMode {
    MetadataOnly,
    Conservative,
    Unsupported,
}
impl VortexMetadataPruningMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::Conservative => "conservative",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn allows_unsafe_pruning(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexMetadataPruningInput {
    pub metadata_summary: crate::VortexMetadataSummaryReport,
    pub predicate: Option<PredicateExpr>,
    pub mode: VortexMetadataPruningMode,
}
impl VortexMetadataPruningInput {
    #[must_use]
    pub fn new(metadata_summary: crate::VortexMetadataSummaryReport) -> Self {
        Self {
            metadata_summary,
            predicate: None,
            mode: VortexMetadataPruningMode::Conservative,
        }
    }
    #[must_use]
    pub fn with_predicate(mut self, predicate: PredicateExpr) -> Self {
        self.predicate = Some(predicate);
        self
    }
    #[must_use]
    pub fn with_mode(mut self, mode: VortexMetadataPruningMode) -> Self {
        self.mode = mode;
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "mode={} predicate_present={}",
            self.mode.as_str(),
            self.predicate.is_some()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexSegmentPruningResult {
    pub segment_id: Option<SegmentId>,
    pub proof: PredicateProof,
    pub decision: PruningDecision,
    pub status: VortexMetadataPruningStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexSegmentPruningResult {
    #[must_use]
    pub fn new(
        segment_id: Option<SegmentId>,
        proof: PredicateProof,
        decision: PruningDecision,
    ) -> Self {
        Self {
            segment_id,
            status: match decision {
                PruningDecision::MetadataOnlyAnswer { .. } => {
                    VortexMetadataPruningStatus::MetadataOnly
                }
                PruningDecision::PruneSegment { .. } => VortexMetadataPruningStatus::Pruned,
                PruningDecision::ReadEncoded { .. } => {
                    VortexMetadataPruningStatus::NeedsEncodedRead
                }
                PruningDecision::NeedPartialDecode { .. }
                | PruningDecision::NeedMaterialization { .. } => {
                    VortexMetadataPruningStatus::NeedsPartialDecode
                }
                PruningDecision::Unsupported { .. } => VortexMetadataPruningStatus::Unsupported,
            },
            proof,
            decision,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn metadata_only(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        Self::new(
            segment_id,
            PredicateProof::AlwaysTrue {
                reason: reason.into(),
            },
            PruningDecision::MetadataOnlyAnswer {
                reason: "metadata-only".to_string(),
            },
        )
    }
    #[must_use]
    pub fn pruned(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        let r = reason.into();
        Self::new(
            segment_id,
            PredicateProof::AlwaysFalse { reason: r.clone() },
            PruningDecision::PruneSegment { reason: r },
        )
    }
    #[must_use]
    pub fn needs_encoded_read(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        let r = reason.into();
        Self::new(
            segment_id,
            PredicateProof::MayMatch { reason: r.clone() },
            PruningDecision::ReadEncoded { reason: r },
        )
    }
    #[must_use]
    pub fn needs_partial_decode(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        let r = reason.into();
        Self::new(
            segment_id,
            PredicateProof::Unknown { reason: r.clone() },
            PruningDecision::NeedPartialDecode { reason: r },
        )
    }
    #[must_use]
    pub fn statistics_unavailable(
        segment_id: Option<SegmentId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            status: VortexMetadataPruningStatus::StatisticsUnavailable,
            ..Self::needs_partial_decode(segment_id, reason)
        }
    }
    #[must_use]
    pub fn unsupported(
        segment_id: Option<SegmentId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let mut out = Self::new(
            segment_id,
            PredicateProof::Unsupported {
                reason: reason.clone(),
            },
            PruningDecision::Unsupported {
                reason: reason.clone(),
            },
        );
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback execution remains disabled".to_string()),
        ));
        out
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn requires_data_read(&self) -> bool {
        self.status.requires_data_read()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "segment={} status={} decision={}",
            self.segment_id
                .as_ref()
                .map_or("<unknown>", SegmentId::as_str),
            self.status.as_str(),
            self.decision.reason()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataPruningReport {
    pub status: VortexMetadataPruningStatus,
    pub mode: VortexMetadataPruningMode,
    pub metadata_planning: Option<crate::VortexMetadataPlanningReport>,
    pub results: Vec<VortexSegmentPruningResult>,
    pub segments_considered: usize,
    pub segments_pruned: usize,
    pub segments_metadata_answered: usize,
    pub segments_requiring_read: usize,
    pub data_executed: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataPruningReport {
    /// # Errors
    /// Returns errors from planning bridge construction.
    pub fn from_input(input: VortexMetadataPruningInput) -> Result<Self> {
        let planning =
            crate::VortexMetadataPlanningReport::from_metadata_summary(input.metadata_summary)?;
        Self::from_planning_report(planning, input.predicate)
    }
    /// # Errors
    /// Returns errors only from `SegmentId`/plan validation paths.
    #[allow(clippy::needless_pass_by_value)]
    pub fn from_planning_report(
        planning_report: crate::VortexMetadataPlanningReport,
        predicate: Option<PredicateExpr>,
    ) -> Result<Self> {
        if planning_report.has_errors() {
            let mut out = Self {
                status: VortexMetadataPruningStatus::Unsupported,
                mode: VortexMetadataPruningMode::Unsupported,
                metadata_planning: Some(planning_report.clone()),
                results: vec![],
                segments_considered: 0,
                segments_pruned: 0,
                segments_metadata_answered: 0,
                segments_requiring_read: 0,
                data_executed: false,
                data_materialized: false,
                object_store_io: planning_report.object_store_io,
                write_io: planning_report.write_io,
                fallback_execution_allowed: false,
                diagnostics: planning_report.diagnostics.clone(),
            };
            out.recompute_counts();
            return Ok(out);
        }
        #[allow(clippy::needless_pass_by_value)]
        let mut out = Self {
            status: VortexMetadataPruningStatus::Planned,
            mode: VortexMetadataPruningMode::Conservative,
            metadata_planning: Some(planning_report.clone()),
            results: vec![],
            segments_considered: 0,
            segments_pruned: 0,
            segments_metadata_answered: 0,
            segments_requiring_read: 0,
            data_executed: false,
            data_materialized: false,
            object_store_io: planning_report.object_store_io,
            write_io: planning_report.write_io,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        let segs = &planning_report.metadata_summary.summary.segments;
        if segs.is_empty() {
            out.status = VortexMetadataPruningStatus::StatisticsUnavailable;
            out.recompute_counts();
            return Ok(out);
        }
        for seg in segs {
            let sid = seg.segment_id.clone();
            let result = if let Some(pred) = &predicate {
                match prove_predicate_from_segment_stats(pred, seg) {
                    PredicateProof::AlwaysFalse { reason } => {
                        VortexSegmentPruningResult::pruned(sid, reason)
                    }
                    PredicateProof::AlwaysTrue { reason } | PredicateProof::MayMatch { reason } => {
                        VortexSegmentPruningResult::needs_encoded_read(sid, reason)
                    }
                    PredicateProof::Unknown { reason } => {
                        VortexSegmentPruningResult::needs_partial_decode(sid, reason)
                    }
                    PredicateProof::Unsupported { reason } => {
                        VortexSegmentPruningResult::unsupported(
                            sid,
                            "metadata_pruning_predicate",
                            reason,
                        )
                    }
                }
            } else {
                VortexSegmentPruningResult::needs_encoded_read(sid, "no predicate provided")
            };
            out.add_result(result);
        }
        out.recompute_counts();
        out.status = if out.segments_pruned > 0 {
            VortexMetadataPruningStatus::Pruned
        } else {
            VortexMetadataPruningStatus::Planned
        };
        Ok(out)
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let reason = reason.into();
        let mut out = Self {
            status: VortexMetadataPruningStatus::Unsupported,
            mode: VortexMetadataPruningMode::Unsupported,
            metadata_planning: None,
            results: vec![],
            segments_considered: 0,
            segments_pruned: 0,
            segments_metadata_answered: 0,
            segments_requiring_read: 0,
            data_executed: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback execution remains disabled".to_string()),
        ));
        out
    }
    pub fn add_result(&mut self, result: VortexSegmentPruningResult) {
        self.results.push(result);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn recompute_counts(&mut self) {
        self.segments_considered = self.results.len();
        self.segments_pruned = self
            .results
            .iter()
            .filter(|r| matches!(r.decision, PruningDecision::PruneSegment { .. }))
            .count();
        self.segments_metadata_answered = self
            .results
            .iter()
            .filter(|r| matches!(r.decision, PruningDecision::MetadataOnlyAnswer { .. }))
            .count();
        self.segments_requiring_read = self
            .results
            .iter()
            .filter(|r| r.requires_data_read())
            .count();
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
            || self
                .results
                .iter()
                .any(VortexSegmentPruningResult::has_errors)
    }
    #[must_use]
    pub const fn is_plan_only(&self) -> bool {
        !self.data_executed
            && !self.data_materialized
            && !self.write_io
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "Vortex metadata pruning\npruning status: {}\nmode: {}\nsegments considered: {}\nsegments pruned: {}\nsegments metadata answered: {}\nsegments requiring read: {}\ndata executed: {}\ndata materialized: {}\nobject-store IO: {}\nwrite IO: {}\nfallback execution disabled",
            self.status.as_str(),
            self.mode.as_str(),
            self.segments_considered,
            self.segments_pruned,
            self.segments_metadata_answered,
            self.segments_requiring_read,
            self.data_executed,
            self.data_materialized,
            self.object_store_io,
            self.write_io
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

/// # Errors
/// Returns any error produced while constructing the metadata pruning report.
pub fn plan_vortex_metadata_pruning(
    planning_report: crate::VortexMetadataPlanningReport,
    predicate: Option<PredicateExpr>,
) -> Result<VortexMetadataPruningReport> {
    VortexMetadataPruningReport::from_planning_report(planning_report, predicate)
}
#[must_use]
pub fn metadata_pruning_is_side_effect_free(report: &VortexMetadataPruningReport) -> bool {
    report.is_plan_only()
        && !report.object_store_io
        && !report.write_io
        && !report.fallback_execution_allowed
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn prove_predicate_from_segment_stats(
    predicate: &PredicateExpr,
    segment: &crate::VortexSegmentMetadataSummary,
) -> PredicateProof {
    match predicate {
        PredicateExpr::AlwaysTrue => {
            return PredicateProof::AlwaysTrue {
                reason: "always true predicate".to_string(),
            };
        }
        PredicateExpr::AlwaysFalse => {
            return PredicateProof::AlwaysFalse {
                reason: "always false predicate".to_string(),
            };
        }
        PredicateExpr::IsNull { .. }
        | PredicateExpr::IsNotNull { .. }
        | PredicateExpr::Compare { .. } => {}
    }

    let Some(column_stats) = (match predicate {
        PredicateExpr::IsNull { column }
        | PredicateExpr::IsNotNull { column }
        | PredicateExpr::Compare { column, .. } => segment
            .columns
            .iter()
            .find(|c| c.column.as_ref() == Some(column)),
        PredicateExpr::AlwaysTrue | PredicateExpr::AlwaysFalse => None,
    }) else {
        return PredicateProof::Unknown {
            reason: "missing segment stats for predicate column".to_string(),
        };
    };
    let stats = &column_stats.stats;
    match predicate {
        PredicateExpr::AlwaysTrue => PredicateProof::AlwaysTrue {
            reason: "always true predicate".to_string(),
        },
        PredicateExpr::AlwaysFalse => PredicateProof::AlwaysFalse {
            reason: "always false predicate".to_string(),
        },
        PredicateExpr::IsNull { .. } => match (stats.row_count, stats.null_count) {
            (_, Some(0)) => PredicateProof::AlwaysFalse {
                reason: "null_count == 0".to_string(),
            },
            (Some(r), Some(n)) if r == n => PredicateProof::AlwaysTrue {
                reason: "all rows are null".to_string(),
            },
            _ => PredicateProof::Unknown {
                reason: "insufficient null statistics".to_string(),
            },
        },
        PredicateExpr::IsNotNull { .. } => match (stats.row_count, stats.null_count) {
            (_, Some(0)) => PredicateProof::AlwaysTrue {
                reason: "null_count == 0".to_string(),
            },
            (Some(r), Some(n)) if r == n => PredicateProof::AlwaysFalse {
                reason: "all rows are null".to_string(),
            },
            _ => PredicateProof::Unknown {
                reason: "insufficient null statistics".to_string(),
            },
        },
        PredicateExpr::Compare { op, value, .. } => {
            let (Some(min), Some(max)) = (&stats.min_value, &stats.max_value) else {
                return PredicateProof::Unknown {
                    reason: "min/max statistics unavailable".to_string(),
                };
            };
            let max_ord = cmp(max, value);
            let min_ord = cmp(min, value);
            match op {
                ComparisonOp::Gt if matches!(max_ord, Some(v) if v <= 0) => {
                    PredicateProof::AlwaysFalse {
                        reason: "max <= value".to_string(),
                    }
                }
                ComparisonOp::GtEq if matches!(max_ord, Some(v) if v < 0) => {
                    PredicateProof::AlwaysFalse {
                        reason: "max < value".to_string(),
                    }
                }
                ComparisonOp::Lt if matches!(min_ord, Some(v) if v >= 0) => {
                    PredicateProof::AlwaysFalse {
                        reason: "min >= value".to_string(),
                    }
                }
                ComparisonOp::LtEq if matches!(min_ord, Some(v) if v > 0) => {
                    PredicateProof::AlwaysFalse {
                        reason: "min > value".to_string(),
                    }
                }
                ComparisonOp::Eq => {
                    if let (Some(c1), Some(c2)) = (cmp(value, min), cmp(value, max)) {
                        if c1 < 0 || c2 > 0 {
                            return PredicateProof::AlwaysFalse {
                                reason: "value outside min/max".to_string(),
                            };
                        }
                    }
                    PredicateProof::MayMatch {
                        reason: "min/max cannot exclude eq".to_string(),
                    }
                }
                ComparisonOp::NotEq => PredicateProof::MayMatch {
                    reason: "conservative not-eq proof".to_string(),
                },
                _ => PredicateProof::MayMatch {
                    reason: "min/max cannot exclude".to_string(),
                },
            }
        }
    }
}

fn cmp(a: &StatValue, b: &StatValue) -> Option<i8> {
    match (a, b) {
        (StatValue::Int64(x), StatValue::Int64(y)) => Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        (StatValue::UInt64(x), StatValue::UInt64(y)) => Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        (StatValue::Float64(x), StatValue::Float64(y)) => x.partial_cmp(y).map(|o| match o {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        (StatValue::Utf8(x), StatValue::Utf8(y)) => Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        (StatValue::Boolean(x), StatValue::Boolean(y)) => Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{ColumnRef, ComparisonOp, SegmentStats};
    fn seg(stats: SegmentStats) -> crate::VortexSegmentMetadataSummary {
        let mut s = crate::VortexSegmentMetadataSummary::unknown();
        let mut c = crate::VortexColumnMetadataSummary::new(ColumnRef::new("x").unwrap());
        c.stats = stats;
        s.add_column(c);
        s
    }
    #[test]
    fn status_unsupported_error() {
        assert!(VortexMetadataPruningStatus::Unsupported.is_error());
    }
    #[test]
    fn status_needs_read() {
        assert!(VortexMetadataPruningStatus::NeedsEncodedRead.requires_data_read());
    }
    #[test]
    fn status_pruned_no_read() {
        assert!(!VortexMetadataPruningStatus::Pruned.requires_data_read());
    }
    #[test]
    fn mode_cons_no_unsafe() {
        assert!(!VortexMetadataPruningMode::Conservative.allows_unsafe_pruning());
    }
    #[test]
    fn result_pruned_no_read() {
        assert!(!VortexSegmentPruningResult::pruned(None, "r").requires_data_read());
    }
    #[test]
    fn result_encoded_read_yes() {
        assert!(VortexSegmentPruningResult::needs_encoded_read(None, "r").requires_data_read());
    }
    #[test]
    fn unsupported_result_has_error() {
        let r = VortexSegmentPruningResult::unsupported(None, "f", "r");
        assert!(r.has_errors());
        assert!(!r.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn unsupported_report_has_error() {
        let r = VortexMetadataPruningReport::unsupported("f", "r");
        assert!(r.has_errors());
        assert!(!r.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn prove_missing_unknown() {
        let p = PredicateExpr::IsNull {
            column: ColumnRef::new("x").unwrap(),
        };
        assert!(matches!(
            prove_predicate_from_segment_stats(&p, &crate::VortexSegmentMetadataSummary::unknown()),
            PredicateProof::Unknown { .. }
        ));
    }
    #[test]
    fn prove_is_null_zero() {
        let mut st = SegmentStats::unknown();
        st.null_count = Some(0);
        let p = PredicateExpr::IsNull {
            column: ColumnRef::new("x").unwrap(),
        };
        assert!(matches!(
            prove_predicate_from_segment_stats(&p, &seg(st)),
            PredicateProof::AlwaysFalse { .. }
        ));
    }
    #[test]
    fn prove_is_not_null_zero() {
        let mut st = SegmentStats::unknown();
        st.null_count = Some(0);
        let p = PredicateExpr::IsNotNull {
            column: ColumnRef::new("x").unwrap(),
        };
        assert!(matches!(
            prove_predicate_from_segment_stats(&p, &seg(st)),
            PredicateProof::AlwaysTrue { .. }
        ));
    }
    #[test]
    fn prove_all_null_is_null_true() {
        let mut st = SegmentStats::unknown();
        st.row_count = Some(10);
        st.null_count = Some(10);
        let p = PredicateExpr::IsNull {
            column: ColumnRef::new("x").unwrap(),
        };
        assert!(matches!(
            prove_predicate_from_segment_stats(&p, &seg(st)),
            PredicateProof::AlwaysTrue { .. }
        ));
    }
    #[test]
    fn prove_all_null_not_null_false() {
        let mut st = SegmentStats::unknown();
        st.row_count = Some(10);
        st.null_count = Some(10);
        let p = PredicateExpr::IsNotNull {
            column: ColumnRef::new("x").unwrap(),
        };
        assert!(matches!(
            prove_predicate_from_segment_stats(&p, &seg(st)),
            PredicateProof::AlwaysFalse { .. }
        ));
    }
    #[test]
    fn prove_gt_max_false() {
        let mut st = SegmentStats::unknown();
        st.min_value = Some(StatValue::Int64(1));
        st.max_value = Some(StatValue::Int64(5));
        let p = PredicateExpr::Compare {
            column: ColumnRef::new("x").unwrap(),
            op: ComparisonOp::Gt,
            value: StatValue::Int64(6),
        };
        assert!(matches!(
            prove_predicate_from_segment_stats(&p, &seg(st)),
            PredicateProof::AlwaysFalse { .. }
        ));
    }
    #[test]
    fn prove_may_match() {
        let mut st = SegmentStats::unknown();
        st.min_value = Some(StatValue::Int64(1));
        st.max_value = Some(StatValue::Int64(10));
        let p = PredicateExpr::Compare {
            column: ColumnRef::new("x").unwrap(),
            op: ComparisonOp::Gt,
            value: StatValue::Int64(3),
        };
        assert!(matches!(
            prove_predicate_from_segment_stats(&p, &seg(st)),
            PredicateProof::MayMatch { .. }
        ));
    }
}
