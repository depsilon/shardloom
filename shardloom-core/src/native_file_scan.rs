//! Completed native-file scan contracts and positional-read observations.
//!
//! Column names describe logical work requested from a provider. They do not
//! measure physical leaf reads, decoded bytes, or independent segment pruning.

/// The actual owner that completed a native-file operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeFileScanRoute {
    RelationalScan,
    Projection,
    MetadataCount,
    CountWhere,
    Aggregate,
    AggregatePass,
    Sort,
    SortPayload,
}

impl NativeFileScanRoute {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RelationalScan => "relational_scan",
            Self::Projection => "projection",
            Self::MetadataCount => "metadata_count",
            Self::CountWhere => "count_where",
            Self::Aggregate => "aggregate",
            Self::AggregatePass => "aggregate_pass",
            Self::Sort => "sort",
            Self::SortPayload => "sort_payload",
        }
    }
}

/// One executed source/column contract, aggregated across repeated invocations.
/// Unknown dependencies remain `None`; `Some(vec![])` means no columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFileScanContract {
    /// Operation-local ordinal into `NativeFileScanEvidence::sources`.
    pub source: usize,
    pub route: NativeFileScanRoute,
    pub source_rows: u64,
    pub available_columns: u64,
    pub projected_columns: Option<Vec<String>>,
    /// Names emitted by this scan owner, before a parent applies its aliases.
    pub output_columns: Option<Vec<String>>,
    pub pushed_predicate_columns: Option<Vec<String>>,
    pub residual_predicate_columns: Option<Vec<String>>,
    pub invocations: u64,
    /// `None` when an owner does not expose all internal passes and retries.
    /// Unknown is distinct from metadata-only completion (`Some(0)`).
    pub provider_scan_invocations: Option<u64>,
    /// Rows delivered by the observed provider/owner, including any discarded
    /// partial pass before an explicitly admitted native retry.
    pub delivered_rows: u64,
    pub whole_file_pruning_checks: u64,
    pub whole_file_exclusions: u64,
}

impl NativeFileScanContract {
    /// Compare the stable contract without retaining predicate literals or a
    /// per-invocation trace. Counter values do not participate in identity.
    #[must_use]
    pub fn same_contract(&self, other: &Self) -> bool {
        self.source == other.source
            && self.route == other.route
            && self.source_rows == other.source_rows
            && self.available_columns == other.available_columns
            && self.projected_columns == other.projected_columns
            && self.output_columns == other.output_columns
            && self.pushed_predicate_columns == other.pushed_predicate_columns
            && self.residual_predicate_columns == other.residual_predicate_columns
    }

    /// Merge identical completed contracts atomically.
    /// # Errors
    /// Rejects a different contract or overflow without changing the receiver.
    pub fn merge(&mut self, other: &Self) -> crate::Result<()> {
        if !self.same_contract(other) {
            return Err(crate::ShardLoomError::InvalidOperation(
                "native file scan evidence cannot merge different contracts".into(),
            ));
        }
        let add = |left: u64, right: u64| {
            left.checked_add(right).ok_or_else(|| {
                crate::ShardLoomError::InvalidOperation(
                    "native file scan evidence counter overflow".into(),
                )
            })
        };
        let invocations = add(self.invocations, other.invocations)?;
        let scans = self
            .provider_scan_invocations
            .zip(other.provider_scan_invocations)
            .map(|(left, right)| add(left, right))
            .transpose()?;
        let rows = add(self.delivered_rows, other.delivered_rows)?;
        let checks = add(
            self.whole_file_pruning_checks,
            other.whole_file_pruning_checks,
        )?;
        let exclusions = add(self.whole_file_exclusions, other.whole_file_exclusions)?;
        self.invocations = invocations;
        self.provider_scan_invocations = scans;
        self.delivered_rows = rows;
        self.whole_file_pruning_checks = checks;
        self.whole_file_exclusions = exclusions;
        Ok(())
    }
}

/// Reads completed under one held file generation during the current operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFileSourceObservation {
    pub source_uri: String,
    /// Descriptor/path metadata identity, not a content checksum or snapshot.
    pub generation_fingerprint: String,
    pub segment_requests: u64,
    /// Successful positional read calls; includes repeated and coalesced reads.
    pub completed_read_requests: u64,
    /// Successful requested-range bytes, including coalescing gaps and repeats.
    /// This does not measure unique bytes, decoded bytes, or storage-device I/O.
    pub completed_read_bytes: u64,
    /// Actual current-operation immutable segment reuse, separate from reads.
    pub segment_reuse: NativeSourceSegmentReuseWork,
}

/// Current-operation logical segment work. Hit bytes are not physical bytes
/// saved: another miss may still coalesce a range containing the cached segment.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeSourceSegmentReuseWork {
    pub lookups: u64,
    pub hits: u64,
    /// Hits published by a different successfully completed native operation.
    pub cross_call_hits: u64,
    pub misses: u64,
    pub hit_segment_bytes: u64,
    pub cross_call_hit_segment_bytes: u64,
    pub stored_segments: u64,
    pub copied_segment_bytes: u64,
    pub retention_declines: u64,
}

/// Shared session state at completion, not an operation-exclusive delta.
/// Retained capacity includes evicted copies still held by a result or slice.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeSourceSegmentCacheSnapshot {
    pub enabled: bool,
    pub retained_entries: u64,
    pub table_reserved_bytes: u64,
    pub retained_capacity_bytes: u64,
    pub retention_limit_bytes: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub pressure_reclamations: u64,
    pub counters_overflowed: bool,
}

/// Actual native-file observations; an empty report means no owner reported.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NativeFileScanEvidence {
    pub sources: Vec<NativeFileSourceObservation>,
    pub contracts: Vec<NativeFileScanContract>,
    /// `None` means this report did not observe a resident segment store.
    pub session_segment_cache: Option<NativeSourceSegmentCacheSnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract() -> NativeFileScanContract {
        NativeFileScanContract {
            source: 0,
            route: NativeFileScanRoute::RelationalScan,
            source_rows: 10,
            available_columns: 3,
            projected_columns: Some(vec!["amount".into()]),
            output_columns: Some(vec!["amount".into()]),
            pushed_predicate_columns: Some(vec!["key".into()]),
            residual_predicate_columns: None,
            invocations: 1,
            provider_scan_invocations: Some(1),
            delivered_rows: 5,
            whole_file_pruning_checks: 1,
            whole_file_exclusions: 0,
        }
    }

    #[test]
    fn scan_contract_merge_preserves_unknown_and_rejects_different_sources_atomically() {
        let original = contract();
        let mut report = original.clone();
        report.merge(&original).unwrap();
        assert_eq!(report.invocations, 2);
        assert_eq!(report.delivered_rows, 10);
        assert_eq!(report.residual_predicate_columns, None);
        let previous = report.clone();
        let mut different = original.clone();
        different.source = 1;
        assert!(report.merge(&different).is_err());
        assert_eq!(report, previous);
        different = original;
        different.residual_predicate_columns = Some(vec![]);
        assert!(report.merge(&different).is_err());
        assert_eq!(report, previous);
    }

    #[test]
    fn scan_contract_overflow_does_not_publish_partial_counts() {
        let mut report = contract();
        report.whole_file_exclusions = u64::MAX;
        let previous = report.clone();
        let mut incoming = contract();
        incoming.whole_file_exclusions = 1;
        assert!(report.merge(&incoming).is_err());
        assert_eq!(report, previous);
    }

    #[test]
    fn scan_contract_merge_keeps_unknown_provider_passes_unknown() {
        let mut report = contract();
        let mut unknown = contract();
        unknown.provider_scan_invocations = None;
        report.merge(&unknown).unwrap();
        report.merge(&contract()).unwrap();
        assert_eq!(report.provider_scan_invocations, None);
        assert_eq!(report.invocations, 3);
        assert_eq!(report.delivered_rows, 15);
    }
}
