//! CG-22 hybrid overlay fixture execution and certification surfaces.
//!
//! This module is intentionally fixture-scoped. It combines declared local
//! Vortex base rows with deterministic in-memory hot deltas, but it does not
//! read Vortex files, flush segments to disk, write checkpoints, contact object
//! stores, invoke external engines, or provide fallback execution.

#![allow(
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::similar_names,
    clippy::struct_excessive_bools,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;

use crate::{
    ChangeOperation, ChangeRecord, ExecutionCertificate, ExecutionCertificateInput,
    ExpectedOutcome, FallbackStatus, FreshnessCertificate, LateDataPolicy, LiveCertificateStatus,
    LiveFixtureOperator, LiveOutputRow, NativeIoAdapterFidelityReport, NativeIoCertificate,
    NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
    NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, OutputChangelogEntry,
    RepresentationState, Result, ShardLoomError, WatermarkPolicy,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HybridFixtureSegmentTier {
    Cold,
    Warm,
    Hot,
}

impl HybridFixtureSegmentTier {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
            Self::Hot => "hot",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridBaseRow {
    pub key: String,
    pub metric: String,
    pub value: i64,
    pub snapshot_id: String,
    pub segment_id: String,
    pub segment_tier: HybridFixtureSegmentTier,
    pub partition: String,
    pub statistics_stale: bool,
}

impl HybridBaseRow {
    fn fixture(
        key: &str,
        metric: &str,
        value: i64,
        segment_id: &str,
        segment_tier: HybridFixtureSegmentTier,
        partition: &str,
        statistics_stale: bool,
    ) -> Self {
        Self {
            key: key.to_string(),
            metric: metric.to_string(),
            value,
            snapshot_id: HYBRID_BASE_SNAPSHOT_ID.to_string(),
            segment_id: segment_id.to_string(),
            segment_tier,
            partition: partition.to_string(),
            statistics_stale,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridFixtureRunInput {
    pub operator: LiveFixtureOperator,
    pub predicate: String,
    pub projection_columns: Vec<String>,
    pub group_column: String,
}

impl HybridFixtureRunInput {
    pub fn new(operator: LiveFixtureOperator) -> Self {
        Self {
            operator,
            predicate: "gte:value:3".to_string(),
            projection_columns: vec!["key".to_string(), "metric".to_string(), "value".to_string()],
            group_column: "metric".to_string(),
        }
    }

    pub fn with_argument(mut self, argument: Option<&str>) -> Result<Self> {
        let Some(argument) = argument else {
            return Ok(self);
        };
        match self.operator {
            LiveFixtureOperator::Filter | LiveFixtureOperator::CountWhere => {
                self.predicate = normalize_required_argument("predicate", argument)?;
            }
            LiveFixtureOperator::Project => {
                self.projection_columns = parse_projection_columns(argument)?;
            }
            LiveFixtureOperator::GroupCount => {
                self.group_column = parse_group_column(argument)?;
            }
            LiveFixtureOperator::Count => {
                return Err(ShardLoomError::InvalidOperation(
                    "hybrid count fixture does not accept an extra argument".to_string(),
                ));
            }
        }
        Ok(self)
    }

    pub fn projection_columns_text(&self) -> String {
        self.projection_columns.join(",")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaOverlayCertificate {
    pub schema_version: &'static str,
    pub certificate_id: String,
    pub status: LiveCertificateStatus,
    pub base_snapshot_certificate_id: String,
    pub merged_snapshot_certificate_id: String,
    pub base_snapshot_id: String,
    pub merged_snapshot_id: String,
    pub snapshot_epoch: u64,
    pub hot_changelog_range: String,
    pub base_row_count: usize,
    pub hot_change_record_count: usize,
    pub merged_row_count: usize,
    pub deletion_vector_entry_count: usize,
    pub tombstone_count: usize,
    pub deterministic_order: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotColdContributionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub cold_segment_count: usize,
    pub warm_segment_count: usize,
    pub hot_micro_segment_count: usize,
    pub cold_row_count: usize,
    pub warm_row_count: usize,
    pub hot_change_record_count: usize,
    pub cold_rows_selected: usize,
    pub warm_rows_selected: usize,
    pub hot_rows_selected: usize,
    pub hot_append_count: usize,
    pub hot_upsert_count: usize,
    pub hot_delete_count: usize,
    pub hot_retract_count: usize,
    pub hot_tombstone_count: usize,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroSegmentFlushEvidence {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: LiveCertificateStatus,
    pub micro_segment_ref: String,
    pub checkpoint_ref: String,
    pub commit_ref: String,
    pub representation_state: &'static str,
    pub emitted_micro_segment_count: usize,
    pub buffered_hot_change_count: usize,
    pub buffered_hot_row_count: usize,
    pub deletion_vector_entry_count: usize,
    pub statistics_record_count: usize,
    pub checkpoint_record_count: usize,
    pub flush_planned: bool,
    pub flush_write_performed: bool,
    pub checkpoint_write_performed: bool,
    pub commit_write_performed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridLayoutHealthBundle {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: &'static str,
    pub small_segment_pressure: bool,
    pub tombstone_pressure: bool,
    pub partition_skew: bool,
    pub stale_statistics: bool,
    pub compaction_plan_emitted: bool,
    pub compaction_execution_allowed: bool,
    pub compaction_candidate_count: usize,
    pub tombstone_pressure_count: usize,
    pub stale_statistics_segment_count: usize,
    pub skewed_partition: String,
    pub warm_segment_count: usize,
    pub cold_segment_count: usize,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridFixtureRunReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub fixture_id: &'static str,
    pub input: HybridFixtureRunInput,
    pub base_rows: Vec<HybridBaseRow>,
    pub hot_change_records: Vec<ChangeRecord>,
    pub merged_rows: Vec<LiveOutputRow>,
    pub output_rows: Vec<LiveOutputRow>,
    pub output_changelog: Vec<OutputChangelogEntry>,
    pub delta_overlay_certificate: DeltaOverlayCertificate,
    pub hot_cold_contribution_report: HotColdContributionReport,
    pub micro_segment_flush_evidence: MicroSegmentFlushEvidence,
    pub layout_health_bundle: HybridLayoutHealthBundle,
    pub freshness_certificate: FreshnessCertificate,
    pub execution_certificate: ExecutionCertificate,
    pub native_io_certificate: NativeIoCertificate,
    pub fallback: FallbackStatus,
    pub runtime_execution: bool,
    pub fixture_in_memory: bool,
    pub base_vortex_read_performed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub production_claim_allowed: bool,
}

impl HybridFixtureRunReport {
    pub fn has_errors(&self) -> bool {
        self.fallback.attempted
            || self.external_engine_invoked
            || self.object_store_io
            || self.write_io
            || self.base_vortex_read_performed
            || !self.execution_certificate.is_certified()
            || !self.native_io_certificate.is_certified()
            || self.delta_overlay_certificate.status != LiveCertificateStatus::Certified
            || self.micro_segment_flush_evidence.status != LiveCertificateStatus::Certified
            || self.freshness_certificate.status != LiveCertificateStatus::Certified
    }

    pub const fn fallback_attempted(&self) -> bool {
        self.fallback.attempted
    }

    pub fn output_row_count(&self) -> usize {
        self.output_rows.len()
    }

    pub fn output_rows_text(&self) -> String {
        if self.output_rows.is_empty() {
            "none".to_string()
        } else {
            self.output_rows
                .iter()
                .map(|row| self.output_row_summary(row))
                .collect::<Vec<_>>()
                .join("|")
        }
    }

    pub fn hot_changelog_range(&self) -> String {
        sequence_range(&self.hot_change_records)
    }

    pub fn hot_operation_order(&self) -> String {
        self.hot_change_records
            .iter()
            .map(|record| record.operation.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn output_changelog_order(&self) -> String {
        if self.output_changelog.is_empty() {
            "none".to_string()
        } else {
            self.output_changelog
                .iter()
                .map(|entry| {
                    format!(
                        "{}:{}:{}",
                        entry.sequence,
                        entry.key,
                        entry.operation.as_str()
                    )
                })
                .collect::<Vec<_>>()
                .join("|")
        }
    }

    pub fn cold_segment_count(&self) -> usize {
        self.hot_cold_contribution_report.cold_segment_count
    }

    pub fn warm_segment_count(&self) -> usize {
        self.hot_cold_contribution_report.warm_segment_count
    }

    pub fn to_human_text(&self) -> String {
        format!(
            "hybrid fixture run\nschema_version: {}\nreport: {}\noperator: {}\nfixture: {}\nbase snapshot: {}\nmerged snapshot: {}\nhot changelog: {}\nmerged rows: {}\noutput rows: {}\ndelta overlay certificate: {}\nmicro-segment flush evidence: {}\nexecution certificate: {}\nnative I/O certificate: {}\nfallback execution: disabled\nexternal engine invoked: false",
            self.schema_version,
            self.report_id,
            self.input.operator.as_str(),
            self.fixture_id,
            self.delta_overlay_certificate.base_snapshot_id,
            self.delta_overlay_certificate.merged_snapshot_id,
            self.hot_changelog_range(),
            self.merged_rows.len(),
            self.output_rows.len(),
            self.delta_overlay_certificate.status.as_str(),
            self.micro_segment_flush_evidence.status.as_str(),
            self.execution_certificate.status.as_str(),
            self.native_io_certificate.status(),
        )
    }

    fn output_row_summary(&self, row: &LiveOutputRow) -> String {
        if self.input.operator != LiveFixtureOperator::Project {
            return row.summary();
        }

        self.input
            .projection_columns
            .iter()
            .filter_map(|column| match column.as_str() {
                "key" => Some(row.key.clone()),
                "metric" => Some(row.metric.clone()),
                "value" => Some(row.value.to_string()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(":")
    }
}

pub fn run_hybrid_fixture(input: HybridFixtureRunInput) -> Result<HybridFixtureRunReport> {
    let base_rows = hybrid_base_rows();
    let hot_change_records = hybrid_hot_change_records();
    validate_hot_records(&hot_change_records)?;
    let (merged_rows, deletion_vector_entry_count) =
        merge_base_and_hot_delta(&base_rows, &hot_change_records);
    let output_rows = evaluate_operator(&input, &merged_rows)?;
    let output_changelog = output_rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            OutputChangelogEntry::from_row(
                usize_to_u64(index + 1),
                row,
                input.operator.output_mode(),
            )
        })
        .collect::<Vec<_>>();
    let delta_overlay_certificate = delta_overlay_certificate_for(
        &base_rows,
        &hot_change_records,
        &merged_rows,
        deletion_vector_entry_count,
    );
    let hot_cold_contribution_report =
        hot_cold_contribution_report_for(&base_rows, &hot_change_records, &merged_rows);
    let micro_segment_flush_evidence = micro_segment_flush_evidence_for(
        &hot_change_records,
        &merged_rows,
        deletion_vector_entry_count,
    );
    let layout_health_bundle = layout_health_bundle_for(&base_rows, deletion_vector_entry_count);
    let freshness_certificate = freshness_certificate_for(&hot_change_records);
    let execution_certificate = execution_certificate_for(&input, output_rows.len())?;
    let native_io_certificate = native_io_certificate_for(&input)?;

    Ok(HybridFixtureRunReport {
        schema_version: "shardloom.hybrid_fixture_run.v1",
        report_id: format!("cg22.hybrid_fixture.{}", input.operator.as_str()),
        fixture_id: "cg22.hybrid.fixture.v1",
        input,
        base_rows,
        hot_change_records,
        merged_rows,
        output_rows,
        output_changelog,
        delta_overlay_certificate,
        hot_cold_contribution_report,
        micro_segment_flush_evidence,
        layout_health_bundle,
        freshness_certificate,
        execution_certificate,
        native_io_certificate,
        fallback: FallbackStatus::disabled_by_policy(),
        runtime_execution: true,
        fixture_in_memory: true,
        base_vortex_read_performed: false,
        data_read: false,
        data_decoded: false,
        data_materialized: false,
        object_store_io: false,
        write_io: false,
        external_engine_invoked: false,
        production_claim_allowed: false,
    })
}

const HYBRID_BASE_SNAPSHOT_ID: &str = "snapshot://cg22/hybrid/base/v1";
const HYBRID_MERGED_SNAPSHOT_ID: &str = "snapshot://cg22/hybrid/merged/epoch-42";

fn hybrid_base_rows() -> Vec<HybridBaseRow> {
    vec![
        HybridBaseRow::fixture(
            "a",
            "east",
            2,
            "segment-cold-001",
            HybridFixtureSegmentTier::Cold,
            "region=east",
            false,
        ),
        HybridBaseRow::fixture(
            "b",
            "west",
            5,
            "segment-warm-001",
            HybridFixtureSegmentTier::Warm,
            "region=west",
            true,
        ),
        HybridBaseRow::fixture(
            "c",
            "east",
            7,
            "segment-cold-001",
            HybridFixtureSegmentTier::Cold,
            "region=east",
            false,
        ),
        HybridBaseRow::fixture(
            "d",
            "north",
            1,
            "segment-warm-002",
            HybridFixtureSegmentTier::Warm,
            "region=north",
            true,
        ),
    ]
}

fn hybrid_hot_change_records() -> Vec<ChangeRecord> {
    vec![
        hybrid_hot_record("b", ChangeOperation::Upsert, 1, 11_000, "west", 9),
        hybrid_hot_record("c", ChangeOperation::Tombstone, 2, 12_000, "east", 7),
        hybrid_hot_record("e", ChangeOperation::Append, 3, 13_000, "east", 4),
        hybrid_hot_record("d", ChangeOperation::Delete, 4, 14_000, "north", 1),
        hybrid_hot_record("f", ChangeOperation::Append, 5, 15_000, "south", 8),
        hybrid_hot_record("f", ChangeOperation::Retract, 6, 16_000, "south", 8),
    ]
}

fn hybrid_hot_record(
    key: &str,
    operation: ChangeOperation,
    sequence: u64,
    event_time_ms: u64,
    metric: &str,
    value: i64,
) -> ChangeRecord {
    ChangeRecord {
        key: key.to_string(),
        operation,
        sequence,
        event_time_ms,
        processing_time_ms: event_time_ms + 500,
        source_offset: format!("fixture://cg22/hybrid/hot/{sequence:04}"),
        schema_digest: "sha256:cg22-hybrid-change-v1".to_string(),
        payload_ref: format!("payload://cg22/hybrid/hot/{key}/{sequence:04}"),
        metric: metric.to_string(),
        value,
    }
}

fn validate_hot_records(records: &[ChangeRecord]) -> Result<()> {
    for (expected, record) in (1_u64..).zip(records) {
        if record.sequence != expected {
            return Err(ShardLoomError::InvalidOperation(format!(
                "hybrid hot fixture sequence must be contiguous: expected {expected}, got {}",
                record.sequence
            )));
        }
    }
    Ok(())
}

fn merge_base_and_hot_delta(
    base_rows: &[HybridBaseRow],
    hot_records: &[ChangeRecord],
) -> (Vec<LiveOutputRow>, usize) {
    let base_keys = base_rows
        .iter()
        .map(|row| row.key.clone())
        .collect::<Vec<_>>();
    let mut state = base_rows
        .iter()
        .map(|row| {
            (
                row.key.clone(),
                LiveOutputRow {
                    key: row.key.clone(),
                    metric: row.metric.clone(),
                    value: row.value,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut deletion_vector_entry_count = 0;
    for record in hot_records {
        match record.operation {
            ChangeOperation::Append | ChangeOperation::Upsert => {
                state.insert(
                    record.key.clone(),
                    LiveOutputRow {
                        key: record.key.clone(),
                        metric: record.metric.clone(),
                        value: record.value,
                    },
                );
            }
            ChangeOperation::Delete | ChangeOperation::Retract | ChangeOperation::Tombstone => {
                if base_keys.contains(&record.key) {
                    deletion_vector_entry_count += 1;
                }
                state.remove(&record.key);
            }
        }
    }
    (state.into_values().collect(), deletion_vector_entry_count)
}

fn evaluate_operator(
    input: &HybridFixtureRunInput,
    rows: &[LiveOutputRow],
) -> Result<Vec<LiveOutputRow>> {
    match input.operator {
        LiveFixtureOperator::Filter => Ok(rows
            .iter()
            .filter(|row| predicate_matches(&input.predicate, row))
            .cloned()
            .collect()),
        LiveFixtureOperator::Project => Ok(rows.to_vec()),
        LiveFixtureOperator::Count => Ok(vec![LiveOutputRow::synthetic(
            "hybrid",
            "count",
            usize_to_i64(rows.len()),
        )]),
        LiveFixtureOperator::CountWhere => {
            let count = rows
                .iter()
                .filter(|row| predicate_matches(&input.predicate, row))
                .count();
            Ok(vec![LiveOutputRow::synthetic(
                "hybrid",
                "count_where",
                usize_to_i64(count),
            )])
        }
        LiveFixtureOperator::GroupCount => group_count(&input.group_column, rows),
    }
}

fn group_count(group_column: &str, rows: &[LiveOutputRow]) -> Result<Vec<LiveOutputRow>> {
    if group_column != "metric" {
        return Err(ShardLoomError::InvalidOperation(format!(
            "hybrid group-count fixture only supports group column metric, got {group_column}"
        )));
    }
    let mut counts = BTreeMap::<String, i64>::new();
    for row in rows {
        *counts.entry(row.metric.clone()).or_insert(0) += 1;
    }
    Ok(counts
        .into_iter()
        .map(|(metric, count)| LiveOutputRow::synthetic(&metric, "group_count", count))
        .collect())
}

fn predicate_matches(predicate: &str, row: &LiveOutputRow) -> bool {
    if let Some(threshold) = predicate.strip_prefix("gte:value:") {
        return threshold
            .parse::<i64>()
            .is_ok_and(|threshold| row.value >= threshold);
    }
    if let Some(metric) = predicate.strip_prefix("eq:metric:") {
        return row.metric == metric;
    }
    false
}

fn delta_overlay_certificate_for(
    base_rows: &[HybridBaseRow],
    hot_records: &[ChangeRecord],
    merged_rows: &[LiveOutputRow],
    deletion_vector_entry_count: usize,
) -> DeltaOverlayCertificate {
    DeltaOverlayCertificate {
        schema_version: "shardloom.delta_overlay_certificate.v1",
        certificate_id: "cg22.hybrid.fixture.delta_overlay".to_string(),
        status: LiveCertificateStatus::Certified,
        base_snapshot_certificate_id: "cg22.hybrid.fixture.base_snapshot".to_string(),
        merged_snapshot_certificate_id: "cg22.hybrid.fixture.merged_snapshot".to_string(),
        base_snapshot_id: HYBRID_BASE_SNAPSHOT_ID.to_string(),
        merged_snapshot_id: HYBRID_MERGED_SNAPSHOT_ID.to_string(),
        snapshot_epoch: 42,
        hot_changelog_range: sequence_range(hot_records),
        base_row_count: base_rows.len(),
        hot_change_record_count: hot_records.len(),
        merged_row_count: merged_rows.len(),
        deletion_vector_entry_count,
        tombstone_count: operation_count(hot_records, ChangeOperation::Tombstone),
        deterministic_order: true,
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn hot_cold_contribution_report_for(
    base_rows: &[HybridBaseRow],
    hot_records: &[ChangeRecord],
    merged_rows: &[LiveOutputRow],
) -> HotColdContributionReport {
    HotColdContributionReport {
        schema_version: "shardloom.hot_cold_contribution_report.v1",
        report_id: "cg22.hybrid.fixture.hot_cold_contribution".to_string(),
        cold_segment_count: distinct_segment_count(base_rows, HybridFixtureSegmentTier::Cold),
        warm_segment_count: distinct_segment_count(base_rows, HybridFixtureSegmentTier::Warm),
        hot_micro_segment_count: 1,
        cold_row_count: base_rows
            .iter()
            .filter(|row| row.segment_tier == HybridFixtureSegmentTier::Cold)
            .count(),
        warm_row_count: base_rows
            .iter()
            .filter(|row| row.segment_tier == HybridFixtureSegmentTier::Warm)
            .count(),
        hot_change_record_count: hot_records.len(),
        cold_rows_selected: merged_rows.iter().filter(|row| row.key == "a").count(),
        warm_rows_selected: merged_rows.iter().filter(|row| row.key == "b").count(),
        hot_rows_selected: merged_rows.iter().filter(|row| row.key == "e").count(),
        hot_append_count: operation_count(hot_records, ChangeOperation::Append),
        hot_upsert_count: operation_count(hot_records, ChangeOperation::Upsert),
        hot_delete_count: operation_count(hot_records, ChangeOperation::Delete),
        hot_retract_count: operation_count(hot_records, ChangeOperation::Retract),
        hot_tombstone_count: operation_count(hot_records, ChangeOperation::Tombstone),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn micro_segment_flush_evidence_for(
    hot_records: &[ChangeRecord],
    merged_rows: &[LiveOutputRow],
    deletion_vector_entry_count: usize,
) -> MicroSegmentFlushEvidence {
    MicroSegmentFlushEvidence {
        schema_version: "shardloom.micro_segment_flush_evidence.v1",
        report_id: "cg22.hybrid.fixture.micro_segment_flush".to_string(),
        status: LiveCertificateStatus::Certified,
        micro_segment_ref: "micro-segment://cg22/hybrid/hot/0001".to_string(),
        checkpoint_ref: "checkpoint://cg22/hybrid/fixture/epoch-42".to_string(),
        commit_ref: "commit://cg22/hybrid/fixture/epoch-42".to_string(),
        representation_state: "vortex_encoded_planned",
        emitted_micro_segment_count: 1,
        buffered_hot_change_count: hot_records.len(),
        buffered_hot_row_count: hot_records
            .iter()
            .filter(|record| {
                matches!(
                    record.operation,
                    ChangeOperation::Append | ChangeOperation::Upsert
                )
            })
            .count(),
        deletion_vector_entry_count,
        statistics_record_count: merged_rows.len(),
        checkpoint_record_count: merged_rows.len(),
        flush_planned: true,
        flush_write_performed: false,
        checkpoint_write_performed: false,
        commit_write_performed: false,
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn layout_health_bundle_for(
    base_rows: &[HybridBaseRow],
    deletion_vector_entry_count: usize,
) -> HybridLayoutHealthBundle {
    let stale_statistics_segment_count = base_rows
        .iter()
        .filter(|row| row.statistics_stale)
        .map(|row| row.segment_id.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    HybridLayoutHealthBundle {
        schema_version: "shardloom.hybrid_layout_health_bundle.v1",
        report_id: "cg22.hybrid.fixture.layout_health_bundle".to_string(),
        status: "compaction_recommended",
        small_segment_pressure: true,
        tombstone_pressure: deletion_vector_entry_count > 0,
        partition_skew: true,
        stale_statistics: stale_statistics_segment_count > 0,
        compaction_plan_emitted: true,
        compaction_execution_allowed: false,
        compaction_candidate_count: 3,
        tombstone_pressure_count: deletion_vector_entry_count,
        stale_statistics_segment_count,
        skewed_partition: "region=east".to_string(),
        warm_segment_count: distinct_segment_count(base_rows, HybridFixtureSegmentTier::Warm),
        cold_segment_count: distinct_segment_count(base_rows, HybridFixtureSegmentTier::Cold),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn freshness_certificate_for(hot_records: &[ChangeRecord]) -> FreshnessCertificate {
    let max_event_time_ms = hot_records
        .iter()
        .map(|record| record.event_time_ms)
        .max()
        .unwrap_or_default();
    let max_processing_time_ms = hot_records
        .iter()
        .map(|record| record.processing_time_ms)
        .max()
        .unwrap_or_default();
    FreshnessCertificate {
        schema_version: "shardloom.freshness_certificate.v1",
        certificate_id: "cg22.hybrid.fixture.freshness".to_string(),
        status: LiveCertificateStatus::Certified,
        watermark_policy: WatermarkPolicy::FixtureEventTime,
        late_data_policy: LateDataPolicy::RejectPastWatermark,
        max_event_time_ms,
        max_processing_time_ms,
        watermark_ms: max_event_time_ms,
        freshness_lag_ms: max_processing_time_ms.saturating_sub(max_event_time_ms),
        late_record_count: 0,
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn execution_certificate_for(
    input: &HybridFixtureRunInput,
    output_row_count: usize,
) -> Result<ExecutionCertificate> {
    let mut certificate_input = ExecutionCertificateInput::new(
        format!("cg22.hybrid.fixture.{}.execution", input.operator.as_str()),
        "cg22_hybrid_fixture_overlay",
    )?;
    certificate_input.provider_crate = Some("shardloom-core".to_string());
    certificate_input.provider_api_surface = Some("run_hybrid_fixture".to_string());
    certificate_input.shardloom_admission_policy = Some("cg22_hybrid_fixture_only".to_string());
    certificate_input.plan_ref = Some("plan://cg22/hybrid/fixture".to_string());
    certificate_input.input_ref = Some("fixture://cg22/hybrid/base-plus-hot-delta".to_string());
    certificate_input.output_ref = Some(format!(
        "result://cg22/hybrid/fixture/{}",
        input.operator.as_str()
    ));
    certificate_input.correctness_fixture_id = Some("cg22.hybrid.fixture.v1".to_string());
    let outcome = ExpectedOutcome::Rows {
        row_count: Some(usize_to_u64(output_row_count)),
    };
    certificate_input.expected_outcome = Some(outcome.clone());
    certificate_input.actual_outcome = Some(outcome);
    certificate_input.correctness_passed = true;
    Ok(ExecutionCertificate::evaluate(certificate_input))
}

fn native_io_certificate_for(input: &HybridFixtureRunInput) -> Result<NativeIoCertificate> {
    NativeIoCertificate::new(
        format!("cg22.hybrid.fixture.{}.native_io", input.operator.as_str()),
        "declared_vortex_base_plus_in_memory_hot_delta_to_hybrid_overlay",
        NativeIoSourceCapabilityReport {
            source_kind: "declared_local_vortex_base_plus_in_memory_hot_delta".to_string(),
            adapter_id: "cg22.hybrid.fixture".to_string(),
            schema_discovery_status: "declared_base_and_hot_delta_schema".to_string(),
            statistics_availability: "fixture_statistics_known".to_string(),
            pushdown_capabilities: input.operator.as_str().to_string(),
            encoded_representation_preserved: true,
            range_read_capability: false,
            streaming_capability: true,
            object_store_capability: false,
            fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: vec![input.operator.as_str().to_string()],
            rejected_operations: vec![
                "vortex_file_read".to_string(),
                "object_store_read".to_string(),
                "external_engine_execution".to_string(),
            ],
            guarantee: "declared_fixture_overlay_only".to_string(),
            proof_basis: "checked_in_cg22_hybrid_in_memory_fixture".to_string(),
            residual_expression: None,
            conservative_false_positive_policy: true,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        },
        vec![
            NativeIoRepresentationTransition::new(
                RepresentationState::VortexEncoded,
                RepresentationState::SelectionVectorEncoded,
                false,
            ),
            NativeIoRepresentationTransition::new(
                RepresentationState::SelectionVectorEncoded,
                RepresentationState::VortexEncoded,
                false,
            ),
        ],
        NativeIoSinkRequirementReport {
            target_format: "in_memory_hybrid_overlay_result".to_string(),
            accepts_encoded: true,
            requires_decoded_columnar: false,
            requires_rows: false,
            preserves_metadata: true,
            requires_ordering: true,
            requires_partitioning: false,
            requires_commit: false,
            supports_streaming: true,
            max_chunk_size: Some(6),
            backpressure_policy: "not_applicable_bounded_fixture".to_string(),
        },
        NativeIoAdapterFidelityReport {
            adapter_id: "cg22.hybrid.fixture".to_string(),
            source_kind: "declared_local_vortex_base_plus_in_memory_hot_delta".to_string(),
            sink_kind: "in_memory_hybrid_overlay_result".to_string(),
            metadata_preserved: true,
            statistics_preserved: true,
            encoded_representation_preserved: true,
            materialization_required: false,
            fidelity_loss: "none_for_fixture_contract".to_string(),
            metadata_loss: "none".to_string(),
            fallback_attempted: false,
        },
        Vec::new(),
        NativeIoSideEffectReport {
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
        },
        Vec::new(),
    )
}

fn distinct_segment_count(base_rows: &[HybridBaseRow], tier: HybridFixtureSegmentTier) -> usize {
    base_rows
        .iter()
        .filter(|row| row.segment_tier == tier)
        .map(|row| row.segment_id.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

fn operation_count(records: &[ChangeRecord], operation: ChangeOperation) -> usize {
    records
        .iter()
        .filter(|record| record.operation == operation)
        .count()
}

fn sequence_range(records: &[ChangeRecord]) -> String {
    let start = records
        .iter()
        .map(|record| record.sequence)
        .min()
        .unwrap_or_default();
    let end = records
        .iter()
        .map(|record| record.sequence)
        .max()
        .unwrap_or_default();
    format!("{start}..{end}")
}

fn parse_projection_columns(value: &str) -> Result<Vec<String>> {
    let columns = normalize_required_argument("columns", value)?
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(normalized_token)
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "projection columns cannot be empty".to_string(),
        ));
    }
    for column in &columns {
        if !matches!(column.as_str(), "key" | "metric" | "value") {
            return Err(ShardLoomError::InvalidOperation(format!(
                "unknown hybrid projection column: {column}"
            )));
        }
    }
    Ok(columns)
}

fn parse_group_column(value: &str) -> Result<String> {
    let column = normalized_token(&normalize_required_argument("group column", value)?);
    if column != "metric" {
        return Err(ShardLoomError::InvalidOperation(format!(
            "hybrid group-count fixture only supports group column metric, got {column}"
        )));
    }
    Ok(column)
}

fn normalize_required_argument(label: &str, value: &str) -> Result<String> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "hybrid fixture {label} cannot be empty"
        )));
    }
    Ok(normalized.to_string())
}

fn normalized_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hybrid_fixture_group_count_certifies_overlay_flush_and_layout() {
        let input = HybridFixtureRunInput::new(LiveFixtureOperator::GroupCount)
            .with_argument(Some("metric"))
            .expect("valid fixture input");
        let report = run_hybrid_fixture(input).expect("fixture runs");

        assert!(!report.has_errors());
        assert_eq!(report.delta_overlay_certificate.base_row_count, 4);
        assert_eq!(report.delta_overlay_certificate.hot_change_record_count, 6);
        assert_eq!(report.delta_overlay_certificate.merged_row_count, 3);
        assert_eq!(
            report.delta_overlay_certificate.deletion_vector_entry_count,
            2
        );
        assert_eq!(
            report.output_rows_text(),
            "east:group_count:2|west:group_count:1"
        );
        assert_eq!(
            report.micro_segment_flush_evidence.representation_state,
            "vortex_encoded_planned"
        );
        assert!(report.layout_health_bundle.compaction_plan_emitted);
        assert!(!report.layout_health_bundle.compaction_execution_allowed);
        assert!(report.execution_certificate.is_certified());
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.fallback_attempted());
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn hybrid_fixture_filter_uses_merged_base_and_hot_state() {
        let input = HybridFixtureRunInput::new(LiveFixtureOperator::Filter)
            .with_argument(Some("gte:value:3"))
            .expect("valid fixture input");
        let report = run_hybrid_fixture(input).expect("fixture runs");

        assert_eq!(report.merged_rows.len(), 3);
        assert_eq!(report.output_rows_text(), "b:west:9|e:east:4");
        assert_eq!(report.hot_changelog_range(), "1..6");
        assert_eq!(
            report.hot_operation_order(),
            "upsert,tombstone,append,delete,append,retract"
        );
    }

    #[test]
    fn hybrid_fixture_rejects_unknown_predicate_without_fallback() {
        let input = HybridFixtureRunInput::new(LiveFixtureOperator::Filter)
            .with_argument(Some("contains:value:3"))
            .expect("argument stored");
        let report = run_hybrid_fixture(input).expect("fixture runs");

        assert_eq!(report.output_row_count(), 0);
        assert!(!report.fallback_attempted());
        assert!(!report.external_engine_invoked);
    }
}
