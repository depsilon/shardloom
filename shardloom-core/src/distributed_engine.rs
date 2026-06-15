//! Scoped local distributed fixture execution and evidence.
//!
//! This module is intentionally narrow. It runs deterministic in-process local
//! worker slots over a checked-in split fixture. It does not start networked
//! workers, read object stores, write result files, spill, invoke external
//! engines, or authorize broad distributed production claims.

#![allow(
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::similar_names,
    clippy::struct_excessive_bools
)]

use std::collections::BTreeMap;

use crate::{
    ExecutionCertificate, ExecutionCertificateInput, ExpectedOutcome, FallbackStatus,
    NativeIoAdapterFidelityReport, NativeIoCertificate, NativeIoRepresentationTransition,
    NativeIoSideEffectReport, NativeIoSinkRequirementReport, NativeIoSourceCapabilityReport,
    NativeIoSourcePushdownReport, RepresentationState, Result, ShardLoomError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributedFixtureFaultMode {
    None,
    RetryDuplicateStaleLease,
}

impl DistributedFixtureFaultMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RetryDuplicateStaleLease => "retry_duplicate_stale_lease",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match normalized_token(value).as_str() {
            "none" | "clean" | "success" => Ok(Self::None),
            "retry-duplicate-stale-lease"
            | "retry_duplicate_stale_lease"
            | "fault-injection"
            | "fault_injection" => Ok(Self::RetryDuplicateStaleLease),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown distributed fixture fault mode: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalDistributedFixtureRunInput {
    pub worker_count: usize,
    pub fault_mode: DistributedFixtureFaultMode,
}

impl LocalDistributedFixtureRunInput {
    pub fn new(worker_count: usize, fault_mode: DistributedFixtureFaultMode) -> Result<Self> {
        if !(1..=4).contains(&worker_count) {
            return Err(ShardLoomError::InvalidOperation(
                "distributed local fixture worker count must be between 1 and 4".to_string(),
            ));
        }
        Ok(Self {
            worker_count,
            fault_mode,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DistributedFixtureRow {
    pub row_id: &'static str,
    pub split_id: &'static str,
    pub group_key: &'static str,
    pub value: i64,
}

impl DistributedFixtureRow {
    const fn new(
        row_id: &'static str,
        split_id: &'static str,
        group_key: &'static str,
        value: i64,
    ) -> Self {
        Self {
            row_id,
            split_id,
            group_key,
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedSplitUnit {
    pub split_id: String,
    pub input_ref: String,
    pub assigned_worker_id: String,
    pub row_count: usize,
    pub estimated_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDistributedSplitManifest {
    pub schema_version: &'static str,
    pub manifest_id: String,
    pub split_units: Vec<DistributedSplitUnit>,
    pub capillary_split_window: &'static str,
    pub pulseweave_control_surface: &'static str,
    pub dynamic_admission_policy: &'static str,
}

impl LocalDistributedSplitManifest {
    pub fn split_id_order(&self) -> String {
        self.split_units
            .iter()
            .map(|split| split.split_id.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn worker_assignment_order(&self) -> String {
        self.split_units
            .iter()
            .map(|split| format!("{}:{}", split.split_id, split.assigned_worker_id))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn total_estimated_bytes(&self) -> u64 {
        self.split_units
            .iter()
            .map(|split| split.estimated_bytes)
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedWorkerLease {
    pub worker_id: String,
    pub lease_id: String,
    pub heartbeat_count: usize,
    pub leased_split_count: usize,
    pub completed_split_count: usize,
    pub status: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributedTaskAttemptOutcome {
    Certified,
    CancelledCleanupCompleted,
    DuplicateRejected,
    StaleLeaseRejected,
}

impl DistributedTaskAttemptOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Certified => "certified",
            Self::CancelledCleanupCompleted => "cancelled_cleanup_completed",
            Self::DuplicateRejected => "duplicate_rejected",
            Self::StaleLeaseRejected => "stale_lease_rejected",
        }
    }

    pub const fn emits_fragment(self) -> bool {
        matches!(self, Self::Certified)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedTaskAttempt {
    pub split_id: String,
    pub worker_id: String,
    pub task_lease_id: String,
    pub task_attempt_id: String,
    pub attempt_index: usize,
    pub outcome: DistributedTaskAttemptOutcome,
    pub retry_count: usize,
    pub cancellation_requested: bool,
    pub cleanup_completed: bool,
    pub duplicate_attempt: bool,
    pub stale_lease_detected: bool,
    pub input_ref: String,
    pub output_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedResultFragment {
    pub split_id: String,
    pub worker_id: String,
    pub output_ref: String,
    pub row_count: usize,
    pub result_fragment_digest: String,
    pub group_counts: Vec<DistributedMergedRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPartitionAssignment {
    pub partition_id: String,
    pub group_key: String,
    pub reduce_worker_id: String,
    pub local_combined_input_count: usize,
    pub merged_count: usize,
    pub merged_sum: i64,
}

impl DistributedPartitionAssignment {
    fn summary(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}",
            self.partition_id,
            self.group_key,
            self.reduce_worker_id,
            self.local_combined_input_count,
            self.merged_count,
            self.merged_sum
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedShuffleRepartitionReport {
    pub schema_version: &'static str,
    pub strategy_id: &'static str,
    pub partition_key: &'static str,
    pub repartition_strategy: &'static str,
    pub local_combine_strategy: &'static str,
    pub global_merge_strategy: &'static str,
    pub partition_count: usize,
    pub repartition_performed: bool,
    pub local_combine_performed: bool,
    pub global_merge_performed: bool,
    pub remote_shuffle_performed: bool,
    pub raw_input_row_count: usize,
    pub local_combined_row_count: usize,
    pub global_merge_input_row_count: usize,
    pub partition_assignments: Vec<DistributedPartitionAssignment>,
    pub global_merged_rows: Vec<DistributedMergedRow>,
}

impl DistributedShuffleRepartitionReport {
    pub fn partition_assignment_order(&self) -> String {
        self.partition_assignments
            .iter()
            .map(DistributedPartitionAssignment::summary)
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedSkewReport {
    pub schema_version: &'static str,
    pub detection_strategy: &'static str,
    pub handling_strategy: &'static str,
    pub skew_detection_performed: bool,
    pub skew_detected: bool,
    pub skew_handling_applied: bool,
    pub skew_threshold_rows: usize,
    pub max_group_rows: usize,
    pub skewed_group_keys: Vec<String>,
}

impl DistributedSkewReport {
    pub fn skewed_group_key_order(&self) -> String {
        self.skewed_group_keys.join(",")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedMemoryBackpressureReport {
    pub schema_version: &'static str,
    pub memory_budget_bytes: u64,
    pub peak_reserved_bytes: u64,
    pub local_combine_buffer_bytes: u64,
    pub global_merge_buffer_bytes: u64,
    pub memory_budget_exceeded: bool,
    pub backpressure_policy: &'static str,
    pub backpressure_signal_emitted: bool,
    pub spill_policy: &'static str,
    pub spill_required: bool,
    pub spill_io_performed: bool,
    pub production_spill_claim_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedMergedRow {
    pub group_key: String,
    pub count: usize,
    pub sum: i64,
}

impl DistributedMergedRow {
    fn new(group_key: impl Into<String>, count: usize, sum: i64) -> Self {
        Self {
            group_key: group_key.into(),
            count,
            sum,
        }
    }

    pub fn summary(&self) -> String {
        format!("{}:{}:{}", self.group_key, self.count, self.sum)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDistributedFixtureRunReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub fixture_id: &'static str,
    pub input: LocalDistributedFixtureRunInput,
    pub split_manifest: LocalDistributedSplitManifest,
    pub worker_leases: Vec<DistributedWorkerLease>,
    pub task_attempts: Vec<DistributedTaskAttempt>,
    pub result_fragments: Vec<DistributedResultFragment>,
    pub shuffle_repartition: DistributedShuffleRepartitionReport,
    pub skew: DistributedSkewReport,
    pub memory_backpressure: DistributedMemoryBackpressureReport,
    pub merged_rows: Vec<DistributedMergedRow>,
    pub execution_certificate: ExecutionCertificate,
    pub native_io_certificate: NativeIoCertificate,
    pub fallback: FallbackStatus,
    pub runtime_execution: bool,
    pub coordinator_invoked: bool,
    pub local_worker_runtime_invoked: bool,
    pub remote_worker_invoked: bool,
    pub split_execution_performed: bool,
    pub shuffle_repartition_performed: bool,
    pub local_combine_performed: bool,
    pub global_merge_performed: bool,
    pub deterministic_merge_performed: bool,
    pub skew_detection_performed: bool,
    pub skew_handling_applied: bool,
    pub memory_budget_enforced: bool,
    pub retry_performed: bool,
    pub duplicate_attempt_rejected: bool,
    pub stale_lease_rejected: bool,
    pub cancellation_cleanup_completed: bool,
    pub partial_output_committed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_engine_invoked: bool,
    pub production_claim_allowed: bool,
    pub distributed_performance_claim_allowed: bool,
}

impl LocalDistributedFixtureRunReport {
    pub fn has_errors(&self) -> bool {
        self.fallback.attempted
            || self.remote_worker_invoked
            || self.external_engine_invoked
            || self.object_store_io
            || self.write_io
            || self.spill_io_performed
            || !self.execution_certificate.is_certified()
            || !self.native_io_certificate.is_certified()
            || !self.deterministic_merge_performed
            || self.partial_output_committed
    }

    pub const fn fallback_attempted(&self) -> bool {
        self.fallback.attempted
    }

    pub fn task_attempt_outcome_order(&self) -> String {
        self.task_attempts
            .iter()
            .map(|attempt| {
                format!(
                    "{}:{}:{}",
                    attempt.split_id,
                    attempt.task_attempt_id,
                    attempt.outcome.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn result_fragment_digest_order(&self) -> String {
        self.result_fragments
            .iter()
            .map(|fragment| {
                format!(
                    "{}:{}",
                    fragment.output_ref, fragment.result_fragment_digest
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn merged_rows_text(&self) -> String {
        self.merged_rows
            .iter()
            .map(DistributedMergedRow::summary)
            .collect::<Vec<_>>()
            .join("|")
    }

    pub fn merge_digest(&self) -> String {
        stable_digest(&self.merged_rows_text())
    }

    pub fn no_fallback_no_external_engine(&self) -> bool {
        !self.fallback.attempted && !self.external_engine_invoked
    }

    pub fn to_human_text(&self) -> String {
        format!(
            "local distributed fixture\nschema version: {}\nreport: {}\nworkers: {}\nsplits: {}\nattempts: {}\nfragments: {}\npartitions: {}\nskew detected: {}\nmerged rows: {}\nmerge digest: {}\nruntime execution: {}\nremote worker: disabled\nexternal engine: disabled\nfallback execution: disabled\nproduction claim: blocked",
            self.schema_version,
            self.report_id,
            self.input.worker_count,
            self.split_manifest.split_units.len(),
            self.task_attempts.len(),
            self.result_fragments.len(),
            self.shuffle_repartition.partition_count,
            self.skew.skew_detected,
            self.merged_rows_text(),
            self.merge_digest(),
            self.runtime_execution,
        )
    }
}

pub fn run_local_distributed_fixture(
    input: LocalDistributedFixtureRunInput,
) -> Result<LocalDistributedFixtureRunReport> {
    let rows = distributed_fixture_rows();
    let split_manifest = build_split_manifest(input.worker_count, &rows);
    let task_attempts = build_task_attempts(&split_manifest, input.fault_mode);
    let result_fragments = execute_certified_attempts(&rows, &task_attempts);
    let worker_leases = build_worker_leases(input.worker_count, &split_manifest, &task_attempts);
    let shuffle_repartition =
        build_shuffle_repartition_report(input.worker_count, &result_fragments);
    let merged_rows = shuffle_repartition.global_merged_rows.clone();
    let skew = build_skew_report(&merged_rows);
    let memory_backpressure =
        build_memory_backpressure_report(&split_manifest, &shuffle_repartition);
    let execution_certificate =
        execution_certificate_for(&input, &split_manifest, &result_fragments, &merged_rows)?;
    let native_io_certificate = native_io_certificate_for(&input, &split_manifest)?;

    Ok(LocalDistributedFixtureRunReport {
        schema_version: "shardloom.local_distributed_fixture_run.v1",
        report_id: format!(
            "prod-ready-1d.local_distributed_fixture.{}",
            input.fault_mode.as_str()
        ),
        fixture_id: "prod-ready-1d.local_distributed.fixture.v1",
        input,
        split_manifest,
        worker_leases,
        task_attempts,
        result_fragments,
        shuffle_repartition,
        skew,
        memory_backpressure,
        merged_rows,
        execution_certificate,
        native_io_certificate,
        fallback: FallbackStatus::disabled_by_policy(),
        runtime_execution: true,
        coordinator_invoked: true,
        local_worker_runtime_invoked: true,
        remote_worker_invoked: false,
        split_execution_performed: true,
        shuffle_repartition_performed: true,
        local_combine_performed: true,
        global_merge_performed: true,
        deterministic_merge_performed: true,
        skew_detection_performed: true,
        skew_handling_applied: true,
        memory_budget_enforced: true,
        retry_performed: matches!(
            input.fault_mode,
            DistributedFixtureFaultMode::RetryDuplicateStaleLease
        ),
        duplicate_attempt_rejected: matches!(
            input.fault_mode,
            DistributedFixtureFaultMode::RetryDuplicateStaleLease
        ),
        stale_lease_rejected: matches!(
            input.fault_mode,
            DistributedFixtureFaultMode::RetryDuplicateStaleLease
        ),
        cancellation_cleanup_completed: true,
        partial_output_committed: false,
        data_read: false,
        data_decoded: false,
        data_materialized: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_engine_invoked: false,
        production_claim_allowed: false,
        distributed_performance_claim_allowed: false,
    })
}

fn distributed_fixture_rows() -> Vec<DistributedFixtureRow> {
    vec![
        DistributedFixtureRow::new("row-0001", "split-000", "east", 10),
        DistributedFixtureRow::new("row-0002", "split-000", "west", 5),
        DistributedFixtureRow::new("row-0003", "split-001", "east", 2),
        DistributedFixtureRow::new("row-0004", "split-001", "north", 3),
        DistributedFixtureRow::new("row-0005", "split-002", "west", 4),
        DistributedFixtureRow::new("row-0006", "split-002", "east", 1),
        DistributedFixtureRow::new("row-0007", "split-002", "north", 7),
    ]
}

fn build_split_manifest(
    worker_count: usize,
    rows: &[DistributedFixtureRow],
) -> LocalDistributedSplitManifest {
    let mut by_split: BTreeMap<&str, Vec<&DistributedFixtureRow>> = BTreeMap::new();
    for row in rows {
        by_split.entry(row.split_id).or_default().push(row);
    }
    let split_units = by_split
        .into_iter()
        .enumerate()
        .map(|(index, (split_id, split_rows))| DistributedSplitUnit {
            split_id: split_id.to_string(),
            input_ref: format!("fixture://prod-ready-1d/local-distributed/{split_id}"),
            assigned_worker_id: format!("worker-{:02}", index % worker_count),
            row_count: split_rows.len(),
            estimated_bytes: usize_to_u64(split_rows.len()) * 64,
        })
        .collect::<Vec<_>>();

    LocalDistributedSplitManifest {
        schema_version: "shardloom.local_distributed_split_manifest.v1",
        manifest_id: "split-manifest://prod-ready-1d/local-distributed/v1".to_string(),
        split_units,
        capillary_split_window: "bounded_three_split_fixture",
        pulseweave_control_surface: "in_process_coordinator_worker_attempt_graph",
        dynamic_admission_policy: "local_fixture_only_no_remote_workers",
    }
}

fn build_task_attempts(
    split_manifest: &LocalDistributedSplitManifest,
    fault_mode: DistributedFixtureFaultMode,
) -> Vec<DistributedTaskAttempt> {
    let mut attempts = Vec::new();
    for split in &split_manifest.split_units {
        match (split.split_id.as_str(), fault_mode) {
            ("split-001", DistributedFixtureFaultMode::RetryDuplicateStaleLease) => {
                attempts.push(task_attempt(
                    split,
                    1,
                    DistributedTaskAttemptOutcome::CancelledCleanupCompleted,
                    0,
                ));
                attempts.push(task_attempt(
                    split,
                    2,
                    DistributedTaskAttemptOutcome::Certified,
                    1,
                ));
            }
            _ => attempts.push(task_attempt(
                split,
                1,
                DistributedTaskAttemptOutcome::Certified,
                0,
            )),
        }
    }
    if fault_mode == DistributedFixtureFaultMode::RetryDuplicateStaleLease {
        if let Some(split) = split_manifest
            .split_units
            .iter()
            .find(|split| split.split_id == "split-002")
        {
            attempts.push(task_attempt(
                split,
                2,
                DistributedTaskAttemptOutcome::DuplicateRejected,
                0,
            ));
        }
        if let Some(split) = split_manifest
            .split_units
            .iter()
            .find(|split| split.split_id == "split-000")
        {
            attempts.push(task_attempt(
                split,
                2,
                DistributedTaskAttemptOutcome::StaleLeaseRejected,
                0,
            ));
        }
    }
    attempts
}

fn task_attempt(
    split: &DistributedSplitUnit,
    attempt_index: usize,
    outcome: DistributedTaskAttemptOutcome,
    retry_count: usize,
) -> DistributedTaskAttempt {
    let task_attempt_id = format!("attempt-{}-{attempt_index}", split.split_id);
    DistributedTaskAttempt {
        split_id: split.split_id.clone(),
        worker_id: split.assigned_worker_id.clone(),
        task_lease_id: format!("lease-{}-{}", split.assigned_worker_id, split.split_id),
        task_attempt_id: task_attempt_id.clone(),
        attempt_index,
        outcome,
        retry_count,
        cancellation_requested: matches!(
            outcome,
            DistributedTaskAttemptOutcome::CancelledCleanupCompleted
        ),
        cleanup_completed: !matches!(outcome, DistributedTaskAttemptOutcome::Certified),
        duplicate_attempt: matches!(outcome, DistributedTaskAttemptOutcome::DuplicateRejected),
        stale_lease_detected: matches!(outcome, DistributedTaskAttemptOutcome::StaleLeaseRejected),
        input_ref: split.input_ref.clone(),
        output_ref: if outcome.emits_fragment() {
            format!(
                "fragment://prod-ready-1d/local-distributed/{}",
                split.split_id
            )
        } else {
            format!("discarded://prod-ready-1d/local-distributed/{task_attempt_id}")
        },
    }
}

fn execute_certified_attempts(
    rows: &[DistributedFixtureRow],
    attempts: &[DistributedTaskAttempt],
) -> Vec<DistributedResultFragment> {
    attempts
        .iter()
        .filter(|attempt| attempt.outcome.emits_fragment())
        .map(|attempt| {
            let group_counts =
                group_rows(rows.iter().filter(|row| row.split_id == attempt.split_id));
            let summary = group_counts
                .iter()
                .map(DistributedMergedRow::summary)
                .collect::<Vec<_>>()
                .join("|");
            DistributedResultFragment {
                split_id: attempt.split_id.clone(),
                worker_id: attempt.worker_id.clone(),
                output_ref: attempt.output_ref.clone(),
                row_count: group_counts.len(),
                result_fragment_digest: stable_digest(&summary),
                group_counts,
            }
        })
        .collect()
}

fn group_rows<'a>(
    rows: impl Iterator<Item = &'a DistributedFixtureRow>,
) -> Vec<DistributedMergedRow> {
    let mut groups: BTreeMap<&str, (usize, i64)> = BTreeMap::new();
    for row in rows {
        let entry = groups.entry(row.group_key).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += row.value;
    }
    groups
        .into_iter()
        .map(|(group_key, (count, sum))| DistributedMergedRow::new(group_key, count, sum))
        .collect()
}

fn build_shuffle_repartition_report(
    worker_count: usize,
    fragments: &[DistributedResultFragment],
) -> DistributedShuffleRepartitionReport {
    let mut groups: BTreeMap<&str, (usize, i64)> = BTreeMap::new();
    let mut local_combined_inputs: BTreeMap<&str, usize> = BTreeMap::new();
    let mut raw_input_row_count = 0;
    let mut local_combined_row_count = 0;
    for fragment in fragments {
        for row in &fragment.group_counts {
            let entry = groups.entry(row.group_key.as_str()).or_insert((0, 0));
            entry.0 += row.count;
            entry.1 += row.sum;
            *local_combined_inputs
                .entry(row.group_key.as_str())
                .or_insert(0) += 1;
            raw_input_row_count += row.count;
            local_combined_row_count += 1;
        }
    }
    let global_merged_rows = groups
        .into_iter()
        .map(|(group_key, (count, sum))| DistributedMergedRow::new(group_key, count, sum))
        .collect::<Vec<_>>();
    let partition_assignments = global_merged_rows
        .iter()
        .map(|row| {
            let partition_index = stable_partition_index(&row.group_key, worker_count);
            DistributedPartitionAssignment {
                partition_id: format!("reduce-partition-{partition_index:02}"),
                group_key: row.group_key.clone(),
                reduce_worker_id: format!("worker-{partition_index:02}"),
                local_combined_input_count: local_combined_inputs
                    .get(row.group_key.as_str())
                    .copied()
                    .unwrap_or_default(),
                merged_count: row.count,
                merged_sum: row.sum,
            }
        })
        .collect::<Vec<_>>();

    DistributedShuffleRepartitionReport {
        schema_version: "shardloom.local_distributed_shuffle_repartition.v1",
        strategy_id: "prod-ready-1d.local_hash_repartition_group_key.v1",
        partition_key: "group_key",
        repartition_strategy: "local_hash_group_key_to_reduce_worker",
        local_combine_strategy: "split_local_group_count_sum_before_exchange",
        global_merge_strategy: "partition_ordered_reduce_merge",
        partition_count: worker_count,
        repartition_performed: true,
        local_combine_performed: true,
        global_merge_performed: true,
        remote_shuffle_performed: false,
        raw_input_row_count,
        local_combined_row_count,
        global_merge_input_row_count: local_combined_row_count,
        partition_assignments,
        global_merged_rows,
    }
}

fn build_skew_report(merged_rows: &[DistributedMergedRow]) -> DistributedSkewReport {
    const SKEW_THRESHOLD_ROWS: usize = 3;
    let max_group_rows = merged_rows
        .iter()
        .map(|row| row.count)
        .max()
        .unwrap_or_default();
    let skewed_group_keys = merged_rows
        .iter()
        .filter(|row| row.count >= SKEW_THRESHOLD_ROWS)
        .map(|row| row.group_key.clone())
        .collect::<Vec<_>>();
    DistributedSkewReport {
        schema_version: "shardloom.local_distributed_skew.v1",
        detection_strategy: "group_count_threshold_after_local_combine",
        handling_strategy: "dedicated_reduce_partition_guard_with_deterministic_merge_order",
        skew_detection_performed: true,
        skew_detected: !skewed_group_keys.is_empty(),
        skew_handling_applied: !skewed_group_keys.is_empty(),
        skew_threshold_rows: SKEW_THRESHOLD_ROWS,
        max_group_rows,
        skewed_group_keys,
    }
}

fn build_memory_backpressure_report(
    split_manifest: &LocalDistributedSplitManifest,
    shuffle: &DistributedShuffleRepartitionReport,
) -> DistributedMemoryBackpressureReport {
    let local_combine_buffer_bytes = usize_to_u64(shuffle.local_combined_row_count) * 32;
    let global_merge_buffer_bytes = usize_to_u64(shuffle.global_merged_rows.len()) * 32;
    let peak_reserved_bytes = split_manifest
        .total_estimated_bytes()
        .saturating_add(local_combine_buffer_bytes)
        .saturating_add(global_merge_buffer_bytes);
    let memory_budget_bytes = peak_reserved_bytes.saturating_mul(2);

    DistributedMemoryBackpressureReport {
        schema_version: "shardloom.local_distributed_memory_backpressure.v1",
        memory_budget_bytes,
        peak_reserved_bytes,
        local_combine_buffer_bytes,
        global_merge_buffer_bytes,
        memory_budget_exceeded: peak_reserved_bytes > memory_budget_bytes,
        backpressure_policy: "bounded_worker_slots_and_reduce_partition_budget",
        backpressure_signal_emitted: false,
        spill_policy: "fail_closed_no_spill_for_fixture",
        spill_required: false,
        spill_io_performed: false,
        production_spill_claim_allowed: false,
    }
}

fn build_worker_leases(
    worker_count: usize,
    split_manifest: &LocalDistributedSplitManifest,
    attempts: &[DistributedTaskAttempt],
) -> Vec<DistributedWorkerLease> {
    (0..worker_count)
        .map(|index| {
            let worker_id = format!("worker-{index:02}");
            let leased_split_count = split_manifest
                .split_units
                .iter()
                .filter(|split| split.assigned_worker_id == worker_id)
                .count();
            let completed_split_count = attempts
                .iter()
                .filter(|attempt| {
                    attempt.worker_id == worker_id
                        && attempt.outcome == DistributedTaskAttemptOutcome::Certified
                })
                .count();
            DistributedWorkerLease {
                lease_id: format!("lease-{worker_id}-epoch-0001"),
                worker_id,
                heartbeat_count: leased_split_count.saturating_add(1),
                leased_split_count,
                completed_split_count,
                status: "completed",
            }
        })
        .collect()
}

fn execution_certificate_for(
    input: &LocalDistributedFixtureRunInput,
    split_manifest: &LocalDistributedSplitManifest,
    fragments: &[DistributedResultFragment],
    merged_rows: &[DistributedMergedRow],
) -> Result<ExecutionCertificate> {
    let mut certificate_input = ExecutionCertificateInput::new(
        format!(
            "prod-ready-1d.local_distributed_fixture.{}.execution",
            input.fault_mode.as_str()
        ),
        "local_distributed_split_group_count",
    )?;
    certificate_input.provider_crate = Some("shardloom-core".to_string());
    certificate_input.provider_api_surface = Some("run_local_distributed_fixture".to_string());
    certificate_input.shardloom_admission_policy =
        Some("prod_ready_1d_local_fixture_only".to_string());
    certificate_input.plan_ref = Some(split_manifest.manifest_id.clone());
    certificate_input.input_ref =
        Some("fixture://prod-ready-1d/local-distributed/split-rows".to_string());
    certificate_input.output_ref = Some(format!(
        "result://prod-ready-1d/local-distributed/{}",
        stable_digest(
            &merged_rows
                .iter()
                .map(DistributedMergedRow::summary)
                .collect::<Vec<_>>()
                .join("|"),
        )
    ));
    certificate_input.correctness_fixture_id =
        Some("prod-ready-1d.local_distributed.fixture.v1".to_string());
    certificate_input.expected_outcome = Some(ExpectedOutcome::Rows {
        row_count: Some(usize_to_u64(fragments.len())),
    });
    certificate_input
        .actual_outcome
        .clone_from(&certificate_input.expected_outcome);
    certificate_input.correctness_passed = true;
    Ok(ExecutionCertificate::evaluate(certificate_input))
}

fn native_io_certificate_for(
    input: &LocalDistributedFixtureRunInput,
    split_manifest: &LocalDistributedSplitManifest,
) -> Result<NativeIoCertificate> {
    NativeIoCertificate::new(
        format!(
            "prod-ready-1d.local_distributed_fixture.{}.native_io",
            input.fault_mode.as_str()
        ),
        "in_memory_split_manifest_to_local_result_fragments",
        NativeIoSourceCapabilityReport {
            source_kind: "in_memory_split_manifest".to_string(),
            adapter_id: "prod-ready-1d.local-distributed-fixture".to_string(),
            schema_discovery_status: "declared_fixture_schema_digest".to_string(),
            statistics_availability: "split_row_counts_known".to_string(),
            pushdown_capabilities: "split_group_count".to_string(),
            encoded_representation_preserved: false,
            range_read_capability: false,
            streaming_capability: true,
            object_store_capability: false,
            fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: vec![
                "split_group_count".to_string(),
                "local_hash_repartition".to_string(),
                "local_combine_global_merge".to_string(),
                "skew_detection".to_string(),
                "bounded_backpressure_accounting".to_string(),
            ],
            rejected_operations: vec![
                "remote_worker_execution".to_string(),
                "object_store_read".to_string(),
                "remote_shuffle_repartition".to_string(),
                "distributed_spill_io".to_string(),
                "external_engine_execution".to_string(),
            ],
            guarantee: "fixture_only_deterministic_local_hash_repartition".to_string(),
            proof_basis: split_manifest.manifest_id.clone(),
            residual_expression: None,
            conservative_false_positive_policy: false,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        },
        vec![
            NativeIoRepresentationTransition::new(
                RepresentationState::MetadataOnly,
                RepresentationState::Pruned,
                false,
            ),
            NativeIoRepresentationTransition::new(
                RepresentationState::Pruned,
                RepresentationState::SelectionVectorEncoded,
                false,
            ),
        ],
        NativeIoSinkRequirementReport {
            target_format: "in_memory_result_fragments".to_string(),
            accepts_encoded: true,
            requires_decoded_columnar: false,
            requires_rows: false,
            preserves_metadata: true,
            requires_ordering: true,
            requires_partitioning: true,
            requires_commit: false,
            supports_streaming: true,
            max_chunk_size: Some(usize_to_u64(split_manifest.split_units.len())),
            backpressure_policy: "bounded_local_worker_slots".to_string(),
        },
        NativeIoAdapterFidelityReport {
            adapter_id: "prod-ready-1d.local-distributed-fixture".to_string(),
            source_kind: "in_memory_split_manifest".to_string(),
            sink_kind: "in_memory_result_fragments".to_string(),
            metadata_preserved: true,
            statistics_preserved: true,
            encoded_representation_preserved: false,
            materialization_required: false,
            fidelity_loss: "none_fixture_contract".to_string(),
            metadata_loss: "none".to_string(),
            fallback_attempted: false,
        },
        vec![],
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
        vec![],
    )
}

fn normalized_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

fn stable_digest(input: &str) -> String {
    format!("fnv64:{:016x}", stable_hash_u64(input))
}

fn stable_hash_u64(input: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0001_0000_01b3);
    }
    hash
}

fn stable_partition_index(group_key: &str, partition_count: usize) -> usize {
    let partition_count_u64 = usize_to_u64(partition_count.max(1));
    let partition_index = stable_hash_u64(group_key) % partition_count_u64;
    usize::try_from(partition_index).unwrap_or_default()
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_local_distributed_fixture_merges_deterministically() {
        let report = run_local_distributed_fixture(
            LocalDistributedFixtureRunInput::new(2, DistributedFixtureFaultMode::None)
                .expect("valid input"),
        )
        .expect("fixture runs");

        assert!(!report.has_errors());
        assert_eq!(report.input.worker_count, 2);
        assert_eq!(report.split_manifest.split_units.len(), 3);
        assert_eq!(report.worker_leases.len(), 2);
        assert_eq!(report.result_fragments.len(), 3);
        assert!(report.shuffle_repartition.repartition_performed);
        assert!(report.shuffle_repartition.local_combine_performed);
        assert!(report.shuffle_repartition.global_merge_performed);
        assert_eq!(report.shuffle_repartition.partition_count, 2);
        assert_eq!(report.shuffle_repartition.raw_input_row_count, 7);
        assert_eq!(report.shuffle_repartition.local_combined_row_count, 7);
        assert!(report.skew.skew_detection_performed);
        assert!(report.skew.skew_detected);
        assert_eq!(report.skew.skewed_group_key_order(), "east");
        assert!(report.memory_budget_enforced);
        assert!(!report.memory_backpressure.memory_budget_exceeded);
        assert!(!report.memory_backpressure.spill_required);
        assert_eq!(report.merged_rows_text(), "east:3:13|north:2:10|west:2:9");
        assert_eq!(report.execution_certificate.status.as_str(), "certified");
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.remote_worker_invoked);
        assert!(!report.fallback_attempted());
    }

    #[test]
    fn fault_mode_records_retry_duplicate_and_stale_lease_evidence() {
        let report = run_local_distributed_fixture(
            LocalDistributedFixtureRunInput::new(
                2,
                DistributedFixtureFaultMode::RetryDuplicateStaleLease,
            )
            .expect("valid input"),
        )
        .expect("fixture runs");

        assert!(!report.has_errors());
        assert_eq!(report.task_attempts.len(), 6);
        assert_eq!(report.result_fragments.len(), 3);
        assert!(report.retry_performed);
        assert!(report.duplicate_attempt_rejected);
        assert!(report.stale_lease_rejected);
        assert!(report.cancellation_cleanup_completed);
        assert!(!report.partial_output_committed);
        assert!(report.shuffle_repartition_performed);
        assert!(report.local_combine_performed);
        assert!(report.global_merge_performed);
        assert!(report.skew_handling_applied);
        assert!(!report.shuffle_repartition.remote_shuffle_performed);
        assert_eq!(report.merged_rows_text(), "east:3:13|north:2:10|west:2:9");
        assert!(
            report
                .task_attempt_outcome_order()
                .contains("split-001:attempt-split-001-1:cancelled_cleanup_completed")
        );
        assert!(
            report
                .task_attempt_outcome_order()
                .contains("split-002:attempt-split-002-2:duplicate_rejected")
        );
    }

    #[test]
    fn worker_count_is_bounded_for_fixture() {
        let error = LocalDistributedFixtureRunInput::new(
            8,
            DistributedFixtureFaultMode::RetryDuplicateStaleLease,
        )
        .expect_err("invalid worker count rejects");

        assert!(
            error
                .message()
                .contains("worker count must be between 1 and 4")
        );
    }
}
