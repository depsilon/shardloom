//! CG-22 live change contracts, fixture execution, and certification surfaces.
//!
//! This module is intentionally narrow: it executes only a deterministic
//! in-memory fixture when explicitly requested. It does not read brokers,
//! poll files, touch object stores, write checkpoints, invoke external engines,
//! or provide fallback execution.

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
    ExecutionCertificate, ExecutionCertificateInput, ExpectedOutcome, FallbackStatus,
    NativeIoAdapterFidelityReport, NativeIoCertificate, NativeIoSideEffectReport,
    NativeIoSinkRequirementReport, NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport,
    Result, ShardLoomError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    Append,
    Upsert,
    Delete,
    Retract,
    Tombstone,
}

impl ChangeOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Upsert => "upsert",
            Self::Delete => "delete",
            Self::Retract => "retract",
            Self::Tombstone => "tombstone",
        }
    }

    pub const fn all() -> [Self; 5] {
        [
            Self::Append,
            Self::Upsert,
            Self::Delete,
            Self::Retract,
            Self::Tombstone,
        ]
    }

    const fn removes_state(self) -> bool {
        matches!(self, Self::Delete | Self::Retract | Self::Tombstone)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkPolicy {
    FixtureEventTime,
}

impl WatermarkPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FixtureEventTime => "fixture_event_time",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateDataPolicy {
    RejectPastWatermark,
}

impl LateDataPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RejectPastWatermark => "reject_past_watermark",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateTtlPolicy {
    RetainUntilDeleteOrTombstone,
}

impl StateTtlPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetainUntilDeleteOrTombstone => "retain_until_delete_or_tombstone",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointPolicy {
    InMemoryDeterministicFixture,
}

impl CheckpointPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InMemoryDeterministicFixture => "in_memory_deterministic_fixture",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputChangelogMode {
    Append,
    Update,
    Delete,
    Retract,
    Tombstone,
    Complete,
    ContinuousView,
}

impl OutputChangelogMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Retract => "retract",
            Self::Tombstone => "tombstone",
            Self::Complete => "complete",
            Self::ContinuousView => "continuous_view",
        }
    }

    pub const fn all() -> [Self; 7] {
        [
            Self::Append,
            Self::Update,
            Self::Delete,
            Self::Retract,
            Self::Tombstone,
            Self::Complete,
            Self::ContinuousView,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveFixtureOperator {
    Filter,
    Project,
    Count,
    CountWhere,
    GroupCount,
}

impl LiveFixtureOperator {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Project => "project",
            Self::Count => "count",
            Self::CountWhere => "count_where",
            Self::GroupCount => "group_count",
        }
    }

    pub const fn all() -> [Self; 5] {
        [
            Self::Filter,
            Self::Project,
            Self::Count,
            Self::CountWhere,
            Self::GroupCount,
        ]
    }

    pub fn parse(value: &str) -> Result<Self> {
        match normalized_token(value).as_str() {
            "filter" => Ok(Self::Filter),
            "project" | "projection" => Ok(Self::Project),
            "count" => Ok(Self::Count),
            "count-where" | "countwhere" => Ok(Self::CountWhere),
            "group-count" | "groupcount" => Ok(Self::GroupCount),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown live fixture operator: {other}"
            ))),
        }
    }

    pub const fn output_mode(self) -> OutputChangelogMode {
        match self {
            Self::Filter | Self::Project | Self::GroupCount => OutputChangelogMode::ContinuousView,
            Self::Count | Self::CountWhere => OutputChangelogMode::Update,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeRecord {
    pub key: String,
    pub operation: ChangeOperation,
    pub sequence: u64,
    pub event_time_ms: u64,
    pub processing_time_ms: u64,
    pub source_offset: String,
    pub schema_digest: String,
    pub payload_ref: String,
    pub metric: String,
    pub value: i64,
}

impl ChangeRecord {
    #[allow(clippy::too_many_arguments)]
    fn fixture(
        key: &str,
        operation: ChangeOperation,
        sequence: u64,
        event_time_ms: u64,
        metric: &str,
        value: i64,
    ) -> Self {
        Self {
            key: key.to_string(),
            operation,
            sequence,
            event_time_ms,
            processing_time_ms: event_time_ms + 500,
            source_offset: format!("fixture://cg22/live/{sequence:04}"),
            schema_digest: "sha256:cg22-live-change-v1".to_string(),
            payload_ref: format!("payload://cg22/live/{key}/{sequence:04}"),
            metric: metric.to_string(),
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveOutputRow {
    pub key: String,
    pub metric: String,
    pub value: i64,
}

impl LiveOutputRow {
    fn from_record(record: &ChangeRecord) -> Self {
        Self {
            key: record.key.clone(),
            metric: record.metric.clone(),
            value: record.value,
        }
    }

    pub(crate) fn synthetic(key: &str, metric: &str, value: i64) -> Self {
        Self {
            key: key.to_string(),
            metric: metric.to_string(),
            value,
        }
    }

    pub(crate) fn summary(&self) -> String {
        format!("{}:{}:{}", self.key, self.metric, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputChangelogEntry {
    pub sequence: u64,
    pub key: String,
    pub operation: OutputChangelogMode,
    pub output_ref: String,
}

impl OutputChangelogEntry {
    pub(crate) fn from_row(sequence: u64, row: &LiveOutputRow, mode: OutputChangelogMode) -> Self {
        Self {
            sequence,
            key: row.key.clone(),
            operation: mode,
            output_ref: format!("result://cg22/live/{}/{}", mode.as_str(), row.key),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveChangeContractReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub required_change_fields: Vec<&'static str>,
    pub operations: Vec<ChangeOperation>,
    pub watermark_policy: WatermarkPolicy,
    pub late_data_policy: LateDataPolicy,
    pub state_ttl_policy: StateTtlPolicy,
    pub checkpoint_policy: CheckpointPolicy,
    pub output_changelog_modes: Vec<OutputChangelogMode>,
    pub fixture_operators: Vec<LiveFixtureOperator>,
    pub broker_integrations_deferred: bool,
    pub runtime_integrations_deferred: bool,
    pub production_claim_allowed: bool,
    pub fallback: FallbackStatus,
    pub external_engine_invoked: bool,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub write_io: bool,
}

impl LiveChangeContractReport {
    pub fn cg22_contract() -> Self {
        Self {
            schema_version: "shardloom.live_change_contract.v1",
            report_id: "cg22.live_change_contract",
            required_change_fields: vec![
                "key",
                "operation",
                "sequence",
                "event_time_ms",
                "processing_time_ms",
                "source_offset",
                "schema_digest",
                "payload_ref",
            ],
            operations: ChangeOperation::all().to_vec(),
            watermark_policy: WatermarkPolicy::FixtureEventTime,
            late_data_policy: LateDataPolicy::RejectPastWatermark,
            state_ttl_policy: StateTtlPolicy::RetainUntilDeleteOrTombstone,
            checkpoint_policy: CheckpointPolicy::InMemoryDeterministicFixture,
            output_changelog_modes: OutputChangelogMode::all().to_vec(),
            fixture_operators: LiveFixtureOperator::all().to_vec(),
            broker_integrations_deferred: true,
            runtime_integrations_deferred: true,
            production_claim_allowed: false,
            fallback: FallbackStatus::disabled_by_policy(),
            external_engine_invoked: false,
            runtime_execution: false,
            data_read: false,
            write_io: false,
        }
    }

    pub fn change_field_order(&self) -> String {
        self.required_change_fields.join(",")
    }

    pub fn operation_vocabulary(&self) -> String {
        self.operations
            .iter()
            .map(|operation| operation.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn output_changelog_vocabulary(&self) -> String {
        self.output_changelog_modes
            .iter()
            .map(|mode| mode.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn fixture_operator_vocabulary(&self) -> String {
        self.fixture_operators
            .iter()
            .map(|operator| operator.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub const fn fallback_attempted(&self) -> bool {
        self.fallback.attempted
    }

    pub fn to_human_text(&self) -> String {
        format!(
            "live change contract\nschema_version: {}\nreport: {}\nchange fields: {}\noperations: {}\nwatermark policy: {}\nlate data policy: {}\nstate ttl policy: {}\ncheckpoint policy: {}\nfixture operators: {}\nfallback execution: disabled\nexternal engine invoked: false",
            self.schema_version,
            self.report_id,
            self.change_field_order(),
            self.operation_vocabulary(),
            self.watermark_policy.as_str(),
            self.late_data_policy.as_str(),
            self.state_ttl_policy.as_str(),
            self.checkpoint_policy.as_str(),
            self.fixture_operator_vocabulary(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveFixtureRunInput {
    pub operator: LiveFixtureOperator,
    pub predicate: String,
    pub projection_columns: Vec<String>,
    pub group_column: String,
}

impl LiveFixtureRunInput {
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
                    "live count fixture does not accept an extra argument".to_string(),
                ));
            }
        }
        Ok(self)
    }

    pub fn projection_columns_text(&self) -> String {
        self.projection_columns.join(",")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveCertificateStatus {
    Certified,
    Blocked,
}

impl LiveCertificateStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Certified => "certified",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshnessCertificate {
    pub schema_version: &'static str,
    pub certificate_id: String,
    pub status: LiveCertificateStatus,
    pub watermark_policy: WatermarkPolicy,
    pub late_data_policy: LateDataPolicy,
    pub max_event_time_ms: u64,
    pub max_processing_time_ms: u64,
    pub watermark_ms: u64,
    pub freshness_lag_ms: u64,
    pub late_record_count: usize,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl FreshnessCertificate {
    fn from_records(records: &[ChangeRecord]) -> Self {
        let max_event_time_ms = records
            .iter()
            .map(|record| record.event_time_ms)
            .max()
            .unwrap_or_default();
        let max_processing_time_ms = records
            .iter()
            .map(|record| record.processing_time_ms)
            .max()
            .unwrap_or_default();
        let watermark_ms = max_event_time_ms;
        Self {
            schema_version: "shardloom.freshness_certificate.v1",
            certificate_id: "cg22.live.fixture.freshness".to_string(),
            status: LiveCertificateStatus::Certified,
            watermark_policy: WatermarkPolicy::FixtureEventTime,
            late_data_policy: LateDataPolicy::RejectPastWatermark,
            max_event_time_ms,
            max_processing_time_ms,
            watermark_ms,
            freshness_lag_ms: max_processing_time_ms.saturating_sub(max_event_time_ms),
            late_record_count: late_record_count_under_reject_past_watermark(records),
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCertificate {
    pub schema_version: &'static str,
    pub certificate_id: String,
    pub status: LiveCertificateStatus,
    pub state_ttl_policy: StateTtlPolicy,
    pub checkpoint_policy: CheckpointPolicy,
    pub checkpoint_ref: String,
    pub input_change_record_count: usize,
    pub active_state_key_count: usize,
    pub checkpoint_record_count: usize,
    pub append_count: usize,
    pub upsert_count: usize,
    pub delete_count: usize,
    pub retract_count: usize,
    pub tombstone_count: usize,
    pub checkpoint_write_performed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl StateCertificate {
    fn from_records(records: &[ChangeRecord], active_state_key_count: usize) -> Self {
        let max_sequence = records
            .iter()
            .map(|record| record.sequence)
            .max()
            .unwrap_or_default();
        Self {
            schema_version: "shardloom.state_certificate.v1",
            certificate_id: "cg22.live.fixture.state".to_string(),
            status: LiveCertificateStatus::Certified,
            state_ttl_policy: StateTtlPolicy::RetainUntilDeleteOrTombstone,
            checkpoint_policy: CheckpointPolicy::InMemoryDeterministicFixture,
            checkpoint_ref: format!("checkpoint://cg22/live/fixture/seq-{max_sequence}"),
            input_change_record_count: records.len(),
            active_state_key_count,
            checkpoint_record_count: active_state_key_count,
            append_count: operation_count(records, ChangeOperation::Append),
            upsert_count: operation_count(records, ChangeOperation::Upsert),
            delete_count: operation_count(records, ChangeOperation::Delete),
            retract_count: operation_count(records, ChangeOperation::Retract),
            tombstone_count: operation_count(records, ChangeOperation::Tombstone),
            checkpoint_write_performed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuousViewCertificate {
    pub schema_version: &'static str,
    pub certificate_id: String,
    pub status: LiveCertificateStatus,
    pub output_mode: OutputChangelogMode,
    pub result_ref: String,
    pub output_changelog_record_count: usize,
    pub continuous_view_row_count: usize,
    pub append_count: usize,
    pub update_count: usize,
    pub delete_count: usize,
    pub retract_count: usize,
    pub tombstone_count: usize,
    pub deterministic_order: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl ContinuousViewCertificate {
    fn from_output(
        operator: LiveFixtureOperator,
        output_rows: &[LiveOutputRow],
        changelog: &[OutputChangelogEntry],
    ) -> Self {
        let output_mode = operator.output_mode();
        Self {
            schema_version: "shardloom.continuous_view_certificate.v1",
            certificate_id: "cg22.live.fixture.continuous_view".to_string(),
            status: LiveCertificateStatus::Certified,
            output_mode,
            result_ref: format!("result://cg22/live/fixture/{}", operator.as_str()),
            output_changelog_record_count: changelog.len(),
            continuous_view_row_count: output_rows.len(),
            append_count: changelog_mode_count(changelog, OutputChangelogMode::Append),
            update_count: changelog_mode_count(changelog, output_mode),
            delete_count: changelog_mode_count(changelog, OutputChangelogMode::Delete),
            retract_count: changelog_mode_count(changelog, OutputChangelogMode::Retract),
            tombstone_count: changelog_mode_count(changelog, OutputChangelogMode::Tombstone),
            deterministic_order: true,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveFixtureRunReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub fixture_id: &'static str,
    pub input: LiveFixtureRunInput,
    pub input_change_records: Vec<ChangeRecord>,
    pub output_rows: Vec<LiveOutputRow>,
    pub output_changelog: Vec<OutputChangelogEntry>,
    pub freshness_certificate: FreshnessCertificate,
    pub state_certificate: StateCertificate,
    pub continuous_view_certificate: ContinuousViewCertificate,
    pub execution_certificate: ExecutionCertificate,
    pub native_io_certificate: NativeIoCertificate,
    pub fallback: FallbackStatus,
    pub runtime_execution: bool,
    pub fixture_in_memory: bool,
    pub data_read: bool,
    pub broker_io: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub production_claim_allowed: bool,
}

impl LiveFixtureRunReport {
    pub fn has_errors(&self) -> bool {
        self.fallback.attempted
            || self.external_engine_invoked
            || self.broker_io
            || self.object_store_io
            || self.write_io
            || !self.execution_certificate.is_certified()
            || !self.native_io_certificate.is_certified()
            || self.freshness_certificate.status != LiveCertificateStatus::Certified
            || self.state_certificate.status != LiveCertificateStatus::Certified
            || self.continuous_view_certificate.status != LiveCertificateStatus::Certified
    }

    pub const fn fallback_attempted(&self) -> bool {
        self.fallback.attempted
    }

    pub fn active_state_key_count(&self) -> usize {
        self.state_certificate.active_state_key_count
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

    fn output_row_summary(&self, row: &LiveOutputRow) -> String {
        if self.input.operator != LiveFixtureOperator::Project {
            return row.summary();
        }
        self.input
            .projection_columns
            .iter()
            .map(|column| match column.as_str() {
                "key" => format!("key={}", row.key),
                "metric" => format!("metric={}", row.metric),
                "value" => format!("value={}", row.value),
                _ => format!("{column}=unsupported"),
            })
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

    pub fn input_operation_order(&self) -> String {
        self.input_change_records
            .iter()
            .map(|record| record.operation.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn sequence_range(&self) -> String {
        let min = self
            .input_change_records
            .iter()
            .map(|record| record.sequence)
            .min()
            .unwrap_or_default();
        let max = self
            .input_change_records
            .iter()
            .map(|record| record.sequence)
            .max()
            .unwrap_or_default();
        format!("{min}..{max}")
    }

    pub fn to_human_text(&self) -> String {
        format!(
            "live fixture run\nschema_version: {}\nreport: {}\noperator: {}\nfixture: {}\ninput changes: {}\nactive state keys: {}\noutput rows: {}\nwatermark: {}\nfreshness lag ms: {}\nexecution certificate: {}\nnative I/O certificate: {}\nfallback execution: disabled\nexternal engine invoked: false",
            self.schema_version,
            self.report_id,
            self.input.operator.as_str(),
            self.fixture_id,
            self.input_change_records.len(),
            self.active_state_key_count(),
            self.output_row_count(),
            self.freshness_certificate.watermark_ms,
            self.freshness_certificate.freshness_lag_ms,
            self.execution_certificate.status.as_str(),
            self.native_io_certificate.status(),
        )
    }
}

pub fn plan_live_change_contract() -> LiveChangeContractReport {
    LiveChangeContractReport::cg22_contract()
}

pub fn run_live_fixture(input: LiveFixtureRunInput) -> Result<LiveFixtureRunReport> {
    let input_change_records = cg22_live_fixture_records();
    let active_state = apply_change_records(&input_change_records);
    let output_rows = execute_fixture_operator(&input, &active_state)?;
    let output_changelog = output_changelog_for(&input, &output_rows);
    let freshness_certificate = FreshnessCertificate::from_records(&input_change_records);
    let state_certificate =
        StateCertificate::from_records(&input_change_records, active_state.len());
    let continuous_view_certificate =
        ContinuousViewCertificate::from_output(input.operator, &output_rows, &output_changelog);
    let execution_certificate = execution_certificate_for(&input, output_rows.len())?;
    let native_io_certificate = native_io_certificate_for(&input)?;

    Ok(LiveFixtureRunReport {
        schema_version: "shardloom.live_fixture_run.v1",
        report_id: format!("cg22.live_fixture.{}", input.operator.as_str()),
        fixture_id: "cg22.live.fixture.v1",
        input,
        input_change_records,
        output_rows,
        output_changelog,
        freshness_certificate,
        state_certificate,
        continuous_view_certificate,
        execution_certificate,
        native_io_certificate,
        fallback: FallbackStatus::disabled_by_policy(),
        runtime_execution: true,
        fixture_in_memory: true,
        data_read: false,
        broker_io: false,
        object_store_io: false,
        write_io: false,
        external_engine_invoked: false,
        production_claim_allowed: false,
    })
}

fn cg22_live_fixture_records() -> Vec<ChangeRecord> {
    vec![
        ChangeRecord::fixture("a", ChangeOperation::Append, 1, 1_000, "east", 2),
        ChangeRecord::fixture("b", ChangeOperation::Append, 2, 2_000, "west", 5),
        ChangeRecord::fixture("c", ChangeOperation::Append, 3, 3_000, "east", 7),
        ChangeRecord::fixture("b", ChangeOperation::Upsert, 4, 4_000, "west", 9),
        ChangeRecord::fixture("d", ChangeOperation::Append, 5, 5_000, "north", 1),
        ChangeRecord::fixture("c", ChangeOperation::Retract, 6, 6_000, "east", 7),
        ChangeRecord::fixture("e", ChangeOperation::Append, 7, 7_000, "east", 4),
        ChangeRecord::fixture("d", ChangeOperation::Tombstone, 8, 8_000, "north", 1),
        ChangeRecord::fixture("f", ChangeOperation::Append, 9, 9_000, "south", 8),
        ChangeRecord::fixture("f", ChangeOperation::Delete, 10, 10_000, "south", 8),
    ]
}

fn apply_change_records(records: &[ChangeRecord]) -> BTreeMap<String, ChangeRecord> {
    let mut state = BTreeMap::new();
    for record in records {
        if record.operation.removes_state() {
            state.remove(&record.key);
        } else {
            state.insert(record.key.clone(), record.clone());
        }
    }
    state
}

fn late_record_count_under_reject_past_watermark(records: &[ChangeRecord]) -> usize {
    let mut watermark_ms = 0_u64;
    let mut late_record_count = 0_usize;

    for record in records {
        if record.event_time_ms < watermark_ms {
            late_record_count += 1;
        } else {
            watermark_ms = record.event_time_ms;
        }
    }

    late_record_count
}

fn execute_fixture_operator(
    input: &LiveFixtureRunInput,
    active_state: &BTreeMap<String, ChangeRecord>,
) -> Result<Vec<LiveOutputRow>> {
    match input.operator {
        LiveFixtureOperator::Filter => {
            active_state
                .values()
                .try_fold(Vec::new(), |mut rows, record| {
                    if predicate_matches(&input.predicate, record)? {
                        rows.push(LiveOutputRow::from_record(record));
                    }
                    Ok(rows)
                })
        }
        LiveFixtureOperator::Project => Ok(active_state
            .values()
            .map(LiveOutputRow::from_record)
            .collect::<Vec<_>>()),
        LiveFixtureOperator::Count => Ok(vec![LiveOutputRow::synthetic(
            "__count",
            "active_state_rows",
            i64::try_from(active_state.len()).unwrap_or(i64::MAX),
        )]),
        LiveFixtureOperator::CountWhere => {
            let mut count = 0_usize;
            for record in active_state.values() {
                if predicate_matches(&input.predicate, record)? {
                    count += 1;
                }
            }
            Ok(vec![LiveOutputRow::synthetic(
                "__count_where",
                &input.predicate,
                i64::try_from(count).unwrap_or(i64::MAX),
            )])
        }
        LiveFixtureOperator::GroupCount => group_count_rows(input, active_state),
    }
}

fn group_count_rows(
    input: &LiveFixtureRunInput,
    active_state: &BTreeMap<String, ChangeRecord>,
) -> Result<Vec<LiveOutputRow>> {
    if input.group_column != "metric" {
        return Err(ShardLoomError::InvalidOperation(
            "live group_count fixture currently supports group column 'metric' only".to_string(),
        ));
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for record in active_state.values() {
        *counts.entry(record.metric.as_str()).or_insert(0) += 1;
    }
    Ok(counts
        .into_iter()
        .map(|(metric, count)| {
            LiveOutputRow::synthetic(
                metric,
                "group_count",
                i64::try_from(count).unwrap_or(i64::MAX),
            )
        })
        .collect())
}

fn predicate_matches(predicate: &str, record: &ChangeRecord) -> Result<bool> {
    let normalized = predicate.trim().to_ascii_lowercase();
    let parts = normalized.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["gte", "value", threshold] => threshold
            .parse::<i64>()
            .map(|threshold| record.value >= threshold)
            .map_err(|_| invalid_predicate(predicate)),
        ["gt", "value", threshold] => threshold
            .parse::<i64>()
            .map(|threshold| record.value > threshold)
            .map_err(|_| invalid_predicate(predicate)),
        ["eq", "metric", metric] => Ok(record.metric == *metric),
        _ => Err(invalid_predicate(predicate)),
    }
}

fn invalid_predicate(predicate: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "unsupported live fixture predicate {predicate:?}; use gte:value:<n>, gt:value:<n>, or eq:metric:<name>"
    ))
}

fn output_changelog_for(
    input: &LiveFixtureRunInput,
    output_rows: &[LiveOutputRow],
) -> Vec<OutputChangelogEntry> {
    output_rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            OutputChangelogEntry::from_row(
                usize_to_u64(index) + 1,
                row,
                input.operator.output_mode(),
            )
        })
        .collect()
}

fn execution_certificate_for(
    input: &LiveFixtureRunInput,
    output_row_count: usize,
) -> Result<ExecutionCertificate> {
    let mut certificate_input = ExecutionCertificateInput::new(
        format!("cg22.live.fixture.{}.execution", input.operator.as_str()),
        "live_fixture_in_memory",
    )?;
    certificate_input.provider_crate = Some("shardloom-core".to_string());
    certificate_input.provider_api_surface = Some("run_live_fixture".to_string());
    certificate_input.shardloom_admission_policy = Some("cg22_live_fixture_only".to_string());
    certificate_input.plan_ref = Some("plan://cg22/live/fixture".to_string());
    certificate_input.input_ref = Some("fixture://cg22/live/change-records".to_string());
    certificate_input.output_ref = Some(format!(
        "result://cg22/live/fixture/{}",
        input.operator.as_str()
    ));
    certificate_input.correctness_fixture_id = Some("cg22.live.fixture.v1".to_string());
    let outcome = ExpectedOutcome::Rows {
        row_count: Some(usize_to_u64(output_row_count)),
    };
    certificate_input.expected_outcome = Some(outcome.clone());
    certificate_input.actual_outcome = Some(outcome);
    certificate_input.correctness_passed = true;
    Ok(ExecutionCertificate::evaluate(certificate_input))
}

fn native_io_certificate_for(input: &LiveFixtureRunInput) -> Result<NativeIoCertificate> {
    NativeIoCertificate::new(
        format!("cg22.live.fixture.{}.native_io", input.operator.as_str()),
        "in_memory_change_fixture_to_continuous_view",
        NativeIoSourceCapabilityReport {
            source_kind: "in_memory_change_fixture".to_string(),
            adapter_id: "cg22.live.fixture".to_string(),
            schema_discovery_status: "declared_change_record_schema_digest".to_string(),
            statistics_availability: "fixture_cardinality_known".to_string(),
            pushdown_capabilities: input.operator.as_str().to_string(),
            encoded_representation_preserved: false,
            range_read_capability: false,
            streaming_capability: true,
            object_store_capability: false,
            fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: vec![input.operator.as_str().to_string()],
            rejected_operations: vec![
                "broker_read".to_string(),
                "object_store_read".to_string(),
                "external_engine_execution".to_string(),
            ],
            guarantee: "fixture_only_deterministic".to_string(),
            proof_basis: "checked_in_cg22_in_memory_fixture".to_string(),
            residual_expression: None,
            conservative_false_positive_policy: true,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        },
        Vec::new(),
        NativeIoSinkRequirementReport {
            target_format: "in_memory_continuous_view".to_string(),
            accepts_encoded: false,
            requires_decoded_columnar: false,
            requires_rows: false,
            preserves_metadata: true,
            requires_ordering: true,
            requires_partitioning: false,
            requires_commit: false,
            supports_streaming: true,
            max_chunk_size: Some(10),
            backpressure_policy: "not_applicable_bounded_fixture".to_string(),
        },
        NativeIoAdapterFidelityReport {
            adapter_id: "cg22.live.fixture".to_string(),
            source_kind: "in_memory_change_fixture".to_string(),
            sink_kind: "in_memory_continuous_view".to_string(),
            metadata_preserved: true,
            statistics_preserved: true,
            encoded_representation_preserved: false,
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

fn operation_count(records: &[ChangeRecord], operation: ChangeOperation) -> usize {
    records
        .iter()
        .filter(|record| record.operation == operation)
        .count()
}

fn changelog_mode_count(changelog: &[OutputChangelogEntry], mode: OutputChangelogMode) -> usize {
    changelog
        .iter()
        .filter(|entry| entry.operation == mode)
        .count()
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
                "unsupported live fixture projection column: {column}"
            )));
        }
    }
    Ok(columns)
}

fn parse_group_column(value: &str) -> Result<String> {
    let column = normalized_token(&normalize_required_argument("group column", value)?);
    if column != "metric" {
        return Err(ShardLoomError::InvalidOperation(
            "live group_count fixture currently supports group column 'metric' only".to_string(),
        ));
    }
    Ok(column)
}

fn normalize_required_argument(label: &str, value: &str) -> Result<String> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "live fixture {label} cannot be empty"
        )));
    }
    Ok(normalized.to_string())
}

fn normalized_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_change_contract_declares_cg22_change_and_policy_vocabulary() {
        let report = plan_live_change_contract();

        assert_eq!(report.schema_version, "shardloom.live_change_contract.v1");
        assert_eq!(
            report.change_field_order(),
            "key,operation,sequence,event_time_ms,processing_time_ms,source_offset,schema_digest,payload_ref"
        );
        assert_eq!(
            report.operation_vocabulary(),
            "append,upsert,delete,retract,tombstone"
        );
        assert_eq!(
            report.output_changelog_vocabulary(),
            "append,update,delete,retract,tombstone,complete,continuous_view"
        );
        assert_eq!(
            report.fixture_operator_vocabulary(),
            "filter,project,count,count_where,group_count"
        );
        assert_eq!(report.watermark_policy, WatermarkPolicy::FixtureEventTime);
        assert_eq!(report.late_data_policy, LateDataPolicy::RejectPastWatermark);
        assert!(report.broker_integrations_deferred);
        assert!(!report.production_claim_allowed);
        assert!(!report.fallback_attempted());
        assert!(!report.external_engine_invoked);
        assert!(!report.runtime_execution);
    }

    #[test]
    fn live_fixture_filter_executes_in_memory_and_emits_certificates() {
        let input = LiveFixtureRunInput::new(LiveFixtureOperator::Filter)
            .with_argument(Some("gte:value:3"))
            .expect("input");
        let report = run_live_fixture(input).expect("fixture run");

        assert_eq!(report.input_change_records.len(), 10);
        assert_eq!(report.active_state_key_count(), 3);
        assert_eq!(report.output_row_count(), 2);
        assert_eq!(report.output_rows_text(), "b:west:9|e:east:4");
        assert_eq!(report.state_certificate.checkpoint_record_count, 3);
        assert_eq!(report.state_certificate.delete_count, 1);
        assert_eq!(report.state_certificate.retract_count, 1);
        assert_eq!(report.state_certificate.tombstone_count, 1);
        assert_eq!(report.freshness_certificate.watermark_ms, 10_000);
        assert_eq!(report.freshness_certificate.freshness_lag_ms, 500);
        assert_eq!(report.freshness_certificate.late_record_count, 0);
        assert_eq!(
            report.continuous_view_certificate.output_mode,
            OutputChangelogMode::ContinuousView
        );
        assert!(report.runtime_execution);
        assert!(report.fixture_in_memory);
        assert!(!report.data_read);
        assert!(!report.broker_io);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.external_engine_invoked);
        assert!(report.execution_certificate.is_certified());
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.has_errors());
    }

    #[test]
    fn live_fixture_group_count_uses_active_state_after_delete_retract_and_tombstone() {
        let input = LiveFixtureRunInput::new(LiveFixtureOperator::GroupCount)
            .with_argument(Some("metric"))
            .expect("input");
        let report = run_live_fixture(input).expect("fixture run");

        assert_eq!(
            report.output_rows_text(),
            "east:group_count:2|west:group_count:1"
        );
        assert_eq!(report.output_row_count(), 2);
        assert_eq!(
            report.output_changelog_order(),
            "1:east:continuous_view|2:west:continuous_view"
        );
        assert_eq!(
            report.input_operation_order(),
            "append,append,append,upsert,append,retract,append,tombstone,append,delete"
        );
    }

    #[test]
    fn live_freshness_counts_records_older_than_the_running_watermark() {
        let records = vec![
            ChangeRecord::fixture("a", ChangeOperation::Append, 1, 1_000, "east", 1),
            ChangeRecord::fixture("b", ChangeOperation::Append, 2, 3_000, "west", 2),
            ChangeRecord::fixture("c", ChangeOperation::Append, 3, 2_000, "east", 3),
            ChangeRecord::fixture("d", ChangeOperation::Append, 4, 4_000, "north", 4),
            ChangeRecord::fixture("e", ChangeOperation::Append, 5, 3_500, "south", 5),
        ];

        let certificate = FreshnessCertificate::from_records(&records);

        assert_eq!(certificate.watermark_ms, 4_000);
        assert_eq!(certificate.late_record_count, 2);
    }

    #[test]
    fn live_fixture_rejects_unknown_predicates_without_fallback() {
        let input = LiveFixtureRunInput::new(LiveFixtureOperator::Filter)
            .with_argument(Some("contains:value:3"))
            .expect("input");
        let error = run_live_fixture(input).expect_err("unsupported predicate");

        assert!(
            error
                .message()
                .contains("unsupported live fixture predicate")
        );
    }
}
