//! Deterministic `PulseWeave` runtime-control policy for prepared/local work.
//!
//! `PulseWeave` is provider-neutral policy code. It plans bounded local task
//! admission, scarcity accounting, run-local feedback, and evidence gates
//! without reading data, performing I/O, mutating global state, or invoking
//! fallback engines.

use shardloom_core::{Result, ShardLoomError};

use crate::{ByteSize, MemoryBudget, MemoryPressureLevel, OperatorMemoryClass};

const PULSEWEAVE_SCHEMA_VERSION: &str = "shardloom.pulseweave.runtime_control.v1";
const FLOW_INVENTORY_SCHEMA_VERSION: &str = "shardloom.pulseweave.flow_inventory.v1";
const SCARCITY_LEDGER_SCHEMA_VERSION: &str = "shardloom.pulseweave.scarcity_ledger.v1";
const ENDOPULSE_SCHEMA_VERSION: &str = "shardloom.pulseweave.endopulse.v1";
const PROOFBOUND_SCHEMA_VERSION: &str = "shardloom.pulseweave.proofbound.v1";
const REQUIRED_PROOFBOUND_EVIDENCE: &str = "prepared_local_route,memory_budget,max_parallelism,task_estimates,materialization_decode_boundary,correctness_digest,output_digest,execution_certificate,native_io_certificate,no_fallback";

/// One local task shape supplied to `PulseWeave` policy planning.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct PulseWeaveTaskShape {
    pub task_id: String,
    pub task_label: String,
    pub operator_memory_class: OperatorMemoryClass,
    pub estimated_memory_bytes: u64,
    pub estimated_rows: Option<u64>,
    pub source_ref: String,
    pub split_ref: String,
    pub can_reorder: bool,
    pub can_split: bool,
    pub can_coalesce: bool,
    pub requires_full_materialization: bool,
    pub downstream_sink_class: String,
}

impl PulseWeaveTaskShape {
    /// Construct a task-shape record with a non-zero memory estimate.
    ///
    /// # Errors
    /// Returns an error when required identity fields are empty.
    pub fn new(
        task_id: impl Into<String>,
        task_label: impl Into<String>,
        operator_memory_class: OperatorMemoryClass,
        estimated_memory_bytes: u64,
    ) -> Result<Self> {
        let task_id = non_empty(task_id.into(), "pulseweave task_id")?;
        let task_label = non_empty(task_label.into(), "pulseweave task_label")?;
        Ok(Self {
            task_id,
            task_label,
            operator_memory_class,
            estimated_memory_bytes: estimated_memory_bytes.max(1),
            estimated_rows: None,
            source_ref: "unknown".to_string(),
            split_ref: "unknown".to_string(),
            can_reorder: false,
            can_split: false,
            can_coalesce: false,
            requires_full_materialization: false,
            downstream_sink_class: "none".to_string(),
        })
    }

    #[must_use]
    pub fn with_estimated_rows(mut self, estimated_rows: u64) -> Self {
        self.estimated_rows = Some(estimated_rows);
        self
    }

    #[must_use]
    pub fn with_refs(
        mut self,
        source_ref: impl Into<String>,
        split_ref: impl Into<String>,
    ) -> Self {
        self.source_ref = source_ref.into();
        self.split_ref = split_ref.into();
        self
    }

    #[must_use]
    pub const fn with_shape_permissions(
        mut self,
        can_reorder: bool,
        can_split: bool,
        can_coalesce: bool,
    ) -> Self {
        self.can_reorder = can_reorder;
        self.can_split = can_split;
        self.can_coalesce = can_coalesce;
        self
    }

    #[must_use]
    pub fn with_materialization_and_sink(
        mut self,
        requires_full_materialization: bool,
        downstream_sink_class: impl Into<String>,
    ) -> Self {
        self.requires_full_materialization = requires_full_materialization;
        self.downstream_sink_class = downstream_sink_class.into();
        self
    }
}

/// Provider-neutral `PulseWeave` policy input.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct PulseWeaveInput {
    pub route: String,
    pub application_scope: String,
    pub workload_id: String,
    pub task_shapes: Vec<PulseWeaveTaskShape>,
    pub memory_budget_bytes: u64,
    pub max_parallelism: usize,
    pub target_task_bytes: u64,
    pub min_task_bytes: u64,
    pub max_task_bytes: u64,
    pub materialization_boundary_explicit: bool,
    pub decode_boundary_explicit: bool,
    pub result_sink_requested: bool,
    pub result_sink_replay_verified: bool,
    pub correctness_digest: Option<String>,
    pub output_digest: Option<String>,
    pub execution_certificate_id: Option<String>,
    pub execution_certificate_status: Option<String>,
    pub native_io_certificate_status: Option<String>,
    pub memory_reservations_requested: usize,
    pub memory_reservations_granted: usize,
    pub memory_reservations_denied: usize,
    pub peak_memory_bytes: u64,
    pub spill_required: bool,
    pub spill_supported: bool,
    pub fallback_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl PulseWeaveInput {
    /// Create a scoped prepared/local `PulseWeave` input.
    ///
    /// # Errors
    /// Returns an error when route/scope/workload labels are empty.
    pub fn new(
        route: impl Into<String>,
        application_scope: impl Into<String>,
        workload_id: impl Into<String>,
        task_shapes: Vec<PulseWeaveTaskShape>,
        memory_budget_bytes: u64,
        max_parallelism: usize,
        target_task_bytes: u64,
    ) -> Result<Self> {
        Ok(Self {
            route: non_empty(route.into(), "pulseweave route")?,
            application_scope: non_empty(application_scope.into(), "pulseweave application_scope")?,
            workload_id: non_empty(workload_id.into(), "pulseweave workload_id")?,
            task_shapes,
            memory_budget_bytes,
            max_parallelism,
            target_task_bytes: target_task_bytes.max(1),
            min_task_bytes: target_task_bytes.max(1).saturating_div(8).max(1),
            max_task_bytes: target_task_bytes.max(1).saturating_mul(2),
            materialization_boundary_explicit: false,
            decode_boundary_explicit: false,
            result_sink_requested: false,
            result_sink_replay_verified: false,
            correctness_digest: None,
            output_digest: None,
            execution_certificate_id: None,
            execution_certificate_status: None,
            native_io_certificate_status: None,
            memory_reservations_requested: 0,
            memory_reservations_granted: 0,
            memory_reservations_denied: 0,
            peak_memory_bytes: 0,
            spill_required: false,
            spill_supported: false,
            fallback_allowed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        })
    }

    #[must_use]
    pub fn with_task_byte_limits(mut self, min_task_bytes: u64, max_task_bytes: u64) -> Self {
        self.min_task_bytes = min_task_bytes.max(1);
        self.max_task_bytes = max_task_bytes.max(self.min_task_bytes);
        self
    }

    #[must_use]
    pub const fn with_boundaries(
        mut self,
        materialization_boundary_explicit: bool,
        decode_boundary_explicit: bool,
    ) -> Self {
        self.materialization_boundary_explicit = materialization_boundary_explicit;
        self.decode_boundary_explicit = decode_boundary_explicit;
        self
    }

    #[must_use]
    pub const fn with_result_sink(
        mut self,
        result_sink_requested: bool,
        result_sink_replay_verified: bool,
    ) -> Self {
        self.result_sink_requested = result_sink_requested;
        self.result_sink_replay_verified = result_sink_replay_verified;
        self
    }

    #[must_use]
    pub fn with_correctness_and_output(
        mut self,
        correctness_digest: impl Into<String>,
        output_digest: impl Into<String>,
    ) -> Self {
        self.correctness_digest = Some(correctness_digest.into());
        self.output_digest = Some(output_digest.into());
        self
    }

    #[must_use]
    pub fn with_execution_certificate(
        mut self,
        certificate_id: impl Into<String>,
        certificate_status: impl Into<String>,
    ) -> Self {
        self.execution_certificate_id = Some(certificate_id.into());
        self.execution_certificate_status = Some(certificate_status.into());
        self
    }

    #[must_use]
    pub fn with_native_io_certificate_status(mut self, status: impl Into<String>) -> Self {
        self.native_io_certificate_status = Some(status.into());
        self
    }

    #[must_use]
    pub const fn with_memory_observations(
        mut self,
        requested: usize,
        granted: usize,
        denied: usize,
        peak_memory_bytes: u64,
    ) -> Self {
        self.memory_reservations_requested = requested;
        self.memory_reservations_granted = granted;
        self.memory_reservations_denied = denied;
        self.peak_memory_bytes = peak_memory_bytes;
        self
    }

    #[must_use]
    pub const fn with_spill(mut self, spill_required: bool, spill_supported: bool) -> Self {
        self.spill_required = spill_required;
        self.spill_supported = spill_supported;
        self
    }

    #[must_use]
    pub const fn with_no_fallback_policy(
        mut self,
        fallback_allowed: bool,
        fallback_attempted: bool,
        external_engine_invoked: bool,
    ) -> Self {
        self.fallback_allowed = fallback_allowed;
        self.fallback_attempted = fallback_attempted;
        self.external_engine_invoked = external_engine_invoked;
        self
    }

    #[must_use]
    pub fn task_count(&self) -> usize {
        self.task_shapes.len()
    }

    #[must_use]
    fn max_task_estimate_bytes(&self) -> u64 {
        self.task_shapes
            .iter()
            .map(|task| task.estimated_memory_bytes)
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    fn average_task_estimate_bytes(&self) -> u64 {
        if self.task_shapes.is_empty() {
            return 0;
        }
        self.task_shapes
            .iter()
            .map(|task| task.estimated_memory_bytes)
            .sum::<u64>()
            .saturating_div(u64::try_from(self.task_shapes.len()).unwrap_or(u64::MAX))
    }

    #[must_use]
    fn task_estimates_present(&self) -> bool {
        !self.task_shapes.is_empty()
            && self
                .task_shapes
                .iter()
                .all(|task| task.estimated_memory_bytes > 0)
    }
}

/// `FlowInventory` bounded work-in-progress plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowInventoryReport {
    pub schema_version: &'static str,
    pub wip_limit: usize,
    pub peak_in_flight: usize,
    pub ready_task_count: usize,
    pub held_for_memory_count: usize,
    pub held_for_downstream_count: usize,
    pub completed_task_count: usize,
    pub failed_task_count: usize,
    pub backpressure_event_count: usize,
    pub existing_scheduler_preserved: bool,
}

impl FlowInventoryReport {
    #[must_use]
    pub fn fields(&self) -> [(&'static str, String); 10] {
        [
            (
                "flow_inventory_schema_version",
                String::from(FLOW_INVENTORY_SCHEMA_VERSION),
            ),
            ("flow_inventory_wip_limit", self.wip_limit.to_string()),
            (
                "flow_inventory_peak_in_flight",
                self.peak_in_flight.to_string(),
            ),
            (
                "flow_inventory_ready_task_count",
                self.ready_task_count.to_string(),
            ),
            (
                "flow_inventory_held_for_memory_count",
                self.held_for_memory_count.to_string(),
            ),
            (
                "flow_inventory_held_for_downstream_count",
                self.held_for_downstream_count.to_string(),
            ),
            (
                "flow_inventory_completed_task_count",
                self.completed_task_count.to_string(),
            ),
            (
                "flow_inventory_failed_task_count",
                self.failed_task_count.to_string(),
            ),
            (
                "flow_inventory_backpressure_event_count",
                self.backpressure_event_count.to_string(),
            ),
            (
                "flow_inventory_existing_scheduler_preserved",
                self.existing_scheduler_preserved.to_string(),
            ),
        ]
    }
}

/// `ScarcityLedger` deterministic action vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScarcityLedgerAction {
    KeepCurrentShape,
    SplitLargeTask,
    CoalesceSmallTasks,
    HoldForMemory,
    HoldForDownstream,
    FailBeforeOom,
    BlockedByMissingEstimate,
    BlockedByUnsupportedSpill,
    BlockedByPolicy,
}

impl ScarcityLedgerAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KeepCurrentShape => "keep_current_shape",
            Self::SplitLargeTask => "split_large_task",
            Self::CoalesceSmallTasks => "coalesce_small_tasks",
            Self::HoldForMemory => "hold_for_memory",
            Self::HoldForDownstream => "hold_for_downstream",
            Self::FailBeforeOom => "fail_before_oom",
            Self::BlockedByMissingEstimate => "blocked_by_missing_estimate",
            Self::BlockedByUnsupportedSpill => "blocked_by_unsupported_spill",
            Self::BlockedByPolicy => "blocked_by_policy",
        }
    }

    #[must_use]
    pub const fn is_blocked(self) -> bool {
        matches!(
            self,
            Self::BlockedByMissingEstimate
                | Self::BlockedByUnsupportedSpill
                | Self::BlockedByPolicy
        )
    }
}

/// `ScarcityLedger` deterministic resource-price decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScarcityLedgerDecision {
    pub schema_version: &'static str,
    pub memory_price_bps: u16,
    pub queue_price_bps: u16,
    pub decode_price_bps: u16,
    pub sink_price_bps: u16,
    pub spill_price_bps: u16,
    pub total_price_bps: u16,
    pub selected_action: ScarcityLedgerAction,
    pub decision_reason: String,
    pub decision_digest: String,
}

impl ScarcityLedgerDecision {
    #[must_use]
    pub fn fields(&self) -> [(&'static str, String); 10] {
        [
            (
                "scarcity_ledger_schema_version",
                SCARCITY_LEDGER_SCHEMA_VERSION.to_string(),
            ),
            (
                "scarcity_ledger_memory_price_bps",
                self.memory_price_bps.to_string(),
            ),
            (
                "scarcity_ledger_queue_price_bps",
                self.queue_price_bps.to_string(),
            ),
            (
                "scarcity_ledger_decode_price_bps",
                self.decode_price_bps.to_string(),
            ),
            (
                "scarcity_ledger_sink_price_bps",
                self.sink_price_bps.to_string(),
            ),
            (
                "scarcity_ledger_spill_price_bps",
                self.spill_price_bps.to_string(),
            ),
            (
                "scarcity_ledger_total_price_bps",
                self.total_price_bps.to_string(),
            ),
            (
                "scarcity_ledger_selected_action",
                self.selected_action.as_str().to_string(),
            ),
            (
                "scarcity_ledger_decision_reason",
                self.decision_reason.clone(),
            ),
            (
                "scarcity_ledger_decision_digest",
                self.decision_digest.clone(),
            ),
        ]
    }
}

/// `EndoPulse` one-window run-local feedback decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndoPulseDecision {
    pub schema_version: &'static str,
    pub signal_set: String,
    pub previous_target_task_bytes: u64,
    pub next_target_task_bytes: u64,
    pub previous_wip_limit: usize,
    pub next_wip_limit: usize,
    pub adjustment_applied: bool,
    pub hysteresis_state: String,
    pub persistent_state_used: bool,
}

impl EndoPulseDecision {
    #[must_use]
    pub fn fields(&self) -> [(&'static str, String); 9] {
        [
            (
                "endopulse_schema_version",
                ENDOPULSE_SCHEMA_VERSION.to_string(),
            ),
            ("endopulse_signal_set", self.signal_set.clone()),
            (
                "endopulse_previous_target_task_bytes",
                self.previous_target_task_bytes.to_string(),
            ),
            (
                "endopulse_next_target_task_bytes",
                self.next_target_task_bytes.to_string(),
            ),
            (
                "endopulse_previous_wip_limit",
                self.previous_wip_limit.to_string(),
            ),
            ("endopulse_next_wip_limit", self.next_wip_limit.to_string()),
            (
                "endopulse_adjustment_applied",
                self.adjustment_applied.to_string(),
            ),
            ("endopulse_hysteresis_state", self.hysteresis_state.clone()),
            (
                "endopulse_persistent_state_used",
                self.persistent_state_used.to_string(),
            ),
        ]
    }
}

/// `ProofBound` automatic application gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofBoundAutoGate {
    pub schema_version: &'static str,
    pub pre_application_status: String,
    pub post_application_status: String,
    pub required_evidence: String,
    pub missing_evidence: String,
    pub certificate_status: String,
    pub no_fallback_status: String,
    pub claim_allowed: bool,
}

impl ProofBoundAutoGate {
    #[must_use]
    pub fn fields(&self) -> [(&'static str, String); 8] {
        [
            (
                "proofbound_schema_version",
                PROOFBOUND_SCHEMA_VERSION.to_string(),
            ),
            (
                "proofbound_pre_application_status",
                self.pre_application_status.clone(),
            ),
            (
                "proofbound_post_application_status",
                self.post_application_status.clone(),
            ),
            (
                "proofbound_required_evidence",
                self.required_evidence.clone(),
            ),
            ("proofbound_missing_evidence", self.missing_evidence.clone()),
            (
                "proofbound_certificate_status",
                self.certificate_status.clone(),
            ),
            (
                "proofbound_no_fallback_status",
                self.no_fallback_status.clone(),
            ),
            ("proofbound_claim_allowed", self.claim_allowed.to_string()),
        ]
    }
}

/// Aggregate `PulseWeave` policy and evidence report.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct PulseWeaveReport {
    pub schema_version: &'static str,
    pub status: String,
    pub application_scope: String,
    pub runtime_decision_applied: bool,
    pub policy_mutated: bool,
    pub decision_digest: String,
    pub blocker: String,
    pub claim_gate_status: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub flow_inventory: FlowInventoryReport,
    pub scarcity_ledger: ScarcityLedgerDecision,
    pub endopulse: EndoPulseDecision,
    pub proofbound: ProofBoundAutoGate,
}

impl PulseWeaveReport {
    #[must_use]
    pub fn batch_window_size(&self, fallback_max_parallelism: usize) -> usize {
        if self.runtime_decision_applied {
            self.endopulse.next_wip_limit.max(1)
        } else {
            fallback_max_parallelism.max(1)
        }
    }

    #[must_use]
    pub fn fields(&self) -> Vec<(String, String)> {
        let mut fields = vec![
            (
                "pulseweave_schema_version".to_string(),
                self.schema_version.to_string(),
            ),
            ("pulseweave_status".to_string(), self.status.clone()),
            (
                "pulseweave_application_scope".to_string(),
                self.application_scope.clone(),
            ),
            (
                "pulseweave_runtime_decision_applied".to_string(),
                self.runtime_decision_applied.to_string(),
            ),
            (
                "pulseweave_policy_mutated".to_string(),
                self.policy_mutated.to_string(),
            ),
            (
                "pulseweave_decision_digest".to_string(),
                self.decision_digest.clone(),
            ),
            ("pulseweave_blocker".to_string(), self.blocker.clone()),
            (
                "pulseweave_claim_gate_status".to_string(),
                self.claim_gate_status.clone(),
            ),
            (
                "pulseweave_fallback_attempted".to_string(),
                self.fallback_attempted.to_string(),
            ),
            (
                "pulseweave_external_engine_invoked".to_string(),
                self.external_engine_invoked.to_string(),
            ),
        ];
        fields.extend(
            self.flow_inventory
                .fields()
                .into_iter()
                .map(|(key, value)| (key.to_string(), value)),
        );
        fields.extend(
            self.scarcity_ledger
                .fields()
                .into_iter()
                .map(|(key, value)| (key.to_string(), value)),
        );
        fields.extend(
            self.endopulse
                .fields()
                .into_iter()
                .map(|(key, value)| (key.to_string(), value)),
        );
        fields.extend(
            self.proofbound
                .fields()
                .into_iter()
                .map(|(key, value)| (key.to_string(), value)),
        );
        fields
    }
}

/// Plan `FlowInventory` bounded WIP from local task evidence.
///
/// # Errors
/// Returns an error for structurally invalid input such as zero memory budget
/// or zero max parallelism.
pub fn plan_flow_inventory(input: &PulseWeaveInput) -> Result<FlowInventoryReport> {
    validate_non_empty_tasks(input)?;
    if input.memory_budget_bytes == 0 {
        return Err(invalid_operation(
            "PulseWeave FlowInventory requires a non-zero memory budget",
        ));
    }
    if input.max_parallelism == 0 {
        return Err(invalid_operation(
            "PulseWeave FlowInventory requires max_parallelism greater than zero",
        ));
    }

    let task_count = input.task_count();
    let max_task_estimate = input.max_task_estimate_bytes().max(1);
    let memory_safe_inflight = input
        .memory_budget_bytes
        .saturating_div(max_task_estimate)
        .max(1);
    let memory_safe_inflight = usize::try_from(memory_safe_inflight).unwrap_or(usize::MAX);
    let sink_limit = if input.result_sink_requested {
        input.max_parallelism.saturating_div(2).max(1)
    } else {
        input.max_parallelism
    };
    let wip_limit = input
        .max_parallelism
        .min(memory_safe_inflight)
        .min(sink_limit)
        .min(task_count.max(1))
        .max(1);
    let held_for_memory_count = input
        .task_shapes
        .iter()
        .filter(|task| task.estimated_memory_bytes > input.memory_budget_bytes)
        .count()
        .saturating_add(input.memory_reservations_denied);
    let held_for_downstream_count = if input.result_sink_requested && task_count > wip_limit {
        task_count - wip_limit
    } else {
        0
    };
    let failed_task_count = input.memory_reservations_denied;
    let completed_task_count = task_count.saturating_sub(failed_task_count);
    let backpressure_event_count =
        usize::from(held_for_memory_count > 0) + usize::from(held_for_downstream_count > 0);
    let peak_in_flight = wip_limit.min(task_count);
    let existing_scheduler_preserved =
        wip_limit == input.max_parallelism.min(task_count.max(1)) && backpressure_event_count == 0;

    Ok(FlowInventoryReport {
        schema_version: FLOW_INVENTORY_SCHEMA_VERSION,
        wip_limit,
        peak_in_flight,
        ready_task_count: task_count,
        held_for_memory_count,
        held_for_downstream_count,
        completed_task_count,
        failed_task_count,
        backpressure_event_count,
        existing_scheduler_preserved,
    })
}

/// Compute deterministic `ScarcityLedger` prices and action.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn compute_scarcity_ledger(
    input: &PulseWeaveInput,
    flow: &FlowInventoryReport,
) -> ScarcityLedgerDecision {
    let memory_pressure = memory_pressure_for(input.memory_budget_bytes, input.peak_memory_bytes);
    let memory_price_bps = if input.memory_reservations_denied > 0 {
        10_000
    } else {
        memory_pressure_price_bps(memory_pressure)
    };
    let queue_price_bps = if flow.wip_limit == 0 {
        10_000
    } else if flow.held_for_downstream_count == 0 {
        0
    } else {
        let numerator = u32::try_from(flow.held_for_downstream_count).unwrap_or(u32::MAX);
        let denominator = u32::try_from(flow.ready_task_count)
            .unwrap_or(u32::MAX)
            .max(1);
        u16::try_from(numerator.saturating_mul(10_000) / denominator).unwrap_or(10_000)
    };
    let decode_price_bps = if input
        .task_shapes
        .iter()
        .any(|task| task.requires_full_materialization)
    {
        2_500
    } else {
        0
    };
    let sink_price_bps = if input.result_sink_requested {
        2_500
    } else {
        0
    };
    let spill_price_bps = if input.spill_required && !input.spill_supported {
        9_000
    } else {
        0
    };
    let total_price_bps = memory_price_bps
        .saturating_add(queue_price_bps)
        .saturating_add(decode_price_bps)
        .saturating_add(sink_price_bps)
        .saturating_add(spill_price_bps)
        .min(10_000);

    let (selected_action, decision_reason) =
        if input.fallback_allowed || input.fallback_attempted || input.external_engine_invoked {
            (
                ScarcityLedgerAction::BlockedByPolicy,
                "fallback or external-engine policy violation blocks PulseWeave application",
            )
        } else if !input.task_estimates_present() {
            (
                ScarcityLedgerAction::BlockedByMissingEstimate,
                "task estimates are required before automatic local work shaping",
            )
        } else if input.spill_required && !input.spill_supported {
            (
                ScarcityLedgerAction::BlockedByUnsupportedSpill,
                "real query-data spill is required but unsupported in this runtime slice",
            )
        } else if input.memory_reservations_denied > 0
            || matches!(memory_pressure, MemoryPressureLevel::Exhausted)
        {
            (
                ScarcityLedgerAction::FailBeforeOom,
                "memory reservation denial requires fail-before-OOM posture",
            )
        } else if matches!(
            memory_pressure,
            MemoryPressureLevel::High | MemoryPressureLevel::Critical
        ) || flow.held_for_memory_count > 0
        {
            (
                ScarcityLedgerAction::HoldForMemory,
                "memory pressure limits additional in-flight work",
            )
        } else if flow.held_for_downstream_count > 0 {
            (
                ScarcityLedgerAction::HoldForDownstream,
                "downstream result-sink pressure limits in-flight work",
            )
        } else if input.average_task_estimate_bytes() > input.max_task_bytes {
            (
                ScarcityLedgerAction::SplitLargeTask,
                "task estimates exceed the configured maximum task bytes",
            )
        } else if input.task_count() > 1
            && input.average_task_estimate_bytes() < input.min_task_bytes
            && input.task_shapes.iter().all(|task| task.can_coalesce)
        {
            (
                ScarcityLedgerAction::CoalesceSmallTasks,
                "small independent tasks can be coalesced inside the local prepared route",
            )
        } else {
            (
                ScarcityLedgerAction::KeepCurrentShape,
                "current local task shape fits budget and evidence gates",
            )
        };

    let decision_digest = pulseweave_digest(&[
        "scarcity_ledger",
        &input.route,
        &input.application_scope,
        &input.workload_id,
        selected_action.as_str(),
        &memory_price_bps.to_string(),
        &queue_price_bps.to_string(),
        &decode_price_bps.to_string(),
        &sink_price_bps.to_string(),
        &spill_price_bps.to_string(),
        &flow.wip_limit.to_string(),
    ]);

    ScarcityLedgerDecision {
        schema_version: SCARCITY_LEDGER_SCHEMA_VERSION,
        memory_price_bps,
        queue_price_bps,
        decode_price_bps,
        sink_price_bps,
        spill_price_bps,
        total_price_bps,
        selected_action,
        decision_reason: decision_reason.to_string(),
        decision_digest,
    }
}

/// Plan one run-local `EndoPulse` adjustment.
#[must_use]
pub fn plan_endopulse_adjustment(
    input: &PulseWeaveInput,
    flow: &FlowInventoryReport,
    ledger: &ScarcityLedgerDecision,
) -> EndoPulseDecision {
    let mut next_target_task_bytes = input.target_task_bytes.max(1);
    let mut next_wip_limit = flow.wip_limit.max(1);
    let mut signals = Vec::new();

    if input.memory_reservations_denied > 0 {
        signals.push("memory_reservation_denied");
        next_target_task_bytes = next_target_task_bytes
            .saturating_div(2)
            .max(input.min_task_bytes);
    }
    if matches!(ledger.selected_action, ScarcityLedgerAction::HoldForMemory) {
        signals.push("memory_pressure");
        next_target_task_bytes = next_target_task_bytes
            .saturating_div(2)
            .max(input.min_task_bytes);
    }
    if matches!(
        ledger.selected_action,
        ScarcityLedgerAction::HoldForDownstream
    ) {
        signals.push("sink_pressure");
        next_wip_limit = next_wip_limit.saturating_sub(1).max(1);
    }
    if matches!(
        ledger.selected_action,
        ScarcityLedgerAction::CoalesceSmallTasks
    ) {
        signals.push("small_task_coalescing");
        next_target_task_bytes = next_target_task_bytes
            .saturating_mul(2)
            .min(input.max_task_bytes.max(input.target_task_bytes));
    }
    if matches!(
        ledger.selected_action,
        ScarcityLedgerAction::BlockedByUnsupportedSpill
    ) {
        signals.push("unsupported_spill_blocker");
    }
    if signals.is_empty() {
        signals.push("stable");
    }

    let adjustment_applied =
        next_target_task_bytes != input.target_task_bytes || next_wip_limit != flow.wip_limit;
    EndoPulseDecision {
        schema_version: ENDOPULSE_SCHEMA_VERSION,
        signal_set: signals.join(","),
        previous_target_task_bytes: input.target_task_bytes,
        next_target_task_bytes,
        previous_wip_limit: flow.wip_limit,
        next_wip_limit,
        adjustment_applied,
        hysteresis_state: "one_window_local_only".to_string(),
        persistent_state_used: false,
    }
}

/// Evaluate `ProofBound` automatic application requirements.
#[must_use]
pub fn evaluate_proofbound_auto_gate(input: &PulseWeaveInput) -> ProofBoundAutoGate {
    let mut missing = Vec::new();
    let pre_application_status = if !is_prepared_local_scope(input) {
        missing.push("prepared_local_route");
        "blocked_route_out_of_scope"
    } else if input.fallback_allowed || input.fallback_attempted || input.external_engine_invoked {
        missing.push("no_fallback");
        "blocked_fallback_policy"
    } else if input.memory_budget_bytes == 0 {
        missing.push("memory_budget");
        "blocked_missing_memory_budget"
    } else if input.max_parallelism == 0 {
        missing.push("max_parallelism");
        "blocked_missing_max_parallelism"
    } else if !input.task_estimates_present() {
        missing.push("task_estimates");
        "blocked_missing_task_estimate"
    } else if input.spill_required && !input.spill_supported {
        missing.push("spill_support");
        "blocked_unsupported_spill"
    } else if !input.materialization_boundary_explicit || !input.decode_boundary_explicit {
        missing.push("materialization_decode_boundary");
        "blocked_missing_materialization_decode_boundary"
    } else {
        "admitted"
    };

    let certificate_status = input
        .execution_certificate_status
        .as_deref()
        .unwrap_or("missing")
        .to_string();
    let no_fallback_status =
        if !input.fallback_allowed && !input.fallback_attempted && !input.external_engine_invoked {
            "verified"
        } else {
            "violated"
        }
        .to_string();

    let post_application_status = if pre_application_status != "admitted" {
        "not_evaluated"
    } else if input
        .correctness_digest
        .as_deref()
        .is_none_or(str::is_empty)
    {
        missing.push("correctness_digest");
        "blocked_missing_correctness_digest"
    } else if input.output_digest.as_deref().is_none_or(str::is_empty) {
        missing.push("output_digest");
        "blocked_missing_output_digest"
    } else if input
        .execution_certificate_id
        .as_deref()
        .is_none_or(str::is_empty)
        || certificate_status != "certified"
    {
        missing.push("execution_certificate");
        "blocked_missing_execution_certificate"
    } else if input
        .native_io_certificate_status
        .as_deref()
        .is_none_or(|status| status != "certified")
    {
        missing.push("native_io_certificate");
        "blocked_missing_native_io_certificate"
    } else if input.result_sink_requested && !input.result_sink_replay_verified {
        missing.push("result_sink_replay");
        "blocked_missing_result_sink_replay"
    } else {
        "certified"
    };

    let claim_allowed = pre_application_status == "admitted"
        && post_application_status == "certified"
        && no_fallback_status == "verified";

    ProofBoundAutoGate {
        schema_version: PROOFBOUND_SCHEMA_VERSION,
        pre_application_status: pre_application_status.to_string(),
        post_application_status: post_application_status.to_string(),
        required_evidence: REQUIRED_PROOFBOUND_EVIDENCE.to_string(),
        missing_evidence: if missing.is_empty() {
            "none".to_string()
        } else {
            missing.join(",")
        },
        certificate_status,
        no_fallback_status,
        claim_allowed,
    }
}

/// Plan the aggregate `PulseWeave` report.
///
/// # Errors
/// Returns an error when the input is structurally invalid.
pub fn plan_pulseweave(input: PulseWeaveInput) -> Result<PulseWeaveReport> {
    let flow_inventory = plan_flow_inventory(&input)?;
    let scarcity_ledger = compute_scarcity_ledger(&input, &flow_inventory);
    let endopulse = plan_endopulse_adjustment(&input, &flow_inventory, &scarcity_ledger);
    let proofbound = evaluate_proofbound_auto_gate(&input);

    let application_blocked =
        !proofbound.claim_allowed || scarcity_ledger.selected_action.is_blocked();
    let runtime_decision_applied = !application_blocked;
    let policy_mutated = runtime_decision_applied
        && (endopulse.adjustment_applied
            || flow_inventory.wip_limit != input.max_parallelism.min(input.task_count().max(1)));
    let status = if runtime_decision_applied {
        "applied"
    } else if proofbound.pre_application_status == "admitted" {
        "blocked"
    } else {
        "report_only_blocked"
    };
    let blocker_id = if runtime_decision_applied {
        "none".to_string()
    } else if proofbound.missing_evidence != "none" {
        proofbound.missing_evidence.clone()
    } else {
        scarcity_ledger.selected_action.as_str().to_string()
    };
    let decision_digest = pulseweave_digest(&[
        "pulseweave",
        &input.route,
        &input.application_scope,
        &input.workload_id,
        &flow_inventory.wip_limit.to_string(),
        scarcity_ledger.selected_action.as_str(),
        &scarcity_ledger.decision_digest,
        &endopulse.next_target_task_bytes.to_string(),
        &endopulse.next_wip_limit.to_string(),
        &proofbound.pre_application_status,
        &proofbound.post_application_status,
    ]);

    Ok(PulseWeaveReport {
        schema_version: PULSEWEAVE_SCHEMA_VERSION,
        status: status.to_string(),
        application_scope: input.application_scope,
        runtime_decision_applied,
        policy_mutated,
        decision_digest,
        blocker: blocker_id,
        claim_gate_status: if runtime_decision_applied {
            "pulseweave_runtime_certified"
        } else {
            "not_pulseweave_claim_grade"
        }
        .to_string(),
        fallback_attempted: input.fallback_attempted,
        external_engine_invoked: input.external_engine_invoked,
        flow_inventory,
        scarcity_ledger,
        endopulse,
        proofbound,
    })
}

fn validate_non_empty_tasks(input: &PulseWeaveInput) -> Result<()> {
    if input.task_shapes.is_empty() {
        return Err(invalid_operation(
            "PulseWeave requires at least one local task shape",
        ));
    }
    Ok(())
}

fn non_empty(value: String, field: &str) -> Result<String> {
    if value.trim().is_empty() {
        return Err(invalid_operation(format!("{field} must not be empty")));
    }
    Ok(value)
}

fn invalid_operation(message: impl Into<String>) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{}; fallback execution was not attempted",
        message.into()
    ))
}

fn is_prepared_local_scope(input: &PulseWeaveInput) -> bool {
    matches!(
        input.route.as_str(),
        "compatibility_import_certified_to_prepared_vortex_batch"
            | "prepared_vortex"
            | "native_vortex"
    ) && input.application_scope.contains("prepared_vortex_local")
}

fn memory_pressure_for(memory_budget_bytes: u64, peak_memory_bytes: u64) -> MemoryPressureLevel {
    let Ok(budget) = MemoryBudget::new(ByteSize::from_bytes(memory_budget_bytes.max(1))) else {
        return MemoryPressureLevel::Exhausted;
    };
    budget.pressure_for_reserved(ByteSize::from_bytes(peak_memory_bytes))
}

const fn memory_pressure_price_bps(pressure: MemoryPressureLevel) -> u16 {
    match pressure {
        MemoryPressureLevel::Normal => 0,
        MemoryPressureLevel::Elevated => 2_500,
        MemoryPressureLevel::High => 6_000,
        MemoryPressureLevel::Critical => 9_000,
        MemoryPressureLevel::Exhausted => 10_000,
    }
}

fn pulseweave_digest(parts: &[&str]) -> String {
    let mut digest = Fnv1a64::new();
    for part in parts {
        digest.update(part.as_bytes());
        digest.update(b"\0");
    }
    format!("fnv1a64:{:016x}", digest.finish())
}

struct Fnv1a64 {
    state: u64,
}

impl Fnv1a64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    const fn new() -> Self {
        Self {
            state: Self::OFFSET,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(Self::PRIME);
        }
    }

    const fn finish(&self) -> u64 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str, class: OperatorMemoryClass, bytes: u64) -> PulseWeaveTaskShape {
        PulseWeaveTaskShape::new(id, id, class, bytes)
            .expect("task")
            .with_refs("source://fixture", format!("split://{id}"))
            .with_shape_permissions(true, true, true)
    }

    fn admitted_input() -> PulseWeaveInput {
        PulseWeaveInput::new(
            "compatibility_import_certified_to_prepared_vortex_batch",
            "prepared_vortex_local_batch",
            "workload://fixture",
            vec![
                task("scan-fact", OperatorMemoryClass::Scan, 64 * 1024),
                task("compute", OperatorMemoryClass::Aggregate, 64 * 1024),
                task("sink", OperatorMemoryClass::Sink, 64 * 1024)
                    .with_materialization_and_sink(false, "vortex_result_sink"),
            ],
            1024 * 1024 * 1024,
            4,
            64 * 1024 * 1024,
        )
        .expect("input")
        .with_task_byte_limits(8 * 1024 * 1024, 128 * 1024 * 1024)
        .with_boundaries(true, true)
        .with_result_sink(true, true)
        .with_correctness_and_output("fnv1a64:correct", "fnv1a64:output")
        .with_execution_certificate("execution.pulseweave.fixture", "certified")
        .with_native_io_certificate_status("certified")
        .with_memory_observations(3, 3, 0, 192 * 1024)
        .with_no_fallback_policy(false, false, false)
    }

    #[test]
    fn flow_inventory_caps_wip_by_sink_pressure() {
        let input = admitted_input();
        let flow = plan_flow_inventory(&input).expect("flow");

        assert_eq!(flow.wip_limit, 2);
        assert_eq!(flow.peak_in_flight, 2);
        assert_eq!(flow.held_for_downstream_count, 1);
        assert_eq!(flow.failed_task_count, 0);
        assert!(!flow.existing_scheduler_preserved);
    }

    #[test]
    fn scarcity_ledger_selects_downstream_hold_for_sink_pressure() {
        let input = admitted_input();
        let flow = plan_flow_inventory(&input).expect("flow");
        let ledger = compute_scarcity_ledger(&input, &flow);

        assert_eq!(
            ledger.selected_action,
            ScarcityLedgerAction::HoldForDownstream
        );
        assert_eq!(ledger.queue_price_bps, 3_333);
        assert_eq!(ledger.sink_price_bps, 2_500);
        assert!(ledger.decision_digest.starts_with("fnv1a64:"));
    }

    #[test]
    fn scarcity_ledger_does_not_charge_queue_price_without_downstream_backlog() {
        let input = admitted_input().with_result_sink(false, false);
        let flow = plan_flow_inventory(&input).expect("flow");
        let ledger = compute_scarcity_ledger(&input, &flow);

        assert_eq!(flow.held_for_downstream_count, 0);
        assert_eq!(ledger.queue_price_bps, 0);
    }

    #[test]
    fn endopulse_reduces_wip_for_sink_pressure_without_persistence() {
        let input = admitted_input();
        let flow = plan_flow_inventory(&input).expect("flow");
        let ledger = compute_scarcity_ledger(&input, &flow);
        let endopulse = plan_endopulse_adjustment(&input, &flow, &ledger);

        assert_eq!(endopulse.previous_wip_limit, 2);
        assert_eq!(endopulse.next_wip_limit, 1);
        assert!(endopulse.adjustment_applied);
        assert!(!endopulse.persistent_state_used);
    }

    #[test]
    fn proofbound_admits_certified_prepared_local_route() {
        let input = admitted_input();
        let gate = evaluate_proofbound_auto_gate(&input);

        assert_eq!(gate.pre_application_status, "admitted");
        assert_eq!(gate.post_application_status, "certified");
        assert_eq!(gate.no_fallback_status, "verified");
        assert!(gate.claim_allowed);
    }

    #[test]
    fn proofbound_blocks_unsupported_spill() {
        let input = admitted_input().with_spill(true, false);
        let gate = evaluate_proofbound_auto_gate(&input);

        assert_eq!(gate.pre_application_status, "blocked_unsupported_spill");
        assert_eq!(gate.post_application_status, "not_evaluated");
        assert!(!gate.claim_allowed);
        assert!(gate.missing_evidence.contains("spill_support"));
    }

    #[test]
    fn pulseweave_report_applies_when_proofbound_certified() {
        let report = plan_pulseweave(admitted_input()).expect("pulseweave report");

        assert_eq!(report.status, "applied");
        assert!(report.runtime_decision_applied);
        assert!(report.policy_mutated);
        assert_eq!(report.batch_window_size(4), 1);
        assert_eq!(report.claim_gate_status, "pulseweave_runtime_certified");
        assert!(report.decision_digest.starts_with("fnv1a64:"));
        let fields = report.fields();
        assert!(fields.iter().any(|(key, value)| {
            key == "pulseweave_runtime_decision_applied" && value == "true"
        }));
        assert!(fields.iter().any(|(key, value)| {
            key == "proofbound_certificate_status" && value == "certified"
        }));
    }

    #[test]
    fn pulseweave_blocks_missing_certificate_and_preserves_fallback_window() {
        let input = PulseWeaveInput {
            execution_certificate_id: None,
            execution_certificate_status: None,
            ..admitted_input()
        };
        let report = plan_pulseweave(input).expect("pulseweave report");

        assert_eq!(report.status, "blocked");
        assert!(!report.runtime_decision_applied);
        assert_eq!(report.batch_window_size(4), 4);
        assert!(report.blocker.contains("execution_certificate"));
    }
}
