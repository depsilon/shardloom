#![allow(
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::return_self_not_must_use
)]

use crate::{ByteSize, TaskId};
use shardloom_core::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus, Result,
    ShardLoomError,
};

fn invalid_operation(message: impl Into<String>) -> ShardLoomError {
    ShardLoomError::InvalidOperation(message.into())
}

fn non_empty(value: String, field: &str) -> Result<String> {
    if value.trim().is_empty() {
        return Err(invalid_operation(format!("{field} must not be empty")));
    }
    Ok(value)
}

/// Planning-only memory budget skeleton; no allocation tracking occurs in this PR.
#[derive(Debug, Clone, PartialEq, Eq)]
/// Planning-only global memory budget domain model.
pub struct MemoryBudget {
    pub total: ByteSize,
    pub soft_limit: ByteSize,
    pub hard_limit: ByteSize,
}
impl MemoryBudget {
    pub fn new(total: ByteSize) -> Result<Self> {
        let soft_limit = ByteSize::from_bytes(total.as_bytes().saturating_mul(8) / 10);
        Self::with_limits(total, soft_limit, total)
    }
    pub fn with_limits(
        total: ByteSize,
        soft_limit: ByteSize,
        hard_limit: ByteSize,
    ) -> Result<Self> {
        if total.as_bytes() == 0 {
            return Err(invalid_operation("memory total must be greater than zero"));
        }
        if soft_limit > hard_limit {
            return Err(invalid_operation("soft_limit must be <= hard_limit"));
        }
        if hard_limit > total {
            return Err(invalid_operation("hard_limit must be <= total"));
        }
        Ok(Self {
            total,
            soft_limit,
            hard_limit,
        })
    }
    pub fn from_gib(total_gib: u64) -> Result<Self> {
        Self::new(ByteSize::from_gib(total_gib))
    }
    pub const fn available_after_reserved(&self, reserved: ByteSize) -> ByteSize {
        ByteSize::from_bytes(self.total.as_bytes().saturating_sub(reserved.as_bytes()))
    }
    pub const fn pressure_for_reserved(&self, reserved: ByteSize) -> MemoryPressureLevel {
        let r = reserved.as_bytes();
        if r >= self.total.as_bytes() {
            MemoryPressureLevel::Exhausted
        } else if r >= self.hard_limit.as_bytes() {
            MemoryPressureLevel::Critical
        } else if r >= self.soft_limit.as_bytes() {
            MemoryPressureLevel::High
        } else if r >= (self.soft_limit.as_bytes() / 2) {
            MemoryPressureLevel::Elevated
        } else {
            MemoryPressureLevel::Normal
        }
    }
    pub fn summary(&self) -> String {
        format!(
            "total={}, soft={}, hard={}",
            self.total.to_human_text(),
            self.soft_limit.to_human_text(),
            self.hard_limit.to_human_text()
        )
    }

    /// Returns canonical terminology for memory-budget reporting.
    ///
    /// This helper is a stable label aid and does not alter memory behavior.
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        "memory_budget"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Pressure levels derived from reserved bytes; used for deterministic planning.
pub enum MemoryPressureLevel {
    Normal,
    Elevated,
    High,
    Critical,
    Exhausted,
}
impl MemoryPressureLevel {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Elevated => "elevated",
            Self::High => "high",
            Self::Critical => "critical",
            Self::Exhausted => "exhausted",
        }
    }
    pub const fn requires_action(&self) -> bool {
        matches!(self, Self::High | Self::Critical | Self::Exhausted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorMemoryClass {
    Scan,
    Filter,
    Projection,
    Aggregate,
    Sort,
    Join,
    Window,
    Repartition,
    Shuffle,
    Udf,
    Translation,
    Sink,
    ExternalEffect,
    Unknown,
}
impl OperatorMemoryClass {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Filter => "filter",
            Self::Projection => "projection",
            Self::Aggregate => "aggregate",
            Self::Sort => "sort",
            Self::Join => "join",
            Self::Window => "window",
            Self::Repartition => "repartition",
            Self::Shuffle => "shuffle",
            Self::Udf => "udf",
            Self::Translation => "translation",
            Self::Sink => "sink",
            Self::ExternalEffect => "external_effect",
            Self::Unknown => "unknown",
        }
    }
    pub const fn is_stateful(&self) -> bool {
        matches!(
            self,
            Self::Aggregate
                | Self::Sort
                | Self::Join
                | Self::Window
                | Self::Repartition
                | Self::Shuffle
        )
    }
    pub const fn is_spill_candidate(&self) -> bool {
        matches!(
            self,
            Self::Aggregate
                | Self::Sort
                | Self::Join
                | Self::Window
                | Self::Repartition
                | Self::Shuffle
                | Self::Translation
                | Self::Sink
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwner {
    pub task_id: Option<TaskId>,
    pub operator_class: OperatorMemoryClass,
    pub label: String,
}
impl MemoryOwner {
    pub fn new(operator_class: OperatorMemoryClass, label: impl Into<String>) -> Result<Self> {
        Ok(Self {
            task_id: None,
            operator_class,
            label: non_empty(label.into(), "memory owner label")?,
        })
    }
    pub fn with_task_id(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }
    pub fn summary(&self) -> String {
        format!(
            "class={}, label={}, task={}",
            self.operator_class.as_str(),
            self.label,
            self.task_id.as_ref().map_or("none", |t| t.as_str())
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MemoryReservationId(String);
impl MemoryReservationId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        Ok(Self(non_empty(value.into(), "memory reservation id")?))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryReservationStatus {
    Requested,
    Granted,
    Denied,
    Released,
}
impl MemoryReservationStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Granted => "granted",
            Self::Denied => "denied",
            Self::Released => "released",
        }
    }
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Denied | Self::Released)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryReservation {
    pub id: MemoryReservationId,
    pub owner: MemoryOwner,
    pub requested: ByteSize,
    pub granted: ByteSize,
    pub status: MemoryReservationStatus,
}
impl MemoryReservation {
    pub const fn request(id: MemoryReservationId, owner: MemoryOwner, requested: ByteSize) -> Self {
        Self {
            id,
            owner,
            requested,
            granted: ByteSize::from_bytes(0),
            status: MemoryReservationStatus::Requested,
        }
    }
    pub fn granted(mut self, granted: ByteSize) -> Self {
        self.granted = granted;
        self.status = MemoryReservationStatus::Granted;
        self
    }
    pub fn denied(mut self) -> Self {
        self.granted = ByteSize::from_bytes(0);
        self.status = MemoryReservationStatus::Denied;
        self
    }
    pub fn released(mut self) -> Self {
        self.status = MemoryReservationStatus::Released;
        self
    }
    pub const fn is_granted(&self) -> bool {
        matches!(self.status, MemoryReservationStatus::Granted)
    }
    pub fn summary(&self) -> String {
        format!(
            "id={}, owner=[{}], requested={}, granted={}, status={}",
            self.id.as_str(),
            self.owner.summary(),
            self.requested.to_human_text(),
            self.granted.to_human_text(),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPoolSnapshot {
    pub budget: MemoryBudget,
    pub reserved: ByteSize,
    pub reservation_count: usize,
}
impl MemoryPoolSnapshot {
    pub const fn new(budget: MemoryBudget, reserved: ByteSize, reservation_count: usize) -> Self {
        Self {
            budget,
            reserved,
            reservation_count,
        }
    }
    pub const fn available(&self) -> ByteSize {
        self.budget.available_after_reserved(self.reserved)
    }
    pub const fn pressure(&self) -> MemoryPressureLevel {
        self.budget.pressure_for_reserved(self.reserved)
    }
    pub fn summary(&self) -> String {
        format!(
            "budget=[{}], reserved={}, available={}, reservations={}, pressure={}",
            self.budget.summary(),
            self.reserved.to_human_text(),
            self.available().to_human_text(),
            self.reservation_count,
            self.pressure().as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Planning-only memory pool and reservation summary; not a live allocator.
pub struct MemoryPoolPlan {
    pub snapshot: MemoryPoolSnapshot,
    pub reservations: Vec<MemoryReservation>,
    pub diagnostics: Vec<Diagnostic>,
}
impl MemoryPoolPlan {
    pub fn new(budget: MemoryBudget) -> Self {
        Self {
            snapshot: MemoryPoolSnapshot::new(budget, ByteSize::from_bytes(0), 0),
            reservations: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_reservation(&mut self, reservation: MemoryReservation) {
        self.reservations.push(reservation);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn reserved_bytes(&self) -> ByteSize {
        ByteSize::from_bytes(
            self.reservations
                .iter()
                .filter(|r| r.is_granted())
                .map(|r| r.granted.as_bytes())
                .sum(),
        )
    }
    pub fn recompute_snapshot(&mut self) {
        self.snapshot.reserved = self.reserved_bytes();
        self.snapshot.reservation_count = self.reservations.len();
    }
    pub const fn pressure(&self) -> MemoryPressureLevel {
        self.snapshot.pressure()
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "memory_pool_plan\n{}\nfallback execution: disabled",
            self.snapshot.summary()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Spill behavior policy for planning; no spill IO is performed in this PR.
pub enum SpillPolicy {
    Never,
    BestEffort,
    Required,
    ForceBeforeOom,
    DisabledForOperator,
}
impl SpillPolicy {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::BestEffort => "best_effort",
            Self::Required => "required",
            Self::ForceBeforeOom => "force_before_oom",
            Self::DisabledForOperator => "disabled_for_operator",
        }
    }
    pub const fn allows_spill(&self) -> bool {
        matches!(
            self,
            Self::BestEffort | Self::Required | Self::ForceBeforeOom
        )
    }
    pub const fn requires_spill_support(&self) -> bool {
        matches!(self, Self::Required | Self::ForceBeforeOom)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillFormat {
    VortexNativeSpill,
    ArrowIpcLikeSpill,
    RowBinarySpill,
    KeyValueRunSpill,
    Unknown,
}
impl SpillFormat {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::VortexNativeSpill => "vortex_native_spill",
            Self::ArrowIpcLikeSpill => "arrow_ipc_like_spill",
            Self::RowBinarySpill => "row_binary_spill",
            Self::KeyValueRunSpill => "key_value_run_spill",
            Self::Unknown => "unknown",
        }
    }
    pub const fn is_columnar(&self) -> bool {
        matches!(self, Self::VortexNativeSpill | Self::ArrowIpcLikeSpill)
    }
    pub const fn is_vortex_native(&self) -> bool {
        matches!(self, Self::VortexNativeSpill)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillCompression {
    None,
    Lz4Like,
    ZstdLike,
    NativeVortex,
    Unknown,
}
impl SpillCompression {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4Like => "lz4_like",
            Self::ZstdLike => "zstd_like",
            Self::NativeVortex => "native_vortex",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillFileStatus {
    Planned,
    Written,
    ReadBack,
    Cleaned,
    Failed,
    Unknown,
}
impl SpillFileStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Written => "written",
            Self::ReadBack => "read_back",
            Self::Cleaned => "cleaned",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Cleaned | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillFileRef {
    pub spill_id: String,
    pub path: String,
    pub owner: MemoryOwner,
    pub format: SpillFormat,
    pub compression: SpillCompression,
    pub size_bytes: Option<ByteSize>,
    pub status: SpillFileStatus,
}
impl SpillFileRef {
    pub fn planned(
        spill_id: impl Into<String>,
        path: impl Into<String>,
        owner: MemoryOwner,
        format: SpillFormat,
    ) -> Result<Self> {
        Ok(Self {
            spill_id: non_empty(spill_id.into(), "spill id")?,
            path: non_empty(path.into(), "spill path")?,
            owner,
            format,
            compression: SpillCompression::None,
            size_bytes: None,
            status: SpillFileStatus::Planned,
        })
    }
    pub fn with_compression(mut self, compression: SpillCompression) -> Self {
        self.compression = compression;
        self
    }
    pub fn with_size_bytes(mut self, size: ByteSize) -> Self {
        self.size_bytes = Some(size);
        self
    }
    pub fn with_status(mut self, status: SpillFileStatus) -> Self {
        self.status = status;
        self
    }
    pub fn summary(&self) -> String {
        format!(
            "spill_id={}, path={}, format={}, compression={}, status={}",
            self.spill_id,
            self.path,
            self.format.as_str(),
            self.compression.as_str(),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPartition {
    pub partition_id: String,
    pub files: Vec<SpillFileRef>,
    pub estimated_rows: Option<u64>,
    pub estimated_encoded_bytes: Option<ByteSize>,
    pub estimated_decoded_bytes: Option<ByteSize>,
}
impl SpillPartition {
    pub fn new(partition_id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            partition_id: non_empty(partition_id.into(), "spill partition id")?,
            files: vec![],
            estimated_rows: None,
            estimated_encoded_bytes: None,
            estimated_decoded_bytes: None,
        })
    }
    pub fn add_file(&mut self, file: SpillFileRef) {
        self.files.push(file);
    }
    pub fn with_estimated_rows(mut self, rows: u64) -> Self {
        self.estimated_rows = Some(rows);
        self
    }
    pub fn with_estimated_encoded_bytes(mut self, bytes: ByteSize) -> Self {
        self.estimated_encoded_bytes = Some(bytes);
        self
    }
    pub fn with_estimated_decoded_bytes(mut self, bytes: ByteSize) -> Self {
        self.estimated_decoded_bytes = Some(bytes);
        self
    }
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
    pub fn summary(&self) -> String {
        format!(
            "partition_id={}, files={}",
            self.partition_id,
            self.file_count()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillDecisionKind {
    KeepInMemory,
    SpillNow,
    SpillLater,
    ReduceParallelism,
    SplitTask,
    FailBeforeOom,
    Unsupported,
}
impl SpillDecisionKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::KeepInMemory => "keep_in_memory",
            Self::SpillNow => "spill_now",
            Self::SpillLater => "spill_later",
            Self::ReduceParallelism => "reduce_parallelism",
            Self::SplitTask => "split_task",
            Self::FailBeforeOom => "fail_before_oom",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn requires_action(&self) -> bool {
        matches!(
            self,
            Self::SpillNow
                | Self::ReduceParallelism
                | Self::SplitTask
                | Self::FailBeforeOom
                | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillDecision {
    pub kind: SpillDecisionKind,
    pub reason: String,
}
impl SpillDecision {
    fn with_kind(kind: SpillDecisionKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
    pub fn keep_in_memory(reason: impl Into<String>) -> Self {
        Self::with_kind(SpillDecisionKind::KeepInMemory, reason)
    }
    pub fn spill_now(reason: impl Into<String>) -> Self {
        Self::with_kind(SpillDecisionKind::SpillNow, reason)
    }
    pub fn spill_later(reason: impl Into<String>) -> Self {
        Self::with_kind(SpillDecisionKind::SpillLater, reason)
    }
    pub fn reduce_parallelism(reason: impl Into<String>) -> Self {
        Self::with_kind(SpillDecisionKind::ReduceParallelism, reason)
    }
    pub fn split_task(reason: impl Into<String>) -> Self {
        Self::with_kind(SpillDecisionKind::SplitTask, reason)
    }
    pub fn fail_before_oom(reason: impl Into<String>) -> Self {
        Self::with_kind(SpillDecisionKind::FailBeforeOom, reason)
    }
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::with_kind(SpillDecisionKind::Unsupported, reason)
    }
    pub const fn requires_action(&self) -> bool {
        self.kind.requires_action()
    }
    pub fn summary(&self) -> String {
        format!("kind={}, reason={}", self.kind.as_str(), self.reason)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPlanStatus {
    Planned,
    SpillNotImplemented,
    Unsupported,
}
impl SpillPlanStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::SpillNotImplemented => "spill_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Planning-only spill plan skeleton with deterministic diagnostics and no fallback execution.
pub struct SpillPlan {
    pub owner: MemoryOwner,
    pub policy: SpillPolicy,
    pub format: SpillFormat,
    pub compression: SpillCompression,
    pub decision: SpillDecision,
    pub partitions: Vec<SpillPartition>,
    pub status: SpillPlanStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl SpillPlan {
    pub fn planned(owner: MemoryOwner, policy: SpillPolicy) -> Self {
        Self {
            owner,
            policy,
            format: SpillFormat::VortexNativeSpill,
            compression: SpillCompression::NativeVortex,
            decision: SpillDecision::keep_in_memory(
                "Planning-only skeleton; no spill IO performed.",
            ),
            partitions: vec![],
            status: SpillPlanStatus::Planned,
            diagnostics: vec![],
        }
    }
    pub fn spill_not_implemented(owner: MemoryOwner, policy: SpillPolicy) -> Self {
        let mut s = Self::planned(owner, policy);
        s.status = SpillPlanStatus::SpillNotImplemented;
        s.decision = SpillDecision::fail_before_oom(
            "Spill behavior is not implemented in this planning skeleton.",
        );
        s.diagnostics.push(Diagnostic::new(DiagnosticCode::NotImplemented, DiagnosticSeverity::Error, DiagnosticCategory::ResourceBudget, "Spill behavior is not implemented for native execution planning.", Some("spill".to_string()), Some("fallback execution was not attempted; Spark/DataFusion/DuckDB/Polars/Velox are not fallback engines".to_string()), Some("Use planning-only commands or wait for native spill support.".to_string()), FallbackStatus::disabled_by_policy()));
        s
    }
    pub fn unsupported(
        owner: MemoryOwner,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let mut s = Self::planned(owner, SpillPolicy::DisabledForOperator);
        s.status = SpillPlanStatus::Unsupported;
        s.decision = SpillDecision::unsupported(reason.clone());
        s.diagnostics.push(Diagnostic::new(
            DiagnosticCode::NoFallbackExecution,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "Unsupported spill/OOM behavior; fallback execution not attempted.",
            Some(feature),
            Some(format!(
                "{reason}; Spark/DataFusion/DuckDB/Polars/Velox are not fallback engines"
            )),
            Some("Use a supported native plan or reduce memory pressure.".to_string()),
            FallbackStatus::disabled_by_policy(),
        ));
        s
    }
    pub fn with_format(mut self, format: SpillFormat) -> Self {
        self.format = format;
        self
    }
    pub fn with_compression(mut self, compression: SpillCompression) -> Self {
        self.compression = compression;
        self
    }
    pub fn with_decision(mut self, decision: SpillDecision) -> Self {
        self.decision = decision;
        self
    }
    pub fn add_partition(&mut self, partition: SpillPartition) {
        self.partitions.push(partition);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "spill_plan\nowner=[{}]\npolicy={}\nformat={}\ncompression={}\ndecision={}\nstatus={}\nfallback execution: disabled",
            self.owner.summary(),
            self.policy.as_str(),
            self.format.as_str(),
            self.compression.as_str(),
            self.decision.summary(),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpillReport {
    pub owner: MemoryOwner,
    pub bytes_spilled: ByteSize,
    pub files_created: usize,
    pub memory_released: ByteSize,
    pub cleanup_completed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl SpillReport {
    pub fn new(owner: MemoryOwner) -> Self {
        Self {
            owner,
            bytes_spilled: ByteSize::from_bytes(0),
            files_created: 0,
            memory_released: ByteSize::from_bytes(0),
            cleanup_completed: false,
            diagnostics: vec![],
        }
    }
    pub fn with_bytes_spilled(mut self, bytes: ByteSize) -> Self {
        self.bytes_spilled = bytes;
        self
    }
    pub fn with_files_created(mut self, files: usize) -> Self {
        self.files_created = files;
        self
    }
    pub fn with_memory_released(mut self, bytes: ByteSize) -> Self {
        self.memory_released = bytes;
        self
    }
    pub fn with_cleanup_completed(mut self, cleanup_completed: bool) -> Self {
        self.cleanup_completed = cleanup_completed;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "spill_report\nowner=[{}]\nbytes_spilled={}\nfiles_created={}\nmemory_released={}\ncleanup_completed={}\nfallback execution: disabled",
            self.owner.summary(),
            self.bytes_spilled.to_human_text(),
            self.files_created,
            self.memory_released.to_human_text(),
            self.cleanup_completed
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Top-level OOM safety planning skeleton combining memory and spill plans.
pub struct OomSafetyPlan {
    pub memory_pool: MemoryPoolPlan,
    pub spill_plans: Vec<SpillPlan>,
    pub pressure: MemoryPressureLevel,
    pub diagnostics: Vec<Diagnostic>,
}
impl OomSafetyPlan {
    pub fn new(memory_pool: MemoryPoolPlan) -> Self {
        let pressure = memory_pool.pressure();
        Self {
            memory_pool,
            spill_plans: vec![],
            pressure,
            diagnostics: vec![],
        }
    }
    pub fn add_spill_plan(&mut self, spill_plan: SpillPlan) {
        self.spill_plans.push(spill_plan);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn requires_action(&self) -> bool {
        self.pressure.requires_action()
            || self
                .spill_plans
                .iter()
                .any(|p| p.decision.requires_action())
    }
    pub fn has_errors(&self) -> bool {
        self.memory_pool.has_errors()
            || self.spill_plans.iter().any(SpillPlan::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "oom_safety_plan\npressure={}\nrequires_action={}\nspill_plans={}\nfallback execution: disabled",
            self.pressure.as_str(),
            self.requires_action(),
            self.spill_plans.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bs(v: u64) -> ByteSize {
        ByteSize::from_bytes(v)
    }
    #[test]
    fn budget_from_gib_rejects_zero() {
        assert!(MemoryBudget::from_gib(0).is_err());
    }
    #[test]
    fn budget_new_limits() {
        let b = MemoryBudget::new(bs(100)).unwrap();
        assert!(b.soft_limit <= b.hard_limit && b.hard_limit <= b.total);
    }
    #[test]
    fn memory_budget_canonical_label_is_memory_budget() {
        assert_eq!(
            MemoryBudget::new(bs(100)).unwrap().canonical_label(),
            "memory_budget"
        );
    }
    #[test]
    fn budget_pressure_normal_below_soft() {
        let b = MemoryBudget::with_limits(bs(100), bs(80), bs(100)).unwrap();
        assert_eq!(b.pressure_for_reserved(bs(10)), MemoryPressureLevel::Normal);
    }
    #[test]
    fn budget_pressure_high_above_soft() {
        let b = MemoryBudget::with_limits(bs(100), bs(80), bs(100)).unwrap();
        assert!(matches!(
            b.pressure_for_reserved(bs(90)),
            MemoryPressureLevel::High | MemoryPressureLevel::Critical
        ));
    }
    #[test]
    fn high_requires_action() {
        assert!(MemoryPressureLevel::High.requires_action());
    }
    #[test]
    fn join_stateful() {
        assert!(OperatorMemoryClass::Join.is_stateful());
    }
    #[test]
    fn join_spill_candidate() {
        assert!(OperatorMemoryClass::Join.is_spill_candidate());
    }
    #[test]
    fn scan_not_stateful() {
        assert!(!OperatorMemoryClass::Scan.is_stateful());
    }
    #[test]
    fn owner_rejects_empty() {
        assert!(MemoryOwner::new(OperatorMemoryClass::Scan, " ").is_err());
    }
    #[test]
    fn reservation_id_rejects_empty() {
        assert!(MemoryReservationId::new(" ").is_err());
    }
    #[test]
    fn reservation_request_requested() {
        let r = MemoryReservation::request(
            MemoryReservationId::new("r1").unwrap(),
            MemoryOwner::new(OperatorMemoryClass::Scan, "x").unwrap(),
            bs(1),
        );
        assert_eq!(r.status, MemoryReservationStatus::Requested);
    }
    #[test]
    fn reservation_granted() {
        let r = MemoryReservation::request(
            MemoryReservationId::new("r1").unwrap(),
            MemoryOwner::new(OperatorMemoryClass::Scan, "x").unwrap(),
            bs(1),
        )
        .granted(bs(1));
        assert!(r.is_granted());
    }
    #[test]
    fn pool_reserved_granted_only() {
        let mut p = MemoryPoolPlan::new(MemoryBudget::new(bs(100)).unwrap());
        let owner = MemoryOwner::new(OperatorMemoryClass::Scan, "x").unwrap();
        p.add_reservation(MemoryReservation::request(
            MemoryReservationId::new("r1").unwrap(),
            owner.clone(),
            bs(10),
        ));
        p.add_reservation(
            MemoryReservation::request(MemoryReservationId::new("r2").unwrap(), owner, bs(10))
                .granted(bs(7)),
        );
        assert_eq!(p.reserved_bytes(), bs(7));
    }
    #[test]
    fn pool_recompute_updates_count() {
        let mut p = MemoryPoolPlan::new(MemoryBudget::new(bs(100)).unwrap());
        let owner = MemoryOwner::new(OperatorMemoryClass::Scan, "x").unwrap();
        p.add_reservation(MemoryReservation::request(
            MemoryReservationId::new("r1").unwrap(),
            owner,
            bs(10),
        ));
        p.recompute_snapshot();
        assert_eq!(p.snapshot.reservation_count, 1);
    }
    #[test]
    fn policy_best_effort_allows() {
        assert!(SpillPolicy::BestEffort.allows_spill());
    }
    #[test]
    fn policy_required_requires_support() {
        assert!(SpillPolicy::Required.requires_spill_support());
    }
    #[test]
    fn format_vortex_native_columnar() {
        assert!(
            SpillFormat::VortexNativeSpill.is_vortex_native()
                && SpillFormat::VortexNativeSpill.is_columnar()
        );
    }
    #[test]
    fn spill_file_rejects_empty_id() {
        let owner = MemoryOwner::new(OperatorMemoryClass::Sort, "s").unwrap();
        assert!(SpillFileRef::planned(" ", "/x", owner, SpillFormat::Unknown).is_err());
    }
    #[test]
    fn spill_file_rejects_empty_path() {
        let owner = MemoryOwner::new(OperatorMemoryClass::Sort, "s").unwrap();
        assert!(SpillFileRef::planned("id", " ", owner, SpillFormat::Unknown).is_err());
    }
    #[test]
    fn spill_partition_rejects_empty() {
        assert!(SpillPartition::new(" ").is_err());
    }
    #[test]
    fn spill_now_requires_action() {
        assert!(SpillDecision::spill_now("x").requires_action());
    }
    #[test]
    fn keep_mem_no_action() {
        assert!(!SpillDecision::keep_in_memory("x").requires_action());
    }
    #[test]
    fn spill_plan_defaults_vortex() {
        let owner = MemoryOwner::new(OperatorMemoryClass::Sort, "s").unwrap();
        assert_eq!(
            SpillPlan::planned(owner, SpillPolicy::BestEffort).format,
            SpillFormat::VortexNativeSpill
        );
    }
    #[test]
    fn spill_not_impl_has_errors() {
        let owner = MemoryOwner::new(OperatorMemoryClass::Sort, "s").unwrap();
        assert!(SpillPlan::spill_not_implemented(owner, SpillPolicy::BestEffort).has_errors());
    }
    #[test]
    fn spill_plan_text_mentions_fallback_disabled() {
        let owner = MemoryOwner::new(OperatorMemoryClass::Sort, "s").unwrap();
        assert!(
            SpillPlan::planned(owner, SpillPolicy::BestEffort)
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn spill_report_defaults_zero() {
        let owner = MemoryOwner::new(OperatorMemoryClass::Sort, "s").unwrap();
        let r = SpillReport::new(owner);
        assert_eq!(r.bytes_spilled, bs(0));
        assert_eq!(r.files_created, 0);
    }
    #[test]
    fn oom_requires_action_for_pressure_or_spill() {
        let mut pool =
            MemoryPoolPlan::new(MemoryBudget::with_limits(bs(100), bs(80), bs(90)).unwrap());
        let owner = MemoryOwner::new(OperatorMemoryClass::Sort, "s").unwrap();
        pool.add_reservation(
            MemoryReservation::request(
                MemoryReservationId::new("r").unwrap(),
                owner.clone(),
                bs(90),
            )
            .granted(bs(90)),
        );
        pool.recompute_snapshot();
        let mut oom = OomSafetyPlan::new(pool);
        assert!(oom.requires_action());
        oom.spill_plans.clear();
        let mut oom2 = OomSafetyPlan::new(MemoryPoolPlan::new(MemoryBudget::new(bs(100)).unwrap()));
        oom2.add_spill_plan(
            SpillPlan::planned(owner, SpillPolicy::BestEffort)
                .with_decision(SpillDecision::spill_now("x")),
        );
        assert!(oom2.requires_action());
    }
    #[test]
    fn oom_text_mentions_fallback_disabled() {
        let oom = OomSafetyPlan::new(MemoryPoolPlan::new(MemoryBudget::new(bs(100)).unwrap()));
        assert!(oom.to_human_text().contains("fallback execution: disabled"));
    }
}
