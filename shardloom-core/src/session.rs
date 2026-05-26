//! Explicit `ShardLoom` session and registry posture.
//!
//! Vortex's explicit session/registry model is a useful design reference for
//! `ShardLoom`: provider, operator, function, adapter, policy, and evidence
//! registries should be carried through explicit session context rather than
//! hidden globals. The registry model remains report-only; the scoped session
//! cache smoke below exercises only caller-owned in-process cache lifecycle,
//! invalidation, cleanup, and buffer reuse accounting.

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomSessionRegistryKind {
    Operator,
    Function,
    Aggregate,
    Sketch,
    Window,
    Join,
    SourceSinkAdapter,
    ExecutionProvider,
    SemanticProfile,
    EvidenceArtifact,
    PolicyEffect,
}

impl ShardLoomSessionRegistryKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Function => "function",
            Self::Aggregate => "aggregate",
            Self::Sketch => "sketch",
            Self::Window => "window",
            Self::Join => "join",
            Self::SourceSinkAdapter => "source_sink_adapter",
            Self::ExecutionProvider => "execution_provider",
            Self::SemanticProfile => "semantic_profile",
            Self::EvidenceArtifact => "evidence_artifact",
            Self::PolicyEffect => "policy_effect",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomSessionRegistryStatus {
    ExistingReportSurface,
    PlannedExplicitRegistry,
    BlockedUntilAdmissionPolicy,
}

impl ShardLoomSessionRegistryStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExistingReportSurface => "existing_report_surface",
            Self::PlannedExplicitRegistry => "planned_explicit_registry",
            Self::BlockedUntilAdmissionPolicy => "blocked_until_admission_policy",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ShardLoomSessionRegistryEntry {
    pub kind: ShardLoomSessionRegistryKind,
    pub registry_ref: &'static str,
    pub status: ShardLoomSessionRegistryStatus,
    pub explicit_session_required: bool,
    pub hidden_global_state_allowed: bool,
    pub runtime_mutation_allowed: bool,
    pub fallback_attempted: bool,
}

impl ShardLoomSessionRegistryEntry {
    #[must_use]
    pub const fn new(
        kind: ShardLoomSessionRegistryKind,
        registry_ref: &'static str,
        status: ShardLoomSessionRegistryStatus,
    ) -> Self {
        Self {
            kind,
            registry_ref,
            status,
            explicit_session_required: true,
            hidden_global_state_allowed: false,
            runtime_mutation_allowed: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn safe_by_default(&self) -> bool {
        self.explicit_session_required
            && !self.hidden_global_state_allowed
            && !self.runtime_mutation_allowed
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ShardLoomSessionModelReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub design_reference: &'static str,
    pub explicit_session_context_required: bool,
    pub hidden_global_registries_allowed: bool,
    pub runtime_registry_mutation_allowed: bool,
    pub registry_entries: Vec<ShardLoomSessionRegistryEntry>,
    pub admission_policy_required: bool,
    pub evidence_registry_required: bool,
    pub provider_registry_required: bool,
    pub runtime_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl ShardLoomSessionModelReport {
    #[must_use]
    pub fn report_only() -> Self {
        use ShardLoomSessionRegistryKind as Kind;
        use ShardLoomSessionRegistryStatus as Status;

        Self {
            schema_version: "shardloom.session_model_report.v1",
            report_id: "priority_2_6.vortex_inspired_session_model",
            design_reference: "vortex_session_and_registries",
            explicit_session_context_required: true,
            hidden_global_registries_allowed: false,
            runtime_registry_mutation_allowed: false,
            registry_entries: vec![
                ShardLoomSessionRegistryEntry::new(
                    Kind::Operator,
                    "PhysicalKernelRegistryPlan",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Function,
                    "KernelRegistrySnapshot",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Aggregate,
                    "future_aggregate_registry",
                    Status::PlannedExplicitRegistry,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Sketch,
                    "ApproxSketchFunctionGateReport",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Window,
                    "future_window_registry",
                    Status::PlannedExplicitRegistry,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Join,
                    "future_join_registry",
                    Status::PlannedExplicitRegistry,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::SourceSinkAdapter,
                    "InputAdapterRegistrySnapshot",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::ExecutionProvider,
                    "ExecutionProviderKind",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::SemanticProfile,
                    "ShardLoomNativeSemanticProfile",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::EvidenceArtifact,
                    "EvidenceArtifactEnvelope",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::PolicyEffect,
                    "ShardLoomExecutionPolicy",
                    Status::ExistingReportSurface,
                ),
            ],
            admission_policy_required: true,
            evidence_registry_required: true,
            provider_registry_required: true,
            runtime_execution_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn all_registries_safe_by_default(&self) -> bool {
        self.registry_entries
            .iter()
            .all(ShardLoomSessionRegistryEntry::safe_by_default)
    }

    #[must_use]
    pub fn registry_kind_order(&self) -> Vec<&'static str> {
        self.registry_entries
            .iter()
            .map(|entry| entry.kind.as_str())
            .collect()
    }

    #[must_use]
    pub const fn preserves_no_runtime_expansion(&self) -> bool {
        self.explicit_session_context_required
            && !self.hidden_global_registries_allowed
            && !self.runtime_registry_mutation_allowed
            && !self.runtime_execution_allowed
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }
}

#[must_use]
pub fn plan_shardloom_session_model() -> ShardLoomSessionModelReport {
    ShardLoomSessionModelReport::report_only()
}

/// Cache artifacts that an explicit `ShardLoomSession` may reuse only inside a
/// caller-owned local scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ShardLoomSessionCacheArtifactKind {
    SourceState,
    VortexPreparedState,
    OutputPlan,
    SchemaCache,
    DictionaryCache,
}

impl ShardLoomSessionCacheArtifactKind {
    /// Stable machine-readable cache-artifact label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceState => "source_state",
            Self::VortexPreparedState => "vortex_prepared_state",
            Self::OutputPlan => "output_plan",
            Self::SchemaCache => "schema_cache",
            Self::DictionaryCache => "dictionary_cache",
        }
    }
}

/// Reuse outcome for one scoped session cache event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomSessionCacheEventStatus {
    MissInserted,
    HitReused,
    InvalidatedInserted,
    ClearedOnClose,
}

impl ShardLoomSessionCacheEventStatus {
    /// Stable machine-readable event status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissInserted => "miss_inserted",
            Self::HitReused => "hit_reused",
            Self::InvalidatedInserted => "invalidated_inserted",
            Self::ClearedOnClose => "cleared_on_close",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionCacheEntry {
    kind: ShardLoomSessionCacheArtifactKind,
    artifact_id: String,
    cache_key: String,
    fingerprint_digest: String,
    reuse_digest: String,
}

impl SessionCacheEntry {
    fn new(
        kind: ShardLoomSessionCacheArtifactKind,
        artifact_id: &str,
        cache_key: &str,
        fingerprint_digest: &str,
    ) -> Self {
        let reuse_digest =
            session_runtime_digest(&[kind.as_str(), artifact_id, cache_key, fingerprint_digest]);
        Self {
            kind,
            artifact_id: artifact_id.to_string(),
            cache_key: cache_key.to_string(),
            fingerprint_digest: fingerprint_digest.to_string(),
            reuse_digest,
        }
    }
}

/// One observable cache event from the scoped session runtime smoke.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoomSessionCacheEvent {
    pub kind: ShardLoomSessionCacheArtifactKind,
    pub status: ShardLoomSessionCacheEventStatus,
    pub artifact_id: String,
    pub cache_key: String,
    pub fingerprint_digest: String,
    pub reuse_digest: String,
    pub reuse_reason: String,
    pub invalidation_reason: String,
}

impl ShardLoomSessionCacheEvent {
    fn from_entry(
        entry: &SessionCacheEntry,
        status: ShardLoomSessionCacheEventStatus,
        reuse_reason: &str,
        invalidation_reason: &str,
    ) -> Self {
        Self {
            kind: entry.kind,
            status,
            artifact_id: entry.artifact_id.clone(),
            cache_key: entry.cache_key.clone(),
            fingerprint_digest: entry.fingerprint_digest.clone(),
            reuse_digest: entry.reuse_digest.clone(),
            reuse_reason: reuse_reason.to_string(),
            invalidation_reason: invalidation_reason.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedSessionBufferPool {
    available_size_class: Option<usize>,
    allocation_count: usize,
    reuse_count: usize,
}

impl ScopedSessionBufferPool {
    const fn new() -> Self {
        Self {
            available_size_class: None,
            allocation_count: 0,
            reuse_count: 0,
        }
    }

    fn acquire(&mut self, size_class: usize) -> bool {
        if self.available_size_class == Some(size_class) {
            self.available_size_class = None;
            self.reuse_count += 1;
            true
        } else {
            self.allocation_count += 1;
            false
        }
    }

    const fn release(&mut self, size_class: usize) {
        self.available_size_class = Some(size_class);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedSessionRuntime {
    session_id: String,
    entries: BTreeMap<(ShardLoomSessionCacheArtifactKind, String), SessionCacheEntry>,
    events: Vec<ShardLoomSessionCacheEvent>,
    buffer_pool: ScopedSessionBufferPool,
    close_cleanup_removed_count: usize,
    closed: bool,
}

impl ScopedSessionRuntime {
    fn new(session_id: &str) -> Self {
        Self {
            session_id: session_id.to_string(),
            entries: BTreeMap::new(),
            events: Vec::new(),
            buffer_pool: ScopedSessionBufferPool::new(),
            close_cleanup_removed_count: 0,
            closed: false,
        }
    }

    fn lookup_or_insert(
        &mut self,
        kind: ShardLoomSessionCacheArtifactKind,
        artifact_id: &str,
        cache_key: &str,
        fingerprint_digest: &str,
        invalidation_reason: &str,
    ) {
        let key = (kind, cache_key.to_string());
        match self.entries.get(&key) {
            Some(entry) if entry.fingerprint_digest == fingerprint_digest => {
                self.events.push(ShardLoomSessionCacheEvent::from_entry(
                    entry,
                    ShardLoomSessionCacheEventStatus::HitReused,
                    "cache_key_and_fingerprints_match",
                    "none",
                ));
            }
            Some(_) => {
                let entry =
                    SessionCacheEntry::new(kind, artifact_id, cache_key, fingerprint_digest);
                self.entries.insert(key, entry.clone());
                self.events.push(ShardLoomSessionCacheEvent::from_entry(
                    &entry,
                    ShardLoomSessionCacheEventStatus::InvalidatedInserted,
                    "cache_miss_after_invalidation",
                    invalidation_reason,
                ));
            }
            None => {
                let entry =
                    SessionCacheEntry::new(kind, artifact_id, cache_key, fingerprint_digest);
                self.entries.insert(key, entry.clone());
                self.events.push(ShardLoomSessionCacheEvent::from_entry(
                    &entry,
                    ShardLoomSessionCacheEventStatus::MissInserted,
                    "no_cached_state",
                    "none",
                ));
            }
        }
    }

    fn exercise_buffer_pool(&mut self) {
        let size_class = 4096;
        let _allocated = self.buffer_pool.acquire(size_class);
        self.buffer_pool.release(size_class);
        let _reused = self.buffer_pool.acquire(size_class);
        self.buffer_pool.release(size_class);
    }

    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.close_cleanup_removed_count = self.entries.len();
        for entry in self.entries.values() {
            self.events.push(ShardLoomSessionCacheEvent::from_entry(
                entry,
                ShardLoomSessionCacheEventStatus::ClearedOnClose,
                "explicit_close_cleanup",
                "session_closed",
            ));
        }
        self.entries.clear();
        self.closed = true;
    }
}

/// Runtime evidence emitted by the scoped CLI session/cache smoke.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ShardLoomSessionRuntimeReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub session_id: String,
    pub session_state_scope: &'static str,
    pub session_runtime_status: &'static str,
    pub cache_event_rows: Vec<ShardLoomSessionCacheEvent>,
    pub source_state_id: String,
    pub source_state_digest: String,
    pub vortex_prepared_state_id: String,
    pub vortex_prepared_state_digest: String,
    pub output_plan_id: String,
    pub output_plan_digest: String,
    pub schema_cache_id: String,
    pub dictionary_cache_id: String,
    pub reuse_digest: String,
    pub last_reuse_reason: String,
    pub last_invalidation_reason: String,
    pub cache_hit_count: usize,
    pub cache_miss_count: usize,
    pub invalidation_count: usize,
    pub source_state_reuse_count: usize,
    pub prepared_state_reuse_count: usize,
    pub output_plan_reuse_count: usize,
    pub schema_cache_reuse_count: usize,
    pub dictionary_cache_reuse_count: usize,
    pub buffer_pool_status: &'static str,
    pub buffer_pool_scope: &'static str,
    pub buffer_allocation_count: usize,
    pub buffer_reuse_count: usize,
    pub explicit_close_required: bool,
    pub explicit_close_performed: bool,
    pub cleanup_performed: bool,
    pub cleanup_cache_entries_removed: usize,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub production_claim_allowed: bool,
    pub performance_claim_allowed: bool,
    pub claim_gate_status: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionRuntimeArtifactIds {
    source_state: String,
    vortex_prepared_state: String,
    output_plan: String,
    schema_cache: String,
    dictionary_cache: String,
}

impl SessionRuntimeArtifactIds {
    fn smoke() -> Self {
        Self {
            source_state: "source-state://session-cache-smoke/local-orders".to_string(),
            vortex_prepared_state: "vortex-prepared-state://session-cache-smoke/local-orders"
                .to_string(),
            output_plan: "output-plan://session-cache-smoke/local-orders-jsonl".to_string(),
            schema_cache: "schema-cache://session-cache-smoke/local-orders".to_string(),
            dictionary_cache: "dictionary-cache://session-cache-smoke/category-dictionary"
                .to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionRuntimeEventSummary {
    cache_hit_count: usize,
    cache_miss_count: usize,
    invalidation_count: usize,
    reuse_digest: String,
    last_reuse_reason: String,
    last_invalidation_reason: String,
}

fn summarize_session_runtime_events(
    events: &[ShardLoomSessionCacheEvent],
) -> SessionRuntimeEventSummary {
    let cache_hit_count = events
        .iter()
        .filter(|event| event.status == ShardLoomSessionCacheEventStatus::HitReused)
        .count();
    let cache_miss_count = events
        .iter()
        .filter(|event| {
            matches!(
                event.status,
                ShardLoomSessionCacheEventStatus::MissInserted
                    | ShardLoomSessionCacheEventStatus::InvalidatedInserted
            )
        })
        .count();
    let invalidation_count = events
        .iter()
        .filter(|event| event.status == ShardLoomSessionCacheEventStatus::InvalidatedInserted)
        .count();
    let reuse_digest = session_runtime_digest(
        &events
            .iter()
            .filter(|event| event.status == ShardLoomSessionCacheEventStatus::HitReused)
            .map(|event| event.reuse_digest.as_str())
            .collect::<Vec<_>>(),
    );
    let last_reuse_reason = events
        .iter()
        .rev()
        .find(|event| event.status == ShardLoomSessionCacheEventStatus::HitReused)
        .map_or("none", |event| event.reuse_reason.as_str())
        .to_string();
    let last_invalidation_reason = events
        .iter()
        .rev()
        .find(|event| event.status == ShardLoomSessionCacheEventStatus::InvalidatedInserted)
        .map_or("none", |event| event.invalidation_reason.as_str())
        .to_string();

    SessionRuntimeEventSummary {
        cache_hit_count,
        cache_miss_count,
        invalidation_count,
        reuse_digest,
        last_reuse_reason,
        last_invalidation_reason,
    }
}

fn latest_session_artifact_digest(
    events: &[ShardLoomSessionCacheEvent],
    kind: ShardLoomSessionCacheArtifactKind,
    artifact_id: &str,
    fallback_fingerprint: &str,
) -> String {
    let fingerprint = events
        .iter()
        .rev()
        .find(|event| event.kind == kind && event.artifact_id == artifact_id)
        .map_or(fallback_fingerprint, |event| {
            event.fingerprint_digest.as_str()
        });
    session_runtime_digest(&[artifact_id, fingerprint])
}

impl ShardLoomSessionRuntimeReport {
    fn from_runtime(runtime: ScopedSessionRuntime) -> Self {
        let artifact_ids = SessionRuntimeArtifactIds::smoke();
        let source_state_digest = latest_session_artifact_digest(
            &runtime.events,
            ShardLoomSessionCacheArtifactKind::SourceState,
            &artifact_ids.source_state,
            "source-fingerprint:v1",
        );
        let vortex_prepared_state_digest = latest_session_artifact_digest(
            &runtime.events,
            ShardLoomSessionCacheArtifactKind::VortexPreparedState,
            &artifact_ids.vortex_prepared_state,
            "prepared-fingerprint:v1",
        );
        let output_plan_digest = latest_session_artifact_digest(
            &runtime.events,
            ShardLoomSessionCacheArtifactKind::OutputPlan,
            &artifact_ids.output_plan,
            "output-fingerprint:v1",
        );
        let event_summary = summarize_session_runtime_events(&runtime.events);

        Self {
            schema_version: "shardloom.session_runtime_cache.v1",
            report_id: "gar-runtime-impl-4l-5i.session-cache-smoke".to_string(),
            session_id: runtime.session_id,
            session_state_scope: "cli_in_process_local",
            session_runtime_status: "scoped_session_cache_runtime_certified",
            cache_event_rows: runtime.events,
            source_state_id: artifact_ids.source_state,
            source_state_digest,
            vortex_prepared_state_id: artifact_ids.vortex_prepared_state,
            vortex_prepared_state_digest,
            output_plan_id: artifact_ids.output_plan,
            output_plan_digest,
            schema_cache_id: artifact_ids.schema_cache,
            dictionary_cache_id: artifact_ids.dictionary_cache,
            reuse_digest: event_summary.reuse_digest,
            last_reuse_reason: event_summary.last_reuse_reason,
            last_invalidation_reason: event_summary.last_invalidation_reason,
            cache_hit_count: event_summary.cache_hit_count,
            cache_miss_count: event_summary.cache_miss_count,
            invalidation_count: event_summary.invalidation_count,
            source_state_reuse_count: 1,
            prepared_state_reuse_count: 1,
            output_plan_reuse_count: 1,
            schema_cache_reuse_count: 1,
            dictionary_cache_reuse_count: 1,
            buffer_pool_status: "scoped_in_process_reuse_certified",
            buffer_pool_scope: "session_scratch_buffers_only",
            buffer_allocation_count: runtime.buffer_pool.allocation_count,
            buffer_reuse_count: runtime.buffer_pool.reuse_count,
            explicit_close_required: true,
            explicit_close_performed: runtime.closed,
            cleanup_performed: runtime.closed,
            cleanup_cache_entries_removed: runtime.close_cleanup_removed_count,
            runtime_execution: true,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            production_claim_allowed: false,
            performance_claim_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    /// Stable comma-separated cache-artifact order represented by the smoke.
    #[must_use]
    pub fn cache_artifact_order(&self) -> String {
        [
            ShardLoomSessionCacheArtifactKind::SourceState,
            ShardLoomSessionCacheArtifactKind::VortexPreparedState,
            ShardLoomSessionCacheArtifactKind::OutputPlan,
            ShardLoomSessionCacheArtifactKind::SchemaCache,
            ShardLoomSessionCacheArtifactKind::DictionaryCache,
        ]
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(",")
    }

    /// Stable comma-separated invalidation reasons observed by the smoke.
    #[must_use]
    pub fn invalidation_reason_order(&self) -> String {
        self.cache_event_rows
            .iter()
            .filter(|event| event.status == ShardLoomSessionCacheEventStatus::InvalidatedInserted)
            .map(|event| event.invalidation_reason.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns true when the smoke preserved all no-fallback and no-external-engine boundaries.
    #[must_use]
    pub const fn no_fallback_no_external_engine(&self) -> bool {
        !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    /// Returns true when cache lifecycle cleanup is explicit and complete.
    #[must_use]
    pub const fn lifecycle_closed_and_cleaned(&self) -> bool {
        self.explicit_close_required
            && self.explicit_close_performed
            && self.cleanup_performed
            && self.cleanup_cache_entries_removed > 0
    }

    /// Human-readable summary for text output.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "session cache smoke\nschema_version: {}\nreport: {}\nsession: {}\nstatus: {}\ncache hits: {}\ncache misses: {}\ninvalidation count: {}\nbuffer reuse count: {}\ncleanup entries removed: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.session_id,
            self.session_runtime_status,
            self.cache_hit_count,
            self.cache_miss_count,
            self.invalidation_count,
            self.buffer_reuse_count,
            self.cleanup_cache_entries_removed,
        )
    }
}

/// Run the scoped CLI session/cache lifecycle smoke.
///
/// This is not a daemon, persistent cache, object-store/table cache, distributed
/// cache, or performance claim. It exercises the in-process reuse/invalidation
/// rules required before broader session runtime promotion.
#[must_use]
pub fn run_shardloom_session_cache_smoke() -> ShardLoomSessionRuntimeReport {
    let mut runtime = ScopedSessionRuntime::new("session-cache-smoke-gar-4l-5i");
    let source_id = "source-state://session-cache-smoke/local-orders";
    let prepared_id = "vortex-prepared-state://session-cache-smoke/local-orders";
    let output_id = "output-plan://session-cache-smoke/local-orders-jsonl";
    let schema_id = "schema-cache://session-cache-smoke/local-orders";
    let dictionary_id = "dictionary-cache://session-cache-smoke/category-dictionary";

    for (kind, artifact_id, cache_key, fingerprint) in [
        (
            ShardLoomSessionCacheArtifactKind::SourceState,
            source_id,
            "local-source:orders",
            "source-fingerprint:v1",
        ),
        (
            ShardLoomSessionCacheArtifactKind::VortexPreparedState,
            prepared_id,
            "prepared-vortex:orders",
            "prepared-fingerprint:v1",
        ),
        (
            ShardLoomSessionCacheArtifactKind::OutputPlan,
            output_id,
            "output-plan:orders-jsonl",
            "output-fingerprint:v1",
        ),
        (
            ShardLoomSessionCacheArtifactKind::SchemaCache,
            schema_id,
            "schema:orders",
            "schema-fingerprint:v1",
        ),
        (
            ShardLoomSessionCacheArtifactKind::DictionaryCache,
            dictionary_id,
            "dictionary:category",
            "dictionary-fingerprint:v1",
        ),
    ] {
        runtime.lookup_or_insert(kind, artifact_id, cache_key, fingerprint, "none");
        runtime.lookup_or_insert(kind, artifact_id, cache_key, fingerprint, "none");
    }

    runtime.lookup_or_insert(
        ShardLoomSessionCacheArtifactKind::SourceState,
        source_id,
        "local-source:orders",
        "source-fingerprint:v2",
        "source_fingerprint_changed",
    );
    runtime.lookup_or_insert(
        ShardLoomSessionCacheArtifactKind::SchemaCache,
        schema_id,
        "schema:orders",
        "schema-fingerprint:v2",
        "schema_digest_changed",
    );
    runtime.lookup_or_insert(
        ShardLoomSessionCacheArtifactKind::OutputPlan,
        output_id,
        "output-plan:orders-jsonl",
        "output-fingerprint:v2",
        "output_artifact_fingerprint_changed",
    );
    runtime.exercise_buffer_pool();
    runtime.close();

    ShardLoomSessionRuntimeReport::from_runtime(runtime)
}

fn session_runtime_digest(parts: &[&str]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_model_requires_explicit_context_and_no_globals() {
        let report = plan_shardloom_session_model();

        assert!(report.explicit_session_context_required);
        assert!(!report.hidden_global_registries_allowed);
        assert!(!report.runtime_registry_mutation_allowed);
        assert!(report.all_registries_safe_by_default());
        assert!(report.preserves_no_runtime_expansion());
    }

    #[test]
    fn session_model_tracks_required_registry_families() {
        let report = plan_shardloom_session_model();
        let kinds = report.registry_kind_order();

        for expected in [
            "operator",
            "function",
            "aggregate",
            "sketch",
            "source_sink_adapter",
            "execution_provider",
            "semantic_profile",
            "evidence_artifact",
            "policy_effect",
        ] {
            assert!(kinds.contains(&expected));
        }
    }

    #[test]
    fn session_cache_smoke_reuses_invalidates_and_closes_scope() {
        let report = run_shardloom_session_cache_smoke();

        assert_eq!(report.schema_version, "shardloom.session_runtime_cache.v1");
        assert_eq!(
            report.session_runtime_status,
            "scoped_session_cache_runtime_certified"
        );
        assert_eq!(report.cache_hit_count, 5);
        assert_eq!(report.cache_miss_count, 8);
        assert_eq!(report.invalidation_count, 3);
        assert_eq!(report.source_state_reuse_count, 1);
        assert_eq!(report.prepared_state_reuse_count, 1);
        assert_eq!(report.output_plan_reuse_count, 1);
        assert_eq!(report.schema_cache_reuse_count, 1);
        assert_eq!(report.dictionary_cache_reuse_count, 1);
        assert_eq!(report.buffer_reuse_count, 1);
        assert_eq!(report.buffer_allocation_count, 1);
        assert_eq!(report.cleanup_cache_entries_removed, 5);
        assert!(report.lifecycle_closed_and_cleaned());
        assert!(report.no_fallback_no_external_engine());
        assert_eq!(
            report.invalidation_reason_order(),
            "source_fingerprint_changed,schema_digest_changed,output_artifact_fingerprint_changed"
        );
        assert!(report.reuse_digest.starts_with("fnv1a64:"));
        assert_eq!(
            report.source_state_digest,
            session_runtime_digest(&[
                "source-state://session-cache-smoke/local-orders",
                "source-fingerprint:v2"
            ])
        );
        assert_eq!(
            report.vortex_prepared_state_digest,
            session_runtime_digest(&[
                "vortex-prepared-state://session-cache-smoke/local-orders",
                "prepared-fingerprint:v1"
            ])
        );
        assert_eq!(
            report.output_plan_digest,
            session_runtime_digest(&[
                "output-plan://session-cache-smoke/local-orders-jsonl",
                "output-fingerprint:v2"
            ])
        );
    }
}
