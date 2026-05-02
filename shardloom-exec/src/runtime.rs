use shardloom_core::{
    ByteRange, ColumnRef, DatasetRef, DatasetUri, Diagnostic, DiagnosticCode,
    MaterializationPolicy, Result, SegmentId, ShardLoomError, UriScheme,
};

/// Planning-time object store classification inferred from dataset URI schemes.
///
/// This is a reference-only type and never performs object-store or filesystem IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreKind {
    LocalFileSystem,
    FileUri,
    S3,
    Gcs,
    Adls,
    Other,
    Unknown,
}

impl ObjectStoreKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalFileSystem => "local_filesystem",
            Self::FileUri => "file_uri",
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::Adls => "adls",
            Self::Other => "other",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn from_uri_scheme(scheme: UriScheme) -> Self {
        match scheme {
            UriScheme::LocalPath => Self::LocalFileSystem,
            UriScheme::File => Self::FileUri,
            UriScheme::S3 => Self::S3,
            UriScheme::Gcs => Self::Gcs,
            UriScheme::Adls => Self::Adls,
            UriScheme::Other => Self::Other,
        }
    }
}

/// Planning reference to an object-store root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStoreRef {
    pub kind: ObjectStoreKind,
    pub root: DatasetUri,
}
impl ObjectStoreRef {
    #[must_use]
    pub fn new(root: DatasetUri) -> Self {
        let kind = ObjectStoreKind::from_uri_scheme(root.scheme());
        Self { kind, root }
    }
    #[must_use]
    pub const fn kind(&self) -> ObjectStoreKind {
        self.kind
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "object_store.kind={} root={}",
            self.kind.as_str(),
            self.root.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPolicy {
    MetadataOnly,
    ByteRangePreferred,
    FullReadAllowed,
    FullReadDenied,
}
impl ReadPolicy {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::ByteRangePreferred => "byte_range_preferred",
            Self::FullReadAllowed => "full_read_allowed",
            Self::FullReadDenied => "full_read_denied",
        }
    }
    #[must_use]
    pub const fn allows_full_read(&self) -> bool {
        matches!(self, Self::FullReadAllowed)
    }
}

/// Planning-only byte-range read request; does not execute reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRangeRequest {
    pub uri: DatasetUri,
    pub range: ByteRange,
    pub policy: ReadPolicy,
}
impl ByteRangeRequest {
    #[must_use]
    pub fn new(uri: DatasetUri, range: ByteRange) -> Self {
        Self {
            uri,
            range,
            policy: ReadPolicy::ByteRangePreferred,
        }
    }
    #[must_use]
    pub const fn with_policy(mut self, policy: ReadPolicy) -> Self {
        self.policy = policy;
        self
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "uri={} range=[{}, {}) policy={}",
            self.uri.as_str(),
            self.range.start,
            self.range.end_exclusive(),
            self.policy.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceBudget {
    pub max_memory_bytes: Option<u64>,
    pub max_disk_spill_bytes: Option<u64>,
    pub max_object_store_requests: Option<u64>,
    pub max_runtime_millis: Option<u64>,
}
impl ResourceBudget {
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            max_memory_bytes: None,
            max_disk_spill_bytes: None,
            max_object_store_requests: None,
            max_runtime_millis: None,
        }
    }
    #[must_use]
    pub const fn memory_limited(max_memory_bytes: u64) -> Self {
        Self {
            max_memory_bytes: Some(max_memory_bytes),
            ..Self::unbounded()
        }
    }
    #[must_use]
    pub const fn with_spill_limit(mut self, max_disk_spill_bytes: u64) -> Self {
        self.max_disk_spill_bytes = Some(max_disk_spill_bytes);
        self
    }
    #[must_use]
    pub const fn with_object_store_request_limit(mut self, max_requests: u64) -> Self {
        self.max_object_store_requests = Some(max_requests);
        self
    }
    #[must_use]
    pub const fn with_runtime_limit(mut self, max_runtime_millis: u64) -> Self {
        self.max_runtime_millis = Some(max_runtime_millis);
        self
    }
    #[must_use]
    pub const fn has_any_limit(&self) -> bool {
        self.max_memory_bytes.is_some()
            || self.max_disk_spill_bytes.is_some()
            || self.max_object_store_requests.is_some()
            || self.max_runtime_millis.is_some()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "memory={:?} spill={:?} object_store_requests={:?} runtime_ms={:?}",
            self.max_memory_bytes,
            self.max_disk_spill_bytes,
            self.max_object_store_requests,
            self.max_runtime_millis
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub retry_reads: bool,
    pub retry_writes: bool,
}
impl RetryPolicy {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            retry_reads: false,
            retry_writes: false,
        }
    }
    #[must_use]
    pub const fn default_read_retries() -> Self {
        Self {
            max_attempts: 3,
            retry_reads: true,
            retry_writes: false,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "attempts={} retry_reads={} retry_writes={}",
            self.max_attempts, self.retry_reads, self.retry_writes
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId(String);
impl TaskId {
    /// Creates a validated task identifier.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when value is empty or whitespace-only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "task id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    MetadataRead,
    SegmentScan,
    SegmentPrune,
    EncodedEvaluate,
    PartialDecode,
    Aggregate,
    Join,
    Repartition,
    WriteOutput,
    Commit,
    Unsupported,
}
impl TaskKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataRead => "metadata_read",
            Self::SegmentScan => "segment_scan",
            Self::SegmentPrune => "segment_prune",
            Self::EncodedEvaluate => "encoded_evaluate",
            Self::PartialDecode => "partial_decode",
            Self::Aggregate => "aggregate",
            Self::Join => "join",
            Self::Repartition => "repartition",
            Self::WriteOutput => "write_output",
            Self::Commit => "commit",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Planned,
    NotStarted,
    Running,
    Completed,
    Failed,
    ExecutionNotImplemented,
    Unsupported,
}
impl TaskStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::NotStarted => "not_started",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::ExecutionNotImplemented => "execution_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::ExecutionNotImplemented | Self::Unsupported
        )
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::Failed | Self::ExecutionNotImplemented | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShuffleRequirement {
    None,
    Avoided { reason: String },
    Required { reason: String },
    Unsupported { reason: String },
}
impl ShuffleRequirement {
    #[must_use]
    pub const fn requires_shuffle(&self) -> bool {
        matches!(self, Self::Required { .. })
    }
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Avoided { reason } | Self::Required { reason } | Self::Unsupported { reason } => {
                Some(reason)
            }
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            Self::None => "shuffle=none".to_string(),
            Self::Avoided { reason } => format!("shuffle=avoided reason={reason}"),
            Self::Required { reason } => format!("shuffle=required reason={reason}"),
            Self::Unsupported { reason } => format!("shuffle=unsupported reason={reason}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentTask {
    pub id: TaskId,
    pub kind: TaskKind,
    pub segments: Vec<SegmentId>,
    pub required_columns: Vec<ColumnRef>,
    pub byte_ranges: Vec<ByteRangeRequest>,
    pub materialization: MaterializationPolicy,
    pub resource_budget: ResourceBudget,
    pub retry_policy: RetryPolicy,
    pub status: TaskStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl SegmentTask {
    #[must_use]
    pub fn new(id: TaskId, kind: TaskKind) -> Self {
        Self {
            id,
            kind,
            segments: vec![],
            required_columns: vec![],
            byte_ranges: vec![],
            materialization: MaterializationPolicy::Late,
            resource_budget: ResourceBudget::unbounded(),
            retry_policy: RetryPolicy::none(),
            status: TaskStatus::Planned,
            diagnostics: vec![],
        }
    }
    pub fn add_segment(&mut self, segment: SegmentId) {
        self.segments.push(segment);
    }
    pub fn add_required_column(&mut self, column: ColumnRef) {
        self.required_columns.push(column);
    }
    pub fn add_byte_range(&mut self, request: ByteRangeRequest) {
        self.byte_ranges.push(request);
    }
    #[must_use]
    pub fn with_materialization(mut self, policy: MaterializationPolicy) -> Self {
        self.materialization = policy;
        self
    }
    #[must_use]
    pub const fn with_resource_budget(mut self, budget: ResourceBudget) -> Self {
        self.resource_budget = budget;
        self
    }
    #[must_use]
    pub const fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }
    #[must_use]
    pub fn execution_not_implemented(id: TaskId, kind: TaskKind) -> Self {
        let mut s = Self::new(id, kind);
        s.status = TaskStatus::ExecutionNotImplemented;
        s.diagnostics.push(Diagnostic::no_fallback_execution("Object-store runtime task execution is not implemented yet. Fallback execution was not attempted and Spark/DataFusion/etc. are not fallback engines."));
        s
    }
    #[must_use]
    pub fn unsupported(
        id: TaskId,
        kind: TaskKind,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let mut s = Self::new(id, kind);
        s.status = TaskStatus::Unsupported;
        s.diagnostics.push(Diagnostic::unsupported(DiagnosticCode::ObjectStoreUnsupported, feature, format!("Unsupported runtime task feature: {reason}. Fallback execution was not attempted and Spark/DataFusion/etc. are not fallback engines."), Some("Use a supported planning-only path while native runtime support is implemented.".to_string())));
        s
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "task={} kind={} status={} segments={} ranges={} fallback_execution=disabled",
            self.id.as_str(),
            self.kind.as_str(),
            self.status.as_str(),
            self.segments.len(),
            self.byte_ranges.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskGraph {
    pub tasks: Vec<SegmentTask>,
    pub shuffle: ShuffleRequirement,
    pub diagnostics: Vec<Diagnostic>,
}
impl TaskGraph {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tasks: vec![],
            shuffle: ShuffleRequirement::None,
            diagnostics: vec![],
        }
    }
    pub fn add_task(&mut self, task: SegmentTask) {
        self.tasks.push(task);
    }
    #[must_use]
    pub fn with_shuffle(mut self, shuffle: ShuffleRequirement) -> Self {
        self.shuffle = shuffle;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.tasks.iter().any(SegmentTask::has_errors)
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub const fn requires_shuffle(&self) -> bool {
        self.shuffle.requires_shuffle()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "tasks={} {} fallback_execution=disabled",
            self.task_count(),
            self.shuffle.summary()
        )
    }
}

impl Default for TaskGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimePlanningStatus {
    Planned,
    ExecutionNotImplemented,
    DistributedNotImplemented,
    Unsupported,
}
impl RuntimePlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::ExecutionNotImplemented => "execution_not_implemented",
            Self::DistributedNotImplemented => "distributed_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimePlanSkeleton {
    pub graph: TaskGraph,
    pub status: RuntimePlanningStatus,
    pub object_store: Option<ObjectStoreRef>,
    pub diagnostics: Vec<Diagnostic>,
}
impl RuntimePlanSkeleton {
    #[must_use]
    pub fn planned(graph: TaskGraph) -> Self {
        Self {
            graph,
            status: RuntimePlanningStatus::Planned,
            object_store: None,
            diagnostics: vec![],
        }
    }
    /// Builds a planning-only runtime skeleton from a dataset reference.
    ///
    /// # Errors
    /// Propagates errors when constructing internal validated identifiers.
    #[allow(clippy::needless_pass_by_value)]
    pub fn for_dataset(dataset: DatasetRef) -> Result<Self> {
        let object_store = ObjectStoreRef::new(dataset.uri.clone());
        let mut graph = TaskGraph::new();
        let task = SegmentTask::new(TaskId::new("metadata-read-0")?, TaskKind::MetadataRead);
        graph.add_task(task);
        Ok(Self {
            graph,
            status: RuntimePlanningStatus::Planned,
            object_store: Some(object_store),
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn execution_not_implemented(graph: TaskGraph) -> Self {
        Self {
            graph,
            status: RuntimePlanningStatus::ExecutionNotImplemented,
            object_store: None,
            diagnostics: vec![Diagnostic::no_fallback_execution(
                "Object-store runtime execution is not implemented yet. Fallback execution was not attempted and Spark/DataFusion/etc. are not fallback engines.",
            )],
        }
    }
    #[must_use]
    pub fn distributed_not_implemented(graph: TaskGraph) -> Self {
        Self {
            graph,
            status: RuntimePlanningStatus::DistributedNotImplemented,
            object_store: None,
            diagnostics: vec![Diagnostic::no_fallback_execution(
                "Distributed runtime execution is not implemented yet. Fallback execution was not attempted and Spark/DataFusion/etc. are not fallback engines.",
            )],
        }
    }
    #[must_use]
    pub fn unsupported(
        graph: TaskGraph,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            graph,
            status: RuntimePlanningStatus::Unsupported,
            object_store: None,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::ObjectStoreUnsupported,
                feature,
                format!(
                    "Unsupported runtime planning feature: {}. Fallback execution was not attempted and Spark/DataFusion/etc. are not fallback engines.",
                    reason.into()
                ),
                Some("Use currently supported planning-only runtime skeleton paths.".to_string()),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.graph.has_errors()
            || self
                .diagnostics
                .iter()
                .any(|d| d.severity.as_str() == "error" || d.severity.as_str() == "fatal")
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "runtime_plan.status={}\nobject_store.kind={}\ntask_count={}\nshuffle={}\nfallback_execution=disabled\ndiagnostics={}\n{}",
            self.status.as_str(),
            self.object_store
                .as_ref()
                .map_or("unknown", |o| o.kind.as_str()),
            self.graph.task_count(),
            self.graph.shuffle.summary(),
            self.diagnostics.len(),
            self.diagnostics
                .iter()
                .map(Diagnostic::to_human_text)
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_store_kind_local_path() {
        assert_eq!(
            ObjectStoreKind::from_uri_scheme(UriScheme::LocalPath),
            ObjectStoreKind::LocalFileSystem
        );
    }
    #[test]
    fn object_store_kind_s3() {
        assert_eq!(
            ObjectStoreKind::from_uri_scheme(UriScheme::S3),
            ObjectStoreKind::S3
        );
    }
    #[test]
    fn object_store_ref_infers_kind() {
        let uri = DatasetUri::new("s3://bucket/table").unwrap();
        let r = ObjectStoreRef::new(uri);
        assert_eq!(r.kind(), ObjectStoreKind::S3);
    }
    #[test]
    fn read_policy_full_allowed() {
        assert!(ReadPolicy::FullReadAllowed.allows_full_read());
    }
    #[test]
    fn read_policy_range_not_full() {
        assert!(!ReadPolicy::ByteRangePreferred.allows_full_read());
    }
    #[test]
    fn byte_range_default_policy() {
        let r = ByteRangeRequest::new(
            DatasetUri::new("file:///tmp/a").unwrap(),
            ByteRange::new(0, 10),
        );
        assert_eq!(r.policy, ReadPolicy::ByteRangePreferred);
    }
    #[test]
    fn resource_budget_unbounded() {
        let b = ResourceBudget::unbounded();
        assert!(!b.has_any_limit());
    }
    #[test]
    fn resource_budget_memory_limited() {
        let b = ResourceBudget::memory_limited(10);
        assert!(b.has_any_limit());
        assert_eq!(b.max_memory_bytes, Some(10));
    }
    #[test]
    fn retry_none_one_attempt() {
        assert_eq!(RetryPolicy::none().max_attempts, 1);
    }
    #[test]
    fn retry_default_reads() {
        let p = RetryPolicy::default_read_retries();
        assert_eq!(p.max_attempts, 3);
        assert!(p.retry_reads);
    }
    #[test]
    fn task_id_rejects_empty() {
        assert!(TaskId::new("   ").is_err());
    }
    #[test]
    fn task_status_terminal_and_error() {
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::ExecutionNotImplemented.is_error());
        assert!(!TaskStatus::Running.is_terminal());
    }
    #[test]
    fn shuffle_required_requires_shuffle() {
        assert!(ShuffleRequirement::Required { reason: "r".into() }.requires_shuffle());
    }
    #[test]
    fn shuffle_none_no_shuffle() {
        assert!(!ShuffleRequirement::None.requires_shuffle());
    }
    #[test]
    fn segment_task_new_defaults_planned() {
        let t = SegmentTask::new(TaskId::new("t1").unwrap(), TaskKind::MetadataRead);
        assert_eq!(t.status, TaskStatus::Planned);
    }
    #[test]
    fn segment_task_execution_not_implemented_has_errors() {
        let t = SegmentTask::execution_not_implemented(
            TaskId::new("t1").unwrap(),
            TaskKind::MetadataRead,
        );
        assert!(t.has_errors());
    }
    #[test]
    fn segment_task_unsupported_has_errors() {
        let t =
            SegmentTask::unsupported(TaskId::new("t1").unwrap(), TaskKind::Unsupported, "f", "r");
        assert!(t.has_errors());
    }
    #[test]
    fn task_graph_counts_tasks() {
        let mut g = TaskGraph::new();
        g.add_task(SegmentTask::new(
            TaskId::new("t1").unwrap(),
            TaskKind::MetadataRead,
        ));
        assert_eq!(g.task_count(), 1);
    }
    #[test]
    fn task_graph_has_errors_in_task() {
        let mut g = TaskGraph::new();
        g.add_task(SegmentTask::execution_not_implemented(
            TaskId::new("t1").unwrap(),
            TaskKind::MetadataRead,
        ));
        assert!(g.has_errors());
    }
    #[test]
    fn runtime_for_dataset_planned() {
        let ds =
            DatasetRef::from_uri(DatasetUri::new("s3://bucket/table.vortex").unwrap()).unwrap();
        let p = RuntimePlanSkeleton::for_dataset(ds).unwrap();
        assert_eq!(p.status, RuntimePlanningStatus::Planned);
        assert_eq!(p.graph.task_count(), 1);
    }
    #[test]
    fn runtime_execution_not_implemented_has_errors() {
        let p = RuntimePlanSkeleton::execution_not_implemented(TaskGraph::new());
        assert!(p.has_errors());
    }
    #[test]
    fn runtime_human_text_mentions_fallback_disabled() {
        let p = RuntimePlanSkeleton::execution_not_implemented(TaskGraph::new());
        assert!(p.to_human_text().contains("fallback_execution=disabled"));
    }
}
