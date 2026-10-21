//! Once-per-execution producers and explicitly retained native source replay.

use super::{
    ArrayRef, BATCH_ROWS, CancellationToken, CollectedVortexRelational, ExecutedVortexRelational,
    Metrics, NativeExecutionContext, PreparedVortexRelational, ReservedVec, ResidentMemorySource,
    ResidentVortexSession, Result, VortexRelationalPlan,
};
use std::cell::{Cell, RefCell};

use super::replay;

pub(super) struct Source {
    pub(super) uri: shardloom_core::DatasetUri,
    pub(super) schema: ResidentMemorySource,
    pub(super) usage: Usage,
}

/// Requirements derived from statically visible native relations before demand.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Usage {
    uses: usize,
    replay: ReplayRequirement,
    detach_before_consume: bool,
    validate_if_unread: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
enum ReplayRequirement {
    #[default]
    None,
    Record,
    Prefill,
}

impl Usage {
    pub(super) fn replay() -> Self {
        Self {
            replay: ReplayRequirement::Record,
            ..Self::default()
        }
    }

    pub(super) fn requires_replay(self) -> bool {
        self.replay != ReplayRequirement::None
    }

    /// Repeated binding of a subtree adds requirements, not source occurrences.
    pub(super) fn extend(&mut self, other: Self) {
        self.uses = self.uses.max(other.uses);
        self.replay = self.replay.max(other.replay);
        self.detach_before_consume |= other.detach_before_consume;
        self.validate_if_unread |= other.validate_if_unread;
    }

    fn combine(mut self, other: Self) -> Self {
        let uses = (self.uses + other.uses).min(2);
        self.extend(other);
        self.uses = uses;
        if uses > 1 {
            self.replay = self.replay.max(ReplayRequirement::Record);
        }
        self
    }
}

type ColumnProvider<'a> =
    dyn FnMut(&ResidentVortexSession) -> Result<Option<ResidentMemorySource>> + 'a;

type ColumnsBySource<'a> = dyn FnMut(
        &shardloom_core::DatasetUri,
        u64,
        &ResidentVortexSession,
    ) -> Result<Option<ResidentMemorySource>>
    + 'a;

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
type ArrowProvider<'a> = dyn FnMut() -> Result<Option<arrow_array::RecordBatch>> + 'a;

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
type ArrowSources<'a> =
    dyn FnMut(&shardloom_core::DatasetUri, u64) -> Result<Option<arrow_array::RecordBatch>> + 'a;

pub(super) enum Provider<'a> {
    Columns(&'a mut ColumnProvider<'a>),
    ColumnsBySource(&'a mut ColumnsBySource<'a>),
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    Arrow(&'a mut ArrowProvider<'a>),
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    ArrowSources(&'a mut ArrowSources<'a>),
}

impl Provider<'_> {
    fn next(
        &mut self,
        uri: &shardloom_core::DatasetUri,
        ordinal: u64,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ResidentMemorySource>> {
        context.check_cancelled()?;
        match self {
            Self::Columns(input) => input(session),
            Self::ColumnsBySource(input) => input(uri, ordinal, session),
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
            Self::Arrow(input) => input()?
                .map(|batch| {
                    ResidentMemorySource::from_arrow_in_context(session, batch, context, true)
                })
                .transpose(),
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
            Self::ArrowSources(input) => input(uri, ordinal)?
                .map(|batch| {
                    ResidentMemorySource::from_arrow_in_context(session, batch, context, true)
                })
                .transpose(),
        }
    }

    fn has_transport_bounds(&self) -> bool {
        matches!(self, Self::Columns(_) | Self::ColumnsBySource(_))
    }
}

/// Successful completion evidence for finite native batch sources.
/// Logical bytes describe input values/offsets/validity/names, not capacity or RSS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedVortexBatchInput {
    /// Number of distinct declared sources that reached their own explicit end.
    pub sources: usize,
    pub payload_batches: u64,
    pub rows: u64,
    pub input_logical_bytes: u64,
    pub intake_payload_bytes_copied: u64,
    /// Referenced Arrow buffer bytes copied into the shared native allocation.
    /// Caller storage and metadata are excluded; this is not process RSS.
    pub intake_columnar_buffer_bytes_copied: u64,
    pub max_batch_rows: usize,
    pub max_retained_input_batches: usize,
    pub max_retained_input_logical_bytes: u64,
    pub end_of_input_observed: bool,
    /// Every delivered native batch owns compact output independently of input.
    pub output_ownership_detached: bool,
    /// Compact copies at ordering retention boundaries, including nested sorts.
    pub ordering_batches_detached: u64,
    /// Counts rows at each ordering boundary, not distinct source rows or bytes.
    pub ordering_rows_detached: u64,
    /// Build batches compacted by joins in a streamed execution. Counts each
    /// retention boundary, including ordinary sources in the same plan.
    pub join_build_batches_detached: u64,
    /// Rows copied at those build boundaries, not distinct source rows or bytes.
    pub join_build_rows_detached: u64,
    /// Nonempty batches compacted at window retention boundaries in a streamed
    /// plan, including ordinary sources and repeated composed window stages.
    pub window_batches_detached: u64,
    /// Rows copied at those boundaries, not distinct source rows or bytes.
    pub window_rows_detached: u64,
    /// Source batches copied and released before a nested subquery demand.
    pub subquery_outer_batches_detached: u64,
    pub subquery_outer_rows_detached: u64,
    /// Nonempty batches compacted at subquery state retention boundaries.
    pub subquery_build_batches_detached: u64,
    pub subquery_build_rows_detached: u64,
    /// Conditionally unused sources drained without evaluating query operators.
    pub subquery_validation_sources: u64,
    pub subquery_validation_batches: u64,
    pub subquery_validation_rows: u64,
    /// Sources retained for reuse, independently of original producer owners.
    pub replay_sources: u64,
    /// Scans of completed replay. Excludes direct consumption during recording,
    /// but includes the first query read when raw replay was completed first.
    pub replay_passes: u64,
    pub replay_rows: u64,
    /// Compact copies into native replay ownership before releasing each input.
    pub replay_batches_detached: u64,
    pub replay_rows_detached: u64,
    /// Native buffer bytes retained by initial replay copies, not capacity/RSS
    /// or subsequent spill serialization and operator copy traffic.
    pub replay_buffer_bytes_copied: u64,
    /// Sources whose completed replay owner is an actual native spill run.
    pub replay_spilled_sources: u64,
    /// Raw sources completed into replay before their first query consumption.
    pub replay_prefilled_sources: u64,
}

impl Default for ExecutedVortexBatchInput {
    fn default() -> Self {
        Self {
            sources: 0,
            payload_batches: 0,
            rows: 0,
            input_logical_bytes: 0,
            intake_payload_bytes_copied: 0,
            intake_columnar_buffer_bytes_copied: 0,
            max_batch_rows: 0,
            max_retained_input_batches: 0,
            max_retained_input_logical_bytes: 0,
            end_of_input_observed: false,
            output_ownership_detached: true,
            ordering_batches_detached: 0,
            ordering_rows_detached: 0,
            join_build_batches_detached: 0,
            join_build_rows_detached: 0,
            window_batches_detached: 0,
            window_rows_detached: 0,
            subquery_outer_batches_detached: 0,
            subquery_outer_rows_detached: 0,
            subquery_build_batches_detached: 0,
            subquery_build_rows_detached: 0,
            subquery_validation_sources: 0,
            subquery_validation_batches: 0,
            subquery_validation_rows: 0,
            replay_sources: 0,
            replay_passes: 0,
            replay_rows: 0,
            replay_batches_detached: 0,
            replay_rows_detached: 0,
            replay_buffer_bytes_copied: 0,
            replay_spilled_sources: 0,
            replay_prefilled_sources: 0,
        }
    }
}

/// An execution-scoped input provider. Each terminal method consumes this adapter;
/// the prepared plan retains schema and reuse declarations, never input payload.
pub struct VortexRelationalBatchInput<'p, 'i> {
    prepared: &'p PreparedVortexRelational,
    input: Provider<'i>,
}

impl PreparedVortexRelational {
    /// Attach one source to an already bound plan with one single-use batch URI. Each call
    /// must transfer a private `from_batch_columns` source from the supplied
    /// session; `None` proves end-of-input. The callback must not reenter query
    /// admission. Its errors prevent a successful final report/publication.
    /// # Errors
    /// Rejects a provider unless the plan declares exactly one streaming source.
    pub fn with_batch_input<'p, 'i>(
        &'p self,
        input: &'i mut ColumnProvider<'i>,
    ) -> Result<VortexRelationalBatchInput<'p, 'i>> {
        self.validate_single_batch_provider()?;
        Ok(VortexRelationalBatchInput {
            prepared: self,
            input: Provider::Columns(input),
        })
    }

    /// Attach a provider for all declared streaming sources. Demand identifies
    /// the source URI and its zero-based batch ordinal; each source starts at
    /// zero and must independently return `None` after its last batch. A source
    /// is consumed once per execution. Ownership and callback restrictions are
    /// the same as for [`Self::with_batch_input`].
    /// # Errors
    /// Rejects a provider when the plan has no streaming source declarations.
    pub fn with_batch_inputs<'p, 'i>(
        &'p self,
        input: &'i mut ColumnsBySource<'i>,
    ) -> Result<VortexRelationalBatchInput<'p, 'i>> {
        self.validate_batch_provider(true)?;
        Ok(VortexRelationalBatchInput {
            prepared: self,
            input: Provider::ColumnsBySource(input),
        })
    }

    /// Import one caller-owned Arrow batch for each native source demand. The
    /// exact declared schema is required even for empty batches. Copies and
    /// private ownership use the existing shared grant without row/JSON conversion.
    /// Declared repeated scans use the same credited native replay as column inputs.
    /// The producer's batch must fit
    /// the available allocation, but has no synthetic row or logical-byte cap.
    /// `None` is required to certify complete input. Producer callbacks must not
    /// reenter this session while its execution is active.
    /// # Errors
    /// Rejects anything other than one streaming declaration before demand.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn with_arrow_batch_input<'p, 'i>(
        &'p self,
        input: &'i mut ArrowProvider<'i>,
    ) -> Result<VortexRelationalBatchInput<'p, 'i>> {
        self.validate_single_batch_provider()?;
        Ok(VortexRelationalBatchInput {
            prepared: self,
            input: Provider::Arrow(input),
        })
    }

    /// Import caller-owned Arrow batches by source URI and zero-based ordinal.
    /// Each declared source must independently reach `None`; repeated scans use
    /// native replay rather than reopening the producer. Schema, ownership,
    /// allocation and callback rules match [`Self::with_arrow_batch_input`].
    /// # Errors
    /// Rejects a provider when the plan has no streaming source declarations.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn with_arrow_batch_inputs<'p, 'i>(
        &'p self,
        input: &'i mut ArrowSources<'i>,
    ) -> Result<VortexRelationalBatchInput<'p, 'i>> {
        self.validate_batch_provider(true)?;
        Ok(VortexRelationalBatchInput {
            prepared: self,
            input: Provider::ArrowSources(input),
        })
    }

    pub(super) fn validate_batch_provider(&self, supplied: bool) -> Result<()> {
        if self.batch_sources.is_empty() == supplied {
            return Err(failed(if supplied {
                "an input provider requires a declared streaming batch source"
            } else {
                "streaming input requires with_batch_input for this execution"
            }));
        }
        Ok(())
    }

    fn validate_single_batch_provider(&self) -> Result<()> {
        self.validate_batch_provider(true)?;
        if self.batch_sources.len() != 1 {
            return Err(failed(
                "this provider requires exactly one streaming source; use with_batch_inputs for multiple sources",
            ));
        }
        Ok(())
    }
}

/// A borrowed execution-scoped source, erased only to keep provider lifetimes
/// out of schema binding. It carries no alternate array representation.
pub(super) trait Input {
    fn require_usage(&self, index: usize, usage: Usage) -> Result<()>;

    fn run(
        &self,
        index: usize,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        consume: &mut dyn FnMut(&ArrayRef) -> Result<()>,
    ) -> Result<()>;

    fn completed(
        &self,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
    ) -> Result<ExecutedVortexBatchInput>;
}

pub(super) struct Execution<'p, 'i> {
    prepared: &'p PreparedVortexRelational,
    provider: RefCell<Provider<'i>>,
    // The provider borrow ends after each request. This separate lease prevents
    // nested demand from retaining one original producer batch while opening another.
    original_owner: RefCell<()>,
    states: ReservedVec<SourceState>,
    report: RefCell<ExecutedVortexBatchInput>,
}

struct SourceState {
    started: Cell<bool>,
    ended: Cell<bool>,
    next_batch: Cell<u64>,
    usage: Cell<Usage>,
    replay: RefCell<Option<replay::Completed>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Consumption {
    Query,
    Prefill,
    Validate,
}

impl<'p, 'i> Execution<'p, 'i> {
    pub(super) fn new(
        prepared: &'p PreparedVortexRelational,
        provider: Provider<'i>,
    ) -> Result<Self> {
        let mut states = ReservedVec::new(prepared.session.memory())?;
        states.reserve(prepared.batch_sources.len())?;
        for source in &prepared.batch_sources {
            states.values.push(SourceState {
                started: Cell::new(false),
                ended: Cell::new(false),
                next_batch: Cell::new(0),
                usage: Cell::new(source.usage),
                replay: RefCell::new(None),
            });
        }
        Ok(Self {
            prepared,
            provider: RefCell::new(provider),
            original_owner: RefCell::new(()),
            states,
            report: RefCell::new(ExecutedVortexBatchInput::default()),
        })
    }

    fn record_input(&self, source: &ResidentMemorySource) -> Result<()> {
        let rows = source.row_count();
        let bytes = source.input_logical_bytes() as u64;
        let mut report = self.report.borrow_mut();
        checked_add(&mut report.payload_batches, 1)?;
        checked_add(&mut report.rows, rows as u64)?;
        checked_add(&mut report.input_logical_bytes, bytes)?;
        checked_add(
            &mut report.intake_payload_bytes_copied,
            source.intake_payload_bytes_copied(),
        )?;
        checked_add(
            &mut report.intake_columnar_buffer_bytes_copied,
            source.intake_columnar_buffer_bytes_copied(),
        )?;
        report.max_batch_rows = report.max_batch_rows.max(rows);
        report.max_retained_input_batches = 1;
        report.max_retained_input_logical_bytes =
            report.max_retained_input_logical_bytes.max(bytes);
        Ok(())
    }

    fn consume_replay(
        &self,
        state: &SourceState,
        declaration: &Source,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        consume: &mut dyn FnMut(&ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if !state.ended.get() || !state.usage.get().requires_replay() {
            return Err(failed(
                "source reuse must be declared before its first input demand and requires completed input",
            ));
        }
        let replay = state
            .replay
            .try_borrow()
            .map_err(|_| failed("streaming replay is already active"))?;
        let replay = replay
            .as_ref()
            .ok_or_else(|| failed("completed streaming replay is absent"))?;
        let mut rows = 0;
        replay.run(declaration.schema.dtype(), metrics, context, &mut |array| {
            consume(array)?;
            checked_add(&mut rows, array.len() as u64)
        })?;
        let mut report = self.report.borrow_mut();
        checked_add(&mut report.replay_passes, 1)?;
        checked_add(&mut report.replay_rows, rows)
    }
}

impl Execution<'_, '_> {
    fn validate_input(
        &self,
        declaration: &Source,
        source: &ResidentMemorySource,
        transport_bounds: bool,
    ) -> Result<()> {
        if !source.belongs_to_session(&self.prepared.session)
            || source.dtype() != declaration.schema.dtype()
            || (transport_bounds
                && (source.row_count() > crate::resident_memory_source::MAX_BATCH_ROWS
                    || source.input_logical_bytes() > 32 * 1024 * 1024))
        {
            return Err(failed(
                "input changed its declared schema, session or finite batch bounds",
            ));
        }
        Ok(())
    }

    fn finish_producer(
        &self,
        state: &SourceState,
        replay: Option<replay::Completed>,
        consumption: Consumption,
    ) -> Result<()> {
        let mut report = self.report.borrow_mut();
        if let Some(replay) = replay {
            checked_add(&mut report.replay_sources, 1)?;
            checked_add(
                &mut report.replay_spilled_sources,
                u64::from(replay.is_spilled()),
            )?;
            *state.replay.borrow_mut() = Some(replay);
        }
        match consumption {
            Consumption::Query => {}
            Consumption::Prefill => {
                checked_add(&mut report.replay_prefilled_sources, 1)?;
            }
            Consumption::Validate => {
                checked_add(&mut report.subquery_validation_sources, 1)?;
            }
        }
        state.ended.set(true);
        Ok(())
    }

    fn run_producer(
        &self,
        index: usize,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        consumption: Consumption,
        consume: &mut dyn FnMut(&ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let state = self
            .states
            .values
            .get(index)
            .ok_or_else(|| failed("streaming input source index is absent"))?;
        let declaration = self
            .prepared
            .batch_sources
            .get(index)
            .ok_or_else(|| failed("streaming input declaration is absent"))?;
        context.check_cancelled()?;
        if state.started.replace(true) {
            return Err(failed("a streaming producer cannot be opened twice"));
        }
        let usage = state.usage.get();
        let schema = (usage.requires_replay() && consumption != Consumption::Validate)
            .then(|| replay::Schema::new(declaration.schema.dtype(), context))
            .transpose()?;
        let mut recorder = schema
            .as_ref()
            .map(|schema| replay::Recorder::new(schema, metrics, context))
            .transpose()?;
        loop {
            context.check_cancelled()?;
            let owner = self.original_owner.try_borrow_mut().map_err(|_| {
                failed("nested input demand requires releasing the original source batch first")
            })?;
            let (next, transport_bounds) = {
                let mut input = self
                    .provider
                    .try_borrow_mut()
                    .map_err(|_| failed("streaming input provider is already active"))?;
                let next = input.next(
                    &declaration.uri,
                    state.next_batch.get(),
                    &self.prepared.session,
                    context,
                )?;
                (next, input.has_transport_bounds())
            };
            context.check_cancelled()?;
            let Some(source) = next else {
                let replay = recorder
                    .take()
                    .map(|value| value.finish(context))
                    .transpose()?;
                context.check_cancelled()?;
                return self.finish_producer(state, replay, consumption);
            };
            self.validate_input(declaration, &source, transport_bounds)?;
            let released = source.batch_release_witness()?;
            self.record_input(&source)?;
            let array = source.batch_array(context)?;
            if let Some(recorder) = &mut recorder {
                let mut copied = replay::CopyStats::default();
                recorder.build(array, &mut copied, context)?;
                let mut report = self.report.borrow_mut();
                checked_add(&mut report.replay_batches_detached, copied.batches)?;
                checked_add(&mut report.replay_rows_detached, copied.rows)?;
                checked_add(&mut report.replay_buffer_bytes_copied, copied.buffer_bytes)?;
            }
            if consumption == Consumption::Query && usage.detach_before_consume {
                let detached = super::super::native_payload::detach_with_policy(
                    array,
                    super::super::native_payload::CopyPolicy::PreserveUnobserved,
                    context,
                )?;
                {
                    let mut report = self.report.borrow_mut();
                    checked_add(&mut report.subquery_outer_batches_detached, 1)?;
                    checked_add(&mut report.subquery_outer_rows_detached, array.len() as u64)?;
                }
                drop(source);
                verify_released(&released)?;
                drop(owner);
                consume(&detached)?;
            } else {
                let result = match consumption {
                    Consumption::Query => consume(array),
                    Consumption::Prefill => Ok(()),
                    Consumption::Validate => {
                        let mut report = self.report.borrow_mut();
                        checked_add(&mut report.subquery_validation_batches, 1)?;
                        checked_add(&mut report.subquery_validation_rows, array.len() as u64)
                    }
                };
                drop(source);
                result?;
                verify_released(&released)?;
                drop(owner);
            }
            state.next_batch.set(
                state
                    .next_batch
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| failed("input batch ordinal overflow"))?,
            );
        }
    }
}

fn verify_released(
    released: &std::sync::Weak<shardloom_exec::live_memory::MemoryLease>,
) -> Result<()> {
    if released.strong_count() != 0 {
        return Err(failed(
            "input aliases remain live after consumption; transfer private batches",
        ));
    }
    Ok(())
}

impl Input for Execution<'_, '_> {
    fn require_usage(&self, index: usize, usage: Usage) -> Result<()> {
        let state = self
            .states
            .values
            .get(index)
            .ok_or_else(|| failed("streaming input source index is absent"))?;
        let mut current = state.usage.get();
        if state.started.get() {
            if usage.requires_replay() && !current.requires_replay() {
                return Err(failed(
                    "source reuse must be declared before its first input demand",
                ));
            }
            if !state.ended.get()
                && ((usage.detach_before_consume && !current.detach_before_consume)
                    || (usage.replay == ReplayRequirement::Prefill
                        && current.replay != ReplayRequirement::Prefill))
            {
                return Err(failed(
                    "nested streaming source roles must be declared before first input demand",
                ));
            }
        }
        current.extend(usage);
        state.usage.set(current);
        Ok(())
    }

    fn run(
        &self,
        index: usize,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        consume: &mut dyn FnMut(&ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let state = self
            .states
            .values
            .get(index)
            .ok_or_else(|| failed("streaming input source index is absent"))?;
        let declaration = self
            .prepared
            .batch_sources
            .get(index)
            .ok_or_else(|| failed("streaming input declaration is absent"))?;
        context.check_cancelled()?;
        if state.started.get() {
            return self.consume_replay(state, declaration, context, metrics, consume);
        }
        if state.usage.get().replay == ReplayRequirement::Prefill {
            self.run_producer(index, context, metrics, Consumption::Prefill, &mut |_| {
                Ok(())
            })?;
            self.consume_replay(state, declaration, context, metrics, consume)
        } else {
            self.run_producer(index, context, metrics, Consumption::Query, consume)
        }
    }

    fn completed(
        &self,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
    ) -> Result<ExecutedVortexBatchInput> {
        for (index, state) in self.states.values.iter().enumerate() {
            if !state.started.get() && state.usage.get().validate_if_unread {
                self.run_producer(index, context, metrics, Consumption::Validate, &mut |_| {
                    Ok(())
                })?;
            }
        }
        if self.states.values.is_empty()
            || self
                .states
                .values
                .iter()
                .any(|state| !state.started.get() || !state.ended.get())
        {
            return Err(failed(
                "streaming input did not reach explicit end-of-input",
            ));
        }
        let mut report = self.report.borrow().clone();
        report.sources = self.states.values.len();
        report.end_of_input_observed = true;
        // Completed replay owners must not survive cleanup/certification.
        for state in &self.states.values {
            state
                .replay
                .try_borrow_mut()
                .map_err(|_| failed("streaming replay remains active at completion"))?
                .take();
        }
        Ok(report)
    }
}

impl VortexRelationalBatchInput<'_, '_> {
    /// Execute each finite input once and stage every admitted local destination.
    /// Multiple destinations retain the native result under the existing query
    /// memory/spill policy; publication follows input completion and certification.
    /// # Errors
    /// Propagates source, schema, resource, cancellation, adapter and commit failures.
    /// Late commit failures preserve already published complete targets.
    #[cfg(feature = "vortex-write")]
    pub fn write_many_controlled(
        self,
        targets: &[(
            std::path::PathBuf,
            super::super::VortexLocalPrimitiveRowExportFormat,
        )],
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<super::WrittenVortexRelationalFanout> {
        self.prepared
            .write_many_with_input(targets, overwrite, cancellation, self.input)
    }

    /// Retain a complete native result after consuming the source exactly once.
    /// All retained output must fit the shared allocation. No row bridge occurs.
    /// # Errors
    /// Propagates input, schema, ownership, cancellation and resource failures.
    pub fn execute_native(
        self,
        cancellation: &CancellationToken,
    ) -> Result<super::ExecutedOwnedVortexRelational> {
        self.prepared
            .retain_complete_result(cancellation, false, Some(self.input))
    }

    /// Deliver Arrow batches incrementally from the same admitted native query.
    /// Batches are provisional until source completion and final certification.
    /// # Errors
    /// Propagates input, consumer, conversion, cancellation and resource failures.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn for_each_arrow_batch(
        self,
        cancellation: &CancellationToken,
        consume: impl FnMut(arrow_array::RecordBatch) -> Result<()>,
    ) -> Result<super::ExecutedVortexArrowRelational> {
        self.prepared
            .for_each_arrow_batch_with_input(cancellation, Some(self.input), consume)
    }

    /// Preserve ordinary source demand when an output window was requested.
    /// Actual batch sources are excluded by the prepared-plan policy.
    /// # Errors
    /// Propagates input, policy, consumer, cancellation and resource failures.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn for_each_arrow_batch_with_output_window(
        self,
        cancellation: &CancellationToken,
        requested: impl FnOnce() -> bool,
        initialize: impl FnOnce(Option<super::OutputCreditWindow>) -> Result<()>,
        consume: impl FnMut(arrow_array::RecordBatch) -> Result<()>,
    ) -> Result<super::ExecutedVortexArrowRelational> {
        self.prepared
            .for_each_arrow_batch_with_input_and_output_window(
                cancellation,
                Some(self.input),
                requested,
                initialize,
                consume,
            )
    }

    /// Deliver independently owned native batches; success requires source end.
    /// # Errors
    /// Propagates input, consumer, cancellation, schema and resource failures.
    pub fn for_each_batch(
        self,
        cancellation: &CancellationToken,
        consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.prepared.for_each_batch_with_input(
            cancellation,
            BATCH_ROWS,
            super::FileScanBatching::Native,
            Some(self.input),
            consume,
        )
    }

    /// Materialize bounded JSON batches through the same native operation.
    /// # Errors
    /// Rejects invalid delivery bounds and propagates every execution failure.
    pub fn for_each_json_batch(
        self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        max_batch_bytes: usize,
        consume: impl FnMut(super::super::collect::SerializedVortexResultBatch) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.prepared.for_each_json_batch_with_input(
            cancellation,
            batch_rows,
            max_batch_bytes,
            Some(self.input),
            consume,
        )
    }

    /// Collect a small complete result, after observing all source input.
    /// # Errors
    /// Rejects output above 65,536 rows or 8 MiB and every execution failure.
    pub fn collect_jsonl(
        self,
        cancellation: &CancellationToken,
    ) -> Result<CollectedVortexRelational> {
        self.prepared
            .collect_jsonl_with_input(cancellation, Some(self.input))
    }

    /// Publish one admitted local output only after input and sink completion.
    /// # Errors
    /// Propagates schema, destination, cancellation, input and sink failures.
    #[cfg(feature = "vortex-write")]
    pub fn write_controlled(
        self,
        path: &std::path::Path,
        format: super::super::VortexLocalPrimitiveRowExportFormat,
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<super::WrittenVortexRelational> {
        self.prepared
            .write_with_input(path, format, overwrite, cancellation, Some(self.input))
    }
}

/// Check the complete lowered shape before any binding that could execute rows.
pub(super) fn classify(
    plan: &VortexRelationalPlan,
    uri: &shardloom_core::DatasetUri,
) -> Result<Usage> {
    let usage = source_usage(plan, uri, 0, false, false)?;
    if usage.uses == 0 {
        Err(failed(
            "streaming plan does not use its declared batch source",
        ))
    } else {
        Ok(usage)
    }
}

/// Dynamic lowering binds native subtrees separately. Resolved relations no
/// longer contain source scans; execution-scoped ownership checks protect those
/// references and final completion checks every declared source.
pub(super) fn classify_dynamic(
    plan: &VortexRelationalPlan,
    uri: &shardloom_core::DatasetUri,
    outer_scope: bool,
) -> Result<Usage> {
    source_usage(plan, uri, 0, true, outer_scope)
}

fn source_usage(
    plan: &VortexRelationalPlan,
    uri: &shardloom_core::DatasetUri,
    depth: usize,
    dynamic: bool,
    outer_scope: bool,
) -> Result<Usage> {
    if depth > 24 {
        return Err(failed("recursive streaming plan exceeds 24 levels"));
    }
    let operator = match plan {
        VortexRelationalPlan::Scan(scan) => {
            return Ok(Usage {
                uses: usize::from(&scan.source_uri == uri),
                ..Usage::default()
            });
        }
        VortexRelationalPlan::Project(project) => {
            return source_usage(&project.input, uri, depth + 1, dynamic, outer_scope);
        }
        VortexRelationalPlan::Filter(filter) => {
            return source_usage(&filter.input, uri, depth + 1, dynamic, outer_scope);
        }
        VortexRelationalPlan::Sort(sort) => {
            return source_usage(&sort.input, uri, depth + 1, dynamic, outer_scope);
        }
        VortexRelationalPlan::Limit(limit) => {
            return source_usage(&limit.input, uri, depth + 1, dynamic, outer_scope);
        }
        VortexRelationalPlan::Aggregate(aggregate) => {
            return source_usage(&aggregate.input, uri, depth + 1, dynamic, outer_scope);
        }
        VortexRelationalPlan::Window(window) => {
            return source_usage(&window.input, uri, depth + 1, dynamic, outer_scope);
        }
        VortexRelationalPlan::Join(join) => {
            let left = source_usage(&join.left, uri, depth + 1, dynamic, outer_scope)?;
            let right = source_usage(&join.right, uri, depth + 1, dynamic, outer_scope)?;
            return Ok(left.combine(right));
        }
        VortexRelationalPlan::Set(set) => {
            let left = source_usage(&set.left, uri, depth + 1, dynamic, outer_scope)?;
            let right = source_usage(&set.right, uri, depth + 1, dynamic, outer_scope)?;
            return Ok(left.combine(right));
        }
        VortexRelationalPlan::Subquery(query) | VortexRelationalPlan::CorrelatedSubquery(query) => {
            let parameterized = matches!(plan, VortexRelationalPlan::CorrelatedSubquery(_));
            let conditional = parameterized
                || query.evaluation_guard.is_some()
                || matches!(
                    query.kind,
                    crate::relational_query::VortexRelationalSubqueryKind::Scalar
                );
            let mut input = source_usage(&query.input, uri, depth + 1, dynamic, outer_scope)?;
            let mut inner = source_usage(
                &query.relation,
                uri,
                depth + 1,
                dynamic,
                outer_scope || parameterized,
            )?;
            if conditional {
                input.detach_before_consume |= input.uses > 0;
                inner.validate_if_unread |= inner.uses > 0;
            }
            if parameterized && inner.uses > 0 {
                inner.replay = inner.replay.max(ReplayRequirement::Record);
            }
            // The producer cannot be reopened or replayed while its original
            // scan is active. Complete shared raw input before either query read.
            let shared = input.uses > 0 && inner.uses > 0;
            let mut usage = input.combine(inner);
            if shared {
                usage.replay = ReplayRequirement::Prefill;
            }
            return Ok(usage);
        }
        VortexRelationalPlan::Outer if outer_scope => return Ok(Usage::default()),
        VortexRelationalPlan::Outer => "correlated source",
        VortexRelationalPlan::Unary(unary)
            if super::super::prepared_unary::supports(unary.request.kind)
                && (dynamic
                    || unary.request.kind != super::VortexQueryPrimitiveKind::PivotRows) =>
        {
            // Existing unary state consumes finite native batches. Pivot still
            // requires execution-time schema discovery through the dynamic path.
            return source_usage(&unary.input, uri, depth + 1, dynamic, outer_scope);
        }
        VortexRelationalPlan::Unary(_) => "unary stateful operator",
        VortexRelationalPlan::ExecutionResult(_) if dynamic => return Ok(Usage::default()),
        VortexRelationalPlan::DeferredSubquery(reference) if dynamic => {
            if let Some(sources) = &reference.sources {
                return Ok(Usage {
                    uses: usize::from(sources.contains(uri)),
                    ..Usage::default()
                });
            }
            "dynamic schema"
        }
        VortexRelationalPlan::ExecutionResult(_) | VortexRelationalPlan::DeferredSubquery(_) => {
            "dynamic schema"
        }
    };
    Err(failed(&format!(
        "streaming input does not admit {operator}; choose explicit resident mode"
    )))
}

fn checked_add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| failed("input completion counter overflow"))?;
    Ok(())
}

pub(super) fn failed(reason: &str) -> shardloom_core::ShardLoomError {
    shardloom_core::ShardLoomError::InvalidOperation(format!(
        "SL-NATIVE-BATCH: {reason}; no fallback execution was attempted"
    ))
}
