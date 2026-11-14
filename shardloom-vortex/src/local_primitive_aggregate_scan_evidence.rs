//! Observe the actual native provider contract after every aggregate pass binds.
//!
//! This adapter owns no execution strategy. The pinned Vortex builder has no
//! public projection/filter accessors, so its existing modifiers are forwarded
//! with the same bound expressions and its iterator remains the producer.

use super::AggregateScanSource;
use crate::local_primitives::PredicateExpr;
use vortex::{
    array::ArrayRef,
    error::{VortexResult, vortex_err},
    expr::BoundExpression,
    io::runtime::BlockingRuntime,
    layout::scan::scan_builder::ScanBuilder,
};

#[cfg(unix)]
use crate::{
    local_primitives::native_file_scan_evidence::Observation,
    resident_session::{NativeExecutionContext, PreparedVortexSource},
};

#[cfg(unix)]
enum Target<'a> {
    Prepared(&'a PreparedVortexSource),
    Path(&'a std::path::Path),
}

#[cfg(unix)]
pub(in crate::local_primitives) struct FileEvidence<'a> {
    context: &'a NativeExecutionContext<'a>,
    target: Target<'a>,
    scans: std::cell::Cell<u64>,
    checks: std::cell::Cell<u64>,
    exclusions: std::cell::Cell<u64>,
}

#[cfg(unix)]
impl<'a> FileEvidence<'a> {
    pub(in crate::local_primitives) fn prepared(
        source: &'a PreparedVortexSource,
        context: &'a NativeExecutionContext<'a>,
    ) -> Option<Self> {
        source
            .has_file_generation()
            .then(|| Self::new(Target::Prepared(source), context))
    }

    pub(in crate::local_primitives) fn path(
        path: &'a std::path::Path,
        context: &'a NativeExecutionContext<'a>,
    ) -> Self {
        Self::new(Target::Path(path), context)
    }

    fn new(target: Target<'a>, context: &'a NativeExecutionContext<'a>) -> Self {
        Self {
            context,
            target,
            scans: std::cell::Cell::new(0),
            checks: std::cell::Cell::new(0),
            exclusions: std::cell::Cell::new(0),
        }
    }

    pub(in crate::local_primitives) fn scan_invocations(&self) -> u64 {
        self.scans.get()
    }

    pub(in crate::local_primitives) fn pruning(&self, excluded: bool) -> VortexResult<()> {
        let checks = increment(&self.checks, 1)?;
        let exclusions = increment(&self.exclusions, u64::from(excluded))?;
        self.checks.set(checks);
        self.exclusions.set(exclusions);
        Ok(())
    }

    fn begin(&self, observation: Observation) -> VortexResult<usize> {
        let scans = increment(&self.scans, 1)?;
        let checks = self.checks.get();
        let exclusions = self.exclusions.get();
        let index = match self.target {
            Target::Prepared(source) => observation.begin(source, self.context, checks, exclusions),
            Target::Path(path) => observation.begin_path(path, self.context, checks, exclusions),
        }
        .map_err(|error| vortex_err!("{error}"))?;
        self.scans.set(scans);
        self.checks.set(0);
        self.exclusions.set(0);
        Ok(index)
    }
}

#[cfg(unix)]
fn increment(value: &std::cell::Cell<u64>, amount: u64) -> VortexResult<u64> {
    value
        .get()
        .checked_add(amount)
        .ok_or_else(|| vortex_err!("aggregate scan evidence counter overflow"))
}

pub(in crate::local_primitives) struct Scan<'a> {
    inner: ScanBuilder<ArrayRef>,
    source: AggregateScanSource<'a>,
    projection: Option<BoundExpression>,
    filter: Option<BoundExpression>,
    residual: Option<&'a PredicateExpr>,
}

impl<'a> Scan<'a> {
    pub(in crate::local_primitives) fn new(
        inner: ScanBuilder<ArrayRef>,
        source: AggregateScanSource<'a>,
    ) -> Self {
        Self {
            inner,
            source,
            projection: None,
            filter: None,
            residual: None,
        }
    }

    pub(in crate::local_primitives) fn with_projection(
        mut self,
        projection: BoundExpression,
    ) -> Self {
        self.inner = self.inner.with_projection(projection.clone());
        self.projection = Some(projection);
        self
    }

    pub(in crate::local_primitives) fn with_filter(mut self, filter: BoundExpression) -> Self {
        self.inner = self.inner.with_filter(filter.clone());
        self.filter = Some(filter);
        self
    }

    pub(in crate::local_primitives) fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.inner = self.inner.with_concurrency(concurrency);
        self
    }

    pub(in crate::local_primitives) fn with_row_range(
        mut self,
        range: std::ops::Range<u64>,
    ) -> Self {
        self.inner = self.inner.with_row_range(range);
        self
    }

    /// The aggregate consumer, after this provider, evaluates this residual.
    pub(in crate::local_primitives) fn with_residual_evidence(
        mut self,
        residual: Option<&'a PredicateExpr>,
    ) -> Self {
        self.residual = residual;
        self
    }

    pub(in crate::local_primitives) fn into_array_iter<'r, B: BlockingRuntime>(
        self,
        runtime: &'r B,
    ) -> VortexResult<impl Iterator<Item = VortexResult<ArrayRef>> + use<'a, 'r, B>> {
        #[cfg(unix)]
        let observed = match self.source {
            AggregateScanSource::ObservedFile(file, evidence) => {
                let mut observation = Observation::file_bound(
                    file,
                    self.projection.as_ref(),
                    self.filter.as_ref(),
                    shardloom_core::native_file_scan::NativeFileScanRoute::AggregatePass,
                    evidence.context,
                )
                .map_err(|error| vortex_err!("{error}"))?;
                observation
                    .residual(self.residual)
                    .map_err(|error| vortex_err!("{error}"))?;
                Some((evidence.context, evidence.begin(observation)?))
            }
            _ => None,
        };
        #[cfg(not(unix))]
        let _ = self.source;
        // Keep the actual provider's error and partial-delivery semantics. A
        // native pressure retry creates a new iterator/contract; already read
        // rows remain observed even if that attempt's aggregate state is dropped.
        let iter = self.inner.into_array_iter(runtime)?;
        Ok(iter.map(move |chunk| {
            let chunk = chunk?;
            #[cfg(unix)]
            if let Some((context, index)) = observed {
                context
                    .add_native_file_scan_rows(index, chunk.len() as u64)
                    .map_err(|error| vortex_err!("{error}"))?;
            }
            Ok(chunk)
        }))
    }
}
