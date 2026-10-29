//! A deferred lowerer can only demand its declared, already registered inputs.

use super::{Binder, Result, SourceIndex, VortexRelationalPlan, failed};
use shardloom_core::DatasetUri;
use shardloom_exec::live_memory::MemoryLease;
use std::sync::Arc;

pub(super) struct DeclaredSources {
    pub(super) identities: Arc<[DatasetUri]>,
    pub(super) scope: Arc<[SourceIndex]>,
    pub(super) metadata: Arc<MemoryLease>,
}

impl Binder<'_> {
    pub(super) fn declared_subquery_sources(
        &mut self,
        sources: &[DatasetUri],
    ) -> Result<DeclaredSources> {
        if sources.len() > 128 {
            return Err(failed("dynamic subquery exceeds 128 declared sources"));
        }
        let bytes = sources
            .iter()
            .try_fold(0usize, |bytes, uri| bytes.checked_add(uri.as_str().len()));
        let bytes = bytes
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add((sources.len() + 1) * 4096))
            .ok_or_else(|| failed("dynamic subquery source metadata overflow"))?;
        self.charge(bytes)?;
        // A caller may retain an invalid cloned reference after execution.
        // Keep the reservation with both source arrays until every owner drops.
        let metadata = Arc::new(self.metadata.split(bytes as u64)?);
        let mut identities = Vec::with_capacity(sources.len());
        let mut scope = Vec::with_capacity(sources.len());
        for uri in sources {
            // Execution-time lookup cannot add a source. It also applies a
            // parent's restriction, including canonical file-source identity.
            let source = self.input(uri)?;
            if !scope.contains(&source) {
                identities.push(uri.clone());
                scope.push(source);
            }
        }
        for source in &scope {
            if let SourceIndex::Batch(index) = source {
                self.require_batch_replay(*index)?;
            }
        }
        Ok(DeclaredSources {
            identities: identities.into(),
            scope: scope.into(),
            metadata,
        })
    }

    pub(in crate::local_primitives::prepared_relational) fn restrict_subquery_sources(
        &mut self,
        sources: Option<Arc<[SourceIndex]>>,
        metadata: Option<Arc<MemoryLease>>,
    ) {
        // Each parameter binder copies the same prepared owner's source lists
        // in the same order. Execution cannot register another source.
        self.source_scope = sources;
        self.source_metadata = metadata;
    }

    pub(super) fn validate_subquery_source_scope(
        &mut self,
        plan: &VortexRelationalPlan,
        depth: usize,
    ) -> Result<()> {
        if self.source_scope.is_none() {
            return Ok(());
        }
        self.cancellation.check()?;
        if depth > crate::relational_query::NATIVE_RECURSION_LIMIT {
            return Err(failed("recursive plan exceeds 24 levels"));
        }
        // Validate the complete subtree before binding a pivot can execute an
        // earlier child. Checking only individual scans would be too late.
        match plan {
            VortexRelationalPlan::Input(_) => {
                return Err(failed("native declaration input is not bound"));
            }
            VortexRelationalPlan::Scan(scan) => {
                self.input(&scan.source_uri)?;
            }
            VortexRelationalPlan::Project(project) => {
                self.validate_subquery_source_scope(&project.input, depth + 1)?;
            }
            VortexRelationalPlan::Filter(filter) => {
                self.validate_subquery_source_scope(&filter.input, depth + 1)?;
            }
            VortexRelationalPlan::Sort(sort) => {
                self.validate_subquery_source_scope(&sort.input, depth + 1)?;
            }
            VortexRelationalPlan::Limit(limit) => {
                self.validate_subquery_source_scope(&limit.input, depth + 1)?;
            }
            VortexRelationalPlan::Pipeline(pipeline) => {
                self.validate_subquery_source_scope(&pipeline.input, depth + 1)?;
            }
            VortexRelationalPlan::Aggregate(aggregate) => {
                self.validate_subquery_source_scope(&aggregate.input, depth + 1)?;
            }
            VortexRelationalPlan::Window(window) => {
                self.validate_subquery_source_scope(&window.input, depth + 1)?;
            }
            VortexRelationalPlan::GapFill(fill) => {
                self.validate_subquery_source_scope(&fill.input, depth + 1)?;
            }
            VortexRelationalPlan::Unary(unary) => {
                self.validate_subquery_source_scope(&unary.input, depth + 1)?;
            }
            VortexRelationalPlan::Join(join) => {
                self.validate_subquery_source_scope(&join.left, depth + 1)?;
                self.validate_subquery_source_scope(&join.right, depth + 1)?;
            }
            VortexRelationalPlan::AsOfJoin(join) => {
                self.validate_subquery_source_scope(&join.left, depth + 1)?;
                self.validate_subquery_source_scope(&join.right, depth + 1)?;
            }
            VortexRelationalPlan::Set(set) => {
                self.validate_subquery_source_scope(&set.left, depth + 1)?;
                self.validate_subquery_source_scope(&set.right, depth + 1)?;
            }
            VortexRelationalPlan::Subquery(query)
            | VortexRelationalPlan::CorrelatedSubquery(query) => {
                self.validate_subquery_source_scope(&query.input, depth + 1)?;
                self.validate_subquery_source_scope(&query.relation, depth + 1)?;
            }
            VortexRelationalPlan::DeferredSubquery(reference) => {
                if let Some(sources) = &reference.sources {
                    for uri in sources.iter() {
                        self.input(uri)?;
                    }
                }
            }
            VortexRelationalPlan::ExecutionResult(_) | VortexRelationalPlan::Outer => {}
        }
        Ok(())
    }
}
