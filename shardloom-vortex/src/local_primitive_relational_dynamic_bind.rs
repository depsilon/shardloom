//! Bind data-dependent schemas inside the existing admitted native execution.

use super::super::{
    ArrayRef, BATCH_ROWS, DynamicLowerer, Metrics, NativeExecutionContext,
    PreparedVortexRelational, RefCell, add,
};
use super::{Binder, BoundUnary, Node, NodeKind, Result, VortexRelationalPlan, failed};
use crate::relational_query::{VortexRelationalDeferredRef, VortexRelationalExecutionRef};

pub(super) struct ExecutionBinding<'a> {
    owner: &'a PreparedVortexRelational,
    context: &'a NativeExecutionContext<'a>,
    metrics: &'a Metrics,
    parameter: Option<&'a ArrayRef>,
}

impl<'a> Binder<'a> {
    pub(in crate::local_primitives::prepared_relational) fn for_execution(
        owner: &'a PreparedVortexRelational,
        context: &'a NativeExecutionContext<'a>,
        metrics: &'a Metrics,
        parameter: Option<&'a ArrayRef>,
    ) -> Result<Self> {
        let mut binder = Self::new(&owner.session)?;
        for (source, path) in owner.sources.iter().zip(&owner.source_paths) {
            binder.charge(
                path.as_os_str()
                    .len()
                    .checked_mul(8)
                    .ok_or_else(|| failed("source path metadata overflow"))?,
            )?;
            binder.sources.push(source.clone())?;
            binder.paths.push(path.clone())?;
        }
        binder.execution = Some(ExecutionBinding {
            owner,
            context,
            metrics,
            parameter,
        });
        if let Some(parameter) = parameter {
            let fields = parameter
                .dtype()
                .as_struct_fields_opt()
                .ok_or_else(|| failed("outer parameter must have a native struct schema"))?;
            binder.charge(fields.names().len() * 4096)?;
            binder.outer_fields = Some(
                fields
                    .names()
                    .iter()
                    .zip(fields.fields())
                    .map(|(name, dtype)| (name.to_string(), dtype))
                    .collect(),
            );
        }
        Ok(binder)
    }

    pub(in crate::local_primitives::prepared_relational) fn defer_subquery(
        &mut self,
        declaration_bytes: usize,
        lower: Box<DynamicLowerer>,
    ) -> Result<VortexRelationalPlan> {
        if self.execution.is_none() || declaration_bytes == 0 {
            return Err(failed(
                "deferred subquery requires admitted execution and metadata credits",
            ));
        }
        if self.deferred.values.len() >= 128 {
            return Err(failed("dynamic execution exceeds 128 deferred relations"));
        }
        self.charge(declaration_bytes)?;
        let slot = self.deferred.values.len();
        self.deferred.push(Some(lower))?;
        Ok(VortexRelationalPlan::DeferredSubquery(
            VortexRelationalDeferredRef {
                scope: self.scope.clone(),
                slot,
            },
        ))
    }

    pub(super) fn take_deferred(
        &mut self,
        reference: &VortexRelationalDeferredRef,
    ) -> Result<Box<DynamicLowerer>> {
        if self.execution.is_none() || !std::sync::Arc::ptr_eq(&self.scope, &reference.scope) {
            return Err(failed("deferred relation belongs to a different execution"));
        }
        self.deferred
            .values
            .get_mut(reference.slot)
            .and_then(Option::take)
            .ok_or_else(|| failed("deferred relation is absent or was consumed twice"))
    }

    pub(in crate::local_primitives::prepared_relational) fn resolve_output(
        &mut self,
        plan: &VortexRelationalPlan,
    ) -> Result<(VortexRelationalPlan, Vec<String>)> {
        if self.execution.is_none() {
            return Err(failed(
                "dynamic schema resolution requires an admitted execution",
            ));
        }
        let node = self.bind(plan, 0)?;
        self.charge(node.fields.len() * 4096)?;
        let columns = node.fields.iter().map(|(name, _)| name.clone()).collect();
        let slot = self.resolved.values.len();
        self.resolved.push(Some(node))?;
        Ok((
            VortexRelationalPlan::ExecutionResult(VortexRelationalExecutionRef {
                scope: self.scope.clone(),
                slot,
            }),
            columns,
        ))
    }

    pub(in crate::local_primitives::prepared_relational) fn ensure_consumed(&self) -> Result<()> {
        if self.resolved.values.iter().any(Option::is_some)
            || self.deferred.values.iter().any(Option::is_some)
        {
            return Err(failed(
                "dynamic schema binding left an unconsumed native relation",
            ));
        }
        Ok(())
    }

    pub(super) fn take_resolved(
        &mut self,
        reference: &VortexRelationalExecutionRef,
    ) -> Result<Node> {
        if self.parameterized_binding {
            return Err(failed(
                "completed relation cannot be reused across correlated parameters; defer its declaration",
            ));
        }
        if self.execution.is_none() || !std::sync::Arc::ptr_eq(&self.scope, &reference.scope) {
            return Err(failed(
                "native relation reference belongs to a different execution",
            ));
        }
        self.resolved
            .values
            .get_mut(reference.slot)
            .and_then(Option::take)
            .ok_or_else(|| failed("native relation reference is absent or was consumed twice"))
    }

    pub(super) fn complete_pivot(&mut self, input: &Node, operation: BoundUnary) -> Result<Node> {
        let execution = self
            .execution
            .as_ref()
            .ok_or_else(|| failed("composed pivot requires execution-time schema binding"))?;
        if self.parameterized_binding
            || self.outer_fields.is_some() && execution.parameter.is_none()
        {
            return Err(failed(
                "correlated dynamic pivot requires per-parameter schema binding",
            ));
        }
        let result = operation.complete_relation_pivot(execution.context, |consume| {
            execution.owner.run(
                input,
                execution.context,
                execution.metrics,
                BATCH_ROWS,
                execution.parameter,
                consume,
            )
        })?;
        let usage = result.usage();
        execution
            .metrics
            .record_unary(usage.items, usage.all_input_retained)?;
        add(&execution.metrics.schema_discovery_stages, 1)?;
        super::validate_width(result.fields.len())?;
        for (name, dtype) in &result.fields {
            super::validate_name(name)?;
            super::validate_key(dtype)?;
        }
        super::validate_unique(&result.fields)?;
        self.charge(result.fields.len() * 4096)?;
        Ok(Node {
            fields: result.fields.clone(),
            kind: NodeKind::CompletedPivot {
                rows: result.rows,
                operation: Box::new(operation),
                result: RefCell::new(Some(result)),
            },
        })
    }
}
