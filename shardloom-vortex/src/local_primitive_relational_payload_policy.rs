//! Explicit vector carriage is a field contract, never a query-wide float waiver.

use super::{
    NativeExecutionContext, Node, NodeKind, PipelineKind, PipelineStage, ReservedVec, Result,
    native_relational_expression::{Expression, Kind, scalar::Function, vector::Kernel},
};

enum Origin<'a> {
    Column(&'a str),
    Unobserved,
    Observed,
}

fn origin(expression: &Expression) -> Origin<'_> {
    match &expression.kind {
        Kind::Column(name) => Origin::Column(name),
        Kind::Function {
            function: Function::Vector(kernel),
            ..
        } if matches!(
            kernel.as_ref(),
            Kernel::Values | Kernel::Element | Kernel::Slice
        ) =>
        {
            Origin::Unobserved
        }
        // Arithmetic, casts, constructors and conditional selectors retain their
        // existing observation rules. A raw child is not a waiver for its parent.
        _ => Origin::Observed,
    }
}

impl Node {
    pub(super) fn carries_unobserved(&self, name: &str) -> bool {
        match self.semantic_kind() {
            NodeKind::Project { input, expressions } => self
                .fields
                .iter()
                .position(|(field, _)| field == name)
                .is_some_and(|index| match origin(&expressions[index]) {
                    Origin::Column(name) => input.carries_unobserved(name),
                    Origin::Unobserved => true,
                    Origin::Observed => false,
                }),
            NodeKind::Filter { input, .. }
            | NodeKind::Limit { input, .. }
            | NodeKind::Sort { input, .. } => input.carries_unobserved(name),
            NodeKind::Pipeline { input, stages } => pipeline_origin(input, stages, name),
            // Other operators retain their own payload-observation boundaries.
            // New vector selection after such an operator starts a new contract.
            _ => false,
        }
    }

    pub(super) fn unobserved_fields(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ReservedVec<usize>> {
        let mut fields = ReservedVec::new(context.memory())?;
        for (index, (name, _)) in self.fields.iter().enumerate() {
            context.check_cancelled()?;
            if self.carries_unobserved(name) {
                fields.push(index)?;
            }
        }
        Ok(fields)
    }
}

fn pipeline_origin<'a>(input: &Node, stages: &'a [PipelineStage], mut name: &'a str) -> bool {
    // Pipelines can contain many stages. Resolve aliases iteratively rather
    // than creating recursive callbacks or copying user-controlled names.
    for stage in stages.iter().rev() {
        match &stage.kind {
            PipelineKind::Project(expressions) => {
                let Some(index) = stage.fields.iter().position(|(field, _)| field == name) else {
                    return false;
                };
                match origin(&expressions[index]) {
                    Origin::Column(column) => name = column,
                    Origin::Unobserved => return true,
                    Origin::Observed => return false,
                }
            }
            PipelineKind::Filter(_) | PipelineKind::Limit { .. } | PipelineKind::Observe(_) => {}
            PipelineKind::ApplicationBatch(_) => return false,
        }
    }
    input.carries_unobserved(name)
}
