//! Bind immutable retained definitions and lexical frontier schemas before demand.

use super::super::retained::{Binding, Definition};
use super::{Binder, DType, Node, NodeKind, Nullability, Result, failed, validate_key};
use crate::relational_query::{
    NATIVE_RECURSION_LIMIT, VortexRelationalJoinKind, VortexRelationalPlan as Plan,
    VortexRelationalRecursionId, VortexRelationalRecursive, VortexRelationalRecursiveInput,
    VortexRelationalStage,
};

pub(super) struct Scope {
    identity: VortexRelationalRecursionId,
    fields: Vec<(String, DType)>,
    uses: usize,
}

impl Binder<'_> {
    pub(super) fn retained_relation(
        &mut self,
        definition: Definition,
        depth: usize,
    ) -> Result<Node> {
        if self.parameterized_binding || self.outer_fields.is_some() {
            return Err(failed(
                "materialized and recursive relations do not admit parameter-dependent retention",
            ));
        }
        let existing = self
            .retained
            .values
            .iter()
            .find(|bound| bound.definition.same(&definition))
            .cloned();
        let binding = if let Some(binding) = existing {
            binding
        } else {
            let input = match &definition {
                Definition::Materialized(input) => {
                    validate_static(input, depth + 1)?;
                    // A materialized definition may be reused by several steps,
                    // but cannot retain a result that depends on one frontier.
                    let prior = self.recursion.take();
                    let result = self.bind(input, depth + 1);
                    self.recursion = prior;
                    result?
                }
                Definition::Recursive(input) => {
                    // Independent recursive definitions may compose, but cannot
                    // capture another definition's current frontier.
                    let prior = self.recursion.take();
                    let result = self.recursive(input, depth);
                    self.recursion = prior;
                    result?
                }
            };
            self.charge(4096)?;
            let binding = Binding::new(definition, input);
            self.retained.push(binding.clone())?;
            binding
        };
        let node = binding.node()?;
        self.charge_fields(node.fields.len())?;
        let fields = node.fields.clone();
        drop(node);
        Ok(Node {
            fields,
            kind: NodeKind::Retained(binding),
        })
    }

    pub(super) fn recursive_input(
        &mut self,
        input: &VortexRelationalRecursiveInput,
    ) -> Result<Node> {
        let scope = self
            .recursion
            .as_ref()
            .ok_or_else(|| failed("recursive input requires its enclosing recursive step"))?;
        if scope.identity != input.identity
            || scope
                .fields
                .iter()
                .map(|(name, _)| name)
                .ne(input.columns.iter())
        {
            return Err(failed(
                "recursive input identity or column names differ from its anchor",
            ));
        }
        let width = scope.fields.len();
        self.charge_fields(width)?;
        let scope = self.recursion.as_mut().expect("admitted recursive scope");
        scope.uses = scope
            .uses
            .checked_add(1)
            .ok_or_else(|| failed("recursive input reference count overflow"))?;
        Ok(Node {
            fields: scope.fields.clone(),
            kind: NodeKind::RecursiveInput(input.identity.clone()),
        })
    }

    fn recursive(&mut self, definition: &VortexRelationalRecursive, depth: usize) -> Result<Node> {
        if self.recursion.is_some() {
            return Err(failed(
                "nested recursive steps require a separate lexical iteration boundary",
            ));
        }
        validate_static(&definition.anchor, depth + 1)?;
        validate_static(&definition.step, depth + 1)?;
        if frontier_uses(&definition.step, &definition.identity, true, depth + 1)? != 1 {
            return Err(failed(
                "recursive term requires exactly one self-reference in an admitted relational position",
            ));
        }
        let anchor = Box::new(self.bind(&definition.anchor, depth + 1)?);
        self.charge_fields(anchor.fields.len())?;
        let fields = anchor
            .fields
            .iter()
            .map(|(name, dtype)| (name.clone(), dtype.with_nullability(Nullability::Nullable)))
            .collect::<Vec<_>>();
        if definition.distinct {
            for (_, dtype) in &fields {
                validate_key(dtype)?;
            }
        }
        self.charge_fields(fields.len())?;
        self.recursion = Some(Scope {
            identity: definition.identity.clone(),
            fields: fields.clone(),
            uses: 0,
        });
        let step = self.bind(&definition.step, depth + 1);
        let scope = self.recursion.take().expect("recursive binding scope");
        let step = Box::new(step?);
        if scope.uses != 1 {
            return Err(failed("recursive term must bind its frontier exactly once"));
        }
        if fields.len() != step.fields.len()
            || fields
                .iter()
                .zip(&step.fields)
                .any(|((_, expected), (_, actual))| {
                    expected.with_nullability(Nullability::NonNullable)
                        != actual.with_nullability(Nullability::NonNullable)
                })
        {
            return Err(failed(
                "recursive term must preserve the anchor's field count and exact value domains; implicit widening, decimal rescaling and timezone changes are not admitted",
            ));
        }
        Ok(Node {
            fields,
            kind: NodeKind::Recursive {
                identity: definition.identity.clone(),
                anchor,
                step,
                distinct: definition.distinct,
            },
        })
    }
}

fn children(plan: &Plan) -> Vec<&Plan> {
    match plan {
        Plan::Project(node) => vec![&node.input],
        Plan::Filter(node) => vec![&node.input],
        Plan::Limit(node) => vec![&node.input],
        Plan::Pipeline(node) => vec![&node.input],
        Plan::Sort(node) => vec![&node.input],
        Plan::Aggregate(node) => vec![&node.input],
        Plan::Window(node) => vec![&node.input],
        Plan::GapFill(node) => vec![&node.input],
        Plan::Unary(node) => vec![&node.input],
        Plan::Join(node) => vec![&node.left, &node.right],
        Plan::AsOfJoin(node) => vec![&node.left, &node.right],
        Plan::Set(node) => vec![&node.left, &node.right],
        Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => vec![&node.input, &node.relation],
        Plan::Materialized(input) => vec![input],
        Plan::Recursive(node) => vec![&node.anchor, &node.step],
        Plan::Scan(_)
        | Plan::Input(_)
        | Plan::Outer
        | Plan::RecursiveInput(_)
        | Plan::ExecutionResult(_)
        | Plan::DeferredSubquery(_) => vec![],
    }
}

fn validate_static(plan: &Plan, depth: usize) -> Result<()> {
    if depth > NATIVE_RECURSION_LIMIT {
        return Err(failed("retained relation plan exceeds 24 levels"));
    }
    match plan {
        Plan::Outer | Plan::ExecutionResult(_) | Plan::DeferredSubquery(_) => {
            return Err(failed(
                "retained definitions require static, parameter-independent native schemas",
            ));
        }
        Plan::Unary(node) if node.request.kind == super::VortexQueryPrimitiveKind::PivotRows => {
            return Err(failed(
                "retained definitions do not admit data-dependent pivot schemas",
            ));
        }
        _ => {}
    }
    for child in children(plan) {
        validate_static(child, depth + 1)?;
    }
    Ok(())
}

fn frontier_uses(
    plan: &Plan,
    identity: &VortexRelationalRecursionId,
    allowed: bool,
    depth: usize,
) -> Result<usize> {
    if depth > NATIVE_RECURSION_LIMIT {
        return Err(failed("recursive term plan exceeds 24 levels"));
    }
    if let Plan::RecursiveInput(input) = plan {
        if !allowed || &input.identity != identity {
            return Err(failed(
                "recursive self-reference is outside an admitted step position",
            ));
        }
        return Ok(1);
    }
    if matches!(plan, Plan::Recursive(_)) {
        // Its own binder hides this outer frontier and validates its separate
        // self-reference. A foreign frontier in that definition is rejected.
        return Ok(0);
    }
    if let Plan::Pipeline(node) = plan
        && node
            .stages
            .iter()
            .any(|stage| matches!(stage, VortexRelationalStage::Observe(_)))
    {
        return Err(failed(
            "recursive steps do not admit repeated observation names",
        ));
    }
    let allowed = allowed
        && match plan {
            Plan::Project(_)
            | Plan::Filter(_)
            | Plan::Sort(_)
            | Plan::Limit(_)
            | Plan::Pipeline(_) => true,
            Plan::Join(node) => matches!(
                node.kind,
                VortexRelationalJoinKind::Inner | VortexRelationalJoinKind::Cross
            ),
            _ => false,
        };
    children(plan)
        .into_iter()
        .try_fold(0_usize, |count, child| {
            count
                .checked_add(frontier_uses(child, identity, allowed, depth + 1)?)
                .ok_or_else(|| failed("recursive input reference count overflow"))
        })
}
