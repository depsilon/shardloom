//! Keep ordinary short trees unchanged while flattening long streaming chains.

use super::{
    VortexRelationalPipeline as Pipeline, VortexRelationalPlan as Plan,
    VortexRelationalStage as Stage,
};

impl Plan {
    /// Compact all long projection/filter/range chains without recursion or
    /// changing their semantics. This is structural lowering, not execution.
    ///
    /// # Panics
    /// Panics if internal traversal and child accounting become inconsistent.
    #[must_use]
    pub fn compact_streaming(self) -> Self {
        let mut pending = vec![(self, false)];
        let mut completed = Vec::new();
        while let Some((mut plan, finish)) = pending.pop() {
            if finish {
                for input in children(&mut plan).into_iter().rev() {
                    *input = completed.pop().expect("visited child");
                }
                completed.push(compact_root(plan));
            } else {
                let inputs = children(&mut plan)
                    .into_iter()
                    .map(|input| std::mem::replace(input, Plan::Input(usize::MAX)))
                    .collect::<Vec<_>>();
                pending.push((plan, true));
                pending.extend(inputs.into_iter().rev().map(|input| (input, false)));
            }
        }
        completed.pop().expect("root visited")
    }
}

fn children(plan: &mut Plan) -> Vec<&mut Plan> {
    match plan {
        Plan::Project(node) => vec![&mut node.input],
        Plan::Filter(node) => vec![&mut node.input],
        Plan::Limit(node) => vec![&mut node.input],
        Plan::Pipeline(node) => vec![&mut node.input],
        Plan::Sort(node) => vec![&mut node.input],
        Plan::Aggregate(node) => vec![&mut node.input],
        Plan::Window(node) => vec![&mut node.input],
        Plan::Unary(node) => vec![&mut node.input],
        Plan::Join(node) => vec![&mut node.left, &mut node.right],
        Plan::AsOfJoin(node) => vec![&mut node.left, &mut node.right],
        Plan::Set(node) => vec![&mut node.left, &mut node.right],
        Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => {
            vec![&mut node.input, &mut node.relation]
        }
        Plan::Scan(_)
        | Plan::Input(_)
        | Plan::Outer
        | Plan::ExecutionResult(_)
        | Plan::DeferredSubquery(_) => vec![],
    }
}

fn compact_root(plan: Plan) -> Plan {
    let mut cursor = &plan;
    let mut length = 0usize;
    loop {
        cursor = match cursor {
            Plan::Project(node) => &node.input,
            Plan::Filter(node) => &node.input,
            Plan::Limit(node) => &node.input,
            Plan::Pipeline(_) => break,
            _ if length <= 8 => return plan,
            _ => break,
        };
        length += 1;
    }
    let mut input = plan;
    let mut reversed = Vec::with_capacity(length);
    loop {
        input = match input {
            Plan::Project(node) => {
                reversed.push(Stage::Project(node.expressions));
                node.input
            }
            Plan::Filter(node) => {
                reversed.push(Stage::Filter(node.predicate));
                node.input
            }
            Plan::Limit(node) => {
                reversed.push(Stage::Limit {
                    offset: node.offset,
                    count: node.count,
                });
                node.input
            }
            Plan::Pipeline(mut node) => {
                match std::mem::replace(&mut node.input, Plan::Input(usize::MAX)) {
                    Plan::Pipeline(mut inner) => {
                        inner.stages.append(&mut node.stages);
                        inner.stages.extend(reversed.into_iter().rev());
                        return Plan::Pipeline(inner);
                    }
                    input => node.input = input,
                }
                node.stages.extend(reversed.into_iter().rev());
                return Plan::Pipeline(node);
            }
            other => {
                return Plan::Pipeline(Box::new(Pipeline {
                    input: other,
                    stages: reversed.into_iter().rev().collect(),
                }));
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational_query::VortexRelationalLimit;

    #[test]
    fn long_streaming_chains_compact_clone_and_release_without_recursive_ownership() {
        let mut plan = Plan::Input(7);
        for offset in 0..8192 {
            plan = Plan::Limit(Box::new(VortexRelationalLimit {
                input: plan,
                offset,
                count: 1,
            }));
        }
        let plan = plan.compact_streaming();
        let Plan::Pipeline(pipeline) = &plan else {
            panic!("flat pipeline")
        };
        assert!(matches!(pipeline.input, Plan::Input(7)));
        assert_eq!(pipeline.stages.len(), 8192);
        assert!(pipeline.stages.iter().enumerate().all(
            |(index, stage)| matches!(stage, Stage::Limit { offset, count: 1 } if *offset == index)
        ));
        assert_eq!(plan.clone(), plan);
    }
}
