//! Borrow equivalent nested nodes and flat stages without rebuilding a tree.

use crate::local_primitives::{
    LocalVortexScanPlan, native_relational_aggregate,
    native_relational_expression::Expression,
    native_relational_sort,
    prepared_relational::{Node, NodeKind, PipelineKind, PipelineStage, ScanSource},
};
use shardloom_core::{DatasetUri, PredicateExpr};
use vortex::array::dtype::DType;

#[derive(Clone, Copy)]
pub(super) enum View<'a> {
    Node(&'a Node),
    Stages {
        input: &'a Node,
        stages: &'a [PipelineStage],
    },
}

pub(super) enum Kind<'a> {
    Scan {
        source: &'a ScanSource,
        source_uri: &'a DatasetUri,
        predicate: &'a Option<PredicateExpr>,
        plan: &'a LocalVortexScanPlan,
    },
    Project {
        input: View<'a>,
        expressions: &'a [Expression],
    },
    Filter {
        input: View<'a>,
        predicate: &'a Expression,
    },
    Aggregate {
        input: View<'a>,
        spec: &'a native_relational_aggregate::Spec,
    },
    Sort {
        input: View<'a>,
        spec: &'a native_relational_sort::Spec,
    },
    Limit {
        input: View<'a>,
        offset: usize,
        count: usize,
    },
    Unsupported,
}

impl<'a> View<'a> {
    fn normalized(mut self) -> Self {
        loop {
            self = match self {
                Self::Node(Node {
                    kind: NodeKind::Pipeline { input, stages },
                    ..
                }) => Self::Stages { input, stages },
                Self::Stages { input, stages: [] } => Self::Node(input),
                _ => return self,
            };
        }
    }

    pub(super) fn fields(self) -> &'a [(String, DType)] {
        match self.normalized() {
            Self::Node(node) => &node.fields,
            Self::Stages { stages, .. } => &stages.last().expect("nonempty stages").fields,
        }
    }

    pub(super) fn kind(self) -> Kind<'a> {
        match self.normalized() {
            Self::Node(node) => match &node.kind {
                NodeKind::Scan {
                    source,
                    source_uri,
                    predicate,
                    plan,
                    ..
                } => Kind::Scan {
                    source,
                    source_uri,
                    predicate,
                    plan,
                },
                NodeKind::Project { input, expressions } => Kind::Project {
                    input: Self::Node(input),
                    expressions,
                },
                NodeKind::Filter { input, predicate } => Kind::Filter {
                    input: Self::Node(input),
                    predicate,
                },
                NodeKind::Aggregate {
                    input,
                    spec,
                    expansion: None,
                    ..
                } => Kind::Aggregate {
                    input: Self::Node(input),
                    spec,
                },
                NodeKind::Sort { input, spec, .. } => Kind::Sort {
                    input: Self::Node(input),
                    spec,
                },
                NodeKind::Limit {
                    input,
                    offset,
                    count,
                } => Kind::Limit {
                    input: Self::Node(input),
                    offset: *offset,
                    count: *count,
                },
                _ => Kind::Unsupported,
            },
            Self::Stages { input, stages } => {
                let (stage, stages) = stages.split_last().expect("nonempty stages");
                let input = Self::Stages { input, stages };
                match &stage.kind {
                    PipelineKind::Project(expressions) => Kind::Project { input, expressions },
                    PipelineKind::Filter(predicate) => Kind::Filter { input, predicate },
                    PipelineKind::Limit { offset, count } => Kind::Limit {
                        input,
                        offset: *offset,
                        count: *count,
                    },
                    PipelineKind::Observe(_) | PipelineKind::ApplicationBatch(_) => {
                        Kind::Unsupported
                    }
                }
            }
        }
    }

    pub(super) fn single_file(mut self) -> Option<usize> {
        loop {
            self = match self.kind() {
                Kind::Scan {
                    source: ScanSource::File(index),
                    ..
                } => return Some(*index),
                Kind::Project { input, .. }
                | Kind::Filter { input, .. }
                | Kind::Aggregate { input, .. }
                | Kind::Sort { input, .. }
                | Kind::Limit { input, .. } => input,
                _ => return None,
            };
        }
    }
}
