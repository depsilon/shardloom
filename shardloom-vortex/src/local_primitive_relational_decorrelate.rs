//! Remove pure equality parameters only after the original tree is bound.
//! Every removed value domain is fixed-width and infallible; sources with
//! callbacks, filters, finite limits or effects keep singleton execution.

use super::super::super::{PipelineKind, ScanSource};
use super::{Binder, DType, Kind, Node, NodeKind, Result, Spec, SubqueryRelation};
use crate::{
    local_primitives::native_relational_expression::{Expression, Kind as Expr},
    relational_query::{VortexRelationalJoinKind as Join, VortexRelationalSide as Side},
};
use shardloom_core::{BinaryOp, ComparisonOp};

const WIDTH: usize = 128;

#[derive(Clone, Copy)]
struct Origin<'a> {
    output: &'a str,
    source: &'a str,
    side: Side,
}

struct Shape<'a> {
    columns: [Option<Origin<'a>>; WIDTH],
    keys: [Option<(&'a str, &'a str)>; WIDTH],
}

impl<'a> Shape<'a> {
    fn column(&self, name: &str) -> Option<Origin<'a>> {
        self.columns
            .iter()
            .flatten()
            .find(|c| c.output == name)
            .copied()
    }

    fn project(
        &mut self,
        fields: &'a [(String, DType)],
        expressions: &'a [Expression],
    ) -> Option<()> {
        if !fixed(fields) {
            return None;
        }
        let mut columns = [None; WIDTH];
        for (index, ((name, _), expression)) in fields.iter().zip(expressions).enumerate() {
            columns[index] = match &expression.kind {
                Expr::Column(source) => Some(Origin {
                    output: name,
                    ..self.column(source)?
                }),
                // SQL EXISTS replaces the selected payload with a true row-
                // presence marker. It has no value domain to evaluate or key
                // origin; any subsequent key use still declines below.
                Expr::Literal(value)
                    if matches!(value.dtype(), DType::Bool(_))
                        && value.as_bool().value() == Some(true) =>
                {
                    None
                }
                _ => return None,
            };
        }
        self.columns = columns;
        Some(())
    }

    fn filter(&mut self, predicate: &'a Expression) -> Option<()> {
        let columns = self.columns;
        equality(predicate, &mut self.keys, &|name| {
            columns.iter().flatten().find(|c| c.output == name).copied()
        })
    }
}

fn fixed(fields: &[(String, DType)]) -> bool {
    fields.len() <= WIDTH
        && fields.iter().all(|(_, dtype)| {
            matches!(dtype, DType::Bool(_))
                || matches!(dtype, DType::Primitive(ptype, _) if ptype.is_int())
        })
}

fn pure_source(node: &Node) -> bool {
    if !fixed(&node.fields) {
        return false;
    }
    match &node.kind {
        NodeKind::Scan {
            source,
            predicate,
            residual,
            plan,
            ..
        } => {
            matches!(source, ScanSource::Memory(projection) if projection.has_infallible_fixed_width_values())
                && predicate.is_none()
                && residual.is_none()
                && plan.filter.is_none()
                && plan.residual_predicate.is_none()
                && plan.source_order_limit.is_none()
                && plan.embedded_derived_column_rewrites.is_empty()
        }
        NodeKind::Project { input, expressions } => {
            expressions
                .iter()
                .all(|e| matches!(e.kind, Expr::Column(_)))
                && pure_source(input)
        }
        NodeKind::Pipeline { input, stages } => {
            pure_source(input)
                && stages.iter().all(|stage| {
                    fixed(&stage.fields)
                        && matches!(&stage.kind, PipelineKind::Project(expressions)
                            if expressions.iter().all(|e| matches!(e.kind, Expr::Column(_))))
                })
        }
        _ => false,
    }
}

fn add_key<'a>(
    keys: &mut [Option<(&'a str, &'a str)>; WIDTH],
    left: Origin<'a>,
    right: Origin<'a>,
) -> Option<()> {
    let pair = match (left.side, right.side) {
        (Side::Left, Side::Right) => (right.source, left.source),
        (Side::Right, Side::Left) => (left.source, right.source),
        _ => return None,
    };
    if !keys.iter().flatten().any(|key| *key == pair) {
        *keys.iter_mut().find(|key| key.is_none())? = Some(pair);
    }
    Some(())
}

fn equality<'a>(
    expression: &'a Expression,
    keys: &mut [Option<(&'a str, &'a str)>; WIDTH],
    resolve: &impl Fn(&'a str) -> Option<Origin<'a>>,
) -> Option<()> {
    match &expression.kind {
        Expr::Binary(left, BinaryOp::And, right) => {
            equality(left, keys, resolve)?;
            equality(right, keys, resolve)
        }
        Expr::Compare(left, ComparisonOp::Eq, right) => {
            let (Expr::Column(left), Expr::Column(right)) = (&left.kind, &right.kind) else {
                return None;
            };
            add_key(keys, resolve(left)?, resolve(right)?)
        }
        _ => None,
    }
}

fn shape(node: &Node) -> Option<Shape<'_>> {
    if !fixed(&node.fields) {
        return None;
    }
    match &node.kind {
        NodeKind::Project { input, expressions } => {
            let mut shape = shape(input)?;
            shape.project(&node.fields, expressions)?;
            Some(shape)
        }
        NodeKind::Filter { input, predicate } => {
            let mut shape = shape(input)?;
            shape.filter(predicate)?;
            Some(shape)
        }
        NodeKind::Pipeline { input, stages } => {
            let mut shape = shape(input)?;
            for stage in stages {
                if !fixed(&stage.fields) {
                    return None;
                }
                match &stage.kind {
                    PipelineKind::Project(expressions) => {
                        shape.project(&stage.fields, expressions)?;
                    }
                    PipelineKind::Filter(predicate) => shape.filter(predicate)?,
                    _ => return None,
                }
            }
            Some(shape)
        }
        NodeKind::Join { left, right, spec }
            if matches!(spec.kind, Join::Cross | Join::Inner)
                && spec.asof.is_none()
                && matches!(right.kind, NodeKind::Outer)
                && fixed(&right.fields)
                && pure_source(left) =>
        {
            let mut shape = Shape {
                columns: [None; WIDTH],
                keys: [None; WIDTH],
            };
            for (index, ((output, _), (side, source))) in
                node.fields.iter().zip(&spec.columns).enumerate()
            {
                shape.columns[index] = Some(Origin {
                    output,
                    source,
                    side: *side,
                });
            }
            for (left, right) in spec.left_keys.iter().zip(&spec.right_keys) {
                add_key(
                    &mut shape.keys,
                    Origin {
                        output: left,
                        source: left,
                        side: Side::Left,
                    },
                    Origin {
                        output: right,
                        source: right,
                        side: Side::Right,
                    },
                )?;
            }
            if let Some(condition) = &spec.condition {
                equality(&condition.expression, &mut shape.keys, &|name| {
                    let index = condition
                        .fields
                        .iter()
                        .position(|(field, _)| field == name)?;
                    let (side, source) = &condition.columns[index];
                    Some(Origin {
                        output: name,
                        source,
                        side: *side,
                    })
                })?;
            }
            Some(shape)
        }
        _ => None,
    }
}

fn inner(node: Node) -> Box<Node> {
    match node.kind {
        NodeKind::Project { input, .. }
        | NodeKind::Filter { input, .. }
        | NodeKind::Pipeline { input, .. } => inner(*input),
        NodeKind::Join { left, .. } => left,
        _ => unreachable!("decorrelation consumes only its previously admitted shape"),
    }
}

pub(super) fn rewrite(
    binder: &mut Binder<'_>,
    input: &Node,
    relation: &mut SubqueryRelation,
    spec: &mut Spec,
) -> Result<bool> {
    if !pure_source(input) || !matches!(spec.kind, Kind::Exists | Kind::In) {
        return Ok(false);
    }
    let SubqueryRelation::Bound(node) = relation else {
        return Ok(false);
    };
    let Some(shape) = shape(node) else {
        return Ok(false);
    };
    let count = shape.keys.iter().flatten().count();
    if count == 0 || count + spec.right_keys.len() > WIDTH {
        return Ok(false);
    }
    let mut membership = [None; WIDTH];
    for (index, name) in spec.right_keys.iter().enumerate() {
        let Some(column) = shape.column(name).filter(|c| c.side == Side::Left) else {
            return Ok(false);
        };
        membership[index] = Some(column.source);
    }
    // The borrowed shape uses bounded stack storage. Admit new owned names
    // before any allocation or mutation of the original bound plan.
    binder.charge_items(count + spec.right_keys.len(), 8192)?;
    let left = shape
        .keys
        .iter()
        .flatten()
        .map(|(outer, _)| (*outer).to_owned())
        .chain(spec.left_keys.iter().cloned())
        .collect();
    let right = shape
        .keys
        .iter()
        .flatten()
        .map(|(_, inner)| (*inner).to_owned())
        .chain(membership.into_iter().flatten().map(str::to_owned))
        .collect();
    spec.left_keys = left;
    spec.right_keys = right;
    spec.correlation = count;
    let original = std::mem::replace(
        node.as_mut(),
        Node {
            fields: Vec::new(),
            kind: NodeKind::Outer,
        },
    );
    *node = inner(original);
    #[cfg(all(test, debug_assertions))]
    crate::local_primitives::native_relational_observation::record(|counts| {
        counts.subquery_equality_rewrites += 1;
    });
    Ok(true)
}
