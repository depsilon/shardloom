//! Bind exact, ordered pure expressions to outputs already owned by a projection.

use super::{Expression, Kind};
use vortex::array::scalar::{PValue, Scalar, ScalarValue};

const MAX_PREVIOUS_OUTPUTS: usize = 16;
const MAX_NODES: usize = 64;

#[cfg(test)]
#[path = "native_relational_expression_reuse_tests.rs"]
mod tests;

impl Expression {
    pub(in crate::local_primitives) fn bind_projection_reuse(expressions: &mut [Self]) {
        // Projection narrowing can remove column-only outputs. Rebind from
        // expression trees so no output index refers to its old position.
        for expression in expressions.iter_mut() {
            expression.clear_reuse();
        }
        for index in 1..expressions.len() {
            let (previous, rest) = expressions.split_at_mut(index);
            let mut remaining = MAX_NODES;
            rest[0].bind_reuse(previous, &mut remaining);
        }
    }

    fn clear_reuse(&mut self) {
        self.reuse_output = None;
        match &mut self.kind {
            Kind::Column(_) | Kind::Literal(_) => {}
            Kind::Unary(_, child) | Kind::Cast { input: child, .. } => child.clear_reuse(),
            Kind::Binary(left, _, right)
            | Kind::Compare(left, _, right)
            | Kind::NullIf(left, right) => {
                left.clear_reuse();
                right.clear_reuse();
            }
            Kind::Conditional(condition, yes, no) => {
                condition.clear_reuse();
                yes.clear_reuse();
                no.clear_reuse();
            }
            Kind::List(children)
            | Kind::Struct(children)
            | Kind::Coalesce(children)
            | Kind::Function { args: children, .. } => {
                for child in children {
                    child.clear_reuse();
                }
            }
        }
    }

    fn bind_reuse(&mut self, previous: &[Self], remaining: &mut usize) {
        if *remaining == 0
            || !matches!(
                self.kind,
                Kind::Column(_)
                    | Kind::Literal(_)
                    | Kind::Unary(..)
                    | Kind::Binary(..)
                    | Kind::Compare(..)
                    | Kind::Conditional(..)
                    | Kind::Coalesce(_)
                    | Kind::NullIf(..)
                    | Kind::Cast { .. }
                    | Kind::List(_)
                    | Kind::Struct(_)
            )
        {
            return;
        }
        *remaining -= 1;
        for (index, prior) in previous.iter().enumerate().rev().take(MAX_PREVIOUS_OUTPUTS) {
            let mut comparison_budget = MAX_NODES;
            if same(self, prior, &mut comparison_budget) {
                self.reuse_output = Some(index);
                return;
            }
        }
        // An identical completed outer expression belongs to this full input.
        // Its lazy branches can have different selected inputs, so never bind
        // those children against the full-input output scope.
        match &mut self.kind {
            Kind::Unary(_, child) => child.bind_reuse(previous, remaining),
            Kind::Binary(left, _, right) | Kind::Compare(left, _, right) => {
                left.bind_reuse(previous, remaining);
                right.bind_reuse(previous, remaining);
            }
            _ => {}
        }
    }
}

fn same(left: &Expression, right: &Expression, remaining: &mut usize) -> bool {
    if *remaining == 0 || left.dtype != right.dtype {
        return false;
    }
    *remaining -= 1;
    match (&left.kind, &right.kind) {
        (Kind::Column(left), Kind::Column(right)) => left == right,
        (Kind::Literal(left), Kind::Literal(right)) => same_literal(left, right),
        (Kind::Unary(lop, left), Kind::Unary(rop, right)) => {
            lop == rop && same(left, right, remaining)
        }
        (Kind::Binary(ll, lop, lr), Kind::Binary(rl, rop, rr)) => {
            lop == rop && same(ll, rl, remaining) && same(lr, rr, remaining)
        }
        (Kind::Compare(ll, lop, lr), Kind::Compare(rl, rop, rr)) => {
            lop == rop && same(ll, rl, remaining) && same(lr, rr, remaining)
        }
        (Kind::Conditional(lc, ly, ln), Kind::Conditional(rc, ry, rn)) => {
            same(lc, rc, remaining) && same(ly, ry, remaining) && same(ln, rn, remaining)
        }
        (Kind::NullIf(ll, lr), Kind::NullIf(rl, rr)) => {
            same(ll, rl, remaining) && same(lr, rr, remaining)
        }
        (
            Kind::Cast {
                input: left,
                tolerant: lt,
            },
            Kind::Cast {
                input: right,
                tolerant: rt,
            },
        ) => lt == rt && same(left, right, remaining),
        (Kind::Coalesce(left), Kind::Coalesce(right))
        | (Kind::List(left), Kind::List(right))
        | (Kind::Struct(left), Kind::Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| same(left, right, remaining))
        }
        _ => false,
    }
}

fn same_literal(left: &Scalar, right: &Scalar) -> bool {
    if left.dtype() != right.dtype() {
        return false;
    }
    match (left.value(), right.value()) {
        (
            Some(ScalarValue::Primitive(PValue::F16(l))),
            Some(ScalarValue::Primitive(PValue::F16(r))),
        ) => l.to_bits() == r.to_bits(),
        (
            Some(ScalarValue::Primitive(PValue::F32(l))),
            Some(ScalarValue::Primitive(PValue::F32(r))),
        ) => l.to_bits() == r.to_bits(),
        (
            Some(ScalarValue::Primitive(PValue::F64(l))),
            Some(ScalarValue::Primitive(PValue::F64(r))),
        ) => l.to_bits() == r.to_bits(),
        // Composite constants are outside this bounded scalar family. Do not
        // inherit a provider's recursive floating-point equality semantics.
        (Some(ScalarValue::Tuple(_) | ScalarValue::Union(_) | ScalarValue::Variant(_)), _)
        | (_, Some(ScalarValue::Tuple(_) | ScalarValue::Union(_) | ScalarValue::Variant(_))) => {
            false
        }
        (left, right) => left == right,
    }
}
