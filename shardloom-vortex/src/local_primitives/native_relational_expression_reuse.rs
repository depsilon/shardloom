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
        for index in 1..expressions.len() {
            let (previous, rest) = expressions.split_at_mut(index);
            let mut remaining = MAX_NODES;
            rest[0].bind_reuse(previous, &mut remaining);
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
