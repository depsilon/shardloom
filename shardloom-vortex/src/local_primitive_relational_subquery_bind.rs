//! Bind subquery arity, exact key domains, scalar dtype and selected evaluation.

use super::super::SubqueryRelation;
use super::{
    Binder, DType, Node, NodeKind, Nullability, Result, failed, field, validate_key_pair,
    validate_name, validate_unique, validate_width,
};
use crate::{
    local_primitives::native_relational_subquery::{Kind, Spec},
    relational_query::{
        VortexRelationalJoinKey, VortexRelationalSubquery, VortexRelationalSubqueryKind,
    },
};

impl Binder<'_> {
    pub(super) fn subquery(
        &mut self,
        query: &VortexRelationalSubquery,
        depth: usize,
        parameterized: bool,
    ) -> Result<Node> {
        validate_name(&query.output_column)?;
        if query.correlation.len() > 128 {
            return Err(failed("subquery correlation exceeds 128 keys"));
        }
        if parameterized && !query.correlation.is_empty() {
            return Err(failed(
                "parameterized correlation belongs in the inner tree before grouping and limit",
            ));
        }
        if matches!(query.kind, VortexRelationalSubqueryKind::Scalar)
            && (query.negated || !query.correlation.is_empty())
        {
            return Err(failed(
                "scalar subqueries require unnegated values and explicit parameterized correlation",
            ));
        }
        let columns = match &query.kind {
            VortexRelationalSubqueryKind::In { columns } => {
                validate_width(columns.len())?;
                columns.as_slice()
            }
            VortexRelationalSubqueryKind::Quantified { columns, .. } => {
                std::slice::from_ref(columns)
            }
            VortexRelationalSubqueryKind::Exists | VortexRelationalSubqueryKind::Scalar => &[],
        };
        self.charge((columns.len() + query.correlation.len() + 1) * 8192)?;
        let input = Box::new(self.bind(&query.input, depth + 1)?);
        let guard = query
            .evaluation_guard
            .as_ref()
            .map(|guard| self.expression(guard, &input.fields, 0))
            .transpose()?;
        if guard
            .as_ref()
            .is_some_and(|guard| !matches!(guard.dtype, DType::Bool(_)))
        {
            return Err(failed(
                "subquery evaluation guard requires a Boolean expression",
            ));
        }
        let relation =
            if let crate::relational_query::VortexRelationalPlan::DeferredSubquery(reference) =
                &query.relation
            {
                if !parameterized {
                    return Err(failed("deferred relation requires per-parameter execution"));
                }
                SubqueryRelation::Dynamic(self.take_deferred(reference)?)
            } else if parameterized {
                self.charge(input.fields.len() * 4096)?;
                let previous = self.outer_fields.replace(input.fields.clone());
                let previous_binding = std::mem::replace(&mut self.parameterized_binding, true);
                let relation = self.bind(&query.relation, depth + 1);
                self.outer_fields = previous;
                self.parameterized_binding = previous_binding;
                SubqueryRelation::Bound(Box::new(relation?))
            } else {
                SubqueryRelation::Bound(Box::new(self.bind(&query.relation, depth + 1)?))
            };
        validate_width(input.fields.len() + 1)?;
        self.charge(input.fields.len() * 4096)?;
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        for key in query.correlation.iter().chain(columns) {
            if let SubqueryRelation::Bound(relation) = &relation {
                bind_key(key, &input.fields, &relation.fields)?;
            } else {
                validate_name(key.left.as_str())?;
                validate_name(key.right.as_str())?;
                field(&input.fields, key.left.as_str())?;
            }
            left_keys.push(key.left.as_str().to_owned());
            right_keys.push(key.right.as_str().to_owned());
        }
        let (kind, dtype) = output_signature(&query.kind, &relation)?;
        let mut fields = input.fields.clone();
        fields.push((query.output_column.clone(), dtype));
        validate_unique(&fields)?;
        let spec = Spec {
            kind,
            guard,
            fields: fields.clone(),
            left_keys,
            right_keys,
            correlation: query.correlation.len(),
            negated: query.negated,
        };
        Ok(Node {
            fields,
            kind: NodeKind::Subquery {
                input,
                relation,
                spec,
                parameterized,
            },
        })
    }
}

fn output_signature(
    kind: &VortexRelationalSubqueryKind,
    relation: &SubqueryRelation,
) -> Result<(Kind, DType)> {
    let kind = match kind {
        VortexRelationalSubqueryKind::Scalar => {
            let SubqueryRelation::Bound(relation) = relation else {
                return Err(failed(
                    "scalar subquery requires a statically bound output schema",
                ));
            };
            let [(column, dtype)] = relation.fields.as_slice() else {
                return Err(failed("scalar subquery requires exactly one output column"));
            };
            return Ok((
                Kind::Scalar {
                    column: column.clone(),
                },
                dtype.as_nullable(),
            ));
        }
        VortexRelationalSubqueryKind::In { .. } => Kind::In,
        VortexRelationalSubqueryKind::Exists => Kind::Exists,
        VortexRelationalSubqueryKind::Quantified {
            comparison,
            quantifier,
            ..
        } => Kind::Quantified {
            comparison: *comparison,
            quantifier: *quantifier,
        },
    };
    let nullable = if matches!(kind, Kind::Exists) {
        Nullability::NonNullable
    } else {
        Nullability::Nullable
    };
    Ok((kind, DType::Bool(nullable)))
}

pub(in crate::local_primitives::prepared_relational) fn validate_relation(
    spec: &Spec,
    left: &[(String, DType)],
    right: &[(String, DType)],
) -> Result<()> {
    for (left_name, right_name) in spec.left_keys.iter().zip(&spec.right_keys) {
        validate_key_pair(field(left, left_name)?, field(right, right_name)?)?;
    }
    Ok(())
}

fn bind_key(
    key: &VortexRelationalJoinKey,
    left: &[(String, DType)],
    right: &[(String, DType)],
) -> Result<()> {
    validate_name(key.left.as_str())?;
    validate_name(key.right.as_str())?;
    validate_key_pair(
        field(left, key.left.as_str())?,
        field(right, key.right.as_str())?,
    )
}
