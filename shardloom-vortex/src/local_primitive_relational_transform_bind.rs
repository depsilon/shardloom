//! Schema-bound projection, predicates, ordering and output ranges.

use super::{
    Binder, DType, Node, NodeKind, Result, failed, field, native_relational_sort, validate_key,
    validate_name, validate_payload, validate_unique, validate_width,
};
use crate::relational_query::{
    VortexRelationalFilter, VortexRelationalLimit, VortexRelationalProject, VortexRelationalSort,
};

impl Binder<'_> {
    pub(super) fn project(
        &mut self,
        project: &VortexRelationalProject,
        depth: usize,
    ) -> Result<Node> {
        validate_width(project.expressions.len())?;
        self.charge_fields(project.expressions.len())?;
        let input = Box::new(self.bind(&project.input, depth + 1)?);
        let mut fields = Vec::new();
        let mut expressions = Vec::new();
        for (name, expression) in &project.expressions {
            validate_name(name)?;
            let mut expression = self.expression(expression, &input.fields, 0)?;
            // A wholly untyped NULL has no value domain to preserve. Give the
            // projected column a stable nullable boolean carrier, so downstream
            // validity reductions keep every NULL row without requiring a new
            // persistence type. Bind the entire expression before this coercion.
            if expression.dtype == DType::Null {
                use crate::local_primitives::native_relational_expression::{Expression, Kind};
                self.charge(4096)?;
                expression = Expression {
                    dtype: DType::Bool(super::Nullability::Nullable),
                    kind: Kind::Cast {
                        input: Box::new(expression),
                        tolerant: false,
                    },
                };
            }
            validate_payload(&expression.dtype)?;
            self.charge(
                usize::try_from(crate::local_primitives::native_payload::metadata_bytes(
                    &expression.dtype,
                )?)
                .map_err(crate::local_primitives::vortex_error)?,
            )?;
            fields.push((name.clone(), expression.dtype.clone()));
            expressions.push(expression);
        }
        validate_unique(&fields)?;
        Ok(Node {
            fields,
            kind: NodeKind::Project { input, expressions },
        })
    }

    pub(super) fn filter(&mut self, filter: &VortexRelationalFilter, depth: usize) -> Result<Node> {
        let input = Box::new(self.bind(&filter.input, depth + 1)?);
        let predicate = self.expression(&filter.predicate, &input.fields, 0)?;
        if !matches!(predicate.dtype, DType::Bool(_) | DType::Null) {
            return Err(failed("filter predicate must produce a nullable boolean"));
        }
        self.charge_fields(input.fields.len())?;
        Ok(Node {
            fields: input.fields.clone(),
            kind: NodeKind::Filter { input, predicate },
        })
    }

    pub(super) fn sort(&mut self, sort: &VortexRelationalSort, depth: usize) -> Result<Node> {
        validate_width(sort.keys.len())?;
        self.charge_fields(sort.keys.len())?;
        let input = Box::new(self.bind(&sort.input, depth + 1)?);
        self.charge_fields(input.fields.len())?;
        for key in &sort.keys {
            validate_name(key.column.as_str())?;
            validate_key(field(&input.fields, key.column.as_str())?)?;
        }
        let spec = native_relational_sort::Spec {
            copy_policy: crate::local_primitives::native_payload::CopyPolicy::ValidateValues,
            fields: input.fields.clone(),
            keys: sort.keys.clone(),
            names: sort
                .keys
                .iter()
                .map(|key| key.column.as_str().to_owned())
                .collect(),
        };
        Ok(Node {
            fields: input.fields.clone(),
            kind: NodeKind::Sort { input, spec },
        })
    }

    pub(super) fn limit(&mut self, limit: &VortexRelationalLimit, depth: usize) -> Result<Node> {
        let prefix = limit
            .offset
            .checked_add(limit.count)
            .ok_or_else(|| failed("output range overflow"))?;
        let mut input = Box::new(self.bind(&limit.input, depth + 1)?);
        if limit.count != 0 {
            cap_rolling_prefix(&mut input, prefix)?;
        }
        self.charge_fields(input.fields.len())?;
        Ok(Node {
            fields: input.fields.clone(),
            kind: NodeKind::Limit {
                input,
                offset: limit.offset,
                count: limit.count,
            },
        })
    }
}

fn cap_rolling_prefix(node: &mut Node, rows: usize) -> Result<()> {
    // Binding has already validated every expression and range. Projections
    // preserve order and cardinality; ranges contribute their skipped prefix.
    // Stop at every other operator, especially filters and sorting, whose
    // output prefix need not be a prefix of their input.
    match &mut node.kind {
        NodeKind::Project { input, .. } => cap_rolling_prefix(input, rows)?,
        NodeKind::Limit {
            input,
            offset,
            count,
        } if *count != 0 => {
            let prefix = offset
                .checked_add(rows.min(*count))
                .ok_or_else(|| failed("output range overflow"))?;
            cap_rolling_prefix(input, prefix)?;
        }
        NodeKind::Unary { operation, .. } => operation.cap_rolling_output(rows),
        _ => {}
    }
    Ok(())
}
