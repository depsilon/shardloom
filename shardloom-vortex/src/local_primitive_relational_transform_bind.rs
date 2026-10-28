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
        let input = self.bind(&project.input, depth + 1)?;
        self.project_input(&project.expressions, input)
    }

    pub(super) fn project_input(
        &mut self,
        projected: &[(String, shardloom_core::Expression)],
        input: Node,
    ) -> Result<Node> {
        validate_width(projected.len())?;
        self.charge_fields(projected.len())?;
        let mut input = Box::new(input);
        let mut fields = Vec::new();
        let mut expressions = Vec::new();
        for (name, expression) in projected {
            validate_name(name)?;
            let mut expression = self.expression(expression, &input.fields, 0)?;
            // A wholly untyped NULL has no value domain to preserve. Give the
            // projected column a stable nullable boolean carrier, so downstream
            // validity reductions keep every NULL row without requiring a new
            // persistence type. Bind the entire expression before this coercion.
            if expression.dtype == DType::Null {
                use crate::local_primitives::native_relational_expression::{
                    Expression, Kind, PREPARATION_BYTES,
                };
                self.charge(PREPARATION_BYTES)?;
                expression = Expression::new(
                    DType::Bool(super::Nullability::Nullable),
                    Kind::Cast {
                        input: Box::new(expression),
                        tolerant: false,
                        temporal: None,
                    },
                );
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
        // Normalize only after every expression and payload has passed ordinary
        // binding. Identical names alone do not preserve types, order or NULL
        // carrier coercion, and a computed expression may still fail at runtime.
        if fields == input.fields
            && expressions
                .iter()
                .zip(&input.fields)
                .all(|(expression, (name, _))| {
                    matches!(
                        &expression.kind,
                        crate::local_primitives::native_relational_expression::Kind::Column(column)
                            if column == name
                    )
                })
        {
            return Ok(*input);
        }
        self.narrow_scan_projection(&mut input, &expressions)?;
        crate::local_primitives::native_relational_expression::Expression::bind_projection_reuse(
            &mut expressions,
        );
        Ok(Node {
            fields,
            kind: NodeKind::Project { input, expressions },
        })
    }

    pub(super) fn filter(&mut self, filter: &VortexRelationalFilter, depth: usize) -> Result<Node> {
        let input = self.bind(&filter.input, depth + 1)?;
        self.filter_input(&filter.predicate, input)
    }

    pub(super) fn filter_input(
        &mut self,
        expression: &shardloom_core::Expression,
        input: Node,
    ) -> Result<Node> {
        let mut input = Box::new(input);
        let metadata_before = self.metadata.bytes();
        let predicate = self.expression(expression, &input.fields, 0)?;
        let predicate_metadata = self.metadata.bytes() - metadata_before;
        if !matches!(predicate.dtype, DType::Bool(_) | DType::Null) {
            return Err(failed("filter predicate must produce a nullable boolean"));
        }
        self.charge_fields(input.fields.len())?;
        if self.push_scan_filter(&mut input, &predicate, predicate_metadata)? {
            return Ok(*input);
        }
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
            kind: NodeKind::Sort {
                input,
                spec,
                output_prefix: None,
            },
        })
    }

    pub(super) fn limit(&mut self, limit: &VortexRelationalLimit, depth: usize) -> Result<Node> {
        let input = self.bind(&limit.input, depth + 1)?;
        self.limit_input(limit.offset, limit.count, input)
    }

    pub(super) fn limit_input(&mut self, offset: usize, count: usize, input: Node) -> Result<Node> {
        let prefix = offset
            .checked_add(count)
            .ok_or_else(|| failed("output range overflow"))?;
        let mut input = Box::new(input);
        // Only an immediate Sort may omit output. Intervening projections and
        // filters can still observe rows that this Limit will discard.
        if let NodeKind::Sort { output_prefix, .. } = &mut input.kind {
            *output_prefix = Some(if count == 0 { 0 } else { prefix });
        }
        if count != 0 {
            cap_rolling_prefix(&mut input, prefix)?;
        }
        self.charge_fields(input.fields.len())?;
        Ok(Node {
            fields: input.fields.clone(),
            kind: NodeKind::Limit {
                input,
                offset,
                count,
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
