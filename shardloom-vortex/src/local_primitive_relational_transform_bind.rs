//! Schema-bound projection, predicates, ordering and output ranges.

use super::{
    Binder, DType, Node, NodeKind, Result, failed, field, native_relational_sort, validate_name,
    validate_scalar, validate_unique, validate_width,
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
        self.charge(project.expressions.len() * 4096)?;
        let input = Box::new(self.bind(&project.input, depth + 1)?);
        let mut fields = Vec::new();
        let mut expressions = Vec::new();
        for (name, expression) in &project.expressions {
            validate_name(name)?;
            let expression = self.expression(expression, &input.fields, 0)?;
            validate_scalar(&expression.dtype)?;
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
        self.charge(input.fields.len() * 4096)?;
        Ok(Node {
            fields: input.fields.clone(),
            kind: NodeKind::Filter { input, predicate },
        })
    }

    pub(super) fn sort(&mut self, sort: &VortexRelationalSort, depth: usize) -> Result<Node> {
        validate_width(sort.keys.len())?;
        self.charge(sort.keys.len() * 4096)?;
        let input = Box::new(self.bind(&sort.input, depth + 1)?);
        self.charge(input.fields.len() * 4096)?;
        for key in &sort.keys {
            validate_name(key.column.as_str())?;
            field(&input.fields, key.column.as_str())?;
        }
        let spec = native_relational_sort::Spec {
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
        limit
            .offset
            .checked_add(limit.count)
            .ok_or_else(|| failed("output range overflow"))?;
        let input = Box::new(self.bind(&limit.input, depth + 1)?);
        self.charge(input.fields.len() * 4096)?;
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
