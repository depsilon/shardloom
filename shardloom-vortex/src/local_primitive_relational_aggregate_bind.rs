//! Bind native batch aggregation without source replay or a row-table adapter.

use super::{
    Binder, DType, Node, NodeKind, Nullability, PType, Result, failed, field, validate_name,
    validate_scalar, validate_unique, validate_width,
};
use crate::{
    local_primitives::{
        SimpleAggregateFunction as Function, native_relational_aggregate as kernel,
    },
    relational_query::VortexRelationalAggregate,
};

impl Binder<'_> {
    pub(super) fn aggregate(
        &mut self,
        aggregate: &VortexRelationalAggregate,
        depth: usize,
    ) -> Result<Node> {
        if aggregate.measures.is_empty() && aggregate.group_by.is_empty() {
            return Err(failed("aggregate requires a group key or measure"));
        }
        validate_width(
            aggregate
                .group_by
                .len()
                .checked_add(aggregate.measures.len())
                .ok_or_else(|| failed("aggregate width overflow"))?,
        )?;
        self.charge((aggregate.group_by.len() + aggregate.measures.len()) * 16_384)?;
        let input = Box::new(self.bind(&aggregate.input, depth + 1)?);
        let mut groups = Vec::new();
        for column in &aggregate.group_by {
            validate_name(column.as_str())?;
            validate_scalar(field(&input.fields, column.as_str())?)?;
            groups.push((
                column.as_str().to_owned(),
                field(&input.fields, column.as_str())?.clone(),
            ));
        }
        validate_unique(&groups)?;
        let mut fields = groups.clone();
        let mut measures = Vec::new();
        for measure in &aggregate.measures {
            validate_name(&measure.alias)?;
            if measure.argument_offset.is_some() || measure.value_transform.is_some() {
                return Err(failed(
                    "aggregate arguments must be lowered to explicit native projections",
                ));
            }
            let function = Function::parse(&measure.function)?;
            let (name, source) = match &measure.column {
                Some(column) => {
                    validate_name(column.as_str())?;
                    (
                        Some(column.as_str().to_owned()),
                        Some(field(&input.fields, column.as_str())?),
                    )
                }
                None if function == Function::Count => (None, None),
                None => return Err(failed("aggregate measure requires an input column")),
            };
            if let Some(source) = source {
                validate_scalar(source)?;
            }
            let dtype = match function {
                Function::Count | Function::CountDistinct => {
                    DType::Primitive(PType::U64, Nullability::NonNullable)
                }
                Function::Sum | Function::Avg => {
                    if !matches!(source, Some(DType::Primitive(_, _))) {
                        return Err(failed("SUM and AVG require numeric arguments"));
                    }
                    DType::Primitive(PType::F64, Nullability::Nullable)
                }
                Function::Min | Function::Max => {
                    source.expect("measure requires a column").as_nullable()
                }
            };
            let distinct_fields = if function == Function::CountDistinct {
                vec![
                    (
                        "group".into(),
                        DType::Primitive(PType::U64, Nullability::NonNullable),
                    ),
                    (
                        "value".into(),
                        source.expect("distinct has a column").clone(),
                    ),
                ]
            } else {
                vec![]
            };
            fields.push((measure.alias.clone(), dtype.clone()));
            measures.push(kernel::Measure {
                function,
                column: name,
                dtype,
                distinct_fields,
                distinct_names: vec!["group".into(), "value".into()],
            });
        }
        validate_unique(&fields)?;
        let spec = kernel::Spec {
            fields: fields.clone(),
            group_names: groups.iter().map(|(name, _)| name.clone()).collect(),
            groups,
            measures,
        };
        Ok(Node {
            fields,
            kind: NodeKind::Aggregate { input, spec },
        })
    }
}
