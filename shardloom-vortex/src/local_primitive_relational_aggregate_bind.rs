//! Bind native batch aggregation without source replay or a row-table adapter.

use super::{
    Binder, DType, Node, NodeKind, Nullability, PType, Result, failed, field, validate_key,
    validate_name, validate_unique, validate_width,
};
use crate::{
    local_primitives::{
        SimpleAggregateFunction as Function, native_decimal_reduce,
        native_relational_aggregate as kernel,
    },
    relational_query::VortexRelationalAggregate,
};

impl Binder<'_> {
    pub(super) fn aggregate(
        &mut self,
        aggregate: &VortexRelationalAggregate,
        depth: usize,
    ) -> Result<Node> {
        let input = Box::new(self.bind(&aggregate.input, depth + 1)?);
        let spec = self.aggregate_spec(&input.fields, &aggregate.group_by, &aggregate.measures)?;
        Ok(Node {
            fields: spec.fields.clone(),
            kind: NodeKind::Aggregate { input, spec },
        })
    }

    pub(super) fn aggregate_spec(
        &mut self,
        input_fields: &[(String, DType)],
        group_by: &[shardloom_core::ColumnRef],
        input_measures: &[crate::query_primitive::VortexSimpleAggregateMeasure],
    ) -> Result<kernel::Spec> {
        if input_measures.is_empty() && group_by.is_empty() {
            return Err(failed("aggregate requires a group key or measure"));
        }
        let width = group_by
            .len()
            .checked_add(input_measures.len())
            .ok_or_else(|| failed("aggregate width overflow"))?;
        validate_width(width)?;
        self.charge_items(width, 16_384)?;
        let mut groups = Vec::new();
        for column in group_by {
            validate_name(column.as_str())?;
            validate_key(field(input_fields, column.as_str())?)?;
            groups.push((
                column.as_str().to_owned(),
                field(input_fields, column.as_str())?.clone(),
            ));
        }
        validate_unique(&groups)?;
        let mut fields = groups.clone();
        let mut measures = Vec::new();
        for measure in input_measures {
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
                        Some(field(input_fields, column.as_str())?),
                    )
                }
                None if function == Function::Count => (None, None),
                None => return Err(failed("aggregate measure requires an input column")),
            };
            if let Some(source) = source {
                validate_key(source)?;
            }
            let decimal_source = match (function, source) {
                (Function::Sum | Function::Avg, Some(DType::Decimal(decimal, _))) => Some(*decimal),
                _ => None,
            };
            let dtype = reduction_dtype(function, source)?;
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
                decimal_source,
                distinct_fields,
                distinct_names: vec!["group".into(), "value".into()],
            });
        }
        validate_unique(&fields)?;
        Ok(kernel::Spec {
            fields,
            group_names: groups.iter().map(|(name, _)| name.clone()).collect(),
            groups,
            measures,
        })
    }
}

pub(super) fn reduction_dtype(function: Function, source: Option<&DType>) -> Result<DType> {
    Ok(match function {
        Function::Count | Function::CountDistinct => {
            DType::Primitive(PType::U64, Nullability::NonNullable)
        }
        Function::Sum | Function::Avg => match source {
            Some(DType::Decimal(decimal, _)) => DType::Decimal(
                native_decimal_reduce::output_dtype(*decimal, function == Function::Avg)?,
                Nullability::Nullable,
            ),
            Some(DType::Primitive(_, _)) => DType::Primitive(PType::F64, Nullability::Nullable),
            _ => return Err(failed("SUM and AVG require numeric arguments")),
        },
        Function::Min | Function::Max => source
            .ok_or_else(|| failed("aggregate measure requires an input column"))?
            .as_nullable(),
    })
}
