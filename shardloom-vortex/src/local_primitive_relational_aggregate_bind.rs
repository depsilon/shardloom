//! Bind native batch aggregation without source replay or a row-table adapter.

use super::{
    Binder, DType, Node, NodeKind, Nullability, PType, Result, failed, field, validate_key,
    validate_name, validate_unique, validate_width,
};
use crate::{
    local_primitives::{
        native_decimal_reduce, native_relational_aggregate as kernel,
        native_relational_aggregate::Function,
    },
    relational_query::VortexRelationalAggregate,
};

pub(super) fn filter_column(
    measure: &crate::query_primitive::VortexSimpleAggregateMeasure,
    fields: &[(String, DType)],
) -> Result<Option<String>> {
    measure
        .filter_column
        .as_ref()
        .map(|column| {
            validate_name(column.as_str())?;
            if !matches!(
                field(fields, column.as_str())?,
                DType::Bool(_) | DType::Null
            ) {
                return Err(failed("aggregate FILTER requires a Boolean input column"));
            }
            Ok(column.as_str().to_owned())
        })
        .transpose()
}

impl Binder<'_> {
    pub(super) fn aggregate(
        &mut self,
        aggregate: &VortexRelationalAggregate,
        depth: usize,
    ) -> Result<Node> {
        let mut input = Box::new(self.bind(&aggregate.input, depth + 1)?);
        let spec = if aggregate.grouping_sets.is_some()
            && aggregate.group_by.is_empty()
            && aggregate.measures.is_empty()
        {
            kernel::Spec {
                fields: vec![],
                groups: vec![],
                group_names: vec![],
                measures: vec![],
            }
        } else {
            self.aggregate_spec(&input.fields, &aggregate.group_by, &aggregate.measures)?
        };
        self.narrow_source_demand(
            &mut input,
            spec.group_names.iter().map(String::as_str).chain(
                spec.measures
                    .iter()
                    .flat_map(kernel::Measure::input_columns),
            ),
        )?;
        if let Some(grouping) = &aggregate.grouping_sets {
            return self.grouping_aggregate(input, spec, grouping);
        }
        Ok(Node {
            fields: spec.fields.clone(),
            kind: NodeKind::Aggregate {
                input,
                spec,
                expansion: None,
            },
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
            let bound = self.aggregate_measure(measure, input_fields)?;
            fields.push((measure.alias.clone(), bound.dtype.clone()));
            measures.push(bound);
        }
        validate_unique(&fields)?;
        Ok(kernel::Spec {
            fields,
            group_names: groups.iter().map(|(name, _)| name.clone()).collect(),
            groups,
            measures,
        })
    }

    fn aggregate_measure(
        &mut self,
        measure: &crate::query_primitive::VortexSimpleAggregateMeasure,
        input_fields: &[(String, DType)],
    ) -> Result<kernel::Measure> {
        validate_name(&measure.alias)?;
        if measure.argument_offset.is_some() || measure.value_transform.is_some() {
            return Err(failed(
                "aggregate arguments must be lowered to explicit native projections",
            ));
        }
        let function = Function::parse(&measure.function)?;
        let filter_column = filter_column(measure, input_fields)?;
        function.validate_separator(measure.separator.as_deref())?;
        self.charge_items(measure.separator.as_ref().map_or(0, String::len), 2)?;
        let quantile_level = function.quantile_level(measure.quantile_level)?;
        if function.requires_secondary() != measure.secondary_column.is_some() {
            return Err(failed(
                "paired statistics and arg_min/arg_max require exactly two input columns; other reducers require one",
            ));
        }
        if !measure.order_by.is_empty() && !function.is_ordered_value() {
            return Err(failed(
                "aggregate-local ordering requires an ordered value reducer",
            ));
        }
        let secondary = measure
            .secondary_column
            .as_ref()
            .map(|column| {
                validate_name(column.as_str())?;
                let source = field(input_fields, column.as_str())?;
                if function.is_pair() {
                    reduction_dtype(function, Some(source))?;
                } else {
                    validate_key(source)?;
                }
                Ok(column.as_str().to_owned())
            })
            .transpose()?;
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
            (
                Function::Sum | Function::Avg | Function::SumDistinct | Function::AvgDistinct,
                Some(DType::Decimal(decimal, _)),
            ) => Some(*decimal),
            _ => None,
        };
        let dtype = declared_reduction_dtype(function, source, measure.decimal_output)?;
        let selection = if function.is_ordered_value() {
            Some(self.selection_spec(measure, input_fields)?)
        } else {
            None
        };
        let distinct_fields = if function.is_distinct() {
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
        Ok(kernel::Measure {
            function,
            column: name,
            secondary_column: secondary,
            quantile_level,
            decimal_output: measure.decimal_output,
            separator: measure.separator.clone(),
            filter_column,
            selection,
            dtype,
            input_ptype: match source {
                Some(DType::Primitive(ptype, _)) => Some(*ptype),
                _ => None,
            },
            decimal_source,
            distinct_fields,
            distinct_names: vec!["group".into(), "value".into()],
        })
    }

    fn selection_spec(
        &mut self,
        measure: &crate::query_primitive::VortexSimpleAggregateMeasure,
        input_fields: &[(String, DType)],
    ) -> Result<kernel::SelectionSpec> {
        if !measure.order_by.is_empty() {
            validate_width(measure.order_by.len())?;
        }
        self.charge_items(measure.order_by.len() + 2, 16_384)?;
        let mut fields = Vec::new();
        for column in measure.input_columns() {
            validate_name(column.as_str())?;
            let dtype = field(input_fields, column.as_str())?;
            validate_key(dtype)?;
            if !fields.iter().any(|(name, _)| name == column.as_str()) {
                fields.push((column.as_str().to_owned(), dtype.clone()));
            }
        }
        Ok(kernel::SelectionSpec {
            fields,
            names: measure
                .secondary_column
                .iter()
                .map(|column| column.as_str().to_owned())
                .chain(
                    measure
                        .order_by
                        .iter()
                        .map(|key| key.column.as_str().to_owned()),
                )
                .collect(),
            order: measure.order_by.clone(),
        })
    }
}

pub(super) fn declared_reduction_dtype(
    function: Function,
    source: Option<&DType>,
    output: Option<shardloom_core::decimal_output::DecimalOutput>,
) -> Result<DType> {
    let default = reduction_dtype(function, source)?;
    let Some(output) = output else {
        return Ok(default);
    };
    if !matches!(
        function,
        Function::Sum
            | Function::Avg
            | Function::SumDistinct
            | Function::AvgDistinct
            | Function::Median
            | Function::QuantileCont
    ) || !matches!(source, Some(DType::Decimal(_, _)))
    {
        return Err(failed(
            "decimal output requires Decimal128 SUM, AVG, their DISTINCT forms, MEDIAN or QUANTILE_CONT",
        ));
    }
    Ok(DType::Decimal(
        native_decimal_reduce::declared_dtype(output),
        Nullability::Nullable,
    ))
}

pub(super) fn reduction_dtype(
    function: impl Into<Function>,
    source: Option<&DType>,
) -> Result<DType> {
    let function = function.into();
    Ok(match function {
        Function::Count | Function::CountDistinct => {
            DType::Primitive(PType::U64, Nullability::NonNullable)
        }
        Function::Sum | Function::Avg | Function::SumDistinct | Function::AvgDistinct => {
            match source {
                Some(DType::Decimal(decimal, _)) => DType::Decimal(
                    native_decimal_reduce::output_dtype(*decimal, function.is_average())?,
                    Nullability::Nullable,
                ),
                Some(DType::Primitive(_, _)) => DType::Primitive(PType::F64, Nullability::Nullable),
                _ => return Err(failed("SUM and AVG require numeric arguments")),
            }
        }
        Function::Min
        | Function::Max
        | Function::First
        | Function::Last
        | Function::ArgMin
        | Function::ArgMax => source
            .ok_or_else(|| failed("aggregate measure requires an input column"))?
            .as_nullable(),
        Function::List => DType::List(
            source
                .ok_or_else(|| failed("list aggregation requires an input column"))?
                .clone()
                .into(),
            Nullability::Nullable,
        ),
        Function::StringAgg => match source {
            Some(DType::Utf8(_)) => DType::Utf8(Nullability::Nullable),
            _ => return Err(failed("string_agg requires a UTF-8 argument")),
        },
        Function::Median | Function::QuantileCont | Function::QuantileDisc => {
            let source = source.ok_or_else(|| failed("quantile requires an input column"))?;
            match source {
                DType::Primitive(_, _) if function == Function::QuantileDisc => {
                    source.as_nullable()
                }
                DType::Primitive(_, _) => DType::Primitive(PType::F64, Nullability::Nullable),
                DType::Decimal(decimal, _) => {
                    let output = native_decimal_reduce::output_dtype(
                        *decimal,
                        function != Function::QuantileDisc,
                    )?;
                    if function == Function::QuantileDisc {
                        source.as_nullable()
                    } else {
                        DType::Decimal(output, Nullability::Nullable)
                    }
                }
                _ => {
                    return Err(failed(
                        "quantiles require primitive numeric or Decimal128 arguments",
                    ));
                }
            }
        }
        Function::VarPop
        | Function::VarSamp
        | Function::StddevPop
        | Function::StddevSamp
        | Function::CovarPop
        | Function::CovarSamp
        | Function::Corr => {
            if !matches!(source, Some(DType::Primitive(_, _))) {
                return Err(failed(
                    "statistical aggregates require primitive numeric arguments",
                ));
            }
            DType::Primitive(PType::F64, Nullability::Nullable)
        }
    })
}
