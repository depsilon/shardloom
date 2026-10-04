//! Bind window key domains, navigation types and reusable partition ordering.

use super::{
    Binder, DType, Node, NodeKind, Nullability, PType, Result, failed, field, validate_name,
    validate_scalar, validate_unique, validate_width,
};
use crate::{
    local_primitives::native_relational_window as kernel,
    relational_query::{
        VortexRelationalWindow, VortexRelationalWindowExpression,
        VortexRelationalWindowFunction as Function,
    },
};

impl Binder<'_> {
    pub(super) fn window(&mut self, window: &VortexRelationalWindow, depth: usize) -> Result<Node> {
        if window.expressions.is_empty() {
            return Err(failed("window requires at least one expression"));
        }
        let width = window
            .columns
            .len()
            .checked_add(window.expressions.len())
            .ok_or_else(|| failed("window schema width overflow"))?;
        validate_width(width)?;
        self.charge(width * 16_384)?;
        let input = Box::new(self.bind(&window.input, depth + 1)?);
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for column in &window.columns {
            validate_name(column.as_str())?;
            fields.push((
                column.as_str().to_owned(),
                field(&input.fields, column.as_str())?.clone(),
            ));
            columns.push(column.as_str().to_owned());
        }
        let mut spec = kernel::Spec {
            fields: vec![],
            columns,
            keys: vec![],
            functions: vec![],
            groups: vec![],
        };
        for expression in &window.expressions {
            validate_name(&expression.output_column)?;
            let dtype = function_dtype(&expression.function, &input.fields)?;
            fields.push((expression.output_column.clone(), dtype));
            self.window_group(expression, &input.fields, &mut spec)?;
        }
        validate_unique(&fields)?;
        spec.fields.clone_from(&fields);
        Ok(Node {
            fields,
            kind: NodeKind::Window { input, spec },
        })
    }

    fn window_group(
        &mut self,
        expression: &VortexRelationalWindowExpression,
        fields: &[(String, DType)],
        spec: &mut kernel::Spec,
    ) -> Result<()> {
        validate_width(expression.order_by.len())?;
        if expression.partition_by.len() > 128 {
            return Err(failed("window partition exceeds 128 columns"));
        }
        self.charge((expression.partition_by.len() + expression.order_by.len()) * 4096)?;
        let mut partition = Vec::new();
        for column in &expression.partition_by {
            let index = key(spec, fields, column.as_str())?;
            if partition.contains(&index) {
                return Err(failed("window partition columns must be unique"));
            }
            partition.push(index);
        }
        let order = expression
            .order_by
            .iter()
            .map(|column| {
                Ok(kernel::OrderKey {
                    key: key(spec, fields, column.column.as_str())?,
                    descending: column.descending,
                    nulls: column.nulls,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let function = spec.functions.len();
        spec.functions.push(expression.function.clone());
        if let Some(group) = spec
            .groups
            .iter_mut()
            .find(|group| group.partition == partition && group.order == order)
        {
            group.functions.push(function);
        } else {
            spec.groups.push(kernel::Group {
                partition,
                order,
                functions: vec![function],
            });
        }
        Ok(())
    }
}

fn key(spec: &mut kernel::Spec, fields: &[(String, DType)], name: &str) -> Result<usize> {
    validate_name(name)?;
    validate_scalar(field(fields, name)?)?;
    if let Some(index) = spec.keys.iter().position(|key| key == name) {
        return Ok(index);
    }
    spec.keys.push(name.to_owned());
    Ok(spec.keys.len() - 1)
}

fn function_dtype(function: &Function, fields: &[(String, DType)]) -> Result<DType> {
    Ok(match function {
        Function::Lag { column, offset } | Function::Lead { column, offset } => {
            if *offset == 0 {
                return Err(failed("window offset must be positive"));
            }
            validate_name(column.as_str())?;
            field(fields, column.as_str())?.as_nullable()
        }
        Function::PercentRank | Function::CumeDist => {
            DType::Primitive(PType::F64, Nullability::NonNullable)
        }
        Function::Ntile { buckets: 0 } => {
            return Err(failed("NTILE bucket count must be positive"));
        }
        _ => DType::Primitive(PType::I64, Nullability::NonNullable),
    })
}
