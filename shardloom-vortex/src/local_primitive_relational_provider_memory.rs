//! Provider-owned declaration capacity, independent of the binder's schema,
//! expression recipes and general-runtime preparation credits.

use super::{Request, Result, failed};
use crate::local_primitives::{
    native_relational_expression::{Expression, Kind},
    prepared_relational::{Node, NodeKind},
};
use shardloom_core::{PredicateExpr, StatValue};

struct Size(u64);

impl Size {
    fn add(&mut self, bytes: usize) -> Result<()> {
        self.0 = self
            .0
            .checked_add(bytes as u64)
            .ok_or_else(|| failed("provider declaration capacity overflow"))?;
        Ok(())
    }

    fn name(&mut self, name: &str) -> Result<()> {
        self.add(64)?;
        self.add(name.len())
    }

    fn predicate(&mut self, predicate: &PredicateExpr) -> Result<()> {
        self.add(256)?;
        if let Some(column) = predicate.column() {
            self.name(column.as_str())?;
        }
        match predicate {
            PredicateExpr::And(children) => {
                for child in children {
                    self.predicate(child)?;
                }
            }
            PredicateExpr::Compare {
                value: StatValue::Utf8(value),
                ..
            }
            | PredicateExpr::StringContains { needle: value, .. } => self.name(value)?,
            PredicateExpr::InList { values, .. } => {
                for value in values {
                    self.add(64)?;
                    if let StatValue::Utf8(value) = value {
                        self.name(value)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn expression(&mut self, expression: &Expression) -> Result<bool> {
        self.add(2048)?;
        match &expression.kind {
            Kind::Column(_) => {}
            Kind::Literal(value) => {
                if let Some(vortex::array::scalar::ScalarValue::Utf8(value)) = value.value() {
                    self.add(
                        value
                            .as_str()
                            .len()
                            .checked_mul(16)
                            .ok_or_else(|| failed("provider literal capacity overflow"))?,
                    )?;
                }
            }
            Kind::Unary(_, child) => return self.expression(child),
            Kind::Binary(left, _, right) | Kind::Compare(left, _, right) => {
                if !self.expression(left)? || !self.expression(right)? {
                    return Ok(false);
                }
            }
            Kind::Conditional(condition, yes, no) => {
                if !self.expression(condition)? || !self.expression(yes)? || !self.expression(no)? {
                    return Ok(false);
                }
            }
            Kind::Function { args, .. } => {
                for arg in args {
                    if !self.expression(arg)? {
                        return Ok(false);
                    }
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn node(&mut self, node: &Node) -> Result<bool> {
        // Names are binder-validated to 256 bytes. Each slot covers the old
        // and new alias map, transformed value (up to three source columns),
        // output/order/HAVING names and vector/tree allocation overhead.
        self.add(
            node.fields
                .len()
                .checked_mul(4096)
                .ok_or_else(|| failed("provider field capacity overflow"))?,
        )?;
        match &node.kind {
            NodeKind::Scan {
                source_uri,
                predicate,
                ..
            } => {
                self.add(
                    source_uri
                        .as_str()
                        .len()
                        .checked_mul(16)
                        .ok_or_else(|| failed("provider source capacity overflow"))?,
                )?;
                if let Some(predicate) = predicate {
                    let mut size = Size(0);
                    size.predicate(predicate)?;
                    self.0 = self
                        .0
                        .checked_add(
                            size.0
                                .checked_mul(16)
                                .ok_or_else(|| failed("provider predicate capacity overflow"))?,
                        )
                        .ok_or_else(|| failed("provider declaration capacity overflow"))?;
                }
                Ok(true)
            }
            NodeKind::Project { input, expressions } => {
                for expression in expressions {
                    if !self.expression(expression)? {
                        return Ok(false);
                    }
                }
                self.node(input)
            }
            NodeKind::Filter { input, predicate } => {
                if !self.expression(predicate)? {
                    return Ok(false);
                }
                self.node(input)
            }
            NodeKind::Aggregate { input, .. }
            | NodeKind::Sort { input, .. }
            | NodeKind::Limit { input, .. } => self.node(input),
            _ => Ok(false),
        }
    }
}

/// Reserve alias/lowering scratch before copying, without cloning bound nodes.
pub(super) fn scratch(root: &Node) -> Result<Option<u64>> {
    let mut size = Size(8192);
    Ok(size.node(root)?.then_some(size.0))
}

/// Bound the request, compiled scan, aggregate rewrite, field validation and
/// provider report copies. Sixteen covers their overlapping owned names and
/// predicates; source schemas and bound expression programs remain borrowed.
/// This credit replaces scratch once lowering has dropped its temporary maps.
pub(super) fn retained(request: &Request, output: &[String], names: &[String]) -> Result<u64> {
    let mut size = Size(0);
    size.add(std::mem::size_of_val(request))?;
    if let Some(uri) = &request.source_uri {
        size.name(uri.as_str())?;
    }
    if let shardloom_plan::ProjectionRequest::Columns(columns) = &request.projection {
        for column in columns {
            size.name(column.as_str())?;
        }
    }
    if let Some(predicate) = &request.predicate {
        size.predicate(predicate)?;
    }
    for name in output.iter().chain(names) {
        size.name(name)?;
    }
    let order = if let Some(aggregate) = &request.simple_aggregate {
        for column in &aggregate.group_by {
            size.name(column.as_str())?;
        }
        for expression in &aggregate.group_expressions {
            size.add(std::mem::size_of_val(expression))?;
            size.name(&expression.alias)?;
            size.name(expression.column.as_str())?;
            size.name(&expression.function)?;
            for column in &expression.extra_columns {
                size.name(column.as_str())?;
            }
        }
        for measure in &aggregate.measures {
            size.add(std::mem::size_of_val(measure))?;
            size.name(&measure.function)?;
            size.name(&measure.alias)?;
            if let Some(column) = &measure.column {
                size.name(column.as_str())?;
            }
            if let Some(function) = &measure.value_transform {
                size.name(function)?;
            }
        }
        for predicate in &aggregate.having {
            size.add(std::mem::size_of_val(predicate))?;
            size.name(&predicate.column)?;
            size.name(&predicate.value)?;
        }
        aggregate.order_by.as_slice()
    } else {
        request
            .sort_rows
            .as_ref()
            .map_or(&[][..], |sort| sort.order_by.as_slice())
    };
    for key in order {
        size.add(std::mem::size_of_val(key))?;
        size.name(&key.column)?;
    }
    size.0
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(|| failed("provider request capacity overflow"))
}
