//! Typed relation construction only; all execution stays in the shared engine.

use super::{expression::NativeExpression, native_error};
use pyo3::{exceptions::PyValueError, prelude::*};
use shardloom_core::{ColumnRef, ComparisonOp};
use shardloom_runtime::native::NativeRelationDeclaration;
use shardloom_vortex::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        NATIVE_RECURSION_LIMIT, VortexRelationalAggregate, VortexRelationalFilter,
        VortexRelationalJoin, VortexRelationalJoinColumn, VortexRelationalJoinKey,
        VortexRelationalJoinKind, VortexRelationalLimit, VortexRelationalNullOrder,
        VortexRelationalOrderKey, VortexRelationalPlan as Plan, VortexRelationalProject,
        VortexRelationalQuantifier, VortexRelationalSet, VortexRelationalSetKind,
        VortexRelationalSide, VortexRelationalSort, VortexRelationalSubquery,
        VortexRelationalSubqueryKind, VortexRelationalWindow,
    },
};
use std::sync::Arc;

#[pyclass(name = "Relation", module = "shardloom._native", frozen)]
pub(super) struct NativeRelation {
    pub(super) declaration: Arc<NativeRelationDeclaration>,
}

impl NativeRelation {
    pub(super) fn node(plan: Plan) -> PyResult<Self> {
        Ok(Self {
            declaration: Arc::new(NativeRelationDeclaration::new(plan).map_err(native_error)?),
        })
    }

    pub(super) fn child(&self) -> PyResult<Plan> {
        if self.declaration.depth() >= NATIVE_RECURSION_LIMIT {
            return Err(PyValueError::new_err("recursive plan exceeds 24 levels"));
        }
        Ok(self.declaration.plan().clone())
    }
}

pub(super) fn column(name: String) -> PyResult<ColumnRef> {
    ColumnRef::new(name).map_err(native_error)
}

pub(super) fn order_keys(
    keys: Vec<(String, bool, Option<String>)>,
) -> PyResult<Vec<VortexRelationalOrderKey>> {
    keys.into_iter()
        .map(|(name, descending, nulls)| {
            let nulls = match nulls.as_deref() {
                None => None,
                Some("first") => Some(VortexRelationalNullOrder::First),
                Some("last") => Some(VortexRelationalNullOrder::Last),
                Some(_) => {
                    return Err(PyValueError::new_err(
                        "null placement must be first, last or None",
                    ));
                }
            };
            Ok(VortexRelationalOrderKey {
                column: column(name)?,
                descending,
                nulls,
            })
        })
        .collect()
}

#[pymethods]
impl NativeRelation {
    #[staticmethod]
    fn scan(reference: &str) -> PyResult<Self> {
        Ok(Self {
            declaration: Arc::new(
                NativeRelationDeclaration::scan(reference).map_err(native_error)?,
            ),
        })
    }

    /// The current outer row, valid inside a correlated subquery relation.
    #[staticmethod]
    fn outer() -> PyResult<Self> {
        Self::node(Plan::Outer)
    }

    fn window(
        &self,
        columns: Vec<String>,
        expressions: Vec<PyRef<'_, super::window::NativeWindowExpression>>,
    ) -> PyResult<Self> {
        Self::node(Plan::Window(Box::new(VortexRelationalWindow {
            input: self.child()?,
            columns: columns.into_iter().map(column).collect::<PyResult<_>>()?,
            expressions: expressions
                .into_iter()
                .map(|expression| expression.value.clone())
                .collect(),
        })))
    }

    /// Append a typed scalar/boolean subquery result for ordinary expressions.
    #[pyo3(signature = (relation, output_column, *, kind="scalar", keys=Vec::new(), correlation=Vec::new(), comparison="eq", quantifier="any", negated=false, guard=None, correlated=false))]
    #[allow(clippy::too_many_arguments)] // The shared declaration has explicit, keyword-only semantic fields.
    fn subquery(
        &self,
        relation: &Self,
        output_column: String,
        kind: &str,
        keys: Vec<(String, String)>,
        correlation: Vec<(String, String)>,
        comparison: &str,
        quantifier: &str,
        negated: bool,
        guard: Option<&NativeExpression>,
        correlated: bool,
    ) -> PyResult<Self> {
        if kind != "quantified" && (comparison != "eq" || quantifier != "any") {
            return Err(PyValueError::new_err(
                "comparison and quantifier options require a quantified subquery",
            ));
        }
        let mut keys = join_keys(keys)?;
        let kind = match kind {
            "scalar" if keys.is_empty() => VortexRelationalSubqueryKind::Scalar,
            "exists" if keys.is_empty() => VortexRelationalSubqueryKind::Exists,
            "in" => VortexRelationalSubqueryKind::In { columns: keys },
            "quantified" if keys.len() == 1 => {
                let comparison = match comparison {
                    "eq" => ComparisonOp::Eq,
                    "not_eq" => ComparisonOp::NotEq,
                    "lt" => ComparisonOp::Lt,
                    "lt_eq" => ComparisonOp::LtEq,
                    "gt" => ComparisonOp::Gt,
                    "gt_eq" => ComparisonOp::GtEq,
                    _ => return Err(PyValueError::new_err("unsupported subquery comparison")),
                };
                let quantifier = match quantifier {
                    "any" => VortexRelationalQuantifier::Any,
                    "all" => VortexRelationalQuantifier::All,
                    _ => {
                        return Err(PyValueError::new_err(
                            "subquery quantifier must be any or all",
                        ));
                    }
                };
                VortexRelationalSubqueryKind::Quantified {
                    columns: keys.remove(0),
                    comparison,
                    quantifier,
                }
            }
            _ => {
                return Err(PyValueError::new_err(
                    "subquery kind/keys are invalid; scalar/exists use no keys and quantified uses one key",
                ));
            }
        };
        let node = Box::new(VortexRelationalSubquery {
            input: self.child()?,
            relation: relation.child()?,
            kind,
            evaluation_guard: guard.map(|value| (*value.value).clone()),
            correlation: join_keys(correlation)?,
            output_column,
            negated,
        });
        Self::node(if correlated {
            Plan::CorrelatedSubquery(node)
        } else {
            Plan::Subquery(node)
        })
    }

    fn project(&self, expressions: Vec<(String, PyRef<'_, NativeExpression>)>) -> PyResult<Self> {
        Self::node(Plan::Project(Box::new(VortexRelationalProject {
            input: self.child()?,
            expressions: expressions
                .into_iter()
                .map(|(name, expr)| (name, (*expr.value).clone()))
                .collect(),
        })))
    }

    fn filter(&self, predicate: &NativeExpression) -> PyResult<Self> {
        Self::node(Plan::Filter(Box::new(VortexRelationalFilter {
            input: self.child()?,
            predicate: (*predicate.value).clone(),
        })))
    }

    fn sort(&self, keys: Vec<(String, bool, Option<String>)>) -> PyResult<Self> {
        Self::node(Plan::Sort(Box::new(VortexRelationalSort {
            input: self.child()?,
            keys: order_keys(keys)?,
        })))
    }

    #[pyo3(signature = (count, *, offset=0))]
    fn limit(&self, count: usize, offset: usize) -> PyResult<Self> {
        Self::node(Plan::Limit(Box::new(VortexRelationalLimit {
            input: self.child()?,
            offset,
            count,
        })))
    }

    /// Computed keys/arguments are expressed by a preceding projection.
    fn aggregate(
        &self,
        group_by: Vec<String>,
        measures: Vec<(String, Option<String>, String)>,
    ) -> PyResult<Self> {
        let measures = measures
            .into_iter()
            .map(|(function, name, alias)| {
                Ok(VortexSimpleAggregateMeasure::new(
                    function,
                    name.map(column).transpose()?,
                    alias,
                ))
            })
            .collect::<PyResult<Vec<_>>>()?;
        Self::node(Plan::Aggregate(Box::new(VortexRelationalAggregate {
            input: self.child()?,
            group_by: group_by.into_iter().map(column).collect::<PyResult<_>>()?,
            measures,
        })))
    }

    #[pyo3(signature = (other, *, kind="union_all"))]
    fn set_operation(&self, other: &Self, kind: &str) -> PyResult<Self> {
        let kind = match kind {
            "union_all" => VortexRelationalSetKind::UnionAll,
            "union_distinct" => VortexRelationalSetKind::UnionDistinct,
            "intersect" => VortexRelationalSetKind::Intersect,
            "except" => VortexRelationalSetKind::Except,
            _ => {
                return Err(PyValueError::new_err(
                    "set kind must be union_all, union_distinct, intersect or except",
                ));
            }
        };
        Self::node(Plan::Set(Box::new(VortexRelationalSet {
            left: self.child()?,
            right: other.child()?,
            kind,
        })))
    }

    /// Explicit projections distinguish both inputs, including same-name fields.
    #[pyo3(signature = (other, keys, columns, *, kind="inner", condition=None))]
    fn join(
        &self,
        other: &Self,
        keys: Vec<(String, String)>,
        columns: Vec<(String, String, String)>,
        kind: &str,
        condition: Option<&NativeExpression>,
    ) -> PyResult<Self> {
        let kind = match kind {
            "inner" => VortexRelationalJoinKind::Inner,
            "left" => VortexRelationalJoinKind::Left,
            "right" => VortexRelationalJoinKind::Right,
            "full" => VortexRelationalJoinKind::Full,
            "left_semi" => VortexRelationalJoinKind::LeftSemi,
            "left_anti" => VortexRelationalJoinKind::LeftAnti,
            "cross" => VortexRelationalJoinKind::Cross,
            _ => return Err(PyValueError::new_err("unsupported native join kind")),
        };
        let keys = join_keys(keys)?;
        let columns = columns
            .into_iter()
            .map(|(side, name, output_column)| {
                let side = match side.as_str() {
                    "left" => VortexRelationalSide::Left,
                    "right" => VortexRelationalSide::Right,
                    _ => {
                        return Err(PyValueError::new_err(
                            "join projection side must be left or right",
                        ));
                    }
                };
                Ok(VortexRelationalJoinColumn {
                    side,
                    column: column(name)?,
                    output_column,
                })
            })
            .collect::<PyResult<_>>()?;
        Self::node(Plan::Join(Box::new(VortexRelationalJoin {
            left: self.child()?,
            right: other.child()?,
            kind,
            keys,
            columns,
            condition: condition.map(|expr| (*expr.value).clone()),
        })))
    }
}

fn join_keys(keys: Vec<(String, String)>) -> PyResult<Vec<VortexRelationalJoinKey>> {
    keys.into_iter()
        .map(|(left, right)| {
            Ok(VortexRelationalJoinKey {
                left: column(left)?,
                right: column(right)?,
            })
        })
        .collect()
}
