//! Typed relation construction only; all execution stays in the shared engine.

use super::{expression::NativeExpression, native_error};
use pyo3::{exceptions::PyValueError, prelude::*};
use shardloom_core::{ColumnRef, ComparisonOp};
use shardloom_runtime::native::NativeRelationDeclaration;
use shardloom_vortex::relational_query::{
    VortexRelationalAggregate, VortexRelationalAsOfDirection, VortexRelationalAsOfJoin,
    VortexRelationalAsOfOptions, VortexRelationalFilter, VortexRelationalGroupingIndicator,
    VortexRelationalGroupingSets, VortexRelationalJoin, VortexRelationalJoinColumn,
    VortexRelationalJoinKey, VortexRelationalJoinKind, VortexRelationalLimit,
    VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalPlan as Plan,
    VortexRelationalProject, VortexRelationalQuantifier, VortexRelationalSet,
    VortexRelationalSetKind, VortexRelationalSide, VortexRelationalSort, VortexRelationalSubquery,
    VortexRelationalSubqueryKind, VortexRelationalWindow,
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

    fn compose(&self, plan: Plan, other: Option<&Self>) -> PyResult<Self> {
        let mut inputs = vec![Arc::clone(&self.declaration)];
        if let Some(other) = other {
            inputs.push(Arc::clone(&other.declaration));
        }
        Ok(Self {
            declaration: Arc::new(
                NativeRelationDeclaration::compose(plan, inputs).map_err(native_error)?,
            ),
        })
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
        self.compose(
            Plan::Window(Box::new(VortexRelationalWindow {
                input: Plan::Input(0),
                columns: columns.into_iter().map(column).collect::<PyResult<_>>()?,
                expressions: expressions
                    .into_iter()
                    .map(|expression| expression.value.clone())
                    .collect(),
            })),
            None,
        )
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
            input: Plan::Input(0),
            relation: Plan::Input(1),
            kind,
            evaluation_guard: guard.map(|value| (*value.value).clone()),
            correlation: join_keys(correlation)?,
            output_column,
            negated,
        });
        self.compose(
            if correlated {
                Plan::CorrelatedSubquery(node)
            } else {
                Plan::Subquery(node)
            },
            Some(relation),
        )
    }

    fn project(&self, expressions: Vec<(String, PyRef<'_, NativeExpression>)>) -> PyResult<Self> {
        self.compose(
            Plan::Project(Box::new(VortexRelationalProject {
                input: Plan::Input(0),
                expressions: expressions
                    .into_iter()
                    .map(|(name, expr)| (name, (*expr.value).clone()))
                    .collect(),
            })),
            None,
        )
    }

    fn filter(&self, predicate: &NativeExpression) -> PyResult<Self> {
        self.compose(
            Plan::Filter(Box::new(VortexRelationalFilter {
                input: Plan::Input(0),
                predicate: (*predicate.value).clone(),
            })),
            None,
        )
    }

    fn sort(&self, keys: Vec<(String, bool, Option<String>)>) -> PyResult<Self> {
        self.compose(
            Plan::Sort(Box::new(VortexRelationalSort {
                input: Plan::Input(0),
                keys: order_keys(keys)?,
            })),
            None,
        )
    }

    #[pyo3(signature = (count, *, offset=0))]
    fn limit(&self, count: usize, offset: usize) -> PyResult<Self> {
        self.compose(
            Plan::Limit(Box::new(VortexRelationalLimit {
                input: Plan::Input(0),
                offset,
                count,
            })),
            None,
        )
    }

    /// Declare `distinct_rows`, `drop_duplicates`, duplicated, tail, sample,
    /// rewrite, melt, rolling, explode or pivot at this position in the plan.
    /// Options are a column list, positive tail count or the shared JSON
    /// operation object. Only duplicate operations accept keep.
    #[pyo3(signature = (operation, options="*", *, keep=None))]
    fn unary(&self, operation: &str, options: &str, keep: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            declaration: Arc::new(
                self.declaration
                    .unary(operation, options, keep)
                    .map_err(native_error)?,
            ),
        })
    }

    /// Computed keys/arguments are projected first; indicators are (alias, key columns) pairs.
    #[pyo3(signature = (group_by, measures, *, grouping_sets=None, grouping_indicators=None))]
    fn aggregate(
        &self,
        group_by: Vec<String>,
        measures: Vec<Bound<'_, PyAny>>,
        grouping_sets: Option<Vec<Vec<String>>>,
        grouping_indicators: Option<Vec<(String, Vec<String>)>>,
    ) -> PyResult<Self> {
        let measures = measures
            .into_iter()
            .map(|value| super::aggregate::parse(&value))
            .collect::<PyResult<Vec<_>>>()?;
        let group_by = group_by
            .into_iter()
            .map(column)
            .collect::<PyResult<Vec<_>>>()?;
        let grouping_sets = match (grouping_sets, grouping_indicators) {
            (None, None) => None,
            (sets, indicators) => Some(VortexRelationalGroupingSets {
                sets: match sets {
                    Some(sets) => sets
                        .into_iter()
                        .map(|set| set.into_iter().map(column).collect::<PyResult<Vec<_>>>())
                        .collect::<PyResult<Vec<_>>>()?,
                    None => vec![group_by.clone()],
                },
                indicators: indicators
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(alias, columns)| {
                        Ok(VortexRelationalGroupingIndicator {
                            columns: columns
                                .into_iter()
                                .map(column)
                                .collect::<PyResult<Vec<_>>>()?,
                            alias,
                        })
                    })
                    .collect::<PyResult<Vec<_>>>()?,
            }),
        };
        self.compose(
            Plan::Aggregate(Box::new(VortexRelationalAggregate {
                input: Plan::Input(0),
                group_by,
                grouping_sets,
                measures,
            })),
            None,
        )
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
        self.compose(
            Plan::Set(Box::new(VortexRelationalSet {
                left: Plan::Input(0),
                right: Plan::Input(1),
                kind,
            })),
            Some(other),
        )
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
        let columns = join_columns(columns)?;
        self.compose(
            Plan::Join(Box::new(VortexRelationalJoin {
                left: Plan::Input(0),
                right: Plan::Input(1),
                kind,
                keys,
                columns,
                condition: condition.map(|expr| (*expr.value).clone()),
            })),
            Some(other),
        )
    }

    /// Preserve left rows and choose one ordered right match in each by-key partition.
    /// Tolerance is inclusive in ordering-key ticks. Backward chooses the last
    /// equal right key; forward the first. Nearest prefers backward on equal
    /// distance and follows the selected direction's duplicate rule.
    #[pyo3(signature = (other, on, columns, *, by=Vec::new(), strategy="backward", tolerance=None, allow_exact_matches=true))]
    #[allow(clippy::too_many_arguments)] // Required relation/columns plus named, orthogonal public matching options.
    fn join_asof(
        &self,
        other: &Self,
        on: (String, String),
        columns: Vec<(String, String, String)>,
        by: Vec<(String, String)>,
        strategy: &str,
        tolerance: Option<u64>,
        allow_exact_matches: bool,
    ) -> PyResult<Self> {
        let direction = match strategy {
            "backward" => VortexRelationalAsOfDirection::Backward,
            "forward" => VortexRelationalAsOfDirection::Forward,
            "nearest" => VortexRelationalAsOfDirection::Nearest,
            _ => {
                return Err(PyValueError::new_err(
                    "as-of strategy must be backward, forward or nearest",
                ));
            }
        };
        self.compose(
            Plan::AsOfJoin(Box::new(VortexRelationalAsOfJoin {
                left: Plan::Input(0),
                right: Plan::Input(1),
                on: VortexRelationalJoinKey {
                    left: column(on.0)?,
                    right: column(on.1)?,
                },
                by: join_keys(by)?,
                options: VortexRelationalAsOfOptions {
                    direction,
                    tolerance,
                    allow_exact_matches,
                },
                columns: join_columns(columns)?,
            })),
            Some(other),
        )
    }
}

fn join_columns(
    columns: Vec<(String, String, String)>,
) -> PyResult<Vec<VortexRelationalJoinColumn>> {
    columns
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
        .collect()
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
