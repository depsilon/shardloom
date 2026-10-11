//! Window descriptors wrap the existing typed analytic IR without evaluation.

use super::{
    expression::NativeExpression,
    relation::{column, order_keys},
};
use pyo3::{exceptions::PyValueError, prelude::*};
use shardloom_core::ExpressionKind;
use shardloom_vortex::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameExclusion as Exclusion,
    VortexRelationalFrameFunction as Reduction, VortexRelationalFrameOffset as Offset,
    VortexRelationalFrameUnit as Unit, VortexRelationalWindowExpression as Window,
    VortexRelationalWindowFrame as Frame, VortexRelationalWindowFunction as Function,
};

#[pyclass(name = "FrameOffset", module = "shardloom._native", frozen)]
pub(super) struct NativeFrameOffset {
    value: Offset,
}

#[pymethods]
impl NativeFrameOffset {
    #[staticmethod]
    fn number(value: &NativeExpression) -> PyResult<Self> {
        let ExpressionKind::Literal(value) = &value.value.kind else {
            return Err(PyValueError::new_err(
                "frame offset requires a literal expression",
            ));
        };
        Ok(Self {
            value: Offset::Number(value.clone()),
        })
    }

    #[staticmethod]
    fn duration_micros(value: u64) -> Self {
        Self {
            value: Offset::DurationMicros(value),
        }
    }
}

#[pyclass(name = "WindowFrame", module = "shardloom._native", frozen)]
pub(super) struct NativeWindowFrame {
    value: Frame,
}

type Endpoint<'py> = (String, Option<PyRef<'py, NativeFrameOffset>>);

fn endpoint((kind, offset): Endpoint<'_>) -> PyResult<Bound> {
    match (kind.as_str(), offset) {
        ("unbounded_preceding", None) => Ok(Bound::UnboundedPreceding),
        ("current_row", None) => Ok(Bound::CurrentRow),
        ("unbounded_following", None) => Ok(Bound::UnboundedFollowing),
        ("preceding", Some(offset)) => Ok(Bound::Preceding(offset.value.clone())),
        ("following", Some(offset)) => Ok(Bound::Following(offset.value.clone())),
        _ => Err(PyValueError::new_err(
            "frame endpoint requires a supported bound and an offset only for preceding/following",
        )),
    }
}

#[pymethods]
impl NativeWindowFrame {
    #[new]
    #[pyo3(signature = (unit, start, end, *, exclusion="no_others"))]
    fn new(unit: &str, start: Endpoint<'_>, end: Endpoint<'_>, exclusion: &str) -> PyResult<Self> {
        let unit = match unit {
            "rows" => Unit::Rows,
            "groups" => Unit::Groups,
            "range" => Unit::Range,
            _ => {
                return Err(PyValueError::new_err(
                    "frame unit must be rows, groups or range",
                ));
            }
        };
        let exclusion = match exclusion {
            "no_others" => Exclusion::NoOthers,
            "current_row" => Exclusion::CurrentRow,
            "group" => Exclusion::Group,
            "ties" => Exclusion::Ties,
            _ => return Err(PyValueError::new_err("unsupported frame exclusion")),
        };
        Ok(Self {
            value: Frame {
                unit,
                start: endpoint(start)?,
                end: endpoint(end)?,
                exclusion,
            },
        })
    }
}

#[pyclass(name = "WindowExpression", module = "shardloom._native", frozen)]
pub(super) struct NativeWindowExpression {
    pub(super) value: Window,
}

#[pymethods]
impl NativeWindowExpression {
    #[new]
    #[pyo3(signature = (function, output_column, *, source_column=None, argument=None, partition_by=Vec::new(), order_by=Vec::new(), frame=None))]
    #[allow(clippy::too_many_arguments)] // Mirrors the shared window declaration, with keyword-only options.
    fn new(
        function: &str,
        output_column: String,
        source_column: Option<String>,
        argument: Option<usize>,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool, Option<String>)>,
        frame: Option<&NativeWindowFrame>,
    ) -> PyResult<Self> {
        let function = function_kind(function, source_column, argument)?;
        Ok(Self {
            value: Window {
                output_column,
                function,
                partition_by: partition_by
                    .into_iter()
                    .map(column)
                    .collect::<PyResult<_>>()?,
                order_by: order_keys(order_by)?,
                frame: frame.map(|frame| frame.value.clone()),
            },
        })
    }
}

fn function_kind(
    name: &str,
    source: Option<String>,
    argument: Option<usize>,
) -> PyResult<Function> {
    if argument.is_some() && !matches!(name, "lag" | "lead" | "ntile" | "nth_value") {
        return Err(PyValueError::new_err(
            "this window function has no numeric argument",
        ));
    }
    if source.is_some()
        && matches!(
            name,
            "row_number"
                | "rank"
                | "dense_rank"
                | "ntile"
                | "percent_rank"
                | "cume_dist"
                | "count_all"
        )
    {
        return Err(PyValueError::new_err(
            "this window function has no source column",
        ));
    }
    let source = source.map(column).transpose()?;
    let field = || {
        source
            .clone()
            .ok_or_else(|| PyValueError::new_err("window function requires a source column"))
    };
    Ok(match name {
        "row_number" => Function::RowNumber,
        "rank" => Function::Rank,
        "dense_rank" => Function::DenseRank,
        "percent_rank" => Function::PercentRank,
        "cume_dist" => Function::CumeDist,
        "lag" => Function::Lag {
            column: field()?,
            offset: argument.unwrap_or(1),
        },
        "lead" => Function::Lead {
            column: field()?,
            offset: argument.unwrap_or(1),
        },
        "ntile" => Function::Ntile {
            buckets: argument
                .ok_or_else(|| PyValueError::new_err("ntile requires a bucket argument"))?,
        },
        "count_all" => Function::Framed(Reduction::CountAll),
        "count" => Function::Framed(Reduction::Count(field()?)),
        "count_distinct" => Function::Framed(Reduction::CountDistinct(field()?)),
        "sum" => Function::Framed(Reduction::Sum(field()?)),
        "avg" => Function::Framed(Reduction::Avg(field()?)),
        "min" => Function::Framed(Reduction::Min(field()?)),
        "max" => Function::Framed(Reduction::Max(field()?)),
        "first_value" => Function::Framed(Reduction::FirstValue(field()?)),
        "last_value" => Function::Framed(Reduction::LastValue(field()?)),
        "nth_value" => Function::Framed(Reduction::NthValue {
            column: field()?,
            index: u64::try_from(
                argument
                    .ok_or_else(|| PyValueError::new_err("nth_value requires an index argument"))?,
            )
            .map_err(|_| PyValueError::new_err("window argument exceeds uint64"))?,
        }),
        _ => return Err(PyValueError::new_err("unsupported native window function")),
    })
}
