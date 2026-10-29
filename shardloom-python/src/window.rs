//! Window descriptors wrap the existing typed analytic IR without evaluation.

use super::{
    expression::NativeExpression,
    relation::{column, order_keys},
};
use pyo3::{exceptions::PyValueError, prelude::*};
use shardloom_core::{
    ExpressionKind,
    temporal::{Ambiguity, TemporalUnit, civil::MonthEnd},
};
use shardloom_vortex::relational_query::{
    VortexRelationalFillMethod as FillMethod, VortexRelationalFrameBound as Bound,
    VortexRelationalFrameExclusion as Exclusion, VortexRelationalFrameFunction as Reduction,
    VortexRelationalFrameOffset as Offset, VortexRelationalFrameUnit as Unit,
    VortexRelationalWindowExpression as Window, VortexRelationalWindowFrame as Frame,
    VortexRelationalWindowFunction as Function,
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

    /// Declare a fixed elapsed duration. Calendar ambiguity is not applicable.
    #[staticmethod]
    #[pyo3(signature = (value, *, unit="us"))]
    fn duration(value: u64, unit: &str) -> PyResult<Self> {
        let unit = TemporalUnit::parse(unit)
            .ok_or_else(|| PyValueError::new_err("duration unit must be s, ms, us or ns"))?;
        Ok(Self {
            value: Offset::Duration { value, unit },
        })
    }

    /// Declare calendar days; ambiguous or nonexistent local times reject by default.
    #[staticmethod]
    #[pyo3(signature = (value, *, ambiguity="reject"))]
    fn calendar_days(value: u64, ambiguity: &str) -> PyResult<Self> {
        let ambiguity = Ambiguity::parse(ambiguity)
            .ok_or_else(|| PyValueError::new_err("ambiguity must be reject, earlier or later"))?;
        Ok(Self {
            value: Offset::CalendarDays {
                days: value,
                ambiguity,
            },
        })
    }

    /// Declare calendar months; month-end and local-time ambiguity reject by default.
    #[staticmethod]
    #[pyo3(signature = (value, *, month_end="reject", ambiguity="reject"))]
    fn calendar_months(value: u64, month_end: &str, ambiguity: &str) -> PyResult<Self> {
        let month_end = MonthEnd::parse(month_end)
            .ok_or_else(|| PyValueError::new_err("month_end must be reject or clamp"))?;
        let ambiguity = Ambiguity::parse(ambiguity)
            .ok_or_else(|| PyValueError::new_err("ambiguity must be reject, earlier or later"))?;
        Ok(Self {
            value: Offset::CalendarMonths {
                months: value,
                month_end,
                ambiguity,
            },
        })
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
    #[pyo3(signature = (function, output_column, *, source_column=None, secondary_column=None, filter_column=None, quantile_level=None, argument=None, partition_by=Vec::new(), order_by=Vec::new(), aggregate_order_by=Vec::new(), frame=None, separator=None, decimal_output=None))]
    #[allow(clippy::too_many_arguments)] // Mirrors the shared window declaration, with keyword-only options.
    fn new(
        function: &str,
        output_column: String,
        source_column: Option<String>,
        secondary_column: Option<String>,
        filter_column: Option<String>,
        quantile_level: Option<&pyo3::Bound<'_, PyAny>>,
        argument: Option<usize>,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool, Option<String>)>,
        aggregate_order_by: Vec<(String, bool, Option<String>)>,
        frame: Option<&NativeWindowFrame>,
        separator: Option<String>,
        decimal_output: Option<&pyo3::Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let level = super::aggregate::parse_quantile_level(function, quantile_level)?;
        let decimal_output = super::aggregate::parse_decimal_output(function, decimal_output)?;
        let filter_column = filter_column.map(column).transpose()?;
        let function = function_kind(
            function,
            source_column,
            secondary_column,
            level,
            argument,
            aggregate_order_by,
            separator,
            filter_column,
            decimal_output,
        )?;
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

fn validate_arguments(
    name: &str,
    source: Option<&str>,
    secondary: Option<&str>,
    argument: Option<usize>,
    has_aggregate_order: bool,
    has_separator: bool,
) -> PyResult<()> {
    let paired = matches!(
        name,
        "covar_pop" | "covar_samp" | "corr" | "arg_min" | "arg_max"
    );
    if paired != secondary.is_some() {
        return Err(PyValueError::new_err(
            "paired statistical and argument-extreme windows require secondary_column; other functions do not accept it",
        ));
    }
    let ordered_reducer = matches!(name, "first" | "last" | "arg_min" | "arg_max");
    let ordered_reducer = ordered_reducer || matches!(name, "list" | "array_agg" | "string_agg");
    if has_aggregate_order && !ordered_reducer {
        return Err(PyValueError::new_err(
            "aggregate_order_by is only supported for first, last, arg_min, arg_max, list, array_agg, and string_agg windows",
        ));
    }
    if (name == "string_agg") != has_separator {
        return Err(PyValueError::new_err(
            "string_agg requires separator; other reducers do not accept it",
        ));
    }
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
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One inert window declaration carries independent aggregate modifiers.
fn function_kind(
    name: &str,
    source: Option<String>,
    secondary: Option<String>,
    quantile_level: Option<shardloom_vortex::query_primitive::VortexQuantileLevel>,
    argument: Option<usize>,
    aggregate_order_by: Vec<(String, bool, Option<String>)>,
    separator: Option<String>,
    filter_column: Option<shardloom_core::ColumnRef>,
    decimal_output: Option<shardloom_core::decimal_output::DecimalOutput>,
) -> PyResult<Function> {
    validate_filter(name, filter_column.is_some())?;
    validate_arguments(
        name,
        source.as_deref(),
        secondary.as_deref(),
        argument,
        !aggregate_order_by.is_empty(),
        separator.is_some(),
    )?;
    let aggregate_order_by = order_keys(aggregate_order_by)?;
    let source = source.map(column).transpose()?;
    let field = || {
        source
            .clone()
            .ok_or_else(|| PyValueError::new_err("window function requires a source column"))
    };
    window_kind(
        name,
        field,
        secondary,
        quantile_level,
        argument,
        aggregate_order_by,
        separator,
        filter_column,
        decimal_output,
    )
}

fn validate_filter(name: &str, has_filter: bool) -> PyResult<()> {
    let aggregate_function = matches!(
        name,
        "var_pop"
            | "var_samp"
            | "variance"
            | "var"
            | "stddev_pop"
            | "stddev_samp"
            | "stddev"
            | "std"
            | "covar_pop"
            | "covar_samp"
            | "corr"
            | "sum_distinct"
            | "avg_distinct"
            | "mean_distinct"
            | "median"
            | "quantile_cont"
            | "quantile_disc"
            | "first"
            | "last"
            | "arg_min"
            | "arg_max"
            | "list"
            | "array_agg"
            | "string_agg"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "count"
            | "count_all"
            | "count_distinct"
    );
    if has_filter && !aggregate_function {
        return Err(PyValueError::new_err(
            "filter_column is only supported for aggregate window functions",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Validated fields construct one native window descriptor.
fn window_kind(
    name: &str,
    field: impl Fn() -> PyResult<shardloom_core::ColumnRef>,
    secondary: Option<String>,
    quantile_level: Option<shardloom_vortex::query_primitive::VortexQuantileLevel>,
    argument: Option<usize>,
    aggregate_order_by: Vec<shardloom_vortex::relational_query::VortexRelationalOrderKey>,
    separator: Option<String>,
    filter_column: Option<shardloom_core::ColumnRef>,
    decimal_output: Option<shardloom_core::decimal_output::DecimalOutput>,
) -> PyResult<Function> {
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
        "forward_fill" | "backward_fill" | "interpolate_linear" => Function::Fill {
            column: field()?,
            method: match name {
                "forward_fill" => FillMethod::Forward,
                "backward_fill" => FillMethod::Backward,
                "interpolate_linear" => FillMethod::Linear,
                _ => unreachable!("matched fill function"),
            },
        },
        "ntile" => Function::Ntile {
            buckets: argument
                .ok_or_else(|| PyValueError::new_err("ntile requires a bucket argument"))?,
        },
        "count_all" | "count" | "count_distinct" | "sum" | "avg" | "min" | "max"
            if filter_column.is_some() || decimal_output.is_some() =>
        {
            let (function, source) = if name == "count_all" {
                ("count", None)
            } else {
                (name, Some(field()?))
            };
            let mut measure = shardloom_vortex::query_primitive::VortexSimpleAggregateMeasure::new(
                function,
                source,
                "window_value".into(),
            );
            measure.filter_column = filter_column;
            measure.decimal_output = decimal_output;
            Function::Framed(Reduction::Aggregate(Box::new(measure)))
        }
        "count_all" => Function::Framed(Reduction::CountAll),
        "count" => Function::Framed(Reduction::Count(field()?)),
        "count_distinct" => Function::Framed(Reduction::CountDistinct(field()?)),
        "sum" => Function::Framed(Reduction::Sum(field()?)),
        "avg" => Function::Framed(Reduction::Avg(field()?)),
        "min" => Function::Framed(Reduction::Min(field()?)),
        "max" => Function::Framed(Reduction::Max(field()?)),
        "var_pop" | "var_samp" | "variance" | "var" | "stddev_pop" | "stddev_samp" | "stddev"
        | "std" | "covar_pop" | "covar_samp" | "corr" | "sum_distinct" | "avg_distinct"
        | "mean_distinct" | "median" | "quantile_cont" | "quantile_disc" | "first" | "last"
        | "arg_min" | "arg_max" => {
            let mut measure = shardloom_vortex::query_primitive::VortexSimpleAggregateMeasure::new(
                name,
                Some(field()?),
                "window_value".into(),
            );
            measure.secondary_column = secondary.map(column).transpose()?;
            measure.filter_column = filter_column;
            measure.quantile_level = quantile_level;
            measure.decimal_output = decimal_output;
            measure.order_by = aggregate_order_by;
            Function::Framed(Reduction::Aggregate(Box::new(measure)))
        }
        "list" | "array_agg" | "string_agg" => {
            let mut measure = shardloom_vortex::query_primitive::VortexSimpleAggregateMeasure::new(
                name,
                Some(field()?),
                "window_value".into(),
            );
            measure.order_by = aggregate_order_by;
            measure.separator = separator;
            measure.filter_column = filter_column;
            Function::Framed(Reduction::Aggregate(Box::new(measure)))
        }
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
