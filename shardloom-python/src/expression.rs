//! Inert Python handles for the shared native scalar expression representation.

use super::native_error;
use pyo3::{
    basic::CompareOp,
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyFloat, PyInt, PyString},
};
use shardloom_core::{
    BinaryOp, ColumnRef, ComparisonOp, ExprId, Expression, ExpressionKind, LogicalDType,
    ScalarValue, UnaryOp,
};
use shardloom_vortex::relational_query::NATIVE_RECURSION_LIMIT;
use std::sync::Arc;

#[pyclass(name = "Expression", module = "shardloom._native", frozen)]
pub(super) struct NativeExpression {
    pub(super) value: Arc<Expression>,
    depth: usize,
}

impl NativeExpression {
    fn node(kind: ExpressionKind, depth: usize) -> PyResult<Self> {
        check_depth(depth)?;
        // A complete relation assigns stable, unique traversal identities before
        // binding. A detached expression is a reusable declaration, not a slot.
        let id = ExprId::new("native-expression").map_err(native_error)?;
        Ok(Self {
            value: Arc::new(Expression::new(id, kind)),
            depth,
        })
    }

    fn scalar(value: ScalarValue) -> PyResult<Self> {
        Self::node(ExpressionKind::Literal(value), 0)
    }

    fn child_depth(&self) -> PyResult<usize> {
        let depth = self.depth + 1;
        check_depth(depth)?;
        Ok(depth)
    }

    fn binary_node(&self, op: BinaryOp, other: &Self) -> PyResult<Self> {
        let depth = self.child_depth()?.max(other.child_depth()?);
        Self::node(
            ExpressionKind::Binary {
                left: Box::new((*self.value).clone()),
                op,
                right: Box::new((*other.value).clone()),
            },
            depth,
        )
    }

    fn unary_node(&self, op: UnaryOp) -> PyResult<Self> {
        let depth = self.child_depth()?;
        Self::node(
            ExpressionKind::Unary {
                op,
                expr: Box::new((*self.value).clone()),
            },
            depth,
        )
    }
}

fn check_depth(depth: usize) -> PyResult<()> {
    if depth > NATIVE_RECURSION_LIMIT {
        return Err(PyValueError::new_err(
            "recursive scalar expression exceeds 24 levels",
        ));
    }
    Ok(())
}

#[pymethods]
impl NativeExpression {
    #[staticmethod]
    fn column(name: String) -> PyResult<Self> {
        Self::node(
            ExpressionKind::Column(ColumnRef::new(name).map_err(native_error)?),
            0,
        )
    }

    /// Exact primitive literals. Rich values have explicit typed constructors.
    #[staticmethod]
    fn literal(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let value = if value.is_none() {
            ScalarValue::Null
        } else if value.is_instance_of::<PyBool>() {
            ScalarValue::Boolean(value.extract()?)
        } else if value.is_instance_of::<PyInt>() {
            if let Ok(value) = value.extract::<i64>() {
                ScalarValue::Int64(value)
            } else {
                ScalarValue::UInt64(value.extract()?)
            }
        } else if value.is_instance_of::<PyFloat>() {
            ScalarValue::Float64(value.extract()?)
        } else if value.is_instance_of::<PyString>() {
            ScalarValue::Utf8(value.extract()?)
        } else if value.is_instance_of::<PyBytes>() {
            ScalarValue::Binary(value.extract()?)
        } else {
            return Err(PyTypeError::new_err(
                "literal requires None, bool, an exact 64-bit integer, float, str or bytes; use typed constructors for rich values",
            ));
        };
        Self::scalar(value)
    }

    #[staticmethod]
    fn uint64(value: u64) -> PyResult<Self> {
        Self::scalar(ScalarValue::UInt64(value))
    }

    #[staticmethod]
    fn decimal128(value: i128, precision: u8, scale: u8) -> PyResult<Self> {
        Self::scalar(ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        })
    }

    #[staticmethod]
    fn date32(days: i32) -> PyResult<Self> {
        Self::scalar(ScalarValue::Date32(days))
    }

    #[staticmethod]
    fn timestamp_micros(microseconds: i64) -> PyResult<Self> {
        Self::scalar(ScalarValue::TimestampMicros(microseconds))
    }

    /// Explicit typed ticks; this is not an implicit integer-to-time cast.
    #[staticmethod]
    fn temporal(ticks: i64, dtype: &str) -> PyResult<Self> {
        use shardloom_core::temporal::{TemporalDType, TemporalValue};
        let dtype = TemporalDType::parse_declaration(dtype)
            .ok_or_else(|| PyValueError::new_err("invalid temporal scalar dtype"))?;
        if let Some(zone) = dtype.timezone() {
            shardloom_core::temporal::timezone::validate_name(zone).map_err(native_error)?;
        }
        Self::scalar(
            TemporalValue::new(ticks, dtype)
                .map_err(native_error)?
                .into_scalar(),
        )
    }

    #[staticmethod]
    fn list(values: Vec<PyRef<'_, Self>>) -> PyResult<Self> {
        let depth = values
            .iter()
            .map(|v| v.child_depth())
            .collect::<PyResult<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(0);
        Self::node(
            ExpressionKind::List {
                values: values.into_iter().map(|v| (*v.value).clone()).collect(),
            },
            depth,
        )
    }

    #[staticmethod]
    fn struct_(fields: Vec<(String, PyRef<'_, Self>)>) -> PyResult<Self> {
        let depth = fields
            .iter()
            .map(|(_, v)| v.child_depth())
            .collect::<PyResult<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(0);
        Self::node(
            ExpressionKind::Struct {
                fields: fields
                    .into_iter()
                    .map(|(name, v)| (name, (*v.value).clone()))
                    .collect(),
            },
            depth,
        )
    }

    #[staticmethod]
    fn call(name: String, args: Vec<PyRef<'_, Self>>) -> PyResult<Self> {
        let depth = args
            .iter()
            .map(|v| v.child_depth())
            .collect::<PyResult<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(0);
        Self::node(
            ExpressionKind::FunctionCall {
                name,
                args: args.into_iter().map(|v| (*v.value).clone()).collect(),
            },
            depth,
        )
    }

    fn alias(&self, name: String) -> PyResult<Self> {
        let depth = self.child_depth()?;
        Self::node(
            ExpressionKind::Alias {
                expr: Box::new((*self.value).clone()),
                alias: name,
            },
            depth,
        )
    }

    #[pyo3(signature = (dtype, *, tolerant=false))]
    fn cast(&self, dtype: &str, tolerant: bool) -> PyResult<Self> {
        let depth = self.child_depth()?;
        let target_dtype = match dtype {
            "bool" | "boolean" => LogicalDType::Boolean,
            "int64" => LogicalDType::Int64,
            "uint64" => LogicalDType::UInt64,
            "float64" => LogicalDType::Float64,
            "utf8" => LogicalDType::Utf8,
            "binary" => LogicalDType::Binary,
            "date32" => LogicalDType::Date32,
            "timestamp_micros" => LogicalDType::TimestampMicros,
            name => shardloom_core::temporal::TemporalDType::parse_declaration(name).map_or_else(
                || LogicalDType::Extension(name.to_owned()),
                |dtype| dtype.logical(),
            ),
        };
        let expr = Box::new((*self.value).clone());
        Self::node(
            if tolerant {
                ExpressionKind::TryCast { expr, target_dtype }
            } else {
                ExpressionKind::Cast { expr, target_dtype }
            },
            depth,
        )
    }

    fn is_null(&self) -> PyResult<Self> {
        self.unary_node(UnaryOp::IsNull)
    }
    fn is_not_null(&self) -> PyResult<Self> {
        self.unary_node(UnaryOp::IsNotNull)
    }
    fn __neg__(&self) -> PyResult<Self> {
        self.unary_node(UnaryOp::Negate)
    }
    fn __invert__(&self) -> PyResult<Self> {
        self.unary_node(UnaryOp::Not)
    }
    fn __add__(&self, other: &Self) -> PyResult<Self> {
        self.binary_node(BinaryOp::Add, other)
    }
    fn __sub__(&self, other: &Self) -> PyResult<Self> {
        self.binary_node(BinaryOp::Subtract, other)
    }
    fn __mul__(&self, other: &Self) -> PyResult<Self> {
        self.binary_node(BinaryOp::Multiply, other)
    }
    fn __truediv__(&self, other: &Self) -> PyResult<Self> {
        self.binary_node(BinaryOp::Divide, other)
    }
    fn __mod__(&self, other: &Self) -> PyResult<Self> {
        self.binary_node(BinaryOp::Remainder, other)
    }
    fn __and__(&self, other: &Self) -> PyResult<Self> {
        self.binary_node(BinaryOp::And, other)
    }
    fn __or__(&self, other: &Self) -> PyResult<Self> {
        self.binary_node(BinaryOp::Or, other)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<Self> {
        let depth = self.child_depth()?.max(other.child_depth()?);
        let op = match op {
            CompareOp::Eq => ComparisonOp::Eq,
            CompareOp::Ne => ComparisonOp::NotEq,
            CompareOp::Lt => ComparisonOp::Lt,
            CompareOp::Le => ComparisonOp::LtEq,
            CompareOp::Gt => ComparisonOp::Gt,
            CompareOp::Ge => ComparisonOp::GtEq,
        };
        Self::node(
            ExpressionKind::Compare {
                left: Box::new((*self.value).clone()),
                op,
                right: Box::new((*other.value).clone()),
            },
            depth,
        )
    }

    #[allow(clippy::unused_self)]
    fn __bool__(&self) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "native expressions describe work; combine predicates with & or | and execute a plan",
        ))
    }
}
