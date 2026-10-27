use pyo3::prelude::*;

#[path = "adbc.rs"]
mod adbc;
#[path = "aggregate.rs"]
mod aggregate;
#[path = "batch_input.rs"]
mod batch_input;
#[path = "capsules.rs"]
mod capsules;
#[path = "expression.rs"]
mod expression;
#[path = "failure.rs"]
mod failure;
#[path = "object_input.rs"]
mod object_input;
#[path = "plan.rs"]
mod plan;
#[path = "relation.rs"]
mod relation;
#[path = "resources.rs"]
mod resources;
#[path = "result.rs"]
mod result;
#[path = "rows.rs"]
mod rows;
#[path = "schema.rs"]
mod schema;
#[path = "session.rs"]
mod session;
#[path = "stream.rs"]
mod stream;
#[path = "window.rs"]
mod window;

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add("execution_provider", "shared_shardloom_native")?;
    module.add_class::<resources::NativeResources>()?;
    module.add_class::<session::NativeSession>()?;
    module.add_class::<session::NativeSource>()?;
    module.add_class::<plan::NativePlan>()?;
    module.add_class::<expression::NativeExpression>()?;
    module.add_class::<relation::NativeRelation>()?;
    module.add_class::<aggregate::NativeAggregateMeasure>()?;
    module.add_class::<window::NativeFrameOffset>()?;
    module.add_class::<window::NativeWindowFrame>()?;
    module.add_class::<window::NativeWindowExpression>()?;
    module.add_class::<result::NativeResult>()?;
    module.add_class::<capsules::NativeColumnarStream>()?;
    module.add_class::<stream::NativeExecutionStream>()?;
    module.add_class::<stream::NativeColumnarBatch>()?;
    module.add_function(wrap_pyfunction!(command, module)?)?;
    module.add_function(wrap_pyfunction!(scalar_function_catalog, module)?)?;
    adbc::register(module)?;
    Ok(())
}

/// Read the same inert definitions used by SQL intake and native binding.
#[pyfunction]
fn scalar_function_catalog(py: Python<'_>) -> PyResult<Vec<Py<pyo3::types::PyDict>>> {
    shardloom_core::expression::builtins::SCALARS
        .iter()
        .map(|definition| {
            let entry = pyo3::types::PyDict::new(py);
            entry.set_item("name", definition.name)?;
            entry.set_item("aliases", definition.aliases)?;
            entry.set_item("min_args", definition.min_args)?;
            entry.set_item("max_args", definition.max_args)?;
            entry.set_item("type_rule", definition.type_rule.contract())?;
            entry.set_item("null_rule", definition.null_rule.contract())?;
            entry.set_item("family", definition.family)?;
            entry.set_item("reference_supported", definition.reference_supported)?;
            entry.set_item("summary", definition.summary)?;
            entry.set_item("execution_provider", "shared_shardloom_native")?;
            entry.set_item("external_engine_invoked", false)?;
            Ok(entry.unbind())
        })
        .collect()
}

#[pyfunction]
fn command(py: Python<'_>, args: Vec<String>) -> PyResult<String> {
    py.detach(|| shardloom_runtime::native::command_json(args))
        .map_err(native_error)
}

// Result::map_err transfers the owned native failure into Python diagnostics.
#[allow(clippy::needless_pass_by_value)]
fn native_error(error: shardloom_core::ShardLoomError) -> PyErr {
    Python::attach(|py| {
        let diagnostic = error.to_diagnostic();
        let message = format!("{}: {error}", diagnostic.code.as_str());
        let exception = if diagnostic.code == shardloom_core::DiagnosticCode::ConfigurationError {
            pyo3::exceptions::PyValueError::new_err(message)
        } else {
            pyo3::exceptions::PyRuntimeError::new_err(message)
        };
        let attach = || -> PyResult<()> {
            let details = pyo3::types::PyDict::new(py);
            details.set_item("code", diagnostic.code.as_str())?;
            details.set_item("severity", diagnostic.severity.as_str())?;
            details.set_item("category", diagnostic.category.as_str())?;
            details.set_item("message", diagnostic.message)?;
            details.set_item("feature", diagnostic.feature)?;
            details.set_item("reason", diagnostic.reason)?;
            details.set_item("suggested_next_step", diagnostic.suggested_next_step)?;
            let fallback = pyo3::types::PyDict::new(py);
            fallback.set_item("attempted", diagnostic.fallback.attempted)?;
            fallback.set_item("allowed", diagnostic.fallback.allowed)?;
            fallback.set_item("engine", diagnostic.fallback.engine)?;
            fallback.set_item("reason", diagnostic.fallback.reason)?;
            details.set_item("fallback", fallback)?;
            exception.value(py).setattr("diagnostic", details)?;
            exception
                .value(py)
                .setattr("external_engine_invoked", false)?;
            Ok(())
        };
        match attach() {
            Ok(()) => exception,
            Err(error) => error,
        }
    })
}
