//! Explicit Python callables share the native application transformation owner.

use super::{
    native_error,
    session::{NativeSession, SessionOwner},
};
use arrow_array::RecordBatch;
use arrow_pyarrow::{FromPyArrow as _, ToPyArrow as _};
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
};
use shardloom_core::{
    ShardLoomError,
    expression::{Determinism, EffectLevel, NullBehavior},
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::application_batch::{
    ApplicationBatchDeclaration, ApplicationBatchFunction, ApplicationBatchLimits,
};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};

#[pyclass(name = "BatchFunction", module = "shardloom._native", frozen)]
pub(super) struct NativeBatchFunction {
    pub(super) value: ApplicationBatchFunction,
    owner: Weak<SessionOwner>,
}

impl NativeBatchFunction {
    pub(super) fn check_owner(&self, owner: &Arc<SessionOwner>) -> PyResult<()> {
        if !Weak::ptr_eq(&self.owner, &Arc::downgrade(owner)) {
            return Err(PyValueError::new_err(
                "batch function belongs to another context",
            ));
        }
        owner.begin().map_err(native_error)?;
        Ok(())
    }
}

#[pyclass(name = "BatchFunctionContext", module = "shardloom._native", frozen)]
pub(super) struct NativeBatchCall {
    cancellation: CancellationToken,
    active: Arc<AtomicBool>,
    float_policy: &'static str,
}

#[pymethods]
impl NativeBatchCall {
    /// Cooperatively check cancellation while this invocation remains active.
    fn check_cancelled(&self) -> PyResult<()> {
        if !self.active.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err(
                "batch function invocation has ended",
            ));
        }
        self.cancellation.check().map_err(native_error)
    }

    #[getter]
    fn float_policy(&self) -> &'static str {
        self.float_policy
    }
}

struct InvocationScope(Arc<AtomicBool>);

impl Drop for InvocationScope {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[pymethods]
impl NativeBatchFunction {
    #[new]
    #[pyo3(signature = (session, name, version, callback, input_schema, output_schema, max_batch_rows, max_output_bytes, workspace_bytes, *, trusted=false, rowwise=false, deterministic=true, null_behavior="null_aware", effect="pure_deterministic"))]
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)] // Every caller declaration is explicit; PyO3 transfers the callback owner.
    fn new(
        py: Python<'_>,
        session: &NativeSession,
        name: String,
        version: String,
        callback: Py<PyAny>,
        input_schema: Vec<(String, String)>,
        output_schema: Vec<(String, String)>,
        max_batch_rows: usize,
        max_output_bytes: u64,
        workspace_bytes: u64,
        trusted: bool,
        rowwise: bool,
        deterministic: bool,
        null_behavior: &str,
        effect: &str,
    ) -> PyResult<Self> {
        session.owner.begin().map_err(native_error)?;
        if !trusted
            || !rowwise
            || !deterministic
            || null_behavior != "null_aware"
            || effect != "pure_deterministic"
        {
            return Err(PyValueError::new_err(
                "batch functions require explicitly trusted, rowwise, deterministic, null-aware code without external effects",
            ));
        }
        if !callback.bind(py).is_callable() {
            return Err(PyTypeError::new_err(
                "batch function implementation must be callable",
            ));
        }
        let declaration = ApplicationBatchDeclaration {
            name: name.clone(),
            version,
            input: ApplicationBatchDeclaration::parse_fields(&input_schema)
                .map_err(native_error)?,
            output: ApplicationBatchDeclaration::parse_fields(&output_schema)
                .map_err(native_error)?,
            determinism: Determinism::Deterministic,
            null_behavior: NullBehavior::NullAware,
            effect: EffectLevel::PureDeterministic,
            limits: ApplicationBatchLimits {
                max_batch_rows,
                max_output_bytes,
                workspace_bytes,
            },
        };
        let value = ApplicationBatchFunction::new_trusted(declaration, move |_, context| {
            let input = context.to_arrow()?;
            let active = Arc::new(AtomicBool::new(true));
            let _scope = InvocationScope(active.clone());
            let output = Python::attach(|py| {
                let call = Py::new(
                    py,
                    NativeBatchCall {
                        cancellation: context.cancellation().clone(),
                        active,
                        float_policy: context.float_policy().as_str(),
                    },
                )?;
                let input = input.to_pyarrow(py)?;
                let returned = callback.bind(py).call1((input, call))?;
                RecordBatch::from_pyarrow_bound(&returned)
            })
            .map_err(|error| {
                ShardLoomError::application_function(
                    format!("application batch function '{name}' failed"),
                    error,
                )
            })?;
            Ok(output.into())
        })
        .map_err(native_error)?;
        Ok(Self {
            value,
            owner: Arc::downgrade(&session.owner),
        })
    }

    #[getter]
    fn name(&self) -> &str {
        self.value.name()
    }

    #[getter]
    fn version(&self) -> &str {
        self.value.version()
    }
}
