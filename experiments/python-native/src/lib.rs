//! Local-only binding experiment. No command strings, subprocess or external executor.
#![forbid(unsafe_code)]

use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyList, PyString},
};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    local_primitives::{
        collect::{PreparedVortexCollect, prepare_rows_in_session},
        prepared_count::{PreparedVortexCountWhere, prepare_count_where_in_session},
    },
    resident_session::{
        OwnedVortexResultBatch, PreparedVortexCount, ResidentSessionSnapshot, ResidentVortexSession,
    },
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, Weak},
};

const MAX_PLANS: usize = 64;
const MAX_FILE_BYTES: u64 = 16 << 20;
const MAX_JSON_BYTES: usize = 8 << 20;
const MAX_FILTER_SOURCE_ROWS: u64 = 65_536;

mod file_admission;
use file_admission::FileAdmission;

fn error(code: &'static str, message: impl ToString) -> PyErr {
    PyRuntimeError::new_err((code, message.to_string()))
}
fn native_error(message: impl ToString) -> PyErr {
    error("SL_NATIVE_EXECUTION", message)
}
fn lock<T>(value: &Mutex<T>) -> PyResult<MutexGuard<'_, T>> {
    value.lock().map_err(|_| {
        error(
            "SL_NATIVE_POISONED",
            "native handle lock poisoned; no fallback attempted",
        )
    })
}
fn path_text(value: &str) -> PyResult<PathBuf> {
    if value.len() > 8192 || !Path::new(value).is_absolute() {
        return Err(PyValueError::new_err(
            "native experiment requires an absolute local path of at most 8192 bytes",
        ));
    }
    Ok(PathBuf::from(value))
}
fn column(value: &str) -> PyResult<ColumnRef> {
    if value.is_empty() || value.len() > 256 {
        return Err(PyValueError::new_err("field requires 1..=256 UTF8 bytes"));
    }
    ColumnRef::new(value).map_err(native_error)
}
fn snapshot(value: ResidentSessionSnapshot) -> BTreeMap<&'static str, u64> {
    BTreeMap::from([
        ("prepared_source_opens", value.prepared_source_opens),
        ("completed_executions", value.completed_executions),
        (
            "provider_background_workers",
            value.provider_background_workers as u64,
        ),
        ("native_owned_bytes", value.memory.reserved_bytes),
        ("native_owned_peak_bytes", value.memory.peak_reserved_bytes),
        ("native_owned_denials", value.memory.denied_reservations),
    ])
}

enum Plan {
    Count(PreparedVortexCount),
    Filtered(PreparedVortexCountWhere),
    Projection {
        prepared: PreparedVortexCollect,
        columns: Vec<String>,
    },
}
impl Plan {
    fn validate_file_metadata(&self, metadata: &std::fs::Metadata) -> shardloom_core::Result<()> {
        match self {
            Self::Count(prepared) => prepared.validate_file_metadata(metadata),
            Self::Filtered(prepared) => prepared.validate_file_metadata(metadata),
            Self::Projection { prepared, .. } => prepared.validate_file_metadata(metadata),
        }
    }
}
struct PlanSlot(Mutex<Option<Plan>>);
struct State {
    session: Option<ResidentVortexSession>,
    plans: Vec<Weak<PlanSlot>>,
    memory_gb: u64,
    parallelism: usize,
    close_snapshot: Option<BTreeMap<&'static str, u64>>,
}
struct Owner(Mutex<State>);
impl State {
    fn session(&self) -> PyResult<&ResidentVortexSession> {
        self.session
            .as_ref()
            .ok_or_else(|| error("SL_NATIVE_CLOSED", "session is closed"))
    }
    fn admit_slot(&mut self) -> PyResult<()> {
        self.session()?;
        self.plans.retain(|plan| plan.strong_count() > 0);
        if self.plans.len() >= MAX_PLANS {
            return Err(error(
                "SL_NATIVE_PLAN_LIMIT",
                "at most 64 live prepared handles",
            ));
        }
        Ok(())
    }
    fn close(&mut self) -> PyResult<BTreeMap<&'static str, u64>> {
        if let Some(previous) = &self.close_snapshot {
            return Ok(previous.clone());
        }
        for plan in self.plans.drain(..).filter_map(|plan| plan.upgrade()) {
            drop(lock(&plan.0)?.take());
        }
        let native = self
            .session
            .take()
            .ok_or_else(|| error("SL_NATIVE_CLOSED", "session is closed"))?;
        let at_close = snapshot(native.snapshot());
        // Result-owned arrays may still hold this runtime. This closes every
        // prepared plan; it does not revoke independently retained results.
        drop(native);
        self.close_snapshot = Some(at_close.clone());
        Ok(at_close)
    }
}

#[pyclass(frozen, name = "Session")]
struct Session {
    owner: Arc<Owner>,
}
impl Session {
    fn prepare(
        &self,
        path: &Path,
        build: impl FnOnce(&State) -> PyResult<Plan>,
    ) -> PyResult<Prepared> {
        let mut state = lock(&self.owner.0)?;
        state.admit_slot()?;
        let admitted = FileAdmission::capture(path, MAX_FILE_BYTES).map_err(native_error)?;
        let native = build(&state)?;
        admitted
            .validate_prepared(|metadata| native.validate_file_metadata(metadata))
            .map_err(native_error)?;
        let plan = Arc::new(PlanSlot(Mutex::new(Some(native))));
        state.plans.push(Arc::downgrade(&plan));
        Ok(Prepared {
            plan,
            owner: Arc::clone(&self.owner),
        })
    }
}
#[pymethods]
impl Session {
    #[new]
    #[pyo3(signature = (memory_gb=1, max_parallelism=1))]
    fn new(py: Python<'_>, memory_gb: u64, max_parallelism: usize) -> PyResult<Self> {
        if !(1..=4).contains(&memory_gb) || !(1..=8).contains(&max_parallelism) {
            return Err(PyValueError::new_err(
                "experiment bounds are 1..=4 GiB and 1..=8 requested CPU lanes",
            ));
        }
        py.check_signals()?;
        py.detach(|| {
            let session = ResidentVortexSession::new(memory_gb << 30, max_parallelism)
                .map_err(native_error)?;
            Ok(Self {
                owner: Arc::new(Owner(Mutex::new(State {
                    session: Some(session),
                    plans: Vec::new(),
                    memory_gb,
                    parallelism: max_parallelism,
                    close_snapshot: None,
                }))),
            })
        })
    }

    fn prepare_count(&self, py: Python<'_>, source: &str) -> PyResult<Prepared> {
        let path = path_text(source)?;
        py.check_signals()?;
        py.detach(|| {
            self.prepare(&path, |state| {
                Ok(Plan::Count(
                    state
                        .session()?
                        .prepare_file(&path)
                        .map_err(native_error)?
                        .prepare_count(),
                ))
            })
        })
    }

    fn prepare_count_where_i64(
        &self,
        py: Python<'_>,
        source: &str,
        field: &str,
        op: &str,
        value: i64,
    ) -> PyResult<Prepared> {
        let path = path_text(source)?;
        let column = column(field)?;
        let op = match op {
            "eq" => ComparisonOp::Eq,
            "ne" => ComparisonOp::NotEq,
            "lt" => ComparisonOp::Lt,
            "le" => ComparisonOp::LtEq,
            "gt" => ComparisonOp::Gt,
            "ge" => ComparisonOp::GtEq,
            _ => {
                return Err(PyValueError::new_err(
                    "comparison must be eq, ne, lt, le, gt or ge",
                ));
            }
        };
        py.check_signals()?;
        py.detach(|| {
            self.prepare(&path, |state| {
                let request = VortexQueryPrimitiveRequest::count_where(
                    DatasetUri::new(path.display().to_string()).map_err(native_error)?,
                    PredicateExpr::Compare {
                        column,
                        op,
                        value: StatValue::Int64(value),
                    },
                );
                let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(
                    state.parallelism,
                    state.memory_gb,
                )
                .map_err(native_error)?;
                let prepared = prepare_count_where_in_session(&request, policy, state.session()?)
                    .map_err(native_error)?;
                if prepared.source_row_count() > MAX_FILTER_SOURCE_ROWS {
                    return Err(error(
                        "SL_NATIVE_ROW_LIMIT",
                        "filtered source exceeds 65536 logical rows; no scan was started",
                    ));
                }
                Ok(Plan::Filtered(prepared))
            })
        })
    }

    fn prepare_projection(
        &self,
        py: Python<'_>,
        source: &str,
        columns: &Bound<'_, PyList>,
        limit: usize,
    ) -> PyResult<Prepared> {
        let path = path_text(source)?;
        if columns.is_empty() || columns.len() > 64 || !(1..=65_536).contains(&limit) {
            return Err(PyValueError::new_err(
                "projection requires 1..=64 fields and explicit 1..=65536 row limit",
            ));
        }
        let mut names = Vec::with_capacity(columns.len());
        let mut refs = Vec::with_capacity(columns.len());
        for item in columns.iter() {
            let name = item.cast::<PyString>()?.to_str()?;
            if names.iter().any(|previous| previous == name) {
                return Err(PyValueError::new_err("projection fields must be unique"));
            }
            refs.push(column(name)?);
            names.push(name.to_owned());
        }
        py.check_signals()?;
        py.detach(|| {
            self.prepare(&path, |state| {
                let request = VortexQueryPrimitiveRequest::project(
                    DatasetUri::new(path.display().to_string()).map_err(native_error)?,
                    ProjectionRequest::columns(refs),
                )
                .with_source_order_limit(limit);
                let prepared =
                    prepare_rows_in_session(&request, state.session()?).map_err(native_error)?;
                Ok(Plan::Projection {
                    prepared,
                    columns: names,
                })
            })
        })
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<BTreeMap<&'static str, u64>> {
        py.detach(|| Ok(snapshot(lock(&self.owner.0)?.session()?.snapshot())))
    }
    fn close(&self, py: Python<'_>) -> PyResult<BTreeMap<&'static str, u64>> {
        py.detach(|| lock(&self.owner.0)?.close())
    }
}

#[pyclass(frozen)]
struct Prepared {
    plan: Arc<PlanSlot>,
    owner: Arc<Owner>,
}
impl Prepared {
    fn use_plan<T>(&self, execute: impl FnOnce(&Plan) -> PyResult<T>) -> PyResult<T> {
        let state = lock(&self.owner.0)?;
        state.session()?;
        let plan = lock(&self.plan.0)?;
        execute(
            plan.as_ref()
                .ok_or_else(|| error("SL_NATIVE_CLOSED", "prepared handle is closed"))?,
        )
    }
}
#[pymethods]
impl Prepared {
    fn execute_count(&self, py: Python<'_>) -> PyResult<u64> {
        py.check_signals()?;
        let result = py.detach(|| {
            self.use_plan(|plan| match plan {
                Plan::Count(count) => count.execute().map_err(native_error),
                Plan::Filtered(count) => {
                    let result = count.execute().map_err(native_error)?;
                    if !result.native_io_certificate.is_certified()
                        || result.report.has_errors()
                        || result.report.fallback_execution_allowed
                    {
                        return Err(error(
                            "SL_NATIVE_CERTIFICATE",
                            "filtered count did not complete with its native I/O certificate",
                        ));
                    }
                    Ok(result.count)
                }
                Plan::Projection { .. } => {
                    Err(PyValueError::new_err("projection is not a scalar count"))
                }
            })
        })?;
        py.check_signals()?;
        Ok(result)
    }
    fn execute_arrays(&self, py: Python<'_>) -> PyResult<NativeBatch> {
        py.check_signals()?;
        let result = py.detach(|| {
            self.use_plan(|plan| match plan {
                Plan::Projection { prepared, columns } => Ok(NativeBatch {
                    result: Mutex::new(Some(prepared.execute_arrays().map_err(native_error)?)),
                    columns: columns.clone(),
                }),
                _ => Err(PyValueError::new_err(
                    "count is not a native array projection",
                )),
            })
        })?;
        py.check_signals()?;
        Ok(result)
    }
    fn execute_json<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        py.check_signals()?;
        let result = py.detach(|| {
            self.use_plan(|plan| match plan {
                Plan::Projection { prepared, .. } => prepared.execute().map_err(native_error),
                _ => Err(PyValueError::new_err("count is not a row projection")),
            })
        })?;
        py.check_signals()?;
        Ok(PyString::new(py, result.values_json.value()))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let _state = lock(&self.owner.0)?;
            drop(lock(&self.plan.0)?.take());
            Ok(())
        })
    }
}

#[pyclass(frozen)]
struct NativeBatch {
    result: Mutex<Option<OwnedVortexResultBatch>>,
    columns: Vec<String>,
}
#[pymethods]
impl NativeBatch {
    fn info(&self, py: Python<'_>) -> PyResult<BTreeMap<&'static str, u64>> {
        py.detach(|| {
            let result = lock(&self.result)?;
            let result = result
                .as_ref()
                .ok_or_else(|| error("SL_NATIVE_CLOSED", "native result is closed"))?;
            Ok(BTreeMap::from([
                ("rows", result.row_count()),
                ("logical_buffer_bytes", result.logical_buffer_bytes()),
                ("native_array_count", result.arrays().len() as u64),
            ]))
        })
    }
    #[pyo3(signature = (max_bytes=MAX_JSON_BYTES))]
    fn to_json<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyString>> {
        py.check_signals()?;
        let output = py.detach(|| {
            let result = lock(&self.result)?;
            result
                .as_ref()
                .ok_or_else(|| error("SL_NATIVE_CLOSED", "native result is closed"))?
                .to_bounded_json(&self.columns, max_bytes)
                .map_err(native_error)
        })?;
        py.check_signals()?;
        // The native bounded String remains charged until the Python-owned
        // Unicode copy has completed. Python allocations are outside that pool.
        Ok(PyString::new(py, output.value()))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            drop(lock(&self.result)?.take());
            Ok(())
        })
    }
}

#[pymodule(gil_used = true)]
fn _shardloom_native_experiment(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;
    module.add_class::<Prepared>()?;
    module.add_class::<NativeBatch>()?;
    module.add("API_STATUS", "local_unpublished_experiment")?;
    module.add("FALLBACK_EXECUTION_ALLOWED", false)?;
    module.add(
        "PROVIDER_VERSION",
        shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
    )?;
    Ok(())
}
