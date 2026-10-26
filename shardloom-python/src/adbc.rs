//! Explicit local ADBC handles over an existing Python/native context.
//! The bounded rendezvous owns only pending, one-use bootstrap capabilities;
//! there is no default database/session and no query execution in registration.

use adbc_core::{
    Database, Driver, Optionable,
    error::{Error, Result, Status},
    options::{OptionConnection, OptionDatabase, OptionValue},
};
use pyo3::{prelude::*, types::PyModule};
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock, Mutex},
};

#[path = "adbc_connection.rs"]
mod connection;
#[path = "adbc_statement.rs"]
mod statement;

type Bridge = Arc<Py<PyAny>>;
static PENDING: LazyLock<Mutex<BTreeMap<String, Py<PyAny>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(_adbc_register, module)?)?;
    module.add_function(wrap_pyfunction!(_adbc_discard, module)?)?;
    Ok(())
}

#[pyfunction]
fn _adbc_register(token: String, bridge: Py<PyAny>) -> PyResult<usize> {
    let mut pending = PENDING
        .lock()
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("ADBC bootstrap lock poisoned"))?;
    if token.len() != 64 || !token.bytes().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "ADBC bootstrap requires a 32-byte hexadecimal capability",
        ));
    }
    if pending.len() >= 64 || pending.contains_key(&token) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "ADBC bootstrap capacity or duplicate capability",
        ));
    }
    pending.insert(token, bridge);
    Ok(AdbcShardloomInit as *const () as usize)
}

#[pyfunction]
fn _adbc_discard(token: &str) -> PyResult<()> {
    let value = PENDING
        .lock()
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("ADBC bootstrap lock poisoned"))?
        .remove(token);
    drop(value);
    Ok(())
}

fn unsupported(feature: &str) -> Error {
    Error::with_message_and_status(
        format!(
            "ShardLoom local ADBC does not support {feature}; no fallback execution was attempted; external_engine_invoked=false"
        ),
        Status::NotImplemented,
    )
}

fn invalid(message: &str) -> Error {
    Error::with_message_and_status(message, Status::InvalidArguments)
}

#[allow(clippy::needless_pass_by_value)] // Result::map_err consumes its error.
fn python_error(error: PyErr) -> Error {
    Python::attach(|py| {
        let code = error
            .value(py)
            .getattr("diagnostic")
            .ok()
            .and_then(|value| value.get_item("code").ok())
            .and_then(|value| value.extract::<String>().ok())
            .or_else(|| {
                error
                    .value(py)
                    .getattr("diagnostics")
                    .ok()?
                    .get_item(0)
                    .ok()?
                    .getattr("code")
                    .ok()?
                    .extract::<String>()
                    .ok()
            })
            .unwrap_or_default();
        let message = error.to_string();
        let status = if code.contains("CANCEL") || message.contains("SL_CANCELLED") {
            Status::Cancelled
        } else if code.contains("TIMEOUT") || message.contains("SL_TIMEOUT") {
            Status::Timeout
        } else if error.is_instance_of::<pyo3::exceptions::PyValueError>(py)
            || error.is_instance_of::<pyo3::exceptions::PyTypeError>(py)
            || code.contains("INVALID")
        {
            Status::InvalidArguments
        } else {
            Status::Internal
        };
        Error::with_message_and_status(message, status)
    })
}

fn call0(bridge: &Bridge, method: &str) -> Result<()> {
    Python::attach(|py| {
        bridge
            .bind(py)
            .call_method0(method)
            .map(|_| ())
            .map_err(python_error)
    })
}

// Required typed option accessors share explicit unsupported diagnostics.
macro_rules! unsupported_option_reads {
    ($option:ty) => {
        fn get_option_bytes(&self, _key: $option) -> Result<Vec<u8>> {
            Err(unsupported("binary options"))
        }
        fn get_option_int(&self, _key: $option) -> Result<i64> {
            Err(unsupported("integer options"))
        }
        fn get_option_double(&self, _key: $option) -> Result<f64> {
            Err(unsupported("floating-point options"))
        }
    };
}
use unsupported_option_reads;

#[derive(Default)]
struct LocalDriver;

#[derive(Default)]
struct LocalDatabase {
    bridge: Option<Bridge>,
}

impl Driver for LocalDriver {
    type DatabaseType = LocalDatabase;
    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(LocalDatabase::default())
    }
    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = self.new_database()?;
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

impl Optionable for LocalDatabase {
    type Option = OptionDatabase;
    fn set_option(&mut self, key: OptionDatabase, value: OptionValue) -> Result<()> {
        if key.as_ref() != "shardloom.context" {
            return Err(unsupported(key.as_ref()));
        }
        if self.bridge.is_some() {
            return Err(invalid("ADBC context is already bound"));
        }
        let OptionValue::String(token) = value else {
            return Err(invalid("ADBC context requires a string capability"));
        };
        let bridge = PENDING
            .lock()
            .map_err(|_| invalid("ADBC bootstrap lock poisoned"))?
            .remove(&token)
            .ok_or_else(|| {
                invalid("ADBC context capability is unknown, expired, or already consumed")
            })?;
        self.bridge = Some(Arc::new(bridge));
        Ok(())
    }
    fn get_option_string(&self, _key: OptionDatabase) -> Result<String> {
        Err(unsupported("database option reads"))
    }
    unsupported_option_reads!(OptionDatabase);
}

impl Database for LocalDatabase {
    type ConnectionType = connection::LocalConnection;
    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts([])
    }
    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let bridge = self.bridge.as_ref().ok_or_else(|| {
            invalid("ADBC requires an explicitly selected existing ShardLoom context")
        })?;
        let bridge = Python::attach(|py| {
            bridge
                .bind(py)
                .call_method0("new_connection")
                .map(Bound::unbind)
        })
        .map_err(python_error)?;
        let mut connection = connection::LocalConnection {
            bridge: Arc::new(bridge),
        };
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

// The reviewed Apache ADBC exporter owns all raw-pointer/release-callback glue.
// No handwritten pointer dereference or foreign execution is added here.
adbc_ffi::export_driver!(AdbcShardloomInit, LocalDriver);
