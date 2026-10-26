//! Catalog metadata and connection-scoped cancellation; no transaction claims.

use super::{Bridge, call0, python_error, unsupported, unsupported_option_reads};
use adbc_core::{
    Connection, Optionable,
    error::Result,
    options::{InfoCode, ObjectDepth, OptionConnection, OptionValue},
    schemas,
};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use arrow_schema::Schema;
use pyo3::prelude::*;
use std::{collections::HashSet, sync::Arc};

pub(super) struct LocalConnection {
    pub(super) bridge: Bridge,
}

impl Optionable for LocalConnection {
    type Option = OptionConnection;
    fn set_option(&mut self, key: OptionConnection, value: OptionValue) -> Result<()> {
        if matches!(
            key.as_ref(),
            "adbc.connection.autocommit" | "adbc.connection.readonly"
        ) && matches!(value, OptionValue::String(ref value) if value == "true")
        {
            return Ok(());
        }
        Err(unsupported(key.as_ref()))
    }
    fn get_option_string(&self, key: OptionConnection) -> Result<String> {
        match key.as_ref() {
            "adbc.connection.autocommit" | "adbc.connection.readonly" => Ok("true".into()),
            "adbc.connection.catalog" | "adbc.connection.db_schema" => Ok(String::new()),
            _ => Err(unsupported(key.as_ref())),
        }
    }
    unsupported_option_reads!(OptionConnection);
}

fn batch_reader(batch: RecordBatch) -> Box<dyn RecordBatchReader + Send> {
    let schema = batch.schema();
    Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
}

impl Connection for LocalConnection {
    type StatementType = super::statement::LocalStatement;
    fn new_statement(&mut self) -> Result<Self::StatementType> {
        call0(&self.bridge, "ensure_open")?;
        Ok(Self::StatementType::new(Arc::clone(&self.bridge)))
    }
    fn cancel(&mut self) -> Result<()> {
        call0(&self.bridge, "cancel")
    }
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let codes = codes.map(|codes| {
            codes
                .into_iter()
                .map(|code| u32::from(&code))
                .collect::<Vec<_>>()
        });
        Python::attach(|py| {
            let batch = self.bridge.bind(py).call_method1(
                "get_info",
                (schemas::GET_INFO_SCHEMA.as_ref().to_pyarrow(py)?, codes),
            )?;
            RecordBatch::from_pyarrow_bound(&batch).map(batch_reader)
        })
        .map_err(python_error)
    }
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Python::attach(|py| {
            let batch = self.bridge.bind(py).call_method1(
                "get_objects",
                (
                    schemas::GET_OBJECTS_SCHEMA.as_ref().to_pyarrow(py)?,
                    i32::from(depth),
                    catalog,
                    db_schema,
                    table_name,
                    table_type,
                    column_name,
                ),
            )?;
            RecordBatch::from_pyarrow_bound(&batch).map(batch_reader)
        })
        .map_err(python_error)
    }
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        Python::attach(|py| {
            Schema::from_pyarrow_bound(
                &self
                    .bridge
                    .bind(py)
                    .call_method1("table_schema", (catalog, db_schema, table_name))?,
            )
        })
        .map_err(python_error)
    }
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let schema = Arc::clone(&schemas::GET_TABLE_TYPES_SCHEMA);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::StringArray::from(vec!["VIEW"]))],
        )
        .map_err(adbc_core::error::Error::from)?;
        Ok(batch_reader(batch))
    }
    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(batch_reader(RecordBatch::new_empty(Arc::clone(
            &schemas::GET_STATISTIC_NAMES_SCHEMA,
        ))))
    }
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(unsupported("catalog statistics"))
    }
    fn commit(&mut self) -> Result<()> {
        Err(unsupported("transactions; local queries use autocommit"))
    }
    fn rollback(&mut self) -> Result<()> {
        Err(unsupported("transactions; local queries use autocommit"))
    }
    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(unsupported("partitioned remote results"))
    }
}

impl Drop for LocalConnection {
    fn drop(&mut self) {
        let _ = call0(&self.bridge, "close");
    }
}
