//! Explicit duration translation through the existing Vortex Arrow plugin API.
//! The private dispatch marker retains Arrow's standard Duration physical type.

use super::duration::Duration;
use arrow_array::{Array, ArrayRef as ArrowArray, make_array};
use arrow_schema::{DataType, Field, Schema, extension::EXTENSION_TYPE_NAME_KEY};
use std::sync::Arc;
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray,
        arrays::ExtensionArray,
        builtins::ArrayBuiltins as _,
        dtype::{DType, extension::ExtVTable},
    },
    arrow::{
        ArrowExport, ArrowExportOptions, ArrowExportVTable, ArrowImport, ArrowImportVTable,
        ArrowSession, ArrowSessionExt as _,
    },
    error::{VortexResult, vortex_ensure, vortex_err},
    session::{VortexSession, registry::Id},
};

#[derive(Debug)]
struct DurationArrow;

pub(super) fn register(session: &VortexSession) {
    session.arrow().register_importer(Arc::new(DurationArrow));
    session.arrow().register_exporter(Arc::new(DurationArrow));
}

/// Supply explicit provider dispatch metadata without touching input buffers.
/// Logical schema admission and its resource bounds belong to the caller;
/// preparation may preserve physical Null/dictionary fields that cannot be keys.
pub(crate) fn field(field: &Field) -> shardloom_core::Result<Field> {
    if !crate::native_payload_schema::arrow_field_metadata_admitted(field) {
        return Err(shardloom_core::ShardLoomError::InvalidOperation(
            "Arrow source extension metadata is not admitted; no fallback execution was attempted"
                .into(),
        ));
    }
    let dtype = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| self::field(f))
                .collect::<shardloom_core::Result<Vec<_>>>()?
                .into(),
        ),
        DataType::List(child) => DataType::List(Arc::new(self::field(child)?)),
        DataType::LargeList(child) => DataType::LargeList(Arc::new(self::field(child)?)),
        DataType::ListView(child) => DataType::ListView(Arc::new(self::field(child)?)),
        DataType::LargeListView(child) => DataType::LargeListView(Arc::new(self::field(child)?)),
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(Arc::new(self::field(child)?), *size)
        }
        other => other.clone(),
    };
    let mut result = field.clone().with_data_type(dtype);
    if matches!(result.data_type(), DataType::Duration(_)) {
        let mut metadata = result.metadata().clone();
        metadata.insert(
            EXTENSION_TYPE_NAME_KEY.to_owned(),
            "shardloom.duration.v1".to_owned(),
        );
        result = result.with_metadata(metadata);
    }
    Ok(result)
}

pub(crate) fn schema(schema: &Schema) -> shardloom_core::Result<Schema> {
    Ok(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| field(f))
            .collect::<shardloom_core::Result<Vec<_>>>()?,
        schema.metadata().clone(),
    ))
}

impl ArrowImportVTable for DurationArrow {
    fn arrow_ext_id(&self) -> Id {
        Duration.id()
    }

    fn from_arrow_field(&self, field: &Field, _: &ArrowSession) -> VortexResult<Option<DType>> {
        let Some(dtype) = crate::temporal_arrow::dtype(field.data_type()) else {
            return Err(vortex_err!("invalid Arrow duration metadata"));
        };
        vortex_ensure!(
            matches!(
                dtype.kind(),
                shardloom_core::temporal::TemporalKind::Duration(_)
            ),
            "duration extension marker requires Arrow Duration storage"
        );
        Ok(Some(
            super::vortex_dtype(&dtype, field.is_nullable().into())
                .map_err(|e| vortex_err!("{e}"))?,
        ))
    }

    fn from_arrow_array(
        &self,
        array: ArrowArray,
        field: &Field,
        dtype: &DType,
        session: &ArrowSession,
    ) -> VortexResult<ArrowImport> {
        vortex_ensure!(
            array.data_type() == field.data_type(),
            "Arrow duration storage metadata mismatch"
        );
        let DType::Extension(extension) = dtype else {
            return Err(vortex_err!("duration requires an extension dtype"));
        };
        vortex_ensure!(
            extension.metadata_opt::<Duration>().is_some()
                && super::temporal_dtype(dtype)
                    .as_ref()
                    .map(crate::temporal_arrow::data_type)
                    .as_ref()
                    == Some(field.data_type()),
            "duration requires matching typed unit metadata"
        );
        let storage = make_array(
            array
                .to_data()
                .into_builder()
                .data_type(DataType::Int64)
                .build()?,
        );
        let storage = session.from_arrow_array(storage, field.is_nullable())?;
        Ok(ArrowImport::Imported(
            ExtensionArray::new(extension.clone(), storage).into_array(),
        ))
    }
}

impl ArrowExportVTable for DurationArrow {
    fn arrow_ext_id(&self) -> Id {
        Duration.id()
    }
    fn vortex_id(&self) -> Id {
        Duration.id()
    }

    fn to_arrow_field(
        &self,
        name: &str,
        dtype: &DType,
        _: &ArrowSession,
    ) -> VortexResult<Option<Field>> {
        let Some(temporal) = super::temporal_dtype(dtype) else {
            return Ok(None);
        };
        if !matches!(
            temporal.kind(),
            shardloom_core::temporal::TemporalKind::Duration(_)
        ) {
            return Ok(None);
        }
        Ok(Some(
            field(&Field::new(
                name,
                crate::temporal_arrow::data_type(&temporal),
                dtype.is_nullable(),
            ))
            .map_err(|error| vortex_err!("{error}"))?,
        ))
    }

    fn execute_arrow(
        &self,
        array: ArrayRef,
        target: &Field,
        options: &ArrowExportOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowExport> {
        let Some(temporal) = super::temporal_dtype(array.dtype()) else {
            return Err(vortex_err!(
                "duration export requires typed temporal metadata"
            ));
        };
        vortex_ensure!(
            matches!(
                temporal.kind(),
                shardloom_core::temporal::TemporalKind::Duration(_)
            ) && crate::temporal_arrow::data_type(&temporal) == *target.data_type(),
            "duration export metadata mismatch"
        );
        let primitive = Field::new(target.name(), DataType::Int64, target.is_nullable());
        let storage = array.cast(DType::Primitive(
            vortex::array::dtype::PType::I64,
            array.dtype().nullability(),
        ))?;
        let result = ArrowSession::default().exporter(options).execute_arrow(
            storage,
            Some(&primitive),
            ctx,
        )?;
        let result = make_array(
            result
                .to_data()
                .into_builder()
                .data_type(target.data_type().clone())
                .build()?,
        );
        Ok(ArrowExport::Exported(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ListArray, RecordBatch, builder::make_builder, types::DurationNanosecondType,
    };
    use arrow_schema::TimeUnit;
    use vortex::{
        VortexSessionDefault as _,
        array::{
            VortexSessionExecute as _,
            arrays::{Struct, struct_::StructArrayExt as _},
        },
    };

    #[test]
    fn temporal_duration_arrow_translation_preserves_full_ticks_nulls_slices_and_empty() {
        let session = VortexSession::default();
        super::super::register(&session);
        let mut ctx = session.create_execution_ctx();
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let dtype = DataType::Duration(unit);
            let mut builder = make_builder(&dtype, 5);
            for value in [Some(99), None, Some(i64::MIN), Some(-1), Some(i64::MAX)] {
                crate::temporal_arrow::append(builder.as_mut(), &dtype, value).unwrap();
            }
            let source = builder.finish();
            for source in [source.slice(1, 4), source.slice(0, 0)] {
                let plain = Schema::new(vec![Field::new("elapsed", dtype.clone(), true)]);
                let tagged = schema(&plain).unwrap();
                let batch = RecordBatch::try_new(Arc::new(plain), vec![source.clone()]).unwrap();
                let native = session
                    .arrow()
                    .from_arrow_record_batch(batch, &tagged)
                    .unwrap();
                let column = native.as_::<Struct>().unmasked_field(0).clone();
                let logical = super::super::temporal_dtype(column.dtype()).unwrap();
                assert_eq!(crate::temporal_arrow::data_type(&logical), dtype);
                let exported = session
                    .arrow()
                    .execute_arrow(column, Some(tagged.field(0)), &mut ctx)
                    .unwrap();
                assert_eq!(exported.to_data(), source.to_data());
            }
        }
    }

    #[test]
    fn temporal_duration_arrow_dispatch_reaches_nullable_nested_children() {
        let session = VortexSession::default();
        super::super::register(&session);
        let source = Arc::new(
            ListArray::from_iter_primitive::<DurationNanosecondType, _, _>([
                Some(vec![Some(i64::MIN), None, Some(i64::MAX)]),
                None,
                Some(vec![]),
            ]),
        ) as ArrowArray;
        let plain = Schema::new(vec![Field::new("values", source.data_type().clone(), true)]);
        let tagged = schema(&plain).unwrap();
        let batch = RecordBatch::try_new(Arc::new(plain), vec![source.clone()]).unwrap();
        let native = session
            .arrow()
            .from_arrow_record_batch(batch, &tagged)
            .unwrap();
        let column = native.as_::<Struct>().unmasked_field(0).clone();
        let DType::List(child, _) = column.dtype() else {
            panic!("native list missing")
        };
        assert_eq!(
            super::super::temporal_dtype(child).unwrap().as_str(),
            "duration(ns)"
        );
        let exported = session
            .arrow()
            .execute_arrow(
                column,
                Some(tagged.field(0)),
                &mut session.create_execution_ctx(),
            )
            .unwrap();
        let output = exported.as_any().downcast_ref::<ListArray>().unwrap();
        let input = source.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.offsets(), input.offsets());
        assert_eq!(output.nulls(), input.nulls());
        assert_eq!(output.values().to_data(), input.values().to_data());
    }

    #[test]
    fn temporal_duration_arrow_rejects_foreign_or_inconsistent_extension_markers() {
        use std::collections::HashMap;
        for (dtype, marker) in [
            (DataType::Duration(TimeUnit::Second), "unknown.duration"),
            (DataType::Int64, "shardloom.duration.v1"),
        ] {
            let field = Field::new("x", dtype, true).with_metadata(HashMap::from([(
                EXTENSION_TYPE_NAME_KEY.into(),
                marker.into(),
            )]));
            assert!(schema(&Schema::new(vec![field])).is_err());
        }
    }

    #[test]
    fn temporal_duration_dispatch_preserves_preparation_dictionary_and_null_fields() {
        let plain = Schema::new(vec![
            Field::new("empty", DataType::Null, true),
            Field::new(
                "domain",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        assert_eq!(schema(&plain).unwrap(), plain);
        // Metadata validation still reaches nested fields before annotation.
        let foreign = Field::new("span", DataType::Duration(TimeUnit::Second), true).with_metadata(
            std::collections::HashMap::from([(
                EXTENSION_TYPE_NAME_KEY.into(),
                "unknown.duration".into(),
            )]),
        );
        let nested = Schema::new(vec![Field::new(
            "values",
            DataType::List(Arc::new(foreign)),
            true,
        )]);
        assert!(schema(&nested).is_err());
    }
}
