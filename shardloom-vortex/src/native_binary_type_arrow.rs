//! Arrow extension carriage for opaque binary-object descriptor cells.
//!
//! This adapter preserves Binary values and validity only. It does not parse a
//! descriptor, resolve an object, or read payload bytes.

use super::{BinaryObjectType, Metadata};
use arrow_array::ArrayRef as ArrowArray;
use arrow_schema::{DataType, Field};
use std::{collections::HashMap, sync::Arc};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray,
        arrays::{ExtensionArray, extension::ExtensionArrayExt as _},
        dtype::{
            DType,
            extension::{ExtDType, ExtVTable},
        },
    },
    arrow::{
        ArrowExport, ArrowExportOptions, ArrowExportVTable, ArrowImport, ArrowImportVTable,
        ArrowSession, ArrowSessionExt as _,
    },
    error::{VortexResult, vortex_ensure, vortex_err},
    session::{VortexSession, registry::Id},
};

const NAME: &str = "ARROW:extension:name";
const META: &str = "ARROW:extension:metadata";
const VERSION: &str = "{\"version\":1}";

#[derive(Debug)]
struct BinaryObjectArrow;

pub(super) fn register(session: &VortexSession) {
    session
        .arrow()
        .register_importer(Arc::new(BinaryObjectArrow));
    session
        .arrow()
        .register_exporter(Arc::new(BinaryObjectArrow));
}

/// Allocation-free wire-shape admission; descriptor contents are opaque here.
pub(crate) fn admitted(field: &Field) -> bool {
    field
        .metadata()
        .get(NAME)
        .is_some_and(|name| name == super::ID)
        && field
            .metadata()
            .get(META)
            .is_some_and(|metadata| metadata == VERSION)
        && field.data_type() == &DataType::Binary
}

pub(crate) fn field(name: &str, dtype: &DType) -> VortexResult<Field> {
    let DType::Extension(extension) = dtype else {
        return Err(vortex_err!("binary-object requires an extension dtype"));
    };
    if extension.metadata_opt::<BinaryObjectType>().is_none() {
        return Err(vortex_err!("binary-object requires typed metadata"));
    }
    let DType::Binary(nullable) = extension.storage_dtype() else {
        return Err(vortex_err!("binary-object storage must be Binary"));
    };
    Ok(
        Field::new(name, DataType::Binary, nullable.is_nullable()).with_metadata(HashMap::from([
            (NAME.to_owned(), super::ID.to_owned()),
            (META.to_owned(), VERSION.to_owned()),
        ])),
    )
}

fn dtype(field: &Field) -> VortexResult<DType> {
    vortex_ensure!(
        admitted(field),
        "invalid Arrow binary-object extension or storage"
    );
    Ok(DType::Extension(
        ExtDType::<BinaryObjectType>::try_new(Metadata, DType::Binary(field.is_nullable().into()))?
            .erased(),
    ))
}

impl ArrowImportVTable for BinaryObjectArrow {
    fn arrow_ext_id(&self) -> Id {
        <BinaryObjectType as ExtVTable>::id(&BinaryObjectType)
    }

    fn from_arrow_field(&self, field: &Field, _: &ArrowSession) -> VortexResult<Option<DType>> {
        dtype(field).map(Some)
    }

    fn from_arrow_array(
        &self,
        array: ArrowArray,
        field: &Field,
        declared: &DType,
        session: &ArrowSession,
    ) -> VortexResult<ArrowImport> {
        vortex_ensure!(
            array.data_type() == field.data_type() && &dtype(field)? == declared,
            "Arrow binary-object storage or metadata mismatch"
        );
        let DType::Extension(extension) = declared else {
            return Err(vortex_err!("binary-object requires extension metadata"));
        };
        let physical = Field::new(field.name(), DataType::Binary, field.is_nullable());
        let storage = session.from_arrow_array(array, &physical)?;
        Ok(ArrowImport::Imported(
            ExtensionArray::try_new(extension.clone(), storage)?.into_array(),
        ))
    }
}

impl ArrowExportVTable for BinaryObjectArrow {
    fn arrow_ext_id(&self) -> Id {
        <BinaryObjectType as ExtVTable>::id(&BinaryObjectType)
    }
    fn vortex_id(&self) -> Id {
        <BinaryObjectType as ExtVTable>::id(&BinaryObjectType)
    }

    fn to_arrow_field(
        &self,
        name: &str,
        dtype: &DType,
        _: &ArrowSession,
    ) -> VortexResult<Option<Field>> {
        field(name, dtype).map(Some)
    }

    fn execute_arrow(
        &self,
        array: ArrayRef,
        target: &Field,
        options: &ArrowExportOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowExport> {
        vortex_ensure!(
            &dtype(target)? == array.dtype(),
            "binary-object export requires matching logical metadata"
        );
        let extension = array.execute::<ExtensionArray>(ctx)?;
        let physical = Field::new(target.name(), DataType::Binary, target.is_nullable());
        let exported = ArrowSession::default().exporter(options).execute_arrow(
            extension.storage_array().clone(),
            Some(&physical),
            ctx,
        )?;
        Ok(ArrowExport::Exported(exported))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array as _, BinaryArray, RecordBatch};
    use arrow_schema::Schema;
    use vortex::{
        VortexSessionDefault as _,
        array::{
            VortexSessionExecute as _,
            arrays::{Struct, struct_::StructArrayExt as _},
        },
    };

    #[test]
    fn binary_object_arrow_roundtrip_preserves_sliced_null_empty_and_bytes() {
        let session = VortexSession::default();
        super::super::register(&session);
        let values: ArrowArray = Arc::new(BinaryArray::from(vec![
            Some(&b"alpha"[..]),
            None,
            Some(&b""[..]),
            Some(&b"\0x"[..]),
        ]));
        let target = Field::new("object", DataType::Binary, true).with_metadata(HashMap::from([
            (NAME.into(), super::super::ID.into()),
            (META.into(), VERSION.into()),
        ]));
        assert!(admitted(&target));
        for input in [values.clone(), values.slice(1, 3), values.slice(2, 0)] {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![target.clone()])),
                vec![input.clone()],
            )
            .unwrap();
            let native = session
                .arrow()
                .from_arrow_record_batch(batch, &Schema::new(vec![target.clone()]))
                .unwrap();
            let native = native.as_::<Struct>().unmasked_field(0).clone();
            assert_eq!(
                session
                    .arrow()
                    .to_arrow_field("object", native.dtype())
                    .unwrap(),
                target
            );
            let output = session
                .arrow()
                .execute_arrow(native, Some(&target), &mut session.create_execution_ctx())
                .unwrap();
            let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
            let input = input.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(input.nulls(), output.nulls());
            assert_eq!(
                input.iter().collect::<Vec<_>>(),
                output.iter().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn binary_object_arrow_rejects_wrong_extension_metadata_or_storage() {
        let session = VortexSession::default();
        super::super::register(&session);
        for (storage, metadata) in [
            (DataType::Binary, "{\"version\":2}"),
            (DataType::Utf8, VERSION),
        ] {
            let field = Field::new("x", storage, true).with_metadata(HashMap::from([
                (NAME.into(), super::super::ID.into()),
                (META.into(), metadata.into()),
            ]));
            assert!(!admitted(&field));
            assert!(session.arrow().from_arrow_field(&field).is_err());
        }
    }
}
