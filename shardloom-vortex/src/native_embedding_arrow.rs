//! Arrow's extension field keys carry the same admitted embedding identity.

use super::{Embedding, Metadata};
use arrow_array::ArrayRef as ArrowArray;
use arrow_schema::{DataType, Field};
use std::{collections::HashMap, sync::Arc};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray,
        arrays::{ExtensionArray, extension::ExtensionArrayExt as _},
        dtype::{
            DType, PType,
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

#[derive(Debug)]
struct EmbeddingArrow;

pub(super) fn register(session: &VortexSession) {
    session.arrow().register_importer(Arc::new(EmbeddingArrow));
    session.arrow().register_exporter(Arc::new(EmbeddingArrow));
}

/// Allocation-free shape admission. The importer validates the complete JSON
/// only after the caller has admitted metadata workspace.
pub(crate) fn admitted(field: &Field) -> bool {
    field
        .metadata()
        .get(NAME)
        .is_some_and(|name| name == super::ID)
        && field
            .metadata()
            .get(META)
            .is_some_and(|value| value.len() <= super::MAX_METADATA_BYTES)
        && matches!(field.data_type(), DataType::FixedSizeList(child, size)
            if *size > 0 && matches!(child.data_type(), DataType::Float32 | DataType::Float64)
                && !child.metadata().contains_key(NAME) && !child.metadata().contains_key(META))
}

pub(crate) fn field(name: &str, dtype: &DType) -> VortexResult<Field> {
    let DType::Extension(extension) = dtype else {
        return Err(vortex_err!("embedding requires an extension dtype"));
    };
    let metadata = extension
        .metadata_opt::<Embedding>()
        .ok_or_else(|| vortex_err!("embedding requires typed metadata"))?;
    let DType::FixedSizeList(child, size, nullable) = extension.storage_dtype() else {
        return Err(vortex_err!("embedding requires fixed-size list storage"));
    };
    let child_type = match child.as_ref() {
        DType::Primitive(PType::F32, _) => DataType::Float32,
        DType::Primitive(PType::F64, _) => DataType::Float64,
        _ => return Err(vortex_err!("embedding requires Float32/Float64 components")),
    };
    let size = i32::try_from(*size)
        .map_err(|_| vortex_err!("embedding dimension exceeds Arrow's signed 32-bit list width"))?;
    Ok(Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", child_type, child.is_nullable())),
            size,
        ),
        nullable.is_nullable(),
    )
    .with_metadata(HashMap::from([
        (NAME.to_owned(), super::ID.to_owned()),
        (META.to_owned(), metadata.as_str().to_owned()),
    ])))
}

fn dtype(field: &Field) -> VortexResult<DType> {
    vortex_ensure!(
        admitted(field),
        "invalid Arrow embedding extension or storage"
    );
    let DataType::FixedSizeList(child, size) = field.data_type() else {
        unreachable!("admitted shape");
    };
    let ptype = if child.data_type() == &DataType::Float32 {
        PType::F32
    } else {
        PType::F64
    };
    let storage = DType::FixedSizeList(
        DType::Primitive(ptype, child.is_nullable().into()).into(),
        u32::try_from(*size).map_err(|error| vortex_err!("{error}"))?,
        field.is_nullable().into(),
    );
    Ok(DType::Extension(
        ExtDType::<Embedding>::try_new(Metadata::parse(&field.metadata()[META])?, storage)?
            .erased(),
    ))
}

impl ArrowImportVTable for EmbeddingArrow {
    fn arrow_ext_id(&self) -> Id {
        Embedding::default().id()
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
            "Arrow embedding storage or metadata mismatch"
        );
        let DType::Extension(extension) = declared else {
            return Err(vortex_err!("embedding requires extension metadata"));
        };
        let physical = Field::new(field.name(), field.data_type().clone(), field.is_nullable());
        let storage = session.from_arrow_array(array, &physical)?;
        Ok(ArrowImport::Imported(
            ExtensionArray::try_new(extension.clone(), storage)?.into_array(),
        ))
    }
}

impl ArrowExportVTable for EmbeddingArrow {
    fn arrow_ext_id(&self) -> Id {
        Embedding::default().id()
    }
    fn vortex_id(&self) -> Id {
        Embedding::default().id()
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
            "embedding export requires matching logical metadata"
        );
        let extension = array.execute::<ExtensionArray>(ctx)?;
        let physical = Field::new(
            target.name(),
            target.data_type().clone(),
            target.is_nullable(),
        );
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
    use arrow_array::{Array as _, FixedSizeListArray, Float32Array, RecordBatch};
    use arrow_schema::Schema;
    use vortex::{
        VortexSessionDefault as _,
        array::{
            VortexSessionExecute as _,
            arrays::{Struct, struct_::StructArrayExt as _},
        },
    };

    #[test]
    fn dense_embedding_arrow_roundtrip_preserves_metadata_special_bits_and_slices() {
        let session = VortexSession::default();
        crate::native_extensions::register(&session);
        let values = Arc::new(Float32Array::from(vec![
            Some(f32::from_bits(0xff80_0042)),
            Some(-0.0),
            None,
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(1.25),
        ]));
        let component = Arc::new(Field::new("item", DataType::Float32, true));
        let input: ArrowArray = Arc::new(FixedSizeListArray::new(component, 2, values, None));
        let target = Field::new("embedding", input.data_type().clone(), false).with_metadata(
            HashMap::from([
                (NAME.into(), super::super::ID.into()),
                (
                    META.into(),
                    r#"{"model_id":"opaque/model","space_id":null,"version":1}"#.into(),
                ),
            ]),
        );
        for input in [input.clone(), input.slice(1, 2), input.slice(2, 0)] {
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
            let exported_field = session
                .arrow()
                .to_arrow_field("embedding", native.dtype())
                .unwrap();
            assert_eq!(exported_field, target);
            let output = session
                .arrow()
                .execute_arrow(native, Some(&target), &mut session.create_execution_ctx())
                .unwrap();
            let output = output
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let input = input.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            assert_eq!(input.nulls(), output.nulls());
            let input = input
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let output = output
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            assert_eq!(input.nulls(), output.nulls());
            assert_eq!(
                input
                    .values()
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>(),
                output
                    .values()
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn dense_embedding_arrow_rejects_changed_metadata_and_invalid_shapes() {
        let session = VortexSession::default();
        crate::native_extensions::register(&session);
        let storage =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 2);
        for raw in [
            r#"{"version":2}"#,
            r#"{"version":1,"space_id":7}"#,
            r#"{"version":1,"version":1}"#,
        ] {
            let field = Field::new("e", storage.clone(), true).with_metadata(HashMap::from([
                (NAME.into(), super::super::ID.into()),
                (META.into(), raw.into()),
            ]));
            assert!(session.arrow().from_arrow_field(&field).is_err());
        }
        let metadata = Metadata::parse(r#"{"version":1}"#).unwrap();
        let dtype = DType::Extension(
            ExtDType::<Embedding>::try_new(
                metadata,
                DType::FixedSizeList(
                    DType::Primitive(PType::F64, false.into()).into(),
                    u32::MAX,
                    false.into(),
                ),
            )
            .unwrap()
            .erased(),
        );
        assert!(
            field("e", &dtype)
                .unwrap_err()
                .to_string()
                .contains("Arrow")
        );
    }
}
