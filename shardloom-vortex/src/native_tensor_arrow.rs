//! Canonical Arrow fixed-shape tensor metadata at the explicit exchange boundary.

use super::{FixedShapeTensor, Metadata};
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
    session::{
        VortexSession,
        registry::{CachedId, Id},
    },
};

const NAME: &str = "ARROW:extension:name";
const META: &str = "ARROW:extension:metadata";
#[derive(Debug)]
struct TensorArrow;

pub(super) fn register(session: &VortexSession) {
    session.arrow().register_importer(Arc::new(TensorArrow));
    session.arrow().register_exporter(Arc::new(TensorArrow));
}

pub(crate) fn element(dtype: &DataType, nullable: bool) -> Option<DType> {
    let ptype = match dtype {
        DataType::Boolean => return Some(DType::Bool(nullable.into())),
        DataType::Int8 => PType::I8,
        DataType::Int16 => PType::I16,
        DataType::Int32 => PType::I32,
        DataType::Int64 => PType::I64,
        DataType::UInt8 => PType::U8,
        DataType::UInt16 => PType::U16,
        DataType::UInt32 => PType::U32,
        DataType::UInt64 => PType::U64,
        DataType::Float16 => PType::F16,
        DataType::Float32 => PType::F32,
        DataType::Float64 => PType::F64,
        _ => return None,
    };
    Some(DType::Primitive(ptype, nullable.into()))
}

/// Allocation-free preflight; full metadata parsing follows workspace admission.
pub(crate) fn admitted(field: &Field) -> bool {
    field
        .metadata()
        .get(NAME)
        .is_some_and(|name| name == super::ARROW_ID)
        && field
            .metadata()
            .get(META)
            .is_some_and(|raw| raw.len() <= super::MAX_METADATA_BYTES)
        && matches!(field.data_type(), DataType::FixedSizeList(child, size)
            if *size >= 0 && element(child.data_type(), child.is_nullable()).is_some()
                && !child.metadata().contains_key(NAME) && !child.metadata().contains_key(META))
}

pub(crate) fn field(name: &str, dtype: &DType) -> VortexResult<Field> {
    let DType::Extension(extension) = dtype else {
        return Err(vortex_err!("tensor requires extension dtype"));
    };
    let metadata = extension
        .metadata_opt::<FixedShapeTensor>()
        .ok_or_else(|| vortex_err!("tensor requires typed metadata"))?;
    let DType::FixedSizeList(child, size, nullable) = extension.storage_dtype() else {
        return Err(vortex_err!("tensor requires fixed-size list storage"));
    };
    let component = match child.as_ref() {
        DType::Bool(_) => DataType::Boolean,
        DType::Primitive(PType::I8, _) => DataType::Int8,
        DType::Primitive(PType::I16, _) => DataType::Int16,
        DType::Primitive(PType::I32, _) => DataType::Int32,
        DType::Primitive(PType::I64, _) => DataType::Int64,
        DType::Primitive(PType::U8, _) => DataType::UInt8,
        DType::Primitive(PType::U16, _) => DataType::UInt16,
        DType::Primitive(PType::U32, _) => DataType::UInt32,
        DType::Primitive(PType::U64, _) => DataType::UInt64,
        DType::Primitive(PType::F16, _) => DataType::Float16,
        DType::Primitive(PType::F32, _) => DataType::Float32,
        DType::Primitive(PType::F64, _) => DataType::Float64,
        _ => return Err(vortex_err!("unsupported tensor element type")),
    };
    let size = i32::try_from(*size).map_err(|_| {
        vortex_err!("tensor shape product exceeds Arrow's signed 32-bit list width")
    })?;
    Ok(Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", component, child.is_nullable())),
            size,
        ),
        nullable.is_nullable(),
    )
    .with_metadata(HashMap::from([
        (NAME.into(), super::ARROW_ID.into()),
        (META.into(), metadata.as_str().into()),
    ])))
}

fn dtype(field: &Field) -> VortexResult<DType> {
    vortex_ensure!(admitted(field), "invalid Arrow tensor extension or storage");
    let DataType::FixedSizeList(child, size) = field.data_type() else {
        unreachable!("admitted shape");
    };
    let storage = DType::FixedSizeList(
        element(child.data_type(), child.is_nullable())
            .expect("admitted element")
            .into(),
        u32::try_from(*size).map_err(|error| vortex_err!("{error}"))?,
        field.is_nullable().into(),
    );
    Ok(DType::Extension(
        ExtDType::<FixedShapeTensor>::try_new(Metadata::parse(&field.metadata()[META])?, storage)?
            .erased(),
    ))
}

fn arrow_id() -> Id {
    static ID: CachedId = CachedId::new(super::ARROW_ID);
    *ID
}

impl ArrowImportVTable for TensorArrow {
    fn arrow_ext_id(&self) -> Id {
        arrow_id()
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
            "Arrow tensor storage or metadata mismatch"
        );
        let DType::Extension(extension) = declared else {
            return Err(vortex_err!("tensor requires extension metadata"));
        };
        let physical = Field::new(field.name(), field.data_type().clone(), field.is_nullable());
        let storage = session.from_arrow_array(array, &physical)?;
        Ok(ArrowImport::Imported(
            ExtensionArray::try_new(extension.clone(), storage)?.into_array(),
        ))
    }
}
impl ArrowExportVTable for TensorArrow {
    fn arrow_ext_id(&self) -> Id {
        arrow_id()
    }
    fn vortex_id(&self) -> Id {
        FixedShapeTensor::default().id()
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
            "tensor export requires matching logical metadata"
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
    use arrow_array::{Array as _, FixedSizeListArray, Float32Array, Int32Array};
    use vortex::{VortexSessionDefault as _, array::VortexSessionExecute as _};

    fn target(storage: DataType, nullable: bool, metadata: &str) -> Field {
        Field::new("t", storage, nullable).with_metadata(HashMap::from([
            (NAME.into(), super::super::ARROW_ID.into()),
            (META.into(), metadata.into()),
        ]))
    }

    #[test]
    fn fixed_tensor_arrow_preserves_slices_nulls_special_bits_and_permutation() {
        let session = VortexSession::default();
        crate::native_extensions::register(&session);
        let values = Arc::new(Float32Array::from(
            vec![Some(1.0); 6]
                .into_iter()
                .chain([
                    Some(f32::from_bits(0xff80_0042)),
                    Some(-0.0),
                    None,
                    Some(f32::INFINITY),
                    Some(f32::NEG_INFINITY),
                    Some(1.25),
                ])
                .collect::<Vec<_>>(),
        ));
        let component = Arc::new(Field::new("item", DataType::Float32, true));
        let input: ArrowArray = Arc::new(FixedSizeListArray::new(component, 6, values, None));
        let target = target(
            input.data_type().clone(),
            true,
            r#"{"dim_names":["Ω","h","w"],"permutation":[2,0,1],"shape":[2,1,3]}"#,
        );
        for input in [input.clone(), input.slice(1, 1), input.slice(2, 0)] {
            let native = session
                .arrow()
                .from_arrow_array(input.clone(), &target)
                .unwrap();
            assert_eq!(
                session.arrow().to_arrow_field("t", native.dtype()).unwrap(),
                target
            );
            let output = session
                .arrow()
                .execute_arrow(native, Some(&target), &mut session.create_execution_ctx())
                .unwrap();
            let output = output
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let input = input.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            assert_eq!(input.len(), output.len());
            assert_eq!(
                (0..input.len())
                    .map(|row| input.is_null(row))
                    .collect::<Vec<_>>(),
                (0..output.len())
                    .map(|row| output.is_null(row))
                    .collect::<Vec<_>>()
            );
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
            assert_eq!(
                (0..input.len())
                    .map(|row| input.is_null(row))
                    .collect::<Vec<_>>(),
                (0..output.len())
                    .map(|row| output.is_null(row))
                    .collect::<Vec<_>>()
            );
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
    fn fixed_tensor_arrow_keeps_rows_for_zero_width_and_scalar_shapes() {
        let session = VortexSession::default();
        crate::native_extensions::register(&session);
        for (size, shape) in [(0, "[0,2]"), (1, "[]")] {
            for nullable in [false, true] {
                let values = Arc::new(Int32Array::from(vec![
                    17;
                    usize::try_from(size).unwrap() * 3
                ]));
                let input: ArrowArray = Arc::new(
                    FixedSizeListArray::try_new_with_length(
                        Arc::new(Field::new("item", DataType::Int32, false)),
                        size,
                        values,
                        nullable.then(|| arrow_buffer::NullBuffer::from(vec![true, false, true])),
                        3,
                    )
                    .unwrap(),
                );
                let target = target(
                    input.data_type().clone(),
                    nullable,
                    &format!(r#"{{"shape":{shape}}}"#),
                );
                let native = session
                    .arrow()
                    .from_arrow_array(input.clone(), &target)
                    .unwrap();
                let output = session
                    .arrow()
                    .execute_arrow(native, Some(&target), &mut session.create_execution_ctx())
                    .unwrap();
                assert_eq!(output.to_data(), input.to_data());
            }
        }
    }

    #[test]
    fn fixed_tensor_arrow_refuses_shape_changes_and_export_width_overflow() {
        let session = VortexSession::default();
        crate::native_extensions::register(&session);
        let physical =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 6);
        for raw in [
            r#"{"shape":[2,4]}"#,
            r#"{"shape":[2,3],"permutation":[0,0]}"#,
            r#"{"shape":[2,3],"unknown":true}"#,
        ] {
            assert!(
                session
                    .arrow()
                    .from_arrow_field(&target(physical.clone(), true, raw))
                    .is_err()
            );
        }
        let original = target(physical.clone(), true, r#"{"shape":[2,3]}"#);
        let array: ArrowArray = Arc::new(FixedSizeListArray::new_null(
            Arc::new(Field::new("item", DataType::Float64, true)),
            6,
            1,
        ));
        let native = session.arrow().from_arrow_array(array, &original).unwrap();
        let changed = target(physical, true, r#"{"shape":[3,2]}"#);
        assert!(
            session
                .arrow()
                .execute_arrow(native, Some(&changed), &mut session.create_execution_ctx())
                .is_err()
        );
        let dtype = DType::Extension(
            ExtDType::<FixedShapeTensor>::try_new(
                Metadata::parse(r#"{"shape":[4294967295]}"#).unwrap(),
                DType::FixedSizeList(
                    DType::Primitive(PType::F64, true.into()).into(),
                    u32::MAX,
                    true.into(),
                ),
            )
            .unwrap()
            .erased(),
        );
        assert!(
            field("t", &dtype)
                .unwrap_err()
                .to_string()
                .contains("Arrow")
        );
    }
}
