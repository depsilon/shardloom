// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Keep Variant chunk boundaries visible to the registered Arrow provider.

use arrow_array::{ArrayRef as ArrowArrayRef, make_array};
use arrow_data::transform::MutableArrayData;
use arrow_schema::Field;
use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::variant::VariantArraySlotsExt;
use vortex_array::arrays::{Chunked, ConstantArray, Variant, VariantArray};
use vortex_array::dtype::DType;
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray};
use vortex_error::VortexResult;

use super::ArrowExporter;

pub(super) fn export(
    exporter: &ArrowExporter<'_>,
    array: &ArrayRef,
    target: &Field,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrowArrayRef>> {
    if !array.dtype().is_variant() {
        return Ok(None);
    }
    let canonical = array.as_opt::<Variant>();
    let core = canonical
        .as_ref()
        .map_or(array, |array| array.core_storage());
    let Some(chunks) = core.as_opt::<Chunked>() else {
        return Ok(None);
    };
    let mut arrays = Vec::with_capacity(chunks.nchunks());
    let mut offset = 0;
    for chunk in chunks.iter_chunks() {
        let end = offset + chunk.len();
        let chunk = if let Some(canonical) = &canonical {
            let shredded = canonical
                .shredded()
                .map(|child| child.slice(offset..end))
                .transpose()?;
            VariantArray::try_new(chunk.clone(), shredded)?.into_array()
        } else {
            chunk.clone()
        };
        arrays.push(exporter.execute_arrow(chunk, Some(target), ctx)?);
        offset = end;
    }
    if arrays.is_empty() {
        // An empty chunk sequence still needs the registered plugin's target
        // admission. Do not manufacture an arbitrary Arrow type merely because
        // there are no values. The zero-length constant retains the logical
        // nullability and prevents another chunk/canonicalization loop.
        let scalar = if array.dtype().is_nullable() {
            Scalar::null(array.dtype().clone())
        } else {
            Scalar::variant(Scalar::null(DType::Null))
        };
        let core = ConstantArray::new(scalar, 0).into_array();
        let empty = match canonical {
            Some(canonical) => {
                VariantArray::try_new(core, canonical.shredded().cloned())?.into_array()
            }
            None => core,
        };
        return exporter.execute_arrow(empty, Some(target), ctx).map(Some);
    }
    if arrays.len() == 1 {
        return Ok(arrays.pop());
    }
    let data = arrays
        .iter()
        .map(|array| array.to_data())
        .collect::<Vec<_>>();
    let mut joined = MutableArrayData::new(data.iter().collect(), false, array.len());
    for (index, chunk) in data.iter().enumerate() {
        joined.try_extend(index, 0, chunk.len())?;
    }
    Ok(Some(make_array(joined.freeze())))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array as _, Int32Array};
    use arrow_schema::DataType;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::{ChunkedArray, PrimitiveArray};
    use vortex_array::dtype::Nullability;
    use vortex_error::{vortex_bail, vortex_ensure};
    use vortex_session::VortexSession;
    use vortex_session::registry::{CachedId, Id};

    use super::*;
    use crate::{ArrowExport, ArrowExportOptions, ArrowExportVTable, ArrowSession};

    static ID: CachedId = CachedId::new("shardloom.test.variant-chunks");

    /// A strict leaf exporter exposes whether dispatch retains chunk boundaries
    /// and aligns the separately shredded values with each core chunk.
    #[derive(Debug)]
    struct LeafExporter;

    impl ArrowExportVTable for LeafExporter {
        fn arrow_ext_id(&self) -> Id {
            *ID
        }

        fn vortex_id(&self) -> Id {
            *ID
        }

        fn to_arrow_field(
            &self,
            _name: &str,
            _dtype: &DType,
            _session: &ArrowSession,
        ) -> VortexResult<Option<Field>> {
            Ok(None)
        }

        fn execute_arrow(
            &self,
            array: ArrayRef,
            target: &Field,
            _options: &ArrowExportOptions,
            ctx: &mut ExecutionCtx,
        ) -> VortexResult<ArrowExport> {
            vortex_ensure!(target.data_type() == &DataType::Int32, "unsupported target");
            let canonical = array.as_opt::<Variant>();
            let core = canonical
                .as_ref()
                .map_or(&array, |value| value.core_storage());
            if core.is::<Chunked>() {
                vortex_bail!("export plugin received an unsplit Variant core");
            }
            let shredded = canonical.as_ref().and_then(|value| value.shredded());
            let mut values = Vec::with_capacity(array.len());
            for row in 0..array.len() {
                let value = match shredded {
                    Some(child) => child.execute_scalar(row, ctx)?,
                    None => core
                        .execute_scalar(row, ctx)?
                        .as_variant()
                        .value()
                        .unwrap()
                        .clone(),
                };
                values.push(value.as_primitive().typed_value::<i32>());
            }
            Ok(ArrowExport::Exported(Arc::new(Int32Array::from(values))))
        }
    }

    fn field() -> Field {
        Field::new("payload", DataType::Int32, true)
            .with_metadata([("ARROW:extension:name".into(), ID.to_string())].into())
    }

    fn constant(value: i32, len: usize) -> ArrayRef {
        ConstantArray::new(
            Scalar::variant(Scalar::primitive(value, Nullability::NonNullable)),
            len,
        )
        .into_array()
    }

    #[test]
    fn variant_chunk_export_keeps_order_and_canonical_shredded_coordinates() {
        let arrow = ArrowSession::default();
        arrow.register_exporter(Arc::new(LeafExporter));
        let mut ctx = VortexSession::empty().create_execution_ctx();
        for (chunks, expected) in [
            (vec![constant(7, 3)], vec![7, 7, 7]),
            (
                vec![constant(1, 2), constant(8, 1), constant(-4, 2)],
                vec![1, 1, 8, -4, -4],
            ),
        ] {
            let core = ChunkedArray::try_new(chunks, DType::Variant(Nullability::NonNullable))
                .unwrap()
                .into_array();
            let output = arrow
                .execute_arrow(core.clone(), Some(&field()), &mut ctx)
                .unwrap();
            assert_eq!(
                output.as_any().downcast_ref::<Int32Array>().unwrap(),
                &Int32Array::from(expected)
            );
            let values = (10..10 + i32::try_from(core.len()).unwrap()).collect::<Vec<_>>();
            let array = VariantArray::try_new(
                core,
                Some(PrimitiveArray::from_iter(values.clone()).into_array()),
            )
            .unwrap()
            .into_array();
            let output = arrow
                .execute_arrow(array, Some(&field()), &mut ctx)
                .unwrap();
            assert_eq!(
                output.as_any().downcast_ref::<Int32Array>().unwrap(),
                &Int32Array::from(values)
            );
        }
    }

    #[test]
    fn variant_empty_chunks_still_visit_registered_target_admission() {
        let mut ctx = VortexSession::empty().create_execution_ctx();
        for nullability in [Nullability::Nullable, Nullability::NonNullable] {
            let empty = ChunkedArray::try_new(Vec::new(), DType::Variant(nullability))
                .unwrap()
                .into_array();
            let arrow = ArrowSession::default();
            arrow.register_exporter(Arc::new(LeafExporter));
            for array in [
                empty.clone(),
                VariantArray::try_new(empty, None).unwrap().into_array(),
            ] {
                let output = arrow
                    .execute_arrow(array.clone(), Some(&field()), &mut ctx)
                    .unwrap();
                assert!(output.is_empty());
                assert_eq!(output.data_type(), &DataType::Int32);
                let bad_target = field().with_data_type(DataType::Float64);
                assert!(
                    arrow
                        .execute_arrow(array, Some(&bad_target), &mut ctx)
                        .unwrap_err()
                        .to_string()
                        .contains("unsupported target")
                );
            }
        }
    }
}
