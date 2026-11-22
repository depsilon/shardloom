// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(
    deprecated,
    reason = "This module defines and implements a deprecated trait `ArrowArrayExecutor`"
)]

pub mod bool;
mod byte;
pub mod byte_view;
mod decimal;
mod dictionary;
mod fixed_size_list;
mod list;
mod list_view;
mod map;
pub mod null;
pub mod primitive;
mod run_end;
mod struct_;
mod temporal;
mod validity;

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use itertools::Itertools;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::List;
use vortex_array::arrays::VarBin;
use vortex_array::arrays::list::ListArraySlotsExt;
use vortex_array::arrays::varbin::VarBinArraySlotsExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::PType;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure_eq;

use crate::ArrowExporter;
use crate::executor::bool::to_arrow_bool;
use crate::executor::byte::to_arrow_byte_array;
use crate::executor::byte_view::to_arrow_byte_view;
use crate::executor::decimal::to_arrow_decimal;
use crate::executor::dictionary::DictExportFrame;
use crate::executor::dictionary::start_arrow_dictionary;
use crate::executor::dictionary::step_arrow_dictionary;
use crate::executor::fixed_size_list::FixedSizeListExportFrame;
use crate::executor::fixed_size_list::finish_arrow_fixed_list;
use crate::executor::fixed_size_list::start_arrow_fixed_list;
use crate::executor::list::ListExportFrame;
use crate::executor::list::finish_arrow_list;
use crate::executor::list::start_arrow_list;
use crate::executor::list_view::ListViewExportFrame;
use crate::executor::list_view::finish_arrow_list_view;
use crate::executor::list_view::start_arrow_list_view;
use crate::executor::map::MapExportFrame;
use crate::executor::map::finish_arrow_map;
use crate::executor::map::start_arrow_map;
use crate::executor::null::to_arrow_null;
use crate::executor::primitive::to_arrow_primitive;
use crate::executor::run_end::RunEndExportFrame;
use crate::executor::run_end::start_arrow_run_end;
use crate::executor::run_end::step_arrow_run_end;
use crate::executor::struct_::StructExportFrame;
use crate::executor::struct_::start_arrow_struct;
use crate::executor::struct_::step_arrow_struct;
use crate::executor::temporal::to_arrow_date;
use crate::executor::temporal::to_arrow_time;
use crate::executor::temporal::to_arrow_timestamp;
use crate::session::ArrowSessionExt;
use crate::session::ExecuteArrowPrep;

/// Trait for executing a Vortex array to produce an Arrow array.
#[deprecated(note = "Use an `ArrowSession` to perform conversions to/from Arrow arrays")]
pub trait ArrowArrayExecutor: Sized {
    /// Execute the array to produce an Arrow array.
    ///
    /// If a [`DataType`] is given, the array will be converted to the desired Arrow type.
    /// If `None`, the array's preferred (cheapest) Arrow type will be used.
    #[deprecated(note = "Use an `ArrowSession` to perform conversions to/from Arrow arrays")]
    fn execute_arrow(
        self,
        data_type: Option<&DataType>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowArrayRef>;

    /// Execute the array to produce an Arrow `RecordBatch` with the given schema.
    #[deprecated(note = "Use an `ArrowSession` to perform conversions to/from Arrow arrays")]
    fn execute_record_batch(
        self,
        schema: &Schema,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<RecordBatch> {
        let array = self.execute_arrow(Some(&DataType::Struct(schema.fields.clone())), ctx)?;
        Ok(RecordBatch::from(array.as_struct()))
    }

    /// Execute the array to produce Arrow `RecordBatch`'s with the given schema.
    #[deprecated(note = "Use an `ArrowSession` to perform conversions to/from Arrow arrays")]
    fn execute_record_batches(
        self,
        schema: &Schema,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Vec<RecordBatch>>;
}

#[expect(deprecated, reason = "backward compatibility")]
impl ArrowArrayExecutor for ArrayRef {
    fn execute_arrow(
        self,
        data_type: Option<&DataType>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowArrayRef> {
        let target = data_type.map(|dt| Field::new("", dt.clone(), self.dtype().is_nullable()));
        let session = ctx.session().clone();
        session.arrow().execute_arrow(self, target.as_ref(), ctx)
    }

    fn execute_record_batches(
        self,
        schema: &Schema,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Vec<RecordBatch>> {
        self.to_array_iterator()
            .map(|a| a?.execute_record_batch(schema, ctx))
            .try_collect()
    }
}

pub(crate) enum ExportTask {
    WithField {
        array: ArrayRef,
        target: Option<Field>,
    },
    Naive {
        array: ArrayRef,
        data_type: Option<DataType>,
    },
}

enum ExportFrame {
    AssertLen(usize),
    Struct(StructExportFrame),
    List32(ListExportFrame<i32>),
    List64(ListExportFrame<i64>),
    ListView32(ListViewExportFrame<i32>),
    ListView64(ListViewExportFrame<i64>),
    FixedSizeList(FixedSizeListExportFrame),
    Map(MapExportFrame),
    Dict(DictExportFrame),
    RunEnd(RunEndExportFrame),
}

impl From<StructExportFrame> for ExportFrame {
    fn from(frame: StructExportFrame) -> Self {
        Self::Struct(frame)
    }
}

impl From<ListExportFrame<i32>> for ExportFrame {
    fn from(frame: ListExportFrame<i32>) -> Self {
        Self::List32(frame)
    }
}

impl From<ListExportFrame<i64>> for ExportFrame {
    fn from(frame: ListExportFrame<i64>) -> Self {
        Self::List64(frame)
    }
}

impl From<ListViewExportFrame<i32>> for ExportFrame {
    fn from(frame: ListViewExportFrame<i32>) -> Self {
        Self::ListView32(frame)
    }
}

impl From<ListViewExportFrame<i64>> for ExportFrame {
    fn from(frame: ListViewExportFrame<i64>) -> Self {
        Self::ListView64(frame)
    }
}

impl From<FixedSizeListExportFrame> for ExportFrame {
    fn from(frame: FixedSizeListExportFrame) -> Self {
        Self::FixedSizeList(frame)
    }
}

impl From<MapExportFrame> for ExportFrame {
    fn from(frame: MapExportFrame) -> Self {
        Self::Map(frame)
    }
}

impl From<DictExportFrame> for ExportFrame {
    fn from(frame: DictExportFrame) -> Self {
        Self::Dict(frame)
    }
}

impl From<RunEndExportFrame> for ExportFrame {
    fn from(frame: RunEndExportFrame) -> Self {
        Self::RunEnd(frame)
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "short-lived iterator step; stacked frames live in Vec"
)]
enum ExportStep {
    Done(ArrowArrayRef),
    Descend {
        frame: ExportFrame,
        task: ExportTask,
    },
}

/// Drive Arrow export iteratively with an explicit frame stack so arbitrarily deep nested arrays
/// execute without overflowing the call stack.
pub(crate) fn run_export(
    exporter: &ArrowExporter<'_>,
    initial: ExportTask,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    let mut stack: Vec<ExportFrame> = Vec::new();
    let mut current_task = initial;

    loop {
        let mut step = match current_task {
            ExportTask::WithField { array, target } => {
                match exporter.prepare_execute_arrow(array, target.as_ref(), ctx)? {
                    ExecuteArrowPrep::Done(arrow) => ExportStep::Done(arrow),
                    ExecuteArrowPrep::Naive { array, data_type } => start_execute_arrow_naive(
                        array,
                        data_type.as_ref(),
                        exporter,
                        &mut stack,
                        ctx,
                    )?,
                }
            }
            ExportTask::Naive { array, data_type } => {
                start_execute_arrow_naive(array, data_type.as_ref(), exporter, &mut stack, ctx)?
            }
        };

        loop {
            match step {
                ExportStep::Descend { frame, task } => {
                    stack.push(frame);
                    current_task = task;
                    break;
                }
                ExportStep::Done(arrow) => match stack.pop() {
                    Some(frame) => {
                        step = advance_export_frame(frame, arrow, ctx)?;
                    }
                    None => return Ok(arrow),
                },
            }
        }
    }
}

fn advance_export_frame(
    frame: ExportFrame,
    completed: ArrowArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ExportStep> {
    match frame {
        ExportFrame::AssertLen(len) => {
            vortex_ensure_eq!(
                completed.len(),
                len,
                "Arrow array length does not match Vortex array length after conversion to {:?}",
                completed
            );
            Ok(ExportStep::Done(completed))
        }
        ExportFrame::Struct(struct_frame) => step_arrow_struct(struct_frame, completed, ctx),
        ExportFrame::List32(list_frame) => {
            finish_arrow_list::<i32>(list_frame, completed).map(ExportStep::Done)
        }
        ExportFrame::List64(list_frame) => {
            finish_arrow_list::<i64>(list_frame, completed).map(ExportStep::Done)
        }
        ExportFrame::ListView32(list_frame) => {
            finish_arrow_list_view::<i32>(list_frame, completed, ctx).map(ExportStep::Done)
        }
        ExportFrame::ListView64(list_frame) => {
            finish_arrow_list_view::<i64>(list_frame, completed, ctx).map(ExportStep::Done)
        }
        ExportFrame::FixedSizeList(fsl_frame) => {
            finish_arrow_fixed_list(fsl_frame, completed).map(ExportStep::Done)
        }
        ExportFrame::Map(map_frame) => finish_arrow_map(map_frame, completed).map(ExportStep::Done),
        ExportFrame::Dict(dict_frame) => step_arrow_dictionary(dict_frame, completed),
        ExportFrame::RunEnd(run_end_frame) => step_arrow_run_end(run_end_frame, completed),
    }
}

fn start_execute_arrow_naive(
    array: ArrayRef,
    data_type: Option<&DataType>,
    exporter: &ArrowExporter<'_>,
    stack: &mut Vec<ExportFrame>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ExportStep> {
    let len = array.len();
    let resolved_type: DataType = match data_type {
        Some(dt) => dt.clone(),
        None => infer_nearest_arrow_type(&array, ctx)?,
    };

    stack.push(ExportFrame::AssertLen(len));

    match &resolved_type {
        DataType::List(elements_field) => start_arrow_list::<i32>(array, elements_field, ctx),
        DataType::LargeList(elements_field) => start_arrow_list::<i64>(array, elements_field, ctx),
        DataType::FixedSizeList(elements_field, list_size) => {
            start_arrow_fixed_list(array, *list_size, elements_field, ctx)
        }
        DataType::ListView(elements_field) => {
            start_arrow_list_view::<i32>(array, elements_field, ctx)
        }
        DataType::LargeListView(elements_field) => {
            start_arrow_list_view::<i64>(array, elements_field, ctx)
        }
        DataType::Struct(fields) => {
            let fields = data_type.map(|_| fields);
            start_arrow_struct(array, fields, ctx)
        }
        DataType::Dictionary(codes_type, values_type) => {
            start_arrow_dictionary(array, codes_type, values_type, ctx)
        }
        DataType::RunEndEncoded(ends_type, values_type) => {
            start_arrow_run_end(array, ends_type.data_type(), values_type, ctx)
        }
        DataType::Map(entries_field, keys_sorted) => {
            start_arrow_map(array, entries_field, *keys_sorted, stack, ctx)
        }
        _ => execute_arrow_leaf(array, &resolved_type, exporter, ctx).map(ExportStep::Done),
    }
}

fn execute_arrow_leaf(
    array: ArrayRef,
    resolved_type: &DataType,
    exporter: &ArrowExporter<'_>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    match resolved_type {
        DataType::Null => to_arrow_null(array, ctx),
        DataType::Boolean => to_arrow_bool(array, ctx),
        DataType::Int8 => to_arrow_primitive::<Int8Type>(array, ctx),
        DataType::Int16 => to_arrow_primitive::<Int16Type>(array, ctx),
        DataType::Int32 => to_arrow_primitive::<Int32Type>(array, ctx),
        DataType::Int64 => to_arrow_primitive::<Int64Type>(array, ctx),
        DataType::UInt8 => to_arrow_primitive::<UInt8Type>(array, ctx),
        DataType::UInt16 => to_arrow_primitive::<UInt16Type>(array, ctx),
        DataType::UInt32 => to_arrow_primitive::<UInt32Type>(array, ctx),
        DataType::UInt64 => to_arrow_primitive::<UInt64Type>(array, ctx),
        DataType::Float16 => to_arrow_primitive::<Float16Type>(array, ctx),
        DataType::Float32 => to_arrow_primitive::<Float32Type>(array, ctx),
        DataType::Float64 => to_arrow_primitive::<Float64Type>(array, ctx),
        DataType::Binary => to_arrow_byte_array::<BinaryType>(array, ctx),
        DataType::LargeBinary => to_arrow_byte_array::<LargeBinaryType>(array, ctx),
        DataType::Utf8 => to_arrow_byte_array::<Utf8Type>(array, ctx),
        DataType::LargeUtf8 => to_arrow_byte_array::<LargeUtf8Type>(array, ctx),
        DataType::BinaryView => to_arrow_byte_view::<BinaryViewType>(array, exporter, ctx),
        DataType::Utf8View => to_arrow_byte_view::<StringViewType>(array, exporter, ctx),
        dt @ (DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)) => to_arrow_decimal(array, dt, ctx),
        dt @ (DataType::Date32 | DataType::Date64) => to_arrow_date(array, dt, ctx),
        dt @ (DataType::Time32(_) | DataType::Time64(_)) => to_arrow_time(array, dt, ctx),
        dt @ DataType::Timestamp(..) => to_arrow_timestamp(array, dt, ctx),
        _ => vortex_bail!("Conversion to Arrow type {resolved_type} is not supported"),
    }
}

/// Determine the preferred (cheapest) Arrow type for an array.
///
/// For most arrays, this returns the canonical Arrow type for the array's dtype.
/// However, some encodings have cheaper Arrow representations:
/// - `VarBinArray`: Uses `Utf8`/`Binary` (offset-based) instead of `Utf8View`/`BinaryView`
/// - `ListArray`: Uses `List` instead of `ListView`
fn infer_nearest_arrow_type(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<DataType> {
    Ok(infer_nearest_arrow_field(array, "", ctx)?
        .data_type()
        .clone())
}

/// Determine the preferred (cheapest) Arrow [`Field`] for an array.
///
/// Unlike [`infer_nearest_arrow_type`] this preserves the Field-level `ARROW:extension:name`
/// metadata that export plugins assign, which a bare [`DataType`] cannot carry.
pub(crate) fn infer_nearest_arrow_field(
    array: &ArrayRef,
    name: &str,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Field> {
    let mut layers = Vec::new();
    let mut cur = array.clone();
    while let Some(list) = cur.as_opt::<List>() {
        let offsets_ptype = PType::try_from(list.offsets().dtype())?;
        let use_large = matches!(offsets_ptype, PType::I64 | PType::U64);
        let elem = list.elements().clone();
        layers.push((use_large, list.dtype().is_nullable()));
        cur = elem;
    }

    let leaf_name = if layers.is_empty() {
        name
    } else {
        Field::LIST_FIELD_DEFAULT_NAME
    };
    let mut field = if let Some(varbin) = cur.as_opt::<VarBin>() {
        let offsets_ptype = PType::try_from(varbin.offsets().dtype())?;
        let use_large = matches!(offsets_ptype, PType::I64 | PType::U64);
        let data_type = match (varbin.dtype(), use_large) {
            (DType::Utf8(_), false) => DataType::Utf8,
            (DType::Utf8(_), true) => DataType::LargeUtf8,
            (DType::Binary(_), false) => DataType::Binary,
            (DType::Binary(_), true) => DataType::LargeBinary,
            _ => unreachable!("VarBinArray must have Utf8 or Binary dtype"),
        };
        Field::new(leaf_name, data_type, cur.dtype().is_nullable())
    } else {
        ctx.session()
            .clone()
            .arrow()
            .to_arrow_field(leaf_name, cur.dtype())?
    };

    for (idx, (use_large, nullable)) in layers.into_iter().enumerate().rev() {
        let wrapper_name = if idx == 0 {
            name
        } else {
            Field::LIST_FIELD_DEFAULT_NAME
        };
        let elem = FieldRef::new(field);
        let data_type = if use_large {
            DataType::LargeList(elem)
        } else {
            DataType::List(elem)
        };
        field = Field::new(wrapper_name, data_type, nullable);
    }

    Ok(field)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::LazyLock;

    use arrow_array::Array;
    use arrow_array::ArrayRef as ArrowArrayRef;
    use arrow_array::BinaryArray;
    use arrow_array::Date32Array;
    use arrow_array::Float64Array;
    use arrow_array::Int32Array;
    use arrow_array::Int64Array;
    use arrow_array::LargeStringArray;
    use arrow_array::TimestampMicrosecondArray;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::TimeUnit;
    use rstest::rstest;
    use vortex_array::ArrayRef;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::BoolArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::VarBinViewArray;
    use vortex_session::VortexSession;

    use crate::ArrowSessionExt;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::empty);

    fn utf8() -> ArrayRef {
        VarBinViewArray::from_iter_str(["a", "bb"]).into_array()
    }

    fn primitive() -> ArrayRef {
        PrimitiveArray::from_iter([1i32, 2]).into_array()
    }

    fn boolean() -> ArrayRef {
        BoolArray::from_iter([true, false]).into_array()
    }

    fn list_target() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
    }

    fn map_target() -> DataType {
        let entries = Field::new_struct(
            "entries",
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int32, true),
            ],
            false,
        );
        DataType::Map(Arc::new(entries), false)
    }

    /// Must error rather than panic inside `execute::<T>`.
    #[rstest]
    #[case::bool_from_utf8(utf8(), DataType::Boolean)]
    #[case::bool_from_primitive(primitive(), DataType::Boolean)]
    #[case::null_from_utf8(utf8(), DataType::Null)]
    #[case::null_from_primitive(primitive(), DataType::Null)]
    #[case::decimal_from_utf8(utf8(), DataType::Decimal128(10, 2))]
    #[case::decimal_from_primitive(primitive(), DataType::Decimal128(10, 2))]
    #[case::list_from_utf8(utf8(), list_target())]
    #[case::list_from_primitive(primitive(), list_target())]
    #[case::list_view_from_primitive(
        primitive(),
        DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true)))
    )]
    #[case::fixed_size_list_from_utf8(
        utf8(),
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 2)
    )]
    #[case::map_from_primitive(primitive(), map_target())]
    #[case::byte_from_primitive(primitive(), DataType::Utf8)]
    #[case::byte_from_bool(boolean(), DataType::Binary)]
    fn incompatible_target_returns_error(#[case] array: ArrayRef, #[case] target: DataType) {
        let mut ctx = SESSION.create_execution_ctx();
        let field = Field::new("f", target.clone(), array.dtype().is_nullable());

        let result = SESSION.arrow().execute_arrow(array, Some(&field), &mut ctx);

        assert!(
            result.is_err(),
            "expected an error exporting to {target:?}, got Ok"
        );
    }

    /// Cross-class conversions that are genuinely supported must keep working.
    #[rstest]
    #[case::bool_to_int32(boolean(), DataType::Int32, Arc::new(Int32Array::from(vec![1, 0])))]
    #[case::bool_to_float64(boolean(), DataType::Float64, Arc::new(Float64Array::from(vec![1., 0.])))]
    #[case::primitive_to_int64(primitive(), DataType::Int64, Arc::new(Int64Array::from(vec![1, 2])))]
    #[case::primitive_to_date32(primitive(), DataType::Date32, Arc::new(Date32Array::from(vec![1, 2])))]
    #[case::primitive_to_timestamp(primitive(), DataType::Timestamp(TimeUnit::Microsecond, None), Arc::new(TimestampMicrosecondArray::from(vec![1, 2])))]
    #[case::utf8_to_binary(utf8(), DataType::Binary, Arc::new(BinaryArray::from(vec![b"a".as_slice(), b"bb".as_slice()])))]
    #[case::utf8_to_large_utf8(utf8(), DataType::LargeUtf8, Arc::new(LargeStringArray::from(vec!["a", "bb"])))]
    fn supported_cross_class_target_still_works(
        #[case] array: ArrayRef,
        #[case] target: DataType,
        #[case] expected: ArrowArrayRef,
    ) {
        let mut ctx = SESSION.create_execution_ctx();
        let field = Field::new("f", target, array.dtype().is_nullable());

        let result = SESSION.arrow().execute_arrow(array, Some(&field), &mut ctx);

        assert_eq!(result.unwrap().to_data(), expected.to_data());
    }

    mod deep_tests {
        use std::thread;

        use arrow_array::Array as ArrowArray;
        use arrow_array::ArrayRef as ArrowArrayRef;
        use vortex_array::ArrayRef;
        use vortex_array::IntoArray;
        use vortex_array::VortexSessionExecute;
        use vortex_array::arrays::ListArray;
        use vortex_array::arrays::PrimitiveArray;
        use vortex_array::arrays::VarBinArray;
        use vortex_array::dtype::DType;
        use vortex_array::dtype::Nullability;
        use vortex_array::validity::Validity;
        use vortex_error::VortexResult;
        use vortex_error::vortex_err;

        use super::SESSION;
        use crate::ArrowSessionExt;
        use crate::executor::infer_nearest_arrow_field;

        #[test]
        fn deep_infer_and_execute_list_varbin_on_small_stack() -> VortexResult<()> {
            let depth = 2048;
            let mut build_ctx = SESSION.create_execution_ctx();
            let mut array: ArrayRef = VarBinArray::from_vec(
                vec!["alpha", "beta"],
                DType::Utf8(Nullability::NonNullable),
            )
            .into_array();
            for _ in 0..depth {
                let offsets = PrimitiveArray::from_iter([0i32, 1, 2]).into_array();
                array = ListArray::try_new(array, offsets, Validity::NonNullable, &mut build_ctx)?
                    .into_array();
            }

            let handle = thread::Builder::new()
                .stack_size(128 * 1024)
                .spawn(move || -> VortexResult<ArrowArrayRef> {
                    let mut ctx = SESSION.create_execution_ctx();
                    let inferred = infer_nearest_arrow_field(&array, "root", &mut ctx)?;
                    assert_eq!(inferred.name(), "root");
                    let arrow = SESSION.arrow().execute_arrow(array, None, &mut ctx)?;
                    assert_eq!(arrow.len(), 2);
                    Ok(arrow)
                })
                .map_err(|err| vortex_err!("spawn 128 KiB thread: {err}"))?;

            let arrow = handle
                .join()
                .map_err(|err| vortex_err!("128 KiB thread panicked: {err:?}"))??;
            assert_eq!(arrow.len(), 2);
            Ok(())
        }
    }
}
