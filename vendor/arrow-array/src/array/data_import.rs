// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered ArrayData -> built-in Array reconstruction. Preserves the original
//! constructor checks, panic/error order, buffer slicing, and metadata while
//! keeping arbitrary nesting off the call stack.

use std::sync::Arc;

use arrow_buffer::{Buffer, NullBuffer, RunEndBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit, UnionMode};

use super::*;

#[cfg(test)]
#[path = "data_import_tests.rs"]
pub(super) mod tests;

#[derive(Clone, Copy)]
enum StructMode {
    WrapArc,
    ConcreteStruct,
    MapEntries,
}

#[derive(Clone, Copy)]
enum DictKeyKind {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

impl DictKeyKind {
    fn from_data_type(key_type: &DataType) -> Self {
        match key_type {
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            dt => unimplemented!("Unexpected dictionary key type {dt}"),
        }
    }

    fn expected_data_type(self) -> DataType {
        match self {
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
        }
    }

    fn finish(
        self,
        data_type: DataType,
        len: usize,
        nulls: Option<NullBuffer>,
        offset: usize,
        buffer: Buffer,
        values: ArrayRef,
    ) -> ArrayRef {
        macro_rules! build {
            ($($ty:ty => $variant:ident),+ $(,)?) => {
                match self {
                    $(Self::$variant => Arc::new(DictionaryArray::<$ty>::from_parts(
                        data_type,
                        len,
                        nulls,
                        offset,
                        buffer,
                        values.into(),
                    )),)+
                }
            };
        }
        build!(
            Int8Type => Int8,
            Int16Type => Int16,
            Int32Type => Int32,
            Int64Type => Int64,
            UInt8Type => UInt8,
            UInt16Type => UInt16,
            UInt32Type => UInt32,
            UInt64Type => UInt64,
        )
    }
}

#[derive(Clone, Copy)]
enum RunKeyKind {
    Int16,
    Int32,
    Int64,
}

impl RunKeyKind {
    fn from_data_type(run_ends_type: &DataType) -> Self {
        match run_ends_type {
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            dt => unimplemented!("Unexpected data type for run_ends array {dt}"),
        }
    }
}

enum RunEndsHeader {
    Int16 {
        data_type: DataType,
        run_ends: RunEndBuffer<i16>,
    },
    Int32 {
        data_type: DataType,
        run_ends: RunEndBuffer<i32>,
    },
    Int64 {
        data_type: DataType,
        run_ends: RunEndBuffer<i64>,
    },
}

impl RunEndsHeader {
    fn parse(kind: RunKeyKind, data: ArrayData) -> (Self, ArrayData) {
        match kind {
            RunKeyKind::Int16 => {
                let (data_type, run_ends, values) = RunArray::<Int16Type>::parse_array_data(data);
                (Self::Int16 { data_type, run_ends }, values)
            }
            RunKeyKind::Int32 => {
                let (data_type, run_ends, values) = RunArray::<Int32Type>::parse_array_data(data);
                (Self::Int32 { data_type, run_ends }, values)
            }
            RunKeyKind::Int64 => {
                let (data_type, run_ends, values) = RunArray::<Int64Type>::parse_array_data(data);
                (Self::Int64 { data_type, run_ends }, values)
            }
        }
    }

    fn finish(self, values: ArrayRef) -> Built {
        match self {
            Self::Int16 {
                data_type,
                run_ends,
            } => Built::Array(Arc::new(RunArray::<Int16Type>::from_parts(
                data_type,
                run_ends,
                values.into(),
            ))),
            Self::Int32 {
                data_type,
                run_ends,
            } => Built::Array(Arc::new(RunArray::<Int32Type>::from_parts(
                data_type,
                run_ends,
                values.into(),
            ))),
            Self::Int64 {
                data_type,
                run_ends,
            } => Built::Array(Arc::new(RunArray::<Int64Type>::from_parts(
                data_type,
                run_ends,
                values.into(),
            ))),
        }
    }
}

#[derive(Clone, Copy)]
enum RootKind {
    Array,
    Struct,
    FixedSizeList,
    Map,
    Union,
    Dictionary(DictKeyKind),
}

enum Built {
    Array(ArrayRef),
    Struct(StructArray),
    FixedSizeList(FixedSizeListArray),
    Map(MapArray),
    Union(UnionArray),
    DictParts {
        data_type: DataType,
        len: usize,
        nulls: Option<NullBuffer>,
        offset: usize,
        buffer: Buffer,
        values: ArrayRef,
    },
}

impl Built {
    fn into_array_ref(self) -> ArrayRef {
        match self {
            Self::Array(array) => array,
            Self::Struct(array) => Arc::new(array),
            Self::FixedSizeList(array) => Arc::new(array),
            Self::Map(array) => Arc::new(array),
            Self::Union(array) => Arc::new(array),
            Self::DictParts { .. } => unreachable!("dictionary parts are only returned at root"),
        }
    }
}

enum ChildTarget {
    Array,
    MapEntries,
}

enum Frame {
    Struct {
        data_type: DataType,
        len: usize,
        nulls: Option<NullBuffer>,
        offset: usize,
        remaining: std::vec::IntoIter<ArrayData>,
        fields: Vec<ArrayRef>,
        mode: StructMode,
    },
    List {
        large: bool,
        data_type: DataType,
        len: usize,
        nulls: Option<NullBuffer>,
        offset: usize,
        buffer: Buffer,
    },
    ListView {
        large: bool,
        data_type: DataType,
        len: usize,
        nulls: Option<NullBuffer>,
        offset: usize,
        offsets_buffer: Buffer,
        sizes_buffer: Buffer,
    },
    FixedSizeList {
        data_type: DataType,
        nulls: Option<NullBuffer>,
        value_length: i32,
        len: usize,
        wrap_arc: bool,
    },
    Map {
        data_type: DataType,
        len: usize,
        nulls: Option<NullBuffer>,
        offset: usize,
        buffer: Buffer,
        wrap_arc: bool,
    },
    Union {
        data_type: DataType,
        type_ids: ScalarBuffer<i8>,
        offsets: Option<ScalarBuffer<i32>>,
        mode: UnionMode,
        parent_offset: usize,
        parent_len: usize,
        current_id: i8,
        remaining: std::vec::IntoIter<(i8, ArrayData)>,
        fields: Vec<Option<ArrayRef>>,
        wrap_arc: bool,
    },
    Dictionary {
        data_type: DataType,
        len: usize,
        nulls: Option<NullBuffer>,
        offset: usize,
        buffer: Buffer,
        key_kind: Option<DictKeyKind>,
    },
    Run {
        header: RunEndsHeader,
    },
}

enum Step {
    Child(Frame, ArrayData, ChildTarget),
    Done(Built),
}

// Release pending frames from innermost to outermost so unwinding on partial
// or malformed nested descriptors never consumes call stack.
struct Pending(Vec<Frame>);

impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

/// Constructs an [`ArrayRef`] from an [`ArrayData`].
///
/// # Notes:
///
/// It is more efficient to directly construct the concrete array type rather
/// than using this function as creating an `ArrayData` requires at least one
/// additional allocation (the Vec of buffers).
pub fn make_array(data: ArrayData) -> ArrayRef {
    build(data, RootKind::Array)
        .expect("make_array failure should panic before returning")
        .into_array_ref()
}

pub(super) fn struct_from_data(data: ArrayData) -> StructArray {
    match build(data, RootKind::Struct).expect("StructArray creation failed") {
        Built::Struct(array) => array,
        _ => unreachable!(),
    }
}

pub(super) fn fixed_size_list_from_data(data: ArrayData) -> FixedSizeListArray {
    match build(data, RootKind::FixedSizeList).expect("FixedSizeListArray creation failed") {
        Built::FixedSizeList(array) => array,
        _ => unreachable!(),
    }
}

pub(super) fn map_from_data(data: ArrayData) -> Result<MapArray, ArrowError> {
    match build(data, RootKind::Map)? {
        Built::Map(array) => Ok(array),
        _ => unreachable!(),
    }
}

pub(super) fn union_from_data(data: ArrayData) -> UnionArray {
    match build(data, RootKind::Union).expect("UnionArray creation failed") {
        Built::Union(array) => array,
        _ => unreachable!(),
    }
}

pub(super) fn dictionary_from_data<K: ArrowDictionaryKeyType>(
    data: ArrayData,
) -> DictionaryArray<K> {
    let key_kind = DictKeyKind::from_data_type(&K::DATA_TYPE);
    match build(data, RootKind::Dictionary(key_kind)).expect("DictionaryArray creation failed") {
        Built::DictParts {
            data_type,
            len,
            nulls,
            offset,
            buffer,
            values,
        } => DictionaryArray::from_parts(data_type, len, nulls, offset, buffer, values.into()),
        _ => unreachable!(),
    }
}

fn build(data: ArrayData, kind: RootKind) -> Result<Built, ArrowError> {
    let mut pending = Pending(Vec::new());
    let mut step = enter(data, kind)?;
    loop {
        match step {
            Step::Child(frame, child, target) => {
                pending.0.push(frame);
                step = match target {
                    ChildTarget::Array => enter(child, RootKind::Array)?,
                    ChildTarget::MapEntries => enter_struct(child, StructMode::MapEntries),
                };
            }
            Step::Done(mut built) => loop {
                let Some(frame) = pending.0.pop() else {
                    return Ok(built);
                };
                match resume(frame, built) {
                    Step::Child(next_frame, child, target) => {
                        pending.0.push(next_frame);
                        step = match target {
                            ChildTarget::Array => enter(child, RootKind::Array)?,
                            ChildTarget::MapEntries => enter_struct(child, StructMode::MapEntries),
                        };
                        break;
                    }
                    Step::Done(finished) => {
                        built = finished;
                    }
                }
            },
        }
    }
}

fn enter(data: ArrayData, kind: RootKind) -> Result<Step, ArrowError> {
    match kind {
        RootKind::Array => Ok(enter_by_data_type(data)),
        RootKind::Struct => Ok(enter_struct(data, StructMode::ConcreteStruct)),
        RootKind::FixedSizeList => Ok(enter_fixed_size_list(data, false)),
        RootKind::Map => enter_map(data, true),
        RootKind::Union => Ok(enter_union(data, false)),
        RootKind::Dictionary(key_kind) => Ok(enter_dictionary(data, key_kind, false)),
    }
}

fn enter_by_data_type(data: ArrayData) -> Step {
    let array = match data.data_type() {
        DataType::Boolean => Arc::new(BooleanArray::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(Int8Array::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(Int16Array::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(Int64Array::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(UInt8Array::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(UInt32Array::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(UInt64Array::from(data)) as ArrayRef,
        DataType::Float16 => Arc::new(Float16Array::from(data)) as ArrayRef,
        DataType::Float32 => Arc::new(Float32Array::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(data)) as ArrayRef,
        DataType::Date32 => Arc::new(Date32Array::from(data)) as ArrayRef,
        DataType::Date64 => Arc::new(Date64Array::from(data)) as ArrayRef,
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(data)) as ArrayRef,
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Arc::new(TimestampSecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Arc::new(TimestampMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Arc::new(TimestampNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(IntervalYearMonthArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Arc::new(IntervalDayTimeArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Arc::new(IntervalMonthDayNanoArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Second) => {
            Arc::new(DurationSecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Arc::new(DurationNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Binary => Arc::new(BinaryArray::from(data)) as ArrayRef,
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(data)) as ArrayRef,
        DataType::FixedSizeBinary(_) => Arc::new(FixedSizeBinaryArray::from(data)) as ArrayRef,
        DataType::BinaryView => Arc::new(BinaryViewArray::from(data)) as ArrayRef,
        DataType::Utf8 => Arc::new(StringArray::from(data)) as ArrayRef,
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data)) as ArrayRef,
        DataType::Utf8View => Arc::new(StringViewArray::from(data)) as ArrayRef,
        DataType::List(_) => {
            return enter_list::<i32>(data, false)
                .expect("Expected infallible creation of GenericListArray from ArrayDataRef failed");
        }
        DataType::LargeList(_) => {
            return enter_list::<i64>(data, true)
                .expect("Expected infallible creation of GenericListArray from ArrayDataRef failed");
        }
        DataType::ListView(_) => {
            return enter_list_view::<i32>(data, false)
                .expect("Expected infallible creation of GenericListViewArray from ArrayDataRef failed");
        }
        DataType::LargeListView(_) => {
            return enter_list_view::<i64>(data, true)
                .expect("Expected infallible creation of GenericListViewArray from ArrayDataRef failed");
        }
        DataType::Struct(_) => return enter_struct(data, StructMode::WrapArc),
        DataType::Map(_, _) => {
            return enter_map(data, false)
                .expect("Expected infallible creation of MapArray from ArrayData failed");
        }
        DataType::Union(_, _) => return enter_union(data, true),
        DataType::FixedSizeList(_, _) => return enter_fixed_size_list(data, true),
        DataType::Dictionary(key_type, _) => {
            let key_kind = DictKeyKind::from_data_type(key_type.as_ref());
            return enter_dictionary(data, key_kind, true);
        }
        DataType::RunEndEncoded(run_ends_type, _) => {
            let run_kind = RunKeyKind::from_data_type(run_ends_type.data_type());
            return enter_run(run_kind, data);
        }
        DataType::Null => Arc::new(NullArray::from(data)) as ArrayRef,
        DataType::Decimal32(_, _) => Arc::new(Decimal32Array::from(data)) as ArrayRef,
        DataType::Decimal64(_, _) => Arc::new(Decimal64Array::from(data)) as ArrayRef,
        DataType::Decimal128(_, _) => Arc::new(Decimal128Array::from(data)) as ArrayRef,
        DataType::Decimal256(_, _) => Arc::new(Decimal256Array::from(data)) as ArrayRef,
        dt => unimplemented!("Unexpected data type {dt}"),
    };
    Step::Done(Built::Array(array))
}

fn finish_struct(
    data_type: DataType,
    len: usize,
    nulls: Option<NullBuffer>,
    fields: Vec<ArrayRef>,
    mode: StructMode,
) -> Built {
    let array = StructArray::from_parts(len, data_type, nulls, fields.into());
    match mode {
        StructMode::WrapArc => Built::Array(Arc::new(array)),
        StructMode::ConcreteStruct | StructMode::MapEntries => Built::Struct(array),
    }
}

fn enter_struct(data: ArrayData, mode: StructMode) -> Step {
    let (data_type, len, nulls, offset, _buffers, child_data) = data.into_parts();
    let count = child_data.len();
    let mut remaining = child_data.into_iter();
    let Some(cd) = remaining.next() else {
        return Step::Done(finish_struct(data_type, len, nulls, Vec::new(), mode));
    };
    let cd = if offset != 0 || len != cd.len() {
        cd.slice(offset, len)
    } else {
        cd
    };
    Step::Child(
        Frame::Struct {
            data_type,
            len,
            nulls,
            offset,
            remaining,
            fields: Vec::with_capacity(count),
            mode,
        },
        cd,
        ChildTarget::Array,
    )
}

fn enter_list<O: OffsetSizeTrait>(data: ArrayData, large: bool) -> Result<Step, ArrowError> {
    let (data_type, len, nulls, offset, buffer, values) =
        GenericListArray::<O>::parse_array_data(data)?;
    Ok(Step::Child(
        Frame::List {
            large,
            data_type,
            len,
            nulls,
            offset,
            buffer,
        },
        values,
        ChildTarget::Array,
    ))
}

fn enter_list_view<O: OffsetSizeTrait>(data: ArrayData, large: bool) -> Result<Step, ArrowError> {
    let (data_type, len, nulls, offset, offsets_buffer, sizes_buffer, values) =
        GenericListViewArray::<O>::parse_array_data(data)?;
    Ok(Step::Child(
        Frame::ListView {
            large,
            data_type,
            len,
            nulls,
            offset,
            offsets_buffer,
            sizes_buffer,
        },
        values,
        ChildTarget::Array,
    ))
}

fn enter_fixed_size_list(data: ArrayData, wrap_arc: bool) -> Step {
    let (data_type, len, nulls, offset, _buffers, child_data) = data.into_parts();
    let value_length = match data_type {
        DataType::FixedSizeList(_, len) => len,
        data_type => {
            panic!(
                "FixedSizeListArray data should contain a FixedSizeList data type, got {data_type}"
            )
        }
    };
    let size = value_length as usize;
    let child = child_data[0].slice(offset * size, len * size);
    Step::Child(
        Frame::FixedSizeList {
            data_type,
            nulls,
            value_length,
            len,
            wrap_arc,
        },
        child,
        ChildTarget::Array,
    )
}

fn enter_map(data: ArrayData, concrete: bool) -> Result<Step, ArrowError> {
    let (data_type, len, nulls, offset, buffer, entries) = MapArray::parse_array_data(data)?;
    Ok(Step::Child(
        Frame::Map {
            data_type,
            len,
            nulls,
            offset,
            buffer,
            wrap_arc: !concrete,
        },
        entries,
        ChildTarget::MapEntries,
    ))
}

fn enter_union(data: ArrayData, wrap_arc: bool) -> Step {
    let (data_type, len, _nulls, offset, buffers, child_data) = data.into_parts();
    let (fields, mode) = match &data_type {
        DataType::Union(fields, mode) => (fields, *mode),
        d => panic!("UnionArray expected ArrayData with type Union got {d}"),
    };
    let (type_ids, offsets) = match mode {
        UnionMode::Sparse => {
            let [buffer]: [Buffer; 1] = buffers.try_into().expect("1 buffer for type_ids");
            (ScalarBuffer::new(buffer, offset, len), None)
        }
        UnionMode::Dense => {
            let [type_ids_buffer, offsets_buffer]: [Buffer; 2] = buffers
                .try_into()
                .expect("2 buffers for type_ids and offsets");
            (
                ScalarBuffer::new(type_ids_buffer, offset, len),
                Some(ScalarBuffer::new(offsets_buffer, offset, len)),
            )
        }
    };
    let max_id = fields.iter().map(|(i, _)| i).max().unwrap_or_default() as usize;
    let boxed_fields = vec![None; max_id + 1];
    let mut remaining = child_data
        .into_iter()
        .zip(fields.iter())
        .map(|(cd, (field_id, _))| (field_id, cd))
        .collect::<Vec<_>>()
        .into_iter();
    let Some((current_id, cd)) = remaining.next() else {
        let array = UnionArray::from_parts(data_type, type_ids, offsets, boxed_fields.into());
        return Step::Done(if wrap_arc {
            Built::Array(Arc::new(array))
        } else {
            Built::Union(array)
        });
    };
    let cd = match mode {
        UnionMode::Sparse => cd.slice(offset, len),
        UnionMode::Dense => cd,
    };
    Step::Child(
        Frame::Union {
            data_type,
            type_ids,
            offsets,
            mode,
            parent_offset: offset,
            parent_len: len,
            current_id,
            remaining,
            fields: boxed_fields,
            wrap_arc,
        },
        cd,
        ChildTarget::Array,
    )
}

fn enter_dictionary(data: ArrayData, key_kind: DictKeyKind, wrap_arc: bool) -> Step {
    let (data_type, len, nulls, offset, mut buffers, mut child_data) = data.into_parts();
    assert_eq!(
        buffers.len(),
        1,
        "DictionaryArray data should contain a single buffer only (keys)."
    );
    let buffer = buffers.pop().expect("checked above");
    assert_eq!(
        child_data.len(),
        1,
        "DictionaryArray should contain a single child array (values)."
    );
    let cd = child_data.pop().expect("checked above");
    let expected_key = key_kind.expected_data_type();
    if let DataType::Dictionary(key_data_type, _) = &data_type {
        assert_eq!(
            &expected_key,
            key_data_type.as_ref(),
            "DictionaryArray's data type must match, expected {} got {}",
            expected_key,
            key_data_type
        );
        Step::Child(
            Frame::Dictionary {
                data_type,
                len,
                nulls,
                offset,
                buffer,
                key_kind: wrap_arc.then_some(key_kind),
            },
            cd,
            ChildTarget::Array,
        )
    } else {
        panic!("DictionaryArray must have Dictionary data type.")
    }
}

fn enter_run(run_kind: RunKeyKind, data: ArrayData) -> Step {
    let (header, values) = RunEndsHeader::parse(run_kind, data);
    Step::Child(Frame::Run { header }, values, ChildTarget::Array)
}

fn resume(frame: Frame, built: Built) -> Step {
    match frame {
        Frame::Struct {
            data_type,
            len,
            nulls,
            offset,
            mut remaining,
            mut fields,
            mode,
        } => {
            fields.push(built.into_array_ref());
            if let Some(cd) = remaining.next() {
                let cd = if offset != 0 || len != cd.len() {
                    cd.slice(offset, len)
                } else {
                    cd
                };
                return Step::Child(
                    Frame::Struct {
                        data_type,
                        len,
                        nulls,
                        offset,
                        remaining,
                        fields,
                        mode,
                    },
                    cd,
                    ChildTarget::Array,
                );
            }
            Step::Done(finish_struct(data_type, len, nulls, fields, mode))
        }
        Frame::List {
            large,
            data_type,
            len,
            nulls,
            offset,
            buffer,
        } => {
            let child = built.into_array_ref();
            Step::Done(if large {
                // SAFETY: `parse_array_data` verified `ArrayData` type and buffer layout.
                let offsets = unsafe { get_offsets_from_buffer::<i64>(buffer, offset, len) };
                Built::Array(Arc::new(LargeListArray::from_parts(
                    data_type,
                    nulls,
                    child.into(),
                    offsets,
                )))
            } else {
                // SAFETY: `parse_array_data` verified `ArrayData` type and buffer layout.
                let offsets = unsafe { get_offsets_from_buffer::<i32>(buffer, offset, len) };
                Built::Array(Arc::new(ListArray::from_parts(
                    data_type,
                    nulls,
                    child.into(),
                    offsets,
                )))
            })
        }
        Frame::ListView {
            large,
            data_type,
            len,
            nulls,
            offset,
            offsets_buffer,
            sizes_buffer,
        } => {
            let child = built.into_array_ref();
            Step::Done(if large {
                let value_offsets = ScalarBuffer::new(offsets_buffer, offset, len);
                let value_sizes = ScalarBuffer::new(sizes_buffer, offset, len);
                Built::Array(Arc::new(LargeListViewArray::from_parts(
                    data_type,
                    nulls,
                    child.into(),
                    value_offsets,
                    value_sizes,
                )))
            } else {
                let value_offsets = ScalarBuffer::new(offsets_buffer, offset, len);
                let value_sizes = ScalarBuffer::new(sizes_buffer, offset, len);
                Built::Array(Arc::new(ListViewArray::from_parts(
                    data_type,
                    nulls,
                    child.into(),
                    value_offsets,
                    value_sizes,
                )))
            })
        }
        Frame::FixedSizeList {
            data_type,
            nulls,
            value_length,
            len,
            wrap_arc,
        } => {
            let child = built.into_array_ref();
            let array =
                FixedSizeListArray::from_parts(data_type, child.into(), nulls, value_length, len);
            Step::Done(if wrap_arc {
                Built::Array(Arc::new(array))
            } else {
                Built::FixedSizeList(array)
            })
        }
        Frame::Map {
            data_type,
            len,
            nulls,
            offset,
            buffer,
            wrap_arc,
        } => {
            let Built::Struct(entries) = built else {
                unreachable!("Map entries must reconstruct as StructArray")
            };
            // SAFETY: `MapArray::parse_array_data` verified `ArrayData` type and layout.
            let offsets = unsafe { get_offsets_from_buffer::<i32>(buffer, offset, len) };
            let array = MapArray::from_parts(data_type, nulls, entries, offsets);
            Step::Done(if wrap_arc {
                Built::Array(Arc::new(array))
            } else {
                Built::Map(array)
            })
        }
        Frame::Union {
            data_type,
            type_ids,
            offsets,
            mode,
            parent_offset,
            parent_len,
            current_id,
            mut remaining,
            mut fields,
            wrap_arc,
        } => {
            fields[current_id as usize] = Some(built.into_array_ref());
            if let Some((next_id, cd)) = remaining.next() {
                let cd = match mode {
                    UnionMode::Sparse => cd.slice(parent_offset, parent_len),
                    UnionMode::Dense => cd,
                };
                return Step::Child(
                    Frame::Union {
                        data_type,
                        type_ids,
                        offsets,
                        mode,
                        parent_offset,
                        parent_len,
                        current_id: next_id,
                        remaining,
                        fields,
                        wrap_arc,
                    },
                    cd,
                    ChildTarget::Array,
                );
            }
            let array = UnionArray::from_parts(data_type, type_ids, offsets, fields.into());
            Step::Done(if wrap_arc {
                Built::Array(Arc::new(array))
            } else {
                Built::Union(array)
            })
        }
        Frame::Dictionary {
            data_type,
            len,
            nulls,
            offset,
            buffer,
            key_kind,
        } => {
            let values = built.into_array_ref();
            Step::Done(match key_kind {
                Some(kind) => {
                    Built::Array(kind.finish(data_type, len, nulls, offset, buffer, values))
                }
                None => Built::DictParts {
                    data_type,
                    len,
                    nulls,
                    offset,
                    buffer,
                    values,
                },
            })
        }
        Frame::Run { header } => Step::Done(header.finish(built.into_array_ref())),
    }
}
