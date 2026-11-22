// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Low-level array data abstractions.
//!
//! Provides utilities for creating, manipulating, and converting Arrow arrays
//! made of primitive types, strings, and nested types.

use super::{ArrayData, ArrayDataBuilder, ByteView, data::new_buffers};
use crate::bit_mask::set_bits;
use arrow_buffer::buffer::{BooleanBuffer, NullBuffer};
use arrow_buffer::{ArrowNativeType, Buffer, IntervalMonthDayNano, MutableBuffer, bit_util, i256};
use arrow_schema::{ArrowError, DataType, IntervalUnit, UnionMode};
use half::f16;
use num_integer::Integer;
use std::mem;

mod boolean;
mod fixed_binary;
mod fixed_size_list;
mod list;
mod list_view;
mod null;
mod primitive;
mod run;
mod structure;
mod union;
mod utils;
mod variable_size;

pub(super) struct ChildExtendTask {
    pub(super) child_idx: usize,
    pub(super) array_idx: usize,
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) wrap_struct_col: bool,
}

pub(super) struct ChildExtendNullsTask {
    pub(super) child_idx: usize,
    pub(super) len: usize,
    pub(super) wrap_struct_col: bool,
}

type ExtendNullBits<'a> = Box<dyn Fn(&mut _MutableArrayData, usize, usize) + 'a>;
// function that extends `[start..start+len]` to the mutable array.
// this is dynamic because different data_types influence how buffers and children are extended.
type Extend<'a> = Box<
    dyn Fn(&mut _MutableArrayData, usize, usize, usize, &mut Vec<ChildExtendTask>) -> Result<(), ArrowError>
        + 'a,
>;

type ExtendNulls = Box<
    dyn Fn(&mut _MutableArrayData, usize, &mut Vec<ChildExtendNullsTask>) -> Result<(), ArrowError>,
>;

/// A mutable [ArrayData] that knows how to freeze itself into an [ArrayData].
/// This is just a data container.
#[derive(Debug)]
struct _MutableArrayData<'a> {
    pub data_type: DataType,
    pub null_count: usize,

    pub len: usize,
    pub null_buffer: Option<MutableBuffer>,

    // arrow specification only allows up to 3 buffers (2 ignoring the nulls above).
    // Thus, we place them in the stack to avoid bound checks and greater data locality.
    pub buffer1: MutableBuffer,
    pub buffer2: MutableBuffer,
    pub child_data: Vec<MutableArrayData<'a>>,
}

impl _MutableArrayData<'_> {
    fn empty() -> Self {
        Self {
            data_type: DataType::Null,
            null_count: 0,
            len: 0,
            null_buffer: None,
            buffer1: MutableBuffer::new(0),
            buffer2: MutableBuffer::new(0),
            child_data: Vec::new(),
        }
    }

    fn null_buffer(&mut self) -> &mut MutableBuffer {
        self.null_buffer
            .as_mut()
            .expect("MutableArrayData not nullable")
    }
}

fn build_extend_null_bits(array: &ArrayData, use_nulls: bool) -> ExtendNullBits<'_> {
    if let Some(nulls) = array.nulls() {
        let bytes = nulls.validity();
        Box::new(move |mutable, start, len| {
            let mutable_len = mutable.len;
            let out = mutable.null_buffer();
            utils::resize_for_bits(out, mutable_len + len);
            mutable.null_count += set_bits(
                out.as_slice_mut(),
                bytes,
                mutable_len,
                nulls.offset() + start,
                len,
            );
        })
    } else if use_nulls {
        Box::new(|mutable, _, len| {
            let mutable_len = mutable.len;
            let out = mutable.null_buffer();
            utils::resize_for_bits(out, mutable_len + len);
            let write_data = out.as_slice_mut();
            (0..len).for_each(|i| {
                bit_util::set_bit(write_data, mutable_len + i);
            });
        })
    } else {
        Box::new(|_, _, _| {})
    }
}

/// Efficiently create an [ArrayData] from one or more existing [ArrayData]s by
/// copying chunks.
///
/// The main use case of this struct is to perform unary operations to arrays of
/// arbitrary types, such as `filter` and `take`.
///
/// # Example
/// ```
/// use arrow_buffer::Buffer;
/// use arrow_data::ArrayData;
/// use arrow_data::transform::MutableArrayData;
/// use arrow_schema::DataType;
/// fn i32_array(values: &[i32]) -> ArrayData {
///   ArrayData::try_new(DataType::Int32, values.len(), None, 0, vec![Buffer::from_slice_ref(values)], vec![]).unwrap()
/// }
/// let arr1  = i32_array(&[1, 2, 3, 4, 5]);
/// let arr2  = i32_array(&[6, 7, 8, 9, 10]);
/// // Create a mutable array for copying values from arr1 and arr2, with a capacity for 6 elements
/// let capacity = 3 * std::mem::size_of::<i32>();
/// let mut mutable = MutableArrayData::new(vec![&arr1, &arr2], false, 10);
/// // Copy the first 3 elements from arr1
/// mutable.extend(0, 0, 3);
/// // Copy the last 3 elements from arr2
/// mutable.extend(1, 2, 5);
/// // Complete the MutableArrayData into a new ArrayData
/// let frozen = mutable.freeze();
/// assert_eq!(frozen, i32_array(&[1, 2, 3, 8, 9, 10]));
/// ```
pub struct MutableArrayData<'a> {
    /// Input arrays: the data being read FROM.
    ///
    /// Note this is "dead code" because all actual references to the arrays are
    /// stored in closures for extending values and nulls.
    #[allow(dead_code)]
    arrays: Vec<&'a ArrayData>,

    /// In progress output array: The data being written TO
    ///
    /// Note these fields are in a separate struct, [_MutableArrayData], as they
    /// cannot be in [MutableArrayData] itself due to mutability invariants (interior
    /// mutability): [MutableArrayData] contains a function that can only mutate
    /// [_MutableArrayData], not [MutableArrayData] itself
    data: _MutableArrayData<'a>,

    /// The child data of the `Array` in Dictionary arrays.
    ///
    /// This is not stored in `_MutableArrayData` because these values are
    /// constant and only needed at the end, when freezing [_MutableArrayData].
    dictionary: Option<ArrayData>,

    /// Variadic data buffers referenced by views.
    ///
    /// Note this this is not stored in `_MutableArrayData` because these values
    /// are constant and only needed at the end, when freezing
    /// [_MutableArrayData]
    variadic_data_buffers: Vec<Buffer>,

    /// function used to extend output array with values from input arrays.
    ///
    /// This function's lifetime is bound to the input arrays because it reads
    /// values from them.
    extend_values: Vec<Extend<'a>>,

    /// function used to extend the output array with nulls from input arrays.
    ///
    /// This function's lifetime is bound to the input arrays because it reads
    /// nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,

    /// function used to extend the output array with null elements.
    ///
    /// This function is independent of the arrays and therefore has no lifetime.
    extend_nulls: ExtendNulls,
}

impl std::fmt::Debug for MutableArrayData<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // ignores the closures.
        f.debug_struct("MutableArrayData")
            .field("data", &self.data)
            .finish()
    }
}

/// Builds an extend that adds `offset` to the source primitive
/// Additionally validates that `max` fits into the
/// the underlying primitive returning None if not
fn build_extend_dictionary(array: &ArrayData, offset: usize, max: usize) -> Option<Extend<'_>> {
    macro_rules! validate_and_build {
        ($dt: ty) => {{
            // `max` is the merged dictionary length; the largest key index is
            // `max - 1`, so the key type only needs to hold `max - 1` (e.g. 256
            // values use keys 0..=255, which fit in u8).
            let _: $dt = max.saturating_sub(1).try_into().ok()?;
            let offset: $dt = offset.try_into().ok()?;
            Some(primitive::build_extend_with_offset(array, offset))
        }};
    }
    match array.data_type() {
        DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
            DataType::UInt8 => validate_and_build!(u8),
            DataType::UInt16 => validate_and_build!(u16),
            DataType::UInt32 => validate_and_build!(u32),
            DataType::UInt64 => validate_and_build!(u64),
            DataType::Int8 => validate_and_build!(i8),
            DataType::Int16 => validate_and_build!(i16),
            DataType::Int32 => validate_and_build!(i32),
            DataType::Int64 => validate_and_build!(i64),
            _ => unreachable!(),
        },
        _ => None,
    }
}

/// Builds an extend that adds `buffer_offset` to any buffer indices encountered
fn build_extend_view(array: &ArrayData, buffer_offset: u32) -> Extend<'_> {
    let views = array.buffer::<u128>(0);
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize, _| {
            mutable
                .buffer1
                .extend(views[start..start + len].iter().map(|v| {
                    let len = *v as u32;
                    if len <= 12 {
                        return *v; // Stored inline
                    }
                    let mut view = ByteView::from(*v);
                    view.buffer_index += buffer_offset;
                    view.into()
                }));
            Ok(())
        },
    )
}

fn build_extend(array: &ArrayData) -> Extend<'_> {
    match array.data_type() {
        DataType::Null => null::build_extend(array),
        DataType::Boolean => boolean::build_extend(array),
        DataType::UInt8 => primitive::build_extend::<u8>(array),
        DataType::UInt16 => primitive::build_extend::<u16>(array),
        DataType::UInt32 => primitive::build_extend::<u32>(array),
        DataType::UInt64 => primitive::build_extend::<u64>(array),
        DataType::Int8 => primitive::build_extend::<i8>(array),
        DataType::Int16 => primitive::build_extend::<i16>(array),
        DataType::Int32 => primitive::build_extend::<i32>(array),
        DataType::Int64 => primitive::build_extend::<i64>(array),
        DataType::Float32 => primitive::build_extend::<f32>(array),
        DataType::Float64 => primitive::build_extend::<f64>(array),
        DataType::Date32 | DataType::Time32(_) | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive::build_extend::<i32>(array)
        }
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => primitive::build_extend::<i64>(array),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            primitive::build_extend::<IntervalMonthDayNano>(array)
        }
        DataType::Decimal32(_, _) => primitive::build_extend::<i32>(array),
        DataType::Decimal64(_, _) => primitive::build_extend::<i64>(array),
        DataType::Decimal128(_, _) => primitive::build_extend::<i128>(array),
        DataType::Decimal256(_, _) => primitive::build_extend::<i256>(array),
        DataType::Utf8 | DataType::Binary => variable_size::build_extend::<i32>(array),
        DataType::LargeUtf8 | DataType::LargeBinary => variable_size::build_extend::<i64>(array),
        DataType::BinaryView | DataType::Utf8View => unreachable!("should use build_extend_view"),
        DataType::Map(_, _) | DataType::List(_) => list::build_extend::<i32>(array),
        DataType::LargeList(_) => list::build_extend::<i64>(array),
        DataType::ListView(_) => list_view::build_extend::<i32>(array),
        DataType::LargeListView(_) => list_view::build_extend::<i64>(array),
        DataType::Dictionary(_, _) => unreachable!("should use build_extend_dictionary"),
        DataType::Struct(_) => structure::build_extend(array),
        DataType::FixedSizeBinary(_) => fixed_binary::build_extend(array),
        DataType::Float16 => primitive::build_extend::<f16>(array),
        DataType::FixedSizeList(_, _) => fixed_size_list::build_extend(array),
        DataType::Union(_, mode) => match mode {
            UnionMode::Sparse => union::build_extend_sparse(array),
            UnionMode::Dense => union::build_extend_dense(array),
        },
        DataType::RunEndEncoded(_, _) => run::build_extend(array),
    }
}

fn build_extend_nulls(data_type: &DataType) -> ExtendNulls {
    Box::new(match data_type {
        DataType::Null => null::extend_nulls,
        DataType::Boolean => boolean::extend_nulls,
        DataType::UInt8 => primitive::extend_nulls::<u8>,
        DataType::UInt16 => primitive::extend_nulls::<u16>,
        DataType::UInt32 => primitive::extend_nulls::<u32>,
        DataType::UInt64 => primitive::extend_nulls::<u64>,
        DataType::Int8 => primitive::extend_nulls::<i8>,
        DataType::Int16 => primitive::extend_nulls::<i16>,
        DataType::Int32 => primitive::extend_nulls::<i32>,
        DataType::Int64 => primitive::extend_nulls::<i64>,
        DataType::Float32 => primitive::extend_nulls::<f32>,
        DataType::Float64 => primitive::extend_nulls::<f64>,
        DataType::Date32 | DataType::Time32(_) | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive::extend_nulls::<i32>
        }
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => primitive::extend_nulls::<i64>,
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            primitive::extend_nulls::<IntervalMonthDayNano>
        }
        DataType::Decimal32(_, _) => primitive::extend_nulls::<i32>,
        DataType::Decimal64(_, _) => primitive::extend_nulls::<i64>,
        DataType::Decimal128(_, _) => primitive::extend_nulls::<i128>,
        DataType::Decimal256(_, _) => primitive::extend_nulls::<i256>,
        DataType::Utf8 | DataType::Binary => variable_size::extend_nulls::<i32>,
        DataType::LargeUtf8 | DataType::LargeBinary => variable_size::extend_nulls::<i64>,
        DataType::BinaryView | DataType::Utf8View => primitive::extend_nulls::<u128>,
        DataType::Map(_, _) | DataType::List(_) => list::extend_nulls::<i32>,
        DataType::LargeList(_) => list::extend_nulls::<i64>,
        DataType::ListView(_) => list_view::extend_nulls::<i32>,
        DataType::LargeListView(_) => list_view::extend_nulls::<i64>,
        DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
            DataType::UInt8 => primitive::extend_nulls::<u8>,
            DataType::UInt16 => primitive::extend_nulls::<u16>,
            DataType::UInt32 => primitive::extend_nulls::<u32>,
            DataType::UInt64 => primitive::extend_nulls::<u64>,
            DataType::Int8 => primitive::extend_nulls::<i8>,
            DataType::Int16 => primitive::extend_nulls::<i16>,
            DataType::Int32 => primitive::extend_nulls::<i32>,
            DataType::Int64 => primitive::extend_nulls::<i64>,
            _ => unreachable!(),
        },
        DataType::Struct(_) => structure::extend_nulls,
        DataType::FixedSizeBinary(_) => fixed_binary::extend_nulls,
        DataType::Float16 => primitive::extend_nulls::<f16>,
        DataType::FixedSizeList(_, _) => fixed_size_list::extend_nulls,
        DataType::Union(_, mode) => match mode {
            UnionMode::Sparse => union::extend_nulls_sparse,
            UnionMode::Dense => union::extend_nulls_dense,
        },
        DataType::RunEndEncoded(_, _) => run::extend_nulls,
    })
}

fn preallocate_offset_and_binary_buffer<Offset: ArrowNativeType + Integer>(
    capacity: usize,
    binary_size: usize,
) -> [MutableBuffer; 2] {
    // offsets
    let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<Offset>());
    // safety: `unsafe` code assumes that this buffer is initialized with one element
    buffer.push(Offset::zero());

    [
        buffer,
        MutableBuffer::new(binary_size * mem::size_of::<u8>()),
    ]
}

/// Define capacities to pre-allocate for child data or data buffers.
#[derive(Debug, Clone)]
pub enum Capacities {
    /// Binary, Utf8 and LargeUtf8 data types
    ///
    /// Defines
    /// * the capacity of the array offsets
    /// * the capacity of the binary/ str buffer
    Binary(usize, Option<usize>),
    /// List and LargeList data types
    ///
    /// Defines
    /// * the capacity of the array offsets
    /// * the capacity of the child data
    List(usize, Option<Box<Capacities>>),
    /// Struct type
    ///
    /// Defines
    /// * the capacity of the array
    /// * the capacities of the fields
    Struct(usize, Option<Vec<Capacities>>),
    /// Dictionary type
    ///
    /// Defines
    /// * the capacity of the array/keys
    /// * the capacity of the values
    Dictionary(usize, Option<Box<Capacities>>),
    /// Don't preallocate inner buffers and rely on array growth strategy
    Array(usize),
}

impl<'a> MutableArrayData<'a> {
    /// Returns a new [MutableArrayData] with capacity to `capacity` slots and
    /// specialized to create an [ArrayData] from multiple `arrays`.
    ///
    /// # Arguments
    /// * `arrays` - the source arrays to copy from
    /// * `use_nulls` - a flag indicating whether the caller intends to call `extend_nulls`.
    ///   Note: null-handling is enabled automatically if any source array contains nulls.
    /// * `capacity` - the preallocated capacity of the output array, in slots (number of elements)
    ///
    /// if `use_nulls` is `false` and no source arrays contains nulls, calling
    /// [MutableArrayData::extend_nulls] or [MutableArrayData::try_extend_nulls] will panic.
    pub fn new(arrays: Vec<&'a ArrayData>, use_nulls: bool, capacity: usize) -> Self {
        Self::with_capacities(arrays, use_nulls, Capacities::Array(capacity))
    }

    /// Similar to [MutableArrayData::new], but lets users define the
    /// preallocated capacities of the array with more granularity.
    ///
    /// See [MutableArrayData::new] for more information on the arguments.
    ///
    /// # Panics
    ///
    /// This function panics if the given `capacities` don't match the data type
    /// of `arrays`. Or when a [Capacities] variant is not yet supported.
    pub fn with_capacities(
        arrays: Vec<&'a ArrayData>,
        use_nulls: bool,
        capacities: Capacities,
    ) -> Self {
        build_mutable_array_data_iter(arrays, use_nulls, capacities)
    }

    fn empty_placeholder() -> Self {
        Self {
            arrays: Vec::new(),
            data: _MutableArrayData::empty(),
            dictionary: None,
            variadic_data_buffers: Vec::new(),
            extend_values: Vec::new(),
            extend_null_bits: Vec::new(),
            extend_nulls: Box::new(|_, _, _| Ok(())),
        }
    }

    /// Extends the in progress array with a region of the input arrays, returning an error on
    /// overflow.
    ///
    /// # Arguments
    /// * `index` - the index of array that you want to copy values from
    /// * `start` - the start index of the chunk (inclusive)
    /// * `end` - the end index of the chunk (exclusive)
    ///
    /// # Errors
    /// Returns an error if offset arithmetic overflows the underlying integer type.
    ///
    /// # Panic
    /// This function panics if there is an invalid index,
    /// i.e. `index` >= the number of source arrays
    /// or `end` > the length of the `index`th array
    pub fn try_extend(&mut self, index: usize, start: usize, end: usize) -> Result<(), ArrowError> {
        try_extend_iter(self, index, start, end)
    }

    /// Extends the in progress array with a region of the input arrays.
    ///
    /// # Panic
    /// This function panics if there is an invalid index,
    /// i.e. `index` >= the number of source arrays,
    /// `end` > the length of the `index`th array,
    /// or the offset type overflows (e.g. more than 2 GiB in a `StringArray`).
    #[deprecated(
        since = "59.0.0",
        note = "Use `try_extend` which returns an error on overflow instead of panicking"
    )]
    pub fn extend(&mut self, index: usize, start: usize, end: usize) {
        self.try_extend(index, start, end)
            .expect("extend failed due to offset overflow")
    }

    /// Extends the in progress array with null elements, ignoring the input arrays, returning an
    /// error on overflow.
    ///
    /// Prefer this over [`extend_nulls`](Self::extend_nulls) to handle cases where the run-end
    /// counter overflows (relevant for `RunEndEncoded` arrays).
    ///
    /// # Panics
    ///
    /// Panics if [`MutableArrayData`] not created with `use_nulls` or nullable source arrays
    pub fn try_extend_nulls(&mut self, len: usize) -> Result<(), ArrowError> {
        try_extend_nulls_iter(self, len)
    }

    /// Extends the in progress array with null elements, ignoring the input arrays.
    ///
    /// # Panics
    ///
    /// Panics if [`MutableArrayData`] not created with `use_nulls` or nullable source arrays,
    /// or if the run-end counter overflows for `RunEndEncoded` arrays.
    #[deprecated(
        since = "59.0.0",
        note = "Use `try_extend_nulls` which returns an error on overflow instead of panicking"
    )]
    pub fn extend_nulls(&mut self, len: usize) {
        self.try_extend_nulls(len)
            .expect("extend_nulls failed due to overflow")
    }

    /// Returns the current length
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len
    }

    /// Returns true if len is 0
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.len == 0
    }

    /// Returns the current null count
    #[inline]
    pub fn null_count(&self) -> usize {
        self.data.null_count
    }

    /// Creates a [ArrayData] from the in progress array, consuming `self`.
    pub fn freeze(self) -> ArrayData {
        unsafe { self.into_builder().build_unchecked() }
    }

    /// Consume self and returns the in progress array as [`ArrayDataBuilder`].
    ///
    /// This is useful for extending the default behavior of MutableArrayData.
    pub fn into_builder(self) -> ArrayDataBuilder {
        into_builder_iter(self)
    }
}

impl Drop for MutableArrayData<'_> {
    fn drop(&mut self) {
        if self.data.child_data.is_empty() {
            return;
        }
        let mut stack = mem::take(&mut self.data.child_data);
        while let Some(mut node) = stack.pop() {
            stack.extend(mem::take(&mut node.data.child_data));
            node.extend_values.clear();
            node.extend_null_bits.clear();
        }
    }
}

struct BuildSpec<'a> {
    arrays: Vec<&'a ArrayData>,
    use_nulls: bool,
    capacities: Capacities,
}

struct PartialMutableNode<'a> {
    arrays: Vec<&'a ArrayData>,
    data_type: DataType,
    buffer1: MutableBuffer,
    buffer2: MutableBuffer,
    null_buffer: Option<MutableBuffer>,
    dictionary: Option<ArrayData>,
    variadic_data_buffers: Vec<Buffer>,
    extend_values: Vec<Extend<'a>>,
    extend_null_bits: Vec<ExtendNullBits<'a>>,
    extend_nulls: ExtendNulls,
}

enum BuildFrame<'a> {
    Children {
        partial: PartialMutableNode<'a>,
        pending: Vec<BuildSpec<'a>>,
        built: Vec<MutableArrayData<'a>>,
    },
    DictConcat {
        partial: PartialMutableNode<'a>,
        dict_lengths: Vec<usize>,
    },
}

fn prepare_partial_node<'a>(
    arrays: Vec<&'a ArrayData>,
    use_nulls: bool,
    capacities: Capacities,
) -> (PartialMutableNode<'a>, Option<BuildFrame<'a>>, Option<BuildSpec<'a>>) {
    let data_type = arrays[0].data_type();

    for a in arrays.iter().skip(1) {
        assert_eq!(
            data_type,
            a.data_type(),
            "Arrays with inconsistent types passed to MutableArrayData"
        )
    }

    let use_nulls = use_nulls | arrays.iter().any(|array| array.null_count() > 0);
    let mut array_capacity;

    let [buffer1, buffer2] = match (data_type, &capacities) {
        (
            DataType::LargeUtf8 | DataType::LargeBinary,
            Capacities::Binary(capacity, Some(value_cap)),
        ) => {
            array_capacity = *capacity;
            preallocate_offset_and_binary_buffer::<i64>(*capacity, *value_cap)
        }
        (DataType::Utf8 | DataType::Binary, Capacities::Binary(capacity, Some(value_cap))) => {
            array_capacity = *capacity;
            preallocate_offset_and_binary_buffer::<i32>(*capacity, *value_cap)
        }
        (_, Capacities::Array(capacity)) => {
            array_capacity = *capacity;
            new_buffers(data_type, *capacity)
        }
        (
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _),
            Capacities::List(capacity, _),
        ) => {
            array_capacity = *capacity;
            new_buffers(data_type, *capacity)
        }
        _ => panic!("Capacities: {capacities:?} not yet supported"),
    };

    let mut child_specs: Vec<BuildSpec<'a>> = Vec::new();
    match &data_type {
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Null
        | DataType::Boolean
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Timestamp(_, _)
        | DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8View
        | DataType::Interval(_)
        | DataType::FixedSizeBinary(_)
        | DataType::Dictionary(_, _) => {}
        DataType::Map(_, _)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => {
            let children = arrays.iter().map(|array| &array.child_data()[0]).collect();
            let capacities = if let Capacities::List(capacity, ref child_capacities) = capacities
            {
                child_capacities
                    .clone()
                    .map(|c| *c)
                    .unwrap_or(Capacities::Array(capacity))
            } else {
                Capacities::Array(array_capacity)
            };
            child_specs.push(BuildSpec {
                arrays: children,
                use_nulls,
                capacities,
            });
        }
        DataType::Struct(fields) => match capacities {
            Capacities::Struct(capacity, Some(ref child_capacities)) => {
                array_capacity = capacity;
                for (i, child_cap) in (0..fields.len()).zip(child_capacities) {
                    let child_arrays = arrays.iter().map(|array| &array.child_data()[i]).collect();
                    child_specs.push(BuildSpec {
                        arrays: child_arrays,
                        use_nulls,
                        capacities: child_cap.clone(),
                    });
                }
            }
            Capacities::Struct(capacity, None) => {
                array_capacity = capacity;
                for i in 0..fields.len() {
                    let child_arrays = arrays.iter().map(|array| &array.child_data()[i]).collect();
                    child_specs.push(BuildSpec {
                        arrays: child_arrays,
                        use_nulls,
                        capacities: Capacities::Array(capacity),
                    });
                }
            }
            _ => {
                for i in 0..fields.len() {
                    let child_arrays = arrays.iter().map(|array| &array.child_data()[i]).collect();
                    child_specs.push(BuildSpec {
                        arrays: child_arrays,
                        use_nulls,
                        capacities: Capacities::Array(array_capacity),
                    });
                }
            }
        },
        DataType::RunEndEncoded(_, _) => {
            let run_ends_child = arrays.iter().map(|array| &array.child_data()[0]).collect();
            let value_child = arrays.iter().map(|array| &array.child_data()[1]).collect();
            child_specs.push(BuildSpec {
                arrays: run_ends_child,
                use_nulls: false,
                capacities: Capacities::Array(array_capacity),
            });
            child_specs.push(BuildSpec {
                arrays: value_child,
                use_nulls,
                capacities: Capacities::Array(array_capacity),
            });
        }
        DataType::FixedSizeList(_, size) => {
            let children = arrays.iter().map(|array| &array.child_data()[0]).collect();
            let capacities = if let Capacities::List(capacity, ref child_capacities) = capacities
            {
                child_capacities
                    .clone()
                    .map(|c| *c)
                    .unwrap_or(Capacities::Array(capacity * *size as usize))
            } else {
                Capacities::Array(array_capacity * *size as usize)
            };
            child_specs.push(BuildSpec {
                arrays: children,
                use_nulls,
                capacities,
            });
        }
        DataType::Union(fields, _) => {
            for i in 0..fields.len() {
                let child_arrays = arrays.iter().map(|array| &array.child_data()[i]).collect();
                child_specs.push(BuildSpec {
                    arrays: child_arrays,
                    use_nulls,
                    capacities: Capacities::Array(array_capacity),
                });
            }
        }
    }

    let mut dict_concat_task: Option<(Vec<&'a ArrayData>, usize, Vec<usize>)> = None;
    let (dictionary, dict_concat) = match &data_type {
        DataType::Dictionary(_, _) => {
            let dict_concat = !arrays
                .windows(2)
                .all(|a| a[0].child_data()[0].ptr_eq(&a[1].child_data()[0]));

            if !dict_concat {
                (Some(arrays[0].child_data()[0].clone()), false)
            } else {
                if let Capacities::Dictionary(_, _) = capacities {
                    panic!("dictionary capacity not yet supported")
                }
                let dictionaries: Vec<_> =
                    arrays.iter().map(|array| &array.child_data()[0]).collect();
                let lengths: Vec<_> = dictionaries.iter().map(|dictionary| dictionary.len()).collect();
                let capacity = lengths.iter().sum();
                dict_concat_task = Some((dictionaries, capacity, lengths));
                (None, true)
            }
        }
        _ => (None, false),
    };

    let variadic_data_buffers = match &data_type {
        DataType::BinaryView | DataType::Utf8View => arrays
            .iter()
            .flat_map(|x| x.buffers().iter().skip(1))
            .map(Buffer::clone)
            .collect(),
        _ => vec![],
    };

    let extend_nulls = build_extend_nulls(data_type);
    let extend_null_bits = arrays
        .iter()
        .map(|array| build_extend_null_bits(array, use_nulls))
        .collect();
    let null_buffer = use_nulls.then(|| {
        let null_bytes = bit_util::ceil(array_capacity, 8);
        MutableBuffer::from_len_zeroed(null_bytes)
    });

    let extend_values = match &data_type {
        DataType::Dictionary(_, _) => {
            let mut next_offset = 0;
            let extend_values: Result<Vec<_>, _> = arrays
                .iter()
                .map(|array| {
                    let offset = next_offset;
                    let dict_len = array.child_data()[0].len();

                    if dict_concat {
                        next_offset += dict_len;
                    }

                    build_extend_dictionary(array, offset, offset + dict_len)
                        .ok_or(ArrowError::DictionaryKeyOverflowError)
                })
                .collect();

            extend_values.expect("MutableArrayData::new is infallible")
        }
        DataType::BinaryView | DataType::Utf8View => {
            let mut next_offset = 0u32;
            arrays
                .iter()
                .map(|arr| {
                    let num_data_buffers = (arr.buffers().len() - 1) as u32;
                    let offset = next_offset;
                    next_offset = next_offset
                        .checked_add(num_data_buffers)
                        .expect("view buffer index overflow");
                    build_extend_view(arr, offset)
                })
                .collect()
        }
        _ => arrays.iter().map(|array| build_extend(array)).collect(),
    };

    let partial = PartialMutableNode {
        arrays,
        data_type: data_type.clone(),
        buffer1,
        buffer2,
        null_buffer,
        dictionary,
        variadic_data_buffers,
        extend_values,
        extend_null_bits,
        extend_nulls,
    };

    if let Some((dictionaries, capacity, dict_lengths)) = dict_concat_task {
        let next = BuildSpec {
            arrays: dictionaries,
            use_nulls: false,
            capacities: Capacities::Array(capacity),
        };
        return (
            partial,
            Some(BuildFrame::DictConcat {
                partial: PartialMutableNode::empty_placeholder(),
                dict_lengths,
            }),
            Some(next),
        );
    }

    if !child_specs.is_empty() {
        child_specs.reverse();
        let next = child_specs.pop().expect("non-empty child specs");
        let built = Vec::with_capacity(child_specs.len() + 1);
        return (
            partial,
            Some(BuildFrame::Children {
                partial: PartialMutableNode::empty_placeholder(),
                pending: child_specs,
                built,
            }),
            Some(next),
        );
    }

    (partial, None, None)
}

impl<'a> PartialMutableNode<'a> {
    fn empty_placeholder() -> Self {
        Self {
            arrays: Vec::new(),
            data_type: DataType::Null,
            buffer1: MutableBuffer::new(0),
            buffer2: MutableBuffer::new(0),
            null_buffer: None,
            dictionary: None,
            variadic_data_buffers: Vec::new(),
            extend_values: Vec::new(),
            extend_null_bits: Vec::new(),
            extend_nulls: Box::new(|_, _, _| Ok(())),
        }
    }

    fn finish(self, child_data: Vec<MutableArrayData<'a>>) -> MutableArrayData<'a> {
        let data = _MutableArrayData {
            data_type: self.data_type,
            len: 0,
            null_count: 0,
            null_buffer: self.null_buffer,
            buffer1: self.buffer1,
            buffer2: self.buffer2,
            child_data,
        };
        MutableArrayData {
            arrays: self.arrays,
            data,
            dictionary: self.dictionary,
            variadic_data_buffers: self.variadic_data_buffers,
            extend_values: self.extend_values,
            extend_null_bits: self.extend_null_bits,
            extend_nulls: self.extend_nulls,
        }
    }
}

fn build_mutable_array_data_iter<'a>(
    arrays: Vec<&'a ArrayData>,
    use_nulls: bool,
    capacities: Capacities,
) -> MutableArrayData<'a> {
    let mut stack: Vec<BuildFrame<'a>> = Vec::new();
    let mut current = BuildSpec {
        arrays,
        use_nulls,
        capacities,
    };

    loop {
        let (partial, maybe_frame, maybe_next) =
            prepare_partial_node(current.arrays, current.use_nulls, current.capacities);
        let mut completed = match (maybe_frame, maybe_next) {
            (Some(mut frame), Some(next)) => {
                match &mut frame {
                    BuildFrame::Children { partial: slot, .. }
                    | BuildFrame::DictConcat { partial: slot, .. } => *slot = partial,
                }
                stack.push(frame);
                current = next;
                continue;
            }
            _ => partial.finish(Vec::new()),
        };

        loop {
            match stack.pop() {
                None => return completed,
                Some(BuildFrame::Children {
                    partial,
                    mut pending,
                    mut built,
                }) => {
                    built.push(completed);
                    if let Some(next) = pending.pop() {
                        stack.push(BuildFrame::Children {
                            partial,
                            pending,
                            built,
                        });
                        current = next;
                        break;
                    }
                    completed = partial.finish(built);
                }
                Some(BuildFrame::DictConcat {
                    mut partial,
                    dict_lengths,
                }) => {
                    for (i, len) in dict_lengths.iter().enumerate() {
                        completed.try_extend(i, 0, *len).expect(
                            "extend failed while building dictionary; \
                             this is a bug in MutableArrayData",
                        );
                    }
                    partial.dictionary = Some(completed.freeze());
                    completed = partial.finish(Vec::new());
                }
            }
        }
    }
}

struct ExtendFrame<'a> {
    node: MutableArrayData<'a>,
    buf1_len: usize,
    buf2_len: usize,
    added_len: usize,
    pending: Vec<ChildExtendTask>,
    slot_idx: usize,
    wrap_struct_col: bool,
}

struct ActiveExtendGuard<'root, 'a> {
    root: &'root mut MutableArrayData<'a>,
    frames: Vec<ExtendFrame<'a>>,
}

impl Drop for ActiveExtendGuard<'_, '_> {
    fn drop(&mut self) {
        while let Some(mut frame) = self.frames.pop() {
            if let Some(parent) = self.frames.last_mut() {
                mem::swap(&mut parent.node.data.child_data[frame.slot_idx], &mut frame.node);
            } else {
                *self.root = frame.node;
            }
        }
    }
}

fn rollback_extend_guard(mut guard: ActiveExtendGuard<'_, '_>, mut err: ArrowError) -> ArrowError {
    while let Some(mut frame) = guard.frames.pop() {
        frame.node.data.buffer1.truncate(frame.buf1_len);
        frame.node.data.buffer2.truncate(frame.buf2_len);
        if let Some(parent) = guard.frames.last_mut() {
            let slot_idx = frame.slot_idx;
            let wrap = frame.wrap_struct_col;
            mem::swap(&mut parent.node.data.child_data[slot_idx], &mut frame.node);
            if wrap {
                err = structure::wrap_column_error(err, slot_idx, &parent.node.data.data_type);
            }
        } else {
            *guard.root = frame.node;
            return err;
        }
    }
    err
}

fn try_extend_iter(
    root: &mut MutableArrayData<'_>,
    index: usize,
    start: usize,
    end: usize,
) -> Result<(), ArrowError> {
    let root_owned = mem::replace(root, MutableArrayData::empty_placeholder());
    let mut guard = ActiveExtendGuard {
        root,
        frames: Vec::new(),
    };
    let mut cur_node = root_owned;
    let mut cur_task = (index, start, end, 0usize, false);

    loop {
        let (array_idx, task_start, task_end, slot_idx, wrap_struct_col) = cur_task;
        let len = task_end - task_start;
        (cur_node.extend_null_bits[array_idx])(&mut cur_node.data, task_start, len);
        let buf1_len = cur_node.data.buffer1.len();
        let buf2_len = cur_node.data.buffer2.len();

        let mut child_tasks = Vec::new();
        let res = (cur_node.extend_values[array_idx])(
            &mut cur_node.data,
            array_idx,
            task_start,
            len,
            &mut child_tasks,
        );
        guard.frames.push(ExtendFrame {
            node: cur_node,
            buf1_len,
            buf2_len,
            added_len: len,
            pending: Vec::new(),
            slot_idx,
            wrap_struct_col,
        });
        if let Err(err) = res {
            return Err(rollback_extend_guard(guard, err));
        }

        if !child_tasks.is_empty() {
            child_tasks.reverse();
            let next = child_tasks.pop().expect("non-empty child tasks");
            let parent = guard.frames.last_mut().expect("parent pushed");
            parent.pending = child_tasks;
            cur_node = mem::replace(
                &mut parent.node.data.child_data[next.child_idx],
                MutableArrayData::empty_placeholder(),
            );
            cur_task = (
                next.array_idx,
                next.start,
                next.end,
                next.child_idx,
                next.wrap_struct_col,
            );
            continue;
        }

        loop {
            let mut finished = guard.frames.pop().expect("active frame");
            finished.node.data.len += finished.added_len;
            let Some(parent) = guard.frames.last_mut() else {
                *guard.root = finished.node;
                return Ok(());
            };
            mem::swap(
                &mut parent.node.data.child_data[finished.slot_idx],
                &mut finished.node,
            );
            if let Some(next) = parent.pending.pop() {
                cur_node = mem::replace(
                    &mut parent.node.data.child_data[next.child_idx],
                    MutableArrayData::empty_placeholder(),
                );
                cur_task = (
                    next.array_idx,
                    next.start,
                    next.end,
                    next.child_idx,
                    next.wrap_struct_col,
                );
                break;
            }
        }
    }
}

struct ExtendNullsFrame<'a> {
    node: MutableArrayData<'a>,
    pending: Vec<ChildExtendNullsTask>,
    slot_idx: usize,
    wrap_struct_col: bool,
    ree_child0_prev: Option<(usize, usize)>,
}

struct ActiveExtendNullsGuard<'root, 'a> {
    root: &'root mut MutableArrayData<'a>,
    frames: Vec<ExtendNullsFrame<'a>>,
}

impl Drop for ActiveExtendNullsGuard<'_, '_> {
    fn drop(&mut self) {
        while let Some(mut frame) = self.frames.pop() {
            if let Some(parent) = self.frames.last_mut() {
                mem::swap(&mut parent.node.data.child_data[frame.slot_idx], &mut frame.node);
            } else {
                *self.root = frame.node;
            }
        }
    }
}

fn rollback_extend_nulls_guard(
    mut guard: ActiveExtendNullsGuard<'_, '_>,
    mut err: ArrowError,
) -> ArrowError {
    while let Some(mut frame) = guard.frames.pop() {
        if let Some((old_len, old_buf1)) = frame.ree_child0_prev {
            frame.node.data.child_data[0].data.len = old_len;
            frame.node.data.child_data[0].data.buffer1.truncate(old_buf1);
        }
        if let Some(parent) = guard.frames.last_mut() {
            let slot_idx = frame.slot_idx;
            let wrap = frame.wrap_struct_col;
            mem::swap(&mut parent.node.data.child_data[slot_idx], &mut frame.node);
            if wrap {
                err = structure::wrap_column_error(err, slot_idx, &parent.node.data.data_type);
            }
        } else {
            *guard.root = frame.node;
            return err;
        }
    }
    err
}

fn try_extend_nulls_iter(root: &mut MutableArrayData<'_>, len: usize) -> Result<(), ArrowError> {
    let root_owned = mem::replace(root, MutableArrayData::empty_placeholder());
    let mut guard = ActiveExtendNullsGuard {
        root,
        frames: Vec::new(),
    };
    let mut cur_node = root_owned;
    let mut cur_task = (len, 0usize, false);

    loop {
        let (task_len, slot_idx, wrap_struct_col) = cur_task;
        cur_node.data.len += task_len;
        let bit_len = bit_util::ceil(cur_node.data.len, 8);
        let nulls = cur_node.data.null_buffer();
        nulls.resize(bit_len, 0);
        cur_node.data.null_count += task_len;

        let ree_child0_prev = match &cur_node.data.data_type {
            DataType::RunEndEncoded(_, _) if !cur_node.data.child_data.is_empty() => Some((
                cur_node.data.child_data[0].data.len,
                cur_node.data.child_data[0].data.buffer1.len(),
            )),
            _ => None,
        };

        let mut child_tasks = Vec::new();
        let res = (cur_node.extend_nulls)(&mut cur_node.data, task_len, &mut child_tasks);
        guard.frames.push(ExtendNullsFrame {
            node: cur_node,
            pending: Vec::new(),
            slot_idx,
            wrap_struct_col,
            ree_child0_prev,
        });
        if let Err(err) = res {
            return Err(rollback_extend_nulls_guard(guard, err));
        }

        if !child_tasks.is_empty() {
            child_tasks.reverse();
            let next = child_tasks.pop().expect("non-empty child null tasks");
            let parent = guard.frames.last_mut().expect("parent pushed");
            parent.pending = child_tasks;
            cur_node = mem::replace(
                &mut parent.node.data.child_data[next.child_idx],
                MutableArrayData::empty_placeholder(),
            );
            cur_task = (next.len, next.child_idx, next.wrap_struct_col);
            continue;
        }

        loop {
            let mut finished = guard.frames.pop().expect("active null frame");
            let Some(parent) = guard.frames.last_mut() else {
                *guard.root = finished.node;
                return Ok(());
            };
            mem::swap(
                &mut parent.node.data.child_data[finished.slot_idx],
                &mut finished.node,
            );
            if let Some(next) = parent.pending.pop() {
                cur_node = mem::replace(
                    &mut parent.node.data.child_data[next.child_idx],
                    MutableArrayData::empty_placeholder(),
                );
                cur_task = (next.len, next.child_idx, next.wrap_struct_col);
                break;
            }
        }
    }
}

struct PartialBuilder {
    data_type: DataType,
    len: usize,
    null_count: usize,
    null_buffer: Option<MutableBuffer>,
    buffers: Vec<Buffer>,
}

struct FreezeFrame<'a> {
    partial: PartialBuilder,
    pending: Vec<MutableArrayData<'a>>,
    built: Vec<ArrayData>,
}

fn prepare_partial_builder<'a>(
    mut mutable: MutableArrayData<'a>,
) -> (PartialBuilder, Vec<MutableArrayData<'a>>, Vec<ArrayData>) {
    let mut data = mem::replace(&mut mutable.data, _MutableArrayData::empty());
    let variadic = mem::take(&mut mutable.variadic_data_buffers);
    let dict = mutable.dictionary.take();

    let buffers = match data.data_type {
        DataType::Null
        | DataType::Struct(_)
        | DataType::FixedSizeList(_, _)
        | DataType::RunEndEncoded(_, _) => vec![],
        DataType::BinaryView | DataType::Utf8View => {
            let mut b = variadic;
            b.insert(0, mem::replace(&mut data.buffer1, MutableBuffer::new(0)).into());
            b
        }
        DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary
        | DataType::ListView(_)
        | DataType::LargeListView(_) => vec![
            mem::replace(&mut data.buffer1, MutableBuffer::new(0)).into(),
            mem::replace(&mut data.buffer2, MutableBuffer::new(0)).into(),
        ],
        DataType::Union(_, mode) => match mode {
            UnionMode::Sparse => vec![mem::replace(&mut data.buffer1, MutableBuffer::new(0)).into()],
            UnionMode::Dense => vec![
                mem::replace(&mut data.buffer1, MutableBuffer::new(0)).into(),
                mem::replace(&mut data.buffer2, MutableBuffer::new(0)).into(),
            ],
        },
        _ => vec![mem::replace(&mut data.buffer1, MutableBuffer::new(0)).into()],
    };

    let (pending, prebuilt) = match data.data_type {
        DataType::Dictionary(_, _) => (Vec::new(), vec![dict.expect("dictionary child")]),
        _ => (mem::take(&mut data.child_data), Vec::new()),
    };

    let partial = PartialBuilder {
        data_type: mem::replace(&mut data.data_type, DataType::Null),
        len: data.len,
        null_count: data.null_count,
        null_buffer: data.null_buffer.take(),
        buffers,
    };
    (partial, pending, prebuilt)
}

fn finish_partial_builder(partial: PartialBuilder, child_data: Vec<ArrayData>) -> ArrayDataBuilder {
    let nulls = match partial.data_type {
        // RunEndEncoded, Null, and Union arrays cannot have top-level null bitmasks
        DataType::RunEndEncoded(_, _) | DataType::Null | DataType::Union(_, _) => None,
        _ => partial
            .null_buffer
            .map(|nulls| {
                let bools = BooleanBuffer::new(nulls.into(), 0, partial.len);
                unsafe { NullBuffer::new_unchecked(bools, partial.null_count) }
            })
            .filter(|n| n.null_count() > 0),
    };

    ArrayDataBuilder::new(partial.data_type)
        .offset(0)
        .len(partial.len)
        .nulls(nulls)
        .buffers(partial.buffers)
        .child_data(child_data)
}

fn into_builder_iter(root: MutableArrayData<'_>) -> ArrayDataBuilder {
    let mut stack: Vec<FreezeFrame<'_>> = Vec::new();
    let mut current = root;

    loop {
        let (partial, mut pending, prebuilt) = prepare_partial_builder(current);
        let mut completed = if pending.is_empty() {
            finish_partial_builder(partial, prebuilt)
        } else {
            pending.reverse();
            let first = pending.pop().expect("non-empty pending builders");
            let cap = pending.len() + 1;
            stack.push(FreezeFrame {
                partial,
                pending,
                built: Vec::with_capacity(cap),
            });
            current = first;
            continue;
        };

        loop {
            match stack.pop() {
                None => return completed,
                Some(FreezeFrame {
                    partial,
                    mut pending,
                    mut built,
                }) => {
                    built.push(unsafe { completed.build_unchecked() });
                    if let Some(next) = pending.pop() {
                        stack.push(FreezeFrame {
                            partial,
                            pending,
                            built,
                        });
                        current = next;
                        break;
                    }
                    completed = finish_partial_builder(partial, built);
                }
            }
        }
    }
}

// See arrow/tests/array_transform.rs for tests of transform functionality

#[cfg(test)]
mod test {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn test_list_append_with_capacities() {
        let array = ArrayData::new_empty(&DataType::List(Arc::new(Field::new(
            "element",
            DataType::Int64,
            false,
        ))));

        let mutable = MutableArrayData::with_capacities(
            vec![&array],
            false,
            Capacities::List(6, Some(Box::new(Capacities::Array(17)))),
        );

        // capacities are rounded up to multiples of 64 by MutableBuffer
        assert_eq!(mutable.data.buffer1.capacity(), 64);
        assert_eq!(mutable.data.child_data[0].data.buffer1.capacity(), 192);
    }

    fn build_deep_transform_array_data(depth: usize, dict_val: i32) -> ArrayData {
        let mut current = ArrayDataBuilder::new(DataType::Int32)
            .len(2)
            .add_buffer(Buffer::from_slice_ref([dict_val, dict_val + 1]))
            .build()
            .unwrap();

        for level in 0..depth {
            let child_field = Arc::new(Field::new("item", current.data_type().clone(), true));
            current = match level % 6 {
                0 => {
                    let fields = arrow_schema::Fields::from(vec![child_field]);
                    ArrayDataBuilder::new(DataType::Struct(fields))
                        .len(2)
                        .add_child_data(current)
                        .build()
                        .unwrap()
                }
                1 => ArrayDataBuilder::new(DataType::List(child_field))
                    .len(2)
                    .add_buffer(Buffer::from_slice_ref([0i32, 1, 2]))
                    .add_child_data(current)
                    .build()
                    .unwrap(),
                2 => ArrayDataBuilder::new(DataType::FixedSizeList(child_field, 1))
                    .len(2)
                    .add_child_data(current)
                    .build()
                    .unwrap(),
                3 => ArrayDataBuilder::new(DataType::ListView(child_field))
                    .len(2)
                    .add_buffer(Buffer::from_slice_ref([0i32, 1]))
                    .add_buffer(Buffer::from_slice_ref([1i32, 1]))
                    .add_child_data(current)
                    .build()
                    .unwrap(),
                4 => {
                    let ends_field = Arc::new(Field::new("run_ends", DataType::Int32, false));
                    let run_ends = ArrayDataBuilder::new(DataType::Int32)
                        .len(2)
                        .add_buffer(Buffer::from_slice_ref([1i32, 2]))
                        .build()
                        .unwrap();
                    ArrayDataBuilder::new(DataType::RunEndEncoded(ends_field, child_field))
                        .len(2)
                        .add_child_data(run_ends)
                        .add_child_data(current)
                        .build()
                        .unwrap()
                }
                _ => ArrayDataBuilder::new(DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(current.data_type().clone()),
                ))
                .len(2)
                .add_buffer(Buffer::from_slice_ref([0i32, 1]))
                .add_child_data(current)
                .build()
                .unwrap(),
            };
        }
        current
    }

    #[test]
    fn deep_mutable_array_data_small_stack_roundtrip() {
        let depth = 512;
        let left = build_deep_transform_array_data(depth, 10);
        let right = build_deep_transform_array_data(depth, 20);

        let handle = std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let mut mutable = MutableArrayData::new(vec![&left, &right], true, 5);
                mutable.try_extend(0, 0, 2).unwrap();
                mutable.try_extend(1, 0, 2).unwrap();
                mutable.try_extend_nulls(1).unwrap();
                assert_eq!(mutable.len(), 5);
                assert_eq!(mutable.null_count(), 1);
                let frozen = mutable.freeze();
                assert_eq!(frozen.len(), 5);
                assert_eq!(frozen.null_count(), 1);

                let to_drop = MutableArrayData::new(vec![&left], true, 2);
                drop(to_drop);
                frozen
            })
            .unwrap();

        let frozen = handle.join().unwrap();
        assert_eq!(frozen.len(), 5);
    }

    #[test]
    fn deep_2048_mutable_array_data_list_struct_map_small_stack() {
        let depth = 2048;
        let mut current = ArrayDataBuilder::new(DataType::Int32)
            .len(2)
            .add_buffer(Buffer::from_slice_ref([1i32, 2]))
            .build()
            .unwrap();
        for level in 0..depth {
            let child_field = Arc::new(Field::new("item", current.data_type().clone(), true));
            current = match level % 3 {
                0 => ArrayDataBuilder::new(DataType::List(child_field))
                    .len(2)
                    .add_buffer(Buffer::from_slice_ref([0i32, 1, 2]))
                    .add_child_data(current)
                    .build()
                    .unwrap(),
                1 => {
                    let fields = arrow_schema::Fields::from(vec![child_field]);
                    ArrayDataBuilder::new(DataType::Struct(fields))
                        .len(2)
                        .add_child_data(current)
                        .build()
                        .unwrap()
                }
                _ => {
                    let key_field = Arc::new(Field::new("key", DataType::Int32, false));
                    let keys = ArrayDataBuilder::new(DataType::Int32)
                        .len(2)
                        .add_buffer(Buffer::from_slice_ref([10i32, 20]))
                        .build()
                        .unwrap();
                    let entry_fields = arrow_schema::Fields::from(vec![key_field, child_field]);
                    let entries = ArrayDataBuilder::new(DataType::Struct(entry_fields.clone()))
                        .len(2)
                        .add_child_data(keys)
                        .add_child_data(current)
                        .build()
                        .unwrap();
                    let entries_field = Arc::new(Field::new_struct("entries", entry_fields, false));
                    ArrayDataBuilder::new(DataType::Map(entries_field, false))
                        .len(2)
                        .add_buffer(Buffer::from_slice_ref([0i32, 1, 2]))
                        .add_child_data(entries)
                        .build()
                        .unwrap()
                }
            };
        }

        let right = current.clone();
        let handle = std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let mut mutable = MutableArrayData::new(vec![&current, &right], true, 5);
                mutable.try_extend(0, 0, 2).unwrap();
                mutable.try_extend(1, 0, 2).unwrap();
                mutable.try_extend_nulls(1).unwrap();
                let frozen = mutable.freeze();
                assert_eq!(frozen.len(), 5);
                assert_eq!(frozen.null_count(), 1);
                frozen
            })
            .unwrap();

        let frozen = handle.join().unwrap();
        assert_eq!(frozen.len(), 5);
    }
}
