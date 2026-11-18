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

// Frozen shallow oracle from the verified 59.3.2 registry archive.
// See structural_reference.source.json. Only type plumbing is adapted.
#![allow(dead_code, deprecated)]
use std::{cmp::Ordering, collections::HashMap, fmt, hash::{Hash, Hasher}, sync::Arc};
use std::fmt::Display;
use crate::{TimeUnit, IntervalUnit, UnionMode};
type FieldRef = Arc<Field>;

// The conversion only builds bounded fixtures; it is not production traversal.
impl Field {
    pub(crate) fn from_current(value: &crate::Field) -> Self {
        let (dict_id, dict_is_ordered) = value.dictionary_flags();
        Self { name: value.name().clone(), data_type: DataType::from(value.data_type()),
            nullable: value.is_nullable(), dict_id, dict_is_ordered, metadata: value.metadata().clone() }
    }
}

impl From<&crate::DataType> for DataType {
    fn from(value: &crate::DataType) -> Self {
        use crate::DataType as Current;
        let field = |f: &crate::FieldRef| Arc::new(Field::from_current(f));
        match value {
            Current::Null => Self::Null, Current::Boolean => Self::Boolean,
            Current::Int8 => Self::Int8, Current::Int16 => Self::Int16,
            Current::Int32 => Self::Int32, Current::Int64 => Self::Int64,
            Current::UInt8 => Self::UInt8, Current::UInt16 => Self::UInt16,
            Current::UInt32 => Self::UInt32, Current::UInt64 => Self::UInt64,
            Current::Float16 => Self::Float16, Current::Float32 => Self::Float32,
            Current::Float64 => Self::Float64,
            Current::Timestamp(unit, timezone) => Self::Timestamp(*unit, timezone.clone()),
            Current::Date32 => Self::Date32, Current::Date64 => Self::Date64,
            Current::Time32(unit) => Self::Time32(*unit), Current::Time64(unit) => Self::Time64(*unit),
            Current::Duration(unit) => Self::Duration(*unit), Current::Interval(unit) => Self::Interval(*unit),
            Current::Binary => Self::Binary, Current::FixedSizeBinary(size) => Self::FixedSizeBinary(*size),
            Current::LargeBinary => Self::LargeBinary, Current::BinaryView => Self::BinaryView,
            Current::Utf8 => Self::Utf8, Current::LargeUtf8 => Self::LargeUtf8, Current::Utf8View => Self::Utf8View,
            Current::List(f) => Self::List(field(f)), Current::ListView(f) => Self::ListView(field(f)),
            Current::FixedSizeList(f, size) => Self::FixedSizeList(field(f), *size),
            Current::LargeList(f) => Self::LargeList(field(f)), Current::LargeListView(f) => Self::LargeListView(field(f)),
            Current::Struct(fs) => Self::Struct(Fields(fs.iter().map(field).collect())),
            Current::Union(fs, mode) => Self::Union(UnionFields(fs.iter().map(|(id, f)| (id, field(f))).collect()), *mode),
            Current::Dictionary(key, value) => Self::Dictionary(Box::new(Self::from(key.as_ref())), Box::new(Self::from(value.as_ref()))),
            Current::Decimal32(p, s) => Self::Decimal32(*p, *s), Current::Decimal64(p, s) => Self::Decimal64(*p, *s),
            Current::Decimal128(p, s) => Self::Decimal128(*p, *s), Current::Decimal256(p, s) => Self::Decimal256(*p, *s),
            Current::Map(f, sorted) => Self::Map(field(f), *sorted),
            Current::RunEndEncoded(a, b) => Self::RunEndEncoded(field(a), field(b)),
        }
    }
}


#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataType {
    /// Null type
    Null,
    /// A boolean datatype representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// A timestamp with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a signed 64-bit integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    ///
    /// Timestamps with a non-empty timezone
    /// ------------------------------------
    ///
    /// If a Timestamp column has a non-empty timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
    /// (the Unix epoch), regardless of the Timestamp's own timezone.
    ///
    /// Therefore, timestamp values with a non-empty timezone correspond to
    /// physical points in time together with some additional information about
    /// how the data was obtained and/or how to display it (the timezone).
    ///
    ///   For example, the timestamp value 0 with the timezone string "Europe/Paris"
    ///   corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
    ///   application may prefer to display it as "January 1st 1970, 01h00" in
    ///   the Europe/Paris timezone (which is the same physical point in time).
    ///
    /// One consequence is that timestamp values with a non-empty timezone
    /// can be compared and ordered directly, since they all share the same
    /// well-known point of reference (the Unix epoch).
    ///
    /// Timestamps with an unset / empty timezone
    /// -----------------------------------------
    ///
    /// If a Timestamp column has no timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.
    ///
    /// Therefore, timestamp values without a timezone cannot be meaningfully
    /// interpreted as physical points in time, but only as calendar / clock
    /// indications ("wall clock time") in an unspecified timezone.
    ///
    ///   For example, the timestamp value 0 with an empty timezone string
    ///   corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
    ///   is not enough information to interpret it as a well-defined physical
    ///   point in time.
    ///
    /// One consequence is that timestamp values without a timezone cannot
    /// be reliably compared or ordered, since they may have different points of
    /// reference.  In particular, it is *not* possible to interpret an unset
    /// or empty timezone as the same as "UTC".
    ///
    /// Conversion between timezones
    /// ----------------------------
    ///
    /// If a Timestamp column has a non-empty timezone, changing the timezone
    /// to a different non-empty value is a metadata-only operation:
    /// the timestamp values need not change as their point of reference remains
    /// the same (the Unix epoch).
    ///
    /// However, if a Timestamp column has no timezone value, changing it to a
    /// non-empty value requires to think about the desired semantics.
    /// One possibility is to assume that the original timestamp values are
    /// relative to the epoch of the timezone being set; timestamp values should
    /// then adjusted to the Unix epoch (for example, changing the timezone from
    /// empty to "Europe/Paris" would require converting the timestamp values
    /// from "Europe/Paris" to "UTC", which seems counter-intuitive but is
    /// nevertheless correct).
    ///
    /// ```
    /// # use arrow_schema::{DataType, TimeUnit};
    /// DataType::Timestamp(TimeUnit::Second, None);
    /// DataType::Timestamp(TimeUnit::Second, Some("literal".into()));
    /// DataType::Timestamp(TimeUnit::Second, Some("string".to_string().into()));
    /// ```
    ///
    /// # Timezone representation
    /// ----------------------------
    /// It is possible to use either the timezone string representation, such as "UTC", or the absolute time zone offset "+00:00".
    /// For timezones with fixed offsets, such as "UTC" or "JST", the offset representation is recommended, as it is more explicit and less ambiguous.
    ///
    /// Most arrow-rs functionalities use the absolute offset representation,
    /// such as [`PrimitiveArray::with_timezone_utc`] that applies a
    /// UTC timezone to timestamp arrays.
    ///
    /// [`PrimitiveArray::with_timezone_utc`]: https://docs.rs/arrow/latest/arrow/array/struct.PrimitiveArray.html#method.with_timezone_utc
    ///
    /// Timezone string parsing
    /// -----------------------
    /// When feature `chrono-tz` is not enabled, allowed timezone strings are fixed offsets of the form "+09:00", "-09" or "+0930".
    ///
    /// When feature `chrono-tz` is enabled, additional strings supported by [chrono_tz](https://docs.rs/chrono-tz/latest/chrono_tz/)
    /// are also allowed, which include [IANA database](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
    /// timezones.
    Timestamp(TimeUnit, Option<Arc<str>>),
    /// A signed 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date32,
    /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds.
    ///
    /// # Valid Ranges
    ///
    /// According to the Arrow specification ([Schema.fbs]), values of Date64
    /// are treated as the number of *days*, in milliseconds, since the UNIX
    /// epoch. Therefore, values of this type  must be evenly divisible by
    /// `86_400_000`, the number of milliseconds in a standard day.
    ///
    /// It is not valid to store milliseconds that do not represent an exact
    /// day. The reason for this restriction is compatibility with other
    /// language's native libraries (specifically Java), which historically
    /// lacked a dedicated date type and only supported timestamps.
    ///
    /// # Validation
    ///
    /// This library does not validate or enforce that Date64 values are evenly
    /// divisible by `86_400_000`  for performance and usability reasons. Date64
    /// values are treated similarly to `Timestamp(TimeUnit::Millisecond,
    /// None)`: values will be displayed with a time of day if the value does
    /// not represent an exact day, and arithmetic will be done at the
    /// millisecond granularity.
    ///
    /// # Recommendation
    ///
    /// Users should prefer [`Date32`] to cleanly represent the number
    /// of days, or one of the Timestamp variants to include time as part of the
    /// representation, depending on their use case.
    ///
    /// # Further Reading
    ///
    /// For more details, see [#5288](https://github.com/apache/arrow-rs/issues/5288).
    ///
    /// [`Date32`]: Self::Date32
    /// [Schema.fbs]: https://github.com/apache/arrow/blob/main/format/Schema.fbs
    Date64,
    /// A signed 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Must be either seconds or milliseconds.
    Time32(TimeUnit),
    /// A signed 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Must be either microseconds or nanoseconds.
    Time64(TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during day light savings time transitions).
    Interval(IntervalUnit),
    /// Opaque binary data of variable length.
    ///
    /// A single Binary array can store up to [`i32::MAX`] bytes
    /// of binary data in total.
    Binary,
    /// Opaque binary data of fixed size.
    ///
    /// Enum parameter specifies the number of bytes per value, defined by the
    /// [`byteWidth` field] in the Arrow Spec
    ///
    /// [`byteWidth` field]: https://github.com/apache/arrow/blob/2a89d03bbefd620b42126b8e00f8ae57e99cd638/format/Schema.fbs#L211
    FixedSizeBinary(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    ///
    /// A single LargeBinary array can store up to [`i64::MAX`] bytes
    /// of binary data in total.
    LargeBinary,
    /// Opaque binary data of variable length.
    ///
    /// Logically the same as [`Binary`], but the internal representation uses a view
    /// struct that contains the string length and either the string's entire data
    /// inline (for small strings) or an inlined prefix, an index of another buffer,
    /// and an offset pointing to a slice in that buffer (for non-small strings).
    ///
    /// [`Binary`]: Self::Binary
    BinaryView,
    /// A variable-length string in Unicode with UTF-8 encoding.
    ///
    /// A single Utf8 array can store up to [`i32::MAX`] bytes
    /// of string data in total.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    ///
    /// A single LargeUtf8 array can store up to [`i64::MAX`] bytes
    /// of string data in total.
    LargeUtf8,
    /// A variable-length string in Unicode with UTF-8 encoding
    ///
    /// Logically the same as [`Utf8`], but the internal representation uses a view
    /// struct that contains the string length and either the string's entire data
    /// inline (for small strings) or an inlined prefix, an index of another buffer,
    /// and an offset pointing to a slice in that buffer (for non-small strings).
    ///
    /// [`Utf8`]: Self::Utf8
    Utf8View,
    /// A list of some logical data type with variable length.
    ///
    /// A single List array can store up to [`i32::MAX`] elements in total.
    List(FieldRef),
    /// A list of some logical data type with variable length.
    ///
    /// Logically the same as [`List`], but the internal representation differs in how child
    /// data is referenced, allowing flexibility in how data is layed out.
    ///
    /// [`List`]: Self::List
    ListView(FieldRef),
    /// A list of some logical data type with fixed length.
    FixedSizeList(FieldRef, i32),
    /// A list of some logical data type with variable length and 64-bit offsets.
    ///
    /// A single LargeList array can store up to [`i64::MAX`] elements in total.
    LargeList(FieldRef),
    /// A list of some logical data type with variable length and 64-bit offsets.
    ///
    /// Logically the same as [`LargeList`], but the internal representation differs in how child
    /// data is referenced, allowing flexibility in how data is layed out.
    ///
    /// [`LargeList`]: Self::LargeList
    LargeListView(FieldRef),
    /// A nested datatype that contains a number of sub-fields.
    Struct(Fields),
    /// A nested datatype that can represent slots of differing types. Components:
    ///
    /// 1. [`UnionFields`]
    /// 2. The type of union (Sparse or Dense)
    Union(UnionFields, UnionMode),
    /// A dictionary encoded array (`key_type`, `value_type`), where
    /// each array element is an index of `key_type` into an
    /// associated dictionary of `value_type`.
    ///
    /// Dictionary arrays are used to store columns of `value_type`
    /// that contain many repeated values using less memory, but with
    /// a higher CPU overhead for some operations.
    ///
    /// This type mostly used to represent low cardinality string
    /// arrays or a limited set of primitive types as integers.
    Dictionary(Box<DataType>, Box<DataType>),
    /// Exact 32-bit width decimal value with precision and scale
    ///
    /// * precision is the maximum number of digits in the unscaled value
    /// * scale controls the position of the decimal point
    ///
    /// The represented value is the unscaled integer multiplied by 10^{-scale}.
    /// For example, the unscaled value 12345 with precision 5 and scale 2
    /// represents 123.45.
    ///
    /// Scale can also be negative. For example, the unscaled value 12 with
    /// precision 2 and scale -3 represents 12000.
    Decimal32(u8, i8),
    /// Exact 64-bit width decimal value with precision and scale
    ///
    /// * precision is the maximum number of digits in the unscaled value
    /// * scale controls the position of the decimal point
    ///
    /// The represented value is the unscaled integer multiplied by 10^{-scale}.
    /// For example, the unscaled value 12345 with precision 5 and scale 2
    /// represents 123.45.
    ///
    /// Scale can also be negative. For example, the unscaled value 12 with
    /// precision 2 and scale -3 represents 12000.
    Decimal64(u8, i8),
    /// Exact 128-bit width decimal value with precision and scale
    ///
    /// * precision is the maximum number of digits in the unscaled value
    /// * scale controls the position of the decimal point
    ///
    /// The represented value is the unscaled integer multiplied by 10^{-scale}.
    /// For example, the unscaled value 12345 with precision 5 and scale 2
    /// represents 123.45.
    ///
    /// Scale can also be negative. For example, the unscaled value 12 with
    /// precision 2 and scale -3 represents 12000.
    Decimal128(u8, i8),
    /// Exact 256-bit width decimal value with precision and scale
    ///
    /// * precision is the maximum number of digits in the unscaled value
    /// * scale controls the position of the decimal point
    ///
    /// The represented value is the unscaled integer multiplied by 10^{-scale}.
    /// For example, the unscaled value 12345 with precision 5 and scale 2
    /// represents 123.45.
    ///
    /// Scale can also be negative. For example, the unscaled value 12 with
    /// precision 2 and scale -3 represents 12000.
    Decimal256(u8, i8),
    /// A Map is a logical nested type that is represented as
    ///
    /// `List<entries: Struct<key: K, value: V>>`
    ///
    /// The keys and values are each respectively contiguous.
    /// The key and value types are not constrained, but keys should be
    /// hashable and unique.
    /// Whether the keys are sorted can be set in the `bool` after the `Field`.
    ///
    /// In a field with Map type, the field has a child Struct field, which then
    /// has two children: key type and the second the value type. The names of the
    /// child fields may be respectively "entries", "key", and "value", but this is
    /// not enforced.
    Map(FieldRef, bool),
    /// A run-end encoding (REE) is a variation of run-length encoding (RLE). These
    /// encodings are well-suited for representing data containing sequences of the
    /// same value, called runs. Each run is represented as a value and an integer giving
    /// the index in the array where the run ends.
    ///
    /// A run-end encoded array has no buffers by itself, but has two child arrays. The
    /// first child array, called the run ends array, holds either 16, 32, or 64-bit
    /// signed integers. The actual values of each run are held in the second child array.
    ///
    /// These child arrays are prescribed the standard names of "run_ends" and "values"
    /// respectively.
    RunEndEncoded(FieldRef, FieldRef),
}

#[derive(Clone)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    #[deprecated(
        since = "54.0.0",
        note = "The ability to preserve dictionary IDs will be removed. With it, all fields related to it."
    )]
    dict_id: i64,
    dict_is_ordered: bool,
    /// A map of key-value pairs containing additional custom meta data.
    metadata: HashMap<String, String>,
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #![expect(deprecated)] // Must still print dict_id, if set
        let Self {
            name,
            data_type,
            nullable,
            dict_id,
            dict_is_ordered,
            metadata,
        } = self;

        let mut s = f.debug_struct("Field");

        if name != "item" {
            // Keep it short when debug-formatting `DataType::List`
            s.field("name", name);
        }

        s.field("data_type", data_type);

        if *nullable {
            s.field("nullable", nullable);
        }

        if *dict_id != 0 {
            s.field("dict_id", dict_id);
        }

        if *dict_is_ordered {
            s.field("dict_is_ordered", dict_is_ordered);
        }

        if !metadata.is_empty() {
            s.field("metadata", metadata);
        }
        s.finish()
    }
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.data_type == other.data_type
            && self.nullable == other.nullable
            && self.metadata == other.metadata
    }
}

impl Eq for Field {}

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Field {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name
            .cmp(other.name())
            .then_with(|| self.data_type.cmp(other.data_type()))
            .then_with(|| self.nullable.cmp(&other.nullable))
            .then_with(|| {
                // ensure deterministic key order
                let mut keys: Vec<&String> =
                    self.metadata.keys().chain(other.metadata.keys()).collect();
                keys.sort();
                for k in keys {
                    match (self.metadata.get(k), other.metadata.get(k)) {
                        (None, None) => {}
                        (Some(_), None) => {
                            return Ordering::Less;
                        }
                        (None, Some(_)) => {
                            return Ordering::Greater;
                        }
                        (Some(v1), Some(v2)) => match v1.cmp(v2) {
                            Ordering::Equal => {}
                            other => {
                                return other;
                            }
                        },
                    }
                }

                Ordering::Equal
            })
    }
}

impl Hash for Field {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
        self.nullable.hash(state);

        // ensure deterministic key order
        let mut keys: Vec<&String> = self.metadata.keys().collect();
        keys.sort();
        for k in keys {
            k.hash(state);
            self.metadata.get(k).expect("key valid").hash(state);
        }
    }
}


#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Fields(Arc<[FieldRef]>);
impl std::ops::Deref for Fields {
    type Target = [FieldRef];
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl<'a> IntoIterator for &'a Fields {
    type Item = &'a FieldRef;
    type IntoIter = std::slice::Iter<'a, FieldRef>;
    fn into_iter(self) -> Self::IntoIter { self.0.iter() }
}
impl std::fmt::Debug for Fields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.as_ref().fmt(f) }
}
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnionFields(Arc<[(i8, FieldRef)]>);
impl std::fmt::Debug for UnionFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.as_ref().fmt(f) }
}
impl UnionFields {
    fn len(&self) -> usize { self.0.len() }
    fn is_empty(&self) -> bool { self.0.is_empty() }
    fn iter(&self) -> impl Iterator<Item=(i8, &FieldRef)> { self.0.iter().map(|(id, f)| (*id, f)) }
    fn size(&self) -> usize { self.iter().map(|(_, f)| f.size() + std::mem::size_of::<(i8, FieldRef)>()).sum() }
}


impl Field {
fn name(&self) -> &String { &self.name }
fn data_type(&self) -> &DataType { &self.data_type }
fn is_nullable(&self) -> bool { self.nullable }
fn metadata(&self) -> &HashMap<String, String> { &self.metadata }
pub fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.data_type)
            + self.data_type.size()
            + self.name.capacity()
            + (std::mem::size_of::<(String, String)>() * self.metadata.capacity())
            + self
                .metadata
                .iter()
                .map(|(k, v)| k.capacity() + v.capacity())
                .sum::<usize>()
    }
pub fn contains(&self, other: &Field) -> bool {
        self.name == other.name
        && self.data_type.contains(&other.data_type)
        && self.dict_is_ordered == other.dict_is_ordered
        // self need to be nullable or both of them are not nullable
        && (self.nullable || !other.nullable)
        // make sure self.metadata is a superset of other.metadata
        && other.metadata.iter().all(|(k, v1)| {
            self.metadata.get(k).map(|v2| v1 == v2).unwrap_or_default()
        })
    }
}

impl Fields {
pub fn contains(&self, other: &Fields) -> bool {
        if Arc::ptr_eq(&self.0, &other.0) {
            return true;
        }
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(a, b)| Arc::ptr_eq(a, b) || a.contains(b))
    }
fn size(&self) -> usize { self.iter().map(|f| f.size() + std::mem::size_of::<FieldRef>()).sum() }
}

impl DataType {
pub fn equals_datatype(&self, other: &DataType) -> bool {
        match (&self, other) {
            (DataType::List(a), DataType::List(b))
            | (DataType::LargeList(a), DataType::LargeList(b))
            | (DataType::ListView(a), DataType::ListView(b))
            | (DataType::LargeListView(a), DataType::LargeListView(b)) => {
                a.is_nullable() == b.is_nullable() && a.data_type().equals_datatype(b.data_type())
            }
            (DataType::FixedSizeList(a, a_size), DataType::FixedSizeList(b, b_size)) => {
                a_size == b_size
                    && a.is_nullable() == b.is_nullable()
                    && a.data_type().equals_datatype(b.data_type())
            }
            (DataType::Struct(a), DataType::Struct(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(a, b)| {
                        a.is_nullable() == b.is_nullable()
                            && a.data_type().equals_datatype(b.data_type())
                    })
            }
            (DataType::Map(a_field, a_is_sorted), DataType::Map(b_field, b_is_sorted)) => {
                a_field.is_nullable() == b_field.is_nullable()
                    && a_field.data_type().equals_datatype(b_field.data_type())
                    && a_is_sorted == b_is_sorted
            }
            (DataType::Dictionary(a_key, a_value), DataType::Dictionary(b_key, b_value)) => {
                a_key.equals_datatype(b_key) && a_value.equals_datatype(b_value)
            }
            (
                DataType::RunEndEncoded(a_run_ends, a_values),
                DataType::RunEndEncoded(b_run_ends, b_values),
            ) => {
                a_run_ends.is_nullable() == b_run_ends.is_nullable()
                    && a_run_ends
                        .data_type()
                        .equals_datatype(b_run_ends.data_type())
                    && a_values.is_nullable() == b_values.is_nullable()
                    && a_values.data_type().equals_datatype(b_values.data_type())
            }
            (
                DataType::Union(a_union_fields, a_union_mode),
                DataType::Union(b_union_fields, b_union_mode),
            ) => {
                a_union_mode == b_union_mode
                    && a_union_fields.len() == b_union_fields.len()
                    && a_union_fields.iter().all(|a| {
                        b_union_fields.iter().any(|b| {
                            a.0 == b.0
                                && a.1.is_nullable() == b.1.is_nullable()
                                && a.1.data_type().equals_datatype(b.1.data_type())
                        })
                    })
            }
            _ => self == other,
        }
    }
pub fn contains(&self, other: &DataType) -> bool {
        match (self, other) {
            (DataType::List(f1), DataType::List(f2))
            | (DataType::LargeList(f1), DataType::LargeList(f2))
            | (DataType::ListView(f1), DataType::ListView(f2))
            | (DataType::LargeListView(f1), DataType::LargeListView(f2)) => f1.contains(f2),
            (DataType::FixedSizeList(f1, s1), DataType::FixedSizeList(f2, s2)) => {
                s1 == s2 && f1.contains(f2)
            }
            (DataType::Map(f1, s1), DataType::Map(f2, s2)) => s1 == s2 && f1.contains(f2),
            (DataType::Struct(f1), DataType::Struct(f2)) => f1.contains(f2),
            (DataType::Union(f1, s1), DataType::Union(f2, s2)) => {
                s1 == s2
                    && f1
                        .iter()
                        .all(|f1| f2.iter().any(|f2| f1.0 == f2.0 && f1.1.contains(f2.1)))
            }
            (DataType::Dictionary(k1, v1), DataType::Dictionary(k2, v2)) => {
                k1.contains(k2) && v1.contains(v2)
            }
            _ => self == other,
        }
    }
pub fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + match self {
                DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Binary
                | DataType::FixedSizeBinary(_)
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => 0,
                DataType::Timestamp(_, s) => s.as_ref().map(|s| s.len()).unwrap_or_default(),
                DataType::List(field)
                | DataType::ListView(field)
                | DataType::FixedSizeList(field, _)
                | DataType::LargeList(field)
                | DataType::LargeListView(field)
                | DataType::Map(field, _) => field.size(),
                DataType::Struct(fields) => fields.size(),
                DataType::Union(fields, _) => fields.size(),
                DataType::Dictionary(dt1, dt2) => dt1.size() + dt2.size(),
                DataType::RunEndEncoded(run_ends, values) => {
                    run_ends.size() - std::mem::size_of_val(run_ends) + values.size()
                        - std::mem::size_of_val(values)
                }
            }
    }
pub fn is_nested(&self) -> bool {
        use DataType::*;
        match self {
            Dictionary(_, v) => DataType::is_nested(v.as_ref()),
            RunEndEncoded(_, v) => DataType::is_nested(v.data_type()),
            List(_)
            | FixedSizeList(_, _)
            | LargeList(_)
            | ListView(_)
            | LargeListView(_)
            | Struct(_)
            | Union(_, _)
            | Map(_, _) => true,
            _ => false,
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn format_metadata(metadata: &HashMap<String, String>) -> String {
            format!("{}", FormatMetadata(metadata))
        }

        fn format_nullability(field: &Field) -> &str {
            if field.is_nullable() { "" } else { "non-null " }
        }

        fn format_field(field: &Field) -> String {
            let name = field.name();
            let maybe_nullable = format_nullability(field);
            let data_type = field.data_type();
            let metadata_str = format_metadata(field.metadata());
            format!("{name:?}: {maybe_nullable}{data_type}{metadata_str}")
        }

        // A lot of these can still be improved a lot.
        // _Some_ of these can be parsed with `FromStr`, but not all (YET!).
        // The goal is that the formatting should always be
        // * Terse and teadable
        // * Reversible (contain all necessary information to reverse it perfectly)

        match &self {
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::Float16 => write!(f, "Float16"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Timestamp(time_unit, timezone) => {
                if let Some(timezone) = timezone {
                    write!(f, "Timestamp({time_unit}, {timezone:?})")
                } else {
                    write!(f, "Timestamp({time_unit})")
                }
            }
            Self::Date32 => write!(f, "Date32"),
            Self::Date64 => write!(f, "Date64"),
            Self::Time32(time_unit) => write!(f, "Time32({time_unit})"),
            Self::Time64(time_unit) => write!(f, "Time64({time_unit})"),
            Self::Duration(time_unit) => write!(f, "Duration({time_unit})"),
            Self::Interval(interval_unit) => write!(f, "Interval({interval_unit:?})"),
            Self::Binary => write!(f, "Binary"),
            Self::FixedSizeBinary(bytes_per_value) => {
                write!(f, "FixedSizeBinary({bytes_per_value:?})")
            }
            Self::LargeBinary => write!(f, "LargeBinary"),
            Self::BinaryView => write!(f, "BinaryView"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::LargeUtf8 => write!(f, "LargeUtf8"),
            Self::Utf8View => write!(f, "Utf8View"),
            Self::List(field)
            | Self::LargeList(field)
            | Self::ListView(field)
            | Self::LargeListView(field) => {
                let type_name = if matches!(self, Self::List(_)) {
                    "List"
                } else if matches!(self, Self::ListView(_)) {
                    "ListView"
                } else if matches!(self, Self::LargeList(_)) {
                    "LargeList"
                } else {
                    "LargeListView"
                };

                let name = field.name();
                let maybe_nullable = format_nullability(field);
                let data_type = field.data_type();
                let field_name_str = if name == "item" {
                    String::default()
                } else {
                    format!(", field: '{name}'")
                };
                let metadata_str = format_metadata(field.metadata());

                // e.g. `LargeList(non-null Uint32)
                write!(
                    f,
                    "{type_name}({maybe_nullable}{data_type}{field_name_str}{metadata_str})"
                )
            }
            Self::FixedSizeList(field, size) => {
                let name = field.name();
                let maybe_nullable = format_nullability(field);
                let data_type = field.data_type();
                let field_name_str = if name == "item" {
                    String::default()
                } else {
                    format!(", field: '{name}'")
                };
                let metadata_str = format_metadata(field.metadata());

                write!(
                    f,
                    "FixedSizeList({size} x {maybe_nullable}{data_type}{field_name_str}{metadata_str})",
                )
            }
            Self::Struct(fields) => {
                write!(f, "Struct(")?;
                if !fields.is_empty() {
                    let fields_str = fields
                        .iter()
                        .map(|field| format_field(field))
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, "{fields_str}")?;
                }
                write!(f, ")")?;
                Ok(())
            }
            Self::Union(union_fields, union_mode) => {
                write!(f, "Union({union_mode:?}")?;
                if !union_fields.is_empty() {
                    write!(f, ", ")?;
                    let fields_str = union_fields
                        .iter()
                        .map(|v| {
                            let type_id = v.0;
                            let field_str = format_field(v.1);
                            format!("{type_id:?}: ({field_str})")
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, "{fields_str}")?;
                }
                write!(f, ")")?;
                Ok(())
            }
            Self::Dictionary(data_type, data_type1) => {
                write!(f, "Dictionary({data_type}, {data_type1})")
            }
            Self::Decimal32(precision, scale) => write!(f, "Decimal32({precision}, {scale})"),
            Self::Decimal64(precision, scale) => write!(f, "Decimal64({precision}, {scale})"),
            Self::Decimal128(precision, scale) => write!(f, "Decimal128({precision}, {scale})"),
            Self::Decimal256(precision, scale) => write!(f, "Decimal256({precision}, {scale})"),
            Self::Map(field, sorted) => {
                write!(f, "Map(")?;
                let map_field_str = format_field(field);
                let keys_are_sorted = if *sorted { "sorted" } else { "unsorted" };

                write!(f, "{map_field_str}, {keys_are_sorted})")?;
                Ok(())
            }
            Self::RunEndEncoded(run_ends_field, values_field) => {
                write!(f, "RunEndEncoded(")?;
                let run_ends_str = format_field(run_ends_field);
                let values_str = format_field(values_field);

                write!(f, "{run_ends_str}, {values_str})")?;
                Ok(())
            }
        }
    }
}

struct FormatMetadata<'a>(&'a HashMap<String, String>);

impl fmt::Display for FormatMetadata<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let metadata = self.0;
        if metadata.is_empty() {
            Ok(())
        } else {
            let mut entries: Vec<(&String, &String)> = metadata.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            write!(f, ", metadata: ")?;
            f.debug_map().entries(entries).finish()
        }
    }
}
