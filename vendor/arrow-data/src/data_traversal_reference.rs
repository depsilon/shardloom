// Test-only pre-traversal reference from the pinned Apache-2.0 provider.
// Source spans and the only permitted name adaptation are in the manifest.
#![allow(dead_code, missing_docs)]
use super::*;

impl ArrayData {
    #[rustfmt::skip]
    pub fn reference_get_buffer_memory_size(&self) -> usize {
        let mut size = 0;
        for buffer in &self.buffers {
            size += buffer.capacity();
        }
        if let Some(bitmap) = &self.nulls {
            size += bitmap.buffer().capacity()
        }
        for child in &self.child_data {
            size += child.reference_get_buffer_memory_size();
        }
        size
    }

    #[rustfmt::skip]
    pub fn reference_get_slice_memory_size(&self) -> Result<usize, ArrowError> {
        let mut result: usize = 0;
        let layout = layout(&self.data_type);

        for spec in layout.buffers.iter() {
            match spec {
                BufferSpec::FixedWidth { byte_width, .. } => {
                    // Offset buffers contain len+1 elements: one boundary per element
                    // plus a final boundary marking the end of the last element.
                    let len = match self.data_type {
                        DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::Map(_, _) => self.len + 1,
                        _ => self.len,
                    };
                    let buffer_size = len.checked_mul(*byte_width).ok_or_else(|| {
                        ArrowError::ComputeError(
                            "Integer overflow computing buffer size".to_string(),
                        )
                    })?;
                    result += buffer_size;
                }
                BufferSpec::VariableWidth => {
                    let buffer_len = match self.data_type {
                        DataType::Utf8 | DataType::Binary => {
                            let offsets = self.typed_offsets::<i32>()?;
                            (offsets[self.len] - offsets[0]) as usize
                        }
                        DataType::LargeUtf8 | DataType::LargeBinary => {
                            let offsets = self.typed_offsets::<i64>()?;
                            (offsets[self.len] - offsets[0]) as usize
                        }
                        _ => {
                            return Err(ArrowError::NotYetImplemented(format!(
                                "Invalid data type for VariableWidth buffer. Expected Utf8, LargeUtf8, Binary or LargeBinary. Got {}",
                                self.data_type
                            )));
                        }
                    };
                    result += buffer_len;
                }
                BufferSpec::BitMap => {
                    let buffer_size = bit_util::ceil(self.len, 8);
                    result += buffer_size;
                }
                BufferSpec::AlwaysNull => {
                    // Nothing to do
                }
            }
        }

        if self.nulls().is_some() {
            result += bit_util::ceil(self.len, 8);
        }

        for child in &self.child_data {
            result += child.reference_get_slice_memory_size()?;
        }
        Ok(result)
    }

    #[rustfmt::skip]
    pub fn reference_get_array_memory_size(&self) -> usize {
        let mut size = mem::size_of_val(self);

        // Calculate rest of the fields top down which contain actual data
        for buffer in &self.buffers {
            size += mem::size_of::<Buffer>();
            size += buffer.capacity();
        }
        if let Some(nulls) = &self.nulls {
            size += nulls.buffer().capacity();
        }
        for child in &self.child_data {
            size += child.reference_get_array_memory_size();
        }

        size
    }

    #[rustfmt::skip]
    pub fn reference_slice(&self, offset: usize, length: usize) -> ArrayData {
        let end = offset
            .checked_add(length)
            .expect("offset + length overflow");
        assert!(end <= self.len());

        if let DataType::Struct(_) = self.data_type() {
            // Structs have no value buffer of their own. Apply the complete
            // parent offset to the children, then reset it on this descriptor.
            // Retaining it as well would slice nested structs twice when an
            // ArrayData consumer constructs the typed child arrays.
            let new_offset = self.offset + offset;
            ArrayData {
                data_type: self.data_type().clone(),
                len: length,
                offset: 0,
                buffers: self.buffers.clone(),
                // Slice child data, to propagate offsets down to them
                child_data: self
                    .child_data()
                    .iter()
                    .map(|data| data.reference_slice(new_offset, length))
                    .collect(),
                nulls: self.nulls.as_ref().map(|x| x.slice(offset, length)),
            }
        } else {
            let mut new_data = self.clone();

            new_data.len = length;
            new_data.offset = offset + self.offset;
            new_data.nulls = self.nulls.as_ref().map(|x| x.slice(offset, length));

            new_data
        }
    }

    #[rustfmt::skip]
    pub fn reference_align_buffers(&mut self) {
        let layout = layout(&self.data_type);
        for (buffer, spec) in self.buffers.iter_mut().zip(&layout.buffers) {
            if let BufferSpec::FixedWidth { alignment, .. } = spec {
                if buffer.as_ptr().align_offset(*alignment) != 0 {
                    *buffer = Buffer::from_slice_ref(buffer.as_ref());
                }
            }
        }
        // align children data recursively
        for data in self.child_data.iter_mut() {
            data.reference_align_buffers()
        }
    }

    #[rustfmt::skip]
    pub fn reference_validate(&self) -> Result<(), ArrowError> {
        // Need at least this much space in each buffer
        let len_plus_offset = checked_len_plus_offset(&self.data_type, self.len, self.offset)?;

        // Check that the data layout conforms to the spec
        let layout = layout(&self.data_type);

        if !layout.can_contain_null_mask && self.nulls.is_some() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Arrays of type {:?} cannot contain a null bitmask",
                self.data_type,
            )));
        }

        // Check data buffers length for view types and other types
        if self.buffers.len() < layout.buffers.len()
            || (!layout.variadic && self.buffers.len() != layout.buffers.len())
        {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Expected {} buffers in array of type {:?}, got {}",
                layout.buffers.len(),
                self.data_type,
                self.buffers.len(),
            )));
        }

        for (i, (buffer, spec)) in self.buffers.iter().zip(layout.buffers.iter()).enumerate() {
            match spec {
                BufferSpec::FixedWidth {
                    byte_width,
                    alignment,
                } => {
                    let min_buffer_size = len_plus_offset.saturating_mul(*byte_width);

                    if buffer.len() < min_buffer_size {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Need at least {} bytes in buffers[{}] in array of type {:?}, but got {}",
                            min_buffer_size,
                            i,
                            self.data_type,
                            buffer.len()
                        )));
                    }

                    let align_offset = buffer.as_ptr().align_offset(*alignment);
                    if align_offset != 0 {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Misaligned buffers[{i}] in array of type {:?}, offset from expected alignment of {alignment} by {}",
                            self.data_type,
                            align_offset.min(alignment - align_offset)
                        )));
                    }
                }
                BufferSpec::VariableWidth => {
                    // not cheap to validate (need to look at the
                    // data). Partially checked in validate_offsets
                    // called below. Can check with `validate_full`
                }
                BufferSpec::BitMap => {
                    let min_buffer_size = bit_util::ceil(len_plus_offset, 8);
                    if buffer.len() < min_buffer_size {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Need at least {} bytes for bitmap in buffers[{}] in array of type {:?}, but got {}",
                            min_buffer_size,
                            i,
                            self.data_type,
                            buffer.len()
                        )));
                    }
                }
                BufferSpec::AlwaysNull => {
                    // Nothing to validate
                }
            }
        }

        // check null bit buffer size
        if let Some(nulls) = self.nulls() {
            if nulls.null_count() > self.len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "null_count {} for an array exceeds length of {} elements",
                    nulls.null_count(),
                    self.len
                )));
            }

            let actual_len = nulls.validity().len();
            let needed_len = bit_util::ceil(len_plus_offset, 8);
            if actual_len < needed_len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "null_bit_buffer size too small. got {actual_len} needed {needed_len}",
                )));
            }

            if nulls.len() != self.len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "null buffer incorrect size. got {} expected {}",
                    nulls.len(),
                    self.len
                )));
            }
        }

        self.reference_validate_child_data()?;

        // Additional Type specific checks
        match &self.data_type {
            DataType::Utf8 | DataType::Binary => {
                self.validate_offsets::<i32>(self.buffers[1].len())?;
            }
            DataType::LargeUtf8 | DataType::LargeBinary => {
                self.validate_offsets::<i64>(self.buffers[1].len())?;
            }
            DataType::Dictionary(key_type, _value_type) => {
                // At the moment, constructing a DictionaryArray will also check this
                if !DataType::is_dictionary_key_type(key_type) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Dictionary key type must be integer, but was {key_type}"
                    )));
                }
            }
            DataType::RunEndEncoded(run_ends_type, _) => {
                if run_ends_type.is_nullable() {
                    return Err(ArrowError::InvalidArgumentError(
                        "The nullable should be set to false for the field defining run_ends array.".to_string()
                    ));
                }
                if !DataType::is_run_ends_type(run_ends_type.data_type()) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "RunArray run_ends types must be Int16, Int32 or Int64, but was {}",
                        run_ends_type.data_type()
                    )));
                }
            }
            DataType::Map(f, _) if f.is_nullable() => {
                return Err(ArrowError::InvalidArgumentError(
                    "The nullable should be set to false for the map entries field.".to_string(),
                ));
            }
            _ => {}
        };

        Ok(())
    }

    #[rustfmt::skip]
    fn reference_validate_child_data(&self) -> Result<(), ArrowError> {
        match &self.data_type {
            DataType::List(field) => {
                let values_data = self.reference_get_single_valid_child_data(field.data_type())?;
                self.validate_offsets::<i32>(values_data.len)?;
                Ok(())
            }
            DataType::LargeList(field) => {
                let values_data = self.reference_get_single_valid_child_data(field.data_type())?;
                self.validate_offsets::<i64>(values_data.len)?;
                Ok(())
            }
            DataType::Map(field, _) => {
                let DataType::Struct(entries_fields) = field.data_type() else {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Map field should be a entries struct data type, got {:?} instead",
                        field.data_type()
                    )));
                };
                if entries_fields.len() != 2 {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Map entries data type should be a struct containing 2 fields, got {} fields",
                        entries_fields.len()
                    )));
                }

                // Key field
                if entries_fields[0].is_nullable() {
                    return Err(ArrowError::InvalidArgumentError(
                        "Map key field must not be nullable".to_string(),
                    ));
                }
                let values_data = self.reference_get_single_valid_child_data(field.data_type())?;
                self.validate_offsets::<i32>(values_data.len)?;
                Ok(())
            }
            DataType::ListView(field) => {
                let values_data = self.reference_get_single_valid_child_data(field.data_type())?;
                self.validate_offsets_and_sizes::<i32>(values_data.len)?;
                Ok(())
            }
            DataType::LargeListView(field) => {
                let values_data = self.reference_get_single_valid_child_data(field.data_type())?;
                self.validate_offsets_and_sizes::<i64>(values_data.len)?;
                Ok(())
            }
            DataType::FixedSizeList(field, list_size) => {
                let values_data = self.reference_get_single_valid_child_data(field.data_type())?;

                let list_size: usize = (*list_size).try_into().map_err(|_| {
                    ArrowError::InvalidArgumentError(format!(
                        "{} has a negative list_size {}",
                        self.data_type, list_size
                    ))
                })?;

                let expected_values_len = self.len
                    .checked_mul(list_size)
                    .expect("integer overflow computing expected number of expected values in FixedListSize");

                if values_data.len < expected_values_len {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Values length {} is less than the length ({}) multiplied by the value size ({}) for {}",
                        values_data.len, self.len, list_size, self.data_type
                    )));
                }

                Ok(())
            }
            DataType::Struct(fields) => {
                self.reference_validate_num_child_data(fields.len())?;
                for (i, field) in fields.iter().enumerate() {
                    let field_data = self.reference_get_valid_child_data(i, field.data_type())?;

                    // Ensure child field has sufficient size
                    if field_data.len < self.len {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "{} child array #{} for field {} has length smaller than expected for struct array ({} < {})",
                            self.data_type,
                            i,
                            field.name(),
                            field_data.len,
                            self.len
                        )));
                    }
                }
                Ok(())
            }
            DataType::RunEndEncoded(run_ends_field, values_field) => {
                self.reference_validate_num_child_data(2)?;
                let run_ends_data = self.reference_get_valid_child_data(0, run_ends_field.data_type())?;
                let values_data = self.reference_get_valid_child_data(1, values_field.data_type())?;
                if run_ends_data.len != values_data.len {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "The run_ends array length should be the same as values array length. Run_ends array length is {}, values array length is {}",
                        run_ends_data.len, values_data.len
                    )));
                }
                if run_ends_data.nulls.is_some() {
                    return Err(ArrowError::InvalidArgumentError(
                        "Found null values in run_ends array. The run_ends array should not have null values.".to_string(),
                    ));
                }
                Ok(())
            }
            DataType::Union(fields, mode) => {
                self.reference_validate_num_child_data(fields.len())?;

                for (i, (_, field)) in fields.iter().enumerate() {
                    let field_data = self.reference_get_valid_child_data(i, field.data_type())?;

                    if mode == &UnionMode::Sparse {
                        let len_plus_offset =
                            checked_len_plus_offset(&self.data_type, self.len, self.offset)?;
                        if field_data.len < len_plus_offset {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Sparse union child array #{} has length smaller than expected for union array ({} < {})",
                                i, field_data.len, len_plus_offset
                            )));
                        }
                    }
                }
                Ok(())
            }
            DataType::Dictionary(_key_type, value_type) => {
                self.reference_get_single_valid_child_data(value_type)?;
                Ok(())
            }
            _ => {
                // other types do not have child data
                if !self.child_data.is_empty() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Expected no child arrays for type {} but got {}",
                        self.data_type,
                        self.child_data.len()
                    )));
                }
                Ok(())
            }
        }
    }

    #[rustfmt::skip]
    fn reference_get_single_valid_child_data(
        &self,
        expected_type: &DataType,
    ) -> Result<&ArrayData, ArrowError> {
        self.reference_validate_num_child_data(1)?;
        self.reference_get_valid_child_data(0, expected_type)
    }

    #[rustfmt::skip]
    fn reference_validate_num_child_data(&self, expected_len: usize) -> Result<(), ArrowError> {
        if self.child_data.len() != expected_len {
            Err(ArrowError::InvalidArgumentError(format!(
                "Value data for {} should contain {} child data array(s), had {}",
                self.data_type,
                expected_len,
                self.child_data.len()
            )))
        } else {
            Ok(())
        }
    }

    #[rustfmt::skip]
    fn reference_get_valid_child_data(
        &self,
        i: usize,
        expected_type: &DataType,
    ) -> Result<&ArrayData, ArrowError> {
        let values_data = self.child_data.get(i).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "{} did not have enough child arrays. Expected at least {} but had only {}",
                self.data_type,
                i + 1,
                self.child_data.len()
            ))
        })?;

        if expected_type != &values_data.data_type {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Child type mismatch for {}. Expected {} but child data had {}",
                self.data_type, expected_type, values_data.data_type
            )));
        }

        values_data.reference_validate()?;
        Ok(values_data)
    }

    #[rustfmt::skip]
    pub fn reference_validate_data(&self) -> Result<(), ArrowError> {
        self.reference_validate()?;

        self.validate_nulls()?;
        self.validate_values()?;
        Ok(())
    }

    #[rustfmt::skip]
    pub fn reference_validate_full(&self) -> Result<(), ArrowError> {
        self.reference_validate_data()?;
        // validate all children recursively
        self.child_data
            .iter()
            .enumerate()
            .try_for_each(|(i, child_data)| {
                child_data.reference_validate_full().map_err(|e| {
                    ArrowError::InvalidArgumentError(format!(
                        "{} child #{} invalid: {}",
                        self.data_type, i, e
                    ))
                })
            })?;
        Ok(())
    }
}
