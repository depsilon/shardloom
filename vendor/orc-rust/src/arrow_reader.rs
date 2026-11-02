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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::array_decoder::NaiveStripeDecoder;
use crate::error::{self, Result};
use crate::predicate::Predicate;
use crate::projection::ProjectionMask;
use crate::reader::memory::DecoderMemory;
use crate::reader::metadata::{read_metadata, FileMetadata};
use crate::reader::ChunkReader;
use crate::row_group_filter::evaluate_predicate;
use crate::row_selection::RowSelection;
use crate::schema::{ArrowSchemaOptions, RootDataType, TimestampPrecision};
use crate::stripe::Stripe;
use snafu::ResultExt;

const DEFAULT_BATCH_SIZE: usize = 8192;

pub struct ArrowReaderBuilder<R> {
    pub(crate) reader: R,
    pub(crate) file_metadata: Arc<FileMetadata>,
    pub(crate) batch_size: usize,
    pub(crate) projection: ProjectionMask,
    pub(crate) schema_ref: Option<SchemaRef>,
    pub(crate) file_byte_range: Option<Range<usize>>,
    pub(crate) row_selection: Option<RowSelection>,
    pub(crate) timestamp_precision: Option<TimestampPrecision>,
    pub(crate) predicate: Option<Predicate>,
    pub(crate) decoder_memory: Option<Arc<dyn DecoderMemory>>,
}

impl<R> ArrowReaderBuilder<R> {
    pub(crate) fn new(reader: R, file_metadata: Arc<FileMetadata>) -> Self {
        Self {
            reader,
            file_metadata,
            batch_size: DEFAULT_BATCH_SIZE,
            projection: ProjectionMask::all(),
            schema_ref: None,
            file_byte_range: None,
            row_selection: None,
            timestamp_precision: None,
            predicate: None,
            decoder_memory: None,
        }
    }

    pub fn file_metadata(&self) -> &FileMetadata {
        &self.file_metadata
    }

    /// Reuse parsed metadata while the caller retains the same file generation.
    /// This constructor performs no reads and does not validate source identity.
    pub fn new_with_metadata(reader: R, file_metadata: Arc<FileMetadata>) -> Self {
        Self::new(reader, file_metadata)
    }

    /// Share metadata without cloning the stripe/statistics inventory.
    pub fn shared_file_metadata(&self) -> Arc<FileMetadata> {
        Arc::clone(&self.file_metadata)
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Attach the caller's shared decoder-buffer budget. Input Bytes keep their
    /// own credits. This covers decompressed stream payloads and replacement
    /// overlap; it does not by itself account for dictionaries, Arrow arrays,
    /// parsed metadata or codec-private workspace.
    pub fn with_decoder_memory(mut self, memory: Arc<dyn DecoderMemory>) -> Self {
        self.decoder_memory = Some(memory);
        self
    }

    pub fn with_projection(mut self, projection: ProjectionMask) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema_ref = Some(schema);
        self
    }

    /// Specifies a range of file bytes that will read the strips offset within this range
    pub fn with_file_byte_range(mut self, range: Range<usize>) -> Self {
        self.file_byte_range = Some(range);
        self
    }

    /// Set a [`RowSelection`] to filter rows
    ///
    /// The [`RowSelection`] specifies which rows should be decoded from the ORC file.
    /// This can be used to skip rows that don't match predicates, reducing I/O and
    /// improving query performance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::fs::File;
    /// # use orc_rust::arrow_reader::ArrowReaderBuilder;
    /// # use orc_rust::row_selection::{RowSelection, RowSelector};
    /// let file = File::open("data.orc").unwrap();
    /// let selection = vec![
    ///     RowSelector::skip(100),
    ///     RowSelector::select(50),
    /// ].into();
    /// let reader = ArrowReaderBuilder::try_new(file)
    ///     .unwrap()
    ///     .with_row_selection(selection)
    ///     .build();
    /// ```
    pub fn with_row_selection(mut self, row_selection: RowSelection) -> Self {
        self.row_selection = Some(row_selection);
        self
    }

    /// Sets the timestamp precision for reading timestamp columns.
    ///
    /// By default, timestamps are read as Nanosecond precision.
    /// Use this method to switch to Microsecond precision if needed for compatibility.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::fs::File;
    /// # use orc_rust::arrow_reader::ArrowReaderBuilder;
    /// # use orc_rust::schema::TimestampPrecision;
    /// let file = File::open("/path/to/file.orc").unwrap();
    /// let reader = ArrowReaderBuilder::try_new(file)
    ///     .unwrap()
    ///     .with_timestamp_precision(TimestampPrecision::Microsecond)
    ///     .build();
    /// ```
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.timestamp_precision = Some(precision);
        self
    }

    /// Set a predicate for row group filtering
    ///
    /// The predicate will be evaluated against row group statistics to automatically
    /// generate a [`RowSelection`] that skips filtered row groups. This provides
    /// efficient predicate pushdown based on ORC row indexes.
    ///
    /// The predicate is evaluated lazily when each stripe is read, using the row group
    /// statistics from the stripe's index section.
    ///
    /// If both `with_predicate()` and `with_row_selection()` are called, the results
    /// are combined using logical AND (both conditions must be satisfied).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::fs::File;
    /// # use orc_rust::{ArrowReaderBuilder, Predicate, PredicateValue};
    /// let file = File::open("data.orc").unwrap();
    ///
    /// // Filter: age >= 18
    /// let predicate = Predicate::gte("age", PredicateValue::Int32(Some(18)));
    ///
    /// let reader = ArrowReaderBuilder::try_new(file)
    ///     .unwrap()
    ///     .with_predicate(predicate)
    ///     .build();
    /// ```
    ///
    /// # Notes
    ///
    /// - Predicate evaluation requires row indexes to be present in the ORC file
    /// - If row indexes are missing, the predicate is ignored (all row groups are kept)
    /// - Only primitive columns have row indexes; predicates on compound types may be limited
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Returns the currently computed schema
    ///
    /// Unless [`with_schema`](Self::with_schema) was called, this is computed dynamically
    /// based on the current projection and the underlying file format.
    pub fn schema(&self) -> SchemaRef {
        if let Some(schema) = &self.schema_ref {
            return Arc::clone(schema);
        }
        let projected_data_type = self
            .file_metadata
            .root_data_type()
            .project(&self.projection);
        let metadata = self
            .file_metadata
            .user_custom_metadata()
            .iter()
            .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).to_string()))
            .collect::<HashMap<_, _>>();
        self.schema_ref.clone().unwrap_or_else(|| {
            let options = match self.timestamp_precision {
                Some(precision) => ArrowSchemaOptions::new().with_timestamp_precision(precision),
                None => ArrowSchemaOptions::new()
                    .with_source_timestamp_units(self.file_metadata.timestamp_units()),
            };
            Arc::new(projected_data_type.create_arrow_schema_with_options(&metadata, options))
        })
    }
}

impl<R: ChunkReader> ArrowReaderBuilder<R> {
    pub fn try_new(mut reader: R) -> Result<Self> {
        let file_metadata = Arc::new(read_metadata(&mut reader)?);
        Ok(Self::new(reader, file_metadata))
    }

    pub fn build(self) -> ArrowReader<R> {
        self.build_with_credit(None)
    }

    /// Build with schema/control structures admitted before construction.
    /// Payloads and codec workspaces keep their separate lifetime-bound credits.
    /// Controlled row-index predicates are not admitted by this entry point.
    pub fn try_build(self) -> Result<ArrowReader<R>> {
        let credit = if let Some(memory) = &self.decoder_memory {
            if self.predicate.is_some() {
                return error::OutOfSpecSnafu {
                    msg: "controlled ORC row-index predicates have no admitted metadata workspace",
                }
                .fail();
            }
            let bytes = crate::reader::structure_memory::reader(
                &self.file_metadata,
                &self.projection,
                self.row_selection.as_ref(),
                std::mem::size_of::<ArrowReader<R>>(),
            )?;
            Some(memory.reserve(bytes).context(error::IoSnafu)?)
        } else {
            None
        };
        Ok(self.build_with_credit(credit))
    }

    fn build_with_credit(
        self,
        credit: Option<Box<dyn crate::reader::memory::DecoderMemoryReservation>>,
    ) -> ArrowReader<R> {
        let schema_ref = self.schema();
        let projected_data_type = self
            .file_metadata
            .root_data_type()
            .project(&self.projection);
        let projected_data_type_clone = projected_data_type.clone();
        let cursor = Cursor {
            reader: self.reader,
            file_metadata: self.file_metadata,
            projected_data_type,
            stripe_index: 0,
            file_byte_range: self.file_byte_range,
            decoder_memory: self.decoder_memory,
        };
        ArrowReader {
            cursor,
            schema_ref,
            current_stripe: None,
            batch_size: self.batch_size,
            row_selection: self.row_selection,
            predicate: self.predicate,
            projected_data_type: projected_data_type_clone,
            failed: false,
            _structure_credit: credit,
        }
    }
}

pub struct ArrowReader<R> {
    cursor: Cursor<R>,
    schema_ref: SchemaRef,
    current_stripe: Option<Box<NaiveStripeDecoder>>,
    batch_size: usize,
    row_selection: Option<RowSelection>,
    predicate: Option<Predicate>,
    projected_data_type: RootDataType,
    failed: bool,
    _structure_credit: Option<Box<dyn crate::reader::memory::DecoderMemoryReservation>>,
}

impl<R> ArrowReader<R> {
    pub fn total_row_count(&self) -> u64 {
        self.cursor.file_metadata.number_of_rows()
    }

    /// Set the positive row limit for subsequent output batches, including the
    /// active stripe. This preserves row-selection position and already held
    /// stripe streams/dictionaries; it neither reloads input nor bounds their
    /// storage or the size of an individual value.
    pub fn set_batch_size(&mut self, batch_size: usize) -> std::result::Result<(), ArrowError> {
        if batch_size == 0 {
            return Err(ArrowError::InvalidArgumentError(
                "ORC batch size must be positive".into(),
            ));
        }
        self.batch_size = batch_size;
        if let Some(stripe) = &mut self.current_stripe {
            stripe.set_batch_size(batch_size);
        }
        Ok(())
    }
}

impl<R: ChunkReader> ArrowReader<R> {
    /// Load the next stripe and prepare each selected stream's first block
    /// before choosing output row geometry. This does not consume output rows.
    /// Already prepared stripes retain their exact selection/read position.
    pub fn prepare_next_stripe(&mut self) -> Result<bool, ArrowError> {
        if self.failed {
            return Err(ArrowError::ParseError(
                "ORC reader previously failed".into(),
            ));
        }
        if self.current_stripe.is_some() {
            return Ok(true);
        }
        match self.load_next_stripe() {
            Ok(ready) => Ok(ready),
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }

    fn load_next_stripe(&mut self) -> Result<bool, ArrowError> {
        let stripe = self.cursor.next().transpose()?;
        match stripe {
            Some(stripe) => {
                let stripe_rows = stripe.number_of_rows();

                // Evaluate predicate if present
                let mut stripe_selection: Option<RowSelection> = None;
                if let Some(ref predicate) = self.predicate {
                    // Try to read row indexes for this stripe
                    match stripe.read_row_indexes(&self.cursor.file_metadata) {
                        Ok(row_index) => {
                            // Evaluate predicate against row group statistics
                            match evaluate_predicate(
                                predicate,
                                &row_index,
                                &self.projected_data_type,
                            ) {
                                Ok(row_group_filter) => {
                                    // Generate RowSelection from filter results
                                    let rows_per_group = self
                                        .cursor
                                        .file_metadata
                                        .row_index_stride()
                                        .unwrap_or(10_000);
                                    stripe_selection = Some(RowSelection::from_row_group_filter(
                                        &row_group_filter,
                                        rows_per_group,
                                        stripe_rows,
                                    ));
                                }
                                Err(_) => {
                                    // Predicate evaluation failed (e.g., column not found)
                                    // Keep all rows (maybe)
                                    stripe_selection = Some(RowSelection::select_all(stripe_rows));
                                }
                            }
                        }
                        Err(_) => {
                            // Row indexes not available, keep all rows (maybe)
                            stripe_selection = Some(RowSelection::select_all(stripe_rows));
                        }
                    }
                }

                // Combine with existing row_selection if present
                let mut final_selection = stripe_selection;
                if let Some(ref mut existing_selection) = self.row_selection {
                    if existing_selection.row_count() > 0 {
                        let existing_for_stripe = existing_selection.split_off(stripe_rows);
                        final_selection = match final_selection {
                            Some(predicate_selection) => {
                                // Both predicate and manual selection: combine with AND
                                Some(existing_for_stripe.and_then(&predicate_selection))
                            }
                            None => Some(existing_for_stripe),
                        };
                    }
                }

                let decoder = NaiveStripeDecoder::new_with_selection(
                    stripe,
                    self.schema_ref.clone(),
                    self.batch_size,
                    final_selection,
                )?;
                self.current_stripe = Some(Box::new(decoder));
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl<R: ChunkReader> RecordBatchReader for ArrowReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }
}

impl<R: ChunkReader> Iterator for ArrowReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            match self.prepare_next_stripe() {
                Ok(false) => return None,
                Err(error) => return Some(Err(error)),
                Ok(true) => (),
            }
            let result = self
                .current_stripe
                .as_mut()
                .expect("prepared stripe")
                .next();
            match result {
                Some(Ok(batch)) => return Some(Ok(batch)),
                Some(Err(error)) => {
                    self.failed = true;
                    self.current_stripe = None;
                    return Some(Err(error.into()));
                }
                None => self.current_stripe = None,
            }
        }
    }
}

pub(crate) struct Cursor<R> {
    pub reader: R,
    pub file_metadata: Arc<FileMetadata>,
    pub projected_data_type: RootDataType,
    pub stripe_index: usize,
    pub file_byte_range: Option<Range<usize>>,
    pub decoder_memory: Option<Arc<dyn DecoderMemory>>,
}

impl<R> Cursor<R> {
    /// Metadata parsing validates physical stripe order. Seek once to a range's
    /// first stripe and stop at its end, without walking excluded inventories
    /// whenever a caller constructs a reader for one selected stripe.
    pub(crate) fn next_stripe_index(&mut self) -> Option<usize> {
        let stripes = self.file_metadata.stripe_metadatas();
        if let Some(range) = &self.file_byte_range {
            if self.stripe_index == 0 {
                self.stripe_index =
                    stripes.partition_point(|stripe| stripe.offset() < range.start as u64);
            }
            if stripes
                .get(self.stripe_index)
                .map_or(true, |stripe| stripe.offset() >= range.end as u64)
            {
                self.stripe_index = stripes.len();
                return None;
            }
        }
        stripes.get(self.stripe_index)?;
        let index = self.stripe_index;
        self.stripe_index += 1;
        Some(index)
    }
}

impl<R: ChunkReader> Iterator for Cursor<R> {
    type Item = Result<Stripe>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next_stripe_index()?;
        let info = &self.file_metadata.stripe_metadatas()[index];
        Some(Stripe::new_with_memory(
            &mut self.reader,
            &self.file_metadata,
            &self.projected_data_type,
            info,
            self.decoder_memory.clone(),
        ))
    }
}
