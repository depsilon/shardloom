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

use std::sync::Arc;

use crate::encoding::integer::RleVersion;
use crate::proto::{column_encoding::Kind as ProtoColumnKind, ColumnEncoding, StripeFooter};
use crate::reader::memory::DecoderMemoryReservation;
use crate::schema::DataType;

/// Keep parsed metadata and its caller-owned credit together through every
/// cloned column, including nested child columns that outlive a stripe handle.
#[derive(Debug)]
pub(crate) struct ColumnFooter {
    pub(crate) data: Arc<StripeFooter>,
    pub(crate) _credit: Option<Box<dyn DecoderMemoryReservation>>,
    pub(crate) structure_credit: Option<Arc<dyn DecoderMemoryReservation>>,
}

impl std::ops::Deref for ColumnFooter {
    type Target = StripeFooter;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    data: Arc<ColumnData>,
    footer: Arc<ColumnFooter>,
}

#[derive(Debug)]
struct ColumnData {
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(name: String, data_type: DataType, footer: Arc<StripeFooter>) -> Self {
        Self::new_with_footer(
            name,
            data_type,
            Arc::new(ColumnFooter {
                data: footer,
                _credit: None,
                structure_credit: None,
            }),
        )
    }

    pub(crate) fn new_with_footer(
        name: String,
        data_type: DataType,
        footer: Arc<ColumnFooter>,
    ) -> Self {
        Self {
            data: Arc::new(ColumnData { data_type, name }),
            footer,
        }
    }

    pub(crate) fn storage_bytes() -> usize {
        std::mem::size_of::<ColumnData>() + 2 * std::mem::size_of::<usize>()
    }

    pub fn dictionary_size(&self) -> usize {
        let column = self.data_type().column_index();
        self.footer.columns[column]
            .dictionary_size
            .unwrap_or_default() as usize
    }

    pub fn encoding(&self) -> ColumnEncoding {
        let column = self.data_type().column_index();
        self.footer.columns[column].clone()
    }

    pub fn rle_version(&self) -> RleVersion {
        // TODO: Validity check for this? e.g. ensure INT column isn't dictionary encoded.
        //       Or maybe check that at init time; to ensure we catch at earliest opportunity?
        match self.encoding().kind() {
            ProtoColumnKind::Direct | ProtoColumnKind::Dictionary => RleVersion::V1,
            ProtoColumnKind::DirectV2 | ProtoColumnKind::DictionaryV2 => RleVersion::V2,
        }
    }

    pub fn data_type(&self) -> &DataType {
        &self.data.data_type
    }

    pub fn name(&self) -> &str {
        &self.data.name
    }

    pub fn column_id(&self) -> u32 {
        self.data_type().column_index() as u32
    }

    pub fn children(&self) -> Vec<Column> {
        match self.data_type() {
            DataType::Boolean { .. }
            | DataType::Byte { .. }
            | DataType::Short { .. }
            | DataType::Int { .. }
            | DataType::Long { .. }
            | DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::String { .. }
            | DataType::Varchar { .. }
            | DataType::Char { .. }
            | DataType::Binary { .. }
            | DataType::Decimal { .. }
            | DataType::Timestamp { .. }
            | DataType::TimestampWithLocalTimezone { .. }
            | DataType::Date { .. } => vec![],
            DataType::Struct { children, .. } => children
                .iter()
                .map(|col| {
                    Self::new_with_footer(
                        col.name().to_string(),
                        col.data_type().clone(),
                        self.footer.clone(),
                    )
                })
                .collect(),
            DataType::List { child, .. } => {
                vec![Self::new_with_footer(
                    "item".to_string(),
                    *child.clone(),
                    self.footer.clone(),
                )]
            }
            DataType::Map { key, value, .. } => {
                vec![
                    Self::new_with_footer("key".to_string(), *key.clone(), self.footer.clone()),
                    Self::new_with_footer("value".to_string(), *value.clone(), self.footer.clone()),
                ]
            }
            DataType::Union { variants, .. } => {
                // TODO: might need corrections
                variants
                    .iter()
                    .enumerate()
                    .map(|(index, data_type)| {
                        Self::new_with_footer(
                            format!("{index}"),
                            data_type.clone(),
                            self.footer.clone(),
                        )
                    })
                    .collect()
            }
        }
    }
}
