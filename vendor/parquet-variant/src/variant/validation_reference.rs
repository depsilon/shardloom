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

//! Frozen recursive validation oracle from parquet-variant 59.3.2.

use super::*;

#[rustfmt::skip]
impl<'m, 'v> Variant<'m, 'v> {
    pub(crate) fn try_new_with_metadata_reference(
        metadata: VariantMetadata<'m>,
        value: &'v [u8],
    ) -> Result<Self, ArrowError> {
        Self::try_new_with_metadata_and_shallow_validation(metadata, value)?.with_full_validation_reference()
    }

    pub(crate) fn with_full_validation_reference(self) -> Result<Self, ArrowError> {
        use Variant::*;
        match self {
            List(list) => list.with_full_validation_reference().map(List),
            Object(obj) => obj.with_full_validation_reference().map(Object),
            _ => Ok(self),
        }
    }
}
