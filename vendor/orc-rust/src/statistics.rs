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

use crate::{error, proto};

/// Contains statistics for a specific column, for the entire file
/// or for a specific stripe.
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    number_of_values: Option<u64>,
    /// Use aid in 'IS NULL' predicates
    has_null: Option<bool>,
    type_statistics: Option<TypeStatistics>,
}

impl ColumnStatistics {
    pub fn number_of_values(&self) -> u64 {
        self.number_of_values.unwrap_or_default()
    }

    pub fn has_null(&self) -> bool {
        self.has_null.unwrap_or_default()
    }

    /// Present non-null count; absence must not prove an empty column.
    pub fn number_of_values_opt(&self) -> Option<u64> {
        self.number_of_values
    }

    /// Present null-presence fact; absence must not prove a null-free column.
    pub fn has_null_opt(&self) -> Option<bool> {
        self.has_null
    }

    pub fn type_statistics(&self) -> Option<&TypeStatistics> {
        self.type_statistics.as_ref()
    }
}

#[derive(Debug, Clone)]
pub enum TypeStatistics {
    /// For TinyInt, SmallInt, Int and BigInt
    Integer {
        min: i64,
        max: i64,
        /// If sum overflows then recorded as None
        sum: Option<i64>,
    },
    /// For Float and Double
    Double {
        min: f64,
        max: f64,
        /// If sum overflows then recorded as None
        sum: Option<f64>,
    },
    String {
        lower_bound: String,
        upper_bound: String,
        /// Total length of all strings
        sum: i64,
        /// If true, 'min' is an exact minimum. If false, it is a lower bound.
        is_exact_min: bool,
        /// If true, 'max' is an exact maximum. If false, it is an upper bound.
        is_exact_max: bool,
    },
    /// For Boolean
    Bucket { true_count: u64 },
    Decimal {
        // TODO: use our own decimal type?
        min: String,
        max: String,
        sum: String,
    },
    Date {
        /// Days since epoch
        min: i32,
        max: i32,
    },
    Binary {
        // Total number of bytes across all values
        sum: i64,
    },
    Timestamp {
        /// Milliseconds since epoch
        /// These were used before ORC-135
        /// Where local timezone offset was included
        min: i64,
        max: i64,
        /// Milliseconds since UNIX epoch
        min_utc: i64,
        max_utc: i64,
    },
    Collection {
        min_children: u64,
        max_children: u64,
        total_children: u64,
    },
}

impl TryFrom<&proto::ColumnStatistics> for ColumnStatistics {
    type Error = error::OrcError;

    fn try_from(value: &proto::ColumnStatistics) -> Result<Self, Self::Error> {
        let alternatives = [
            value.int_statistics.is_some(),
            value.double_statistics.is_some(),
            value.string_statistics.is_some(),
            value.bucket_statistics.is_some(),
            value.decimal_statistics.is_some(),
            value.date_statistics.is_some(),
            value.binary_statistics.is_some(),
            value.timestamp_statistics.is_some(),
            value.collection_statistics.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        let type_statistics = if value.number_of_values == Some(0) || alternatives != 1 {
            None
        } else if let Some(stats) = &value.int_statistics {
            stats
                .minimum
                .zip(stats.maximum)
                .filter(|(min, max)| min <= max)
                .map(|(min, max)| TypeStatistics::Integer {
                    min,
                    max,
                    sum: stats.sum,
                })
        } else if let Some(stats) = &value.double_statistics {
            stats
                .minimum
                .zip(stats.maximum)
                .filter(|(min, max)| min <= max)
                .map(|(min, max)| TypeStatistics::Double {
                    min,
                    max,
                    sum: stats.sum,
                })
        } else if let Some(stats) = &value.string_statistics {
            let lower_bound = stats
                .minimum
                .as_deref()
                .map(|s| (s, true))
                .or_else(|| stats.lower_bound.as_deref().map(|s| (s, false)));
            let upper_bound = stats
                .maximum
                .as_deref()
                .map(|s| (s, true))
                .or_else(|| stats.upper_bound.as_deref().map(|s| (s, false)));
            lower_bound
                .zip(upper_bound)
                .filter(|((min, _), (max, _))| min <= max)
                .map(
                    |((lower_bound, is_exact_min), (upper_bound, is_exact_max))| {
                        TypeStatistics::String {
                            lower_bound: lower_bound.to_owned(),
                            upper_bound: upper_bound.to_owned(),
                            sum: stats.sum(),
                            is_exact_min,
                            is_exact_max,
                        }
                    },
                )
        } else if let Some(stats) = &value.bucket_statistics {
            match (stats.count.as_slice(), value.number_of_values) {
                ([true_count], Some(total)) if *true_count <= total => {
                    Some(TypeStatistics::Bucket {
                        true_count: *true_count,
                    })
                }
                _ => None,
            }
        } else if let Some(stats) = &value.decimal_statistics {
            Some(TypeStatistics::Decimal {
                min: stats.minimum().to_owned(),
                max: stats.maximum().to_owned(),
                sum: stats.sum().to_owned(),
            })
        } else if let Some(stats) = &value.date_statistics {
            Some(TypeStatistics::Date {
                min: stats.minimum(),
                max: stats.maximum(),
            })
        } else if let Some(stats) = &value.binary_statistics {
            Some(TypeStatistics::Binary { sum: stats.sum() })
        } else if let Some(stats) = &value.timestamp_statistics {
            Some(TypeStatistics::Timestamp {
                min: stats.minimum(),
                max: stats.maximum(),
                min_utc: stats.minimum_utc(),
                max_utc: stats.maximum_utc(),
            })
        } else {
            value
                .collection_statistics
                .as_ref()
                .map(|stats| TypeStatistics::Collection {
                    min_children: stats.min_children(),
                    max_children: stats.max_children(),
                    total_children: stats.total_children(),
                })
        };
        Ok(Self {
            number_of_values: value.number_of_values,
            has_null: value.has_null,
            type_statistics,
        })
    }
}

#[cfg(test)]
mod presence_tests {
    use super::*;

    #[test]
    fn missing_facts_and_incomplete_or_inverted_integer_extrema_remain_unknown() {
        let raw = proto::ColumnStatistics::default();
        let stats = ColumnStatistics::try_from(&raw).unwrap();
        assert_eq!(stats.number_of_values_opt(), None);
        assert_eq!(stats.has_null_opt(), None);
        for (min, max) in [(None, Some(4)), (Some(0), None), (Some(5), Some(4))] {
            let raw = proto::ColumnStatistics {
                number_of_values: Some(2),
                int_statistics: Some(proto::IntegerStatistics {
                    minimum: min,
                    maximum: max,
                    sum: None,
                }),
                ..Default::default()
            };
            assert!(ColumnStatistics::try_from(&raw)
                .unwrap()
                .type_statistics()
                .is_none());
        }
        let raw = proto::ColumnStatistics {
            number_of_values: Some(0),
            has_null: Some(true),
            ..Default::default()
        };
        let stats = ColumnStatistics::try_from(&raw).unwrap();
        assert_eq!(stats.number_of_values_opt(), Some(0));
        assert_eq!(stats.has_null_opt(), Some(true));
    }

    #[test]
    fn malformed_boolean_buckets_and_conflicting_variants_do_not_supply_bounds() {
        for counts in [vec![], vec![3], vec![1, 1]] {
            let raw = proto::ColumnStatistics {
                number_of_values: Some(2),
                bucket_statistics: Some(proto::BucketStatistics { count: counts }),
                ..Default::default()
            };
            assert!(ColumnStatistics::try_from(&raw)
                .unwrap()
                .type_statistics()
                .is_none());
        }
        let raw = proto::ColumnStatistics {
            number_of_values: Some(2),
            int_statistics: Some(proto::IntegerStatistics {
                minimum: Some(0),
                maximum: Some(1),
                sum: None,
            }),
            bucket_statistics: Some(proto::BucketStatistics { count: vec![1] }),
            ..Default::default()
        };
        assert!(ColumnStatistics::try_from(&raw)
            .unwrap()
            .type_statistics()
            .is_none());
    }

    #[test]
    fn missing_string_bound_is_unknown_and_conservative_bounds_retain_exactness() {
        let mut raw = proto::ColumnStatistics {
            number_of_values: Some(2),
            string_statistics: Some(proto::StringStatistics {
                minimum: Some("a".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(ColumnStatistics::try_from(&raw)
            .unwrap()
            .type_statistics()
            .is_none());
        raw.string_statistics.as_mut().unwrap().upper_bound = Some("z".into());
        assert!(matches!(
            ColumnStatistics::try_from(&raw).unwrap().type_statistics(),
            Some(TypeStatistics::String {
                is_exact_min: true,
                is_exact_max: false,
                ..
            })
        ));
    }
}
