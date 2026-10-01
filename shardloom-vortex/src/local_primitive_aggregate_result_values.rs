//! Read typed final values from the existing aggregate states. Shared by report
//! rendering and owned/streamed native output; does not execute an aggregate.

use super::{
    AggregateDistinctValue, AggregateGroupKey, AggregateNumericPairKey, AggregateValueTransform,
    CompactAggregateMeasureValue, GroupedAggregateState, GroupedAggregateStates,
    NumericPairCompactMeasures, Result, ShardLoomError, SimpleAggregateFunction,
    SimpleAggregateState, SimpleAggregateStates, TransformedDictionaryDenseGeneralState,
    result_batch::Value, simple_average_value, usize_to_u64,
};

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native aggregate result: {reason}; no fallback execution was attempted"
    ))
}

impl SimpleAggregateState {
    pub(super) fn native_result_value(&self) -> Result<Value<'_>> {
        Ok(match self.function {
            SimpleAggregateFunction::Count => Value::UInt(self.count),
            SimpleAggregateFunction::CountDistinct => {
                Value::UInt(usize_to_u64(self.distinct_values.len())?)
            }
            SimpleAggregateFunction::Sum if self.count > 0 => Value::Float(self.sum),
            SimpleAggregateFunction::Avg if self.count > 0 => {
                Value::Float(simple_average_value(self.sum, self.count))
            }
            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => Value::Null,
            SimpleAggregateFunction::Min => self.min.as_ref().map_or(Value::Null, Value::from),
            SimpleAggregateFunction::Max => self.max.as_ref().map_or(Value::Null, Value::from),
        })
    }
}

impl SimpleAggregateStates {
    pub(super) fn native_result_value(&self, column: usize) -> Result<Value<'_>> {
        let state = self
            .states
            .get(column)
            .ok_or_else(|| failed("measure column is absent"))?;
        if self.partition_distinct_completed
            && state.function == SimpleAggregateFunction::CountDistinct
        {
            Ok(Value::UInt(state.count))
        } else {
            state.native_result_value()
        }
    }
}

impl CompactAggregateMeasureValue {
    pub(super) fn native_result_value(
        &self,
        function: SimpleAggregateFunction,
    ) -> Result<Value<'static>> {
        Ok(match function {
            SimpleAggregateFunction::Count => Value::UInt(self.count),
            SimpleAggregateFunction::Sum if self.count > 0 => Value::Float(self.sum),
            SimpleAggregateFunction::Avg if self.count > 0 => {
                Value::Float(simple_average_value(self.sum, self.count))
            }
            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => Value::Null,
            _ => return Err(failed("compact measures admit COUNT/SUM/AVG only")),
        })
    }
}

impl GroupedAggregateStates<'_> {
    pub(super) fn native_numeric_pair_value(
        &self,
        key: AggregateNumericPairKey,
        measures: &NumericPairCompactMeasures,
        column: usize,
    ) -> Result<Value<'static>> {
        match column {
            0 => Ok(Value::integer(
                key.first_bits,
                key.key_kinds & AggregateNumericPairKey::FIRST_SIGNED != 0,
            )),
            1 => Ok(Value::integer(
                key.second_bits,
                key.key_kinds & AggregateNumericPairKey::SECOND_SIGNED != 0,
            )),
            _ => {
                let index = column - 2;
                let spec = self
                    .compact_measure_specs
                    .as_ref()
                    .and_then(|specs| specs.get(index))
                    .ok_or_else(|| failed("numeric pair measure schema is absent"))?;
                measures
                    .values()
                    .get(index)
                    .ok_or_else(|| failed("numeric pair measure state is absent"))?
                    .native_result_value(spec.function)
            }
        }
    }

    pub(super) fn native_result_columns(&self) -> Vec<String> {
        self.group_columns
            .iter()
            .map(|column| column.name.clone())
            .chain(
                self.state_template
                    .states
                    .iter()
                    .map(|state| state.alias.clone()),
            )
            .collect()
    }

    pub(super) fn native_group_value<'a>(
        &'a self,
        key: &'a AggregateGroupKey,
        group: &'a GroupedAggregateState,
        column: usize,
    ) -> Result<Value<'a>> {
        if column < self.group_columns.len() {
            if let Some(values) = group.group_values()
                && values.len() == self.group_columns.len()
            {
                return Ok(Value::from(&values[column]));
            }
            if let Some(position) = self.key_position_for_group_index(column) {
                return Ok(
                    match key
                        .get(position)
                        .ok_or_else(|| failed("group key component is absent"))?
                    {
                        AggregateDistinctValue::Null => Value::Null,
                        AggregateDistinctValue::Boolean(value) => Value::Bool(*value),
                        AggregateDistinctValue::Int64(value) => Value::Int(*value),
                        AggregateDistinctValue::UInt64(value) => Value::UInt(*value),
                        AggregateDistinctValue::Float64Bits(value) => {
                            Value::Float(f64::from_bits(*value))
                        }
                        AggregateDistinctValue::Utf8(value) => Value::Text(value.as_ref().into()),
                        AggregateDistinctValue::Utf8Interned(id) => {
                            Value::Text(self.string_interner.value(*id)?.into())
                        }
                    },
                );
            }
            return self
                .reconstruct_group_value_from_key(key, column)
                .map(Value::from);
        }
        let measure = column - self.group_columns.len();
        match group {
            GroupedAggregateState::General { states, .. } => states.native_result_value(measure),
            GroupedAggregateState::CompactCountStar { count, .. } if measure == 0 => {
                Ok(Value::UInt(*count))
            }
            GroupedAggregateState::CompactMeasures { measures, .. } => {
                let spec = self
                    .compact_measure_specs
                    .as_ref()
                    .and_then(|specs| specs.get(measure))
                    .ok_or_else(|| failed("compact measure schema is absent"))?;
                measures
                    .values()
                    .get(measure)
                    .ok_or_else(|| failed("compact measure state is absent"))?
                    .native_result_value(spec.function)
            }
            GroupedAggregateState::CompactCountStar { .. } => {
                Err(failed("completed measure changed its declared schema"))
            }
        }
    }
}

impl TransformedDictionaryDenseGeneralState {
    pub(super) fn native_result_value(&self, state: &SimpleAggregateState) -> Result<Value<'_>> {
        Ok(match state.function {
            SimpleAggregateFunction::Count => Value::UInt(self.row_count),
            SimpleAggregateFunction::Sum if self.row_count > 0 => Value::Float(self.length_sum),
            SimpleAggregateFunction::Avg if self.row_count > 0 => {
                Value::Float(simple_average_value(self.length_sum, self.row_count))
            }
            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => Value::Null,
            SimpleAggregateFunction::Min | SimpleAggregateFunction::Max => {
                let minimum = state.function == SimpleAggregateFunction::Min;
                match state.value_transform {
                    AggregateValueTransform::Identity => {
                        let value = if minimum {
                            &self.min_utf8
                        } else {
                            &self.max_utf8
                        };
                        value
                            .as_ref()
                            .map_or(Value::Null, |value| Value::Text(value.as_ref().into()))
                    }
                    AggregateValueTransform::Length => {
                        let value = if minimum {
                            self.min_length
                        } else {
                            self.max_length
                        };
                        value.map_or(Value::Null, Value::UInt)
                    }
                    _ => {
                        return Err(failed(
                            "dense transformed measure does not admit this transform",
                        ));
                    }
                }
            }
            SimpleAggregateFunction::CountDistinct => {
                return Err(failed("dense transformed measure does not admit DISTINCT"));
            }
        })
    }
}
