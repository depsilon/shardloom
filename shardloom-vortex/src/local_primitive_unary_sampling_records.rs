//! Typed sample population records retain original payload and exact private bits.

use super::{BoundUnary, NativeExecutionContext, Record, ReservedVec, Result, failed};
use crate::local_primitives::{
    logical_field_from_native_array, native_payload, native_relational_records as records,
    native_relational_sort::Spec, prepared_unary::values,
};
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::ArrayRef;

pub(super) const CUMULATIVE: usize = 2;
const PAYLOAD: usize = 3;
const SCORE: &str = "score";

pub(super) struct Schema {
    pub(super) population: Spec,
    pub(super) output: Spec,
    pub(super) names: Vec<String>,
    pub(super) payload_indices: Vec<usize>,
    payload_names: Vec<String>,
    _metadata: MemoryLease,
}

impl Schema {
    pub(super) fn new(plan: &BoundUnary, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let bytes = plan
            .fields
            .iter()
            .try_fold(32_768_u64, |bytes, (name, dtype)| {
                native_payload::metadata_bytes(dtype)?
                    .checked_add(name.len() as u64 * 2)
                    .and_then(|field| field.checked_mul(8))
                    .and_then(|field| bytes.checked_add(field))
                    .ok_or_else(|| failed("sampling schema capacity overflow"))
            })?;
        let metadata = context.memory().reserve(bytes)?;
        let mut fields = vec![
            (records::ORDINAL.into(), records::u64_type()),
            (SCORE.into(), records::u64_type()),
            ("cumulative_weight_bits".into(), records::u64_type()),
        ];
        let payload_names = (0..plan.fields.len())
            .map(|index| format!("payload_{index}"))
            .collect::<Vec<_>>();
        fields.extend(
            payload_names
                .iter()
                .zip(&plan.fields)
                .map(|(name, (_, dtype))| (name.clone(), dtype.clone())),
        );
        let names = fields.iter().map(|(name, _)| name.clone()).collect();
        let payload_indices = (PAYLOAD..fields.len()).collect();
        let replacement = plan.request.sample_with_replacement;
        let keys = if replacement {
            vec![records::ORDINAL.into()]
        } else {
            vec![SCORE.into(), records::ORDINAL.into()]
        };
        let mut population = records::order(fields.clone(), keys)?;
        population.keys[0].descending = !replacement;
        population.copy_policy = native_payload::CopyPolicy::PreserveUnobserved;
        let mut output = records::order(fields, vec![records::ORDINAL.into()])?;
        output.copy_policy = native_payload::CopyPolicy::PreserveUnobserved;
        Ok(Self {
            population,
            output,
            names,
            payload_indices,
            payload_names,
            _metadata: metadata,
        })
    }

    pub(super) fn build(
        &self,
        rows: &[Record],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.population.fields.len())?;
        columns
            .values
            .push(records::unsigned(rows.len(), context, |row| {
                Ok(rows[row].ordinal)
            })?);
        columns
            .values
            .push(records::unsigned(rows.len(), context, |row| {
                Ok(rows[row].score)
            })?);
        columns
            .values
            .push(records::unsigned(rows.len(), context, |row| {
                Ok(rows[row].cumulative_weight)
            })?);
        for (column, (_, dtype)) in self.population.fields[PAYLOAD..].iter().enumerate() {
            context.check_cancelled()?;
            columns.values.push(if native_payload::is_nested(dtype) {
                native_payload::retained_column(dtype, rows.len(), context, |row| {
                    rows[row]
                        .row
                        .native(column)
                        .cloned()
                        .map(Some)
                        .ok_or_else(|| failed("sampling nested payload is absent"))
                })?
            } else {
                records::preserved_column(dtype, rows.len(), context, |row| {
                    values::borrowed(rows[row].row.scalar(column)?)
                })?
            });
        }
        records::structure(&self.population.fields, columns, rows.len())
    }

    pub(super) fn deliver(
        &self,
        array: &ArrayRef,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.payload_names.len())?;
        for name in &self.payload_names {
            columns
                .values
                .push(logical_field_from_native_array(array, name)?);
        }
        records::delivered(&plan.fields, columns, array.len(), context)
    }
}

/// Numeric f64 comparison represented by an unsigned key. Resident sampling
/// treats -0 and +0 as ties, and valid tiny weights can yield -infinity.
pub(super) fn ordered_score(value: f64) -> Result<u64> {
    const SIGN: u64 = 1 << 63;
    if value.is_nan() {
        return Err(failed("weighted sample score is NaN"));
    }
    let bits = if value == 0.0 { 0 } else { value.to_bits() };
    Ok(if bits & SIGN != 0 { !bits } else { bits ^ SIGN })
}

#[cfg(test)]
mod tests {
    use super::ordered_score;

    #[test]
    fn sampling_private_score_order_preserves_infinities_subnormals_and_zero_ties() {
        let values = [
            f64::NEG_INFINITY,
            -f64::MAX,
            -1.0,
            -f64::MIN_POSITIVE,
            -f64::from_bits(1),
            -0.0,
            0.0,
            f64::from_bits(1),
            1.0,
            f64::INFINITY,
        ];
        for left in values {
            for right in values {
                assert_eq!(
                    ordered_score(left)
                        .unwrap()
                        .cmp(&ordered_score(right).unwrap()),
                    left.partial_cmp(&right).unwrap()
                );
            }
        }
        assert!(ordered_score(f64::NAN).is_err());
    }
}
