//! Shared recognition of the admitted native temporal storage domains.

use vortex::array::{
    dtype::{DType, PType},
    extension::datetime::{Date, TimeUnit, Timestamp},
};

/// Recognize provider metadata, never an extension name or its storage alone.
pub(crate) fn temporal_storage(dtype: &DType) -> Option<PType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    let storage = if extension.metadata_opt::<Date>() == Some(&TimeUnit::Days) {
        PType::I32
    } else if extension
        .metadata_opt::<Timestamp>()
        .is_some_and(|metadata| metadata.unit == TimeUnit::Microseconds && metadata.tz.is_none())
    {
        PType::I64
    } else {
        return None;
    };
    matches!(extension.storage_dtype(), DType::Primitive(ptype, _) if *ptype == storage)
        .then_some(storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cmp::Ordering, sync::Arc};
    use vortex::array::{
        dtype::{
            Nullability,
            extension::{ExtDType, ExtVTable},
        },
        extension::datetime::TimestampOptions,
        scalar::{PValue, Scalar, ScalarValue},
    };

    fn scalar(dtype: &DType, value: i64) -> Scalar {
        Scalar::try_new(
            dtype.clone(),
            Some(ScalarValue::Primitive(PValue::I64(value))),
        )
        .expect("every admitted timestamp storage value must construct a scalar")
    }

    #[test]
    fn full_domain_timestamp_scalars_preserve_storage_order_and_metadata() {
        let extension = Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable);
        let metadata = extension.metadata().clone();
        let serialized = Timestamp.serialize_metadata(&metadata).unwrap();
        assert_eq!(
            Timestamp.deserialize_metadata(&serialized).unwrap(),
            metadata
        );
        let dtype = DType::Extension(extension.erased());
        assert_eq!(temporal_storage(&dtype), Some(PType::I64));
        // Include both storage endpoints, the Jiff span boundary and values
        // outside its calendar but inside its span, as well as ordinary dates.
        let values = [
            i64::MIN,
            i64::MIN + 1,
            -631_107_417_600_000_001,
            -631_107_417_600_000_000,
            -400_000_000_000_000_000,
            -1,
            0,
            1,
            1_700_000_000_000_000,
            300_000_000_000_000_000,
            631_107_417_600_000_000,
            631_107_417_600_000_001,
            i64::MAX - 1,
            i64::MAX,
        ];
        let scalars = values.map(|value| {
            let item = scalar(&dtype, value);
            assert_eq!(item.dtype(), &dtype);
            assert_eq!(
                item.as_extension()
                    .to_storage_scalar()
                    .as_primitive()
                    .as_::<i64>(),
                Some(value)
            );
            item
        });
        for pair in scalars.windows(2) {
            assert_eq!(pair[0].partial_cmp(&pair[1]), Some(Ordering::Less));
        }
        let null = Scalar::try_new(dtype, None).unwrap();
        assert!(null.is_null());
        assert!(null.as_extension().to_storage_scalar().is_null());
    }

    #[test]
    fn full_domain_timestamp_display_is_exact_and_does_not_panic() {
        let dtype = DType::Extension(
            Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
        );
        for value in [
            i64::MIN,
            -400_000_000_000_000_000,
            300_000_000_000_000_000,
            i64::MAX,
        ] {
            assert_eq!(
                scalar(&dtype, value).as_extension().to_string(),
                format!("{value}us since Unix epoch")
            );
        }
        for (value, expected) in [
            (-1, "1969-12-31T23:59:59.999999Z"),
            (0, "1970-01-01T00:00:00Z"),
            (1, "1970-01-01T00:00:00.000001Z"),
        ] {
            assert_eq!(scalar(&dtype, value).as_extension().to_string(), expected);
        }
        assert_eq!(
            Scalar::try_new(dtype, None)
                .unwrap()
                .as_extension()
                .to_string(),
            "null"
        );
    }

    #[test]
    fn full_domain_timestamp_admission_does_not_expand_other_temporal_contracts() {
        let options = TimestampOptions {
            unit: TimeUnit::Microseconds,
            tz: None,
        };
        assert!(
            ExtDType::<Timestamp>::try_new(
                options,
                DType::Primitive(PType::U64, Nullability::Nullable)
            )
            .is_err()
        );
        for (unit, timezone, value) in [
            (TimeUnit::Days, None, 0),
            (TimeUnit::Seconds, None, 300_000_000_000),
            (TimeUnit::Milliseconds, None, 300_000_000_000_000),
            (
                TimeUnit::Microseconds,
                Some(Arc::from("UTC")),
                300_000_000_000_000_000,
            ),
            (TimeUnit::Microseconds, Some(Arc::from("Not/A/Timezone")), 0),
        ] {
            let dtype = DType::Extension(
                Timestamp::new_with_tz(unit, timezone, Nullability::Nullable).erased(),
            );
            assert_eq!(temporal_storage(&dtype), None);
            assert!(
                Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(value)))).is_err()
            );
        }
        let required = DType::Extension(
            Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased(),
        );
        assert!(Scalar::try_new(required, None).is_err());
    }
}
