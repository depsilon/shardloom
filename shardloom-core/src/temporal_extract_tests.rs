use super::*;

const PARTS: [Part; 11] = [
    Part::Year,
    Part::Month,
    Part::Day,
    Part::DayOfWeek,
    Part::DayOfYear,
    Part::Hour,
    Part::Minute,
    Part::Second,
    Part::Millisecond,
    Part::Microsecond,
    Part::Nanosecond,
];

fn agrees(name: &str, values: &[i64]) {
    let source = TemporalDType::parse(name).unwrap();
    for part in PARTS {
        let admitted = match source.kind() {
            Kind::Date32 | Kind::Date64 => part.date_part(),
            Kind::Time(_) => !part.date_part(),
            Kind::Timestamp { .. } => true,
            Kind::Duration(_) => false,
        };
        let kernel = Kernel::bind(Operation::Extract(part), source.clone(), None);
        if !admitted {
            assert!(kernel.is_err());
            continue;
        }
        let kernel = kernel.expect("supported extraction remains admitted");
        for &value in values {
            let expected = source
                .validate_value(value)
                .and_then(|()| kernel.civil(value).map(|civil| extract(civil, part)));
            let actual = kernel.apply(value, None);
            assert_eq!(
                actual.as_ref().ok(),
                expected.as_ref().ok(),
                "{name} {part:?} {value}"
            );
            assert_eq!(
                actual.as_ref().err().map(ToString::to_string),
                expected.as_ref().err().map(ToString::to_string),
                "{name} {part:?} {value}"
            );
        }
    }
}

#[test]
fn prepared_temporal_extract_matches_full_civil_over_endpoints_and_negative_fractions() {
    for unit in [
        Unit::Seconds,
        Unit::Milliseconds,
        Unit::Microseconds,
        Unit::Nanoseconds,
    ] {
        let ticks = i64::try_from(NANOS_PER_SECOND / unit.nanoseconds()).unwrap();
        let mut values = vec![i64::MIN, i64::MIN + 1, i64::MAX - 1, i64::MAX, -1, 0, 1];
        for seconds in [1, 59, 60, 3_599, 3_600, 86_399, 86_400, 86_401] {
            for sign in [-1, 1] {
                let boundary = sign * seconds * ticks;
                values.extend([boundary - 1, boundary, boundary + 1]);
            }
        }
        let mut seed = 0x0030_2026_6a91u64;
        for _ in 0..127 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            values.push(i64::from_ne_bytes(seed.to_ne_bytes()));
        }
        for text in [
            "1900-02-28T23:59:59",
            "2000-02-29T12:34:56",
            "1968-02-29T00:00:00",
            "2024-12-31T23:59:59",
        ] {
            values.push(civil::parse_timestamp(text).unwrap().0.ticks(unit).unwrap());
        }
        agrees(&format!("timestamp({unit})"), &values);
    }
    agrees(
        "date32",
        &[
            i64::from(i32::MIN),
            i64::from(i32::MIN) + 1,
            -719_528,
            -1,
            0,
            1,
            11_016,
            i64::from(i32::MAX),
        ],
    );
    let day = 86_400_000;
    agrees(
        "date64",
        &[
            i64::MIN / day * day,
            -719_528 * day,
            -day,
            0,
            day,
            11_016 * day,
            i64::MAX / day * day,
        ],
    );
}

#[test]
fn prepared_temporal_extract_preserves_restricted_values_and_timezone_results_and_errors() {
    agrees(
        "date32",
        &[
            i64::MIN,
            i64::from(i32::MIN) - 1,
            i64::from(i32::MAX) + 1,
            i64::MAX,
        ],
    );
    agrees(
        "date64",
        &[i64::MIN, i64::MAX, -86_400_001, -1, 1, 86_400_001],
    );
    for unit in [
        Unit::Seconds,
        Unit::Milliseconds,
        Unit::Microseconds,
        Unit::Nanoseconds,
    ] {
        let day = i64::try_from(NANOS_PER_DAY / unit.nanoseconds()).unwrap();
        agrees(
            &format!("time({unit})"),
            &[i64::MIN, -1, 0, 1, day / 2, day - 1, day, i64::MAX],
        );
        let mut values = vec![i64::MIN, i64::MAX, -1, 0, 1];
        for text in [
            "2024-03-10T06:59:59",
            "2024-03-10T07:00:00",
            "2024-11-03T05:59:59",
            "2024-11-03T06:00:00",
        ] {
            values.push(civil::parse_timestamp(text).unwrap().0.ticks(unit).unwrap());
        }
        for zone in ["UTC", "+05:45", "America/New_York"] {
            agrees(&format!("timestamp({unit},{zone})"), &values);
        }
    }
}
