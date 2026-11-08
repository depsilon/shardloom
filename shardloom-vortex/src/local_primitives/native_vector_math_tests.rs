use super::*;
use serde_json::Value;

fn bits(value: &Value) -> u64 {
    u64::from_str_radix(value.as_str().unwrap(), 16).unwrap()
}

fn components(row: &Value, side: &str) -> Vec<f64> {
    let width = row[format!("{side}_width")].as_u64().unwrap();
    row[side]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| match width {
            16 => vortex::array::dtype::half::f16::from_bits(u16::try_from(bits(value)).unwrap())
                .to_f64(),
            32 => f64::from(f32::from_bits(u32::try_from(bits(value)).unwrap())),
            64 => f64::from_bits(bits(value)),
            _ => panic!("unexpected reference width"),
        })
        .collect()
}

fn reductions(left: &[f64], right: &[f64]) -> [Magnitude; 4] {
    let mut dot = Accumulator::new();
    let mut left_square = Accumulator::new();
    let mut right_square = Accumulator::new();
    let mut distance = Accumulator::new();
    for (&left, &right) in left.iter().zip(right) {
        dot.product(left, right, false, false).unwrap();
        left_square.product(left, left, false, false).unwrap();
        right_square.product(right, right, false, false).unwrap();
        distance.product(left, left, false, false).unwrap();
        distance.product(right, right, false, false).unwrap();
        distance.product(left, right, true, true).unwrap();
    }
    [
        dot.finish(),
        left_square.finish(),
        right_square.finish(),
        distance.finish(),
    ]
}

fn corpus_screen(actual: f64, expected: &Value, ulps: u64, case: &str) {
    let expected = bits(expected);
    assert!(actual.is_finite(), "{case}: nonfinite ratio");
    assert_eq!(actual.to_bits() & SIGN, expected & SIGN, "{case}: sign");
    assert!(
        actual.to_bits().abs_diff(expected) <= ulps,
        "{case}: {:016x} differs from {expected:016x}",
        actual.to_bits()
    );
}

#[test]
fn finite_vector_reductions_match_preimplementation_independent_reference() {
    let reference: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/native_vector_numerical_reference_r2.json"
    ))
    .unwrap();
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 270);
    for row in cases {
        let name = row["id"].as_str().unwrap();
        let left = components(row, "left");
        let right = components(row, "right");
        let [dot, ls, rs, distance] = reductions(&left, &right);
        let expected = &row["expected"];
        for (label, actual) in [
            ("dot", dot.rounded()),
            ("norm_left", ls.sqrt().unwrap()),
            ("norm_right", rs.sqrt().unwrap()),
            ("l2_distance", distance.sqrt().unwrap()),
        ] {
            assert_eq!(actual.to_bits(), bits(&expected[label]), "{name}: {label}");
        }
        let cosine = cosine(&dot, &ls, &rs).unwrap();
        assert_eq!(cosine.is_none(), expected["cosine"].is_null(), "{name}");
        if let Some(actual) = cosine {
            corpus_screen(actual, &expected["cosine"], 8, name);
            if dot.is_zero() {
                assert_eq!(actual.to_bits(), 0, "{name}: exact zero dot");
            }
        }
        let normalizer = Normalizer::new(&ls).unwrap();
        assert_eq!(
            normalizer.is_none(),
            expected["normalized_left"].is_null(),
            "{name}"
        );
        if let Some(normalizer) = normalizer {
            let expected = expected["normalized_left"].as_array().unwrap();
            assert_eq!(left.len(), expected.len());
            for (&component, expected) in left.iter().zip(expected) {
                let actual = normalizer.component(component);
                corpus_screen(actual, expected, 4, name);
                if component == 0.0 {
                    assert_eq!(actual.to_bits(), component.to_bits(), "{name}: zero sign");
                }
            }
        }
    }
}

#[test]
fn vector_rounding_boundaries_match_exact_rational_midpoint_reference() {
    let reference: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/native_vector_rounding_reference.json"
    ))
    .unwrap();
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 4918);
    for row in cases {
        let name = row["id"].as_str().unwrap();
        let mut magnitude = Magnitude {
            words: [0; WORDS],
            negative: false,
        };
        for word in row["words"].as_array().unwrap() {
            magnitude.words[usize::try_from(word[0].as_u64().unwrap()).unwrap()] = bits(&word[1]);
        }
        assert_eq!(
            magnitude.rounded().to_bits(),
            bits(&row["rounded"]),
            "{name}: round"
        );
        assert_eq!(
            magnitude.sqrt().unwrap().to_bits(),
            bits(&row["sqrt"]),
            "{name}: root"
        );
        if !magnitude.is_zero() {
            magnitude.negative = true;
            assert_eq!(
                magnitude.rounded().to_bits(),
                bits(&row["rounded"]) | SIGN,
                "{name}: sign"
            );
            assert!(magnitude.sqrt().is_err());
        }
    }
}

#[test]
fn vector_accumulator_carry_borrow_and_capacity_are_checked() {
    for boundary in 1..WORDS {
        let mut bank = [0; WORDS];
        bank[..boundary].fill(u64::MAX);
        add_word(&mut bank, 0, 1).unwrap();
        assert!(bank[..boundary].iter().all(|word| *word == 0));
        assert_eq!(bank[boundary], 1);
        let mut one = [0; WORDS];
        one[0] = 1;
        subtract(&mut bank, &one);
        assert!(bank[..boundary].iter().all(|word| *word == u64::MAX));
        assert!(bank[boundary..].iter().all(|word| *word == 0));
    }
    let mut bank = [u64::MAX; WORDS];
    assert!(add_word(&mut bank, 0, 1).is_err());
    let mut accumulator = Accumulator::new();
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(accumulator.product(invalid, 0.0, false, false).is_err());
        assert!(accumulator.product(1.0, invalid, false, false).is_err());
    }
    accumulator
        .product(f64::MAX, f64::MAX, false, true)
        .unwrap();
    accumulator.product(f64::MAX, f64::MAX, true, true).unwrap();
    assert_eq!(accumulator.finish().rounded().to_bits(), 0);
}

#[test]
fn vector_final_scaling_has_one_subnormal_rounding_and_retains_sign() {
    assert_eq!(scale_normal(1.0, -1075).to_bits(), 0);
    assert_eq!(scale_normal(-1.0, -1075).to_bits(), SIGN);
    assert_eq!(
        scale_normal(f64::from_bits(0x3ff0_0000_0000_0001), -1075).to_bits(),
        1
    );
    assert_eq!(scale_normal(1.5, -1074).to_bits(), 2);
    assert_eq!(scale_normal(1.25, -1073).to_bits(), 2);
    assert_eq!(scale_normal(1.75, -1073).to_bits(), 4);
    assert_eq!(
        scale_normal(f64::from_bits(0x3fff_ffff_ffff_ffff), -1023).to_bits(),
        1 << 52
    );
    assert_eq!(scale_normal(1.0, 1023).to_bits(), 0x7fe0_0000_0000_0000);
    assert_eq!(scale_normal(1.0, 1024).to_bits(), INFINITY);
}
