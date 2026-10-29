use super::*;

#[test]
fn explicit_decimal_domains_and_names_are_validated() {
    for (precision, scale) in [(0, 0), (39, 0), (1, 2), (38, 39)] {
        assert!(DecimalOutput::new(precision, scale, DecimalRounding::Exact).is_err());
    }
    for mode in [
        DecimalRounding::Exact,
        DecimalRounding::HalfEven,
        DecimalRounding::HalfAway,
        DecimalRounding::TowardZero,
        DecimalRounding::Floor,
        DecimalRounding::Ceil,
    ] {
        assert_eq!(DecimalRounding::parse(mode.as_str()).unwrap(), mode);
    }
    assert!(DecimalRounding::parse("half_up").is_err());
    assert!(DecimalRounding::parse("HALF_EVEN").is_err());
}

#[test]
fn explicit_decimal_reference_matrix_matches_python_fraction_and_decimal() {
    let mut tested = 0;
    for line in include_str!("decimal_output_reference.tsv")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let columns: Vec<_> = line.split('\t').collect();
        assert_eq!(columns.len(), 11);
        let output = DecimalOutput::new(
            columns[7].parse().unwrap(),
            columns[8].parse().unwrap(),
            DecimalRounding::parse(columns[9]).unwrap(),
        )
        .unwrap();
        let source_scale: u8 = columns[2].parse().unwrap();
        let result = if columns[0] == "total" {
            let mut bytes = [0_u8; 32];
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&columns[1][index * 2..index * 2 + 2], 16).unwrap();
            }
            output.total(bytes, source_scale, columns[3].parse().unwrap())
        } else {
            let left =
                Decimal128Operand::decimal(columns[1].parse().unwrap(), 38, source_scale).unwrap();
            let right = Decimal128Operand::decimal(
                columns[3].parse().unwrap(),
                38,
                columns[4].parse().unwrap(),
            )
            .unwrap();
            match columns[0] {
                "rescale" => output.rescale(left),
                "interpolate" => output.interpolate(
                    left,
                    right,
                    columns[5].parse().unwrap(),
                    columns[6].parse().unwrap(),
                ),
                operation => output.binary(
                    left,
                    match operation {
                        "add" => BinaryOp::Add,
                        "subtract" => BinaryOp::Subtract,
                        "multiply" => BinaryOp::Multiply,
                        "divide" => BinaryOp::Divide,
                        _ => panic!("unknown reference operation"),
                    },
                    right,
                ),
            }
        };
        match columns[10] {
            "inexact" => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("nonzero fractional digits"),
                "{line}"
            ),
            "overflow" => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("precision overflow"),
                "{line}"
            ),
            "zero" => assert!(
                result.unwrap_err().to_string().contains("division by zero"),
                "{line}"
            ),
            expected => {
                let actual = result.unwrap_or_else(|error| panic!("{line}: {error}"));
                assert_eq!(actual.value(), expected.parse::<i128>().unwrap(), "{line}");
                assert_eq!(
                    actual.precision_scale(),
                    (output.precision(), output.scale()),
                    "{line}"
                );
            }
        }
        tested += 1;
    }
    assert_eq!(tested, 3462);
}

#[test]
fn decimal_ratio_declarations_and_wide_integer_boundaries_fail_explicitly() {
    let output = DecimalOutput::new(38, 0, DecimalRounding::HalfEven).unwrap();
    let value = Decimal128Operand::decimal(1, 38, 0).unwrap();
    assert!(
        output
            .total([0; 32], 0, 0)
            .unwrap_err()
            .to_string()
            .contains("division by zero")
    );
    assert!(output.total([0; 32], 39, 1).is_err());
    assert!(output.interpolate(value, value, 1, 0).is_err());
    assert!(output.interpolate(value, value, 0, 1075).is_err());
    assert!(
        output
            .interpolate(value, Decimal128Operand::decimal(1, 38, 1).unwrap(), 1, 1)
            .is_err()
    );
    assert!(output.binary(value, BinaryOp::And, value).is_err());
    let one = Unsigned::from_u128(1);
    assert!(one.shift_left(1536).is_err());
    let high = one.shift_left(1535).unwrap();
    assert!(high.add(high).is_err());
    assert!(high.multiply(Unsigned::from_u128(2)).is_err());
    assert!(Unsigned::ZERO.subtract(one).is_err());
    assert!(one.divide(Unsigned::ZERO).is_err());
    assert!(Unsigned::power10(77).is_err());
}
