#!/usr/bin/env python3
"""Generate deterministic Decimal128 cases with Python's independent exact oracles.

This is test-data generation only. ShardLoom runtime never invokes this script.
"""

import argparse
from decimal import (
    Decimal,
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    localcontext,
)
from fractions import Fraction
from pathlib import Path
import random


MODES = {
    "exact": None,
    "half_even": ROUND_HALF_EVEN,
    "half_away": ROUND_HALF_UP,
    "toward_zero": ROUND_DOWN,
    "floor": ROUND_FLOOR,
    "ceil": ROUND_CEILING,
}
MAXIMUM = 10**38 - 1


def expected(value, precision, scale, mode):
    scaled = value * 10**scale
    if mode == "exact":
        if scaled.denominator != 1:
            return "inexact"
        coefficient = scaled.numerator
    else:
        # The largest dyadic denominator has 1074 bits. This precision retains
        # it exactly; other admitted rational denominators cannot approach a
        # rounding boundary by even remotely 1800 decimal digits.
        with localcontext() as context:
            context.prec = 1800
            converted = Decimal(scaled.numerator) / Decimal(scaled.denominator)
            coefficient = int(converted.to_integral_value(rounding=MODES[mode]))
        # Separate Fraction checks establish the selected adjacent integer and
        # direction. Ties are independently classified without Decimal context.
        low = scaled.numerator // scaled.denominator
        high = -((-scaled.numerator) // scaled.denominator)
        assert coefficient in (low, high)
        if mode == "floor":
            assert coefficient == low
        elif mode == "ceil":
            assert coefficient == high
        elif mode == "toward_zero":
            assert coefficient == (high if scaled < 0 else low)
        else:
            distances = {candidate: abs(scaled - candidate) for candidate in (low, high)}
            assert distances[coefficient] == min(distances.values())
            if low != high and distances[low] == distances[high]:
                tie = (low if low % 2 == 0 else high) if mode == "half_even" else (low if scaled < 0 else high)
                assert coefficient == tie
    return "overflow" if abs(coefficient) >= 10**precision else str(coefficient)


def cases():
    # kind, left, left-scale, right, right-scale, weight-numerator, weight-shift,
    # output-precision, output-scale. All operand precisions are 38.
    for value in [0, 1, -1, 125, -125, 135, -135, 149, -149, 150, -150, 151, -151, 995, -995, MAXIMUM, -MAXIMUM]:
        for source, precision, target in [(2, 38, 1), (2, 2, 0), (38, 38, 0), (0, 38, 38)]:
            yield ("rescale", value, source, 0, 0, 0, 0, precision, target)
    values = [0, 1, -1, 5, -5, 10**19 + 1, -(10**19 + 1), MAXIMUM, -MAXIMUM]
    randomizer = random.Random(20261029)
    for operation in ("add", "subtract", "multiply", "divide"):
        for _ in range(60):
            left = randomizer.choice(values) if randomizer.randrange(2) else randomizer.randrange(-MAXIMUM, MAXIMUM + 1)
            right = randomizer.choice(values) if randomizer.randrange(2) else randomizer.randrange(-MAXIMUM, MAXIMUM + 1)
            precision = randomizer.choice([1, 2, 18, 38])
            yield (operation, left, randomizer.choice([0, 2, 19, 38]), right, randomizer.choice([0, 2, 19, 38]), 0, 0, precision, randomizer.randrange(precision + 1))
    for probability in [0.0, float.fromhex("0x0.0000000000001p-1022"), 1e-300, 0.1, 0.5, 0.9, float.fromhex("0x1.fffffffffffffp-1")]:
        numerator, denominator = probability.as_integer_ratio()
        shift = denominator.bit_length() - 1
        assert denominator == 1 << shift
        for left, right in [(0, 10), (-10, 0), (-2, -1), (1, 2), (-MAXIMUM, MAXIMUM), (MAXIMUM - 1, MAXIMUM)]:
            for source, target in [(0, 0), (38, 0), (0, 38), (2, 1)]:
                yield ("interpolate", left, source, right, source, numerator, shift, 38, target)
    for count in [1, 2, 3, 2**64 - 1]:
        for source in [0, 2, 38]:
            for coefficient in [MAXIMUM * count, -MAXIMUM * count, 1, -1]:
                for divisor in [1, count]:
                    yield ("total", coefficient, source, divisor, 0, 0, 0, 38, randomizer.choice([0, 6, 38]))
    # Deliberate post-rounding precision overflow and mixed-scale cancellation.
    yield ("rescale", 995, 2, 0, 0, 0, 0, 2, 1)
    yield ("rescale", -995, 2, 0, 0, 0, 0, 2, 1)
    yield ("add", MAXIMUM, 38, -MAXIMUM, 38, 0, 0, 1, 0)
    yield ("multiply", MAXIMUM, 38, MAXIMUM, 38, 0, 0, 38, 38)
    yield ("divide", MAXIMUM, 38, MAXIMUM, 0, 0, 0, 38, 38)


def generate():
    rows = ["# kind\tleft\tleft_scale\tright\tright_scale\tnumerator\tshift\tprecision\tscale\trounding\texpected"]
    for operation, left, ls, right, rs, numerator, shift, precision, scale in cases():
        lhs, rhs = Fraction(left, 10**ls), Fraction(right, 10**rs)
        error = None
        if operation == "rescale":
            value = lhs
        elif operation == "add":
            value = lhs + rhs
        elif operation == "subtract":
            value = lhs - rhs
        elif operation == "multiply":
            value = lhs * rhs
        elif operation == "divide":
            if rhs == 0:
                error = "zero"
                value = Fraction(0)
            else:
                value = lhs / rhs
        elif operation == "interpolate":
            value = lhs + (rhs - lhs) * Fraction(numerator, 1 << shift)
        elif operation == "total":
            value = lhs / right
            left = left.to_bytes(32, "little", signed=True).hex()
        else:
            raise AssertionError(operation)
        for mode in MODES:
            result = error or expected(value, precision, scale, mode)
            rows.append("\t".join(map(str, (operation, left, ls, right, rs, numerator, shift, precision, scale, mode, result))))
    return "\n".join(rows) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = generate()
    args.output.write_text(data)
    print(f"wrote {len(data.splitlines()) - 1} exact decimal references")
