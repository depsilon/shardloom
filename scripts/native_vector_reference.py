"""Prepare independent finite-vector references; never execute the native engine.

All products and differences use Fraction from actual input bits. Square-root
ratios round by searching binary64 neighbors and comparing exact rational
midpoints. This is a test oracle, not a proposed runtime implementation.
"""

import argparse
from datetime import datetime, timezone
from fractions import Fraction
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import random
import struct


FORMATS = {16: ("e", "H", 5, 10), 32: ("f", "I", 8, 23), 64: ("d", "Q", 11, 52)}
MAX = 0x7FEFFFFFFFFFFFFF
INF = 0x7FF0000000000000
SIGN = 1 << 63


def to_float(width, bits):
    real, integer, _, _ = FORMATS[width]
    return struct.unpack(">" + real, struct.pack(">" + integer, bits))[0]


def to_bits(width, value):
    real, integer, _, _ = FORMATS[width]
    return struct.unpack(">" + integer, struct.pack(">" + real, value))[0]


@lru_cache(maxsize=4096)
def rational(bits):
    assert 0 <= bits <= MAX
    return Fraction.from_float(to_float(64, bits))


OVERFLOW_MIDPOINT = rational(MAX) + Fraction(2) ** 970


@lru_cache(maxsize=4096)
def rounded_positive(value, squared):
    """Round value (or sqrt(value)) to nearest binary64, ties to even."""
    assert isinstance(value, Fraction) and value >= 0
    if not value:
        return 0
    transform = (lambda candidate: candidate * candidate) if squared else (lambda x: x)
    if value >= transform(OVERFLOW_MIDPOINT):
        return INF
    low, high = 0, MAX
    while low < high:
        middle = (low + high + 1) // 2
        if transform(rational(middle)) <= value:
            low = middle
        else:
            high = middle - 1
    if low == MAX:
        return MAX
    midpoint = (rational(low) + rational(low + 1)) / 2
    boundary = transform(midpoint)
    return low + int(value > boundary or (value == boundary and low & 1))


def rounded(value, squared=False, negative_zero=False):
    negative = value < 0 or (not value and negative_zero)
    return rounded_positive(abs(value), squared) | (SIGN if negative else 0)


def hexadecimal(bits):
    return f"{bits:016x}"


def expected(left_width, left_bits, right_width, right_bits):
    assert len(left_bits) == len(right_bits)
    left_float = [to_float(left_width, bits) for bits in left_bits]
    right_float = [to_float(right_width, bits) for bits in right_bits]
    left = [Fraction.from_float(value) for value in left_float]
    right = [Fraction.from_float(value) for value in right_float]
    dot = sum((a * b for a, b in zip(left, right)), Fraction())
    square_left = sum((value * value for value in left), Fraction())
    square_right = sum((value * value for value in right), Fraction())
    distance = sum(((a - b) ** 2 for a, b in zip(left, right)), Fraction())
    cosine = None
    if square_left and square_right:
        cosine = rounded(dot * dot / (square_left * square_right), squared=True)
        if dot < 0:
            cosine |= SIGN
    normalized = None
    if square_left:
        normalized = []
        for value, original in zip(left, left_float):
            result = rounded(value * value / square_left, squared=True)
            if to_bits(64, original) & SIGN:
                result |= SIGN
            normalized.append(hexadecimal(result))
    return dict(
        dot=hexadecimal(rounded(dot)),
        norm_left=hexadecimal(rounded(square_left, squared=True)),
        norm_right=hexadecimal(rounded(square_right, squared=True)),
        l2_distance=hexadecimal(rounded(distance, squared=True)),
        cosine=hexadecimal(cosine) if cosine is not None else None,
        normalized_left=normalized,
        zero_norm_left=not square_left,
        zero_norm_right=not square_right,
    )


def check_oracle():
    controls = 0
    for bits in [0, 1, 2, 3, 0x000FFFFFFFFFFFFF, 0x0010000000000000,
                 0x3FEFFFFFFFFFFFFF, 0x3FF0000000000000, 0x3FF0000000000001,
                 0x4008000000000000, MAX - 1, MAX]:
        value = rational(bits)
        assert rounded_positive(value * value, True) == bits
        assert rounded_positive(value, False) == bits
        controls += 2
        if bits < MAX:
            midpoint = (value + rational(bits + 1)) / 2
            target = bits + (bits & 1)
            assert rounded_positive(midpoint, False) == target
            assert rounded_positive(midpoint * midpoint, True) == target
            controls += 2
    assert rounded_positive(Fraction(25), True) == to_bits(64, 5.0)
    assert rounded_positive(OVERFLOW_MIDPOINT, False) == INF
    assert rounded_positive(OVERFLOW_MIDPOINT**2, True) == INF
    assert rounded_positive(OVERFLOW_MIDPOINT - 1, False) == MAX
    controls += 4
    rng = random.Random(0x564543544F525245)
    for _ in range(256):
        width = rng.choice([16, 32, 64])
        _, _, exponent_bits, fraction_bits = FORMATS[width]
        bits = rng.randrange(((1 << exponent_bits) - 1) << fraction_bits)
        value = Fraction.from_float(to_float(width, bits))
        assert rounded_positive(value * value, True) == to_bits(64, float(value))
        ratio = Fraction(rng.randrange(1, 1 << 106), rng.randrange(1, 1 << 106))
        assert rounded_positive(ratio, False) == to_bits(64, float(ratio))
        controls += 2
    return controls


def cases():
    rows = []

    def add(name, left, right, left_width=64, right_width=64, raw=False):
        assert len(left) == len(right)
        if not raw:
            left = [to_bits(left_width, value) for value in left]
            right = [to_bits(right_width, value) for value in right]
        rows.append(dict(id=name, left_width=left_width, right_width=right_width,
                         left=[f"{value:0{left_width // 4}x}" for value in left],
                         right=[f"{value:0{right_width // 4}x}" for value in right],
                         expected=expected(left_width, left, right_width, right)))

    maximum, tiny = to_float(64, MAX), to_float(64, 1)
    add("empty", [], [])
    add("zero_signs", [0.0, -0.0], [-0.0, 0.0])
    add("three_four", [3.0, 4.0], [1.0, 0.0])
    add("unit_orthogonal", [1.0, 0.0], [0.0, 1.0])
    add("unit_equal", [1.0, 0.0], [1.0, 0.0])
    add("unit_opposite", [1.0, 0.0], [-1.0, 0.0])
    add("normalization_signed_zero", [-0.0, 3.0, 4.0], [0.0, 3.0, 4.0])
    add("overflow_products_cancel_to_one", [maximum, maximum, 1.0], [maximum, -maximum, 1.0])
    add("overflow_products_cancel_to_small", [2.0**1023, 2.0**1023, 2.0**-50],
        [2.0**1023, -(2.0**1023), 2.0**-50])
    add("half_subnormal_products", [tiny, tiny], [0.5, 0.5])
    add("positive_underflow_tie", [tiny], [0.5])
    add("negative_underflow_tie", [-tiny], [0.5])
    add("minimum_subnormal_norm", [tiny], [0.0])
    add("two_minimum_subnormals", [tiny, tiny], [tiny, -tiny])
    add("cosine_unit_despite_dot_underflow", [tiny, tiny], [tiny, tiny])
    add("cosine_unit_despite_dot_overflow", [maximum, maximum], [maximum, maximum])
    add("normalization_despite_norm_overflow", [maximum, maximum], [maximum, -maximum])
    add("maximum_equal", [maximum], [maximum])
    add("maximum_opposite", [maximum], [-maximum])
    add("maximum_adjacent", [MAX], [MAX - 1], raw=True)
    add("one_adjacent", [0x3FF0000000000001], [0x3FF0000000000000], raw=True)
    add("cosine_legitimate_underflow", [tiny, maximum], [tiny, 0.0])
    add("cosine_mixed_extremes", [maximum, tiny, -maximum], [maximum, tiny, maximum])
    add("literal_width_1536", [(-1.0)**index for index in range(1536)], [1.0] * 1536)
    add("mixed_half_double", [0.5, -1.5, 0.0, 2.0**-24], [2.0, 3.0, -0.0, 0.5], 16, 64)
    add("mixed_single_double", [2.0**-149, 2.0**127, -0.5], [0.5, 2.0**-127, -3.0], 32, 64)
    rng = random.Random(0x5348415244564543)
    for left_width, right_width, count in [(64, 64, 128), (32, 32, 32), (16, 16, 32),
                                            (16, 32, 16), (32, 64, 16)]:
        for index in range(count):
            dimension = rng.randrange(1, 18)

            def values(width):
                _, _, ebits, fbits = FORMATS[width]
                emax = (1 << ebits) - 2
                candidates = [0, 1, 2, emax // 2, emax // 2 + 1, emax - 1, emax]
                return [(rng.randrange(2) << (width - 1)) |
                        (rng.choice(candidates) << fbits) | rng.getrandbits(fbits)
                        for _ in range(dimension)]

            left, right = values(left_width), values(right_width)
            add(f"finite_{left_width}_{right_width}_{index:03}", left, right,
                left_width, right_width, raw=True)
            if index < 4:
                add(f"reverse_{left_width}_{right_width}_{index:03}", left[::-1], right[::-1],
                    left_width, right_width, raw=True)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    controls = check_oracle()
    rows = cases()
    report = dict(
        status="independent_numerical_preparation",
        created_at=datetime.now(timezone.utc).isoformat(),
        source_commit="d29bff42c5b3aacf7a61463bf2c8f9cec2bdf216",
        native_engine_executed=False, implementation_accepted=False,
        reference="Exact rational products/differences; nearest-even binary64 midpoint comparisons",
        notation="Hexadecimal IEEE bits; null cosine/normalization means mathematically undefined zero norm, not a frozen public null policy",
        scope="Finite present equal-length inputs only; no embedding/type/null/nonfinite admission or tolerance decision",
        oracle_controls_passed=controls, case_count=len(rows), cases=rows,
        generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    )
    payload = json.dumps(report, indent=2).encode() + b"\n"
    assert len(payload) < 8 * 1024 * 1024
    with args.output.open("xb") as stream:
        stream.write(payload)
    print(json.dumps(dict(status=report["status"], cases=len(rows), oracle_controls=controls,
                          bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())))


if __name__ == "__main__":
    main()
