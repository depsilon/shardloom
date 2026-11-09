"""Independent bit fixtures and exact rational assertions for vector acceptance."""

from fractions import Fraction
import json
import math
from pathlib import Path
import struct

FIXTURES = Path(__file__).resolve().parents[2] / "shardloom-vortex/tests/fixtures"


def corpus(name):
    return json.loads((FIXTURES / ("native_vector_" + name + ".json")).read_text())


def floating(width, bits):
    return struct.unpack({16: "<e", 32: "<f", 64: "<d"}[width],
                         int(bits, 16).to_bytes(width // 8, "little"))[0]


def hexadecimal(value):
    return struct.pack(">d", value).hex()


def float_array(pa, width, values):
    """Preserve original float bits, including signalling NaNs and signed zeros."""
    data, mask = bytearray(), bytearray((len(values) + 7) // 8)
    for index, value in enumerate(values):
        data.extend(int(value or "0", 16).to_bytes(width // 8, "little"))
        if value is not None:
            mask[index // 8] |= 1 << (index % 8)
    dtype = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}[width]
    return pa.Array.from_buffers(dtype, len(values), [pa.py_buffer(mask), pa.py_buffer(data)])


def dense_array(pa, width, rows, fixed=None):
    values, offsets = [], [0]
    for row in rows:
        values.extend(row if row is not None else [None] * (fixed or 0))
        offsets.append(len(values))
    child = float_array(pa, width, values)
    mask = pa.array([row is None for row in rows], pa.bool_())
    if fixed is not None:
        return pa.FixedSizeListArray.from_arrays(child, fixed, mask=mask)
    return pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), child, mask=mask)


def rational_values(width, values):
    return [Fraction.from_float(floating(width, value)) for value in values]


def ratio_bound(test, actual, squared, negative, multiplier, label):
    """Check abs(y-x) <= c*u*abs(x)+eta/2 using only exact squared inequalities."""
    test.assertTrue(math.isfinite(actual), label)
    test.assertEqual(hexadecimal(actual)[0] in "89abcdef", negative, label)
    magnitude = Fraction.from_float(abs(actual))
    half_subnormal = Fraction(1, 1 << 1075)
    relative = Fraction(multiplier, 1 << 53)
    if magnitude > half_subnormal:
        test.assertLessEqual((magnitude - half_subnormal) ** 2,
                             (1 + relative) ** 2 * squared, label)
    test.assertGreaterEqual((magnitude + half_subnormal) ** 2,
                            (1 - relative) ** 2 * squared, label)


def assert_numeric(test, case, name, actual):
    expected = case["expected"][name]
    label = case["id"] + ":" + name
    if expected is None:
        test.assertIsNone(actual, label)
        return
    if name not in {"cosine", "normalized_left"}:
        test.assertEqual(hexadecimal(actual), expected, label)
        return
    left = rational_values(case["left_width"], case["left"])
    left_square = sum((v * v for v in left), Fraction())
    if name == "normalized_left":
        test.assertEqual(len(actual), len(left), label)
        for index, (value, original) in enumerate(zip(actual, left)):
            negative = int(case["left"][index], 16) >> (case["left_width"] - 1) != 0
            ratio_bound(test, value, original * original / left_square, negative, 3, label)
    else:
        right = rational_values(case["right_width"], case["right"])
        right_square = sum((v * v for v in right), Fraction())
        dot = sum((a * b for a, b in zip(left, right)), Fraction())
        test.assertLessEqual(abs(actual), 1.0, label)
        ratio_bound(test, actual, dot * dot / (left_square * right_square), dot < 0, 5, label)


NUMERICAL_OPERATIONS = (
    ("dot", "vector_dot", ("a", "b")),
    ("norm_left", "vector_norm", ("a",)),
    ("norm_right", "vector_norm", ("b",)),
    ("l2_distance", "vector_l2_distance", ("a", "b")),
    ("cosine", "vector_cosine_similarity", ("a", "b")),
    ("normalized_left", "vector_normalize", ("a",)),
)


def overflow(expected):
    return isinstance(expected, str) and expected in {"7ff0000000000000", "fff0000000000000"}
