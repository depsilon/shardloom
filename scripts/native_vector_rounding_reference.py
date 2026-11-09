"""Freeze exact rational expectations for the prepared vector-rounding cases.

The case set predates native implementation. Expectations use the independent
binary64-neighbor oracle; no runtime rounding formula or engine is executed.
"""

import argparse
from fractions import Fraction
import hashlib
import importlib.util
import json
from pathlib import Path
import random


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "native_vector_reference.py"
SPEC = importlib.util.spec_from_file_location("vector_reference", SOURCE)
REFERENCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REFERENCE)


def rounding_cases():
    cases = {"zero": 0, "minimum_quantum": 1}
    for high in range(4262):
        cases[f"power_{high}"] = 1 << high
    for shift in (1, 63, 64, 65, 127, 128, 1073, 1074, 1075,
                  2047, 2048, 2095, 2096, 3117, 3118, 4095, 4096, 4209):
        for integer in ((1 << 52), (1 << 52) + 1, (1 << 53) - 1):
            center = (2 * integer + 1) << (shift - 1)
            for delta in (-1, 0, 1):
                cases[f"dot_midpoint_{shift}_{integer}_{delta}"] = center + delta
    for quantum in (-1074, -1073, -1050, -1023, -1022, -1000,
                    -500, -53, -52, -1, 0, 1, 500, 970, 971, 972, 1005):
        shift = 2148 + 2 * quantum
        for integer in (1, 2, 3, (1 << 52), (1 << 52) + 1, (1 << 53) - 1):
            boundary = Fraction((2 * integer + 1) ** 2, 4) * (1 << shift)
            if boundary.denominator != 1:
                floor = boundary.numerator // boundary.denominator
                cases[f"sqrt_boundary_floor_{quantum}_{integer}"] = floor
                cases[f"sqrt_boundary_ceil_{quantum}_{integer}"] = floor + 1
            else:
                for delta in (-1, 0, 1):
                    value = boundary.numerator + delta
                    if value >= 0:
                        cases[f"sqrt_midpoint_{quantum}_{integer}_{delta}"] = value
    rng = random.Random(0x524F554E44564543)
    for index in range(192):
        high = rng.randrange(4262)
        cases[f"random_{index}"] = (1 << high) | rng.getrandbits(high)
    return cases


def fixture():
    rows = []
    for name, magnitude in rounding_cases().items():
        exact = Fraction(magnitude, 1 << 2148)
        words = [[index, f"{(magnitude >> (64 * index)) & ((1 << 64) - 1):016x}"]
                 for index in range(68)
                 if (magnitude >> (64 * index)) & ((1 << 64) - 1)]
        assert magnitude.bit_length() <= 68 * 64
        rows.append(dict(id=name, words=words,
                         rounded=f"{REFERENCE.rounded_positive(exact, False):016x}",
                         sqrt=f"{REFERENCE.rounded_positive(exact, True):016x}"))
    assert len(rows) == 4918
    return dict(schema_version=1, reference="exact rational binary64 midpoint comparisons",
                native_engine_executed=False, case_count=len(rows),
                generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                oracle_sha256=hashlib.sha256(SOURCE.read_bytes()).hexdigest(), cases=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    data = json.dumps(fixture(), indent=2).encode() + b"\n"
    assert len(data) < 8 << 20
    if args.check:
        assert args.output.read_bytes() == data, "vector rounding fixture differs from oracle"
    else:
        with args.output.open("xb") as stream:
            stream.write(data)
    print(json.dumps(dict(status="passed", cases=4918, checks=9836,
                          bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                          native_engine_executed=False)))


if __name__ == "__main__":
    main()
