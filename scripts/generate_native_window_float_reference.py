#!/usr/bin/env python3
"""Independent exact-rational fixtures for native moving F64 totals."""

from __future__ import annotations

import argparse
import json
import math
import struct
import sys
from fractions import Fraction
from pathlib import Path


def bits(value: float) -> str:
    return struct.pack(">d", value).hex()


def case(values: list[float]) -> dict:
    total = sum((Fraction.from_float(value) for value in values), Fraction())
    try:
        result = bits(float(total))
    except OverflowError:
        result = None
    # Fraction canonicalizes exact cancellation, including signed zeros, to +0.
    return {"inputs": [bits(value) for value in values], "sum_bits": result,
            "avg_bits": bits(float(total / len(values)))}


def document() -> dict:
    tiny = float.fromhex("0x0.0000000000001p-1022")
    maximum = sys.float_info.max
    sequences = [
        [maximum, maximum], [maximum, maximum, -maximum], [1e300, 1.0, -1e300],
        [tiny, 0.0], [3 * tiny, 0.0], [-tiny, 0.0], [1.0, 2.0**-53],
        [math.nextafter(1.0, 2.0), 2.0**-53],
        [sys.float_info.min, -float.fromhex("0x0.fffffffffffffp-1022")],
        [maximum, -maximum], [0.0, -0.0],
    ]
    cases = [case(values[start:]) for values in sequences for start in range(len(values))]
    state = 0x6A09E667F3BCC909
    exponents = [0, 1, 2, 100, 512, 1022, 1023, 1024, 1535, 2045, 2046]
    for index in range(96):
        values = []
        for _ in range(1 + index % 16):
            state = (6364136223846793005 * state + 1442695040888963407) & ((1 << 64) - 1)
            representation = ((state >> 63) << 63) | (exponents[state % len(exponents)] << 52) | (state & ((1 << 52) - 1))
            values.append(struct.unpack(">d", representation.to_bytes(8, "big"))[0])
        cases.append(case(values))
    return {"schema_version": 1, "oracle": "Python fractions.Fraction", "cases": cases}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] /
                        "shardloom-vortex/tests/fixtures/native_window_float_reference.json")
    args = parser.parse_args()
    encoded = (json.dumps(document(), indent=2, allow_nan=False) + "\n").encode()
    if args.check:
        if args.output.read_bytes() != encoded:
            raise SystemExit("native window floating reference differs from its exact rational oracle")
        print("120 exact-rational native window floating cases match")
    else:
        with args.output.open("xb") as stream:
            stream.write(encoded)


if __name__ == "__main__":
    main()
