# SPDX-License-Identifier: Apache-2.0
"""Predeclare full sampling pressure outputs without invoking ShardLoom.

The existing independent oracle ranks the whole population or scans cumulative
weights for each draw. Fractions here are exact binary fractions. The weighted
fixture excludes source row zero, then uses each remaining source index as its
positive weight; all sums are exact in binary64.
"""
import argparse
import json
from pathlib import Path

from native_unary_reference import sample_ordinals


def cases_for(rows, width, grant, replacement_count):
    cases = []
    for weighted in (False, True):
        first = int(weighted)
        population = rows - first
        weights = list(range(1, rows)) if weighted else None
        for mode, replace, count, fraction in (
            ("fraction", False, None, 0.5),
            ("replacement", True, replacement_count, None),
            ("fraction-replacement", True, None, 1 / 64),
        ):
            # Exact integer ceiling for these power-of-two denominators only.
            denominator = 2 if fraction == 0.5 else 64
            target = count if count is not None else (population + denominator - 1) // denominator
            selected = sample_ordinals(population, target, 7, weights=weights, replace=replace)
            cases.append({
                "name": mode + ("-weighted" if weighted else "-unweighted"),
                "rows": rows, "width": width, "grant_bytes": grant,
                "first": first, "population": population, "seed": 7,
                "weighted": weighted, "replace": replace, "count": count,
                "fraction": fraction, "expected": [first + row for row in selected],
            })
    return cases


def fixture():
    return {"schema": "shardloom.sampling_population_reference.v1",
            "cases": cases_for(8193, 2048, 16 << 20, 3),
            "failure_cases": cases_for(65, 32768, 32 << 20, 7),
            "typed_cases": cases_for(6, 600 << 10, 64 << 20, 11)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    destination = Path(__file__).resolve().parents[1] / "shardloom-vortex/tests/fixtures/native_sampling_population_reference.json"
    text = json.dumps(fixture(), separators=(",", ":")) + "\n"
    if args.check:
        assert destination.read_text() == text, "sampling reference fixture differs"
    else:
        destination.write_text(text)


if __name__ == "__main__":
    main()
