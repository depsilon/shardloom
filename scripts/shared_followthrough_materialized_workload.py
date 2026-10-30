# SPDX-License-Identifier: Apache-2.0
"""Finite, independent repeated-relation workload; no query-engine reference."""
from dataclasses import asdict, dataclass
import hashlib
import json
import random

from query_surface.semantics import ResultContract, ResultType, TypedResult, result_record


@dataclass(frozen=True)
class Fixture:
    name: str
    rows: int
    width: int
    seed: int
    dimension_keys: int = 127
    row_group_rows: int = 251


@dataclass(frozen=True)
class Case:
    name: str
    fixture: str
    consumers: tuple[int, ...]
    result: str = "aggregate"
    limit: int | None = None
    memory_bytes: int = 512 << 20
    spill: bool = False


FIXTURES = (
    Fixture("ordinary", 32771, 48, 0x30_2026_810),
    Fixture("wide", 4099, 96, 0x30_2026_811),
    Fixture("empty", 0, 48, 0x30_2026_812),
    Fixture("pressure", 32771, 1024, 0x30_2026_813),
    Fixture("semantic", 109, 17, 0x30_2026_814),
)
CASES = (
    Case("one_consumer", "ordinary", (1,)),
    Case("two_consumers", "ordinary", (1, 3)),
    Case("four_consumers", "ordinary", (1, 3, 7, 11)),
    Case("selective_consumers", "ordinary", (97, 131)),
    Case("wide_complete_result", "wide", (1, 3), "rows"),
    Case("few_rows", "ordinary", (1, 3), "rows", 5),
    Case("zero_rows", "ordinary", (1, 3), "rows", 0),
    Case("empty_input", "empty", (1, 3)),
    Case("spill_one_consumer", "pressure", (1,), memory_bytes=64 << 20, spill=True),
    Case("spill_two_consumers", "pressure", (1, 3), memory_bytes=64 << 20, spill=True),
)


def rows(spec):
    """Seeded logical values and layout, independent of native plan evaluation."""
    rng = random.Random(spec.seed)
    dimension = []
    for key in range(spec.dimension_keys):
        for variant in range(2 if key % 11 == 0 else 1):
            dimension.append(dict(key=key, variant=2 * key + variant,
                bucket=None if key % 19 == 0 else "region_" + str(key % 13),
                bias=None if key % 17 == 0 else key % 31 - 15))
    # A NULL build key must never match a NULL probe key under ordinary equality.
    dimension.append(dict(key=None, variant=-1, bucket="null_key", bias=1000))
    rng.shuffle(dimension)
    words = ("  ALPHA  ", " MÜNCHEN ", " 東京 ", "", "  İ  ", "  Straße  ")
    fact = []
    for row in range(spec.rows):
        word = words[rng.randrange(len(words))]
        text = None if row % 23 == 0 else word + str(row % 37) + " "
        long = (f"{row:08x}:AbC:" + "xY" * spec.width)[:spec.width]
        fact.append(dict(row_id=row,
            key=None if row % 29 == 0 else rng.randrange(spec.dimension_keys + 7),
            value=None if row % 31 == 0 else rng.randrange(-2001, 2002),
            text=text, wide_text=" " + long + " "))
    rng.shuffle(fact)
    return fact, dimension


def normalized_rows(spec):
    fact, dimension = rows(spec)
    index = {}
    for right in dimension:
        if right["key"] is not None:
            index.setdefault(right["key"], []).append(right)
    output = []
    for left in fact:
        matches = index.get(left["key"], ()) if left["key"] is not None else ()
        for right in matches or (None,):
            text = left["text"]
            text = None if text is None else text.strip()
            wide = left["wide_text"].strip()
            bias = 0 if right is None or right["bias"] is None else right["bias"]
            output.append(dict(row_id=left["row_id"], variant=None if right is None else right["variant"],
                bucket=None if right is None else right["bucket"],
                score=None if left["value"] is None else left["value"] + bias,
                label=None if text is None else text.lower(),
                loud_label=None if text is None else text.upper(),
                wide_label=wide.lower(), wide_loud=wide.upper()))
    return output


BASE_SCHEMA = tuple((name, ResultType(kind, True)) for name, kind in (
    ("row_id", "int64"), ("variant", "int64"), ("bucket", "utf8"), ("score", "int64"),
    ("label", "utf8"), ("loud_label", "utf8"), ("wide_label", "utf8"), ("wide_loud", "utf8")))
CONSUMER = (("consumer", ResultType("int64", False)),)
AGGREGATE_SCHEMA = CONSUMER + (("bucket", ResultType("utf8", True)),
    ("count", ResultType("uint64", False)), ("total", ResultType("float64", True)),
    ("chars", ResultType("float64", True)))


def reference(case, spec):
    normalized = normalized_rows(spec)
    output = []
    for consumer, divisor in enumerate(case.consumers):
        selected = [row for row in normalized if row["row_id"] % divisor == 0]
        if case.result == "rows":
            output.extend(dict(consumer=consumer, **row) for row in selected)
            continue
        groups = {}
        for row in selected:
            state = groups.setdefault(row["bucket"], [0, [], []])
            state[0] += 1
            if row["score"] is not None:
                state[1].append(row["score"])
            if row["label"] is not None:
                state[2].append(sum(len(row[name]) for name in
                    ("label", "loud_label", "wide_label", "wide_loud")))
        output.extend(dict(consumer=consumer, bucket=bucket, count=count,
            total=float(sum(values)) if values else None,
            chars=float(sum(lengths)) if lengths else None)
            for bucket, (count, values, lengths) in groups.items())
    if case.result == "rows":
        output.sort(key=lambda row: (row["consumer"], row["row_id"],
            row["variant"] is None, row["variant"] or 0))
        schema = CONSUMER + BASE_SCHEMA
    else:
        output.sort(key=lambda row: (row["consumer"], row["bucket"] is None, row["bucket"] or ""))
        schema = AGGREGATE_SCHEMA
    if case.limit is not None:
        output = output[:case.limit]
    result = TypedResult(schema, output)
    # Validate every value/width/nullability before it can become a frozen oracle.
    result_record(result)
    return result, len(normalized)


def sql(case, sources, materialized):
    def path(name):
        return "'" + str(sources[name]).replace("'", "''") + "'"
    base = ("SELECT f.row_id AS row_id,d.variant AS variant,d.bucket AS bucket,"
        "f.value+COALESCE(d.bias,0) AS score,LOWER(TRIM(f.text)) AS label,"
        "UPPER(TRIM(f.text)) AS loud_label,LOWER(TRIM(f.wide_text)) AS wide_label,"
        f"UPPER(TRIM(f.wide_text)) AS wide_loud FROM {path('fact')} AS f "
        f"LEFT JOIN {path('dimension')} AS d ON f.key=d.key")
    branches = []
    for consumer, divisor in enumerate(case.consumers):
        selected = ("row_id,variant,bucket,score,label,loud_label,wide_label,wide_loud"
            if case.result == "rows" else "bucket,COUNT(*) AS count,SUM(score) AS total,"
            "SUM(LENGTH(label)+LENGTH(loud_label)+LENGTH(wide_label)+LENGTH(wide_loud)) AS chars")
        branch = f"SELECT {consumer} AS consumer,{selected} FROM kept"
        if divisor != 1:
            branch += f" WHERE row_id%{divisor}=0"
        if case.result == "aggregate":
            branch += " GROUP BY bucket"
        branches.append(branch)
    order = "consumer,row_id,variant NULLS LAST" if case.result == "rows" else "consumer,bucket NULLS LAST"
    suffix = "" if case.limit is None else f" LIMIT {case.limit}"
    mode = "MATERIALIZED " if materialized else ""
    return f"WITH kept AS {mode}({base}) SELECT * FROM ({' UNION ALL '.join(branches)}) AS result ORDER BY {order}{suffix}"


def declaration(case, spec):
    return dict(case=asdict(case), fixture=asdict(spec),
        query_template=sql(case, {name: 'fixture://' + name for name in ('fact', 'dimension')}, True),
        ordinary_template=sql(case, {name: 'fixture://' + name for name in ('fact', 'dimension')}, False),
        ordered=True, exact_values=True)


def identity(case, spec):
    return hashlib.sha256(json.dumps(declaration(case, spec), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def contract(case):
    return ResultContract(CONSUMER + BASE_SCHEMA if case.result == "rows" else AGGREGATE_SCHEMA, ordered=True)
