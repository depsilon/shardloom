# SPDX-License-Identifier: Apache-2.0
"""Independent complete workflows for the ten-owner native performance batch.

This module imports no query engine. Python records, exact rational ranks,
stable sorting and complete joins define results. PyArrow only writes and
reopens source files. All case choices precede either runtime observation.
"""

from collections import defaultdict
from fractions import Fraction
import math


SEED = 0x31_2026_BA7
LEVELS = (0.0, 0.1, 0.5, 0.9, 1.0)
GRID = (
    ("B1", (65_537, 524_309), ("prefix_1", "prefix_127", "prefix_8193", "full")),
    ("B2", (4_097, 32_771), ("single", "case_length", "trim_lengths", "numeric")),
    ("B3", (4_097, 131_101), ("constant", "runend", "sliced_runend", "primitive")),
    ("B4", (2_051, 16_387), ("ascii", "concat_two", "concat_four", "unicode")),
    ("B5", (257, 2_051), ("inner_range", "left_two", "semi", "anti")),
    ("B6", (127, 1_025), ("exists", "in", "guarded_exists", "uncorrelated")),
    ("B7", (8_193, 65_537), ("sort", "join", "window_rank", "resident_sort")),
    ("B8", (8_193, 65_537), ("unique", "duplicates", "misses", "keyless")),
    ("B9", (4_097, 32_771), ("fragment_1", "fragment_17", "fragment_1024", "pressured")),
    ("B10", (4_097, 131_101), ("median", "continuous", "discrete", "grouped")),
)


def cases():
    return [dict(family=family, rows=size, shape=shape,
                 case_id=f"{family}_{shape}_{size}")
            for family, sizes, shapes in GRID for size in sizes for shape in shapes]


def positions(count):
    return [*range(1, count, 2), *reversed(range(0, count, 2))]


def numeric(row):
    return None if row % 19 == 0 else (row * 17 + SEED) % 257 - 128


def encoded_values(shape, count):
    if shape == "constant":
        return [7] * count
    skip = 7 if shape == "sliced_runend" else 0
    return [None if row % 19 == 0 else (row // 127 * 17 + SEED) % 257 - 128
            for row in positions(count + skip)[skip:]]


def text(row, width, *, unicode=False):
    if row % 19 == 0:
        return None
    alphabet = "AbZ 09_-" if not unicode else "Straße ΣΟΣ 東京 İ "
    start = (row * 17 + SEED) % len(alphabet)
    word = alphabet[start:] + alphabet[:start]
    return (word * ((width + len(word) - 1) // len(word)))[:width]


def records(case):
    family, shape, count = (case[key] for key in ("family", "shape", "rows"))
    order = positions(count)
    if family == "B1":
        return {"input": [dict(id=i, n=numeric(i), text=text(i, 127)) for i in order]}
    if family in ("B2", "B4"):
        return {"input": [dict(id=i, n=numeric(i), **{
            f"t{j}": text(i + j, (0, 7, 127, 1025)[(i + j) % 4], unicode=shape == "unicode")
            for j in range(4)}) for i in order]}
    if family == "B3":
        return {"input": [dict(n=value) for value in encoded_values(shape, count)]}
    if family == "B5":
        def side(size):
            return [dict(id=i, k=None if i % 19 == 0 else i % 31,
                         n=numeric(i)) for i in positions(size)]
        return {"left": side(count), "right": side(509)}
    if family == "B6":
        def side(size):
            return [dict(id=i, k=None if i % 19 == 0 else i % 127,
                         v=None if i % 19 == 0 or i % 37 == 0 else (i * 7 + SEED) % 257)
                    for i in positions(size)]
        return {"outer": side(count), "inner": side(4097)}
    if family in ("B7", "B8"):
        duplicate = family == "B7" or shape == "duplicates"
        modulus = 31 if family == "B8" and duplicate else 127
        width = 7 if shape == "resident_sort" else 1025
        build = [dict(id=i, k=None if i % 19 == 0 else i % modulus if duplicate else i,
                      payload=f"{i:08d}:" + "p" * width) for i in order]
        if family == "B7" and shape != "join":
            return {"input": build}
        probe = []
        for i in positions(257):
            key = (count + i * 251 if shape == "misses" else
                   i % modulus if duplicate else (i * 251 + SEED) % count)
            probe.append(dict(id=i, k=None if i % 19 == 0 else key))
        return {"left": probe, "right": build}
    if family == "B9":
        return {"input": [dict(id=i, n=numeric(i), **({
            f"p{j}": None if i % 19 == 0 else (i + j * 17) % 257 for j in range(16)
        } if shape == "pressured" else {})) for i in order]}
    if family == "B10":
        return {"input": [dict(id=i, grp=i % 7,
                               n=None if i % 19 == 0 else i % 127 if shape == "discrete" else i)
                          for i in order]}
    raise ValueError("unknown family")


def source_schema(case, name):
    import pyarrow as pa
    family, shape = case["family"], case["shape"]
    integer = lambda name, nullable=True: pa.field(name, pa.int64(), nullable=nullable)
    if family == "B1":
        fields = [integer("id", False), integer("n"), pa.field("text", pa.string())]
    elif family in ("B2", "B4"):
        fields = [integer("id", False), integer("n"), *(pa.field(f"t{j}", pa.string()) for j in range(4))]
    elif family == "B3":
        fields = [integer("n", shape != "constant")]
    elif family == "B5":
        fields = [integer("id", False), integer("k"), integer("n")]
    elif family == "B6":
        # The public object-source declaration uses nullable scalar fields.
        fields = [integer("id"), integer("k"), integer("v")]
    elif family in ("B7", "B8"):
        fields = [integer("id", False), integer("k")]
        if name != "left":
            fields.append(pa.field("payload", pa.string(), nullable=False))
    elif family == "B9":
        fields = [integer("id"), integer("n")]
        if shape == "pressured":
            fields += [integer(f"p{j}") for j in range(16)]
    elif family == "B10":
        fields = [integer("id", False), integer("grp", False), integer("n")]
    else:
        raise ValueError("unknown family")
    return pa.schema(fields)


def source_kind(case):
    return {"B3": "vortex", "B6": "memory", "B9": "batches"}.get(case["family"], "parquet")


def batch_columns(case):
    return ["id", "n", *([f"p{j}" for j in range(16)] if case["shape"] == "pressured" else [])]


def resources(case):
    spill = case["family"] in ("B7", "B8") and case["shape"] != "resident_sort"
    spill |= case["family"] == "B9" and case["shape"] == "pressured"
    memory = 64 << 20 if case["family"] == "B9" and spill else (
        16 << 20 if case["family"] in ("B7", "B8") else 2 << 30)
    return dict(memory_bytes=memory,
                parallelism=2, timeout_seconds=300, spill=spill,
                spill_buffer_bytes=1 << 20, spill_quota_bytes=256 << 20)


def quoted(value):
    return "'" + str(value).replace("'", "''") + "'"


def expressions(case):
    family, shape = case["family"], case["shape"]
    if family == "B2":
        return {"single": ["LOWER(t0)"], "case_length": ["LOWER(t0)", "UPPER(t0)", "LENGTH(t0)"],
                "trim_lengths": ["TRIM(t0)", "LENGTH(t0)", "BYTE_LENGTH(t0)"],
                "numeric": ["n+1", "n*2", "ABS(n)"]}[shape]
    return {"ascii": ["LOWER(t0)", "UPPER(t0)"],
            "concat_two": ["CONCAT(t0,t1)"],
            "concat_four": ["CONCAT(t0,t1,t2,t3)"],
            "unicode": ["LOWER(t0)", "UPPER(t0)"]}[shape]


def text_outputs(case, row):
    t = row["t0"]
    lower = None if t is None else t.lower()
    upper = None if t is None else t.upper()
    length = None if t is None else len(t)
    if case["family"] == "B4":
        if case["shape"].startswith("concat"):
            width = 2 if case["shape"] == "concat_two" else 4
            values = [row[f"t{i}"] for i in range(width)]
            return [None if any(value is None for value in values) else "".join(values)]
        return [lower, upper]
    n = row["n"]
    return {"single": [lower], "case_length": [lower, upper, length],
            "trim_lengths": [None if t is None else t.strip(), length, None if t is None else len(t.encode())],
            "numeric": [None if n is None else n + 1, None if n is None else n * 2, None if n is None else abs(n)]}[case["shape"]]


def statement(case, sources):
    family, shape = case["family"], case["shape"]
    source = lambda name: quoted(sources[name])
    if family == "B1":
        limit = "" if shape == "full" else " LIMIT " + shape.split("_")[1]
        return f"SELECT id,n,text FROM {source('input')}{limit}"
    if family in ("B2", "B4"):
        outputs = expressions(case)
        projection = ",".join(f"{value} AS v{i}" for i, value in enumerate(outputs))
        aggregate = []
        for i in range(len(outputs)):
            aggregate += [f"COUNT(v{i}) AS c{i}", f"MIN(v{i}) AS lo{i}", f"MAX(v{i}) AS hi{i}"]
            if family == "B4":
                aggregate += [f"MIN(LENGTH(v{i})) AS short{i}", f"MAX(LENGTH(v{i})) AS long{i}"]
        return f"SELECT {','.join(aggregate)} FROM (SELECT {projection} FROM {source('input')}) AS p"
    if family == "B3":
        return (f"SELECT valid,lo+0 AS lo,hi+0 AS hi FROM (SELECT COUNT(n) AS valid,MIN(n) AS lo,MAX(n) AS hi "
                f"FROM {source('input')}) AS a WHERE valid>=0")
    if family == "B5":
        kind = {"inner_range": "INNER", "left_two": "LEFT", "semi": "LEFT SEMI", "anti": "LEFT ANTI"}[shape]
        output = "l.id AS lid,l.n AS ln" + ("" if shape in ("semi", "anti") else ",r.id AS rid,r.n AS rn")
        condition = "l.k=r.k AND l.n<r.n" + (" AND l.id<>r.id" if shape == "left_two" else "")
        return f"SELECT {output} FROM {source('left')} AS l {kind} JOIN {source('right')} AS r ON {condition}"
    if family == "B6":
        inner = f"SELECT v FROM {source('inner')} WHERE k=outer.k"
        predicate = f"EXISTS ({inner})" if "exists" in shape else f"v IN ({inner})"
        if shape == "guarded_exists":
            predicate = f"CASE WHEN id%2=0 THEN {predicate} ELSE FALSE END"
        elif shape == "uncorrelated":
            predicate = f"v IN (SELECT v FROM {source('inner')})"
        return f"SELECT id,k,v,{predicate} AS answer FROM {source('outer')}"
    if family == "B7" and shape != "join":
        if shape == "window_rank":
            return (f"SELECT id,LENGTH(payload) AS width,rank FROM (SELECT id,payload,"
                    f"RANK() OVER (ORDER BY k ASC NULLS LAST) AS rank FROM {source('input')}) AS w")
        return (f"SELECT id,k,LENGTH(payload) AS width FROM (SELECT id,k,payload FROM "
                f"{source('input')} ORDER BY k ASC NULLS LAST) AS s")
    if family in ("B7", "B8"):
        condition = "l.k=r.k" if shape != "keyless" else "r.id<l.k AND r.id>=l.k-1"
        joined = (f"SELECT l.id AS lid,r.id AS rid,LENGTH(r.payload) AS width FROM {source('left')} AS l "
                  f"LEFT JOIN {source('right')} AS r ON {condition}")
        return ("SELECT lid,COUNT(rid) AS matches,MIN(rid) AS first_id,MAX(rid) AS last_id,"
                f"MAX(width) AS width FROM ({joined}) AS j GROUP BY lid ORDER BY lid")
    if family == "B9":
        names = batch_columns(case)
        reductions = ",".join(f"COUNT({name}) AS c{i},MIN({name}) AS lo{i},MAX({name}) AS hi{i}"
                              for i, name in enumerate(names))
        output = ",".join(f"{side}.{field}{i} AS {side}_{field}{i}"
                          for side in ("a", "b") for i in range(len(names)) for field in ("c", "lo", "hi"))
        return (f"SELECT {output} FROM (SELECT {reductions} FROM {source('input')}) AS a CROSS JOIN "
                f"(SELECT {reductions} FROM {source('input')}) AS b")
    if family == "B10":
        projection = ("MEDIAN(n) AS q0" if shape in ("median", "grouped") else
                      ",".join(f"QUANTILE_{'DISC' if shape == 'discrete' else 'CONT'}(n,{level}) AS q{i}"
                               for i, level in enumerate(LEVELS)))
        grouping = " GROUP BY grp ORDER BY grp" if shape == "grouped" else ""
        return f"SELECT {'grp,' if shape == 'grouped' else ''}{projection} FROM {source('input')}{grouping}"
    raise ValueError("unknown family")


def reductions(values):
    present = [value for value in values if value is not None]
    return len(present), min(present, default=None), max(present, default=None)


def null_last(value):
    return value is None, value or 0


def in_set(value, candidates):
    if not candidates:
        return False
    if value is not None and value in candidates:
        return True
    return None if value is None or None in candidates else False


def quantile(values, level, discrete=False):
    ordered = sorted(value for value in values if value is not None)
    if not ordered:
        return None
    rank = Fraction.from_float(level) * (len(ordered) if discrete else len(ordered) - 1)
    if discrete:
        return ordered[max(0, math.ceil(rank) - 1)]
    lower, upper = math.floor(rank), math.ceil(rank)
    return float(ordered[lower]) + (ordered[upper] - ordered[lower]) * float(rank - lower)


def reference_rows(case, sources):
    family, shape = case["family"], case["shape"]
    if family == "B1":
        count = case["rows"] if shape == "full" else int(shape.split("_")[1])
        return sources["input"][:count]
    if family in ("B2", "B4"):
        columns = list(zip(*(text_outputs(case, row) for row in sources["input"])))
        result = {}
        for i, column in enumerate(columns):
            result.update(zip((f"c{i}", f"lo{i}", f"hi{i}"), reductions(column)))
            if family == "B4":
                _, small, large = reductions([None if v is None else len(v) for v in column])
                result.update({f"short{i}": small, f"long{i}": large})
        return [result]
    if family == "B3":
        return [dict(zip(("valid", "lo", "hi"), reductions([r["n"] for r in sources["input"]])))]
    if family == "B5":
        result = []
        for left in sources["left"]:
            matches = [right for right in sources["right"]
                       if left["k"] is not None and left["k"] == right["k"]
                       and left["n"] is not None and right["n"] is not None and left["n"] < right["n"]
                       and (shape != "left_two" or left["id"] != right["id"])]
            row = dict(lid=left["id"], ln=left["n"])
            if shape in ("semi", "anti"):
                if bool(matches) == (shape == "semi"):
                    result.append(row)
            else:
                if not matches and shape == "left_two":
                    matches = [dict(id=None, n=None)]
                result += [dict(row, rid=right["id"], rn=right["n"]) for right in matches]
        return result
    if family == "B6":
        result = []
        for left in sources["outer"]:
            candidates = sources["inner"] if shape == "uncorrelated" else [r for r in sources["inner"]
                if left["k"] is not None and r["k"] == left["k"]]
            answer = bool(candidates) if "exists" in shape else in_set(left["v"], [r["v"] for r in candidates])
            if shape == "guarded_exists" and left["id"] % 2:
                answer = False
            result.append(dict(left, answer=answer))
        return result
    if family == "B7" and shape != "join":
        ordered = sorted(sources["input"], key=lambda r: null_last(r["k"]))
        if shape == "window_rank":
            ranks = {}
            for index, row in enumerate(ordered, 1):
                ranks.setdefault(row["k"], index)
            return [dict(id=r["id"], width=len(r["payload"]), rank=ranks[r["k"]]) for r in sources["input"]]
        return [dict(id=r["id"], k=r["k"], width=len(r["payload"])) for r in ordered]
    if family in ("B7", "B8"):
        result = []
        by_key = defaultdict(list)
        for row in sources["right"]:
            by_key[row["id"] if shape == "keyless" else row["k"]].append(row)
        for left in sorted(sources["left"], key=lambda r: r["id"]):
            key = left["k"]
            matches = [] if key is None else by_key[key - 1 if shape == "keyless" else key]
            count, first, last = reductions([row["id"] for row in matches])
            result.append(dict(lid=left["id"], matches=count, first_id=first, last_id=last,
                               width=max((len(r["payload"]) for r in matches), default=None)))
        return result
    if family == "B9":
        result = {}
        for side in ("a", "b"):
            for i, name in enumerate(batch_columns(case)):
                result.update(zip((f"{side}_{field}{i}" for field in ("c", "lo", "hi")),
                                  reductions([row[name] for row in sources["input"]])))
        return [result]
    if family == "B10":
        groups = defaultdict(list)
        for row in sources["input"]:
            groups[row["grp"] if shape == "grouped" else 0].append(row["n"])
        levels = (0.5,) if shape in ("median", "grouped") else LEVELS
        return [dict({"grp": group} if shape == "grouped" else {}, **{
            f"q{i}": quantile(values, level, shape == "discrete") for i, level in enumerate(levels)})
            for group, values in sorted(groups.items())]
    raise ValueError("unknown family")


def output_fields(case):
    """Exact result types without importing a file or query provider."""
    family, shape = case["family"], case["shape"]
    integer = lambda name, nullable=True: (name, "int64", nullable)
    count = lambda name: (name, "uint64", False)
    if family == "B1":
        return [integer("id", False), integer("n"), ("text", "utf8", True)]
    if family in ("B2", "B4"):
        fields = []
        for i, expression in enumerate(expressions(case)):
            dtype = "int64" if expression in ("LENGTH(t0)", "BYTE_LENGTH(t0)", "n+1", "n*2", "ABS(n)") else "utf8"
            fields += [count(f"c{i}"), (f"lo{i}", dtype, True), (f"hi{i}", dtype, True)]
            if family == "B4":
                fields += [integer(f"short{i}"), integer(f"long{i}")]
        return fields
    if family == "B3":
        return [count("valid"), integer("lo"), integer("hi")]
    if family == "B5":
        fields = [integer("lid", False), integer("ln")]
        if shape not in ("semi", "anti"):
            fields += [integer("rid", shape == "left_two"), integer("rn")]
        return fields
    if family == "B6":
        return [*(integer(name) for name in ("id", "k", "v")),
                ("answer", "bool", shape in ("in", "uncorrelated"))]
    if family == "B7" and shape != "join":
        return ([integer("id", False), integer("width", False), integer("rank", False)] if shape == "window_rank" else
                [integer("id", False), integer("k"), integer("width", False)])
    if family in ("B7", "B8"):
        return [integer("lid", False), count("matches"), integer("first_id"), integer("last_id"), integer("width")]
    if family == "B9":
        return [field for side in ("a", "b") for i in range(len(batch_columns(case)))
                for field in (count(f"{side}_c{i}"), integer(f"{side}_lo{i}"), integer(f"{side}_hi{i}"))]
    if family == "B10":
        length = 1 if shape in ("median", "grouped") else 5
        return ([integer("grp", False)] if shape == "grouped" else []) + [
            (f"q{i}", "int64" if shape == "discrete" else "float64", True) for i in range(length)]
    raise ValueError("unknown family")


def output_schema(case):
    import pyarrow as pa
    types = dict(int64=pa.int64(), uint64=pa.uint64(), utf8=pa.string(), bool=pa.bool_(), float64=pa.float64())
    return pa.schema([pa.field(name, types[dtype], nullable=nullable)
                      for name, dtype, nullable in output_fields(case)])


def reference(case, sources):
    from query_surface.semantics import TypedResult
    from shardloom._result_schema import ResultType
    fields = tuple((name, ResultType(dtype, nullable)) for name, dtype, nullable in output_fields(case))
    return TypedResult(fields, reference_rows(case, sources))


def write_parquet(case, directory, sources):
    import pyarrow as pa
    import pyarrow.parquet as pq
    paths = {}
    for name, rows in sources.items():
        path = directory / f"{name}.parquet"
        if path.exists():
            raise ValueError("fixture destination already exists")
        table = pa.Table.from_pylist(rows, schema=source_schema(case, name))
        row_group_rows = 256 if case["family"] in ("B7", "B8") else len(rows)
        options = dict(row_group_size=row_group_rows, use_dictionary=False, compression="zstd")
        if case["family"] == "B1":
            options.update(write_batch_size=1024, max_rows_per_page=1024, data_page_size=1)
        pq.write_table(table, path, **options)
        actual = pq.read_table(path)
        if actual.schema != table.schema or not actual.equals(table):
            raise AssertionError("complete independent source readback differs")
        paths[name] = path
    return paths
