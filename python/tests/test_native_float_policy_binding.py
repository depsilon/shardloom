# SPDX-License-Identifier: Apache-2.0
"""Independent floating-domain checks through the real public execution owners."""

from fractions import Fraction
import json
import math
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


NAN64 = 0x7ff8000000000000
BITS64 = [0x8000000000000000, 0, 0xfff0000000000000, 0x7ff0000000000000,
          0xfff0000000000042, 0x7ff8000000000081, 0x7ff0000000000001,
          1, 0x8000000000000001, 0x3ff0000000000000, 0xbff0000000000000, None,
          0x7fefffffffffffff, 0xffefffffffffffff]
BITS32 = [0x80000000, 0, 0xff800000, 0x7f800000,
          0xff800042, 0x7fc00081, 0x7f800001,
          1, 0x80000001, 0x3f800000, 0xbf800000, None, 0x7f7fffff, 0xff7fffff]


def float_value(bits, width=64):
    return None if bits is None else struct.unpack("<d" if width == 64 else "<f",
        bits.to_bytes(width // 8, "little"))[0]


def result_bits(value):
    if value is None:
        return None
    return NAN64 if math.isnan(value) else int.from_bytes(struct.pack("<d", value), "little")


def bit_class(bits, width=64):
    """Classify raw input without evaluating or quieting a signaling NaN."""
    if bits is None:
        return None
    mantissa = 52 if width == 64 else 23
    exponent = (1 << (width - mantissa - 1)) - 1
    special = ((bits >> mantissa) & exponent) == exponent
    payload = bits & ((1 << mantissa) - 1)
    return {"is_nan": special and payload != 0, "is_infinite": special and payload == 0,
            "is_finite": not special, "signbit": bool(bits >> (width - 1))}


def semantic_key(bits, width=64):
    category = bit_class(bits, width)
    if category is None:
        return (4, 0)
    if category["is_nan"]:
        return (3, 0)
    if category["is_infinite"]:
        return (0 if category["signbit"] else 2, 0)
    return (1, Fraction.from_float(float_value(bits, width)))


def reduce_bits(bits, function, width=64, level=None):
    """Independent value-level reference; no engine implementation is imported."""
    present = [bit for bit in bits if bit is not None]
    if function.endswith("_distinct"):
        first = {}
        for bit in present:
            first.setdefault(semantic_key(bit, width), bit)
        present = list(first.values())
        function = function.removesuffix("_distinct")
    if function == "count":
        return len(present)
    if not present:
        return None
    ordered = sorted(present, key=lambda bit: semantic_key(bit, width))
    if function in ("min", "max", "quantile_disc"):
        if function == "min":
            key = semantic_key(ordered[0], width)
        elif function == "max":
            key = semantic_key(ordered[-1], width)
        else:
            rank = max(0, math.ceil(Fraction.from_float(level) * len(ordered)) - 1)
            return ordered[rank]
        return next(bit for bit in present if semantic_key(bit, width) == key)
    values = [float_value(bit, width) for bit in present]
    if function in ("sum", "avg"):
        if any(math.isnan(value) for value in values) or (math.inf in values and -math.inf in values):
            value = math.nan
        elif math.inf in values:
            value = math.inf
        elif -math.inf in values:
            value = -math.inf
        else:
            exact = sum(map(Fraction.from_float, values), Fraction())
            value = float(exact / (len(values) if function == "avg" else 1))
        return result_bits(value)
    if function in ("median", "quantile_cont"):
        level = 0.5 if function == "median" else level
        rank = Fraction.from_float(level) * (len(ordered) - 1)
        low, high = math.floor(rank), math.ceil(rank)
        left, right = float_value(ordered[low], width), float_value(ordered[high], width)
        if low == high or ordered[low] == ordered[high]:
            value = left
        elif math.isnan(left) or math.isnan(right) or (left == -math.inf and right == math.inf):
            value = math.nan
        elif math.isinf(left) or math.isinf(right):
            value = left if math.isinf(left) else right
        else:
            weight = float(rank - low)
            value = left * (1-weight) + right * weight
        return result_bits(value)
    if function in ("var_pop", "var_samp", "stddev_pop", "stddev_samp"):
        correction = int(function.endswith("samp"))
        if len(values) <= correction:
            return None
        if any(not math.isfinite(value) for value in values):
            return NAN64
        values = list(map(Fraction.from_float, values))
        mean = sum(values, Fraction()) / len(values)
        variance = sum(((value-mean)**2 for value in values), Fraction()) / (len(values)-correction)
        return result_bits(math.sqrt(float(variance)) if function.startswith("stddev") else float(variance))
    raise AssertionError(function)


def describe(table):
    """Complete logical values; floating observations are raw little-endian bits."""
    import pyarrow as pa

    def value(array, index):
        if not array[index].is_valid:
            return None
        dtype = array.type
        if pa.types.is_dictionary(dtype):
            return value(array.dictionary, array.indices[index].as_py())
        if pa.types.is_floating(dtype):
            width = dtype.bit_width // 8
            offset = (array.offset + index) * width
            return {"bits": bytes(memoryview(array.buffers()[1])[offset:offset + width]).hex()}
        if pa.types.is_struct(dtype):
            return {field.name: value(array.field(i), index) for i, field in enumerate(dtype)}
        if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            start, end = array.offsets[index].as_py(), array.offsets[index + 1].as_py()
            return [value(array.values, i) for i in range(start, end)]
        return array[index].as_py()

    return [{"name": field.name, "type": str(field.type), "nullable": field.nullable,
             "values": [value(array, i) for array in chunked.chunks
                        for i in range(len(array))]}
            for field, chunked in zip(table.schema, table.columns)]


class NativeFloatPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    def setUp(self):
        import shardloom as sl
        self.real_popen = subprocess.Popen
        self.process = patch("subprocess.Popen", side_effect=AssertionError("external execution"))
        self.process.start()
        self.addCleanup(self.process.stop)
        self.directory = tempfile.TemporaryDirectory(prefix="shardloom-float-policy-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.sessions, self.clients = {}, {}
        for policy in ("strict", "extended"):
            session = self.native.Session(self.native.ExecutionResources(128 << 20, 2), float_policy=policy)
            client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20,
                                       max_parallelism=2, float_policy=policy)
            self.addCleanup(session.close)
            self.addCleanup(client.close)
            self.sessions[policy], self.clients[policy] = session, client
        self.sequence = 0

    def column(self, bits, width=64):
        present = bytearray((len(bits) + 7) // 8)
        for index, number in enumerate(bits):
            if number is not None:
                present[index // 8] |= 1 << (index % 8)
        return self.pa.Array.from_buffers(self.pa.float64() if width == 64 else self.pa.float32(),
            len(bits), [self.pa.py_buffer(bytes(present)),
                       self.pa.py_buffer(b"".join((number or 0).to_bytes(width // 8, "little") for number in bits))],
            null_count=sum(number is None for number in bits))

    def fixture(self):
        pa = self.pa
        return pa.Table.from_arrays([pa.array(range(len(BITS64)), pa.int64()),
            self.column(BITS64), self.column(BITS32, 32)], schema=pa.schema([
                pa.field("id", pa.int64(), nullable=False), pa.field("f64", pa.float64()),
                pa.field("f32", pa.float32())]))

    def write_input(self, table, extension="arrow", name="input"):
        path = self.root / f"{name}.{extension}"
        if extension == "parquet":
            import pyarrow.parquet as parquet
            parquet.write_table(table, path, row_group_size=3, use_dictionary=True)
            self.equal(parquet.read_table(path), table)
        elif extension == "vortex":
            source = self.sessions["extended"].from_arrow(table)
            self.sessions["extended"].sql("SELECT * FROM 'memory://fixture'",
                sources=[("memory://fixture", source)]).write(path, "write_vortex")
        else:
            with self.pa.OSFile(str(path), "wb") as stream, self.pa.ipc.new_file(stream, table.schema) as writer:
                writer.write_table(table, max_chunksize=3)
            self.equal(self.pa.ipc.open_file(str(path)).read_all(), table)
        return path

    def frame(self, path, policy="extended"):
        import shardloom as sl
        reader = {".vortex": sl.read_vortex, ".arrow": sl.read_arrow_ipc, ".parquet": sl.read_parquet}[path.suffix]
        return reader(path, client=self.clients[policy])

    def equal(self, actual, expected, *, approximate=()):
        observed, wanted = describe(actual), describe(expected)
        for actual_column, expected_column in zip(observed, wanted):
            if expected_column["name"] not in approximate:
                continue
            for row, (value, reference) in enumerate(zip(actual_column["values"], expected_column["values"])):
                if value is None or reference is None:
                    continue
                number = struct.unpack("<d", bytes.fromhex(value["bits"]))[0]
                target = struct.unpack("<d", bytes.fromhex(reference["bits"]))[0]
                if math.isfinite(target):
                    # Only named finite statistical outputs admit their existing
                    # numerical tolerance. Special values and carriage stay exact.
                    self.assertTrue(math.isclose(number, target, rel_tol=1e-14, abs_tol=1e-14),
                                    (actual_column["name"], row, number, target))
                    actual_column["values"][row] = reference
        self.assertEqual(observed, wanted)

    def native_table(self, result, policy):
        report = result.report
        self.assertEqual(report["float_policy"], policy)
        self.assertEqual(report["fallback_attempted"], "false")
        self.assertEqual(report["external_engine_invoked"], "false")
        return self.pa.RecordBatchReader.from_stream(result).read_all()

    def check_envelope(self, envelope, policy):
        self.assertEqual(envelope.field("float_policy"), policy)
        self.assertFalse(envelope.fallback.attempted)
        self.assertEqual(envelope.field("external_engine_invoked"), "false")

    def five(self, frame, relation, sql, expected, *, approximate=()):
        """Four in-process collections plus the CLI's complete Arrow output."""
        import shardloom as sl
        policy = frame.client.float_policy
        session = self.sessions[policy]
        result = frame.collect(check=True)
        self.check_envelope(result.envelope, policy)
        results = [("dataframe", result.to_arrow())]
        sql_result = sl.sql(sql, client=frame.client).collect(check=True)
        self.check_envelope(sql_result.envelope, policy)
        results.append(("python_sql", sql_result.to_arrow()))
        for name, plan in (("typed", session.plan(relation)),
                           ("workflow", session.workflow(["sql", "--sql", sql, "--request", "collect",
                             "--bounded", "true", "--memory-bytes", str(128 << 20), "--max-parallelism", "2"]))):
            results.append((name, self.native_table(plan.collect(), policy)))
        self.sequence += 1
        output = self.root / f"cli-{self.sequence}.arrow"
        with patch("subprocess.Popen", self.real_popen):
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                                    transport="subprocess", use_persistent_worker=False,
                                    memory_bytes=128 << 20, max_parallelism=2, timeout=30, float_policy=policy)
            try:
                report = sl.sql(sql, client=cli).write(output, output_format="arrow-ipc", check=True)
                self.check_envelope(report.envelope, policy)
                results.append(("cli", self.pa.ipc.open_file(str(output)).read_all()))
            finally:
                cli.close()
        for name, table in results:
            with self.subTest(surface=name):
                self.equal(table, expected, approximate=approximate)
        return results

    def test_float_carriage_and_classification_match_five_surfaces(self):
        from shardloom import functions as f
        pa, native, source = self.pa, self.native, self.fixture()
        for extension in ("arrow", "parquet", "vortex"):
            path = self.write_input(source, extension)
            base, scan = self.frame(path), native.Relation.scan(str(path))
            self.five(base, scan, f"SELECT * FROM '{path}'", source)
            self.five(base.limit(0), scan.limit(0), f"SELECT * FROM '{path}' LIMIT 0", source.slice(0, 0))
            selected, typed, arrays, fields = ["id"], [("id", native.Expression.column("id"))], [source["id"]], [source.schema.field("id")]
            for column, bits, width in (("f64", BITS64, 64), ("f32", BITS32, 32)):
                for function in ("is_nan", "is_infinite", "is_finite", "signbit"):
                    name = column + "_" + function
                    selected.append(getattr(f, function)(column).alias(name))
                    typed.append((name, native.Expression.call(function, [native.Expression.column(column)])))
                    arrays.append(pa.array([None if bit is None else bit_class(bit, width)[function] for bit in bits], pa.bool_()))
                    fields.append(pa.field(name, pa.bool_()))
            expected = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
            sql = "SELECT " + ",".join(map(str, selected)) + f" FROM '{path}'"
            self.five(base.select(*selected), scan.project(typed), sql, expected)
            if extension == "vortex":
                self.five(self.frame(path, "strict").select(*selected), scan.project(typed), sql, expected)

    def test_float_keys_use_one_order_and_preserve_representative_bits(self):
        from shardloom import functions as f
        pa, native, source = self.pa, self.native, self.fixture()
        path = self.write_input(source)
        for name, bits, width in (("f64", BITS64, 64), ("f32", BITS32, 32)):
            ordered = sorted(range(len(bits)), key=lambda row: semantic_key(bits[row], width))
            base, scan = self.frame(path), native.Relation.scan(str(path))
            self.five(base.sort(name, nulls="last"), scan.sort([(name, False, "last")]),
                f"SELECT * FROM '{path}' ORDER BY {name} NULLS LAST", source.take(pa.array(ordered)))
            groups = {}
            for row, number in enumerate(bits):
                key = semantic_key(number, width)
                if key not in groups:
                    groups[key] = [row, 0]
                groups[key][1] += 1
            entries = [groups[key] for key in sorted(groups)]
            expected = pa.Table.from_arrays([self.column([bits[row] for row, _ in entries], width),
                pa.array([count for _, count in entries], pa.uint64())],
                schema=pa.schema([source.schema.field(name), pa.field("n", pa.uint64(), nullable=False)]))
            self.five(base.group_by(name).agg(f.count().alias("n")).sort(name, nulls="last"),
                scan.aggregate([name], [native.AggregateMeasure("count", None, "n")]).sort([(name, False, "last")]),
                f"SELECT {name},COUNT(*) AS n FROM '{path}' GROUP BY {name} ORDER BY {name} NULLS LAST", expected)
            self.five(base.select(name).distinct().sort(name, nulls="last"),
                scan.project([(name, native.Expression.column(name))]).unary("distinct_rows").sort([(name, False, "last")]),
                f"SELECT DISTINCT {name} FROM '{path}' ORDER BY {name} NULLS LAST", expected.select([name]))

    def test_float_arithmetic_and_exact_comparisons_match_five_surfaces(self):
        from shardloom import functions as f
        pa, native, source = self.pa, self.native, self.fixture()
        path = self.write_input(source, "parquet")
        col, lit, call = native.Expression.column, native.Expression.literal, native.Expression.call
        selected, typed, arrays, fields = ["id"], [("id", col("id"))], [source["id"]], [source.schema.field("id")]
        for name, bits, width in (("f64", BITS64, 64), ("f32", BITS32, 32)):
            for label, public, expression, calculate in (
                ("add", f.col(name) + 1.0, col(name) + lit(1.0), lambda x: x + 1.0),
                ("sub", f.col(name) - 1.0, col(name) - lit(1.0), lambda x: x - 1.0),
                ("mul", f.col(name) * 0.0, col(name) * lit(0.0), lambda x: x * 0.0),
                ("div", f.col(name) / 0.0, col(name) / lit(0.0),
                 lambda x: math.nan if x == 0.0 or math.isnan(x) else math.copysign(math.inf, x)),
                ("rem", f.col(name) % 0.0, col(name) % lit(0.0), lambda x: math.nan),
            ):
                alias = name + "_" + label
                selected.append(public.alias(alias))
                typed.append((alias, expression))
                arrays.append(self.column([None if bit is None else result_bits(calculate(float_value(bit, width))) for bit in bits]))
                fields.append(pa.field(alias, pa.float64()))
            # Exact typed comparisons use analytical identity, not IEEE NaN != NaN.
            for label, public, expression, predicate in (
                ("self_equal", f.col(name) == f.col(name), col(name) == col(name), lambda key: True),
                ("positive", f.col(name) > 0.0, col(name) > lit(0.0), lambda key: key > (1, 0)),
                ("nonpositive", f.col(name) <= 0.0, col(name) <= lit(0.0), lambda key: key <= (1, 0)),
            ):
                alias = name + "_" + label
                selected.append(public.alias(alias))
                typed.append((alias, expression))
                arrays.append(pa.array([None if bit is None else predicate(semantic_key(bit, width)) for bit in bits], pa.bool_()))
                fields.append(pa.field(alias, pa.bool_()))
        expected = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        self.five(self.frame(path).select(*selected), native.Relation.scan(str(path)).project(typed),
            "SELECT " + ",".join(map(str, selected)) + f" FROM '{path}'", expected)

    def test_float_reducers_and_moving_windows_recover_after_specials(self):
        import shardloom as sl
        from shardloom import functions as f
        pa, native = self.pa, self.native
        reducers = [("sum", None), ("avg", None), ("sum_distinct", None), ("avg_distinct", None),
                    ("count", None), ("count_distinct", None), ("min", None), ("max", None),
                    ("median", None), ("quantile_cont", 0.25), ("quantile_cont", 1.0),
                    ("quantile_disc", 0.5), ("var_pop", None), ("var_samp", None),
                    ("stddev_pop", None), ("stddev_samp", None)]
        groups = [[None, None], [1.0, 2.0, 3.0], [math.inf, 4.0, math.inf],
                  [-math.inf, -2.0, -math.inf], [math.inf, -math.inf, 3.0],
                  [float_value(0xfff0000000000042), float_value(0x7ff8000000000081), 6.0],
                  [-0.0, 0.0, -0.0], [math.nan]]
        bits = [None if value is None else int.from_bytes(struct.pack("<d", value), "little")
                for group in groups for value in group]
        keys = [index for index, group in enumerate(groups) for _ in group]
        keep = [index % 4 != 0 for index in range(len(bits))]
        source = pa.Table.from_arrays([pa.array(keys, pa.int64()), self.column(bits), pa.array(keep, pa.bool_())],
            schema=pa.schema([pa.field("g", pa.int64(), nullable=False), pa.field("x", pa.float64()),
                              pa.field("keep", pa.bool_(), nullable=False)]))
        path = self.write_input(source, "parquet")
        for filtered in (False, True):
            selected, typed, fields = [], [], [source.schema.field("g")]
            columns = [pa.array(range(len(groups)), pa.int64())]
            for index, (function, level) in enumerate(reducers):
                name = "r" + str(index)
                expression = getattr(f, function)("x", *([] if level is None else [level]))
                if filtered:
                    expression = expression.filter(f.col("keep"))
                selected.append(expression.alias(name))
                typed.append(native.AggregateMeasure(function, "x", name, quantile_level=level,
                    filter_column="keep" if filtered else None))
                wanted = [reduce_bits([bit for row, bit in enumerate(bits) if keys[row] == group
                                      and (not filtered or keep[row])], function, level=level)
                          for group in range(len(groups))]
                count = function.startswith("count")
                columns.append(pa.array(wanted, pa.uint64()) if count else self.column(wanted))
                fields.append(pa.field(name, pa.uint64() if count else pa.float64(), nullable=not count))
            expected = pa.Table.from_arrays(columns, schema=pa.schema(fields))
            self.five(self.frame(path).group_by("g").agg(*selected).sort("g"),
                native.Relation.scan(str(path)).aggregate(["g"], typed).sort([("g", False, None)]),
                "SELECT g," + ",".join(map(str, selected)) + f" FROM '{path}' GROUP BY g ORDER BY g", expected)

        sequence = [1.0, math.inf, 3.0, -math.inf, 5.0, 6.0, math.nan, 8.0, 9.0, 10.0, None, 12.0]
        bits = [None if value is None else result_bits(value) for value in sequence]
        source = pa.Table.from_arrays([pa.array(range(len(bits)), pa.int64()), self.column(bits)],
            schema=pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.float64())]))
        path = self.write_input(source, "parquet", "window")
        specification = sl.Window.order_by("id").rows_between(-2, 0)
        frame = native.WindowFrame("rows", ("preceding", native.FrameOffset.number(native.Expression.literal(2))), ("current_row", None))
        selected, typed, fields, columns = ["id"], [], [source.schema.field("id")], [source["id"]]
        for index, (function, level) in enumerate(reducers):
            name = "r" + str(index)
            selected.append(getattr(f, function)("x", *([] if level is None else [level])).over(specification).alias(name))
            typed.append(native.WindowExpression(function, name, source_column="x", quantile_level=level,
                order_by=[("id", False, None)], frame=frame))
            wanted = [reduce_bits(bits[max(0, row-2):row+1], function, level=level) for row in range(len(bits))]
            count = function.startswith("count")
            columns.append(pa.array(wanted, pa.uint64()) if count else self.column(wanted))
            fields.append(pa.field(name, pa.uint64() if count else pa.float64(), nullable=not count))
        expected = pa.Table.from_arrays(columns, schema=pa.schema(fields))
        self.five(self.frame(path).select(*selected).sort("id"),
            native.Relation.scan(str(path)).window(["id"], typed).sort([("id", False, None)]),
            "SELECT " + ",".join(map(str, selected)) + f" FROM '{path}' ORDER BY id", expected)

    def test_float_unary_and_exact_mixed_comparisons(self):
        from shardloom import functions as f
        pa, native = self.pa, self.native
        path = self.write_input(self.fixture(), "parquet")
        col, call = native.Expression.column, native.Expression.call
        # The established arithmetic result domain is Float64 even for F32
        # operands. Generated NaNs have canonical bits independently of input.
        for name, bits, width in (("f64", BITS64, 64), ("f32", BITS32, 32)):
            declarations = [(-f.col(name)).alias("neg"), f.abs(name).alias("abs")]
            expressions = [("neg", -col(name)), ("abs", call("abs", [col(name)]))]
            negative = [None if bit is None else result_bits(-float_value(bit, width)) for bit in bits]
            absolute = [None if bit is None else result_bits(abs(float_value(bit, width))) for bit in bits]
            expected = pa.Table.from_arrays([self.column(negative), self.column(absolute)],
                schema=pa.schema([pa.field("neg", pa.float64()), pa.field("abs", pa.float64())]))
            self.five(self.frame(path).select(*declarations), native.Relation.scan(str(path)).project(expressions),
                "SELECT " + ",".join(map(str, declarations)) + f" FROM '{path}'", expected)

        # Converting the integer to Float64 before comparison would lose this
        # boundary. Nonfinite values must not become integer endpoints either.
        wide = [2**53+1, 2**63, 2**64-1, 0, 2**64-1, 0]
        floats = [float(2**53), float(2**63), float(2**64), -0.0, math.inf, math.nan]
        table = pa.table({"u": pa.array(wide, pa.uint64()), "x": pa.array(floats, pa.float64())})
        path = self.write_input(table, "parquet", "wide-integers")
        selected = [(f.col("u") == f.col("x")).alias("eq"), (f.col("u") < f.col("x")).alias("lt")]
        typed = [("eq", col("u") == col("x")), ("lt", col("u") < col("x"))]
        expected = pa.table({"eq": pa.array([False, True, False, True, False, False], pa.bool_()),
                             "lt": pa.array([False, False, True, False, True, True], pa.bool_())})
        self.five(self.frame(path).select(*selected), native.Relation.scan(str(path)).project(typed),
            "SELECT " + ",".join(map(str, selected)) + f" FROM '{path}'", expected)

    def test_f32_selected_reducers_preserve_width_and_payload_bits(self):
        from shardloom import functions as f
        pa, native = self.pa, self.native
        path = self.write_input(pa.table({"x": self.column(BITS32, 32)}), "parquet", "f32-reducers")
        declarations = [("sum", None), ("avg", None), ("min", None), ("max", None),
                        ("quantile_disc", 1.0), ("quantile_cont", 1.0)]
        selected, typed, arrays, fields = [], [], [], []
        for index, (function, level) in enumerate(declarations):
            name = f"r{index}"
            selected.append(getattr(f, function)("x", *([] if level is None else [level])).alias(name))
            typed.append(native.AggregateMeasure(function, "x", name, quantile_level=level))
            width = 32 if function in ("min", "max", "quantile_disc") else 64
            arrays.append(self.column([reduce_bits(BITS32, function, 32, level)], width))
            fields.append(pa.field(name, pa.float32() if width == 32 else pa.float64()))
        self.five(self.frame(path).agg(*selected), native.Relation.scan(str(path)).aggregate([], typed),
            "SELECT " + ",".join(map(str, selected)) + f" FROM '{path}'",
            pa.Table.from_arrays(arrays, schema=pa.schema(fields)))

    def test_strict_intake_and_text_refusals_preserve_binary_reuse(self):
        import shardloom as sl
        from shardloom import functions as f
        from shardloom.errors import ShardLoomCommandError
        source = self.fixture()
        with self.assertRaisesRegex(RuntimeError, "non-finite"):
            self.sessions["strict"].from_arrow(source)
        path = self.write_input(source)
        query = self.frame(path)
        retained = query.collect(check=True)
        # Extended preparation cannot make this same Arrow file acceptable to
        # a strict session through a cache hit.
        with self.assertRaisesRegex(ShardLoomCommandError, "non-finite"):
            self.frame(path, "strict").collect(check=True)
        statement = f"SELECT * FROM '{path}'"
        for plan in (self.sessions["strict"].sql(statement),
                     self.sessions["strict"].plan(self.native.Relation.scan(str(path)))):
            with self.assertRaisesRegex(RuntimeError, "non-finite"):
                plan.collect()
        for output_format in ("csv", "json", "jsonl"):
            destination = self.root / f"invalid.{output_format}"
            with self.assertRaisesRegex(ShardLoomCommandError, "non-finite|nonfinite|finite"):
                query.write(destination, output_format=output_format, check=True)
            self.assertFalse(destination.exists())
            self.equal(query.collect(check=True).to_arrow(), source)
        outputs = []
        for output_format, suffix in (("vortex", "vortex"), ("parquet", "parquet"), ("arrow-ipc", "arrow")):
            destination = self.root / f"roundtrip.{suffix}"
            report = query.write(destination, output_format=output_format, check=True)
            self.assertFalse(report.envelope.fallback.attempted)
            outputs.append(destination)
        path.unlink()
        self.clients["extended"].close()
        self.equal(retained.to_arrow(), source)
        receiver = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2, float_policy="extended")
        self.addCleanup(receiver.close)
        for destination in outputs:
            reader = {".vortex": sl.read_vortex, ".parquet": sl.read_parquet, ".arrow": sl.read_arrow_ipc}[destination.suffix]
            self.equal(reader(destination, client=receiver).collect(check=True).to_arrow(), source)
        # Strict classification is permitted on native storage, while arithmetic
        # and key observation preserve the established strict refusal.
        vortex = outputs[0]
        for expression in (f.col("f64") + 1.0, f.col("f64") > 0.0):
            with self.assertRaisesRegex(ShardLoomCommandError, "non-finite|nonfinite|finite"):
                self.frame(vortex, "strict").select(expression.alias("x")).collect(check=True)
        with self.assertRaisesRegex(ShardLoomCommandError, "non-finite|nonfinite|finite"):
            self.frame(vortex, "strict").sort("f64").collect(check=True)

    def test_dictionary_and_nested_keys_keep_first_bits_and_parent_nulls(self):
        from shardloom import functions as f
        pa, native = self.pa, self.native
        bits = [0xfff0000000000042, 0x7ff8000000000081, 0x8000000000000000, 0, None, 0x7ff0000000000000]
        primitive = self.column(bits)
        structures = pa.StructArray.from_arrays([primitive], names=["value"],
            mask=pa.array([False, False, False, False, False, True]))
        dictionaries = pa.DictionaryArray.from_arrays(pa.array([0, 1, 2, 3, 4, None], pa.int8()), primitive)
        for name, values, expected_values, representatives, counts in (
            ("nested", structures, structures, [0, 2, 4, 5], [2, 2, 1, 1]),
            ("dictionary", dictionaries, primitive, [0, 2, 4], [2, 2, 2]),
        ):
            source = pa.table({"id": pa.array(range(6), pa.int64()), "payload": values})
            path = self.write_input(source, name=name)
            expected = pa.Table.from_arrays([expected_values.take(pa.array(representatives)),
                pa.array(counts, pa.uint64()), pa.array(representatives, pa.int64())],
                schema=pa.schema([pa.field("payload", expected_values.type), pa.field("n", pa.uint64(), nullable=False),
                                  pa.field("first_id", pa.int64())]))
            self.five(self.frame(path).group_by("payload").agg(f.count().alias("n"), f.min("id").alias("first_id")).sort("first_id"),
                native.Relation.scan(str(path)).aggregate(["payload"], [native.AggregateMeasure("count", None, "n"),
                    native.AggregateMeasure("min", "id", "first_id")]).sort([("first_id", False, None)]),
                f"SELECT payload,COUNT(*) AS n,MIN(id) AS first_id FROM '{path}' GROUP BY payload ORDER BY first_id", expected)

    def test_strict_nested_parent_null_does_not_observe_masked_nonfinite_bits(self):
        from shardloom import functions as f
        pa, native = self.pa, self.native
        for width, special in ((32, 0xff800042), (64, 0xfff0000000000042)):
            child = self.column([special, 0], width)
            structures = pa.StructArray.from_arrays([child], names=["value"], mask=pa.array([True, False]))
            source = pa.table({"id": pa.array([0, 1], pa.int64()), "payload": structures})
            path = self.write_input(source, name=f"masked-{width}")
            expected = pa.Table.from_arrays([structures, pa.array([1, 1], pa.uint64()), pa.array([0, 1], pa.int64())],
                schema=pa.schema([source.schema.field("payload"), pa.field("n", pa.uint64(), nullable=False),
                                  pa.field("first_id", pa.int64())]))
            self.five(self.frame(path, "strict").group_by("payload").agg(f.count().alias("n"),
                f.min("id").alias("first_id")).sort("first_id"), native.Relation.scan(str(path)).aggregate(["payload"],
                [native.AggregateMeasure("count", None, "n"), native.AggregateMeasure("min", "id", "first_id")]).sort([("first_id", False, None)]),
                f"SELECT payload,COUNT(*) AS n,MIN(id) AS first_id FROM '{path}' GROUP BY payload ORDER BY first_id", expected)

    def test_nested_duplicate_selection_uses_extended_identity_at_every_depth(self):
        pa, native = self.pa, self.native
        for width, bits in ((32, [0xff800042, 0x7fc00081, 0x80000000, 0, None, 0x7f800000]),
                            (64, [0xfff0000000000042, 0x7ff8000000000081,
                                  0x8000000000000000, 0, None, 0x7ff0000000000000])):
            lists = pa.ListArray.from_arrays(pa.array(range(7), pa.int32()), self.column(bits, width))
            nested = pa.StructArray.from_arrays([lists], names=["values"],
                mask=pa.array([False, False, False, False, False, True]))
            source = pa.table({"id": pa.array(range(6), pa.int64()), "payload": nested})
            path = self.write_input(source, name=f"nested-dedup-{width}")
            for keep, selected in (("first", [0, 2, 4, 5]), ("last", [1, 3, 4, 5]), (False, [4, 5])):
                spelling = "false" if keep is False else keep
                for operation in ("drop_duplicates", "duplicated"):
                    frame = getattr(self.frame(path), operation)("payload", keep=keep)
                    relation = native.Relation.scan(str(path)).unary(operation, "payload", keep=spelling)
                    expected = (source.take(pa.array(selected)) if operation == "drop_duplicates" else
                        pa.Table.from_arrays([pa.array([row not in selected for row in range(6)])],
                            schema=pa.schema([pa.field("duplicated", pa.bool_(), nullable=False)])))
                    self.five(frame, relation, frame._relation_statement(), expected)

    def test_pivot_float_keys_and_unique_values_preserve_first_representatives(self):
        pa, native = self.pa, self.native
        for width, bits in ((32, [0xff800042, 0x7fc00081, 0x80000000, 0, 0xff800000, 0x7f800000]),
                            (64, [0xfff0000000000042, 0x7ff8000000000081, 0x8000000000000000, 0,
                                  0xfff0000000000000, 0x7ff0000000000000])):
            source = pa.table({"g": self.column(bits, width), "category": pa.array(["a"] * 6),
                               "x": self.column(bits, width)})
            path = self.write_input(source, name=f"pivot-float-keys-{width}")
            for aggregate in ("count", "first_unique"):
                frame = (self.frame(path).pivot(index="g", columns="category", values="x") if aggregate == "first_unique"
                         else self.frame(path).pivot_table(index="g", columns="category", values="x", aggfunc="count"))
                frame = frame.sort("g")
                options = json.dumps({"index": "g", "columns": "category", "values": "x", "aggregate": aggregate})
                relation = native.Relation.scan(str(path)).unary("pivot", options).sort([("g", False, None)])
                representatives = [4, 2, 5, 0]
                keys = self.column([bits[row] for row in representatives], width)
                values = (pa.array([1, 2, 1, 2], pa.uint64()) if aggregate == "count" else keys)
                expected = pa.Table.from_arrays([keys, values],
                    schema=pa.schema([pa.field("g", keys.type), pa.field("pivot_a", values.type)]))
                self.five(frame, relation, frame._relation_statement(), expected)
            source = pa.table({"g": pa.array(["one"] * 4), "category": self.column(bits[:4], width),
                               "x": pa.array([1., 2., 3., 4.])})
            path = self.write_input(source, name=f"pivot-float-categories-{width}")
            frame = self.frame(path).pivot_table(index="g", columns="category", values="x", aggfunc="sum")
            relation = native.Relation.scan(str(path)).unary("pivot", json.dumps(
                {"index": "g", "columns": "category", "values": "x", "aggregate": "sum"}))
            expected = pa.table({"g": ["one"], "pivot_0": [7.], "pivot_nan": [3.]})
            self.five(frame, relation, frame._relation_statement(), expected)

    def test_streaming_float_bits_eof_close_and_late_error(self):
        import shardloom as sl
        from shardloom import types as t
        source = self.fixture()
        # Explicit source schema preserves F32 and nullability through the
        # declared application producer boundary.
        schema = {"id": t.int64(nullable=False), "f64": t.float64(), "f32": t.float32()}
        for mode in ("collect", "batches", "write", "empty", "late_error", "zero_error"):
            with self.subTest(mode=mode):
                client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20,
                                           max_parallelism=2, float_policy="extended")
                self.addCleanup(client.close)
                trace = []
                def producer():
                    trace.append("opened")
                    try:
                        yield source.slice(0, 0)
                        if mode != "empty":
                            for row in range(source.num_rows):
                                yield source.slice(row, 1)
                        if mode.endswith("error"):
                            raise LookupError("float producer refused")
                        trace.append("ended")
                    finally:
                        trace.append("closed")
                query = sl.from_batches(producer, schema=schema, streaming=True, client=client).select("id", "f64", "f32")
                output = self.root / f"stream-{mode}.vortex"
                if mode.endswith("error"):
                    if mode == "zero_error":
                        query = query.limit(0)
                    with self.assertRaisesRegex(LookupError, "float producer refused"):
                        query.write_vortex(output)
                    self.assertFalse(output.exists())
                    self.assertEqual(trace, ["opened", "closed"])
                    client.close()
                    continue
                if mode == "batches":
                    with query.iter_batches(batch_rows=1) as stream:
                        chunks = [batch.to_arrow() for batch in stream]
                        report = stream.report
                    actual = self.pa.concat_tables(chunks)
                elif mode == "write":
                    report = query.write_vortex(output)
                    actual = sl.read_vortex(output, client=client).collect(check=True).to_arrow()
                else:
                    report = query.collect(check=True)
                    client.close()
                    actual = report.to_arrow()
                self.equal(actual, source.slice(0, 0) if mode == "empty" else source)
                self.assertEqual(trace, ["opened", "ended", "closed"])
                self.assertTrue(report.envelope.field_bool("native_input_end_observed"))
                self.assertEqual(report.envelope.field("float_policy"), "extended")
                self.assertFalse(report.envelope.fallback.attempted)
                client.close()

    def test_pivot_margins_share_extended_reductions(self):
        pa, native = self.pa, self.native
        bits = [0xfff0000000000042, result_bits(5.0), result_bits(math.inf), result_bits(-math.inf),
                result_bits(-0.0), result_bits(0.0), result_bits(1.0), result_bits(3.0),
                result_bits(-math.inf), result_bits(4.0), 0x7ff8000000000081, result_bits(7.0)]
        groups = [group for group in ("A", "B", "C") for _ in range(4)]
        categories = ["a", "a", "b", "b"] * 3
        source = pa.table({"g": pa.array(groups), "category": pa.array(categories), "x": self.column(bits)})
        path = self.write_input(source, name="pivot-specials")
        for aggregate in ("sum", "mean", "count", "min", "max"):
            for margins in (False, True):
                frame = self.frame(path).pivot_table(index="g", columns="category", values="x",
                    aggfunc=aggregate, margins=margins, margins_name="Total").sort("g")
                options = json.dumps({"index": "g", "columns": "category", "values": "x",
                    "aggregate": aggregate, "margins": margins, "margins_name": "Total"})
                relation = native.Relation.scan(str(path)).unary("pivot", options).sort([("g", False, None)])
                output_groups = ["A", "B", "C"] + (["Total"] if margins else [])
                output_categories = ["a", "b"] + (["Total"] if margins else [])
                columns, fields = [pa.array(output_groups)], [pa.field("g", pa.string())]
                for category in output_categories:
                    wanted = [reduce_bits([bit for bit, g, c in zip(bits, groups, categories)
                        if (group == "Total" or g == group) and (category == "Total" or c == category)],
                        "avg" if aggregate == "mean" else aggregate) for group in output_groups]
                    dtype = pa.uint64() if aggregate == "count" else pa.float64()
                    columns.append(pa.array(wanted, dtype) if aggregate == "count" else self.column(wanted))
                    fields.append(pa.field("pivot_" + category.lower(), dtype))
                expected = pa.Table.from_arrays(columns, schema=pa.schema(fields))
                self.five(frame, relation, frame._relation_statement(), expected)

    def test_join_and_set_keys_agree_without_changing_selected_bits(self):
        pa, native, source = self.pa, self.native, self.fixture()
        right_indices = [1, 4, 6, 3, 9, 11]
        left_path = self.write_input(source, "parquet", "join-left")
        right_path = self.write_input(source.take(pa.array(right_indices)), "parquet", "join-right")
        left, right = self.frame(left_path), self.frame(right_path)
        a, b = native.Relation.scan(str(left_path)), native.Relation.scan(str(right_path))
        col = native.Expression.column
        for name, bits, width in (("f64", BITS64, 64), ("f32", BITS32, 32)):
            pairs = []
            for row, bit in enumerate(bits):
                matches = [other for other in right_indices if bit is not None and bits[other] is not None
                           and semantic_key(bit, width) == semantic_key(bits[other], width)]
                pairs.extend((row, other) for other in (matches or [None]))
            expected = pa.Table.from_arrays([
                pa.array([row for row, _ in pairs], pa.int64()), self.column([bits[row] for row, _ in pairs], width),
                pa.array([other for _, other in pairs], pa.int64())], schema=pa.schema([
                    pa.field("left_id", pa.int64(), nullable=False), source.schema.field(name).with_name("value"),
                    pa.field("right_id", pa.int64())]))
            frame = left.join(right, on=name, how="left").select("f.id AS left_id", f"f.{name} AS value", "d.id AS right_id").sort("left_id", "right_id", nulls="last")
            relation = a.join(b, [(name, name)], [("left", "id", "left_id"), ("left", name, "value"),
                ("right", "id", "right_id")], kind="left").sort([("left_id", False, "last"), ("right_id", False, "last")])
            self.five(frame, relation,
                f"SELECT f.id AS left_id,f.{name} AS value,d.id AS right_id FROM '{left_path}' AS f LEFT JOIN '{right_path}' AS d ON f.{name}=d.{name} ORDER BY left_id NULLS LAST,right_id NULLS LAST", expected)
            right_keys = {semantic_key(bits[row], width) for row in right_indices}
            first = {}
            for row, bit in enumerate(bits):
                first.setdefault(semantic_key(bit, width), row)
            for method, kind, sql_kind in (("union", "union_distinct", "UNION"),
                                          ("intersect", "intersect", "INTERSECT"),
                                          ("except_", "except", "EXCEPT")):
                chosen = [row for key, row in sorted(first.items()) if kind == "union_distinct"
                    or (key in right_keys) == (kind == "intersect")]
                expected = pa.Table.from_arrays([self.column([bits[row] for row in chosen], width)],
                    schema=pa.schema([source.schema.field(name)]))
                frame = getattr(left.select(name), method)(right.select(name)).sort(name, nulls="last")
                relation = a.project([(name, col(name))]).set_operation(b.project([(name, col(name))]), kind=kind).sort([(name, False, "last")])
                self.five(frame, relation,
                    f"SELECT {name} FROM '{left_path}' {sql_kind} SELECT {name} FROM '{right_path}' ORDER BY {name} NULLS LAST", expected)

    def test_bounded_rolling_recovers_after_special_values_expire(self):
        pa, native = self.pa, self.native
        path = self.write_input(pa.table({"x": self.column(BITS64)}), name="rolling-specials")
        for aggregate in ("sum", "mean", "count", "min", "max"):
            for center in (False, True):
                frame = getattr(self.frame(path).rolling(3, min_periods=1, center=center), aggregate)("x", alias="value")
                options = json.dumps({"source_column": "x", "output_column": "value", "window_size": 3,
                    "min_periods": 1, "aggregate": aggregate, "center": center})
                relation = native.Relation.scan(str(path)).unary("rolling", options)
                wanted = [reduce_bits(BITS64[max(0, row-(1 if center else 2)):row+(2 if center else 1)],
                    "avg" if aggregate == "mean" else aggregate) for row in range(len(BITS64))]
                dtype = pa.uint64() if aggregate == "count" else pa.float64()
                expected = pa.Table.from_arrays([pa.array(wanted, dtype) if aggregate == "count" else self.column(wanted)],
                    schema=pa.schema([pa.field("value", dtype, nullable=False)]))
                self.five(frame, relation, frame._relation_statement(), expected)

    def test_paired_moments_ignore_null_pairs_and_recover_in_windows(self):
        import shardloom as sl
        from shardloom import functions as f
        pa, native = self.pa, self.native
        xs = [0.0, 2.0, math.inf, 4.0, math.nan, 6.0, None, math.inf, 8.0, 10.0]
        ys = [0.0, 4.0, 6.0, 8.0, 10.0, 12.0, math.inf, None, 16.0, 20.0]
        functions = ("covar_pop", "covar_samp", "corr")

        def reference(rows, function):
            pairs = [(xs[row], ys[row]) for row in rows if xs[row] is not None and ys[row] is not None]
            minimum = 2 if function in ("covar_samp", "corr") else 1
            if len(pairs) < minimum:
                return None
            if any(not math.isfinite(value) for pair in pairs for value in pair):
                return NAN64
            x, y = zip(*[(Fraction.from_float(a), Fraction.from_float(b)) for a, b in pairs])
            mx, my = sum(x) / len(x), sum(y) / len(y)
            cross = sum((a-mx)*(b-my) for a, b in zip(x, y))
            if function == "corr":
                variance = sum((a-mx)**2 for a in x) * sum((b-my)**2 for b in y)
                return None if not variance else result_bits(float(cross) / math.sqrt(float(variance)))
            return result_bits(float(cross / (len(pairs) - int(function == "covar_samp"))))

        for width in (32, 64):
            dtype = pa.float32() if width == 32 else pa.float64()
            source = pa.table({"id": pa.array(range(len(xs)), pa.int64()),
                               "g": pa.array([row // 2 for row in range(len(xs))], pa.int64()),
                               "x": pa.array(xs, dtype), "y": pa.array(ys, dtype)})
            path = self.write_input(source, "parquet", f"paired-{width}")
            for window in (False, True):
                column = "id" if window else "g"
                rows = list(range(len(xs) if window else len(xs) // 2))
                columns, fields = [pa.array(rows, pa.int64())], [source.schema.field(column)]
                expressions, measures = [], []
                for function in functions:
                    expression = getattr(f, function)("x", "y")
                    if window:
                        expression = expression.over(sl.Window.order_by("id").rows_between(-1, 0))
                        measure = native.WindowExpression(function, function, source_column="x", secondary_column="y",
                            order_by=[("id", False, None)], frame=native.WindowFrame("rows",
                                ("preceding", native.FrameOffset.number(native.Expression.literal(1))), ("current_row", None)))
                    else:
                        measure = native.AggregateMeasure(function, "x", function, secondary_column="y")
                    expressions.append(expression.alias(function))
                    measures.append(measure)
                    columns.append(self.column([reference(range(max(0, row-1), row+1) if window
                        else range(row*2, row*2+2), function) for row in rows]))
                    fields.append(pa.field(function, pa.float64()))
                frame, relation = self.frame(path), native.Relation.scan(str(path))
                frame = frame.select(column, *expressions) if window else frame.group_by(column).agg(*expressions)
                relation = relation.window([column], measures) if window else relation.aggregate([column], measures)
                statement = (f"SELECT {column}," + ",".join(map(str, expressions)) + f" FROM '{path}'"
                             + ("" if window else f" GROUP BY {column}") + f" ORDER BY {column}")
                self.five(frame.sort(column), relation.sort([(column, False, None)]), statement,
                    pa.Table.from_arrays(columns, schema=pa.schema(fields)), approximate=("corr",))

    def test_float_window_peers_share_analytical_keys(self):
        import shardloom as sl
        from shardloom import functions as f
        pa, native, source = self.pa, self.native, self.fixture()
        path = self.write_input(source, "parquet", "peers")
        for name, bits, width in (("f64", BITS64, 64), ("f32", BITS32, 32)):
            keys = [semantic_key(bit, width) for bit in bits]
            distinct = sorted(set(keys))
            ranks = [1 + sum(other < key for other in keys) for key in keys]
            dense = [1 + distinct.index(key) for key in keys]
            specification = sl.Window.order_by(name, nulls="last")
            expressions = [f.rank().over(specification).alias("r"), f.dense_rank().over(specification).alias("d")]
            typed = [native.WindowExpression(function, alias, order_by=[(name, False, "last")])
                     for function, alias in (("rank", "r"), ("dense_rank", "d"))]
            expected = pa.Table.from_arrays([source["id"], source[name], pa.array(ranks, pa.int64()), pa.array(dense, pa.int64())],
                schema=pa.schema([source.schema.field("id"), source.schema.field(name),
                                  pa.field("r", pa.int64(), nullable=False), pa.field("d", pa.int64(), nullable=False)]))
            self.five(self.frame(path).select("id", name, *expressions).sort("id"),
                native.Relation.scan(str(path)).window(["id", name], typed).sort([("id", False, None)]),
                "SELECT id," + name + "," + ",".join(map(str, expressions)) + f" FROM '{path}' ORDER BY id", expected)

    def test_nonfinite_casts_have_explicit_integer_and_text_boundaries(self):
        from shardloom import functions as f
        from shardloom.errors import ShardLoomCommandError
        pa, native = self.pa, self.native
        for name, bits, width in (("f64", BITS64, 64), ("f32", BITS32, 32)):
            chosen = [bits[row] for row in (0, 1, 2, 3, 4, 5, 6, 9, 10, 11)]
            path = self.write_input(pa.table({"x": self.column(chosen, width)}), "parquet", "casts-" + name)
            selected = [f.try_cast("x", "int64").alias("integer"), f.cast("x", "utf8").alias("text")]
            typed = [("integer", native.Expression.column("x").cast("int64", tolerant=True)),
                     ("text", native.Expression.column("x").cast("utf8"))]
            expected = pa.table({"integer": pa.array([0, 0, None, None, None, None, None, 1, -1, None], pa.int64()),
                "text": pa.array(["-0", "0", "-inf", "inf", "NaN", "NaN", "NaN", "1", "-1", None])})
            self.five(self.frame(path).select(*selected), native.Relation.scan(str(path)).project(typed),
                "SELECT " + ",".join(map(str, selected)) + f" FROM '{path}'", expected)
            with self.assertRaisesRegex(ShardLoomCommandError, "cannot be converted to the bound scalar dtype"):
                self.frame(path).select(f.cast("x", "int64").alias("integer")).collect(check=True)
            self.equal(self.frame(path).collect(check=True).to_arrow(), pa.table({"x": self.column(chosen, width)}))

    def test_private_spill_preserves_signaling_f32_and_f64_payloads(self):
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError
        pa = self.pa
        count = 8005
        keys = [f"{group:08}" + "f" * 2296 for group in range(count)]
        bits32 = [BITS32[group % len(BITS32)] for group in range(count)]
        bits64 = [BITS64[group % len(BITS64)] for group in range(count)]
        source = pa.table({"id": pa.array(range(count * 2), pa.int64()),
                           "g": pa.array(keys * 2),
                           "f32": self.column(bits32 * 2, 32),
                           "f64": self.column(bits64 * 2)})
        expected = source.slice(count)
        path = self.root / "private-float-spill.arrow"
        with pa.OSFile(str(path), "wb") as stream, pa.ipc.new_file(stream, source.schema) as writer:
            writer.write_table(source, max_chunksize=251)
        del source
        workspace = self.root / "private-float-runs"
        workspace.mkdir()
        (workspace / "foreign").write_bytes(b"preserve")
        grant = 32 << 20
        spill = {"workspace": str(workspace), "quota_bytes": 256 << 20, "buffer_bytes": 2 << 20}

        def consume(memory, options=None):
            client = sl.ShardLoomClient(transport="native", memory_bytes=memory,
                max_parallelism=1, float_policy="extended")
            try:
                frame = sl.read_arrow_ipc(path, client=client).drop_duplicates("g", keep="last")
                delivered = 0
                with frame.iter_batches(batch_rows=127, spill=options) as batches:
                    for batch in batches:
                        actual = batch.to_arrow()
                        self.equal(actual, expected.slice(delivered, actual.num_rows))
                        delivered += actual.num_rows
                    report = batches.report
                self.assertEqual(delivered, count)
                self.check_envelope(report.envelope, "extended")
                return report
            finally:
                client.close()

        consume(1 << 30)
        with self.assertRaisesRegex(ShardLoomCommandError, "reservation denied"):
            consume(grant)
        self.assertEqual(sorted(item.name for item in workspace.iterdir()), ["foreign"])
        report = consume(grant, spill)
        self.assertGreater(report.envelope.field_int("relational_spill_runs_written"), 3)
        self.assertGreater(report.envelope.field_int("relational_spill_merge_passes"), 1)
        self.assertTrue(report.envelope.field_bool("relational_spill_owned_cleanup_completed"))
        self.assertLessEqual(report.envelope.field_int("relational_spill_peak_disk_bytes"), spill["quota_bytes"])
        self.assertLessEqual(report.envelope.field_int("execution_resource_observed_native_peak_reserved_bytes"), grant)
        self.assertEqual(sorted(item.name for item in workspace.iterdir()), ["foreign"])
        self.assertEqual((workspace / "foreign").read_bytes(), b"preserve")

    def test_actual_float_spill_pressure_cleans_up_and_retries_after_faults(self):
        import shardloom as sl
        from shardloom import functions as f
        from shardloom.errors import ShardLoomCommandError
        pa = self.pa
        count = 8005
        patterns = [[result_bits(1.0), result_bits(2.0), result_bits(3.0)],
                    [result_bits(math.inf), result_bits(4.0), result_bits(math.inf)],
                    [result_bits(-math.inf), result_bits(math.inf), result_bits(5.0)],
                    [0xfff0000000000042, 0x7ff8000000000081, result_bits(6.0)],
                    [result_bits(-0.0), result_bits(0.0), result_bits(0.0)]]
        key = lambda group: f"{group:08}" + "f" * 2296
        bits = [bit for group in range(count) for bit in patterns[group % len(patterns)]]
        table = pa.table({"id": pa.array(range(len(bits)), pa.int64()),
                         "g": pa.array([key(group) for group in range(count) for _ in range(3)]),
                         "category": pa.array(["a"] * len(bits)), "x": self.column(bits)})
        path = self.root / "pressure.arrow"
        with pa.OSFile(str(path), "wb") as stream, pa.ipc.new_file(stream, table.schema) as writer:
            writer.write_table(table, max_chunksize=251)
        del table
        workspace = self.root / "float-runs"
        workspace.mkdir()
        (workspace / "foreign").write_bytes(b"preserve")
        # Public aggregate spills require a 2 MiB operator buffer. The shared
        # 32 MiB grant also leaves room for compatibility-input normalization;
        # retained keys/results still exceed that grant without actual runs.
        grant = 32 << 20
        spill = {"workspace": str(workspace), "quota_bytes": 256 << 20, "buffer_bytes": 2 << 20}

        def owner(memory):
            value = sl.ShardLoomClient(transport="native", memory_bytes=memory, max_parallelism=1,
                                      float_policy="extended")
            self.addCleanup(value.close)
            return value

        def query(client, kind):
            source = sl.read_arrow_ipc(path, client=client)
            if kind == "group":
                return source.group_by("g").agg(f.sum("x").alias("total"), f.avg("x").alias("mean"),
                    f.min("x").alias("low"), f.max("x").alias("high"))
            if kind == "pivot":
                return source.pivot_table(index="g", columns="category", values="x", aggfunc="sum",
                                          margins=True, margins_name="Total")
            window = sl.Window.order_by("id").rows_between(-2, 0)
            return source.select("id", "g", f.sum("x").over(window).alias("total"))

        def wanted(kind, start, length):
            rows = range(start, start+length)
            groups = [row // 3 if kind == "window" else row for row in rows]
            arrays, fields = [], []
            if kind == "window":
                arrays.append(pa.array(rows, pa.int64()))
                fields.append(pa.field("id", pa.int64()))
            arrays.append(pa.array(["Total" if kind == "pivot" and group == count else key(group) for group in groups]))
            fields.append(pa.field("g", pa.string()))
            names = ([("total", "sum"), ("mean", "avg"), ("low", "min"), ("high", "max")]
                     if kind == "group" else [("pivot_a", "sum"), ("pivot_total", "sum")]
                     if kind == "pivot" else [("total", "sum")])
            for name, function in names:
                observed = [reduce_bits(bits[max(0, row-2):row+1] if kind == "window"
                    else bits if group == count else patterns[group % len(patterns)], function)
                    for row, group in zip(rows, groups)]
                arrays.append(self.column(observed))
                fields.append(pa.field(name, pa.float64()))
            return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

        def clean():
            self.assertEqual(sorted(item.name for item in workspace.iterdir()), ["foreign"])
            self.assertEqual((workspace / "foreign").read_bytes(), b"preserve")

        def consume(frame, kind, options=None):
            delivered = 0
            with frame.iter_batches(batch_rows=127, spill=options) as batches:
                for batch in batches:
                    actual = batch.to_arrow()
                    self.equal(actual, wanted(kind, delivered, actual.num_rows))
                    delivered += actual.num_rows
                report = batches.report
            self.assertEqual(delivered, len(bits) if kind == "window" else count + int(kind == "pivot"))
            self.check_envelope(report.envelope, "extended")
            clean()
            return report

        for kind in ("group", "window", "pivot"):
            # Pivot conservatively charges several key copies per cell. Its
            # ample reservation includes those charges, rather than only the
            # logical input size; the pressure execution keeps the small grant.
            # The existing pivot output owner reserves 1024 long-key rows at
            # once, so its minimum working set is larger than the other two.
            kind_grant = 64 << 20 if kind == "pivot" else grant
            ample, bounded = owner(1 << 30), owner(kind_grant)
            with self.subTest(kind=kind):
                consume(query(ample, kind), kind)
                ample.close()
                frame = query(bounded, kind)
                with self.assertRaisesRegex(ShardLoomCommandError, "reservation denied"):
                    consume(frame, kind)
                clean()
                report = consume(frame, kind, spill)
                self.assertGreater(report.envelope.field_int("relational_spill_runs_written"), 3)
                self.assertGreater(report.envelope.field_int("relational_spill_merge_passes"), 1)
                self.assertTrue(report.envelope.field_bool("relational_spill_owned_cleanup_completed"))
                self.assertLessEqual(report.envelope.field_int("relational_spill_peak_disk_bytes"), spill["quota_bytes"])
                self.assertLessEqual(report.envelope.field_int("execution_resource_observed_native_peak_reserved_bytes"), kind_grant)
                if kind == "group":
                    with self.assertRaisesRegex(ShardLoomCommandError, "quota"):
                        consume(frame, kind, {**spill, "quota_bytes": 32 << 10})
                    clean()
                    for cancel in (False, True):
                        batches = frame.iter_batches(batch_rows=127, spill=spill)
                        retained = next(batches)
                        retained_table = retained.to_arrow()
                        if cancel:
                            bounded.cancel()
                            with self.assertRaisesRegex(ShardLoomCommandError, "cancel"):
                                next(batches)
                        batches.close()
                        self.equal(retained_table, wanted(kind, 0, retained_table.num_rows))
                        clean()
                    consume(frame, kind, spill)
                bounded.close()


if __name__ == "__main__":
    unittest.main()
