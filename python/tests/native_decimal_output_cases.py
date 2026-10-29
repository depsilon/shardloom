# SPDX-License-Identifier: Apache-2.0
"""Exact independent decimal references across the five native public interfaces."""

from datetime import datetime, timedelta
from decimal import Decimal, localcontext, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_DOWN, ROUND_FLOOR, ROUND_CEILING
from fractions import Fraction
import os
from pathlib import Path
import tempfile
from unittest.mock import patch
from zoneinfo import ZoneInfo


MODES = {"half_even": ROUND_HALF_EVEN, "half_away": ROUND_HALF_UP,
         "toward_zero": ROUND_DOWN, "floor": ROUND_FLOOR, "ceil": ROUND_CEILING}


def decimal(coefficient, scale):
    """Construct exact input/output values independently of the ambient context."""
    return Decimal((int(coefficient < 0), tuple(map(int, str(abs(coefficient)))), -scale))


def rounded(value, precision, scale, mode):
    if value is None:
        return None
    value = Fraction(value) * 10**scale
    if mode == "exact":
        assert value.denominator == 1
        coefficient = value.numerator
    else:
        with localcontext() as context:
            context.prec = 1800
            coefficient = int((Decimal(value.numerator) / Decimal(value.denominator))
                              .to_integral_value(rounding=MODES[mode]))
    assert abs(coefficient) < 10**precision
    return decimal(coefficient, scale)


def reduction(values, function, level=None):
    observed = [Fraction(value) for value in values if value is not None]
    if function.endswith("_distinct"):
        observed = sorted(set(observed))
        function = function.removesuffix("_distinct")
    if not observed:
        return None
    if function == "sum":
        return sum(observed, Fraction(0))
    if function == "avg":
        return sum(observed, Fraction(0)) / len(observed)
    observed.sort()
    level = 0.5 if function == "median" else level
    rank = Fraction.from_float(level) * (len(observed) - 1)
    lower = rank.numerator // rank.denominator
    upper = -(-rank.numerator // rank.denominator)
    return observed[lower] + (observed[upper] - observed[lower]) * (rank-lower)


def client(case):
    import shardloom as sl
    value = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    case.addCleanup(value.close)
    return value


def write(case, path, table):
    if path.suffix == ".parquet":
        import pyarrow.parquet as parquet
        parquet.write_table(table, path, row_group_size=3, use_dictionary=True)
    else:
        with case.pa.OSFile(str(path), "wb") as stream, case.pa.ipc.new_file(stream, table.schema) as output:
            for batch in table.to_batches(max_chunksize=3):
                output.write_batch(batch)


def read(path, owner):
    import shardloom as sl
    return (sl.read_parquet if path.suffix == ".parquet" else sl.read_arrow_ipc)(path, client=owner)


def equal(case, tables, expected):
    for table in tables:
        case.assertEqual(table.schema, expected.schema)
        case.assertTrue(table.equals(expected), f"{table}\n{expected}")


def check_scalars(case):
    import shardloom as sl
    from shardloom import functions as f
    pa, native, owner = case.pa, case.native, client(case)
    left = [125, -125, 135, -135, 0, 1, 99995, None, 10**27+1, -10**27-1]
    right = [500, -125, 125, 250, -500, 7000, 3000, 2000, 1000, -1000]
    numbers = [2, -2, 7, 3, 1, 8, 2, 2, 4, -4]
    source = pa.Table.from_arrays([
        pa.array(range(len(left)), pa.int64()),
        pa.array([None if x is None else decimal(x,2) for x in left], pa.decimal128(38,2)),
        pa.array([decimal(x,3) for x in right], pa.decimal128(10,3)),
        pa.array(numbers, pa.int64()),
    ], schema=pa.schema([pa.field("id",pa.int64(),nullable=False),
                         pa.field("a",pa.decimal128(38,2)),
                         pa.field("b",pa.decimal128(10,3),nullable=False),
                         pa.field("n",pa.int64(),nullable=False)]))
    col, lit, call = native.Expression.column, native.Expression.literal, native.Expression.call
    declarations, typed, wanted, fields = [], [("id", col("id"))], [source["id"]], [source.schema.field("id")]
    for mode in MODES:
        for operation in ("rescale", "add", "subtract", "multiply", "divide"):
            name = f"{operation}_{mode}"
            operands = [col("a")] + ([] if operation == "rescale" else [col("n" if operation == "divide" else "b")])
            typed.append((name, call("decimal_"+operation, operands+[lit(38),lit(1),lit(mode)])))
            args = [f.col("a")] + ([] if operation == "rescale" else [f.col("n" if operation == "divide" else "b")])
            declarations.append(getattr(f,"decimal_"+operation)(*args,38,1,mode).alias(name))
            output = []
            for a,b,n in zip(left,right,numbers):
                if a is None:
                    output.append(None)
                    continue
                a,b = Fraction(a,100),Fraction(b,1000)
                value = {"rescale":a,"add":a+b,"subtract":a-b,"multiply":a*b,"divide":a/n}[operation]
                output.append(rounded(value,38,1,mode))
            wanted.append(pa.array(output,pa.decimal128(38,1)))
            fields.append(pa.field(name,pa.decimal128(38,1)))
    declarations.append(f.decimal_rescale("a",38,4,"exact").alias("exact"))
    typed.append(("exact",call("decimal_rescale",[col("a"),lit(38),lit(4),lit("exact")])))
    wanted.append(pa.array([None if x is None else decimal(x*100,4) for x in left],pa.decimal128(38,4)))
    fields.append(pa.field("exact",pa.decimal128(38,4)))
    expected = pa.Table.from_arrays(wanted,schema=pa.schema(fields))
    with tempfile.TemporaryDirectory(prefix="shardloom-decimal-scalars-") as directory:
        for extension in ("arrow","parquet"):
            path=Path(directory)/f"input.{extension}"
            write(case,path,source)
            frame=read(path,owner).select("id",*declarations)
            relation=native.Relation.scan(str(path)).project(typed)
            sql="SELECT id,"+",".join(map(str,declarations))+f" FROM '{path}'"
            equal(case,case.all_surfaces(frame,relation,sql),expected)
            equal(case,case.all_surfaces(frame.limit(0),relation.limit(0),sql+" LIMIT 0"),expected.slice(0,0))


REDUCERS = [("sum","total",None),("avg","mean",None),("sum_distinct","distinct_total",None),
            ("avg_distinct","distinct_mean",None),("median","median",None),
            ("quantile_cont","q10",0.1),("quantile_cont","tiny",float.fromhex("0x0.0000000000001p-1022")),
            ("quantile_cont","last",1.0)]


def declarations(case, mode, filtered):
    from shardloom import functions as f
    expressions, measures = [], []
    for function,name,level in REDUCERS:
        args = ["amount"] + ([] if level is None else [level])
        expression=getattr(f,function)(*args,precision=38,scale=2,rounding=mode)
        if filtered:
            expression=expression.filter(f.col("keep"))
        expressions.append(expression.alias(name))
        measures.append(case.native.AggregateMeasure(function,"amount",name,
            quantile_level=level,filter_column="keep" if filtered else None,decimal_output=(38,2,mode)))
    return expressions,measures


def population(case):
    pa=case.pa
    coefficients=[125,-125,135,-135,125,None,1,0,10**30-1,-10**30+1,126]
    amounts=[None if i%3==2 or coefficients[i%len(coefficients)] is None else decimal(coefficients[i%len(coefficients)],2)
             for i in range(29)]
    groups=[("A","B","N")[i%3] for i in range(len(amounts))]
    keep=[None if i%7==0 else i%4!=0 for i in range(len(amounts))]
    dates=[datetime(2024,3,1,tzinfo=ZoneInfo("America/New_York"))+timedelta(days=i) for i in range(len(amounts))]
    fields=[pa.field("id",pa.int64(),nullable=False),pa.field("g",pa.string(),nullable=False),
            pa.field("amount",pa.decimal128(38,2)),pa.field("keep",pa.bool_()),
            pa.field("instant",pa.timestamp("us","America/New_York"),nullable=False)]
    return pa.Table.from_arrays([pa.array(range(len(amounts)),pa.int64()),pa.array(groups),
        pa.array(amounts,pa.decimal128(38,2)),pa.array(keep),pa.array(dates,fields[-1].type)],schema=pa.schema(fields))


def expected_reductions(case, groups, mode, group_field=None):
    fields=([group_field] if group_field is not None else [])+[case.pa.field(name,case.pa.decimal128(38,2)) for _,name,_ in REDUCERS]
    rows=[]
    for key,values in groups:
        row={} if group_field is None else {group_field.name:key}
        row.update({name:rounded(reduction(values,function,level),38,2,mode) for function,name,level in REDUCERS})
        rows.append(row)
    return case.pa.Table.from_pylist(rows,schema=case.pa.schema(fields))


def check_aggregates(case):
    import shardloom as sl
    native, owner, source=case.native,client(case),population(case)
    rows=source.to_pylist()
    with tempfile.TemporaryDirectory(prefix="shardloom-decimal-aggregates-") as directory:
        for extension in ("arrow","parquet"):
            path=Path(directory)/f"input.{extension}"
            write(case,path,source)
            for mode in MODES:
                for filtered in (False,True):
                    expressions,measures=declarations(case,mode,filtered)
                    frame=read(path,owner).group_by("g").agg(*expressions).sort("g")
                    relation=native.Relation.scan(str(path)).aggregate(["g"],measures).sort([("g",False,None)])
                    sql="SELECT g,"+",".join(map(str,expressions))+f" FROM '{path}' GROUP BY g ORDER BY g"
                    groups=[(group,[row["amount"] for row in rows if row["g"]==group and (not filtered or row["keep"] is True)])
                            for group in ("A","B","N")]
                    expected=expected_reductions(case,groups,mode,source.schema.field("g"))
                    equal(case,case.all_surfaces(frame,relation,sql),expected)
            # A scalar empty aggregate preserves its declared schema and one NULL result row.
            expressions,measures=declarations(case,"exact",False)
            frame=read(path,owner).limit(0).select(*expressions)
            relation=native.Relation.scan(str(path)).limit(0).aggregate([],measures)
            sql="SELECT "+",".join(map(str,expressions))+f" FROM (SELECT * FROM '{path}' LIMIT 0) AS empty"
            equal(case,case.all_surfaces(frame,relation,sql),expected_reductions(case,[(None,[])],"exact"))
        # Close the producer and remove the source before checking all stored results.
        expressions,measures=declarations(case,"half_even",True)
        frame=read(path,owner).group_by("g").agg(*expressions).sort("g")
        relation=native.Relation.scan(str(path)).aggregate(["g"],measures).sort([("g",False,None)])
        expected=expected_reductions(case,[(g,[r["amount"] for r in rows if r["g"]==g and r["keep"] is True]) for g in ("A","B","N")],
                                     "half_even",source.schema.field("g"))
        producer=native.Session(native.ExecutionResources(128<<20,2))
        retained=producer.plan(relation).collect()
        producer.close()
        outputs=[]
        for fmt,reader in (("vortex",sl.read_vortex),("parquet",sl.read_parquet),("arrow-ipc",sl.read_arrow_ipc)):
            destination=Path(directory)/f"result.{fmt}"
            report=frame.write(destination,output_format=fmt,check=True)
            case.assertFalse(report.envelope.fallback.attempted)
            outputs.append((destination,reader))
        path.unlink()
        equal(case,[case.table(retained)],expected)
        for destination,reader in outputs:
            equal(case,[reader(destination,client=owner).collect(check=True).to_arrow()],expected)
        receiver=native.Session(native.ExecutionResources(128<<20,2))
        reused=receiver.sql("SELECT * FROM 'memory://decimal'",sources=[("memory://decimal",retained.as_source())]).collect()
        receiver.close()
        equal(case,[case.table(reused)],expected)


def check_windows(case):
    import shardloom as sl
    from shardloom import functions as f
    native,owner,source=case.native,client(case),population(case)
    rows=source.to_pylist()
    with tempfile.TemporaryDirectory(prefix="shardloom-decimal-windows-") as directory:
        path=Path(directory)/"input.arrow"
        write(case,path,source)
        for unit in ("rows","range","calendar"):
            order="instant" if unit=="calendar" else "id"
            if unit=="rows":
                specification=sl.Window.order_by(order).rows_between(-3,0)
                frame=native.WindowFrame("rows",("preceding",native.FrameOffset.number(native.Expression.literal(3))),("current_row",None))
            elif unit=="range":
                specification=sl.Window.order_by(order).range_between(-3,0)
                frame=native.WindowFrame("range",("preceding",native.FrameOffset.number(native.Expression.literal(3))),("current_row",None))
            else:
                specification=sl.Window.order_by(order).range_between(sl.Window.calendar_days(-1),0)
                frame=native.WindowFrame("range",("preceding",native.FrameOffset.calendar_days(1)),("current_row",None))
            for mode in MODES:
                expressions,typed=[],[]
                for function,name,level in REDUCERS:
                    args=["amount"]+([] if level is None else [level])
                    expressions.append(getattr(f,function)(*args,precision=38,scale=2,rounding=mode)
                        .filter(f.col("keep")).over(specification).alias(name))
                    typed.append(native.WindowExpression(function,name,source_column="amount",quantile_level=level,
                        filter_column="keep",decimal_output=(38,2,mode),order_by=[(order,False,None)],frame=frame))
                query=read(path,owner).select("id",*expressions).sort("id")
                relation=native.Relation.scan(str(path)).window(["id"],typed).sort([("id",False,None)])
                sql="SELECT id,"+",".join(map(str,expressions))+f" FROM '{path}' ORDER BY id"
                groups=[(row["id"],[other["amount"] for other in rows if
                    (row["id"]-(1 if unit=="calendar" else 3)<=other["id"]<=row["id"]) and other["keep"] is True]) for row in rows]
                expected=expected_reductions(case,groups,mode,source.schema.field("id"))
                equal(case,case.all_surfaces(query,relation,sql),expected)


def refuse(case, frame, relation, statement, message):
    import shardloom as sl
    from shardloom.errors import ShardLoomCommandError
    arguments=["sql","--sql",statement,"--request","collect","--bounded","true",
               "--memory-bytes",str(128<<20),"--max-parallelism","2"]
    for surface,error,run in [("dataframe",ShardLoomCommandError,lambda:frame.collect(check=True)),
        ("typed",RuntimeError,lambda:case.session.plan(relation).collect()),
        ("sql",RuntimeError,lambda:case.session.sql(statement).collect()),
        ("workflow",RuntimeError,lambda:case.session.workflow(arguments).collect())]:
        with case.subTest(surface=surface),case.assertRaisesRegex(error,message):
            run()
    with patch("subprocess.Popen",case.real_popen):
        cli=sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),transport="subprocess",
            use_persistent_worker=False,memory_bytes=128<<20,max_parallelism=2,timeout=30)
        try:
            with case.assertRaisesRegex(ShardLoomCommandError,message) as caught:
                sl.sql(statement,client=cli).collect(check=True)
            case.assertFalse(caught.exception.envelope.fallback.attempted)
        finally:
            cli.close()


def check_exact_integer_sum(case):
    import shardloom as sl
    from shardloom import functions as f
    pa,native,owner=case.pa,case.native,client(case)
    with tempfile.TemporaryDirectory(prefix="shardloom-exact-integer-sum-") as directory:
        for kind,values in ((pa.int64(),[-2**63,2**63-1,2**63-1,None,5]),
                            (pa.uint64(),[2**64-1,2**64-1,1,None,0])):
            path=Path(directory)/f"{kind}.arrow"
            source=pa.Table.from_arrays([pa.array(range(5),pa.int64()),pa.array([0,0,1,1,1],pa.int64()),
                pa.array(values,kind)],schema=pa.schema([pa.field("id",pa.int64(),nullable=False),
                    pa.field("g",pa.int64(),nullable=False),pa.field("value",kind)]))
            write(case,path,source)
            raw=read(path,owner).select("id","g",f.col("value").cast("DECIMAL(38,0)").alias("amount"))
            relation=native.Relation.scan(str(path)).project([("id",native.Expression.column("id")),
                ("g",native.Expression.column("g")),("amount",native.Expression.column("value").cast("decimal128(38,0)"))])
            sql_source=f"(SELECT id,g,CAST(value AS DECIMAL(38,0)) AS amount FROM '{path}') AS exact_values"
            for grouped,empty in ((False,False),(True,False),(False,True)):
                current=raw.limit(0) if empty else raw
                typed=relation.limit(0) if empty else relation
                select=[f.sum("amount").alias("total"),f.sum_distinct("amount").alias("distinct_total")]
                current=current.group_by("g").agg(*select).sort("g") if grouped else current.select(*select)
                typed=typed.aggregate(["g"] if grouped else [],[("sum","amount","total"),("sum_distinct","amount","distinct_total")])
                if grouped:
                    typed=typed.sort([("g",False,None)])
                inner=f"(SELECT * FROM {sql_source} LIMIT 0) AS empty" if empty else sql_source
                sql=("SELECT "+("g," if grouped else "")+",".join(map(str,select))+f" FROM {inner}"
                     +(" GROUP BY g ORDER BY g" if grouped else ""))
                groups=[(g,[value for i,value in enumerate(values) if (0 if i<2 else 1)==g and value is not None]) for g in (0,1)] if grouped else [(None,[] if empty else [value for value in values if value is not None])]
                expected=pa.Table.from_pylist([dict({"g":g} if grouped else {},
                    total=None if not numbers else decimal(sum(numbers),0),
                    distinct_total=None if not numbers else decimal(sum(set(numbers)),0)) for g,numbers in groups],
                    schema=pa.schema(([source.schema.field("g")] if grouped else [])+
                        [pa.field("total",pa.decimal128(38,0)),pa.field("distinct_total",pa.decimal128(38,0))]))
                equal(case,case.all_surfaces(current,typed,sql),expected)
            specification=sl.Window.order_by("id").rows_between(sl.Window.unbounded_preceding,0)
            select=f.sum("amount").over(specification).alias("total")
            query=raw.select("id",select)
            typed=relation.window(["id"],[native.WindowExpression("sum","total",source_column="amount",
                order_by=[("id",False,None)],frame=native.WindowFrame("rows",("unbounded_preceding",None),("current_row",None)))])
            expected=pa.Table.from_pylist([{"id":i,"total":decimal(sum(v for v in values[:i+1] if v is not None),0)} for i in range(5)],
                schema=pa.schema([source.schema.field("id"),pa.field("total",pa.decimal128(38,0))]))
            equal(case,case.all_surfaces(query,typed,f"SELECT id,{select} FROM {sql_source}"),expected)


def check_refusals(case):
    import shardloom as sl
    from shardloom import functions as f
    pa,native,owner=case.pa,case.native,client(case)
    with tempfile.TemporaryDirectory(prefix="shardloom-decimal-refusals-") as directory:
        for index,(values,dtype,function,p,s,mode,message) in enumerate([
            ([],pa.int64(),"avg",8,2,"half_even","requires Decimal128"),
            ([decimal(1,0),decimal(0,0),decimal(0,0)],pa.decimal128(8,0),"avg",8,2,"exact","fractional digits"),
            ([decimal(999,2),decimal(999,2)],pa.decimal128(3,2),"sum",3,2,"half_even","precision overflow"),
        ]):
            path=Path(directory)/f"invalid-{index}.arrow"
            write(case,path,pa.table({"amount":pa.array(values,dtype)}))
            expression=getattr(f,function)("amount",precision=p,scale=s,rounding=mode).alias("value")
            frame=read(path,owner).select(expression)
            relation=native.Relation.scan(str(path)).aggregate([], [native.AggregateMeasure(function,"amount","value",decimal_output=(p,s,mode))])
            refuse(case,frame,relation,f"SELECT {expression} FROM '{path}'",message)
        for descriptor in (
            lambda **kw:native.AggregateMeasure("avg","amount","value",**kw),
            lambda **kw:native.WindowExpression("avg","value",source_column="amount",**kw),
        ):
            for options in ([8,2,"exact"],(8,2),(True,2,"exact"),(8,2.0,"exact"),(0,0,"exact"),
                            (39,2,"exact"),(8,9,"exact"),(8,2,"half_up"),(8,2,None)):
                with case.assertRaises((TypeError,ValueError)):
                    descriptor(decimal_output=options)
        for function in ("count","min","quantile_disc"):
            kwargs={"quantile_level":0.5} if function=="quantile_disc" else {}
            with case.assertRaises(ValueError):
                native.AggregateMeasure(function,"amount","value",decimal_output=(8,2,"exact"),**kwargs)
            with case.assertRaises(ValueError):
                native.WindowExpression(function,"value",source_column="amount",decimal_output=(8,2,"exact"),**kwargs)


def check_streams_and_lazy(case):
    import shardloom as sl
    from shardloom import functions as f, types as t
    pa, native = case.pa, case.native
    with tempfile.TemporaryDirectory(prefix="shardloom-decimal-stream-") as directory:
        source = pa.table({"g": ["a", "a", "b", "b"],
                           "amount": pa.array([decimal(124, 2), decimal(125, 2), None, decimal(-125, 2)],
                                              pa.decimal128(38, 2))})
        expected = pa.Table.from_pylist([
            {"g": "a", "total": decimal(249, 2), "mean": decimal(12, 1)},
            {"g": "b", "total": decimal(-125, 2), "mean": decimal(-12, 1)},
        ], schema=pa.schema([pa.field("g", pa.string()), pa.field("total", pa.decimal128(8, 2)),
                             pa.field("mean", pa.decimal128(8, 1))]))
        for mode in ("collect", "batches", "write", "empty", "late_error", "zero_error"):
            with case.subTest(stream=mode):
                owner, trace = client(case), []
                def producer():
                    trace.append("opened")
                    try:
                        yield source.slice(0, 0)
                        if mode != "empty":
                            for index in range(source.num_rows):
                                yield source.slice(index, 1)
                        if mode.endswith("error"):
                            raise LookupError("decimal producer refused")
                        trace.append("ended")
                    finally:
                        trace.append("closed")
                raw = sl.from_batches(producer, schema={"g": t.string(), "amount": t.decimal128(38, 2)},
                                      streaming=True, client=owner)
                query = raw.select("g", f.decimal_rescale("amount", 38, 3, "exact").alias("amount"))
                query = query.group_by("g").agg(
                    total=f.sum("amount", precision=8, scale=2, rounding="exact"),
                    mean=f.avg("amount", precision=8, scale=1, rounding="half_even"),
                ).sort("g")
                output = Path(directory) / f"stream-{mode}.vortex"
                if mode.endswith("error"):
                    if mode == "zero_error":
                        query = query.limit(0)
                    with case.assertRaisesRegex(LookupError, "decimal producer refused"):
                        query.write_vortex(output)
                    case.assertFalse(output.exists())
                    case.assertEqual(trace, ["opened", "closed"])
                    owner.close()
                    continue
                if mode == "batches":
                    with query.iter_batches(batch_rows=1) as stream:
                        chunks = [batch.to_arrow() for batch in stream]
                        report = stream.report
                    actual = pa.concat_tables(chunks)
                elif mode == "write":
                    report = query.write_vortex(output)
                    actual = sl.read_vortex(output, client=owner).collect(check=True).to_arrow()
                else:
                    report = query.collect(check=True)
                    owner.close()
                    actual = report.to_arrow()
                equal(case, [actual], expected.slice(0, 0) if mode == "empty" else expected)
                case.assertEqual(trace, ["opened", "ended", "closed"])
                case.assertTrue(report.envelope.field_bool("native_input_end_observed"))
                case.assertEqual(report.envelope.field_int("native_input_batch_sources"), 1)
                case.assertFalse(report.envelope.fallback.attempted)
                case.assertEqual(report.envelope.field("external_engine_invoked"), "false")
                owner.close()

        owner = client(case)
        source = pa.table({"id": pa.array([0, 1, 2, 3], pa.int64()),
                           "amount": pa.array([decimal(125, 2), None, decimal(-125, 2), decimal(100, 2)],
                                              pa.decimal128(8, 2)),
                           "divisor": pa.array([2, 0, 0, 0], pa.int64())})
        path = Path(directory) / "lazy.arrow"
        write(case, path, source)
        expression = "CASE_WHEN(id<2,DECIMAL_DIVIDE(amount,divisor,8,2,'half_even'),CAST(NULL AS DECIMAL(8,2)))"
        frame = read(path, owner).select("id", expression + " AS value")
        col, lit, call = native.Expression.column, native.Expression.literal, native.Expression.call
        value = call("decimal_divide", [col("amount"), col("divisor"), lit(8), lit(2), lit("half_even")])
        value = call("case_when", [col("id") < lit(2), value, lit(None).cast("decimal128(8,2)")])
        relation = native.Relation.scan(str(path)).project([("id", col("id")), ("value", value)])
        expected = pa.Table.from_arrays([
            source["id"], pa.array([decimal(62, 2), None, None, None], pa.decimal128(8, 2)),
        ], schema=pa.schema([source.schema.field("id"), pa.field("value", pa.decimal128(8, 2))]))
        equal(case, case.all_surfaces(frame, relation, f"SELECT id,{expression} AS value FROM '{path}'"), expected)
