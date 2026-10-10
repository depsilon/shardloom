# SPDX-License-Identifier: Apache-2.0
"""Independent exact values and DTypes for the public typed input contract."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from shardloom._result_schema import ResultType as T


def rich_fixture():
    """Include every admitted leaf and recursive container, without input helpers."""
    schema, fields = {}, []
    rows, expected = [{}, {}, {}], [{}, {}, {}]
    for signed in (True, False):
        for bits in (8, 16, 32, 64):
            name = ("i" if signed else "u") + str(bits)
            token = ("int" if signed else "uint") + str(bits)
            schema[name] = token
            fields.append((name, T(token, True)))
            limits = (-(1 << (bits - 1)), (1 << (bits - 1)) - 1, 0) if signed else (0, (1 << bits) - 1, 1)
            for row, wanted, value in zip(rows, expected, limits):
                row[name] = wanted[name] = value
    schema.update(f32="float32", f64="float64", flag="bool", text="utf8",
                  blob="binary", amount="decimal128(38,2)", day="date32", stamp="timestamp_micros")
    fields.extend((name, T(token, True)) for name, token in (
        ("f32", "float32"), ("f64", "float64"), ("flag", "bool"), ("text", "utf8"),
        ("blob", "binary")))
    fields.extend((("amount", T("decimal128", True, (38, 2))),
                   ("day", T("date32", True)), ("stamp", T("timestamp_micros", True))))
    rows[0].update(f32=-1.25, f64=-2.5, flag=True, text='東京 λ "\n', blob=b"\x00\xffA",
                   amount=Decimal("-999999999999999999999999999999999999.99"),
                   day=-(1 << 31), stamp=-(1 << 63))
    rows[1].update(f32=1.25, f64=2.5, flag=False, text="", blob=bytearray(),
                   amount=Decimal("999999999999999999999999999999999999.99"),
                   day=(1 << 31) - 1, stamp=(1 << 63) - 1)
    rows[2].update(f32=0.0, f64=0.0, flag=None, text="space,;%=null", blob=memoryview(b"\x00"),
                   amount=Decimal("0.00"), day=dt.date(1969, 12, 31),
                   stamp=dt.datetime(1969, 12, 31, 23, 59, 59, 999999))
    expected[0].update(f32=-1.25, f64=-2.5, flag=True, text='東京 λ "\n', blob="00ff41",
                       amount="decimal128(38,2):-99999999999999999999999999999999999999",
                       day=-(1 << 31), stamp=-(1 << 63))
    expected[1].update(f32=1.25, f64=2.5, flag=False, text="", blob="",
                       amount="decimal128(38,2):99999999999999999999999999999999999999",
                       day=(1 << 31) - 1, stamp=(1 << 63) - 1)
    expected[2].update(f32=0.0, f64=0.0, flag=None, text="space,;%=null", blob="00",
                       amount="decimal128(38,2):0", day=-1, stamp=-1)
    schema["samples"] = {"type": "list", "item": {"type": "fixed_size_list", "size": 2,
                          "item": {"type": "int16", "nullable": False}}}
    fields.append(("samples", T("list", True, (T("fixed_size_list", True, (T("int16", False), 2)),))))
    schema["detail"] = {"type": "struct", "fields": {
        "tag": {"type": "utf8", "nullable": False},
        "amounts": {"type": "list", "item": "decimal128(8,2)"}, "payload": "binary",
        "enabled": {"type": "bool", "nullable": False},
        "number": {"type": "int64", "nullable": False},
        "measure": {"type": "float64", "nullable": False}}}
    fields.append(("detail", T("struct", True, (
        ("tag", T("utf8", False)), ("amounts", T("list", True, (T("decimal128", True, (8, 2)),))),
        ("payload", T("binary", True)), ("enabled", T("bool", False)),
        ("number", T("int64", False)), ("measure", T("float64", False))))))
    schema["empty_vector"] = {"type": "fixed_size_list", "size": 0, "item": "uint8"}
    fields.append(("empty_vector", T("fixed_size_list", True, (T("uint8", True), 0))))
    rows[0].update(samples=[(1, -2), None, (-32768, 32767)], empty_vector=[], detail={
        "tag": "λ", "amounts": [Decimal("-12.34"), None, Decimal("0.01")], "payload": b"\0",
        "enabled": True, "number": -(1 << 63), "measure": -1.25})
    expected[0].update(samples=[[1, -2], None, [-32768, 32767]], empty_vector=[], detail={
        "tag": "λ", "amounts": ["decimal128(8,2):-1234", None, "decimal128(8,2):1"], "payload": "00",
        "enabled": True, "number": -(1 << 63), "measure": -1.25})
    rows[1].update(samples=[], empty_vector=None, detail=None)
    expected[1].update(samples=[], empty_vector=None, detail=None)
    rows[2].update(samples=None, empty_vector=[], detail={
        "tag": "", "amounts": [], "payload": b"", "enabled": False, "number": (1 << 63) - 1,
        "measure": 1.25})
    expected[2].update(samples=None, empty_vector=[], detail={
        "tag": "", "amounts": [], "payload": "", "enabled": False, "number": (1 << 63) - 1,
        "measure": 1.25})
    rows.append(dict.fromkeys(schema))
    expected.append(dict.fromkeys(schema))
    return schema, tuple(fields), rows, expected


def rich_python_values():
    """Expected logical objects from declared fixture values, without output adapters."""
    _, _, supplied, transport = rich_fixture()
    expected = [dict(row) for row in transport]
    for index in range(3):
        expected[index]["blob"] = bytes(supplied[index]["blob"])
        expected[index]["amount"] = supplied[index]["amount"]
        expected[index]["day"] = supplied[index]["day"]
        expected[index]["stamp"] = supplied[index]["stamp"]
        if supplied[index]["detail"] is not None:
            expected[index]["detail"] = dict(supplied[index]["detail"])
    return expected
