"""Deferred implementations for query literals.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _normalize_decimal_cast_dtype(dtype):
    compact = "".join(dtype.split())
    names = ("decimal128", "decimal", "numeric")
    name = compact.split("(", 1)[0]
    if name not in names:
        return None
    if "(" not in compact:
        return "decimal128(38,0)"
    if not compact.endswith(")"):
        raise ValueError("decimal cast dtype must use decimal128(precision,scale)")
    args = compact[compact.find("(") + 1 : -1].split(",")
    if len(args) != 2:
        raise ValueError("decimal cast dtype must use decimal128(precision,scale)")
    try:
        precision = int(args[0])
        scale = int(args[1])
    except ValueError as exc:
        raise ValueError("decimal cast precision and scale must be integers") from exc
    if precision < 1 or precision > 38 or scale < 0 or scale > precision:
        raise ValueError(
            "decimal cast precision/scale must satisfy 1 <= precision <= 38 and 0 <= scale <= precision"
        )
    return f"decimal128({precision},{scale})"


def _normalize_interval_integer(value):
    if isinstance(value, bool):
        raise ValueError("interval literal value must be a signed integer literal")
    if isinstance(value, int):
        return value
    text = _api._require_non_empty("interval literal value", value)
    if text in {"+", "-"} or not all(
        ch.isdigit() or (index == 0 and ch in {"+", "-"})
        for index, ch in enumerate(text)
    ):
        raise ValueError("interval literal value must be a signed integer literal")
    return int(text)


def _normalize_date_arithmetic_days(value):
    if isinstance(value, _api.ColumnExpression):
        return _api._sql_computed_projection_expression(value)
    interval = _api._coerce_interval_literal(value)
    if interval is not None:
        if interval.unit != "DAY":
            raise ValueError("date arithmetic interval literals admit DAY units only")
        if _api.builtins.abs(interval.value) > _api.MAX_DATE_ARITHMETIC_DAYS:
            raise ValueError("date arithmetic days admits absolute values <= 366000")
        return interval.sql
    if isinstance(value, bool):
        raise ValueError("date arithmetic days must be a signed integer literal")
    if isinstance(value, int):
        days = value
    else:
        text = _api._require_non_empty("date arithmetic days", value)
        if text in {"+", "-"} or not all(
            ch.isdigit() or (index == 0 and ch in {"+", "-"})
            for index, ch in enumerate(text)
        ):
            raise ValueError("date arithmetic days must be a signed integer literal")
        days = int(text)
    if not -(1 << 63) <= days < (1 << 64):
        raise ValueError("date arithmetic days must fit int64 or uint64")
    return str(days)


def _normalize_timestamp_arithmetic_seconds(value):
    if isinstance(value, _api.ColumnExpression):
        return _api._sql_computed_projection_expression(value)
    interval = _api._coerce_interval_literal(value)
    if interval is not None:
        seconds = interval.value * _api._INTERVAL_SECOND_MULTIPLIERS[interval.unit]
        if _api.builtins.abs(seconds) > _api.MAX_TIMESTAMP_ARITHMETIC_SECONDS:
            raise ValueError(
                "timestamp arithmetic seconds admits absolute values <= 31622400000"
            )
        return interval.sql
    if isinstance(value, bool):
        raise ValueError("timestamp arithmetic seconds must be a signed integer literal")
    if isinstance(value, int):
        seconds = value
    else:
        text = _api._require_non_empty("timestamp arithmetic seconds", value)
        if text in {"+", "-"} or not all(
            ch.isdigit() or (index == 0 and ch in {"+", "-"})
            for index, ch in enumerate(text)
        ):
            raise ValueError(
                "timestamp arithmetic seconds must be a signed integer literal"
            )
        seconds = int(text)
    if not -(1 << 63) <= seconds < (1 << 64):
        raise ValueError("timestamp arithmetic seconds must fit int64 or uint64")
    return str(seconds)


def _parse_interval_literal_sql(value):
    text = value.strip()
    lowered = text.lower()
    if not lowered.startswith("interval"):
        return None
    if len(text) > len("interval") and not text[len("interval")].isspace():
        return None
    parts = text.split(maxsplit=2)
    if len(parts) != 3 or parts[0].lower() != "interval":
        raise ValueError(
            "interval SQL literals must use INTERVAL '<signed integer>' DAY|HOUR|MINUTE|SECOND"
        )
    literal = parts[1]
    if len(literal) < 3 or not literal.startswith("'") or not literal.endswith("'"):
        raise ValueError("interval SQL literal value must be single quoted")
    unit = _api._normalize_interval_unit(parts[2])
    return _api._interval_literal(literal[1:-1], unit)


def _sql_temporal_difference_arg(value, dtype):
    if isinstance(value, _api.ColumnExpression):
        return value.sql
    if dtype == "date32":
        if isinstance(value, _api.datetime):
            raise TypeError("date_diff_days arguments must be date values or columns")
        if isinstance(value, _api.date):
            return f"DATE '{value.isoformat()}'"
    elif dtype == "timestamp_micros":
        if isinstance(value, _api.datetime):
            return f"TIMESTAMP '{_api._normalize_timestamp_literal(value)}'"
    else:
        raise ValueError("temporal difference dtype must be date32 or timestamp_micros")
    raise TypeError("temporal difference arguments must be shardloom columns or typed literals")


def _decimal_controls(precision, scale, rounding):
    if type(precision) is not int:
        raise TypeError("decimal precision must be an integer")
    if not 1 <= precision <= 38:
        raise ValueError("decimal precision must be between 1 and 38")
    if type(scale) is not int:
        raise TypeError("decimal scale must be an integer")
    if not 0 <= scale <= precision:
        raise ValueError("decimal scale must be between 0 and precision")
    if type(rounding) is not str:
        raise TypeError("decimal rounding must be a string")
    if rounding not in {"exact", "half_even", "half_away", "toward_zero", "floor", "ceil"}:
        raise ValueError("unsupported decimal rounding mode")
    return precision, scale, rounding


def _sql_literal(value):
    if value is None:
        raise ValueError("SQL NULL comparisons must use is_null() or is_not_null()")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not _api.math.isfinite(value):
            raise ValueError("SQL float literals must be finite")
        return str(value)
    if isinstance(value, _api.Decimal):
        return _api._sql_decimal_literal(value)
    if isinstance(value, _api.datetime):
        return f"TIMESTAMP '{_api._normalize_timestamp_literal(value)}'"
    if isinstance(value, _api.date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, (bytes, bytearray)):
        return f"X'{bytes(value).hex()}'"
    if isinstance(value, str):
        return _api._sql_string_function_literal("predicate string literal", value, allow_empty=True)
    raise TypeError(
        "SQL predicate literals must be bool, int, float, Decimal, str, bytes, date, datetime, or None"
    )


def _decimal_literal_metadata(value):
    if not value.is_finite():
        raise ValueError("Decimal literals must be finite")
    _, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)
    scale = max(-exponent, 0)
    precision = max(len(digits) + max(exponent, 0), scale, 1)
    if precision > 38 or scale > 38:
        raise ValueError("Decimal literals must fit decimal128 precision and scale <= 38")
    # Check metadata first: formatting a huge exponent must never allocate a
    # correspondingly huge SQL string. This formatted value is at most 41 bytes.
    return precision, scale


def _vortex_exact_scalar_payload(value):
    if isinstance(value, _api.Decimal):
        precision, scale = _api._decimal_literal_metadata(value)
        return {"type": f"decimal128({precision},{scale})", "value": format(value, "f")}
    if isinstance(value, _api.datetime):
        return {"type": "timestamp_micros", "value": _api._normalize_timestamp_literal(value)}
    if isinstance(value, _api.date):
        return {"type": "date32", "value": value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {"type": "binary", "value": bytes(value).hex()}
    return None


def _vortex_expression_scalar_payload(value, *, target_dtype, allow_null):
    if target_dtype is None:
        # Literal typing is independent of input schema; native binding and
        # checked coercion remain authoritative for the produced column.
        if value is None:
            return {"type": "null", "value": None} if allow_null else None
        if isinstance(value, bool):
            return {"type": "boolean", "value": value}
        if isinstance(value, int) and -(1 << 63) <= value < (1 << 64):
            return {"type": "int64" if value < (1 << 63) else "uint64", "value": value}
        if isinstance(value, float) and _api.math.isfinite(value):
            return {"type": "float64", "value": value}
        if isinstance(value, str):
            return {"type": "utf8", "value": value}
        return _api._vortex_exact_scalar_payload(value)
    temporal = _api.parse_temporal_declaration(target_dtype)
    dtype = temporal or target_dtype.strip().lower().replace("-", "_")
    decimal_dtype = _api._normalize_decimal_cast_dtype(dtype)
    supported_dtype = dtype in {
        "bool",
        "boolean",
        "int64",
        "int",
        "integer",
        "uint64",
        "uint",
        "unsigned",
        "float64",
        "float",
        "double",
        "utf8",
        "string",
        "str",
        "binary",
        "date32",
        "timestamp_micros",
    } or decimal_dtype is not None or temporal is not None
    if value is None:
        if not allow_null:
            return None
        return {"type": "null", "value": None} if supported_dtype else None
    if decimal_dtype is not None:
        if isinstance(value, _api.Decimal):
            return _api._vortex_exact_scalar_payload(value)
        if isinstance(value, int) and not isinstance(value, bool) and -(1 << 63) <= value < (1 << 64):
            return {"type": "int64" if value < (1 << 63) else "uint64", "value": value}
        return None
    if temporal is not None:
        if temporal in {"date32", "timestamp_micros"}:
            literal = _api._vortex_exact_scalar_payload(value)
            return literal if literal is not None and literal["type"] == temporal else None
        from ._input_schema import temporal_input_value
        try:
            return {"type": temporal, "value": str(temporal_input_value(temporal, value))}
        except (ValueError, TypeError):
            return None
    if dtype in {"binary", "date32", "timestamp_micros"}:
        literal = _api._vortex_exact_scalar_payload(value)
        return literal if literal is not None and literal["type"] == dtype else None
    if dtype in {"bool", "boolean"}:
        if isinstance(value, bool):
            return {"type": "boolean", "value": value}
        return None
    if dtype in {"int64", "int", "integer"}:
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return {"type": "int64", "value": value}
    if dtype in {"uint64", "uint", "unsigned"}:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        return {"type": "uint64", "value": value}
    if dtype in {"float64", "float", "double"}:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        parsed = float(value)
        if not _api.math.isfinite(parsed):
            return None
        return {"type": "float64", "value": parsed}
    if dtype in {"utf8", "string", "str"}:
        if isinstance(value, str):
            return {"type": "utf8", "value": value}
        return None
    return None
