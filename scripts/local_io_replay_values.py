# SPDX-License-Identifier: Apache-2.0
"""Independent saved-value readers for the finite local I/O evidence contract."""
import csv
import io
import json

from native_workflow_protocol import strict_json
from query_surface.replay import need


def arrow():
    # Optional evidence dependencies never enter ShardLoom execution.
    import pyarrow
    return pyarrow


def ipc(data):
    pa = arrow()
    return pa.ipc.open_file(pa.BufferReader(data)).read_all()


def logical(table):
    pa = arrow()

    def field(value):
        return pa.field(value.name, dtype(value.type), value.nullable, value.metadata)

    def dtype(value):
        if pa.types.is_large_string(value) or pa.types.is_string_view(value):
            return pa.string()
        if pa.types.is_large_binary(value) or pa.types.is_binary_view(value):
            return pa.binary()
        if pa.types.is_fixed_size_list(value):
            return pa.list_(field(value.value_field), value.list_size)
        if pa.types.is_list(value) or pa.types.is_large_list(value):
            return pa.list_(field(value.value_field))
        if pa.types.is_struct(value):
            return pa.struct([field(child) for child in value])
        return value

    schema = pa.schema([field(value) for value in table.schema], table.schema.metadata)
    return table.cast(schema)


def compare(actual, expected):
    actual, expected = logical(actual), logical(expected)
    need(actual.schema.equals(expected.schema, check_metadata=True),
         'saved logical schema/nullability/metadata differs')
    need(actual.equals(expected), 'complete saved values or row order differ')
    return expected.num_rows


def translated(table, fmt):
    """Only the original declared ORC/Avro fidelity translations are admitted."""
    pa = arrow()
    if fmt not in {'orc', 'avro'}:
        return table

    def dtype(value):
        if pa.types.is_timestamp(value):
            if fmt == 'orc':
                return pa.timestamp('ns', 'UTC' if value.tz else None)
            return pa.timestamp(value.unit, '+00:00' if value.tz else None)
        if pa.types.is_list(value):
            return pa.list_(field(value.value_field).with_name('item'))
        if pa.types.is_struct(value):
            return pa.struct([field(child) for child in value])
        return value

    def field(value):
        metadata = None
        if fmt == 'orc' and pa.types.is_timestamp(value.type):
            metadata = {b'shardloom.arrow.timestamp.unit': value.type.unit.encode()}
        return pa.field(value.name, dtype(value.type), True, metadata)

    return table.cast(pa.schema([field(value) for value in table.schema]))


def avro_fields(actual, expected):
    need(isinstance(actual, list) and len(actual) == len(expected), 'Avro field count differs')
    for item, field in zip(actual, expected, strict=True):
        need(item.get('name') == field.name, 'Avro field name/order differs')
        avro_type(item.get('type'), field)


def avro_type(actual, expected):
    pa = arrow()
    choices = actual if isinstance(actual, list) else [actual]
    nulls = [item for item in choices if item == 'null' or item == {'type': 'null'}]
    values = [item for item in choices if item not in nulls]
    need(len(nulls) == int(expected.nullable) and len(values) == 1, 'Avro nullability differs')
    value, dtype = values[0], expected.type
    wire = value.get('type') if isinstance(value, dict) else value
    primitives = {pa.int64(): 'long', pa.int32(): 'int', pa.float64(): 'double',
                  pa.float32(): 'float', pa.string(): 'string', pa.bool_(): 'boolean',
                  pa.binary(): 'bytes'}
    if dtype in primitives:
        valid = wire == primitives[dtype] and not (isinstance(value, dict) and value.get('logicalType'))
    elif pa.types.is_decimal128(dtype):
        valid = (isinstance(value, dict) and wire in {'bytes', 'fixed'}
                 and value.get('logicalType') == 'decimal' and value.get('precision') == dtype.precision
                 and value.get('scale', 0) == dtype.scale)
    elif pa.types.is_date32(dtype):
        valid = isinstance(value, dict) and wire == 'int' and value.get('logicalType') == 'date'
    elif pa.types.is_timestamp(dtype):
        tag = ('timestamp-' if dtype.tz else 'local-timestamp-') + {
            'ms': 'millis', 'us': 'micros', 'ns': 'nanos'}[dtype.unit]
        valid = isinstance(value, dict) and wire == 'long' and value.get('logicalType') == tag
    elif pa.types.is_list(dtype):
        valid = isinstance(value, dict) and wire == 'array'
        if valid:
            avro_type(value['items'], dtype.value_field)
    elif pa.types.is_struct(dtype):
        valid = isinstance(value, dict) and wire == 'record'
        if valid:
            avro_fields(value.get('fields'), dtype)
    else:
        valid = False
    need(valid, 'Avro physical/logical type differs')


def read_output(data, fmt, schema):
    pa = arrow()
    stream = pa.BufferReader(data)
    if fmt == 'parquet':
        import pyarrow.parquet
        return pyarrow.parquet.ParquetFile(stream).read()
    if fmt == 'arrow-ipc':
        return pa.ipc.open_file(stream).read_all()
    if fmt == 'orc':
        import pyarrow.orc
        return pyarrow.orc.ORCFile(stream).read()
    if fmt == 'avro':
        import fastavro
        reader = fastavro.reader(io.BytesIO(data))
        avro_fields(reader.writer_schema.get('fields'), schema)
        return pa.Table.from_pylist(list(reader), schema=schema)
    if fmt in {'json', 'jsonl'}:
        values = strict_json(data) if fmt == 'json' else [strict_json(line) for line in data.splitlines()]
        need(isinstance(values, list) and all(isinstance(row, dict) and set(row) == set(schema.names)
             for row in values), 'JSON rows/fields differ')
        for row in values:
            for field in schema:
                value = row[field.name]
                if value is None:
                    need(field.nullable, 'unexpected JSON null')
                elif pa.types.is_integer(field.type):
                    need(type(value) is int, 'JSON integer type differs')
                elif pa.types.is_floating(field.type):
                    need(type(value) in (int, float), 'JSON floating type differs')
                elif pa.types.is_boolean(field.type):
                    need(type(value) is bool, 'JSON boolean type differs')
                else:
                    need(pa.types.is_string(field.type) and type(value) is str, 'JSON string type differs')
        return pa.Table.from_pylist(values, schema=schema)
    need(fmt == 'csv', 'unsupported saved output format')
    records = csv.reader(io.StringIO(data.decode(), newline=''), strict=True)
    need(next(records, None) == schema.names, 'CSV names/order differ')
    rows = []
    for cells in records:
        need(len(cells) == len(schema), 'CSV width differs')
        row = {}
        for field, value in zip(schema, cells, strict=True):
            if not value.strip() or value.strip().lower() == 'null':
                parsed = None
            elif pa.types.is_string(field.type):
                parsed = value
            elif pa.types.is_boolean(field.type):
                need(value.lower() in {'true', 'false'}, 'invalid CSV boolean')
                parsed = value.lower() == 'true'
            else:
                parsed = int(value) if pa.types.is_integer(field.type) else float(value)
            row[field.name] = parsed
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def wire_scalar(scalar, dtype):
    pa = arrow()
    if not scalar.is_valid:
        return None
    if pa.types.is_date(dtype) or pa.types.is_timestamp(dtype):
        return scalar.value
    if pa.types.is_decimal(dtype):
        value = scalar.as_py().as_tuple()
        exponent = value.exponent + dtype.scale
        need(exponent >= 0, 'decimal scale differs')
        coefficient = int(''.join(str(digit) for digit in value.digits)) * 10 ** exponent
        return f'decimal128({dtype.precision},{dtype.scale}):{(-1 if value.sign else 1) * coefficient}'
    if pa.types.is_binary(dtype):
        return scalar.as_py().hex()
    if pa.types.is_list(dtype):
        return [wire_scalar(value, dtype.value_type) for value in scalar.values]
    if pa.types.is_struct(dtype):
        return {field.name: wire_scalar(scalar[field.name], field.type) for field in dtype}
    return scalar.as_py()


def compare_wire(data, fmt, expected):
    """Rich text checks exact wire values, without claiming embedded typed schemas."""
    pa = arrow()
    rows = [{field.name: wire_scalar(column[row], field.type)
             for field, column in zip(expected.schema, expected.columns, strict=True)}
            for row in range(expected.num_rows)]
    if fmt in {'json', 'jsonl'}:
        actual = strict_json(data) if fmt == 'json' else [strict_json(line) for line in data.splitlines()]
        need(exact_json(actual, rows), 'complete rich JSON wire values/order differ')
        return
    need(fmt == 'csv', 'unsupported rich wire format')
    old_limit = csv.field_size_limit(64 << 20)
    try:
        reader = csv.reader(io.StringIO(data.decode(), newline=''), strict=True)
        need(next(reader, None) == expected.schema.names, 'rich CSV header/order differs')
        expected_cells = []
        for row in rows:
            cells = []
            for field in expected.schema:
                value = row[field.name]
                if value is None:
                    cells.append('')
                elif pa.types.is_string(field.type):
                    cells.append(value)
                elif pa.types.is_binary(field.type) or pa.types.is_decimal(field.type) or pa.types.is_nested(field.type):
                    cells.append(json.dumps(value, ensure_ascii=False, separators=(',', ':')))
                elif isinstance(value, bool):
                    cells.append(str(value).lower())
                else:
                    cells.append(str(value))
            expected_cells.append(cells)
        need(list(reader) == expected_cells, 'complete rich CSV cells/order differ')
    finally:
        csv.field_size_limit(old_limit)


def exact_json(actual, expected):
    # Python equality alone would equate true with 1, or 1.0 with integer ticks.
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(exact_json(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(exact_json(a, b) for a, b in zip(actual, expected))
    return actual == expected
