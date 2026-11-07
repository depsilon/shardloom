"""Raw binary16 reference values and mixed native lifecycle fixtures."""

import struct

if __package__:
    from .test_native_tensor_binding import _rows, _digest
    from .test_native_tensor_variable_binding import _independent_schema_equal
else:
    from test_native_tensor_binding import _rows, _digest
    from test_native_tensor_variable_binding import _independent_schema_equal


BITS = [0x8000, 0, 0xfc00, 0x7c00, 0xfc42, 0x7e81, 0x7c01,
        1, 0x8001, 0x3c00, 0xbc00, None, 0x7bff, 0xfbff, 0x3555, 0x2e66, 0x2e67]
FINITE_BITS = [0x8000, 0, 1, 0x8001, 0x3c00, 0xbc00, None,
               0x7bff, 0xfbff, 0x3555, 0x2e66, 0x2e67]


def _half_array(pa, bits):
    valid = bytearray((len(bits) + 7) // 8)
    for index, value in enumerate(bits):
        if value is not None:
            valid[index // 8] |= 1 << (index % 8)
    return pa.Array.from_buffers(pa.float16(), len(bits), [pa.py_buffer(valid),
        pa.py_buffer(b"".join(struct.pack("<H", value or 0) for value in bits))],
        null_count=sum(value is None for value in bits))


def _wide(bits):
    """Independent Python reference that does not quiet a carried half NaN."""
    if bits is None:
        return None
    if bits & 0x7c00 == 0x7c00:
        raw = ((bits & 0x8000) << 48) | 0x7ff0000000000000 | ((bits & 0x3ff) << 42)
        return struct.unpack("<d", struct.pack("<Q", raw))[0]
    return struct.unpack("<e", struct.pack("<H", bits))[0]


def _independent_mask(table):
    """Rows without absent fixed-list parents, including nested collections.

    PyArrow 25's Parquet reader cannot reconstruct some fixed-list null parents.
    Primitive null components remain included in this independent-read subset.
    """
    import pyarrow as pa

    def contains_fixed(dtype):
        if isinstance(dtype, pa.BaseExtensionType):
            return contains_fixed(dtype.storage_type)
        if pa.types.is_fixed_size_list(dtype):
            return True
        if pa.types.is_list(dtype):
            return contains_fixed(dtype.value_type)
        if pa.types.is_struct(dtype):
            return any(contains_fixed(field.type) for field in dtype)
        return False

    def admitted(array, row):
        if isinstance(array.type, pa.BaseExtensionType):
            return admitted(array.storage, row)
        if not array[row].is_valid:
            return not contains_fixed(array.type)
        if pa.types.is_fixed_size_list(array.type) or pa.types.is_list(array.type):
            child = array[row].values
            return all(admitted(child, index) for index in range(len(child)))
        if pa.types.is_struct(array.type):
            return all(admitted(array.field(index), row) for index in range(array.type.num_fields))
        return True

    return [all(admitted(column, row) for column in batch.columns)
            for batch in table.to_batches() for row in range(batch.num_rows)]


def _fixture(count, *, finite=False):
    import pyarrow as pa
    from shardloom import types as t
    from shardloom._result_schema import _arrow_field

    pattern = FINITE_BITS if finite else BITS
    half = t.float16()
    declarations = [
        ("id", t.int64(nullable=False)), ("x", half),
        ("values", t.list_(half)), ("vector", t.fixed_size_list(half, 3)),
        ("embedding", t.embedding(half, 3, metadata={"space_id": "binary16"})),
        ("vectors", t.list_(t.fixed_size_list(half, 3))),
        ("embeddings", t.list_(t.embedding(half, 3, metadata={"space_id": "binary16"}))),
        ("tensor", t.fixed_shape_tensor(half, [2, 2], permutation=[1, 0], dim_names=["行", "column"])),
        ("variable", t.variable_shape_tensor(half, 2, permutation=[1, 0], uniform_shape=[None, 2])),
        ("content", t.binary()),
    ]
    fields = [_arrow_field(name, dtype, pa) for name, dtype in declarations]
    by_name = {field.name: field for field in fields}

    def values(length, shift=0):
        return _half_array(pa, [pattern[(index + shift) % len(pattern)] for index in range(length)])

    def fixed(field, length, shift):
        width = field.type.list_size
        return pa.FixedSizeListArray.from_arrays(values(length * width, shift), type=field.type,
            mask=pa.array([row % 17 == 0 for row in range(length)], pa.bool_()))

    offsets = [0]
    for row in range(count):
        offsets.append(offsets[-1] + row % 4)
    parent_mask = pa.array([row % 13 == 0 for row in range(count)], pa.bool_())
    ragged = pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), values(offsets[-1], 2),
        type=by_name["values"].type, mask=parent_mask)
    vector = fixed(by_name["vector"], count, 3)
    embedding = fixed(by_name["embedding"], count, 4)
    collections = []
    for name, shift in (("vectors", 5), ("embeddings", 6)):
        field = by_name[name]
        child = fixed(field.type.value_field, offsets[-1], shift)
        collections.append(pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), child,
            type=field.type, mask=parent_mask))
    tensor = fixed(by_name["tensor"], count, 7)
    shape = [[row % 4, 2] for row in range(count)]
    data_offsets = [0]
    for dimensions in shape:
        data_offsets.append(data_offsets[-1] + dimensions[0] * dimensions[1])
    variable_type = by_name["variable"].type
    data = pa.ListArray.from_arrays(pa.array(data_offsets, pa.int32()), values(data_offsets[-1], 8),
                                   type=variable_type.field("data").type)
    dimensions = pa.FixedSizeListArray.from_arrays(
        pa.array([dimension for row in shape for dimension in row], pa.int32()),
        type=variable_type.field("shape").type)
    variable = pa.StructArray.from_arrays([data, dimensions], fields=list(variable_type),
                                          mask=parent_mask)
    columns = [pa.array(range(count), pa.int64()), values(count), ragged, vector, embedding,
               *collections, tensor, variable,
               pa.array([bytes((row % 251, 0, 255)) * 192 for row in range(count)], pa.binary())]
    return pa.Table.from_arrays(columns, schema=pa.schema(fields))
