"""Declaration byte bounds remain separate from operation schema admission."""

import json
import unittest

from shardloom._input_schema import normalize_schema, wire_schema
from shardloom._result_schema import schema_fields

if __package__:
    from .wide_schema_fixture import KINDS, schema
else:
    from wide_schema_fixture import KINDS, schema


def nodes(dtype):
    if dtype.name == "struct":
        return 1 + sum(nodes(child) for _, child in dtype.parameters)
    if dtype.name == "list":
        return 1 + nodes(dtype.parameters[0])
    return 1


class WideSchemaTests(unittest.TestCase):
    def test_fixed_wide_shapes_preserve_every_declared_field_in_wire_metadata(self):
        expected = {"siblings": 1026, "nodes": 5121, "metadata": 8511}
        for kind in KINDS:
            with self.subTest(kind=kind):
                fields = normalize_schema(schema(kind))
                self.assertEqual(nodes(dict(fields)["payload"]), expected[kind])
                if kind == "metadata":
                    # Every former schema node cost at least 1024 bytes, even
                    # before names and extension metadata were added.
                    self.assertGreater(nodes(dict(fields)["payload"]) * 1024, 8 << 20)
                wire = wire_schema(fields)
                self.assertLess(len(json.dumps(wire).encode()), 8 << 20)
                for (name, value), (_, dtype) in zip(wire, fields):
                    # The public result decoder independently checks all names,
                    # nested types and extension metadata after input encoding.
                    native = json.loads(value["native"]["dtype"])
                    wrapper = {"Struct": [{"names": [name], "dtypes": [native]}, False]}
                    decoded = schema_fields(json.dumps(wrapper), "vortex.dtype.serde.v1")
                    self.assertEqual(decoded, ((name, dtype),))

    def test_wire_size_and_cyclic_depth_controls_are_still_explicit(self):
        with self.assertRaisesRegex(ValueError, "8 MiB declaration"):
            normalize_schema({"wide": {"type": "struct", "fields": {"n" * (8 << 20): "int64"}}})
        cyclic = {"type": "list"}
        cyclic["item"] = cyclic
        with self.assertRaisesRegex(ValueError, "depth 24"):
            normalize_schema({"cyclic": cyclic})


if __name__ == "__main__":
    unittest.main()
