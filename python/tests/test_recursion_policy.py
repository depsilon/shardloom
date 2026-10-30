import unittest
from unittest.mock import patch

import shardloom as sl
from shardloom._native_backend import NativeBackend, normalize_recursion_limit


class RecursionPolicyTests(unittest.TestCase):
    def test_validation(self):
        self.assertEqual(normalize_recursion_limit(1), 1)
        self.assertEqual(normalize_recursion_limit((1 << 64) - 1), (1 << 64) - 1)
        for value in (True, False, 0, -1, 1 << 64, 1.0, "10", None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                sl.ShardLoomClient(recursion_limit=value)

    def test_client_and_context_immutability_and_inheritance(self):
        client = sl.ShardLoomClient(transport="subprocess", recursion_limit=37)
        self.assertEqual(client.recursion_limit, 37)
        context = sl.context(client=client)
        self.assertEqual(context.recursion_limit, 37)
        with self.assertRaises(AttributeError):
            client.recursion_limit = 42
        with self.assertRaises(AttributeError):
            context.recursion_limit = 42
        with self.assertRaises(ValueError):
            sl.context(client=client, recursion_limit=38)
        with self.assertRaises(ValueError):
            context.session(recursion_limit=38)
        self.assertEqual(context.session(recursion_limit=37).client, client)

    def test_invalid_limit_rejected_without_native_import(self):
        with patch("shardloom._native_backend.load_native", side_effect=AssertionError("loaded")):
            with self.assertRaises(ValueError):
                sl.ShardLoomClient(transport="native", recursion_limit=0)
            with self.assertRaises(ValueError):
                sl.context(transport="native", recursion_limit=True)

    def test_subprocess_argument_only_when_nondefault(self):
        default = sl.ShardLoomClient(transport="subprocess")
        self.assertNotIn("--recursion-limit", default._command_args(["run"]))
        custom = sl.ShardLoomClient(transport="subprocess", recursion_limit=25)
        args = custom._command_args(["run"])
        self.assertEqual(args[args.index("--recursion-limit") + 1], "25")

    def test_native_backend_passes_limit_to_session(self):
        class FakeModule:
            class Session:
                def __init__(self, allocation, **kwargs):
                    self.kwargs = kwargs

        with patch("shardloom._native_backend.load_native", return_value=FakeModule):
            backend = NativeBackend(None, None, recursion_limit=9)
        self.assertEqual(backend.session.kwargs["recursion_limit"], 9)


if __name__ == "__main__":
    unittest.main()
