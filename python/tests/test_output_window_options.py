"""Validate delivery configuration before loading native code or demanding input."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import shardloom as sl
from shardloom._native_backend import NativeBackend, NativeBatchIterator, validate_output_window_bytes


WINDOW = 4 << 20


class IntegerOverrides(int):
    def forbidden(self, *args):
        raise AssertionError("integer override invoked")

    __eq__ = __ne__ = __bool__ = __int__ = __index__ = forbidden


class OutputWindowOptionTests(unittest.TestCase):
    def test_only_zero_and_four_mib_integers_are_accepted(self):
        class Integer(int):
            pass

        for value in (0, WINDOW, Integer(0), Integer(WINDOW)):
            self.assertEqual(validate_output_window_bytes(value), value)
        for value in (True, False, None, 0.0, float("nan"), "4194304"):
            with self.subTest(value=value), self.assertRaises(TypeError):
                validate_output_window_bytes(value)
        for value in (-1, 1, WINDOW - 1, WINDOW + 1, 1 << 80):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_output_window_bytes(value)
        for number in (0, WINDOW):
            normalized = validate_output_window_bytes(IntegerOverrides(number))
            self.assertIs(type(normalized), int)
            self.assertEqual(normalized, number)
        with self.assertRaises(ValueError):
            validate_output_window_bytes(IntegerOverrides(7))

    def test_client_refuses_invalid_options_before_backend_or_producer_work(self):
        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        with patch.object(client, "_native_backend_for", side_effect=AssertionError("native loading")):
            for value in (True, None, 1.0, "4194304", -1, 1, 1 << 80, IntegerOverrides(7)):
                with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                    client.public_workflow_batches("sql", sql_statement="SELECT 1", output_window_bytes=value)

    def test_subprocess_refuses_window_before_starting_or_consuming(self):
        client = sl.ShardLoomClient(binary="must-not-run", transport="subprocess",
                                   memory_bytes=64 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        opened = []

        def producer():
            opened.append(True)
            yield [{"n": 1}]

        frame = sl.from_batches(producer, schema={"n": "int64"}, streaming=True, client=client)
        with patch("subprocess.Popen", side_effect=AssertionError("process started")), \
             patch.object(client, "_native_backend_for", side_effect=AssertionError("native loading")):
            with self.assertRaisesRegex(ValueError, "transport='native'"):
                frame.iter_batches(output_window_bytes=WINDOW)
        self.assertEqual(opened, [])

    def test_facades_forward_explicit_configuration(self):
        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        queries = (sl.range(0, 17, client=client), sl.sql("SELECT 1 AS n", client=client))
        with patch.object(client, "public_workflow_batches") as batches:
            for query in queries:
                query.iter_batches(output_window_bytes=WINDOW)
                self.assertEqual(batches.call_args.kwargs["output_window_bytes"], WINDOW)
                query.iter_batches()
                self.assertEqual(batches.call_args.kwargs["output_window_bytes"], 0)

    def test_client_omits_default_key_and_preserves_nonzero(self):
        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        backend = Mock()
        backend.batches.side_effect = lambda *args: Mock()
        with patch.object(client, "_native_backend_for", return_value=backend):
            for value in (0, WINDOW):
                iterator = client.public_workflow_batches("sql", sql_statement="SELECT 1", output_window_bytes=value)
                request = backend.batches.call_args.args[0]
                self.assertEqual(request.get("output_window_bytes", 0), value)
                self.assertEqual("output_window_bytes" in request, value != 0)
                self.assertIs(iterator._client, client)

    def test_direct_backend_validates_and_first_demand_forwards_only_nonzero(self):
        for value in (True, -1, "4194304"):
            with self.assertRaises((TypeError, ValueError)):
                NativeBackend.batches(object(), {"output_window_bytes": value}, None, 2048)
        with patch("shardloom._native_backend.NativeBatchIterator") as construct:
            original = {"output_window_bytes": IntegerOverrides(WINDOW)}
            NativeBackend.batches(object(), original, None, 2048)
            forwarded = construct.call_args.args[1]
            self.assertIs(type(forwarded["output_window_bytes"]), int)
            self.assertEqual(forwarded["output_window_bytes"], WINDOW)
            self.assertIsInstance(original["output_window_bytes"], IntegerOverrides)
        for request in ({}, {"output_window_bytes": 0}, {"output_window_bytes": WINDOW}):
            with self.subTest(request=request):
                marker = RuntimeError("native first demand")
                plan = Mock()
                plan.iter_batches.side_effect = marker
                backend = SimpleNamespace(timeout=None, module=object(), _plan=Mock(return_value=plan))
                iterator = NativeBatchIterator(backend, request, object(), 2048)
                backend._plan.assert_not_called()
                with patch("shardloom._native_backend.native_resources", return_value="resources"):
                    with self.assertRaises(RuntimeError) as caught:
                        next(iterator)
                self.assertIs(caught.exception, marker)
                expected = {"resources": "resources", "timeout": None}
                if request.get("output_window_bytes"):
                    expected["output_window_bytes"] = WINDOW
                plan.iter_batches.assert_called_once_with(**expected)
                iterator.close()


if __name__ == "__main__":
    unittest.main()
