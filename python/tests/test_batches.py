"""Boundary contract tests; fake peers never evaluate a ShardLoom query."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

from shardloom import ResultBatch, ResultBatchIterator, ShardLoomClient, from_batches
from shardloom._batches import BatchInput
from shardloom.errors import ShardLoomProtocolError


SCHEMA = {"Struct": [{"names": ["n"], "dtypes": [{"Primitive": ["i64", True]}]}, False]}


@unittest.skipUnless(sys.platform != "win32", "batch pipe transport requires Unix")
class BatchAdapterTests(unittest.TestCase):
    def peer(self, body, *, timeout=3):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "peer.py"
        trace = Path(directory.name) / "trace.jsonl"
        prelude = f"""
import json, sys, time
from pathlib import Path
assert sys.argv[1:] == ['python-worker', '--batch-stream'], sys.argv
trace = Path({str(trace)!r})
def read():
    line = sys.stdin.readline()
    if not line:
        raise SystemExit(0)
    value = json.loads(line)
    with trace.open('a') as output:
        output.write(json.dumps(value) + '\\n')
    return value
def send(value):
    if value.get('schema_version') == 'shardloom.output.v2':
        value.update(result={{'fields':value['fields']}}, result_refs=[], artifacts=[],
                     artifact_refs=[], certificates=[], policy={{}}, lifecycle={{}}, capability_snapshot={{}})
    print(json.dumps(value), flush=True)
def batch(index=0, rows=None, schema=None):
    rows = [{{'n': 7}}] if rows is None else rows
    send({{'kind':'batch','index':index,'row_count':len(rows),'rows':rows,
          'schema':{SCHEMA!r} if schema is None else schema}})
def finish(rows, batches):
    send({{'schema_version':'shardloom.output.v2','command':'run','status':'success',
          'summary':'complete','human_text':'complete',
          'fallback':{{'attempted':False,'allowed':False,'engine':None,'reason':'disabled'}},
          'diagnostics':[], 'fields':[{{'key':key,'value':str(value)}} for key,value in [
              ('native_batch_protocol','shardloom.native_batches.v1'),
              ('result_payload_complete','true'),('output_row_count',rows),
              ('native_result_batches_acknowledged',batches)]]}})
request = read()
"""
        path.write_text(prelude + textwrap.dedent(body), encoding="utf-8")
        client = ShardLoomClient(binary=[sys.executable, str(path)], timeout=timeout)
        self.addCleanup(client.close)
        return client, trace

    def transaction(self, body, **kwargs):
        client, trace = self.peer(body, timeout=kwargs.pop("timeout", 3))
        iterator = ResultBatchIterator(client, ["run", "sql"], **kwargs)
        self.addCleanup(iterator.close)
        return iterator, trace

    def test_result_consumption_is_lazy_and_acknowledges_only_on_next_pull(self):
        iterator, trace = self.transaction("""
            batch()
            assert read() == {'kind':'ack','index':0}
            batch(1, [{'n':None}])
            assert read() == {'kind':'ack','index':1}
            finish(2, 2)
        """)
        self.assertFalse(trace.exists())
        first = next(iterator)
        self.assertIsInstance(first, ResultBatch)
        self.assertEqual(first.python_objects, ({"n": 7},))
        self.assertEqual(first.result_columns, ("n",))
        self.assertIsNone(iterator.report)
        self.assertEqual(len(trace.read_text().splitlines()), 1)
        self.assertEqual(next(iterator).result_rows, ({"n": None},))
        self.assertEqual(len(trace.read_text().splitlines()), 2)
        with self.assertRaises(StopIteration):
            next(iterator)
        self.assertIsNotNone(iterator.report)
        self.assertEqual(iterator._process.poll(), 0)
        self.assertEqual(len(trace.read_text().splitlines()), 3)

    def test_empty_result_keeps_its_declared_schema(self):
        iterator, _ = self.transaction("batch(rows=[])\nread()\nfinish(0, 1)\n")
        batches = list(iterator)
        self.assertEqual(batches[0].result_rows, ())
        self.assertEqual(batches[0].result_columns, ("n",))
        self.assertIsNotNone(iterator.report)

    def test_early_close_cancels_without_acknowledging_or_replaying(self):
        iterator, trace = self.transaction("batch()\nassert read() == {'kind':'cancel'}\n")
        next(iterator)
        iterator.close()
        iterator.close()
        self.assertIsNone(iterator.report)
        self.assertEqual(iterator._process.poll(), 0)
        messages = [json.loads(line) for line in trace.read_text().splitlines()]
        self.assertEqual(messages[-1], {"kind": "cancel"})
        self.assertEqual(len(messages), 2)

    def test_source_declaration_and_factory_are_inert_and_fresh(self):
        opened = []
        def factory():
            opened.append(True)
            return iter([[{"n": 1}]])
        frame = from_batches(factory, schema={"n": "int64"}, binary="unused")
        self.assertEqual(opened, [])
        source = frame.source.batch_input
        self.assertEqual(list(source.open()), [[{"n": 1}]])
        self.assertEqual(list(source.open()), [[{"n": 1}]])
        self.assertEqual(opened, [True, True])
        one_shot = BatchInput([[{"n": 1}]], (("n", "int64"),))
        list(one_shot.open())
        with self.assertRaisesRegex(ValueError, "already been consumed"):
            one_shot.open()

    def test_public_batch_input_is_pulled_on_native_demand(self):
        client, trace = self.peer("""
            args = request['args']
            binding = json.loads(args[args.index('--source-bindings') + 1])
            uri = next(iter(binding))
            assert binding[uri]['memory_input']['kind'] == 'batches'
            for index in range(3):
                send({'kind':'input','uri':uri,'index':index,'max_rows':2048,'max_bytes':8388608})
                response = read()
                assert response['uri'] == uri and response['index'] == index
                if index == 2:
                    assert response['kind'] == 'end'
                else:
                    assert response['kind'] == 'rows'
                    assert response['rows'] == [[str(index)]]
            batch(rows=[{'n':0},{'n':1}])
            read()
            finish(2, 1)
        """)
        consumed = []
        def producer():
            for n in range(2):
                consumed.append(n)
                yield [{"n": n}]
        frame = from_batches(producer(), schema={"n": "int64"}, client=client)
        iterator = frame.iter_batches()
        self.addCleanup(iterator.close)
        self.assertEqual(consumed, [])
        result = list(iterator)
        self.assertEqual(consumed, [0, 1])
        self.assertEqual(result[0].result_rows, ({"n": 0}, {"n": 1}))
        self.assertIsNotNone(iterator.report)
        self.assertEqual(len(trace.read_text().splitlines()), 5)

    def test_input_types_shape_width_and_finite_values_are_checked(self):
        source = BatchInput([], (("n", "int64"), ("f", "float64"), ("b", "bool"), ("s", "utf8")))
        row = {"n": -(1 << 63), "f": 1.25, "b": True, "s": 'λ"\n'}
        self.assertEqual(source.encode([row]), [[str(-(1 << 63)), "1.25", "true", 'λ"\n']])
        self.assertEqual(source.encode([dict.fromkeys(row)]), [[None] * 4])
        for invalid in [dict(row, n=1 << 63), dict(row, f=float("inf")), dict(row, b=1), {"n": 1}]:
            with self.assertRaises((TypeError, ValueError)):
                source.encode([invalid])
        with self.assertRaisesRegex(ValueError, "2,048"):
            source.encode([row] * 2049)
        with self.assertRaises(ValueError):
            from_batches([], schema={f"c{n}": "int64" for n in range(129)})
        from_batches([], schema={f"c{n}": "int64" for n in range(128)})
        for schema in [{}, {"n": "decimal"}, {"": "int64"}, {"λ" * 129: "utf8"}]:
            with self.assertRaises(ValueError):
                from_batches([], schema=schema)

    def test_bad_batch_sequences_shapes_and_final_counts_never_report_success(self):
        for body in [
            "batch(index=True)", "batch(index=1)", "batch(rows=[{'wrong':7}])",
            "batch()\nread()\nfinish(2,1)", "batch()\nread()\nfinish(1,2)",
            "batch()\nread()\nbatch(1,schema={'Struct':[{'names':['n'],'dtypes':[{'Utf8':True}]},False]})",
            "print('not json', flush=True)", "print('{}', flush=True)",
            "print('{\"kind\":\"batch\",\"kind\":\"other\"}', flush=True)",
            "batch(rows=[{'n':float('nan')}])",
            "print('{\"kind\":\"batch\",\"rows\":[{\"n\":1e309}]}', flush=True)",
        ]:
            with self.subTest(body=body):
                iterator, _ = self.transaction(body)
                with self.assertRaises(ShardLoomProtocolError):
                    list(iterator)
                self.assertIsNone(iterator.report)
                self.assertIsNotNone(iterator._process.poll())

    def test_eof_after_a_provisional_batch_is_failure(self):
        iterator, _ = self.transaction("batch()\nread()\n")
        next(iterator)
        with self.assertRaisesRegex(ShardLoomProtocolError, "final report"):
            next(iterator)
        self.assertIsNone(iterator.report)
        self.assertIsNotNone(iterator._process.poll())

    def test_timeout_cancels_one_process_without_query_replay(self):
        iterator, trace = self.transaction("assert read() == {'kind':'cancel'}\n", timeout=0.15)
        with self.assertRaises(subprocess.TimeoutExpired):
            next(iterator)
        self.assertIsNone(iterator.report)
        self.assertIsNotNone(iterator._process.poll())
        self.assertEqual(len(trace.read_text().splitlines()), 2)

    def test_producer_failure_is_preserved_and_native_process_closes(self):
        def failing():
            raise RuntimeError("producer failure")
            yield []
        iterator, _ = self.transaction("""
            send({'kind':'input','uri':'memory://x','index':0,'max_rows':2048,'max_bytes':8388608})
            assert read() == {'kind':'cancel'}
        """, inputs={"memory://x": BatchInput(failing(), (("n", "int64"),))})
        with self.assertRaisesRegex(RuntimeError, "producer failure"):
            next(iterator)
        self.assertIsNotNone(iterator._process.poll())
        self.assertIsNone(iterator.report)

    def test_cleanup_visits_all_owners_and_preserves_active_consumer_error(self):
        closed = []
        class Owner:
            def __init__(self, index):
                self.index = index
            def close(self):
                closed.append(self.index)
                raise RuntimeError("close failure")
        iterator, _ = self.transaction("raise AssertionError('must remain lazy')")
        iterator._iterators = {"a": Owner(1), "b": Owner(2)}
        with self.assertRaisesRegex(ValueError, "consumer failure"):
            with iterator:
                raise ValueError("consumer failure")
        self.assertEqual(closed, [1, 2])
        self.assertTrue(iterator._closed)
        self.assertEqual(iterator._iterators, {})

    def test_factory_stop_iteration_is_a_failure_without_a_final_report(self):
        def factory():
            raise StopIteration
        iterator, _ = self.transaction("""
            send({'kind':'input','uri':'memory://x','index':0,'max_rows':2048,'max_bytes':8388608})
            assert read() == {'kind':'cancel'}
        """, inputs={"memory://x": BatchInput(factory, (("n", "int64"),))})
        with self.assertRaisesRegex(ShardLoomProtocolError, "without its final report"):
            list(iterator)
        self.assertIsNotNone(iterator._process.poll())
        self.assertIsNone(iterator.report)

    def test_declared_inputs_must_end_before_success(self):
        iterator, _ = self.transaction("batch()\nread()\nfinish(1,1)",
            inputs={"memory://x": BatchInput([], (("n", "int64"),))})
        with self.assertRaisesRegex(ShardLoomProtocolError, "complete delivery"):
            list(iterator)
        self.assertIsNone(iterator.report)

    def test_forced_termination_closes_process_and_reports_unconfirmed_cleanup(self):
        iterator, _ = self.transaction("batch()\nread()\ntime.sleep(30)\n")
        next(iterator)
        with self.assertRaisesRegex(ShardLoomProtocolError, "cleanup could not be confirmed"):
            iterator.close()
        self.assertIsNotNone(iterator._process.poll())
        self.assertTrue(iterator._closed)
        self.assertIsNone(iterator.report)


if __name__ == "__main__":
    unittest.main()
