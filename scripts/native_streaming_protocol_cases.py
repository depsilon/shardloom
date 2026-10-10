# SPDX-License-Identifier: Apache-2.0
"""Adversarial real-worker frames; these peers never evaluate query expressions."""

from __future__ import annotations

import json
import os
import selectors
import subprocess
import time

from shardloom.models import OutputEnvelope
from native_streaming_input_cases import declaration


class Peer:
    def __init__(self, harness, name, *, write=False, source=None, memory_input=None):
        self.harness, self.name = harness, name
        source = source if source is not None else harness.context.from_batches(
            [], schema={"n": "int64"}, streaming=True)
        self.uri = source.source.uri
        self.target = harness.output / (name + ".vortex")
        kwargs = declaration(source)
        del kwargs["input_batches"]
        if memory_input is not None:
            kwargs["source_bindings"][self.uri]["memory_input"] = memory_input
        names = [name for name, _ in dict(source.source.memory_input)["schema"]]
        kwargs.update(sql_statement=f"SELECT {','.join(names)} FROM '{self.uri}'", bounded=True,
                      requested_output="write_vortex" if write else "collect")
        if write:
            kwargs["output_ref"] = self.target
        args = harness.client._public_workflow_facade_args("run", "dataframe", **kwargs)
        self.frames = []
        self.stderr = (harness.logs / (name + "-stderr.txt")).open("xb")
        self.process = subprocess.Popen([str(harness.binary), "python-worker", "--batch-stream"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr, bufsize=0)
        os.set_blocking(self.process.stdout.fileno(), False)
        self.buffer = bytearray()
        self.send({"args": args, "stream_results": not write, "batch_rows": 2048})

    def send(self, value):
        self.frames.append({"direction": "in", "value": value})
        self.process.stdin.write(json.dumps(value, separators=(",", ":")).encode() + b"\n")
        self.process.stdin.flush()

    def read(self):
        deadline = time.monotonic() + 10
        while b"\n" not in self.buffer:
            assert len(self.buffer) < 16 << 20
            with selectors.DefaultSelector() as selector:
                selector.register(self.process.stdout, selectors.EVENT_READ)
                assert selector.select(max(0, deadline - time.monotonic())), "native peer response timeout"
            piece = os.read(self.process.stdout.fileno(), 65536)
            assert piece, "worker closed before final response"
            self.buffer.extend(piece)
        line, _, self.buffer = self.buffer.partition(b"\n")
        value = json.loads(line)
        self.frames.append({"direction": "out", "raw": line.decode(), "value": value})
        return value

    def demand(self, index):
        value = self.read()
        assert value == {"kind": "input", "uri": self.uri, "index": index,
                         "max_rows": 2048, "max_bytes": 8 << 20}, value

    def rows(self, index, values):
        self.send({"kind": "rows", "uri": self.uri, "index": index, "rows": values})

    def failed(self):
        envelope = OutputEnvelope.from_json(self.read())
        self.harness.envelope(self.name, envelope, success=False)
        code = self.process.wait(timeout=10)
        assert code != 0
        assert not self.target.exists()
        return {"child_returncode": code, "successful_prefix": False, "child_exited": True}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        # An unexpected assertion still leaves complete frames and drains the
        # owned child. A required forced stop is test failure, never a pass.
        forced = self.process.poll() is None
        if forced:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
        self.process.stdin.close()
        self.process.stdout.close()
        self.stderr.close()
        self.harness.json(self.harness.logs / (self.name + "-frames.json"),
                          {"frames": self.frames, "forced_stop": forced,
                           "returncode": self.process.returncode})
        if forced and _[0] is None:
            raise AssertionError("native peer required a forced stop")


def run(harness):
    for failure in ("wrong-source", "wrong-sequence", "wrong-end", "malformed-native-value", "eof", "cancel", "wrong-ack"):
        name = "protocol-" + failure
        def run_case(failure=failure, name=name):
            with Peer(harness, name) as peer:
                peer.demand(0)
                peer.rows(0, [["1"]])
                batch = peer.read()
                assert batch["kind"] == "batch" and batch["rows"] == [{"n": 1}] and batch["index"] == 0
                if failure == "wrong-ack":
                    peer.send({"kind": "ack", "index": 1})
                else:
                    peer.send({"kind": "ack", "index": 0})
                    peer.demand(1)
                    if failure == "eof":
                        peer.process.stdin.close()
                    elif failure == "cancel":
                        peer.send({"kind": "cancel"})
                    elif failure == "wrong-end":
                        peer.send({"kind": "end", "uri": peer.uri, "index": 0})
                    else:
                        peer.send({"kind": "rows", "uri": "memory://wrong" if failure == "wrong-source" else peer.uri,
                                   "index": 0 if failure == "wrong-sequence" else 1,
                                   "rows": [["invalid-int" if failure == "malformed-native-value" else "2"]]})
                return peer.failed()
        harness.case(name, run_case)

    for failure in ("eof", "cancel", "malformed-native-value"):
        name = "protocol-write-" + failure
        def write_case(failure=failure, name=name):
            before = set(harness.output.iterdir())
            with Peer(harness, name, write=True) as peer:
                peer.demand(0)
                peer.rows(0, [["1"]])
                peer.demand(1)
                assert not peer.target.exists()
                if failure == "eof":
                    peer.process.stdin.close()
                elif failure == "cancel":
                    peer.send({"kind": "cancel"})
                else:
                    peer.rows(1, [["invalid-int"]])
                result = peer.failed()
            assert set(harness.output.iterdir()) == before, "failure retained a staging file"
            return result
        harness.case(name, write_case)
