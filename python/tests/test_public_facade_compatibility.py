"""Canonical Python contracts survive deferred capability implementations."""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import inspect
import json
import os
from pathlib import Path
import pickle
import subprocess
import sys
import threading
from types import SimpleNamespace
import typing
import unittest
from unittest import mock
import weakref

import shardloom
from shardloom import client, query
from shardloom.models import OutputEnvelope


def canonical_classes():
    for module in (client, query):
        source = Path(inspect.getsourcefile(module))
        declared = [node.name for node in ast.parse(source.read_text()).body
                    if isinstance(node, ast.ClassDef)]
        seen = set()
        for name in declared:
            cls = getattr(module, name)
            if id(cls) not in seen:
                seen.add(id(cls))
                yield module, cls, source


def declaration_digest(source):
    """Portable syntax contract: retain declarations, replace implementation bodies.

    The empty type_params field was added in Python 3.12. Ignore that empty
    spelling difference so the same fixture covers all supported interpreters.
    """
    tree = ast.parse(source)
    for node in tree.body:
        declarations = node.body if isinstance(node, ast.ClassDef) else (node,)
        for declaration in declarations:
            if isinstance(declaration, (ast.FunctionDef, ast.AsyncFunctionDef)):
                first = declaration.body[0]
                doc = [first] if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str) else []
                declaration.body = [*doc, ast.Pass()]

    def normalize(value):
        if isinstance(value, ast.AST):
            return [type(value).__name__, [[key, normalize(item)] for key, item in ast.iter_fields(value)
                                         if not (key == 'type_params' and not item)]]
        if isinstance(value, list):
            return [normalize(item) for item in value]
        if value is Ellipsis:
            return {'constant': 'ellipsis'}
        if isinstance(value, bytes):
            return {'bytes_hex': value.hex()}
        return value

    return hashlib.sha256(json.dumps(normalize(tree), separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


class PublicFacadeCompatibilityTests(unittest.TestCase):
    def test_canonical_declarations_match_the_original_compatibility_contract(self):
        fixture = Path(__file__).with_name('fixtures') / 'public_facade_declarations.json'
        baseline = json.loads(fixture.read_text())
        for module in (client, query):
            with self.subTest(module=module.__name__):
                self.assertEqual(declaration_digest(Path(module.__file__).read_text()),
                                 baseline['modules'][module.__name__])

    def test_classes_and_all_declared_callables_have_canonical_source_and_hints(self):
        classes = list(canonical_classes())
        self.assertEqual(len(classes), 76)
        self.assertEqual(sum(dataclasses.is_dataclass(cls) for _, cls, _ in classes), 73)
        for module, cls, source in classes:
            with self.subTest(cls=cls.__qualname__):
                self.assertEqual(cls.__module__, module.__name__)
                self.assertIs(getattr(module, cls.__name__), cls)
                self.assertEqual(Path(inspect.getsourcefile(cls)), source)
                self.assertIn(f'class {cls.__name__}', inspect.getsource(cls))
                typing.get_type_hints(cls)
                if not issubclass(cls, BaseException):
                    inspect.signature(cls)
                declared_methods = {node.name for node in ast.parse(inspect.getsource(cls)).body[0].body
                                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
                for name, descriptor in cls.__dict__.items():
                    if name not in declared_methods:
                        continue
                    if isinstance(descriptor, property):
                        method = descriptor.fget
                    elif isinstance(descriptor, (classmethod, staticmethod)):
                        method = descriptor.__func__
                    else:
                        method = descriptor
                    if not inspect.isfunction(method):
                        continue
                    with self.subTest(method=name):
                        self.assertEqual(method.__module__, module.__name__)
                        self.assertTrue(method.__globals__ is module.__dict__, name)
                        self.assertEqual(Path(inspect.getsourcefile(method)), source)
                        self.assertIn(f'def {name}(', inspect.getsource(method))
                        inspect.signature(method)
                        typing.get_type_hints(method)
        self.assertIs(query.Result, query.VortexWorkflowExecutionReport)
        self.assertIs(shardloom.Result, query.Result)

    def test_dataclass_identity_and_pickle_do_not_require_optional_runtime_owners(self):
        # Populate inert slots without invoking source/session constructors. The
        # protocol must resolve each actual canonical class, including private
        # dataclasses, independently of optional runtime or result payloads.
        for _, cls, _ in canonical_classes():
            if not dataclasses.is_dataclass(cls):
                continue
            with self.subTest(cls=cls.__qualname__):
                value = object.__new__(cls)
                for field in dataclasses.fields(cls):
                    object.__setattr__(value, field.name, None)
                for protocol in (4, pickle.HIGHEST_PROTOCOL):
                    restored = pickle.loads(pickle.dumps(value, protocol=protocol))
                    self.assertIs(type(restored), cls)
                    self.assertEqual(dataclasses.asdict(restored), dataclasses.asdict(value))

    def test_envelope_wrappers_keep_frozen_slots_replace_and_subclass_fields(self):
        envelope = OutputEnvelope.from_field_mapping({'message': 'one'})
        replacement = OutputEnvelope.from_field_mapping({'message': 'two'})
        wrappers = [cls for module, cls, _ in canonical_classes()
                    if module is client and dataclasses.is_dataclass(cls)
                    and tuple(field.name for field in dataclasses.fields(cls)) == ('envelope',)]
        self.assertEqual(len(wrappers), 28)
        for cls in wrappers:
            with self.subTest(cls=cls.__qualname__):
                value = cls(envelope=envelope)
                self.assertFalse(hasattr(value, '__dict__'))
                self.assertEqual(tuple(inspect.signature(cls).parameters), ('envelope',))
                self.assertEqual(dataclasses.asdict(value), {'envelope': dataclasses.asdict(envelope)})
                self.assertEqual(dataclasses.replace(value, envelope=replacement), cls(replacement))
                self.assertEqual(pickle.loads(pickle.dumps(value)), value)
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    value.envelope = replacement
                child = type('Child', (cls,), {})(envelope)
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    child.envelope = replacement
                self.assertEqual(child.envelope, envelope)

    def test_deferred_implementations_observe_canonical_module_patches(self):
        envelope = OutputEnvelope.from_field_mapping({})
        instance = client.ShardLoomClient(memory_bytes=1 << 20, max_parallelism=1)
        with mock.patch.object(client, '_schema_command_arg', return_value='patched-schema') as schema, \
             mock.patch.object(instance, 'run', return_value=envelope) as run:
            instance.public_workflow_route('sql', sql_statement='SELECT 1', source_schema={'n': 'i64'})
            schema.assert_called_once_with({'n': 'i64'})
            self.assertIn('patched-schema', run.call_args.args[0])
        with mock.patch.object(query, '_require_non_empty', wraps=query._require_non_empty) as require:
            frame = query.LazyFrame.from_sql('SELECT 1 AS n', instance)
            self.assertIsInstance(frame, query.LazyFrame)
            require.assert_any_call('SQL statement', 'SELECT 1 AS n')
        instance.close()

    def test_delegated_native_request_keeps_exact_public_argument_names(self):
        instance = client.ShardLoomClient(transport='native', memory_bytes=1 << 20, max_parallelism=1)
        execute = mock.Mock(return_value=object())
        with mock.patch.object(instance, '_native_backend_for', return_value=SimpleNamespace(execute=execute)):
            instance.public_workflow_run('sql', sql_statement='SELECT 1 AS n')
        request = execute.call_args.args[0]
        expected = set(inspect.signature(client.ShardLoomClient.public_workflow_run).parameters) - {'self'}
        self.assertEqual(set(request), expected)
        self.assertEqual(request['sql_statement'], 'SELECT 1 AS n')
        self.assertNotIn('_impl', request)
        instance.close()

    def test_real_subclasses_keep_classmethod_construction_and_method_overrides(self):
        class Client(client.ShardLoomClient):
            def run(self, args, *, check=True):
                return OutputEnvelope.from_field_mapping({'args': str(args)})

        class Frame(query.LazyFrame):
            pass

        instance = Client.from_env({}, memory_bytes=1 << 20, max_parallelism=1)
        self.assertIs(type(instance), Client)
        frame = Frame.from_sql('SELECT 1 AS n', instance)
        self.assertIs(type(frame), Frame)
        self.assertIs(instance.public_workflow_route('sql', sql_statement=frame.statement).envelope.__class__, OutputEnvelope)
        instance.close()

    def test_subprocess_entry_points_work_with_native_import_forbidden_and_unrelated_code_deferred(self):
        script = r'''
import importlib.abc, json, sys
sys.path.insert(0, sys.argv[1])
class ForbidNative(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'shardloom._native':
            raise AssertionError('subprocess installation requested native extension')
sys.meta_path.insert(0, ForbidNative())
import shardloom as sl
from shardloom.models import OutputEnvelope
payload = OutputEnvelope.from_field_mapping({'scope': 'subprocess-only'}).raw
client = sl.ShardLoomClient(binary=[sys.executable, '-c', 'import json; print(' + repr(json.dumps(payload)) + ')'],
                           transport='subprocess', use_persistent_worker=False,
                           memory_bytes=1 << 20, max_parallelism=1)
frame = sl.sql('SELECT 1 AS n', client=client)
assert frame.statement == 'SELECT 1 AS n'
assert client.run(['doctor']).field_map['scope'] == 'subprocess-only'
assert frame.filter('n > 0').collect().envelope.field_map['scope'] == 'subprocess-only'
for name in ('_client_commands', '_client_report_views', '_query_report_views', '_query_legacy_lowering'):
    assert 'shardloom.' + name not in sys.modules, name
assert 'shardloom._native' not in sys.modules
client.close()
'''
        completed = subprocess.run([sys.executable, '-B', '-I', '-c', script,
                                    str(Path(shardloom.__file__).resolve().parent.parent)],
                                   text=True, capture_output=True, env=dict(os.environ), timeout=30)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_frame_actions_share_description_without_reusing_resource_admission(self):
        from shardloom import _connected, _query_statement
        from shardloom._native_backend import NativeBackend

        instance = client.ShardLoomClient(memory_bytes=1 << 20, max_parallelism=1)
        source = query.WorkflowSource('csv', '/unopened/input.csv', schema=(('n', 'i64'),))
        frame = query.LazyFrame(source, instance).filter('n > 1').select('n')
        plan = SimpleNamespace(source_references=(source.uri,), release_prepared=mock.Mock())
        backend = SimpleNamespace(session=SimpleNamespace(sql=mock.Mock(return_value=plan)),
                                  _plans=weakref.WeakSet(), _plans_lock=threading.Lock())
        with mock.patch.object(_query_statement, 'LazyFrame__relation_statement',
                               wraps=_query_statement.LazyFrame__relation_statement) as render, \
             mock.patch.object(_connected, '_input_arguments', wraps=_connected._input_arguments) as bindings:
            statement = frame.statement
            self.assertIs(_connected.native_declaration(backend, frame), plan)
            self.assertIs(_connected.native_declaration(backend, frame), plan)
            self.assertEqual(frame.statement, statement)
            render.assert_called_once_with(frame)
            bindings.assert_called_once()
            backend.session.sql.assert_called_once()
            self.assertEqual(backend.session.sql.call_args.args, (statement,))
            self.assertEqual(backend.session.sql.call_args.kwargs['files'][0][0], source.uri)
            NativeBackend.release_prepared(backend)
            plan.release_prepared.assert_called_once()
            self.assertIs(_connected.native_declaration(backend, frame), plan)
            # Fresh native preparation/source admission still receives a request;
            # only the immutable Python description survives release_prepared().
            self.assertEqual(backend.session.sql.call_count, 2)
            self.assertEqual(bindings.call_count, 2)
            render.assert_called_once_with(frame)
            child = frame.limit(2)
            self.assertIsNot(child._native_plan, frame._native_plan)
            self.assertNotEqual(child.statement, statement)
            self.assertEqual(render.call_count, 2)
        instance.close()

    def test_retained_bindings_do_not_expose_mutable_description_containers(self):
        instance = client.ShardLoomClient(memory_bytes=1 << 20, max_parallelism=1)
        source = query.WorkflowSource('iceberg', '/unopened/table', table_snapshot=(('snapshot_id', 17),))
        frame = query.LazyFrame(source, instance)
        with mock.patch.object(query, '_workflow_source_bindings', wraps=query._workflow_source_bindings) as bindings:
            before = frame._declared_input_kwargs()
            changed = frame._declared_input_kwargs()
            changed['source_bindings'][source.uri]['table_snapshot']['snapshot_id'] = 99
            changed['input_uri'] = '/changed'
            self.assertEqual(frame._declared_input_kwargs(), before)
            bindings.assert_called_once()
        instance.close()

    def test_description_failure_can_retry_and_subclass_rendering_stays_dynamic(self):
        from shardloom import _query_statement

        class Frame(query.LazyFrame):
            pass

        instance = client.ShardLoomClient(memory_bytes=1 << 20, max_parallelism=1)
        frame = query.LazyFrame(query.WorkflowSource('vortex', '/unopened/input.vortex'), instance)
        with mock.patch.object(_query_statement, 'LazyFrame__relation_statement',
                               side_effect=[ValueError('incomplete'), None]) as render:
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                frame._relation_statement()
            self.assertIsNone(frame._relation_statement())
            self.assertIsNone(frame._relation_statement())
            self.assertEqual(render.call_count, 2)
        subclass = Frame(frame.source, instance)
        with mock.patch.object(_query_statement, 'LazyFrame__relation_statement', side_effect=['first', 'second']):
            self.assertEqual(subclass.statement, 'first')
            self.assertEqual(subclass.statement, 'second')
        instance.close()


if __name__ == '__main__':
    unittest.main()
