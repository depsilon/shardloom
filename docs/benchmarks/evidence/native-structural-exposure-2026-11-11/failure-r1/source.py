"""Inspect frozen declarations only: no native load, query execution or timing."""
from collections import Counter
from dataclasses import fields, is_dataclass
import hashlib
import json
from pathlib import Path
import sys

OUT = Path(__file__).resolve().parents[1]
E = OUT.parent / 'expanded-query-2026-11-03'
FROZEN = E / 'investigation-original-harness-r1'
W = Path('/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo')
sys.path[:0] = [str(FROZEN / 'python/src'), str(FROZEN / 'benchmarks/traditional_analytics')]
from query_surface.corpus import corpus
from query_surface.expanded_catalog import build, spaces
from query_surface.specification import Expression


def content(path):
    value = path.read_bytes()
    return dict(bytes=len(value), sha256=hashlib.sha256(value).hexdigest())


def walk(node, path='root', parents=()):
    yield node, path, parents
    for i, child in enumerate(node.inputs):
        yield from walk(child, path + '/' + str(i), (node,) + parents)


def expressions(value):
    if isinstance(value, Expression):
        yield value
    if is_dataclass(value):
        for field in fields(value):
            yield from expressions(getattr(value, field.name))
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from expressions(child)


def unary_chain(node):
    while node.op in {'filter', 'project'}:
        yield node
        assert len(node.inputs) == 1
        node = node.inputs[0]


def behind_chain(node):
    for member in unary_chain(node):
        node = member.inputs[0]
    return node


def main():
    design_path = E / 'design.json'
    assert content(design_path)['sha256'] == 'fd0a2604c9abc888a7a24ba75078569fc33e354b16cc98372977c15306a39216'
    design = json.loads(design_path.read_bytes())
    originals = {c.id: c for c in corpus(interactions='all', include_pending=False)}
    definitions = {s.name: s for s in spaces()}
    nodes, exprs, flags = Counter(), Counter(), Counter()
    result = []
    fixed = {'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64',
             'date32', 'date64', 'time32', 'time64', 'timestamp', 'duration'}
    expected_ops = {'limit', 'sort', 'project', 'scan', 'filter', 'aggregate', 'distinct',
                    'join', 'set', 'window_case', 'asof_case', 'explode_case',
                    'membership_case', 'materialized_case', 'recursive_case'}
    for declaration in design['cases']:
        item = build(declaration, definitions) if 'cell' in declaration else originals[declaration['id']]
        assert item.identity == declaration['case_identity'], declaration['id']
        matches = {}
        sorts = []
        for node, path, parents in walk(item.relation):
            nodes[node.op] += 1
            assert node.op in expected_ops, node.op
            exprs.update(e.op for e in expressions(node.args))
            if node.op == 'aggregate':
                chain = list(unary_chain(node.inputs[0]))
                if any(c.op == 'filter' for c in chain):
                    matches.setdefault('filter_to_aggregate_through_projects_filters', []).append(path)
                    computed = any(e.op not in {'column', 'literal'}
                                   for c in chain if c.op == 'project' for e in expressions(c.args))
                    computed |= any(e.op not in {'column', 'literal'} for e in expressions(node.args))
                    if computed:
                        matches.setdefault('filtered_expression_to_aggregate', []).append(path)
            if node.op == 'join':
                for i, child in enumerate(node.inputs):
                    if behind_chain(child).op == 'join':
                        matches.setdefault('join_to_join_through_projects_filters', []).append(path + '/' + str(i))
            if node.op == 'sort':
                parent_limit = bool(parents and parents[0].op == 'limit')
                key_types = [dict(node.inputs[0].schema)[key.column].name for key in node.args]
                one_fixed = len(node.args) == 1 and key_types[0] in fixed
                key = 'sort_directly_under_limit' if parent_limit else 'sort_without_direct_limit_parent'
                matches.setdefault(key, []).append(path)
                if one_fixed and not parent_limit:
                    matches.setdefault('single_fixed_key_sort_without_direct_limit_parent', []).append(path)
                sorts.append(dict(path=path, keys=[dict(column=k.column, descending=k.descending,
                    nulls=k.nulls, dtype=t) for k, t in zip(node.args, key_types)],
                    direct_limit=list(parents[0].args) if parent_limit else None,
                    ancestor_operators=[p.op for p in parents]))
            if node.op in {'window_case', 'asof_case', 'membership_case'}:
                matches.setdefault(node.op, []).append(path)
        flags.update(matches)
        result.append(dict(id=item.id, identity=item.identity, matches=matches, sorts=sorts))
    assert len(result) == 10000 and len({r['id'] for r in result}) == 10000
    assert nodes.keys() == expected_ops
    assert not any(name.endswith('._native') for name in sys.modules)
    sources = {}
    for module in list(sys.modules.values()):
        path = getattr(module, '__file__', None)
        if path and Path(path).is_relative_to(FROZEN) and Path(path).is_file():
            sources[str(Path(path).relative_to(FROZEN))] = content(Path(path))
    owner_paths = ['local_primitive_relational_correlated.rs', 'local_primitive_relational_decorrelate.rs',
        'local_primitive_relational_transform.rs', 'local_primitive_prepared_relational.rs',
        'local_primitives/native_relational_join.rs', 'local_primitives/native_relational_sort.rs',
        'local_primitives/native_relational_order.rs', 'local_primitives/native_relational_observation.rs',
        'local_primitives/native_relational_batch.rs']
    owners = {p: content(W / 'shardloom-vortex/src' / p) for p in owner_paths}
    report = dict(schema_version=1, status='passed', scope='frozen_declaration_exposure_only',
        query_executions=0, source_revision='591fdb42f6ffcd5e9003b4a5c130324d7a34a62c',
        design=content(design_path), attachment=content(OUT / 'latest-structural-request.txt'),
        audit=content(Path(__file__)), imported_declaration_sources=sources, owner_sources=owners,
        request_count=len(result), unique_identities=len({r['identity'] for r in result}),
        relation_nodes=dict(sorted(nodes.items())), expression_nodes=dict(sorted(exprs.items())),
        matching_request_counts=dict(sorted(flags.items())),
        scalar_correlated_count_declarations=0,
        limitations=['No scalar-correlated declaration exists in these enumerated AST operators/expressions; '
                    'the 392 membership declarations are uncorrelated IN/NOT IN.',
                    'Filter chains do not prove sparse survivors or expensive repeated preparations.',
                    'Sort ancestry is syntactic; planner lowering, demand propagation, actual input order, '
                    'spill and full-sort activation remain unmeasured.',
                    'Window and ASOF ordering are separate owners, not general full-sort cases.',
                    'No engine cost is attributed to an entire family.'], cases=result)
    target = OUT / 'structural-exposure-audit-r1.json'
    assert not target.exists()
    target.write_text(json.dumps(report, sort_keys=True, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k not in {'cases', 'imported_declaration_sources',
                       'owner_sources'}}, indent=2))


if __name__ == '__main__':
    main()
