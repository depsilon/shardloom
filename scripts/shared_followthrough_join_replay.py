# SPDX-License-Identifier: Apache-2.0
"""Rebuild independent join-fixture results from saved test measurements."""
import json

from native_workflow_protocol import strict_json
from query_surface.replay import need, sha

SHAPES = (
    ('star_entity_first', False, False, False, False),
    ('star_product_first', True, False, False, False),
    ('active_region_report_prefix', False, True, False, False),
    ('wide_outer_star', False, False, True, True),
)
SIZES = ((0, 17), (17, 1), (257, 17), (8191, 1024))


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def cases():
    return [dict(name=name, products_first=products, report=report, wide=wide,
                 outer=outer, rows=rows, batch_rows=batch)
            for name, products, report, wide, outer in SHAPES for rows, batch in SIZES]


def dimensions(products, active):
    count, duplicate, groups = (61, 11, 5) if products else (127, 13, 8)
    return [(None, 777)] + [(key, key % groups if key % 17 else None)
        for key in reversed(range(count)) if not active or key % 3
        for _ in range(2 if key % duplicate == 0 else 1)]


def reference(case):
    """Literal bounded nested-loop equality and integer reductions, no engine."""
    entities, products = dimensions(False, case['report']), dimensions(True, False)
    groups, stages = {}, [0] * (1 if case['report'] else 2)
    for row in range(case['rows']):
        entity = (row * 17 + 43) % 137 if row % 29 else None
        product = (row * 7 + 23) % 71 if row % 31 else None
        value = (row * 13 + 19) % 1001 - 500 if row % 11 else None
        text = f'μλ:{row:05}:' + 'AbÇ' * 24 if row % 23 else None

        def matches(key, domain):
            selected = [group for other, group in domain if key is not None and key == other]
            return selected if selected or not case['outer'] else [None]

        regions = matches(entity, entities)
        categories = [None] if case['report'] else matches(product, products)
        stages[0] += len(categories if case['products_first'] else regions)
        if not case['report']:
            stages[1] += len(regions) * len(categories)
        for region in regions:
            for category in categories:
                key = dict(r=region)
                if not case['report']:
                    key['c'] = category
                if case['wide']:
                    key['t'] = text
                output = groups.setdefault(encoded(key), dict(**key, count=0, sum=None))
                output['count'] += 1
                if value is not None:
                    output['sum'] = (output['sum'] or 0) + value
    output = list(groups.values())
    for item in output:
        if item['sum'] is not None:
            need(abs(item['sum']) < 1 << 53, 'join reference exceeds exact floating domain')
            item['sum'] = float(item['sum'])
    return sorted(output, key=encoded), stages


def validate_report(value):
    case = value['case']
    need(case in cases(), 'unknown join measurement declaration')
    expected, stage_rows = reference(case)
    need(value['actual'] == value['expected'] == expected
         and value['complete_result_rows'] == len(expected)
         and value['complete_result_sha256'] == sha(encoded(expected).encode()),
         'complete join measurement result differs')
    need([row['rows'] for row in value['stages']] == stage_rows, 'join multiplicity changed')
    fields = ([2] if case['report'] else [4 if case['wide'] else 3] * 2)
    stages = value['stages'] + [value['aggregate_output']]
    fields += [3 if case['report'] else 5 if case['wide'] else 4]
    for stage, width in zip(stages, fields):
        need(set(stage) == {'rows', 'batches', 'cells', 'visible_buffer_bytes', 'largest_output_buffer_bytes'}
             and all(type(number) is int and number >= 0 for number in stage.values()),
             'invalid measured output counters')
        need(stage['cells'] == stage['rows'] * width
             and stage['largest_output_buffer_bytes'] <= stage['visible_buffer_bytes']
             and stage['rows'] <= stage['batches'] * case['batch_rows']
             and (stage['batches'] == 0) == (stage['rows'] == 0), 'inconsistent measured output counters')
    need(value['aggregate_output']['rows'] == len(expected), 'aggregate output coverage changed')
    build_batches = (len(dimensions(False, case['report'])) + 16) // 17
    if not case['report']:
        build_batches += (len(dimensions(True, False)) + 16) // 17
    need(value['producer_batches_released'] == build_batches + (case['rows'] + case['batch_rows'] - 1) // case['batch_rows'],
         'source-release witness coverage changed')
    for name in ('source_release_before_next_batch', 'retained_values_after_session_close', 'final_output_credits_released'):
        need(value[name] is True, 'join lifetime proof missing: ' + name)
    return {key: value[key] for key in ('case', 'stages', 'aggregate_output', 'producer_batches_released',
        'complete_result_rows', 'complete_result_sha256')}


def replay_log(data):
    reports = [strict_json(line[len('JOIN_MOVEMENT '):]) for line in data.decode().splitlines()
               if line.startswith('JOIN_MOVEMENT ')]
    need([row['case'] for row in reports] == cases(), 'join measurement grid incomplete or reordered')
    return [validate_report(row) for row in reports]
