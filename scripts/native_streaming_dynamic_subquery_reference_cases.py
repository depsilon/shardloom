# SPDX-License-Identifier: Apache-2.0
"""Exact query/expected-value controls retained from the pre-change 32-case baseline."""

from __future__ import annotations

import shardloom as sl
from shardloom._result_schema import ResultType as T
from native_report_evidence import require_native_allocation_peak
from native_streaming_subquery_cases import POLICY

SCHEMA = {'n': 'int64', 'metric': 'float64'}
LEFT = [{'n': n, 'metric': float(n * 10)} for n in range(1, 6)]
RIGHT = LEFT
KINDS = {
    'in': LEFT,
    'not-in': [],
    'exists': LEFT[1:],
    'not-exists': LEFT[:1],
    'any': LEFT[1:],
    'all': LEFT,
    'row-in': LEFT[:1],
    'empty-outer': [],
}


def declare(left, right, label):
    kind, shared = label.rsplit('-shared-', 1)
    if shared == 'True':
        right = left
    predicate = (sl.col('n') < sl.outer('n') if kind in ('exists', 'not-exists')
                 else sl.col('n') <= sl.outer('n'))
    inner = right.filter(predicate).select(
        'n AS entity',
        "CASE WHEN n <= 3 THEN 'a' ELSE 'b' END AS category",
        'metric AS amount',
    ).pivot_table(index='entity', columns='category', values='amount', aggfunc='sum')
    if kind == 'in':
        return left.filter(sl.col('n').isin_source(inner.agg(n='COUNT(*)'), 'n'))
    if kind == 'not-in':
        return left.filter(sl.col('n').not_in_source(inner.agg(n='COUNT(*)'), 'n'))
    if kind == 'exists':
        return left.filter(sl.exists_source(inner))
    if kind == 'not-exists':
        return left.filter(sl.not_exists_source(inner))
    if kind == 'any':
        return left.filter(sl.col('n').any_source('>', inner, 'entity'))
    if kind == 'all':
        return left.filter(sl.col('n').all_source('>=', inner, 'entity'))
    if kind == 'row-in':
        return left.filter(sl.row_in_source(['n', 'metric'],
            inner.agg(n='COUNT(*)', metric='SUM(pivot_a)'), ['n', 'metric']))
    assert kind == 'empty-outer'
    return left.limit(0).filter(sl.exists_source(inner))


def run(harness):
    for streaming in (True, False):
        for shared in (False, True):
            for kind, expected in KINDS.items():
                name = f'dynamic-subquery-baseline-{kind}-shared-{shared}-streaming-{streaming}'

                def check(name=name, kind=kind, shared=shared, streaming=streaming, expected=expected):
                    events = []

                    def frame(label, rows):
                        def produce():
                            events.append([label, 'opened'])
                            try:
                                for batch in ([], rows[:1], rows[1:3], [], rows[3:]):
                                    yield batch
                                events.append([label, 'ended'])
                            finally:
                                events.append([label, 'closed'])
                        return harness.context.from_batches(produce, schema=SCHEMA, streaming=streaming)

                    left, right = frame('left', LEFT), frame('right', RIGHT)
                    query = declare(left, right, f'{kind}-shared-{shared}')
                    assert events == []
                    harness.json(harness.logs / (name + '-declaration.json'),
                                 dict(sql=query._relation_statement(), expected=expected, streaming=streaming))
                    report = query.collect(check=True, **POLICY)
                    harness.values(name, report.result_rows, expected)
                    assert report.result_schema == (("n", T("int64", True)), ("metric", T("float64", True)))
                    labels = ['left'] if shared else ['left', 'right']
                    for label in labels:
                        assert [event for source, event in events if source == label] == ['opened', 'ended', 'closed'], events
                    assert {source for source, _ in events} == set(labels)
                    if streaming:
                        harness.completed(name, report.envelope, sources=len(labels), batches=5 * len(labels), rows=5 * len(labels))
                    else:
                        harness.envelope(name, report.envelope)
                    require_native_allocation_peak(name, report.envelope, POLICY['memory_bytes'])
                    return dict(producer_events=events, outcome='streaming_success' if streaming else 'resident_control_passed')
                harness.case(name, check)
