# SPDX-License-Identifier: Apache-2.0
"""Replay debug-only state-access work and compiled-code inspection identities."""
import re

from native_workflow_protocol import strict_json
from query_surface.replay import need


def work(log):
    bindings = [strict_json(row.encode()) for row in re.findall(r'grouped_state_binding_work=(\{[^\n]+\})', log)]
    proofs = [strict_json(row.encode()) for row in re.findall(r'grouped_state_count_proof=(\{[^\n]+\})', log)]
    wanted = {(group, policy, unsigned, floating)
              for group in (1, 8, 512, 513, 1024, 1025, 4097)
              for policy in ('Strict', 'Extended')
              for unsigned, floating in ((False, False), (True, False), (False, True))}
    seen = set()
    for row in bindings:
        key = (row['groups'], row['policy'], row['unsigned'], row['floating'])
        need(key in wanted and key not in seen, 'typed state binding work coverage changed')
        seen.add(key)
        group = row['groups']
        count = group * 3
        paged = 2 * int(group > 512) + int(group > 1024)
        need(row['input_rows'] == count and row['paged_binds'] == paged
             and row['contiguous_binds'] == 3 - paged
             and row['count_proof_rows'] == count - (count + 6) // 7,
             'typed state binding or proof work changed')
    need(seen == wanted and len(bindings) == 42, 'typed state binding work incomplete')
    wanted = {(group, nullable, reverse, variant) for group in (1, 512, 513, 1025)
              for nullable in (False, True) for reverse in (False, True) for variant in range(4)}
    seen = set()
    for row in proofs:
        key = (row['groups'], row['nullable'], row['reverse'], row['variant'])
        need(key in wanted and key not in seen, 'typed count proof coverage changed')
        seen.add(key)
        group, nullable, _, variant = key
        count = group * 8
        nulls = (count - 1) // 3 - (group - 1) // 3 if nullable else 0
        need(row['input_rows'] == count and row['admitted_batches'] == int(variant == 0)
             and row['count_proof_rows'] == (count - nulls if variant == 0 else 0),
             'typed paired count proof admission changed')
        overflow = group == 1 and not nullable and variant == 1
        need((isinstance(row['error'], str) and 'aggregate count overflow' in row['error']) if overflow
             else row['error'] is None, 'typed paired count proof changed first errors')
    need(seen == wanted and len(proofs) == 64, 'typed count proof work incomplete')
    for name in (
        'typed_state_bindings_follow_each_payload_width_through_page_growth',
        'typed_pair_uses_last_safe_headroom_and_declines_unequal_or_unproven_counts',
        'typed_state_cancellation_after_allocation_preserves_state_and_refunds',
        'typed_pair_binds_under_full_pool_pressure_without_allocating',
    ):
        need(name + ' ... ok' in log, 'required typed state contract test missing: ' + name)
    return dict(binding_controls=42, count_proof_controls=64,
                debug_only=True, shipping_timing_claim=False)


def assembly(bundle, manifest, review, runtime):
    need(manifest['binary_sha256'] == runtime['products']['_native.abi3.so']
         and review['status'] == 'passed' and review['queries_executed'] is False
         and review['runtime_product_sha256'] == manifest['binary_sha256'],
         'compiled-code inspection runtime changed')
    records = manifest['records'] + review['additional_records']
    need(len(records) == 20 and len({row['file'] for row in records}) == len(records)
         and sum('updates::numeric::<' in row['symbol'] for row in records) == 6
         and sum('pair::accumulate::' in row['symbol'] for row in records) == 12
         and sum('pair::update_pair::' in row['symbol'] for row in records) == 2,
         'compiled-code inspection coverage incomplete')
    for row in records:
        raw = bundle.identity(row['identity'])
        need(row['symbol'].encode() in raw and row['address'] and row['command'][0] == manifest['tool'],
             'compiled-code symbol binding changed')
    need(review['contiguous_page_lookup_removed'] is True
         and review['paged_path_preserved'] is True
         and review['paired_count_overflow_checks_removed_after_admission'] is True
         and review['element_bounds_checks_preserved'] is True
         and review['ordinary_count_overflow_checks_preserved'] is True
         and review['production_timing_claim'] is False,
         'compiled-code inspection did not establish the proposed removed work')
    return dict(status='passed', symbols=len(records), queries_executed=False,
                scope='retained optimized-code inspection; no elapsed speedup attribution')
