# SPDX-License-Identifier: Apache-2.0
"""Reject substituted child receipts, compiler provenance and missing evidence."""
import copy
import json
import lzma

import verify_comparison as verify


def main():
    packet = json.loads(lzma.decompress(verify.PACKET.read_bytes()))
    members = packet['members']
    protocol = json.loads(members['cyclic-existence-scoring-protocol-852.json']['text'])
    summary = json.loads(members['workflows/comparison.json']['text'])
    build = json.loads(members['cyclic-existence-candidate-build-852.json']['text'])
    verify.verify_workflow_receipts(summary, protocol, members)
    verify.verify_artifacts(build)
    rejected = []

    def reject(name, run):
        try:
            run()
        except ValueError:
            rejected.append(name)
        else:
            raise AssertionError('corruption passed: ' + name)

    for name, mutate in (
        ('missing child receipt', lambda value: value['workflow_receipts'].pop()),
        ('failed comparison', lambda value: value.update(status='failed')),
        ('wrong child digest', lambda value: value['workflow_receipts'][0]['result'].update(sha256='0' * 64)),
        ('wrong log digest', lambda value: value['workflow_receipts'][0]['log'].update(sha256='0' * 64)),
        ('nonfinite process diagnostic', lambda value: value['workflow_receipts'][0].update(diagnostic_process_seconds=float('nan'))),
        ('substituted summary workflow', lambda value: value['workflows'].__setitem__(0, value['workflows'][1])),
    ):
        changed = copy.deepcopy(summary)
        mutate(changed)
        reject(name, lambda: verify.verify_workflow_receipts(changed, protocol, members))
    for name, mutate in (
        ('wrong child position', lambda value: value.update(index=1)),
        ('wrong child protocol', lambda value: value['protocol'].update(sha256='0' * 64)),
        ('wrong child library', lambda value: value['library'].update(sha256='0' * 64)),
        ('failed child', lambda value: value.update(status='failed')),
    ):
        changed = dict(members)
        child = json.loads(members['workflows/workflow-000.json']['text'])
        mutate(child)
        data = json.dumps(child).encode()
        changed['workflows/workflow-000.json'] = dict(**verify.identity(data), text=data.decode())
        # Refresh the digest too: provenance checks must reject semantic changes.
        changed_summary = copy.deepcopy(summary)
        changed_summary['workflow_receipts'][0]['result'] = verify.identity(data)
        reject(name, lambda: verify.verify_workflow_receipts(changed_summary, protocol, changed))
    for name, mutate in (
        ('cached native build', lambda value: value['artifacts'][0].update(fresh=True)),
        ('observer build', lambda value: value['artifacts'][0]['profile'].update(test=True)),
        ('unoptimized build', lambda value: value['artifacts'][0]['profile'].update(opt_level='0')),
        ('missing native artifact', lambda value: value['artifacts'].pop()),
        ('changed compiler source', lambda value: value.update(source_unchanged=False)),
    ):
        changed = copy.deepcopy(build)
        mutate(changed)
        reject(name, lambda: verify.verify_artifacts(changed))
    for name, mutate in (
        ('changed memory grant', lambda value: value['initial'].update(memory_limit_bytes=1)),
        ('changed CPU grant', lambda value: value['final'].update(admitted_max_parallelism=2)),
        ('reservation exceeds grant', lambda value: value['final'].update(peak_reserved_bytes=verify.workload.GRANT + 1)),
        ('missing completed execution', lambda value: value['final'].update(completed_executions=0)),
    ):
        changed = copy.deepcopy(summary['workflows'][0])
        mutate(changed)
        reject(name, lambda: verify.verify_reported_resources(changed, protocol['comparison']['schedule'][0]))
    reject('missing source closure', lambda: verify.verify_sources('', []))
    changed = copy.deepcopy(packet)
    changed['members']['workflows/workflow-000.log']['text'] += 'changed'
    reject('changed retained member', lambda: verify.checked_members(changed))
    print(json.dumps(dict(status='passed', corruptions_rejected=rejected), sort_keys=True))


if __name__ == '__main__':
    main()
