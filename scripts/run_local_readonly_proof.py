#!/usr/bin/env python3
"""Run a bounded read-only proof under the existing local UAT watchdog.

The source and generated logs must be local, unsynced files. Additional tracked
script inputs may live in the checkout: they are read and fingerprinted, never
used as output directories. This runner does not download inputs or remove data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import signal
import sys
from pathlib import Path

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from run_clickbench_paired_query_uat import generation
from run_clickbench_query_uat import run_profiled_command


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uat-root', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--name', required=True, help='unique alphanumeric/hyphen log name')
    parser.add_argument('--input', type=Path, action='append', default=[], help='additional read-only script or executable input')
    parser.add_argument('--timeout-seconds', type=int, default=600)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.name.replace('-', '').isalnum() or len(args.name) > 100:
        parser.error('--name requires 1–100 alphanumeric/hyphen characters')
    if not 1 <= args.timeout_seconds <= 3600:
        parser.error('--timeout-seconds must be 1–3600')
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or not Path(command[0]).is_absolute() or not Path(command[0]).is_file():
        parser.error('command requires an existing absolute executable path')
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    source = require_local_path(args.source, Path.home(), sys.platform)
    if not root.is_dir() or not source.is_file():
        parser.error('--uat-root directory and --source file must already exist')
    logs = require_local_path(root / 'logs' / ('readonly-proof-' + args.name), Path.home(), sys.platform)
    if not logs.is_relative_to(root):
        parser.error('log destination must remain inside --uat-root')
    # Fence the aliases the child actually receives as well as their resolved
    # targets. Otherwise retargeting a symlink can evade a target-only snapshot.
    aliases = [args.source.expanduser().absolute(), Path(command[0]),
               *(p.expanduser().absolute() for p in args.input)]
    inputs = list(dict.fromkeys([source, *aliases,
                                *(p.resolve(strict=True) for p in aliases)]))
    if any(not p.is_file() for p in inputs):
        parser.error('all --input values must be existing regular files')
    def guard():
        return check_budgets(root, source, logs, min_free_bytes=12 * GIB,
            reserve_bytes=0, max_workspace_bytes=100 * GIB, max_log_bytes=256 * MIB)
    guard()
    lock = root / '.ingest-uat.lock'
    lock.mkdir()
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    old_handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        logs.mkdir(parents=True, exist_ok=False)
        before = {str(p): generation(p) for p in inputs}
        with Path(command[0]).open('rb') as stream:
            binary_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
        # Tracked scripts are small; do not rehash the potentially large source.
        input_hashes = {}
        source_aliases = {source, aliases[0]}
        for p in inputs:
            if p in source_aliases:
                continue
            with p.open('rb') as stream:
                input_hashes[str(p)] = hashlib.file_digest(stream, 'sha256').hexdigest()
        result = run_profiled_command(command, logs / 'proof', args.timeout_seconds, guard)
        after = {str(p): generation(p) for p in inputs}
        receipt = {'command': command, 'binary_sha256': binary_hash,
            'additional_input_sha256': input_hashes, 'inputs_before': before,
            'inputs_after': after, 'unchanged': before == after, 'result': result}
        (logs / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        print(json.dumps({'logs': str(logs), **receipt}), flush=True)
        return 0 if before == after and result['returncode'] == 0 and not result['guard_failures'] else 1
    finally:
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        lock.rmdir()


if __name__ == '__main__':
    raise SystemExit(main())
