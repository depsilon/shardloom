# SPDX-License-Identifier: Apache-2.0
"""Replay the saved r17 local I/O evidence without original paths or engine execution."""
import argparse
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'scripts'), str(REPO / 'benchmarks/traditional_analytics')]
from local_io_portable_replay import Collection, verify


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.report and args.report.exists():
        parser.error('report already exists; preserve the previous observation')
    progress = lambda message: print(message, file=sys.stderr, flush=True)
    report = verify(Collection(args.packet, progress), progress)
    serialized = json.dumps(report, indent=2, allow_nan=False) + '\n'
    if args.report:
        with args.report.open('x') as output:
            output.write(serialized)
    print(serialized, end='')


if __name__ == '__main__':
    main()
