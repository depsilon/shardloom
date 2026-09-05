#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Record native process duration independently of the outer watchdog interval."""
import argparse
import json
import resource
import signal
import subprocess
import sys
import time
from pathlib import Path

from run_clickbench_query_uat import stop_process


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timing", type=Path, required=True)
    parser.add_argument("--pid-file", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a native command is required")

    def interrupted(_signal, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    started = time.perf_counter()
    process = subprocess.Popen(command, start_new_session=True)
    code = 130
    try:
        args.pid_file.write_text(str(process.pid))
        code = process.wait()
    except KeyboardInterrupt:
        code = 130
    finally:
        # Avoid a second signal interrupting the required child cleanup.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        stop_process(process)
        elapsed = time.perf_counter() - started
        # This supervisor launches only the native child. macOS reports bytes;
        # Linux reports KiB. This is observation, not an enforced RSS limit.
        peak_rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        peak_rss_bytes = int(peak_rss * (1 if sys.platform == "darwin" else 1024))
        args.timing.write_text(json.dumps({"seconds": elapsed, "returncode": code,
            "peak_rss_bytes": peak_rss_bytes,
            "peak_rss_scope": "native_child_os_high_water_mark_not_a_reservation_limit",
            "timing_boundary": "native process creation through complete output and process exit"}) + "\n")
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    raise SystemExit(main())
