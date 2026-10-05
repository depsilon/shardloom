# SPDX-License-Identifier: Apache-2.0
"""Sequential local benchmark admission and sampled resource observations."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import time

from local_uat_storage import GIB, MIB, check_budgets, require_local_path


class BenchmarkGuard:
    def __init__(self, root: Path, *, binary: Path, memory_gb: int):
        if os.name != "posix":
            raise ValueError("this supervised local harness requires POSIX process groups")
        self.root = require_local_path(root, Path.home(), sys.platform)
        self.binary_name = binary.name
        self.rss_limit = memory_gb * GIB + 512 * MIB
        self.samples: list[dict] = []
        self.last_sample = 0.0
        self.lock = self.root / ".ingest-uat.lock"
        self.acquired = False

    def __enter__(self):
        self.check()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock.mkdir()
        self.acquired = True
        return self

    def __exit__(self, *_):
        if self.acquired:
            self.lock.rmdir()
            self.acquired = False

    def check(self):
        storage = check_budgets(
            self.root, self.root / "reserved-output", self.root / "logs",
            min_free_bytes=12 * GIB, reserve_bytes=64 * MIB,
            max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB,
        )
        if time.monotonic() - self.last_sample < 2:
            return
        self.last_sample = time.monotonic()
        output = subprocess.check_output(
            ["ps", "-axo", "pid=,ppid=,pgid=,rss=,comm="], text=True, timeout=5)
        rows = []
        for line in output.splitlines():
            fields = line.strip().split(None, 4)
            if len(fields) == 5:
                rows.append((*map(int, fields[:4]), fields[4]))
        owned = {os.getpid()}
        while True:
            children = {pid for pid, parent, _, _, _ in rows if parent in owned}
            if children <= owned:
                break
            owned.update(children)
        conflicts = [{"pid": pid, "executable": name}
                     for pid, _, _, _, name in rows if pid not in owned and
                     (Path(name).name in {"cargo", "rustc", "shardloom", "pytest", "nextest",
                                          self.binary_name, "plain_vortex_fixture"}
                      or Path(name).name.startswith(("shardloom-", "compare-native-artifacts")))]
        if conflicts:
            raise ValueError(f"another native workload is active: {conflicts}")
        rss = sum(rss * 1024 for pid, _, _, rss, _ in rows if pid in owned)
        self.samples.append({"monotonic_seconds": self.last_sample,
                             "host_load": os.getloadavg(), "owned_rss_bytes": rss,
                             "owned_pids": sorted(owned), **storage})
        if rss > self.rss_limit:
            raise ValueError(f"sampled benchmark memory exceeds {self.rss_limit} bytes")
