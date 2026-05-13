<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local Python Smoke

Run a source-checkout Python smoke without installing runtime dependencies:

```powershell
python examples\local-python-smoke\run.py --repo-root .
```

The script imports the source-tree Python package, resolves the local CLI,
runs status, smoke, and capability checks, and exits nonzero if fallback is
attempted.
