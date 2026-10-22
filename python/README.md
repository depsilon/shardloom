# ShardLoom Python

ShardLoom Python provides a local, Vortex-native execution engine without external query-engine fallback. Use `sl.context` with explicit memory and parallelism settings to run work.

<a id="local-use"></a>

## Install

<!-- BEGIN SHARDLOOM RELEASE -->
Published **0.7.0**. [Install and platform requirements](https://shardloom.io/field-guide/start-local-proof).

```sh
python -m pip install shardloom==0.7.0
```

- macOS 26 or newer, Apple silicon; CPython 3.10 or newer; native or subprocess.

- Linux x86_64, glibc 2.39 or newer; CPython 3.10 or newer; native or subprocess.

- Windows x86_64; CPython 3.13; subprocess.

The example selects native execution. Use `transport="subprocess"` for the Windows CLI-backed package.
<!-- END SHARDLOOM RELEASE -->

<a id="cli-backed-client-installation-and-configuration"></a>
<a id="public-local-runtime-universal-ingest-into-a-vortex-middle"></a>

## First query

<!-- BEGIN SHARDLOOM QUICKSTART -->
```python
from pathlib import Path
from tempfile import TemporaryDirectory
import shardloom as sl

workspace = TemporaryDirectory(prefix="shardloom-orders-")
orders = Path(workspace.name) / "orders.csv"
orders.write_text("order_id,status\n1,paid\n2,pending\n3,paid\n", encoding="utf-8")

# Illustrative caller choices: 256 MiB and two execution lanes.
ctx = sl.context(transport="native", memory_bytes=256 << 20, max_parallelism=2)

paid = ctx.read_csv(orders).filter(sl.col("status") == "paid").sort("order_id")
result = paid.collect(check=True)
print(result.to_rows())

ctx.close()
workspace.cleanup()
```

Expected values:

```python
({'order_id': 1, 'status': 'paid'}, {'order_id': 3, 'status': 'paid'})
```
<!-- END SHARDLOOM QUICKSTART -->

Example resources are caller choices, not defaults. See the [first workflow](https://shardloom.io/field-guide/start-local-proof) for SQL, write, and reopen examples.

## More tasks

- [Python surface](https://shardloom.io/field-guide/python-surface)
- [Runtime and I/O](https://shardloom.io/field-guide/runtime-and-io)
- [Resources](https://shardloom.io/field-guide/resources)
- [Reference](https://shardloom.io/field-guide/reference)
- [Limitations](https://shardloom.io/field-guide/limitations)
- [Development](https://shardloom.io/field-guide/development)

<a id="source-only-native-python-api"></a>

## Native Python API

The optional native API supports retained owners through plans and results; Arrow conversion is an explicit export boundary. See the [embedding guide](https://shardloom.io/field-guide/embedding) and [native source-build instructions](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/source-checkout-install.md#optional-native-python-binding).

<a id="package-build-smoke"></a>
<a id="local-analytics-benchmark"></a>
<a id="quickstart-proof"></a>
<a id="test"></a>

## Contributor tooling

- [Local source rehearsal](https://github.com/depsilon/shardloom/blob/main/docs/release/local-source-rehearsal.md)
- [Source checkout and install](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/source-checkout-install.md)
- [Benchmarks](https://github.com/depsilon/shardloom/tree/main/docs/benchmarks)
- [Python tests](https://github.com/depsilon/shardloom/tree/main/python/tests)
