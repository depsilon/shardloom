# SPDX-License-Identifier: Apache-2.0
"""Published-package example. Region markers also supply the guide and READMEs."""

# snippet: setup
from pathlib import Path
from tempfile import TemporaryDirectory
import shardloom as sl

workspace = TemporaryDirectory(prefix="shardloom-orders-")
orders = Path(workspace.name) / "orders.csv"
orders.write_text("order_id,status\n1,paid\n2,pending\n3,paid\n", encoding="utf-8")

# Illustrative caller choices: 256 MiB and two execution lanes.
ctx = sl.context(transport="native", memory_bytes=256 << 20, max_parallelism=2)
# snippet: dataframe
paid = ctx.read_csv(orders).filter(sl.col("status") == "paid").sort("order_id")
result = paid.collect(check=True)
print(result.python_objects)
# snippet: sql
sql_result = ctx.sql(
    f"SELECT order_id, status FROM '{orders}' "
    "WHERE status = 'paid' ORDER BY order_id"
).collect(check=True)
print(sql_result.python_objects)
# snippet: write
output = Path(workspace.name) / "paid.vortex"
written = paid.write_vortex(output)
reopened = ctx.read_vortex(output).sort("order_id").collect(check=True)
print(reopened.python_objects)
# snippet: cleanup
ctx.client.close()
workspace.cleanup()
# snippet: end
