"""Legacy environment-key names; execution resources have no numeric defaults.

Load these deliberately with ``ExecutionResources.from_env()``. Importing this
module performs no environment reads and grants no permission to execute.
"""


SHARDLOOM_MEMORY_GB_ENV = "SHARDLOOM_MEMORY_GB"
SHARDLOOM_MEMORY_BYTES_ENV = "SHARDLOOM_MEMORY_BYTES"
SHARDLOOM_MAX_PARALLELISM_ENV = "SHARDLOOM_MAX_PARALLELISM"
