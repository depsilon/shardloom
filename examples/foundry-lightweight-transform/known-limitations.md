<!-- SPDX-License-Identifier: Apache-2.0 -->

# Known Limitations

- This example does not run in Foundry and does not import Foundry SDKs.
- It does not invoke Foundry output APIs and does not write Foundry result or evidence datasets.
- It does not use Foundry Spark or any managed-platform compute.
- It does not execute a staged dataset through ShardLoom yet; that proof belongs to P9.6.
- It writes a local certificate-style JSON file only.
- It does not write direct S3/object-store outputs or perform object-store commits.
- It does not certify virtual tables, Foundry Spark, Snowflake, Databricks, BigQuery, or external
  compute as ShardLoom-native execution.
- It does not publish a Conda package, Marketplace product, Compute Module, or BYOC image.
- The staged CSV is a fixture for path/boundary documentation, not a production dataset.
