# dbt-remote-state

dbt-remote state is a fork of the dbt Core plugin [dbt-loom](https://github.com/nicholasyager/dbt-remote-state). Whereas dbt-remote-state focuses on injecting cross-project dependencies, this *experimental* plugin can be used to work
with remote state.

I am grateful for the work of the author of dbt-loom and their broader contributions to the dbt community. The majority of the code for this package is borrowed unmodified from the dbt-loom package.

dbt-remote-state aims to provides the same supports for reading remote state as provided by dbt-loom:

- Local manifest files
- Remote manifest files via http(s)
- `dbt-core` Hosting Providers
  - Datacoves
  - Paradime
- Object Storage
  - GCS
  - S3-compatible object storage services
  - Azure Storage
- Database Warehouse Storage
  - Snowflake stages
  - Databricks Volume, DBFS, and Workspace locations

## Getting Started

This package is a work-in-progress. You can hack on it / install it in a dbt project by cloning the repo
and doing `poetry add -e /path/to/cloned/repo`

## Known Caveats

dbt plugins are only supported in dbt-core version 1.6.0-b8 and newer. This
means you must be using a dbt adapter compatible with this version.
