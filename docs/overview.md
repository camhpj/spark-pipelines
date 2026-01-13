# Overview

`spark-preprocessor` is a compiler that turns a single pipeline YAML into three artifacts:

1. A SQLMesh project (models/macros/tests) suitable for Databricks execution.
2. A rendered Spark SQL string for the final enriched model.
3. A Databricks notebook (`.py`) that profiles selected tables using ydata-profiling.

The compiler is pure Python. Spark is only required when you run the generated artifacts
inside Databricks.

## End-to-end flow

1. Author a pipeline YAML (mapping + pipeline + features + optional profiling).
2. Run `spark-preprocessor compile` to generate a SQLMesh project and artifacts.
3. Run the SQLMesh project inside Databricks using the runtime entrypoint.
4. (Optional) Import the profiling notebook into Databricks and run it interactively.

## Core concepts

- **Mapping**: Maps canonical entities and columns to physical tables/columns.
- **Semantic views**: SQLMesh views built from mappings (`semantic.<entity>`).
- **Features**: Registry-backed Python implementations that add columns.
- **Spine**: The base entity/key used to join feature outputs.
- **Grain**: Pipeline grain, currently optimized for `PERSON`/`person_id`.
- **Compile report**: JSON summary of included/skipped features and resolved tables.

For details, see the other guides in this folder.
