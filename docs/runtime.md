# Databricks runtime

`spark-preprocessor` is designed to run **inside** Databricks. It does not
connect to Databricks from an external environment.

## Runtime entrypoint

The runtime entrypoint applies a compiled SQLMesh project:

```
spark_preprocessor.runtime.apply_pipeline:main
```

Arguments:

- `--pipeline <path>`: pipeline YAML (same one used at compile time).
- `--project <dir>`: compiled SQLMesh project directory.
- `--environment <name>`: optional SQLMesh environment (default: none).

The runtime:

1. Loads the pipeline document (for logging/context).
   - If `pipeline.execution_target: databricks`, the runtime also ensures the derived
     internal schemas exist (semantic/features) before applying the project.
2. Creates a SQLMesh `Context` from the compiled project.
3. Runs `plan` and `apply` to materialize the output table.

## Typical Databricks flow

1. Build the wheel and upload it to the cluster/job.
2. Compile the pipeline locally and upload the compiled `out_dir` to DBFS.
3. Configure a Databricks job task to run the entrypoint with `--pipeline` and
   `--project` pointing at DBFS paths.

The runtime uses the active Spark session available in the Databricks cluster.
No external connection configuration is required.
