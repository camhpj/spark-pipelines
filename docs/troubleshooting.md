# Troubleshooting

## Configuration errors

- **"Spine entity ... is not mapped"**
  - Ensure `pipeline.spine.entity` exists in `mapping.entities`.

- **"Spine key ... is not mapped"**
  - Ensure the spine key is included in the mapping columns for the spine entity.

- **"Spine columns are missing"**
  - Add the listed columns to the spine entity mapping.

- **"Invalid canonical column names"**
  - Canonical column names must be lower_snake_case.

## Feature errors

- **"Unknown feature key"**
  - Confirm the feature is registered by key and that the shared wheel is installed.

- **"missing columns"**
  - Mapping or feature requirements are incomplete; add the required columns.

- **"missing dependent feature outputs"**
  - A feature references another feature's output that is not included.
  - Add the missing feature to the pipeline or adjust expressions.

- **"Column name collisions"**
  - Enable prefixing or use `collision_policy: auto_prefix`.

## SQL validation errors

- **SQLGlot parse failures**
  - The rendered SQL is not valid Spark SQL. Inspect the feature expressions or
    join clauses and adjust them to the Spark dialect.

## Profiling issues

- **Notebook fails to read tables**
  - Confirm the semantic views and output table exist in the Databricks environment.
  - Ensure the job ran successfully before profiling.
