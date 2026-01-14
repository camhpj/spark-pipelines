# Databricks Namespacing

Verified on 2026-01-14.

## Overview

When `pipeline.execution_target == "databricks"`, the compiler derives per-pipeline internal schemas
for semantic and feature models from the output table identifier and `pipeline.slug`.

## Inputs

- `pipeline.output.table`: must be 3-part (`catalog.schema.table`).
- `pipeline.slug`: identifier-safe string used to namespace internal schemas.
- `pipeline.databricks.semantic_schema_suffix` (default `_semantic`)
- `pipeline.databricks.features_schema_suffix` (default `_features`)

## Derived schemas

Let `pipeline.output.table = <catalog>.<base_schema>.<output_table>`.

- Semantic schema: `<base_schema>__<slug><semantic_schema_suffix>`
- Features schema: `<base_schema>__<slug><features_schema_suffix>`

## Model names (Databricks mode)

- Semantic entity model: `<catalog>.<semantic_schema>.<entity>`
- Semantic reference model: `<catalog>.<semantic_schema>.reference__<reference>`
- Feature-emitted model: `<catalog>.<features_schema>.<feature_slug>__<purpose>`
  - `feature_slug` is derived from the dotted feature key by replacing `.` with `__` (and validating each segment).
- Final output model: uses `pipeline.output.table` unchanged.

## Runtime behavior

The Databricks runtime entrypoint ensures the derived semantic/features schemas exist before
SQLMesh plan/apply by executing:

- `CREATE SCHEMA IF NOT EXISTS <catalog>.<semantic_schema>`
- `CREATE SCHEMA IF NOT EXISTS <catalog>.<features_schema>`

## Identifier constraints

The naming helpers validate identifier parts against `^[A-Za-z0-9_]+$` and reject empty parts.
This is intended to avoid relying on quoting/escaping behavior in generated SQL.
