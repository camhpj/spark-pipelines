# Mapping specification

The mapping connects canonical entities/columns to physical tables/columns.
It can be provided as:

- A standalone `mapping.yaml`, or
- The `mapping:` section inside the full pipeline YAML.

## Structure

```yaml
mapping:
  entities:
    patients:
      table: "catalog.schema.patients_raw"
      columns:
        person_id: "member_id"
        date_of_birth: "dob"
  references:
    drug_crosswalk:
      table: "catalog.schema.drug_xwalk"
      columns:
        ndc: "ndc"
        group: "drug_group"
```

### Fields

- `entities` (required): canonical entities mapped to physical tables.
- `references` (optional): reference tables mapped to physical tables.
- `table`: a fully qualified table identifier.
- `columns`: canonical column name -> physical column name.

## Rules and validation

- Canonical column names must be `lower_snake_case`.
- Required canonical columns (from the semantic contract) must be present.
- The spine entity and spine key must exist in the mapping.

## How mappings are used

For each mapping, the compiler generates a SQLMesh semantic view:

- Local mode (`execution_target: local`):
  - `semantic.<entity>` for entities
  - `semantic.reference__<name>` for references
- Databricks mode (`execution_target: databricks`): semantic views are written into a derived
  schema in the pipeline's catalog:
  - `<catalog>.<base_schema>__<slug>_semantic.<entity>`
  - `<catalog>.<base_schema>__<slug>_semantic.reference__<name>`

These semantic views are the only upstreams referenced by features.
