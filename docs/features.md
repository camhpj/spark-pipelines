# Feature authoring

Features are Python implementations registered by key. The compiler resolves features by key and
uses their metadata for validation and output assembly.

## Mental model

The compiler always builds the final enriched model roughly as:

- `FROM semantic.<spine_entity> p`
- `SELECT` the spine columns plus each feature's `select_expressions`
- `JOIN` clauses are concatenated from each feature's `join_models`

Features should treat the `semantic.*` models as their inputs:

- Canonical entities: `semantic.<entity>` (from `mapping.entities`)
- Canonical references: `semantic.reference__<reference>` (from `mapping.references`)

## Registry

Features are registered in the global registry:

```python
from spark_preprocessor.features.registry import register_feature

register_feature(MyFeature())
```

Built-in features are registered on import of `spark_preprocessor.features`.

## Metadata

Each feature defines `meta` describing params, requirements, and provided columns.

Key fields:

- `key`: unique feature identifier (used in YAML).
- `params`: parameter specs (type-checked at compile time).
- `requirements`: required canonical columns per entity/reference key (validated against the mapping).
- `provides`: the columns the feature outputs (must match `AS <alias>` in SQL).
- `compatible_grains`: optional tuple of supported grains.

Supported parameter types: `int`, `float`, `bool`, `str`, `date`, `enum`, `column_ref`.

### Requirements: canonical keys, not physical tables

`FeatureRequirement.entity` must be a canonical mapping key, not a physical table name:

- Good: `patients`, `physicians_65_forward`
- Bad: `catalog.schema.patients_raw`, `geisinger.reference_data.physicians_65_forward`

The compiler validates requirements by checking your `mapping.entities` / `mapping.references`
entries contain the required columns.

### Canonical vs physical column names

Requirements and feature SQL operate on canonical column names, which are validated (currently
`lower_snake_case`). If a source column isn't snake_case (for example `active65F`), you must rename
it in the mapping:

```yaml
mapping:
  references:
    physicians_65_forward:
      table: geisinger.reference_data.physicians_65_forward
      columns:
        pcp_name: pcp_name
        active65f: active65F
```

## Build API

`build(ctx, params)` returns `FeatureAssets`:

- `models`: SQLMesh models to create (optional).
- `join_models`: how to join models into the final output.
- `select_expressions`: SQL expressions for output columns.
- `tests`: SQLMesh tests to include in the generated project.

### BuildContext utilities

- `ctx.resolve_column_ref("column")` resolves to the spine entity by default.
- `ctx.column_ref_sql("column")` returns `p.column` **only for the spine entity**.

That restriction is intentional: it forces non-spine dependencies to be made explicit through
`join_models` (and/or feature-defined intermediate models).

### Joining non-spine data (entities or references)

If your feature needs data that isn't on the spine entity:

1. Ensure the upstream exists in the mapping (`mapping.entities` or `mapping.references`) so the
   compiler can validate it.
2. Create a join via `JoinModelSpec` and reference the join alias in your SQL.
3. Optionally emit a feature-defined model (`SqlmeshModelSpec`) if you need to filter/dedupe before
   joining.

`JoinModelSpec` is rendered into the final SQL as:

```
<join_type> JOIN <model_name> <alias> ON <on>
```

`model_name` should be a SQLMesh model name, typically one of:

- `semantic.<entity>`
- `semantic.reference__<reference>`
- A feature-defined model you emit via `FeatureAssets.models`

#### Example: reference join via a filtered feature model

This pattern filters a reference table down to join keys, then joins that to the spine.

```python
from spark_preprocessor.features.base import (
    BuildContext,
    ColumnSpec,
    FeatureAssets,
    FeatureMetadata,
    FeatureRequirement,
    JoinModelSpec,
    SqlmeshModelSpec,
)


class Forward65FlagFeature:
    meta = FeatureMetadata(
        key="geisinger.forward_65_flag",
        description="Flag if PCP is an active 65 Forward physician.",
        params=(),
        requirements=(
            FeatureRequirement(entity="patients", columns=frozenset({"pcp_name"})),
            FeatureRequirement(
                entity="physicians_65_forward",
                columns=frozenset({"pcp_name", "active65f"}),
            ),
        ),
        provides=(ColumnSpec(name="forward_65_flag", dtype="str"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx: BuildContext, params: dict[str, object]) -> FeatureAssets:
        del params

        model_name = "features.geisinger_forward_65_flag__active_pcp"
        join_alias = "f65"

        sql = (
            "SELECT DISTINCT pcp_name "
            "FROM semantic.reference__physicians_65_forward "
            "WHERE active65f = 1 AND pcp_name IS NOT NULL"
        )

        return FeatureAssets(
            models=[SqlmeshModelSpec(name=model_name, sql=sql, kind="VIEW", tags=[])],
            join_models=[
                JoinModelSpec(
                    model_name=model_name,
                    alias=join_alias,
                    on=f"COALESCE({ctx.column_ref_sql('pcp_name')}, '') = {join_alias}.pcp_name",
                    join_type="LEFT",
                )
            ],
            select_expressions=[
                f"CASE WHEN {join_alias}.pcp_name IS NOT NULL THEN 'Y' ELSE 'N' END AS forward_65_flag"
            ],
            tests=[],
        )
```

## Implementing a new feature

Use this checklist when adding a feature (built-in or external).

1. **Define metadata** with a unique `key`, `params`, `requirements`, `provides`, and (if needed)
   `compatible_grains`.
2. **Implement build** and return `FeatureAssets`:
   - If the feature only uses spine columns, return `select_expressions` only.
   - If it needs other entities/references, create a join spec (and optionally a feature model).
3. **Validate outputs**:
   - Every `select_expression` must have an `AS <alias>` matching `provides`.
   - If you reference another feature's output, ensure the YAML lists it first.
4. **Register the feature** in the registry so the compiler can resolve it.
5. **Add tests** that cover parameter validation and SQL generation.

### Registration patterns

- **Built-in**: register in `spark_preprocessor.features` on import.
- **External wheel**: register at import time in your package.

Note: the `spark-preprocessor` CLI does not currently auto-discover plugins, so the process must
import the external package before compilation (for example via a wrapper entrypoint).

### Common pitfalls

- Using `ctx.column_ref_sql()` for non-spine columns (it will raise).
- Declaring requirements against physical table names (requirements are validated against mapping keys).
- Forgetting to alias expressions or mismatching `provides`.
- Declaring a non-`PERSON` grain without `compatible_grains`.

## Feature dependencies

Features can reference other feature outputs in their expressions. The compiler checks for missing
dependencies and will fail or skip (depending on validation policy).

## Built-in features

- `age`: computes age in years from two date columns.
- `age_bucket`: bucketizes the `age` output into ranges.
  - Ensure `age` appears before `age_bucket` in the pipeline feature list.

Both built-ins are compatible with `PERSON` grain only.

## Minimal example

```python
from spark_preprocessor.features.base import (
    BuildContext,
    ColumnSpec,
    FeatureAssets,
    FeatureMetadata,
    FeatureParamSpec,
)


class ExampleFeature:
    meta = FeatureMetadata(
        key="example",
        description="Example feature",
        params=(FeatureParamSpec(name="value", type="int"),),
        requirements=(),
        provides=(ColumnSpec(name="example_value", dtype="int"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx: BuildContext, params: dict[str, object]) -> FeatureAssets:
        value = params["value"]
        return FeatureAssets(
            models=[],
            join_models=[],
            select_expressions=[f"{value} AS example_value"],
            tests=[],
        )
```
