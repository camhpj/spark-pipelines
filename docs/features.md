# Feature authoring

Features are Python implementations registered by key. The compiler resolves
features by key and uses their metadata for validation and output assembly.

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
- `requirements`: required canonical columns per entity/reference.
- `provides`: the columns the feature outputs.
- `compatible_grains`: optional tuple of supported grains.

Supported parameter types: `int`, `float`, `bool`, `str`, `date`, `enum`, `column_ref`.

## Build API

`build(ctx, params)` returns `FeatureAssets`:

- `models`: SQLMesh models to create (optional).
- `join_models`: how to join models into the final output.
- `select_expressions`: SQL expressions for output columns.
- `tests`: SQLMesh tests to include in the project.

### BuildContext utilities

- `ctx.resolve_column_ref("column")` resolves to the spine entity by default.
- `ctx.column_ref_sql("column")` returns `p.column` **only for the spine entity**.
  For non-spine entities, create a join model and reference its alias.

## Implementing a new feature

Use this checklist when adding a feature (built-in or external).

1. **Define metadata** with a unique `key`, `params`, `requirements`, `provides`,
   and (if needed) `compatible_grains`.
2. **Implement build** and return `FeatureAssets`:
   - If the feature only uses spine columns, return `select_expressions` only.
   - If it needs other entities, create a SQLMesh model and a join spec.
3. **Validate outputs**:
   - Every `select_expression` must have an `AS <alias>` matching `provides`.
   - If you reference another feature's output, ensure the YAML lists it first.
4. **Register the feature** in the registry so the compiler can resolve it.
5. **Add tests** that cover parameter validation and SQL generation.

### Registration patterns

- **Built-in**: register in `spark_preprocessor.features` on import.
- **External wheel**: register at import time in your package; ensure the wheel
  is installed wherever `spark-preprocessor compile` runs.

### Common pitfalls

- Using `ctx.column_ref_sql()` for non-spine columns (it will raise).
- Forgetting to alias expressions or mismatching `provides`.
- Declaring a non-`PERSON` grain without `compatible_grains`.

## Feature dependencies

Features can reference other feature outputs in their expressions. The compiler
checks for missing dependencies and will fail or skip (depending on validation policy).

## Built-in features

- `age`: computes age in years from two date columns.
- `age_bucket`: bucketizes the `age` output into ranges.
  - Ensure `age` appears before `age_bucket` in the pipeline feature list.

Both built-ins are compatible with `PERSON` grain only.

## Minimal example

```python
from spark_preprocessor.features.base import (
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

    def build(self, ctx, params):
        value = params["value"]
        return FeatureAssets(
            models=[],
            join_models=[],
            select_expressions=[f"{value} AS example_value"],
            tests=[],
        )
```
