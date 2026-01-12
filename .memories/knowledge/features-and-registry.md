# Features and Registry

Verified on 2026-01-12.

## Registry
- Global in-memory registry maps feature `key` to feature instances.
- Registration fails if a key is already registered.

## Feature Interface
- `FeatureMetadata` includes `key`, `description`, `params`, `requirements`, `provides`, and optional `compatible_grains`.
- `FeatureAssets` includes `models`, `join_models`, `select_expressions`, and `tests`.
- `Feature.build(ctx, params)` returns `FeatureAssets`.

## Parameter Types
- Supported types: `int`, `float`, `bool`, `str`, `date`, `enum`, `column_ref`.
- `column_ref` validation checks mapped columns across entities and references.
- `BuildContext.column_ref_sql()` only supports spine entity references and raises for non-spine refs.

Previously:
> `column_ref` currently resolves to the pipeline spine entity only; other entities are rejected in `BuildContext.column_ref_sql()`.

## Built-in Features
- `age`: requires params `start` and `end` (column refs), outputs `age` as `INT` using `months_between(end, start) / 12`.
- `age_bucket`: derives from `age` and outputs `age_bucket` with fixed buckets: `0-17`, `18-34`, `35-49`, `50-64`, `65+`.
