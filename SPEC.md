# Python Project Specification: `spark-preprocessor`

## 1. Overview

`spark-preprocessor` is a standalone Python library that generates a tested, reproducible enrichment pipeline for Databricks from a feature/pipeline specification.

It produces three primary artifacts:

1. A **SQLMesh project** (models/macros/tests) that can be executed **from inside Databricks** (job task calling a wheel entrypoint).
2. A **rendered SQL string** for the final enriched dataset (Spark/Databricks dialect) suitable for copy/paste or job execution.
3. A **Databricks-compatible profiling notebook** that profiles:

   * selected raw canonical entities (sampled),
   * the final enriched output table (sampled),
     using ydata-profiling’s **PySpark integration**.

The library has **no Spark dependency**; Spark is used only in the generated Databricks notebook and in Databricks runtime jobs.

---

## 2. Primary workflow

### 2.1 Authoring

* Feature logic is implemented as Python classes/functions distributed via a **shared wheel**.
* Client pipelines are defined in YAML (with optional Python helper DSL), referencing features by registry key.

### 2.2 Compile (idempotent)

`compile(pipeline.yaml, out_dir)`:

* regenerates the SQLMesh project and all derived artifacts deterministically (no in-place manual edits preserved).
* writes a compile report with validations, resolved mappings, and included/skipped features.

### 2.3 Run in Databricks

A Databricks Job uses a wheel entrypoint to:

* load the generated SQLMesh project and pipeline spec,
* run SQLMesh plan/apply to build the enriched output table.

### 2.4 Profile in Databricks

User imports the generated notebook into Databricks and runs it interactively; it writes and displays ydata-profiling reports.

---

## 3. Dependencies and packaging

### 3.1 Base dependencies

* `sqlglot>=28.5.0`
* `ydata-profiling>=4.18.0`
* `sqlmesh` (core dependency; version pinned during implementation)

SQLMesh leverages SQLGlot for SQL semantic understanding and is the transformation/test framework.

### 3.2 Optional extras

* `spark-preprocessor[duckdb]`: `duckdb`, `pyarrow`
* `spark-preprocessor[databricks]`: SQLMesh Databricks engine dependencies as documented
* `spark-preprocessor[spark-notebook]`: only if someone runs profiling notebooks outside Databricks (not required for v1)

### 3.3 Distribution

* Shared feature library and compiler are distributed to Databricks clusters/jobs as a **wheel**.

---

## 4. Domain contract

## 4.1 Canonical entities

* `patients`
* `encounters`
* `medications`
* `procedures`
* `insurance`
* `diagnoses`

## 4.2 Canonical pattern

* Default entity spine: `patients`
* Default join key: `person_id`
* Default join strategy: `LEFT JOIN`

## 4.3 Grain

* Pipeline grain is parameterized but v1 is optimized for `PERSON` / `person_id`.
* Non-person grains are permitted only if:

  * the pipeline specifies a different spine/key, and
  * all selected features declare compatibility.

---

## 5. Mapping and semantic layer

## 5.1 MappingSpec (required)

Clients must provide mappings for entities and optional reference tables.

### YAML schema (mapping)

```yaml
mapping:
  entities:
    patients:
      table: "catalog.schema.patients_raw"
      columns:
        person_id: "member_id"
        date_of_birth: "dob"
    medications:
      table: "catalog.schema.medications_raw"
      columns:
        person_id: "member_id"
        drug_ndc: "drug_NDC"
        order_id: "order_id"
  references:
    drug_crosswalk:
      table: "catalog.schema.drug_xwalk"
      columns:
        ndc: "ndc"
        group: "drug_group"
```

## 5.2 SemanticContract

The library provides a versioned `SemanticContract` describing:

* required keys (`person_id` required for patients; foreign keys elsewhere)
* optional columns (extensible)
* recommended types (validation hints)
* canonical naming rules

## 5.3 Generated semantic models (views)

For every mapped entity/reference table, generate a SQLMesh model (materialized as **view**) that:

* selects from the physical table
* aliases physical columns to canonical names
* exposes only mapped columns + required keys

Location:

* `models/semantic/<entity>.sql`
* `models/semantic/reference__<name>.sql`

These models are the only upstreams referenced by features, ensuring portability.

---

## 6. Feature system

## 6.1 Registry-based discovery

Features are discovered by key via a registry (your choice 1=b).

* `register_feature(feature: Feature)`
* `get_feature(key: str) -> Feature`
* The shared wheel registers all shipped features on import.

YAML references features by `key`.

## 6.2 Feature types (hybrid)

### A) ExpressionFeature

* contributes select expressions to the final model
* may depend on joinable feature models

### B) ModelFeature

* generates one or more SQLMesh models that output:

  * `join_key`(s) + feature columns

## 6.3 Feature interface (public API)

```python
@dataclass(frozen=True)
class FeatureParamSpec:
    name: str
    type: str                 # "int"|"float"|"bool"|"str"|"date"|"enum"|"column_ref"
    required: bool = True
    default: object | None = None
    enum_values: tuple[str, ...] | None = None

@dataclass(frozen=True)
class FeatureRequirement:
    entity: str               # e.g., "patients" or "reference.drug_crosswalk"
    columns: frozenset[str]   # canonical column names required

@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: str | None = None
    description: str | None = None

@dataclass(frozen=True)
class FeatureMetadata:
    key: str
    description: str | None
    params: tuple[FeatureParamSpec, ...]
    requirements: tuple[FeatureRequirement, ...]
    provides: tuple[ColumnSpec, ...]

class Feature(Protocol):
    meta: FeatureMetadata

    def build(self, ctx: "BuildContext", params: dict[str, object]) -> "FeatureAssets":
        ...
```

## 6.4 Parameter type validation (compile-time)

When compiling from YAML:

* validate param presence, types, enums
* validate `column_ref` params refer to canonical columns in the mapped entities (or pipeline spine columns, depending on context)

Type validation is strict; failures are compilation errors.

## 6.5 FeatureAssets

`build()` returns:

```python
@dataclass
class SqlmeshModelSpec:
    name: str                  # SQLMesh model name
    sql: str                   # model SQL
    kind: str                  # "VIEW"|"TABLE"|... (features usually VIEW)
    tags: list[str]

@dataclass
class JoinModelSpec:
    model_name: str
    alias: str
    on: str                    # join predicate in spark SQL
    join_type: str             # "LEFT"

@dataclass
class SqlmeshTestSpec:
    name: str
    yaml: str                  # sqlmesh test definition

@dataclass
class FeatureAssets:
    models: list[SqlmeshModelSpec]
    join_models: list[JoinModelSpec]
    select_expressions: list[str]
    tests: list[SqlmeshTestSpec]
```

---

## 7. Pipeline specification

## 7.1 YAML schema (pipeline)

```yaml
pipeline:
  name: "client_x_enriched"
  slug: "client_x_enriched"
  version: "v1.2.3"                 # git tag
  execution_target: "local"         # local|databricks
  grain: "PERSON"
  spine:
    entity: "patients"
    key: "person_id"
    columns: ["person_id", "date_of_birth"]  # ONLY explicitly requested
  output:
    table: "catalog.schema.enriched_client_x"
    materialization: "table"
  databricks:
    semantic_schema_suffix: "_semantic"
    features_schema_suffix: "_features"
  naming:
    prefixing:
      enabled: false
      scheme: "namespace"           # namespace|feature
      separator: "__"
    collision_policy: "fail"        # fail|auto_prefix
  validation:
    on_missing_required_column: "fail"  # fail|warn_skip

features:
  - key: "patient_age"
    params:
      start: "date_of_birth"
      end: "encounter_date"
  - key: "maintenance_medication_count"
    params:
      min_orders: 4

profiling:
  enabled: true
  sample_rows: 100000
  profile_raw_entities: ["patients", "medications"]
  profile_output: true
  output_dir: "dbfs:/FileStore/profiles/client_x"
```

Notes:

* `spine.columns` controls which spine columns are included.
* Feature columns are added per feature.
* Output table name is explicit; pipeline name/version stored as metadata.

## 7.2 Metadata embedding

On the final output table:

* add comment/properties containing:

  * pipeline name
  * pipeline version tag
  * compile timestamp
  * feature list (keys + versions if tracked)
    Implementation uses Databricks table properties or comment statements as supported.

---

## 8. Compiler behavior (idempotent)

## 8.1 Inputs

* pipeline YAML
* feature registry (from installed wheel)
* semantic contract version (default latest unless specified)

## 8.2 Outputs

Directory layout:

```
<out_dir>/
  sqlmesh.yaml
  models/
    semantic/
      patients.sql
      ...
      reference__drug_crosswalk.sql
    features/
      <feature_key>/
        *.sql
    marts/
      enriched__<pipeline_name>.sql
  tests/
    *.yaml
  notebooks/
    profile__<pipeline_name>.py
  rendered/
    enriched__<pipeline_name>.sql
  manifest/
    compile_report.json
```

## 8.3 Algorithm

1. Load YAML, parse into structured `PipelineSpec`.
2. Resolve entities + references from mapping.
3. Generate semantic models as **views**.
4. Resolve features by registry key.
5. Validate:

   * mapping completeness for required entities/columns
   * feature params types
   * feature requirements against available canonical columns
6. Apply validation policy:

   * `fail`: raise error immediately
   * `warn_skip`: omit feature (and dependents), record in report
7. Generate feature assets (models/tests/select expressions).
8. Generate final enriched model:

   * `FROM semantic.patients p`
   * include only `spine.columns`
   * left join each feature join model
   * select feature-provided columns
9. Configure final model materialization as **table** to explicit output name.
10. Render final SQL (Spark dialect) and write to `rendered/`.
11. Generate profiling notebook.
12. Write `compile_report.json` including:

* included/skipped features
* resolved table identifiers
* output table identifier
* pipeline metadata
* profiling plan

Idempotence rule:

* The compiler rewrites the entire `out_dir` content (or a controlled subset) deterministically. No manual edits preserved.

---

## 9. SQLMesh integration

## 9.1 SQLMesh config

Generate `sqlmesh.yaml` configured for Databricks engine.

## 9.2 Execution entrypoint (Databricks job)

Provide:

* `spark_preprocessor.runtime.apply_pipeline:main`

Responsibilities:

* read pipeline YAML + generated SQLMesh project path
* initialize SQLMesh context
* run plan/apply against the Databricks target environment
* ensure output table is created/updated
* exit with status and structured logs

Primary supported runtime mode is SQLMesh-invoked-from-Databricks.

---

## 10. Profiling notebook generation (Databricks + Spark)

## 10.1 Format

Generate a Databricks notebook source file (`.py`) with `# COMMAND ----------`.

## 10.2 Backend

Use Spark DataFrames and ydata-profiling PySpark integration .

## 10.3 Notebook steps

1. Read config cell (paths, sample size, table list).
2. For each raw entity in `profile_raw_entities`:

   * read the **semantic view** (canonical columns)
   * sample `N` rows
   * generate ydata-profiling report
   * write HTML to output dir
   * display inline
3. Read output table:

   * sample `N`
   * profile + write HTML + display
4. Summarize run metadata (counts, schema, runtime).

Sampling default:

* `LIMIT sample_rows` with optional randomization/hashing if configured later.

---

## 11. Local testing (no Spark)

## 11.1 Test tiers

### A) Compilation tests (required)

* Run on every PR.
* Assert:

  * compiler produces expected directory layout
  * rendered SQL parses under SQLGlot `spark` dialect
  * requested spine columns present; non-requested absent
  * feature columns present
  * collision policy works
  * validation policies work

### B) SQLMesh tests (required where portable)

* Run SQLMesh tests locally with a lightweight engine (recommended: DuckDB).
* Some Spark-specific SQL may not execute on DuckDB; those tests may be marked “databricks-only” and excluded from local suite.

### C) DuckDB smoke suite (recommended)

* Execute a minimal subset end-to-end to validate join + aggregation logic for representative features.

## 11.2 Golden SQL comparisons

Where appropriate:

* Compare normalized ASTs rather than raw SQL strings:

  * `parse_one(sql).sql(dialect="spark", pretty=False)`.

---

## 12. CLI

Implement the following commands:

1. `spark-preprocessor compile --pipeline pipeline.yaml --out <dir>`
2. `spark-preprocessor render-sql --pipeline pipeline.yaml --out <dir>`
3. `spark-preprocessor test --pipeline pipeline.yaml --project <dir>`
4. `spark-preprocessor scaffold --mapping mapping.yaml --out <dir>` (optional in v1)

---

## 13. Conventions and policies

### 13.1 Naming policy

* Feature-provided names by default.
* Optional prefixing configurable; collisions default to fail unless `auto_prefix`.

### 13.2 Missing mappings policy

Configurable:

* `fail` (default)
* `warn_skip` (skips feature + dependents, records in report)

### 13.3 Output table metadata

* Always write explicit output table name.
* Add metadata for pipeline name and version tag (and optionally feature list).

---

## 14. Implementation checklist (deliverables)

1. Core dataclasses and YAML parsing/validation
2. Feature registry and base Feature classes
3. Compiler that generates SQLMesh project + rendered SQL + compile report
4. SQLMesh Databricks runtime entrypoint (`apply_pipeline`)
5. Profiling notebook generator (Spark + ydata-profiling integration)
6. Local test harness:

   * compilation tests
   * DuckDB smoke tests
7. CLI commands

---

## 15. Acceptance criteria

A) Given a pipeline YAML + mapping, `compile` produces:

* a valid SQLMesh project directory
* a rendered SQL file that parses with SQLGlot spark dialect
* a Databricks notebook that runs and produces HTML profiling reports

B) Local `pytest` can run:

* compilation tests without Spark
* DuckDB smoke tests without Spark

C) In Databricks:

* wheel entrypoint can run SQLMesh apply and materialize the output table
* profiling notebook can profile selected raw entities and output table interactively
