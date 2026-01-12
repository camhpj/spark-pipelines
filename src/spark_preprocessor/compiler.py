"""Pipeline compiler for spark-preprocessor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
from typing import Iterable

import json

from spark_preprocessor import features as feature_registry
from spark_preprocessor.errors import ConfigurationError, ValidationError
from spark_preprocessor.features.base import (
    BuildContext,
    FeatureAssets,
    FeatureMetadata,
    FeatureParamSpec,
    FeatureRequirement,
    JoinModelSpec,
    SqlmeshModelSpec,
)
from spark_preprocessor.schema import PipelineDocument, load_pipeline_document
from spark_preprocessor.semantic_contract import (
    SemanticContract,
    default_semantic_contract,
)
from spark_preprocessor.sqlmesh_project import (
    SqlmeshConfig,
    render_sqlmesh_config,
    render_sqlmesh_model,
)
from spark_preprocessor.profiling import render_profiling_notebook


@dataclass(frozen=True)
class SelectExpression:
    expression: str
    alias: str
    source_feature: str

    def render(self) -> str:
        return f"{self.expression} AS {self.alias}"


@dataclass
class BuiltFeature:
    key: str
    metadata: FeatureMetadata
    assets: FeatureAssets
    select_expressions: list[SelectExpression]


@dataclass(frozen=True)
class CompileReport:
    pipeline_name: str
    pipeline_version: str
    output_table: str
    included_features: list[str]
    skipped_features: dict[str, str]
    resolved_tables: dict[str, str]
    profiling: dict[str, object]
    compiled_at: str


def compile_pipeline(pipeline_path: str | Path, out_dir: str | Path) -> CompileReport:
    """Compile a pipeline YAML into SQLMesh assets and artifacts."""

    pipeline_path = Path(pipeline_path)
    out_dir = Path(out_dir)

    compiled_at = datetime.now(timezone.utc).isoformat()

    document = load_pipeline_document(pipeline_path)
    contract = default_semantic_contract()

    _validate_pipeline(document, contract)

    _wipe_out_dir(out_dir)
    _ensure_layout(out_dir)

    ctx = BuildContext(
        pipeline_name=document.pipeline.name,
        spine_entity=document.pipeline.spine.entity,
        spine_alias="p",
        mapping=document.mapping,
        semantic_contract=contract,
        naming=document.pipeline.naming,
    )

    semantic_models = _build_semantic_models(document, contract)
    built_features, skipped = _build_features(document, ctx)

    final_model_spec, rendered_sql = _build_final_model(
        document, ctx, built_features, compiled_at
    )

    sqlmesh_config = render_sqlmesh_config(SqlmeshConfig())

    profiling_text = None
    if document.profiling and document.profiling.enabled:
        profiling_text = render_profiling_notebook(document)

    report = _build_compile_report(document, built_features, skipped, compiled_at)

    _write_sqlmesh_project(
        out_dir,
        semantic_models,
        built_features,
        final_model_spec,
        document.pipeline.name,
    )
    _write_rendered_sql(out_dir, document.pipeline.name, rendered_sql)
    _write_compile_report(out_dir, report)
    if profiling_text:
        _write_profiling_notebook(out_dir, document.pipeline.name, profiling_text)
    _write_sqlmesh_config(out_dir, sqlmesh_config)

    return report


def _validate_pipeline(document: PipelineDocument, contract: SemanticContract) -> None:
    pipeline = document.pipeline
    mapping = document.mapping

    if not mapping.has_entity(pipeline.spine.entity):
        raise ConfigurationError(
            f"Spine entity '{pipeline.spine.entity}' is not mapped in mapping.entities"
        )

    if pipeline.spine.key not in mapping.entity_columns(pipeline.spine.entity):
        raise ConfigurationError(
            f"Spine key '{pipeline.spine.key}' is not mapped for entity '{pipeline.spine.entity}'"
        )

    for entity_name, required in contract.required_columns.items():
        if not mapping.has_entity(entity_name):
            continue
        missing = required.difference(mapping.entity_columns(entity_name).keys())
        if missing:
            raise ConfigurationError(
                f"Entity '{entity_name}' is missing required columns: {sorted(missing)}"
            )


def _wipe_out_dir(out_dir: Path) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


def _ensure_layout(out_dir: Path) -> None:
    for path in [
        out_dir / "models" / "semantic",
        out_dir / "models" / "features",
        out_dir / "models" / "marts",
        out_dir / "tests",
        out_dir / "notebooks",
        out_dir / "rendered",
        out_dir / "manifest",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def _build_semantic_models(
    document: PipelineDocument, contract: SemanticContract
) -> list[SqlmeshModelSpec]:
    models: list[SqlmeshModelSpec] = []
    mapping = document.mapping

    for entity in sorted(mapping.entities.keys()):
        model_name = f"semantic.{entity}"
        sql = _render_semantic_sql(
            mapping.entity_table(entity), mapping.entity_columns(entity)
        )
        models.append(SqlmeshModelSpec(name=model_name, sql=sql, kind="VIEW", tags=[]))

    for reference in sorted(mapping.references.keys()):
        model_name = f"semantic.reference__{reference}"
        sql = _render_semantic_sql(
            mapping.entity_table(reference), mapping.entity_columns(reference)
        )
        models.append(SqlmeshModelSpec(name=model_name, sql=sql, kind="VIEW", tags=[]))

    return models


def _render_semantic_sql(table: str, columns: dict[str, str]) -> str:
    select_lines = [
        f"  {physical} AS {canonical}"
        for canonical, physical in sorted(columns.items())
    ]
    return "SELECT\n" + ",\n".join(select_lines) + f"\nFROM {table}"


def _build_features(
    document: PipelineDocument, ctx: BuildContext
) -> tuple[list[BuiltFeature], dict[str, str]]:
    features: list[BuiltFeature] = []
    skipped: dict[str, str] = {}
    available_columns: set[str] = set()
    known_outputs: set[str] = set()
    validation_policy = document.pipeline.validation.on_missing_required_column

    for feature_cfg in document.features:
        feature = feature_registry.get_feature(feature_cfg.key)
        known_outputs.update({spec.name for spec in feature.meta.provides})

    for feature_cfg in document.features:
        feature = feature_registry.get_feature(feature_cfg.key)
        metadata = feature.meta

        params = _validate_params(metadata, feature_cfg.params)
        missing_required = _check_requirements(metadata.requirements, ctx)
        missing_column_refs = _check_column_refs(metadata, params, ctx)

        if missing_required or missing_column_refs:
            message = "missing columns"
            if validation_policy == "warn_skip":
                skipped[metadata.key] = message
                continue
            raise ValidationError(f"Feature '{metadata.key}' has {message}")

        assets = feature.build(ctx, params)
        select_expressions = _parse_select_expressions(
            assets.select_expressions, metadata.key
        )

        provided = {spec.name for spec in metadata.provides}
        if (
            select_expressions
            and {expr.alias for expr in select_expressions} != provided
        ):
            raise ValidationError(
                f"Feature '{metadata.key}' select aliases do not match provides"
            )

        dependency_miss = _missing_feature_dependencies(
            select_expressions, available_columns, known_outputs
        )
        if dependency_miss:
            message = "missing dependent feature outputs"
            if validation_policy == "warn_skip":
                skipped[metadata.key] = message
                continue
            raise ValidationError(
                f"Feature '{metadata.key}' has {message}: {dependency_miss}"
            )

        features.append(
            BuiltFeature(
                key=metadata.key,
                metadata=metadata,
                assets=assets,
                select_expressions=select_expressions,
            )
        )
        available_columns.update(provided)

    return features, skipped


def _validate_params(
    metadata: FeatureMetadata, raw_params: dict[str, object]
) -> dict[str, object]:
    params: dict[str, object] = {}
    allowed = {spec.name for spec in metadata.params}
    unexpected = set(raw_params.keys()) - allowed
    if unexpected:
        raise ValidationError(
            f"Feature '{metadata.key}' has unexpected params: {unexpected}"
        )

    for spec in metadata.params:
        value = raw_params.get(spec.name, spec.default)
        if value is None:
            if spec.required:
                raise ValidationError(
                    f"Feature '{metadata.key}' missing required param '{spec.name}'"
                )
            continue
        _check_param_type(metadata.key, spec, value)
        params[spec.name] = value
    return params


def _check_param_type(feature_key: str, spec: FeatureParamSpec, value: object) -> None:
    param_type = spec.type
    if param_type == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValidationError(
                f"Feature '{feature_key}' param '{spec.name}' must be int"
            )
    elif param_type == "float":
        if not isinstance(value, (int, float)):
            raise ValidationError(
                f"Feature '{feature_key}' param '{spec.name}' must be float"
            )
    elif param_type == "bool":
        if not isinstance(value, bool):
            raise ValidationError(
                f"Feature '{feature_key}' param '{spec.name}' must be bool"
            )
    elif param_type in {"str", "date", "column_ref"}:
        if not isinstance(value, str):
            raise ValidationError(
                f"Feature '{feature_key}' param '{spec.name}' must be str"
            )
    elif param_type == "enum":
        if not isinstance(value, str):
            raise ValidationError(
                f"Feature '{feature_key}' param '{spec.name}' must be str"
            )
        if spec.enum_values and value not in spec.enum_values:
            raise ValidationError(
                f"Feature '{feature_key}' param '{spec.name}' must be one of {spec.enum_values}"
            )
    else:
        raise ValidationError(
            f"Feature '{feature_key}' param '{spec.name}' has unknown type"
        )


def _check_requirements(
    requirements: Iterable[FeatureRequirement], ctx: BuildContext
) -> set[str]:
    missing: set[str] = set()
    for req in requirements:
        for column in req.columns:
            if not ctx.mapping.has_column(req.entity, column):
                missing.add(f"{req.entity}.{column}")
    return missing


def _check_column_refs(
    metadata: FeatureMetadata, params: dict[str, object], ctx: BuildContext
) -> set[str]:
    missing: set[str] = set()
    for spec in metadata.params:
        if spec.type != "column_ref":
            continue
        value = params.get(spec.name)
        if value is None:
            continue
        ref = ctx.resolve_column_ref(str(value))
        if ref.entity != ctx.spine_entity:
            missing.add(f"{ref.entity}.{ref.column}")
            continue
        if not ctx.mapping.has_column(ref.entity, ref.column):
            missing.add(f"{ref.entity}.{ref.column}")
    return missing


def _parse_select_expressions(
    expressions: list[str], feature_key: str
) -> list[SelectExpression]:
    parsed: list[SelectExpression] = []
    for expression in expressions:
        match = re.search(
            r"\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", expression, re.IGNORECASE
        )
        if not match:
            raise ValidationError(
                f"Feature '{feature_key}' expression missing alias: {expression}"
            )
        alias = match.group(1)
        expr_body = expression[: match.start()].strip()
        parsed.append(
            SelectExpression(
                expression=expr_body, alias=alias, source_feature=feature_key
            )
        )
    return parsed


def _missing_feature_dependencies(
    expressions: list[SelectExpression],
    available_columns: set[str],
    known_outputs: set[str],
) -> set[str]:
    missing: set[str] = set()
    for expr in expressions:
        referenced = _expression_references(
            expr.expression, known_outputs - {expr.alias}
        )
        missing.update(referenced - available_columns)
    return missing


def _build_final_model(
    document: PipelineDocument,
    ctx: BuildContext,
    features: list[BuiltFeature],
    compiled_at: str,
) -> tuple[SqlmeshModelSpec, str]:
    naming = document.pipeline.naming

    select_expressions = [
        expr for feature in features for expr in feature.select_expressions
    ]

    rename_map = _build_rename_map(select_expressions, naming)
    rename_map = _apply_collision_policy(
        rename_map, select_expressions, naming, document
    )

    updated_expressions = _apply_renames(select_expressions, rename_map)

    base_exprs, derived_exprs = _split_derived_expressions(updated_expressions)

    spine_columns = document.pipeline.spine.columns
    spine_selects = [f"{ctx.spine_alias}.{col} AS {col}" for col in spine_columns]

    join_clauses = _render_join_clauses(features)
    base_selects = spine_selects + [expr.render() for expr in base_exprs]

    base_sql = _render_select_statement(
        base_selects,
        f"semantic.{ctx.spine_entity}",
        ctx.spine_alias,
        join_clauses,
    )

    if derived_exprs:
        outer_selects = ["base.*"] + [expr.render() for expr in derived_exprs]
        final_sql = _render_with_base(base_sql, outer_selects)
    else:
        final_sql = base_sql

    model_name = document.pipeline.output.table
    kind = "TABLE" if document.pipeline.output.materialization == "table" else "VIEW"
    final_sql = _prepend_metadata(document, features, compiled_at, final_sql)
    model_spec = SqlmeshModelSpec(name=model_name, sql=final_sql, kind=kind, tags=[])
    return model_spec, final_sql


def _prepend_metadata(
    document: PipelineDocument, features: list[BuiltFeature], compiled_at: str, sql: str
) -> str:
    feature_keys = ", ".join(feature.key for feature in features)
    metadata_lines = [
        f"-- pipeline_name: {document.pipeline.name}",
        f"-- pipeline_version: {document.pipeline.version}",
        f"-- compiled_at: {compiled_at}",
        f"-- features: {feature_keys}",
    ]
    return "\n".join(metadata_lines) + "\n" + sql


def _build_rename_map(expressions: list[SelectExpression], naming) -> dict[str, str]:
    rename_map: dict[str, str] = {}
    for expr in expressions:
        alias = expr.alias
        if naming.prefixing.enabled:
            prefix = _feature_prefix(expr.source_feature, naming)
            alias = f"{prefix}{naming.prefixing.separator}{alias}"
        rename_map[expr.alias] = alias
    return rename_map


def _apply_collision_policy(
    rename_map: dict[str, str],
    expressions: list[SelectExpression],
    naming,
    document: PipelineDocument,
) -> dict[str, str]:
    final_map = dict(rename_map)
    collisions = _find_collisions(
        final_map, expressions, document.pipeline.spine.columns
    )
    if not collisions:
        return final_map

    if naming.collision_policy == "fail":
        raise ValidationError(f"Column name collisions: {sorted(collisions)}")

    for expr in expressions:
        if final_map[expr.alias] in collisions:
            prefix = _feature_prefix(expr.source_feature, naming)
            final_map[expr.alias] = f"{prefix}{naming.prefixing.separator}{expr.alias}"

    collisions = _find_collisions(
        final_map, expressions, document.pipeline.spine.columns
    )
    if collisions:
        raise ValidationError(
            f"Column name collisions after auto-prefix: {sorted(collisions)}"
        )

    return final_map


def _find_collisions(
    rename_map: dict[str, str],
    expressions: list[SelectExpression],
    spine_columns: list[str],
) -> set[str]:
    seen = set(spine_columns)
    collisions: set[str] = set()
    for expr in expressions:
        alias = rename_map[expr.alias]
        if alias in seen:
            collisions.add(alias)
        seen.add(alias)
    return collisions


def _apply_renames(
    expressions: list[SelectExpression], rename_map: dict[str, str]
) -> list[SelectExpression]:
    updated: list[SelectExpression] = []
    for expr in expressions:
        expr_text = expr.expression
        for old, new in rename_map.items():
            pattern = rf"\b{re.escape(old)}\b"
            expr_text = re.sub(pattern, new, expr_text)
        updated.append(
            SelectExpression(
                expression=expr_text,
                alias=rename_map[expr.alias],
                source_feature=expr.source_feature,
            )
        )
    return updated


def _split_derived_expressions(
    expressions: list[SelectExpression],
) -> tuple[list[SelectExpression], list[SelectExpression]]:
    aliases = {expr.alias for expr in expressions}
    base: list[SelectExpression] = []
    derived: list[SelectExpression] = []
    for expr in expressions:
        references = _expression_references(expr.expression, aliases - {expr.alias})
        if references:
            derived.append(expr)
        else:
            base.append(expr)
    return base, derived


def _expression_references(expression: str, candidates: set[str]) -> set[str]:
    referenced: set[str] = set()
    for candidate in candidates:
        if re.search(rf"\b{re.escape(candidate)}\b", expression):
            referenced.add(candidate)
    return referenced


def _render_join_clauses(features: list[BuiltFeature]) -> list[str]:
    join_models: list[JoinModelSpec] = []
    for feature in features:
        join_models.extend(feature.assets.join_models)
    return [
        f"{join.join_type} JOIN {join.model_name} {join.alias} ON {join.on}"
        for join in join_models
    ]


def _render_select_statement(
    select_expressions: list[str],
    from_model: str,
    from_alias: str,
    join_clauses: list[str],
) -> str:
    select_lines = [f"  {expr}" for expr in select_expressions]
    join_lines = "".join([f"\n{join}" for join in join_clauses])
    return (
        "SELECT\n"
        + ",\n".join(select_lines)
        + f"\nFROM {from_model} {from_alias}"
        + join_lines
    )


def _render_with_base(base_sql: str, outer_selects: list[str]) -> str:
    base_indented = "\n".join(["  " + line for line in base_sql.splitlines()])
    outer_lines = [f"  {expr}" for expr in outer_selects]
    return (
        "WITH base AS (\n"
        + base_indented
        + "\n)\n"
        + "SELECT\n"
        + ",\n".join(outer_lines)
        + "\nFROM base"
    )


def _feature_prefix(feature_key: str, naming) -> str:
    if naming.prefixing.scheme == "feature":
        return feature_key
    if "." in feature_key:
        return feature_key.split(".", 1)[0]
    return feature_key


def _build_compile_report(
    document: PipelineDocument,
    features: list[BuiltFeature],
    skipped: dict[str, str],
    compiled_at: str,
) -> CompileReport:
    resolved_tables = {
        **{
            entity: mapping.table
            for entity, mapping in document.mapping.entities.items()
        },
        **{
            f"reference.{name}": mapping.table
            for name, mapping in document.mapping.references.items()
        },
    }
    profiling_payload: dict[str, object] = {}
    if document.profiling:
        profiling_payload = document.profiling.model_dump()

    return CompileReport(
        pipeline_name=document.pipeline.name,
        pipeline_version=document.pipeline.version,
        output_table=document.pipeline.output.table,
        included_features=[feature.key for feature in features],
        skipped_features=skipped,
        resolved_tables=resolved_tables,
        profiling=profiling_payload,
        compiled_at=compiled_at,
    )


def _write_sqlmesh_project(
    out_dir: Path,
    semantic_models: list[SqlmeshModelSpec],
    features: list[BuiltFeature],
    final_model: SqlmeshModelSpec,
    pipeline_name: str,
) -> None:
    for model in semantic_models:
        path = out_dir / "models" / "semantic" / f"{model.name.split('.', 1)[1]}.sql"
        path.write_text(render_sqlmesh_model(model))

    for feature in features:
        if not feature.assets.models:
            continue
        feature_dir = out_dir / "models" / "features" / feature.key
        feature_dir.mkdir(parents=True, exist_ok=True)
        for model in feature.assets.models:
            filename = f"{model.name.replace('.', '__')}.sql"
            (feature_dir / filename).write_text(render_sqlmesh_model(model))

    final_path = out_dir / "models" / "marts" / f"enriched__{pipeline_name}.sql"
    final_path.write_text(render_sqlmesh_model(final_model))

    for feature in features:
        for test in feature.assets.tests:
            test_path = out_dir / "tests" / f"{test.name}.yaml"
            test_path.write_text(test.yaml)


def _write_rendered_sql(out_dir: Path, pipeline_name: str, sql: str) -> None:
    path = out_dir / "rendered" / f"enriched__{pipeline_name}.sql"
    path.write_text(sql)


def _write_compile_report(out_dir: Path, report: CompileReport) -> None:
    path = out_dir / "manifest" / "compile_report.json"
    path.write_text(json.dumps(report.__dict__, indent=2, sort_keys=True))


def _write_profiling_notebook(out_dir: Path, pipeline_name: str, text: str) -> None:
    path = out_dir / "notebooks" / f"profile__{pipeline_name}.py"
    path.write_text(text)


def _write_sqlmesh_config(out_dir: Path, text: str) -> None:
    path = out_dir / "sqlmesh.yaml"
    path.write_text(text)
