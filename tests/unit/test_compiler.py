from contextlib import nullcontext as does_not_raise

import pytest

from spark_preprocessor.compiler import (
    BuiltFeature,
    SelectExpression,
    _build_features,
    _build_semantic_models,
    _check_param_type,
    _ensure_layout,
    _render_semantic_sql,
    _validate_params,
    _validate_pipeline,
    _write_sqlmesh_project,
    _wipe_out_dir,
    _apply_reference_renames,
    _expression_references,
    _parse_select_expressions,
    _resolve_select_expressions,
    _split_derived_expressions,
)
from spark_preprocessor.errors import ConfigurationError, ValidationError
from spark_preprocessor.features.base import (
    BuildContext,
    ColumnSpec,
    FeatureAssets,
    FeatureMetadata,
    FeatureParamSpec,
    FeatureRequirement,
    JoinModelSpec,
    SqlmeshModelSpec,
    SqlmeshTestSpec,
)
from spark_preprocessor.features.registry import register_feature
from spark_preprocessor.schema import (
    MappingSpec,
    NamingConfig,
    PipelineDocument,
    PrefixingConfig,
)
from spark_preprocessor.semantic_contract import default_semantic_contract


def test_parse_select_expressions_requires_alias() -> None:
    with pytest.raises(ValidationError):
        _parse_select_expressions(["1"], "f")


def test_parse_select_expressions_parses_alias() -> None:
    parsed = _parse_select_expressions(["1 AS foo"], "f")
    assert parsed == [SelectExpression(expression="1", alias="foo", source_feature="f")]


def test_expression_references_uses_word_boundaries() -> None:
    refs = _expression_references("foo + foobar + bar", {"foo", "bar"})
    assert refs == {"foo", "bar"}


def test_split_derived_expressions_splits_on_references() -> None:
    exprs = [
        SelectExpression(expression="1", alias="a", source_feature="f"),
        SelectExpression(expression="a + 1", alias="b", source_feature="f"),
    ]
    base, derived = _split_derived_expressions(exprs)
    assert [e.alias for e in base] == ["a"]
    assert [e.alias for e in derived] == ["b"]


def test_apply_reference_renames_replaces_whole_tokens() -> None:
    exprs = [
        SelectExpression(expression="a + aa", alias="out", source_feature="f"),
    ]
    updated = _apply_reference_renames(exprs, {"a": "x"})
    assert updated[0].expression == "x + aa"


@pytest.mark.parametrize(
    "naming,expectation",
    [
        (NamingConfig(prefixing=PrefixingConfig(enabled=False), collision_policy="fail"), does_not_raise()),
        (NamingConfig(prefixing=PrefixingConfig(enabled=True), collision_policy="fail"), does_not_raise()),
    ],
)
def test_resolve_select_expressions_basic(naming: NamingConfig, expectation) -> None:
    expressions = [
        SelectExpression(expression="1", alias="a", source_feature="feat.a"),
        SelectExpression(expression="2", alias="b", source_feature="feat.b"),
    ]
    with expectation:
        resolved, rename_map = _resolve_select_expressions(expressions, naming, spine_columns=["id"])
    assert len(resolved) == 2
    assert rename_map


def test_resolve_select_expressions_fails_on_collision_with_spine_column() -> None:
    naming = NamingConfig(prefixing=PrefixingConfig(enabled=False), collision_policy="fail")
    expressions = [SelectExpression(expression="1", alias="person_id", source_feature="f")]
    with pytest.raises(ValidationError):
        _resolve_select_expressions(expressions, naming, spine_columns=["person_id"])


def test_resolve_select_expressions_auto_prefix_resolves_collision() -> None:
    naming = NamingConfig(prefixing=PrefixingConfig(enabled=False), collision_policy="auto_prefix")
    expressions = [SelectExpression(expression="1", alias="person_id", source_feature="f.x")]
    resolved, _ = _resolve_select_expressions(expressions, naming, spine_columns=["person_id"])
    assert resolved[0].alias != "person_id"


def test_resolve_select_expressions_rejects_ambiguous_reference() -> None:
    naming = NamingConfig(prefixing=PrefixingConfig(enabled=False), collision_policy="fail")
    expressions = [
        SelectExpression(expression="1", alias="dup", source_feature="a"),
        SelectExpression(expression="2", alias="dup", source_feature="b"),
        SelectExpression(expression="dup + 1", alias="x", source_feature="c"),
    ]
    with pytest.raises(ValidationError):
        _resolve_select_expressions(expressions, naming, spine_columns=[])


def test_validate_pipeline_rejects_unknown_spine_entity() -> None:
    document = PipelineDocument.model_validate(
        {
            "mapping": {"entities": {}},
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
        }
    )
    with pytest.raises(ConfigurationError, match="Spine entity"):
        _validate_pipeline(document, default_semantic_contract())


def test_validate_pipeline_rejects_unmapped_spine_key() -> None:
    document = PipelineDocument.model_validate(
        {
            "mapping": {
                "entities": {"patients": {"table": "t", "columns": {"not_person_id": "x"}}}
            },
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
        }
    )
    with pytest.raises(ConfigurationError, match="Spine key"):
        _validate_pipeline(document, default_semantic_contract())


def test_validate_pipeline_rejects_missing_spine_columns() -> None:
    document = PipelineDocument.model_validate(
        {
            "mapping": {"entities": {"patients": {"table": "t", "columns": {"person_id": "x"}}}},
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {
                    "entity": "patients",
                    "key": "person_id",
                    "columns": ["person_id", "missing_col"],
                },
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
        }
    )
    with pytest.raises(ConfigurationError, match="Spine columns are missing"):
        _validate_pipeline(document, default_semantic_contract())


def test_validate_pipeline_rejects_non_person_default_spine() -> None:
    document = PipelineDocument.model_validate(
        {
            "mapping": {"entities": {"patients": {"table": "t", "columns": {"person_id": "x"}}}},
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "ENCOUNTER",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
        }
    )
    with pytest.raises(ConfigurationError, match="Non-PERSON grain"):
        _validate_pipeline(document, default_semantic_contract())


def test_validate_pipeline_rejects_invalid_canonical_names() -> None:
    document = PipelineDocument.model_validate(
        {
            "mapping": {
                "entities": {
                    "patients": {"table": "t", "columns": {"person_id": "x", "BadName": "y"}}
                }
            },
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
        }
    )
    with pytest.raises(ConfigurationError, match="Invalid canonical column names"):
        _validate_pipeline(document, default_semantic_contract())


def test_validate_pipeline_rejects_invalid_canonical_names_in_references() -> None:
    document = PipelineDocument.model_validate(
        {
            "mapping": {
                "entities": {"patients": {"table": "t", "columns": {"person_id": "x"}}},
                "references": {"icd10": {"table": "t2", "columns": {"BadName": "code"}}},
            },
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
        }
    )
    with pytest.raises(ConfigurationError, match="Invalid canonical column names"):
        _validate_pipeline(document, default_semantic_contract())


def test_validate_pipeline_rejects_missing_required_columns_for_mapped_entity() -> None:
    contract = default_semantic_contract()
    document = PipelineDocument.model_validate(
        {
            "mapping": {"entities": {"patients": {"table": "t", "columns": {"x": "x"}}}},
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "x", "columns": ["x"]},
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
        }
    )
    with pytest.raises(ConfigurationError, match="missing required columns"):
        _validate_pipeline(document, contract)


@pytest.mark.parametrize(
    "spec,value,expectation",
    [
        (FeatureParamSpec(name="p", type="int"), True, pytest.raises(ValidationError)),
        (FeatureParamSpec(name="p", type="int"), 1, does_not_raise()),
        (FeatureParamSpec(name="p", type="float"), 1, does_not_raise()),
        (FeatureParamSpec(name="p", type="float"), 1.2, does_not_raise()),
        (FeatureParamSpec(name="p", type="bool"), False, does_not_raise()),
        (FeatureParamSpec(name="p", type="bool"), "no", pytest.raises(ValidationError)),
        (FeatureParamSpec(name="p", type="str"), "x", does_not_raise()),
        (FeatureParamSpec(name="p", type="str"), 1, pytest.raises(ValidationError)),
        (FeatureParamSpec(name="p", type="date"), "2020-01-01", does_not_raise()),
        (FeatureParamSpec(name="p", type="column_ref"), "person_id", does_not_raise()),
        (
            FeatureParamSpec(name="p", type="enum", enum_values=("a", "b")),
            "a",
            does_not_raise(),
        ),
        (
            FeatureParamSpec(name="p", type="enum", enum_values=("a", "b")),
            "c",
            pytest.raises(ValidationError),
        ),
    ],
)
def test_check_param_type_validation(spec: FeatureParamSpec, value: object, expectation) -> None:
    with expectation:
        _check_param_type("f", spec, value)


def test_validate_params_rejects_unexpected_params() -> None:
    metadata = FeatureMetadata(
        key="unit.params",
        description=None,
        params=(FeatureParamSpec(name="a", type="int"),),
        requirements=(),
        provides=(ColumnSpec(name="x"),),
    )
    with pytest.raises(ValidationError, match="unexpected params"):
        _validate_params(metadata, {"nope": 1})


def test_validate_params_rejects_missing_required_param() -> None:
    metadata = FeatureMetadata(
        key="unit.params",
        description=None,
        params=(FeatureParamSpec(name="a", type="int", required=True),),
        requirements=(),
        provides=(ColumnSpec(name="x"),),
    )
    with pytest.raises(ValidationError, match="missing required param"):
        _validate_params(metadata, {})


def test_validate_params_skips_missing_optional_param() -> None:
    metadata = FeatureMetadata(
        key="unit.params",
        description=None,
        params=(FeatureParamSpec(name="a", type="int", required=False, default=None),),
        requirements=(),
        provides=(ColumnSpec(name="x"),),
    )
    assert _validate_params(metadata, {}) == {}


def test_build_features_warn_skip_vs_fail_for_incompatible_grain() -> None:
    class _GrainFeature:
        meta = FeatureMetadata(
            key="unit.grain",
            description=None,
            params=(),
            requirements=(),
            provides=(ColumnSpec(name="x"),),
            compatible_grains=("PERSON",),
        )

        def build(self, ctx, params):  # noqa: D401 - testing helper
            return FeatureAssets(models=[], join_models=[], select_expressions=["1 AS x"], tests=[])

    register_feature(_GrainFeature())
    mapping = MappingSpec.model_validate(
        {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}
    )
    ctx = BuildContext(
        pipeline_name="p",
        spine_entity="patients",
        spine_alias="p",
        mapping=mapping,
        semantic_contract=default_semantic_contract(),
        naming=NamingConfig(),
    )

    warn_doc = PipelineDocument.model_validate(
        {
            "mapping": mapping.model_dump(),
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "ENCOUNTER",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
                "validation": {"on_missing_required_column": "warn_skip"},
            },
            "features": [{"key": "unit.grain"}],
        }
    )
    built, skipped = _build_features(warn_doc, ctx)
    assert built == []
    assert skipped["unit.grain"] == "incompatible with pipeline grain"

    fail_doc = PipelineDocument.model_validate(
        {
            "mapping": mapping.model_dump(),
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "ENCOUNTER",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
                "validation": {"on_missing_required_column": "fail"},
            },
            "features": [{"key": "unit.grain"}],
        }
    )
    with pytest.raises(ValidationError, match="incompatible with pipeline grain"):
        _build_features(fail_doc, ctx)


def test_build_features_warn_skip_on_missing_columns() -> None:
    class _ReqFeature:
        meta = FeatureMetadata(
            key="unit.req",
            description=None,
            params=(FeatureParamSpec(name="col", type="column_ref"),),
            requirements=(FeatureRequirement(entity="patients", columns=frozenset({"missing"})),),
            provides=(ColumnSpec(name="x"),),
            compatible_grains=("PERSON",),
        )

        def build(self, ctx, params):  # noqa: D401 - testing helper
            return FeatureAssets(models=[], join_models=[], select_expressions=["1 AS x"], tests=[])

    register_feature(_ReqFeature())
    mapping = MappingSpec.model_validate(
        {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}
    )
    ctx = BuildContext(
        pipeline_name="p",
        spine_entity="patients",
        spine_alias="p",
        mapping=mapping,
        semantic_contract=default_semantic_contract(),
        naming=NamingConfig(),
    )
    doc = PipelineDocument.model_validate(
        {
            "mapping": mapping.model_dump(),
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
                "validation": {"on_missing_required_column": "warn_skip"},
            },
            "features": [{"key": "unit.req", "params": {"col": "missing_col"}}],
        }
    )
    built, skipped = _build_features(doc, ctx)
    assert built == []
    assert skipped["unit.req"] == "missing columns"


def test_build_features_fails_on_missing_columns_with_fail_policy() -> None:
    class _ReqFeature:
        meta = FeatureMetadata(
            key="unit.req_fail",
            description=None,
            params=(FeatureParamSpec(name="col", type="column_ref"),),
            requirements=(FeatureRequirement(entity="patients", columns=frozenset({"missing"})),),
            provides=(ColumnSpec(name="x"),),
            compatible_grains=("PERSON",),
        )

        def build(self, ctx, params):  # noqa: D401 - testing helper
            return FeatureAssets(models=[], join_models=[], select_expressions=["1 AS x"], tests=[])

    register_feature(_ReqFeature())
    mapping = MappingSpec.model_validate(
        {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}
    )
    ctx = BuildContext(
        pipeline_name="p",
        spine_entity="patients",
        spine_alias="p",
        mapping=mapping,
        semantic_contract=default_semantic_contract(),
        naming=NamingConfig(),
    )
    doc = PipelineDocument.model_validate(
        {
            "mapping": mapping.model_dump(),
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
                "validation": {"on_missing_required_column": "fail"},
            },
            "features": [{"key": "unit.req_fail", "params": {"col": "missing_col"}}],
        }
    )
    with pytest.raises(ValidationError, match="missing columns"):
        _build_features(doc, ctx)


def test_build_features_warn_skip_on_missing_dependency_due_to_order() -> None:
    class _ProvidesX:
        meta = FeatureMetadata(
            key="unit.provides_x",
            description=None,
            params=(),
            requirements=(),
            provides=(ColumnSpec(name="x"),),
            compatible_grains=("PERSON",),
        )

        def build(self, ctx, params):  # noqa: D401 - testing helper
            return FeatureAssets(models=[], join_models=[], select_expressions=["1 AS x"], tests=[])

    class _NeedsX:
        meta = FeatureMetadata(
            key="unit.needs_x",
            description=None,
            params=(),
            requirements=(),
            provides=(ColumnSpec(name="y"),),
            compatible_grains=("PERSON",),
        )

        def build(self, ctx, params):  # noqa: D401 - testing helper
            return FeatureAssets(models=[], join_models=[], select_expressions=["x + 1 AS y"], tests=[])

    register_feature(_ProvidesX())
    register_feature(_NeedsX())
    mapping = MappingSpec.model_validate(
        {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}
    )
    ctx = BuildContext(
        pipeline_name="p",
        spine_entity="patients",
        spine_alias="p",
        mapping=mapping,
        semantic_contract=default_semantic_contract(),
        naming=NamingConfig(),
    )
    doc = PipelineDocument.model_validate(
        {
            "mapping": mapping.model_dump(),
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
                "validation": {"on_missing_required_column": "warn_skip"},
            },
            "features": [{"key": "unit.needs_x"}, {"key": "unit.provides_x"}],
        }
    )
    built, skipped = _build_features(doc, ctx)
    assert skipped["unit.needs_x"] == "missing dependent feature outputs"
    assert [feature.key for feature in built] == ["unit.provides_x"]


def test_build_features_fails_on_missing_dependency_with_fail_policy() -> None:
    class _ProvidesX:
        meta = FeatureMetadata(
            key="unit.provides_x_fail",
            description=None,
            params=(),
            requirements=(),
            provides=(ColumnSpec(name="x"),),
            compatible_grains=("PERSON",),
        )

        def build(self, ctx, params):  # noqa: D401 - testing helper
            return FeatureAssets(models=[], join_models=[], select_expressions=["1 AS x"], tests=[])

    class _NeedsX:
        meta = FeatureMetadata(
            key="unit.needs_x_fail",
            description=None,
            params=(),
            requirements=(),
            provides=(ColumnSpec(name="y"),),
            compatible_grains=("PERSON",),
        )

        def build(self, ctx, params):  # noqa: D401 - testing helper
            return FeatureAssets(models=[], join_models=[], select_expressions=["x + 1 AS y"], tests=[])

    register_feature(_ProvidesX())
    register_feature(_NeedsX())
    mapping = MappingSpec.model_validate(
        {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}
    )
    ctx = BuildContext(
        pipeline_name="p",
        spine_entity="patients",
        spine_alias="p",
        mapping=mapping,
        semantic_contract=default_semantic_contract(),
        naming=NamingConfig(),
    )
    doc = PipelineDocument.model_validate(
        {
            "mapping": mapping.model_dump(),
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
                "validation": {"on_missing_required_column": "fail"},
            },
            "features": [{"key": "unit.needs_x_fail"}],
        }
    )
    with pytest.raises(ValidationError, match="missing dependent feature outputs"):
        _build_features(doc, ctx)


def test_build_features_rejects_select_alias_mismatch() -> None:
    class _BadAliases:
        meta = FeatureMetadata(
            key="unit.bad_aliases",
            description=None,
            params=(),
            requirements=(),
            provides=(ColumnSpec(name="expected"),),
            compatible_grains=("PERSON",),
        )

        def build(self, ctx, params):  # noqa: D401 - testing helper
            return FeatureAssets(
                models=[],
                join_models=[],
                select_expressions=["1 AS actual"],
                tests=[],
            )

    register_feature(_BadAliases())
    mapping = MappingSpec.model_validate(
        {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}
    )
    ctx = BuildContext(
        pipeline_name="p",
        spine_entity="patients",
        spine_alias="p",
        mapping=mapping,
        semantic_contract=default_semantic_contract(),
        naming=NamingConfig(),
    )
    doc = PipelineDocument.model_validate(
        {
            "mapping": mapping.model_dump(),
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
                "validation": {"on_missing_required_column": "warn_skip"},
            },
            "features": [{"key": "unit.bad_aliases"}],
        }
    )
    with pytest.raises(ValidationError, match="select aliases do not match provides"):
        _build_features(doc, ctx)


def test_render_semantic_sql_sorts_columns() -> None:
    sql = _render_semantic_sql("tbl", {"b": "b_phys", "a": "a_phys"})
    assert sql.startswith("SELECT\n")
    assert "a_phys AS a" in sql
    assert "b_phys AS b" in sql


def test_build_semantic_models_includes_reference_models() -> None:
    contract = default_semantic_contract()
    doc = PipelineDocument.model_validate(
        {
            "mapping": {
                "entities": {"patients": {"table": "patients_raw", "columns": {"person_id": "pid"}}},
                "references": {"icd10": {"table": "icd10_raw", "columns": {"code": "code"}}},
            },
            "pipeline": {
                "name": "p",
                "version": "v",
                "grain": "PERSON",
                "spine": {"entity": "patients", "key": "person_id", "columns": ["person_id"]},
                "output": {"table": "t", "materialization": "table"},
            },
            "features": [],
        }
    )
    models = _build_semantic_models(doc, contract)
    names = {m.name for m in models}
    assert "semantic.patients" in names
    assert "semantic.reference__icd10" in names


def test_wipe_out_dir_removes_existing_contents(tmp_path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "stale.txt").write_text("x")
    _wipe_out_dir(out_dir)
    assert out_dir.exists()
    assert not (out_dir / "stale.txt").exists()


def test_write_sqlmesh_project_writes_feature_models_and_tests(tmp_path) -> None:
    out_dir = tmp_path / "out"
    _ensure_layout(out_dir)

    semantic_models = [
        SqlmeshModelSpec(name="semantic.patients", sql="SELECT 1 AS person_id", kind="VIEW", tags=[])
    ]
    final_model = SqlmeshModelSpec(
        name="catalog.schema.output",
        sql="SELECT 1 AS person_id",
        kind="TABLE",
        tags=[],
    )

    feature_model = SqlmeshModelSpec(
        name="feature.some_model",
        sql="SELECT 1 AS x",
        kind="VIEW",
        tags=[],
    )
    feature_test = SqlmeshTestSpec(name="unit_feature_test", yaml="test: ok\n")
    feature = BuiltFeature(
        key="unit.feature_assets",
        metadata=FeatureMetadata(
            key="unit.feature_assets",
            description=None,
            params=(),
            requirements=(),
            provides=(ColumnSpec(name="x"),),
            compatible_grains=("PERSON",),
        ),
        assets=FeatureAssets(
            models=[feature_model],
            join_models=[JoinModelSpec(model_name="semantic.patients", alias="p", on="1=1", join_type="LEFT")],
            select_expressions=[],
            tests=[feature_test],
        ),
        select_expressions=[],
    )

    _write_sqlmesh_project(
        out_dir=out_dir,
        semantic_models=semantic_models,
        features=[feature],
        final_model=final_model,
        pipeline_name="p",
    )

    assert (out_dir / "models" / "semantic" / "patients.sql").exists()
    assert (
        out_dir / "models" / "features" / "unit.feature_assets" / "feature__some_model.sql"
    ).exists()
    assert (out_dir / "models" / "marts" / "enriched__p.sql").exists()
    assert (out_dir / "tests" / "unit_feature_test.yaml").read_text() == "test: ok\n"
