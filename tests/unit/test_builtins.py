from spark_preprocessor.features.base import BuildContext
from spark_preprocessor.features.builtins import AgeBucketFeature, AgeFeature, register_builtins
from spark_preprocessor.schema import MappingSpec, NamingConfig
from spark_preprocessor.semantic_contract import default_semantic_contract


def test_age_feature_build_renders_expected_sql() -> None:
    ctx = BuildContext(
        pipeline_name="p",
        pipeline_slug="p",
        spine_entity="patients",
        spine_alias="p",
        mapping=MappingSpec.model_validate(
            {"entities": {"patients": {"table": "t", "columns": {"dob": "dob", "as_of": "as_of"}}}}
        ),
        semantic_contract=default_semantic_contract(),
        naming=NamingConfig(),
    )
    assets = AgeFeature().build(ctx, {"start": "dob", "end": "as_of"})
    assert assets.select_expressions == [
        "CAST(FLOOR(months_between(p.as_of, p.dob) / 12) AS INT) AS age"
    ]


def test_age_bucket_feature_build_renders_expected_sql() -> None:
    ctx = BuildContext(
        pipeline_name="p",
        pipeline_slug="p",
        spine_entity="patients",
        spine_alias="p",
        mapping=MappingSpec.model_validate(
            {"entities": {"patients": {"table": "t", "columns": {"person_id": "pid"}}}}
        ),
        semantic_contract=default_semantic_contract(),
        naming=NamingConfig(),
    )
    assets = AgeBucketFeature().build(ctx, {})
    assert assets.select_expressions == [
        (
            "CASE "
            "WHEN age IS NULL THEN NULL "
            "WHEN age < 18 THEN '0-17' "
            "WHEN age < 35 THEN '18-34' "
            "WHEN age < 50 THEN '35-49' "
            "WHEN age < 65 THEN '50-64' "
            "ELSE '65+' "
            "END AS age_bucket"
        )
    ]


def test_register_builtins_registers_expected_keys() -> None:
    seen: list[str] = []

    def register(feature) -> None:
        seen.append(feature.meta.key)

    register_builtins(register)
    assert set(seen) == {"age", "age_bucket"}
