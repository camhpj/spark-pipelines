"""Built-in features."""

from spark_preprocessor.features.base import (
    ColumnSpec,
    FeatureAssets,
    FeatureMetadata,
    FeatureParamSpec,
    BuildContext,
)


class AgeFeature:
    """Compute age in years from a start and end date column."""

    meta = FeatureMetadata(
        key="age",
        description="Age in years computed from two date columns.",
        params=(
            FeatureParamSpec(name="start", type="column_ref"),
            FeatureParamSpec(name="end", type="column_ref"),
        ),
        requirements=(),
        provides=(ColumnSpec(name="age", dtype="int"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx: BuildContext, params: dict[str, object]) -> FeatureAssets:
        start = params["start"]
        end = params["end"]
        start_sql = ctx.column_ref_sql(str(start))
        end_sql = ctx.column_ref_sql(str(end))
        expression = f"CAST(FLOOR(months_between({end_sql}, {start_sql}) / 12) AS INT)"
        return FeatureAssets(
            models=[],
            join_models=[],
            select_expressions=[f"{expression} AS age"],
            tests=[],
        )


class AgeBucketFeature:
    """Bucketize age into a fixed set of ranges."""

    meta = FeatureMetadata(
        key="age_bucket",
        description="Bucketized age derived from the age feature.",
        params=(),
        requirements=(),
        provides=(ColumnSpec(name="age_bucket", dtype="str"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx: BuildContext, params: dict[str, object]) -> FeatureAssets:
        expression = (
            "CASE "
            "WHEN age IS NULL THEN NULL "
            "WHEN age < 18 THEN '0-17' "
            "WHEN age < 35 THEN '18-34' "
            "WHEN age < 50 THEN '35-49' "
            "WHEN age < 65 THEN '50-64' "
            "ELSE '65+' "
            "END"
        )
        return FeatureAssets(
            models=[],
            join_models=[],
            select_expressions=[f"{expression} AS age_bucket"],
            tests=[],
        )


def register_builtins(register) -> None:
    register(AgeFeature())
    register(AgeBucketFeature())
