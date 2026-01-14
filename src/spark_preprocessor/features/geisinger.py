"""Geisinger client specific features."""

from spark_preprocessor.features.base import (
    BuildContext,
    ColumnSpec,
    FeatureAssets,
    FeatureMetadata,
    FeatureRequirement,
    JoinModelSpec,
    SqlmeshModelSpec,
)
from collections.abc import Callable

from spark_preprocessor.features.base import Feature


class Forward65FlagFeature:
    """Flag patients whose PCP is associated with Geisinger 65 Forward.

    This feature expects:
    - A spine entity column `patients.pcp_name`.
    - A reference table `physicians_65_forward` containing `pcp_name` and `active65f`.

    The output is a stable `Y`/`N` flag to match downstream expectations in existing
    Geisinger SQL.
    """

    meta = FeatureMetadata(
        key="geisinger.forward_65_flag",
        description=(
            "Flag indicating if a person has a PCP associated with Geisinger 65 Forward."
        ),
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
        model_name = ctx.feature_model_name(self.meta.key, purpose="65_forward_pcp")
        join_alias = "f65"
        reference_model = ctx.semantic_reference_model_name("physicians_65_forward")
        sql = f"""
            SELECT DISTINCT pcp_name
            FROM {reference_model}
            WHERE active65f = 1
                AND pcp_name IS NOT NULL
        """
        pcp_name_sql = ctx.column_ref_sql("pcp_name")

        return FeatureAssets(
            models=[
                SqlmeshModelSpec(
                    name=model_name,
                    sql=sql,
                    kind="VIEW",
                    tags=[],
                )
            ],
            join_models=[
                JoinModelSpec(
                    model_name=model_name,
                    alias=join_alias,
                    on=f"COALESCE({pcp_name_sql}, '') = {join_alias}.pcp_name",
                    join_type="LEFT",
                )
            ],
            select_expressions=[
                f"CASE WHEN {join_alias}.pcp_name IS NOT NULL THEN 'Y' ELSE 'N' END AS forward_65_flag"
            ],
            tests=[],
        )


def register_geisinger_features(register: Callable[[Feature], None]) -> None:
    """Register Geisinger features into the feature registry.

    Args:
        register: Registry hook (typically `spark_preprocessor.features.register_feature`).
    """

    register(Forward65FlagFeature())
