import pytest

from spark_preprocessor.errors import FeatureNotFoundError, ValidationError
from spark_preprocessor.features.base import ColumnSpec, FeatureAssets, FeatureMetadata
from spark_preprocessor.features.registry import get_feature, list_features, register_feature


class _UnitFeature:
    meta = FeatureMetadata(
        key="unit.registry.feature",
        description=None,
        params=(),
        requirements=(),
        provides=(ColumnSpec(name="x", dtype="int"),),
        compatible_grains=("PERSON",),
    )

    def build(self, ctx, params):  # noqa: D401 - testing helper
        return FeatureAssets(models=[], join_models=[], select_expressions=["1 AS x"], tests=[])


def test_register_feature_rejects_duplicate_key() -> None:
    feature = _UnitFeature()
    register_feature(feature)
    with pytest.raises(ValidationError):
        register_feature(feature)


def test_get_feature_raises_on_unknown_key() -> None:
    with pytest.raises(FeatureNotFoundError):
        get_feature("unit.registry.missing")


def test_list_features_includes_registered_feature() -> None:
    register_feature(_UnitFeature())
    assert "unit.registry.feature" in set(list_features())

