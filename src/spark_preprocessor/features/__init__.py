"""Feature registry and built-ins."""

from spark_preprocessor.features.builtins import register_builtins
from spark_preprocessor.features.geisinger import register_geisinger_features
from spark_preprocessor.features.registry import (
    get_feature,
    list_features,
    register_feature,
)

register_builtins(register_feature)
register_geisinger_features(register_feature)

__all__ = ["get_feature", "list_features", "register_feature"]
