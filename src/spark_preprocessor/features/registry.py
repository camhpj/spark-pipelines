"""Feature registry."""

from __future__ import annotations

from collections.abc import Iterable

from spark_preprocessor.errors import FeatureNotFoundError, ValidationError
from spark_preprocessor.features.base import Feature

_REGISTRY: dict[str, Feature] = {}


def register_feature(feature: Feature) -> None:
    key = feature.meta.key
    if key in _REGISTRY:
        raise ValidationError(f"Feature already registered: {key}")
    _REGISTRY[key] = feature


def get_feature(key: str) -> Feature:
    try:
        return _REGISTRY[key]
    except KeyError as exc:
        raise FeatureNotFoundError(f"Unknown feature key: {key}") from exc


def list_features() -> Iterable[str]:
    return _REGISTRY.keys()
