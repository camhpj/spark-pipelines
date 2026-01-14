from collections.abc import Generator

import pytest

from spark_preprocessor.features import registry as feature_registry


@pytest.fixture(autouse=True)
def _restore_feature_registry() -> Generator[None, None, None]:
    """Keep feature registry mutations from leaking across tests."""
    snapshot = dict(feature_registry._REGISTRY)
    yield
    feature_registry._REGISTRY.clear()
    feature_registry._REGISTRY.update(snapshot)
