import yaml

from spark_preprocessor.features.base import SqlmeshModelSpec
from spark_preprocessor.sqlmesh_project import (
    SqlmeshConfig,
    render_models,
    render_sqlmesh_config,
    render_sqlmesh_model,
)


def test_render_sqlmesh_model_renders_full_for_table() -> None:
    spec = SqlmeshModelSpec(
        name="catalog.schema.model",
        sql="SELECT 1 AS x",
        kind="TABLE",
        tags=[],
    )
    text = render_sqlmesh_model(spec)
    assert "kind FULL" in text


def test_render_sqlmesh_model_renders_tags() -> None:
    spec = SqlmeshModelSpec(
        name="catalog.schema.model",
        sql="SELECT 1 AS x",
        kind="VIEW",
        tags=["a", "b"],
    )
    text = render_sqlmesh_model(spec)
    assert "tags [a, b]" in text


def test_render_sqlmesh_config_has_expected_shape() -> None:
    payload = yaml.safe_load(render_sqlmesh_config(SqlmeshConfig()))
    assert payload["model_defaults"]["dialect"] == "spark"
    assert payload["engine"]["type"] == "databricks"


def test_render_models_renders_mapping() -> None:
    specs = [
        SqlmeshModelSpec(name="a", sql="SELECT 1", kind="VIEW", tags=[]),
        SqlmeshModelSpec(name="b", sql="SELECT 2", kind="VIEW", tags=[]),
    ]
    rendered = render_models(specs)
    assert set(rendered.keys()) == {"a", "b"}
