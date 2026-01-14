"""Model naming utilities for local and Databricks targets."""

import re
from dataclasses import dataclass


_IDENTIFIER_PART_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_identifier_part(value: str, *, label: str) -> None:
    if not value:
        raise ValueError(f"{label} must be non-empty")
    if "`" in value:
        raise ValueError(f"{label} must not contain backticks: {value!r}")
    if not _IDENTIFIER_PART_PATTERN.match(value):
        raise ValueError(f"{label} must match ^[A-Za-z0-9_]+$: {value!r}")


def parse_three_part_table(identifier: str) -> tuple[str, str, str]:
    """Parse a Databricks 3-part table identifier: catalog.schema.table."""

    parts = identifier.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Expected a 3-part identifier 'catalog.schema.table', got {identifier!r}"
        )
    catalog, schema, table = parts
    _validate_identifier_part(catalog, label="catalog")
    _validate_identifier_part(schema, label="schema")
    _validate_identifier_part(table, label="table")
    return catalog, schema, table


def feature_slug(feature_key: str) -> str:
    """Convert a logical dotted feature key into an identifier-safe slug."""

    parts = feature_key.split(".")
    if any(not part for part in parts):
        raise ValueError(f"Invalid feature key: {feature_key!r}")
    for part in parts:
        _validate_identifier_part(part, label="feature key segment")
    return "__".join(parts)


def pipeline_slug(value: str) -> str:
    """Validate a pipeline slug value."""

    _validate_identifier_part(value, label="pipeline slug")
    return value


def quote_databricks_identifier_part(part: str) -> str:
    """Quote a catalog/schema/table identifier part for Databricks SQL."""

    _validate_identifier_part(part, label="identifier")
    return f"`{part}`"


@dataclass(frozen=True)
class DatabricksNamespaces:
    """Resolved Databricks namespaces for internal and output relations."""

    catalog: str
    base_schema: str
    semantic_schema: str
    features_schema: str
    output_table: str

    @property
    def base_namespace(self) -> str:
        return f"{self.catalog}.{self.base_schema}"

    @property
    def semantic_namespace(self) -> str:
        return f"{self.catalog}.{self.semantic_schema}"

    @property
    def features_namespace(self) -> str:
        return f"{self.catalog}.{self.features_schema}"


def databricks_namespaces(
    *,
    output_table: str,
    pipeline_slug_value: str,
    semantic_schema_suffix: str,
    features_schema_suffix: str,
) -> DatabricksNamespaces:
    catalog, base_schema, table = parse_three_part_table(output_table)
    slug = pipeline_slug(pipeline_slug_value)

    semantic_schema = f"{base_schema}__{slug}{semantic_schema_suffix}"
    features_schema = f"{base_schema}__{slug}{features_schema_suffix}"
    _validate_identifier_part(semantic_schema, label="semantic schema")
    _validate_identifier_part(features_schema, label="features schema")

    return DatabricksNamespaces(
        catalog=catalog,
        base_schema=base_schema,
        semantic_schema=semantic_schema,
        features_schema=features_schema,
        output_table=table,
    )


def semantic_entity_model_name(namespaces: DatabricksNamespaces, entity: str) -> str:
    _validate_identifier_part(entity, label="entity")
    return f"{namespaces.catalog}.{namespaces.semantic_schema}.{entity}"


def semantic_reference_model_name(
    namespaces: DatabricksNamespaces, reference: str
) -> str:
    _validate_identifier_part(reference, label="reference")
    return f"{namespaces.catalog}.{namespaces.semantic_schema}.reference__{reference}"


def feature_model_name(
    namespaces: DatabricksNamespaces, *, feature_key: str, purpose: str
) -> str:
    slug = feature_slug(feature_key)
    _validate_identifier_part(purpose, label="feature model purpose")
    return f"{namespaces.catalog}.{namespaces.features_schema}.{slug}__{purpose}"
