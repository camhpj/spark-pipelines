"""Pydantic schemas for pipeline and mapping specifications."""

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from spark_preprocessor.errors import ConfigurationError


class EntityMapping(BaseModel):
    """Mapping for a canonical entity or reference table."""

    model_config = ConfigDict(extra="forbid")

    table: str
    columns: dict[str, str]


class MappingSpec(BaseModel):
    """Mapping specification from canonical entities to physical tables."""

    model_config = ConfigDict(extra="forbid")

    entities: dict[str, EntityMapping]
    references: dict[str, EntityMapping] = Field(default_factory=dict)

    def has_entity(self, name: str) -> bool:
        return name in self.entities

    def has_reference(self, name: str) -> bool:
        return name in self.references

    def has_column(self, entity: str, column: str) -> bool:
        if entity in self.entities:
            return column in self.entities[entity].columns
        if entity in self.references:
            return column in self.references[entity].columns
        return False

    def entity_table(self, entity: str) -> str:
        if entity in self.entities:
            return self.entities[entity].table
        if entity in self.references:
            return self.references[entity].table
        raise ConfigurationError(f"Unknown entity or reference: {entity}")

    def entity_columns(self, entity: str) -> dict[str, str]:
        if entity in self.entities:
            return self.entities[entity].columns
        if entity in self.references:
            return self.references[entity].columns
        raise ConfigurationError(f"Unknown entity or reference: {entity}")


def load_mapping_spec(path: Path) -> MappingSpec:
    """Load a mapping specification from YAML.

    Args:
        path: Path to the mapping YAML.

    Returns:
        Parsed MappingSpec.

    Raises:
        ConfigurationError: If the file is missing or invalid.
    """

    if not path.exists():
        raise ConfigurationError(f"Mapping file not found: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ConfigurationError("Mapping YAML must be a mapping at the top level")
    if "mapping" in data:
        data = data["mapping"]
    try:
        return MappingSpec.model_validate(data)
    except Exception as exc:
        raise ConfigurationError("Mapping YAML failed validation") from exc


class PrefixingConfig(BaseModel):
    """Config for column name prefixing."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    scheme: Literal["namespace", "feature"] = "namespace"
    separator: str = "__"


class NamingConfig(BaseModel):
    """Config for output column naming."""

    model_config = ConfigDict(extra="forbid")

    prefixing: PrefixingConfig = Field(default_factory=PrefixingConfig)
    collision_policy: Literal["fail", "auto_prefix"] = "fail"


class ValidationConfig(BaseModel):
    """Validation policy options."""

    model_config = ConfigDict(extra="forbid")

    on_missing_required_column: Literal["fail", "warn_skip"] = "fail"


class SpineConfig(BaseModel):
    """Spine configuration for the pipeline."""

    model_config = ConfigDict(extra="forbid")

    entity: str
    key: str = "person_id"
    columns: list[str]


class OutputConfig(BaseModel):
    """Output table configuration."""

    model_config = ConfigDict(extra="forbid")

    table: str
    materialization: Literal["table", "view"] = "table"


class PipelineMeta(BaseModel):
    """Pipeline metadata and configuration."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str
    grain: str = "PERSON"
    spine: SpineConfig
    output: OutputConfig
    naming: NamingConfig = Field(default_factory=NamingConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)


class FeatureConfig(BaseModel):
    """Feature selection with params."""

    model_config = ConfigDict(extra="forbid")

    key: str
    params: dict[str, Any] = Field(default_factory=dict)


class ProfilingConfig(BaseModel):
    """Profiling notebook configuration."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    sample_rows: int = 100000
    sampling_mode: Literal["random", "deterministic"] = "random"
    sampling_seed: int = 42
    profile_raw_entities: list[str] = Field(default_factory=list)
    profile_output: bool = True
    output_dir: str | None = None


class PipelineDocument(BaseModel):
    """Top-level document including mapping and pipeline sections."""

    model_config = ConfigDict(extra="forbid")

    mapping: MappingSpec
    pipeline: PipelineMeta
    features: list[FeatureConfig] = Field(default_factory=list)
    profiling: ProfilingConfig | None = None


def load_pipeline_document(path: Path) -> PipelineDocument:
    """Load a pipeline document from YAML.

    Args:
        path: Path to the YAML document.

    Returns:
        Parsed PipelineDocument.

    Raises:
        ConfigurationError: If the file is missing or invalid.
    """

    if not path.exists():
        raise ConfigurationError(f"Pipeline file not found: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ConfigurationError("Pipeline YAML must be a mapping at the top level")
    try:
        return PipelineDocument.model_validate(data)
    except Exception as exc:  # pydantic validation errors are already informative
        raise ConfigurationError("Pipeline YAML failed validation") from exc
