"""Feature interfaces and data structures."""

from dataclasses import dataclass
from typing import Literal, Protocol, TYPE_CHECKING


ParamType = Literal["int", "float", "bool", "str", "date", "enum", "column_ref"]


@dataclass(frozen=True)
class FeatureParamSpec:
    name: str
    type: ParamType
    required: bool = True
    default: object | None = None
    enum_values: tuple[str, ...] | None = None


@dataclass(frozen=True)
class FeatureRequirement:
    entity: str
    columns: frozenset[str]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class FeatureMetadata:
    key: str
    description: str | None
    params: tuple[FeatureParamSpec, ...]
    requirements: tuple[FeatureRequirement, ...]
    provides: tuple[ColumnSpec, ...]
    compatible_grains: tuple[str, ...] | None = None


@dataclass(frozen=True)
class SqlmeshModelSpec:
    name: str
    sql: str
    kind: str
    tags: list[str]


@dataclass(frozen=True)
class JoinModelSpec:
    model_name: str
    alias: str
    on: str
    join_type: str


@dataclass(frozen=True)
class SqlmeshTestSpec:
    name: str
    yaml: str


@dataclass(frozen=True)
class FeatureAssets:
    models: list[SqlmeshModelSpec]
    join_models: list[JoinModelSpec]
    select_expressions: list[str]
    tests: list[SqlmeshTestSpec]


class Feature(Protocol):
    meta: FeatureMetadata

    def build(
        self, ctx: "BuildContext", params: dict[str, object]
    ) -> FeatureAssets: ...


@dataclass(frozen=True)
class ColumnRef:
    entity: str
    column: str


@dataclass(frozen=True)
class BuildContext:
    pipeline_name: str
    spine_entity: str
    spine_alias: str
    mapping: "MappingSpec"
    semantic_contract: "SemanticContract"
    naming: "NamingConfig"

    def resolve_column_ref(self, raw: str) -> ColumnRef:
        if "." in raw:
            entity, column = raw.split(".", 1)
        else:
            entity, column = self.spine_entity, raw
        return ColumnRef(entity=entity, column=column)

    def column_ref_sql(self, raw: str) -> str:
        ref = self.resolve_column_ref(raw)
        if ref.entity != self.spine_entity:
            raise ValueError(
                f"Column ref '{raw}' is not on the spine entity '{self.spine_entity}'"
            )
        return f"{self.spine_alias}.{ref.column}"


if TYPE_CHECKING:
    from spark_preprocessor.schema import MappingSpec, NamingConfig
    from spark_preprocessor.semantic_contract import SemanticContract
