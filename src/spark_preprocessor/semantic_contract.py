"""Semantic contract definitions."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SemanticContract:
    """Defines required/optional canonical columns and conventions."""

    version: str
    required_columns: dict[str, frozenset[str]]
    optional_columns: dict[str, frozenset[str]]
    recommended_types: dict[str, dict[str, str]]
    naming_rules: tuple[str, ...]

    def required_for(self, entity: str) -> frozenset[str]:
        return self.required_columns.get(entity, frozenset())

    def optional_for(self, entity: str) -> frozenset[str]:
        return self.optional_columns.get(entity, frozenset())

    def recommended_type(self, entity: str, column: str) -> str | None:
        return self.recommended_types.get(entity, {}).get(column)


def default_semantic_contract() -> SemanticContract:
    """Build the default semantic contract."""

    required = {
        "patients": frozenset({"person_id"}),
        "encounters": frozenset({"person_id"}),
        "medications": frozenset({"person_id"}),
        "procedures": frozenset({"person_id"}),
        "insurance": frozenset({"person_id"}),
        "diagnoses": frozenset({"person_id"}),
    }
    return SemanticContract(
        version="v1",
        required_columns=required,
        optional_columns={},
        recommended_types={},
        naming_rules=("lower_snake_case",),
    )
