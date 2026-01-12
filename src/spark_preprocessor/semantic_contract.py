"""Semantic contract definitions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SemanticContract:
    """Defines required canonical columns per entity."""

    required_columns: dict[str, frozenset[str]]

    def required_for(self, entity: str) -> frozenset[str]:
        return self.required_columns.get(entity, frozenset())


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
    return SemanticContract(required_columns=required)
