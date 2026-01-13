from spark_preprocessor.semantic_contract import SemanticContract, default_semantic_contract


def test_default_semantic_contract_has_expected_required_columns() -> None:
    contract = default_semantic_contract()
    assert contract.version == "v1"
    assert "lower_snake_case" in contract.naming_rules
    assert contract.required_for("patients") == frozenset({"person_id"})


def test_semantic_contract_helpers_default_to_empty_or_none() -> None:
    contract = SemanticContract(
        version="vX",
        required_columns={},
        optional_columns={},
        recommended_types={},
        naming_rules=(),
    )
    assert contract.required_for("patients") == frozenset()
    assert contract.optional_for("patients") == frozenset()
    assert contract.recommended_type("patients", "person_id") is None

