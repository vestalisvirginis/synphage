import pytest
from dagster import AssetCheckSeverity, AssetCheckSpec
from cuallee import Check, CheckLevel

from synphage.utils.check_factory import _check_severity, _create_check_specs


@pytest.mark.parametrize(
    "level, result",
    [
        (CheckLevel.WARNING, AssetCheckSeverity.WARN),
        (CheckLevel.ERROR, AssetCheckSeverity.ERROR),
    ],
    ids=["warning", "error"],
)
def test_check_severity(level, result):
    check = Check(level=level)
    result = _check_severity(check)
    assert isinstance(result, AssetCheckSeverity)
    assert result == result


def test_create_check_specs():
    check_dictionnary = {
        "is_complete": {
            "check_type": "std",
            "check_name": "is_complete",
            "cols": ["A"],
            "check_value": None,
            "check_description": "Description check 1",
        },
        "is_dna": {
            "check_type": "bio",
            "check_name": "is_dna",
            "cols": ["A"],
            "check_value": None,
        },
        "is_in": {
            "check_type": "parameterised",
            "check_name": "is_in",
            "cols": ["A"],
            "check_value": ("A", "C", "T", "G"),
        },
    }
    asset_name = "test_check"
    result = _create_check_specs(check_dictionnary, asset_name)
    assert isinstance(result, tuple)
    result_check = result[0]
    result_specs = result[1]
    assert isinstance(result_check, Check)
    assert len(result_check.keys) == 3
    assert isinstance(result_specs, list)
    assert result_specs == [
        AssetCheckSpec(
            name="is_complete.A", asset="test_check", description="Description check 1"
        ),
        AssetCheckSpec(name="is_dna.A", asset="test_check"),
        AssetCheckSpec(name="is_in.A", asset="test_check"),
    ]
