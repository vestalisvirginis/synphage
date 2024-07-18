from dagster import AssetCheckSeverity, AssetCheckSpec

from toolz import compose, juxt  # type: ignore
from operator import itemgetter as it
from operator import methodcaller as mc
from cuallee import Check, CheckLevel  # type: ignore


def _check_severity(check: Check) -> AssetCheckSeverity:
    if check.level == CheckLevel.WARNING:
        return AssetCheckSeverity.WARN
    else:
        return AssetCheckSeverity.ERROR


def _create_check_specs(check_dict, asset_name) -> tuple[Check, list[AssetCheckSpec]]:
    """Create the checks and the check_spec to be passed in the validation asset from a given dictionnary of checks"""
    check = Check()
    for key in check_dict:
        check_type, check_name, cols, value = compose(
            juxt(
                map(
                    it,
                    [
                        "check_type",
                        "check_name",
                        "cols",
                        "check_value",
                    ],
                )
            ),
            mc("get", key),
        )(check_dict)
        if check_type == "bio":
            juxt(map(lambda x: mc(check_name, x), cols))(check.bio)
        elif check_type == "parameterised":
            juxt(map(lambda x: mc(check_name, x, value), cols))(check)
        else:
            juxt(map(lambda x: mc(check_name, x), cols))(check)

    check_specs = [
        AssetCheckSpec(
            name=f"{item.name}.{item.column}",
            asset=asset_name,
            description=check_dict[item.name].get("check_description"),
        )
        for item in check.rules
    ]

    return check, check_specs
