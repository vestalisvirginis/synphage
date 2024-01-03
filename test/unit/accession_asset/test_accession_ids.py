import os

from pathlib import PosixPath
from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import accession_ids
from synphage.resources.ncbi_resource import NCBIConnection


def test_accession_ids(mock_env_ncbi_count):
    context = build_asset_context(resources={"ncbi_connection": NCBIConnection(email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY"))})
    asset_input = 2
    result = accession_ids(context, asset_input)
    assert isinstance(result, dict)
    assert isinstance(result["IdList"], list)
    assert result["IdList"] == ['6JG8_D', '6JG8_C']
    # should be 2
    # Full result {'Count': '579', 'RetMax': '2', 'RetStart': '0', 'QueryKey': '1', 'WebEnv': 'MCID_6595637029885262012b901a', 'IdList': ['6JG8_D', '6JG8_C'], 'TranslationSet': [{'From': 'Spbetavirus', 'To': '"Spbetavirus"[Organism] OR Spbetavirus[All Fields]'}], 'TranslationStack': [{'Term': '"Spbetavirus"[Organism]', 'Field': 'Organism', 'Count': '42', 'Explode': 'Y'}, {'Term': 'Spbetavirus[All Fields]', 'Field': 'All Fields', 'Count': '542', 'Explode': 'N'}, 'OR', 'GROUP'], 'QueryTranslation': '"Spbetavirus"[Organism] OR Spbetavirus[All Fields]'}


def test_accession_ids_asset(mock_env_ncbi_count):
    @asset(name="accession_count")
    def mock_upstream():
        return 2

    assets = [accession_ids, mock_upstream]
    result = materialize_to_memory(assets, resources={"ncbi_connection": NCBIConnection(email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY"))})
    assert result.success
    result_dict = result.output_for_node("accession_ids")
    assert result_dict["IdList"] == ['6JG8_D', '6JG8_C']
