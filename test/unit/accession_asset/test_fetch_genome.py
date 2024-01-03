import os

from pathlib import PosixPath
from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import fetch_genome
from synphage.resources.ncbi_resource import NCBIConnection


ACCESSION_IDS = {'Count': '579', 'RetMax': '2', 'RetStart': '0', 'QueryKey': '1', 'WebEnv': 'MCID_6595637029885262012b901a', 'IdList': ['6JG8_D', '6JG8_C'], 'TranslationSet': [{'From': 'Spbetavirus', 'To': '"Spbetavirus"[Organism] OR Spbetavirus[All Fields]'}], 'TranslationStack': [{'Term': '"Spbetavirus"[Organism]', 'Field': 'Organism', 'Count': '42', 'Explode': 'Y'}, {'Term': 'Spbetavirus[All Fields]', 'Field': 'All Fields', 'Count': '542', 'Explode': 'N'}, 'OR', 'GROUP'], 'QueryTranslation': '"Spbetavirus"[Organism] OR Spbetavirus[All Fields]'}


def test_fetch_genome(mock_env_ncbi_fetch):
    _path = "/".join([os.getenv("PHAGY_DIRECTORY"), "download"])
    os.makedirs(_path, exist_ok=True)
    context = build_asset_context(resources={"ncbi_connection": NCBIConnection(email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY"))})
    ids_asset_input = ACCESSION_IDS 
    downloaded_asset_input = ['6JG8_D']
    result = fetch_genome(context, ids_asset_input, downloaded_asset_input)
    assert isinstance(result, list)
    assert result == list(map(lambda x: f"{_path}/{x}.gb", ACCESSION_IDS["IdList"]))


def test_fetch_genome_asset(mock_env_ncbi_fetch):
    @asset(name="accession_ids")
    def mock_upstream_ids():
        return ACCESSION_IDS 
    @asset(name="downloaded_genomes")
    def mock_upstream_download():
        return ['6JG8_D']

    _path = "/".join([os.getenv("PHAGY_DIRECTORY"), "download"])
    os.makedirs(_path, exist_ok=True)
    
    assets = [fetch_genome, mock_upstream_ids, mock_upstream_download]
    result = materialize_to_memory(assets, resources={"ncbi_connection": NCBIConnection(email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY"))})
    assert result.success
    genomes = result.output_for_node("fetch_genome")
    assert genomes == list(map(lambda x: f"{_path}/{x}.gb", ACCESSION_IDS["IdList"]))

