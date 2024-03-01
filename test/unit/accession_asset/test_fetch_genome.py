import os

from pathlib import Path
from dagster import materialize_to_memory, build_asset_context, asset

from synphage.assets.ncbi_connect.accession import fetch_genome, QueryConfig
from synphage.resources.ncbi_resource import NCBIConnection


ACCESSION_IDS = {
    "Count": "2",
    "RetMax": "2",
    "RetStart": "0",
    "QueryKey": "1",
    "WebEnv": "MCID_65957a742f85c1163859e8eb",
    "IdList": ["NZ_CP045811.1", "CP045811.1"],
    "TranslationSet": [
        {
            "From": "Bacillus subtilis",
            "To": '"Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]',
        }
    ],
    "TranslationStack": [
        {
            "Term": '"Bacillus subtilis"[Organism]',
            "Field": "Organism",
            "Count": "76615",
            "Explode": "Y",
        },
        {
            "Term": "Bacillus subtilis[All Fields]",
            "Field": "All Fields",
            "Count": "285695",
            "Explode": "N",
        },
        "OR",
        "GROUP",
        {
            "Term": "strain[All Fields]",
            "Field": "All Fields",
            "Count": "146180873",
            "Explode": "N",
        },
        "AND",
        {
            "Term": "P9_B1[All Fields]",
            "Field": "All Fields",
            "Count": "184",
            "Explode": "N",
        },
        "AND",
        "GROUP",
    ],
    "QueryTranslation": '("Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]) AND strain[All Fields] AND P9_B1[All Fields]',
}

#@pytest.mark.skip(reason="need to fix test with mock resource")
def test_fetch_genome(mock_env_ncbi_fetch):
    _path = str(Path(os.getenv("DATA_DIR")) / "download")
    os.makedirs(_path, exist_ok=True)
    context = build_asset_context(
        resources={
            "ncbi_connection": NCBIConnection(
                email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY")
            )
        }
    )
    ids_asset_input = ACCESSION_IDS
    downloaded_asset_input = ["NZ_CP045811.1"]
    config_input = QueryConfig(search_key='("Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]) AND strain[All Fields] AND P9_B1[All Fields]')
    result = fetch_genome(
        context, ids_asset_input, downloaded_asset_input, config_input
    )
    assert isinstance(result, list)
    assert result == list(map(lambda x: f"{_path}/{x}.gb", ACCESSION_IDS["IdList"]))


#@pytest.mark.skip(reason="need to fix test with mock resource")
def test_fetch_genome_asset(mock_env_ncbi_fetch):
    @asset(name="accession_ids")
    def mock_upstream_ids():
        return ACCESSION_IDS

    @asset(name="downloaded_genomes")
    def mock_upstream_download():
        return ["NZ_CP045811.1"]

    @asset(name="setup_query_config")
    def mock_config_upstream():
        return QueryConfig(search_key='("Bacillus subtilis"[Organism] OR Bacillus subtilis[All Fields]) AND strain[All Fields] AND P9_B1[All Fields]')

    _path = str(Path(os.getenv("DATA_DIR")) / "download")
    os.makedirs(_path, exist_ok=True)

    assets = [
        fetch_genome,
        mock_upstream_ids,
        mock_upstream_download,
        mock_config_upstream,
    ]
    result = materialize_to_memory(
        assets,
        resources={
            "ncbi_connection": NCBIConnection(
                email=os.getenv("EMAIL"), api_key=os.getenv("API_KEY")
            )
        },
    )
    assert result.success
    genomes = result.output_for_node("fetch_genome")
    assert genomes == list(map(lambda x: f"{_path}/{x}.gb", ACCESSION_IDS["IdList"]))
