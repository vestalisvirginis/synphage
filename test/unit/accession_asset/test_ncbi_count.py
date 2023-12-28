from synphage.assets.ncbi_connect.accession import (
    _get_ncbi_count_result,
    NucleotideRecord,
)


QUERY_RESULT = {
    "Term": "Listeria_ivanovii",
    "eGQueryResult": [
        {"DbName": "pubmed", "MenuName": "PubMed", "Count": "270", "Status": "Ok"},
        {"DbName": "pmc", "MenuName": "PubMed Central", "Count": "999", "Status": "Ok"},
        {"DbName": "mesh", "MenuName": "MeSH", "Count": "7", "Status": "Ok"},
        {"DbName": "books", "MenuName": "Books", "Count": "3", "Status": "Ok"},
        {
            "DbName": "pubmedhealth",
            "MenuName": "PubMed Health",
            "Count": "Error",
            "Status": "Database Error",
        },
        {
            "DbName": "omim",
            "MenuName": "OMIM",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {
            "DbName": "ncbisearch",
            "MenuName": "Site Search",
            "Count": "4",
            "Status": "Ok",
        },
        {
            "DbName": "nuccore",
            "MenuName": "Nucleotide",
            "Count": "5384",
            "Status": "Ok",
        },
        {"DbName": "nucgss", "MenuName": "GSS", "Count": "0", "Status": "Ok"},
        {"DbName": "nucest", "MenuName": "EST", "Count": "0", "Status": "Ok"},
        {"DbName": "protein", "MenuName": "Protein", "Count": "94866", "Status": "Ok"},
        {"DbName": "genome", "MenuName": "Genome", "Count": "1", "Status": "Ok"},
        {"DbName": "structure", "MenuName": "Structure", "Count": "6", "Status": "Ok"},
        {"DbName": "taxonomy", "MenuName": "Taxonomy", "Count": "1", "Status": "Ok"},
        {
            "DbName": "snp",
            "MenuName": "SNP",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {
            "DbName": "dbvar",
            "MenuName": "dbVar",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {"DbName": "gene", "MenuName": "Gene", "Count": "8780", "Status": "Ok"},
        {"DbName": "sra", "MenuName": "SRA", "Count": "96", "Status": "Ok"},
        {
            "DbName": "biosystems",
            "MenuName": "BioSystems",
            "Count": "Error",
            "Status": "Database Error",
        },
        {"DbName": "unigene", "MenuName": "UniGene", "Count": "0", "Status": "Ok"},
        {
            "DbName": "cdd",
            "MenuName": "Conserved Domains",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {"DbName": "clone", "MenuName": "Clone", "Count": "0", "Status": "Ok"},
        {"DbName": "popset", "MenuName": "PopSet", "Count": "27", "Status": "Ok"},
        {
            "DbName": "geoprofiles",
            "MenuName": "GEO Profiles",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {
            "DbName": "gds",
            "MenuName": "GEO DataSets",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {
            "DbName": "homologene",
            "MenuName": "HomoloGene",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {
            "DbName": "pccompound",
            "MenuName": "PubChem Compound",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {
            "DbName": "pcsubstance",
            "MenuName": "PubChem Substance",
            "Count": "2",
            "Status": "Ok",
        },
        {
            "DbName": "pcassay",
            "MenuName": "PubChem BioAssay",
            "Count": "25",
            "Status": "Ok",
        },
        {
            "DbName": "nlmcatalog",
            "MenuName": "NLM Catalog",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {"DbName": "probe", "MenuName": "Probe", "Count": "0", "Status": "Ok"},
        {
            "DbName": "gap",
            "MenuName": "dbGaP",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
        {
            "DbName": "proteinclusters",
            "MenuName": "Protein Clusters",
            "Count": "2273",
            "Status": "Ok",
        },
        {
            "DbName": "bioproject",
            "MenuName": "BioProject",
            "Count": "28",
            "Status": "Ok",
        },
        {
            "DbName": "biosample",
            "MenuName": "BioSample",
            "Count": "130",
            "Status": "Ok",
        },
        {
            "DbName": "biocollections",
            "MenuName": "BioCollections",
            "Count": "0",
            "Status": "Term or Database is not found",
        },
    ],
}


def test_get_ncbi_count_result():
    db = "nuccore"
    count_result = QUERY_RESULT
    result = _get_ncbi_count_result(count_result, db)
    assert isinstance(result, NucleotideRecord)
    assert result.dbname == "nuccore"
    assert result.menu == "Nucleotide"
    assert result.count == "5384"
    assert result.status == "Ok"
