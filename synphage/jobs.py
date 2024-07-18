from dagster import (
    AssetSelection,
    define_asset_job,
    ExperimentalWarning,
    ConfigArgumentWarning,
)

import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)
warnings.filterwarnings("ignore", category=ConfigArgumentWarning)


# Data acquisition

# Job 1 : get data from the user
get_user_data = define_asset_job(
    name="get_user_data",
    selection=(
        AssetSelection.groups("Users_input")
        | AssetSelection.groups("Status")
        & AssetSelection.assets("genbank_history").downstream(depth=2)
    ),
)

# Job 2 : download gb files from the ncbi database
download = define_asset_job(
    name="download",
    selection=(
        AssetSelection.groups("NCBI_connect")
        | AssetSelection.groups("Status")
        & AssetSelection.assets("genbank_history").downstream(depth=2)
    ),
)

# Job 3 : validations with to reload + refresh UI
validations = define_asset_job(
    name="make_validation",
    selection=AssetSelection.groups("Status")
    & AssetSelection.assets("reload_ui_asset").downstream(depth=3),
)

# Job 4 : blastn
blastn = define_asset_job(
    name="make_blastn",
    selection=(
        AssetSelection.assets("append_processed_df")
        | AssetSelection.groups("Blaster")
        & AssetSelection.assets("create_fasta_n").downstream()
    ),
)

# Job 5 : blastp
blastp = define_asset_job(
    name="make_blastp",
    selection=(
        AssetSelection.assets("append_processed_df")
        | AssetSelection.groups("Blaster")
        & AssetSelection.assets("create_fasta_p").downstream()
    ),
)

# Job 6 : blastn and blastp combined
all_blast = define_asset_job(
    name="make_all_blast",
    selection=(
        AssetSelection.assets("append_processed_df") | AssetSelection.groups("Blaster")
    ),
)

# Job 7 : create the synteny diagram
plot = define_asset_job(
    name="make_plot",
    selection=AssetSelection.groups("Viewer"),
)
