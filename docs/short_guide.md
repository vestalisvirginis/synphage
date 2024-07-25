# Getting started with synphage

`synphage` is a pipeline to create genome synteny graphics from genbank files.
`synphage` is available via [pip install](https://pypi.org/project/synphage/) or as [docker image](https://hub.docker.com/r/vestalisvirginis/synphage).  
For more detailed instruction, consult [synphage installation guide](https://vestalisvirginis.github.io/synphage/installation/).  


## Installation 

If you are familiar with Python, you can install `synphage` via [`pip`](installation.md#/pip-install). If not, we recommend using the [`docker image`](installation.md#docker-install) instead.


### Requirements 

`synphage` is available independently of your operative system, you can run `synphage` on:

- `Linux` :material-penguin:
- `MacOS` :material-apple:
- `Windows` :material-microsoft-windows:

The binaries in the python `wheel` are built universally so as far as you have `Python 3.11` you are all set.

### Via pip

Users installing synphage with pip, need to have Blast+ installed as well (see [requirement installation](installation.md#/pip-dependencies)). 

```bash
# Latest
pip install synphage
```

For more details, see the [Installation guide](installation.md#pip-install).

### Via docker

```bash
docker pull vestalisvirginis/synphage:<tag>
```

???+ note 
    Replace `<tag>` with the [latest image tag](https://hub.docker.com/r/vestalisvirginis/synphage/tags).  
    [See complete documention](https://vestalisvirginis.github.io/synphage/installation#/docker-install)

The [Docker image](https://hub.docker.com/r/vestalisvirginis/synphage) comes with all the dependencies pre-installed. For more details, see the [Installation guide](installation.md#docker-install).


### Additional dependencies

synphage relies on one non-python dependency that needs to be manually installed when synphage is installed with pip:
- [Blast+](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) >= 2.12.0  

Install `Blast+` using your package manager of choice, e.g. for linux ubuntu:
``` bash
apt update
apt install -y ncbi-blast+
```

or by downloading an [executables](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) appropriate for your system. For help, check the complete [installation documentation](https://www.ncbi.nlm.nih.gov/books/NBK569861/).  


## Usage

### Setup 

synphage requires the user to specify the following environment variables:
- `INPUT_DIR` : to specify the path to the folder containing the user's `GenBank files`. If not set, this path will be defaulted to the temp folder. This path can also be modified at run time.  
- `OUTPUT_DIR`: to specify the path to the folder where the data generated during the run will be stored. If not set, this path will be defaulted to the temp folder.  
- `EMAIL` (optional): to connect to the NCBI database.  
- `API_KEY` (optional): to connect to the NCBI database and download files.  


???+ tip
    These variables can be set with a `.env` file located in your working directory (Dagster will automatically load them from the .env file when initialising the pipeline) or can be passed in the terminal before starting to run synphage:  
    === ":material-file-document-outline: .env"
        ``` .env
        INPUT_DIR=path/to/my/data/
        OUTPUT_DIR=path/to/synphage/data
        EMAIL=user.email@email.com
        API_KEY=UserApiKey
        ```
    === ":octicons-terminal-16: Bash"
        ``` bash
        export INPUT_DIR=<path_to_data_folder>
        export OUTPUT_DIR=<path_to_synphage_folder>
        export EMAIL=user.email@email.com
        export API_KEY=UserApiKey
        ```

???+ note
    For docker users, the `INPUT_DIR` is defaulted to `/user_files` and `OUTPUT_DIR` is defaulted to `/data`.  
    For more detailed explainations on using `synphage docker image`, check our [documentation](installation.md#/run-synphage-container).


### Running Synphage

A step-by-step example, performed on a group of closely related *Lactococcus* phages is available in the [documentation](https://vestalisvirginis.github.io/synphage/phages/).

#### Starting Dagster

`synphage` uses [Dagster](https://dagster.io). In order to run synphage jobs, you need to start dagster first.

Set up the environment variable DAGSTER_HOME in order to keep a trace of your previous run (optional). For more information, see [Dagster documentation](https://docs.dagster.io/deployment/dagster-instance). 

```bash
export DAGSTER_HOME=<dagster_home_directory>

dagster dev -h 0.0.0.0 -p 3000 -m synphage
```


#### Running the jobs

synphage pipeline is composed of `four steps` that need to be run `sequencially`.
[See complete documention](pipeline.md)

##### Step 1: Loading the data into the pipeline
Data is loaded into the pipeline from the `input_folder` set by the user `and/or` `downloaded` from the NCBI.  
- `step_1a_get_user_data` : load user's data
- `step_1b_download` : download data from the NCBI

???+ tip
   - Only one of the jobs is required to successfully run step 1.
   - Configuration is required for `step_1b_download` job: `search_key`, that receives the keywords for querying the NCBI database.  

###### Query config options :
Field Name | Description | Default Value
 ------- | ----------- | ----
`search_key` | Keyword(s) for NCBI query | Myoalterovirus


???+ tip
    Both jobs can be run if the user needs both, local and downloaded files.

##### Step 2: Data validation
Completeness of the data is validated at this step.
- `step_2_make_validation` : perform checks and transformations on the dataset that are required for downstream processing

???+ warning
    This step is required and cannot be skipped.

##### Step 3: Blasting the data
The blast is performed at this step of the pipeline and three different `options` are available:  
- `step_3a_make_blastn` : run a Nucleotide BLAST on the dataset
- `step_3b_make_blastp` : run a Protein BLAST on the dataset
- `step_3c_make_all_blast` : run both, Nucleotide and Protein BLAST simultaneously  

???+ tip
    - Only one of the above jobs is required to successfully run step 3.
    - Both `step_3a_make_blastn` and `step_3b_make_blastp` jobs can be run sequencially, mainly in the case where the user decide to run the second job based on the results obtained for the first one.


##### Step 4: Synteny plot
The graph is created during this last step. The step 4 can be run multiple times with different configurations and different sets of data, as long as the data have been processed once through steps 1, 2 and 3.
- `step_4_make_plot` : use data generated at step 3 and the genbank files to plot the synteny diagram  
  
???+ warning
    Configuration is require for `step_4_make_plot` job: `graph_type`, that receives either `blastn` or `blastp` as value for specifying what dataset to use for the plot. Default value is set to `blastn`. For more information about the configuration at step 4, check the [documentation](configurations.md).

???+ tip
    - Different synteny plots can be generated from the same set of genomes. In this case the three first steps only need to be run once and the fourth step, `step_4_make_plot`, can be triggered separately for each graphs.
    - For modifying the sequences to be plotted (selected sequences, order, orientation), the sequences.csv file generated at step3 can be modify and saved under a different name. This new `.csv` can be passed in the job configuration `sequence_file`.

    === ":fontawesome-solid-file-csv: sequences.csv"
        ``` txt 
        genome_1.gb,0
        genome_2.gb,1
        genome_3.gb,0
        ```


###### Plotting config options

The appearance of the plot can be modified through the configuration. 

Field Name | Description | Default Value
 ------- | ----------- | ----
`title` | Generated plot file title | synteny_plot
`graph_type` | Type of dataset to use for the plot | blastn
`colours` | Gene identity colour bar | ["#fde725", "#90d743", "#35b779", "#21918c", "#31688e", "#443983", "#440154"] 
`gradient` | Nucleotide identity colour bar | #B22222
`graph_shape` | Linear or circular representation | linear
`graph_pagesize` | Output document format | A4
`graph_fragments` | Number of fragments | 1
`graph_start` | Sequence start | 1
`graph_end` | Sequence end | length of the longest genome


## Output

synphage's output consists of four to six main parquet files (depending if blastn and blastp were both executed) and the synteny graphic. However all the data generated by the synphage pipeline are made available in your data directory.  
(see [Documentation](output.md))

### Generated data architecture

```
.
├── <path_to_synphage_folder>/
│   ├── download/
│   ├── fs/
│   ├── genbank/
│   ├── gene_identity/
│   │   ├── fasta_n/
│   │   ├── blastn_database/
│   │   └── blastn/
│   ├── protein_identity/
│   │   ├── fasta_p/
│   │   ├── blastp_database/
│   │   └── blastp/
│   ├── tables/
│   │   ├── genbank_db.parquet
│   │   ├── processed_genbank_df.parquet
│   │   ├── blastn_summary.parquet
│   │   ├── blastp_summary.parquet
│   │   ├── gene_uniqueness.parquet
│   │   └── protein_uniqueness.parquet
│   ├── sequences.csv
│   └── synteny/
│      ├── colour_table.parquet
│      ├── synteny_graph.png
│      └── synteny_graph.svg
└── ...
```


### Tables

The `tables` folder contains the four to six main parquet files generated by the pipeline.
1. `genbank_db.parquet` : original data parsed from the GenBank files. 
2. `processed_genbank_df.parquet` : data processed during the validation step. It contains two additional columns:
   - `gb_type` : specifying what type of data is used as unique identifier of the coding elements
   - `key`: unique identifier based on the columns: `filename`, `id` and `locus_tag`.
3. `blastn_summary.parquet` : data parsed from the `blastn` output json files. It contains the collection of the best match for each sequence against each genomes. The percentage of identity between two sequences are then used for calculating the plot cross-links between the sequences.  
4. `blastp_summary.parquet` : data parsed from the `blastp` output json files. It contains the collection of the best match for each sequence against each genomes. The percentage of identity between two sequences are then used for calculating the plot cross-links between the sequences.  
5. `gene_uniqueness.parquet` : combines both `processed_genbank_df.parquet` and `blastn_summary.parquet` in a single parquet file, allowing the user to quickly know how many matches their sequence(s) of interest has/have retrieved. These data are then used to compute the colour code used for the synteny plot. The result of the computation is recorded in the `colour_table.parquet`. This file is over-written between each `plot` run. 
6. `protein_uniqueness.parquet` : combines both `processed_genbank_df.parquet` and `blastp_summary.parquet` in a single parquet file, allowing the user to quickly know how many matches their sequence(s) of interest has/have retrieved. These data are then used to compute the colour code used for the synteny plot. The result of the computation is recorded in the `colour_table.parquet`. This file is over-written between each `plot` run. 


### Synteny plot

The `synteny plot` is generated as `.svg file` and `.png file`, and contains the sequences indicated in the `sequences.csv` file. The genes are colour-coded according to their abundance (percentage) among the plotted sequences. The cross-links between each consecutive sequence indicates the percentage of similarities between those two sequences.
