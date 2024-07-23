# synphage

Pipeline to create phage genome synteny graphics from genbank files

This library provides an intuitive tool to create synteny graphics highlighting the conserved genes between multiple genome sequences.  
This tool is primarily designed to work with phage genomes or other short sequences of interest, although it works with bacterial genomes as well.

Despite numerous synteny tools available on the market, this tool has been conceived because none of the available tools allows to visualise gene conservation in multiple sequences at one glance (as typically cross-links are drawn only between two consecutive sequences for a better readability).

As a result `synphage` was born.  

In addition to show conserved genes across multiple sequences, the originality of this library stands in the fact that when working on the same set of genomes the initial blast and computation need to be run only once. Multiple graphics can then be generated from these data, comparing all the genomes or only a set of genomes from the analysed dataset. Moreover, the generated data is also available to the user as a table, where individual genes or groups of genes can easily be checked by name for conservation or uniqueness in the other genomes.


## Stats 
[![PyPI version](https://badge.fury.io/py/synphage.svg)](https://badge.fury.io/py/synphage)
[![ci](https://github.com/vestalisvirginis/synphage/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/vestalisvirginis/synphage/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/vestalisvirginis/synphage/graph/badge.svg?token=HX32HRFS3V)](https://codecov.io/github/vestalisvirginis/synphage)
[![](https://img.shields.io/pypi/dm/synphage.svg?style=popout-square)](https://pypi.org/project/synphage/)
[![License](https://img.shields.io/github/license/vestalisvirginis/synphage.svg?style=popout-square)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


## Install

`synphage` is available via [pip install](https://pypi.org/project/synphage/) or as [docker image](https://hub.docker.com/r/vestalisvirginis/synphage).  
For more detailed instruction, consult [synphage installation guide](https://vestalisvirginis.github.io/synphage/installation/).  

### Via pip 
``` bash
pip install synphage
```

### Via docker
```bash
docker pull vestalisvirginis/synphage:<tag>
```
>[!NOTE]
>Replace `<tag>` with the [latest image tag](https://hub.docker.com/r/vestalisvirginis/synphage/tags).


### Additional dependencies

synphage relies on one non-python dependency that need to be manually installed when synphage is installed with pip:
- [Blast+](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) >= 2.12.0  

For LINUX ubuntu:
``` bash
apt update
apt install -y ncbi-blast+
```

For other systems, you can download the [executables](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) check the [installation documentation](https://www.ncbi.nlm.nih.gov/books/NBK569861/).  


## Usage

### Setup  

synphage requires to specify the following environment variables:
- `INPUT_DIR` : to specify the path to the folder containing the user's `GenBank files`. If not set, this path will be defaulted to the temp folder. The path can also be configured at run time.  
- `OUTPUT_DIR`: to specify the path to the folder where the data generated during the run will be stored. If not set, this path will be defaulted to the temp folder.  
- `EMAIL` (optional): to connect to the NCBI database.  
- `API_KEY` (optional): to connect to the NCBI database and download files.  

>[!TIP]
>These variables can be set by a `.env` file located in your working directory or can be passed directly in the terminal:  
>**.env**
>``` .env
>INPUT_DIR=path/to/my/data/
>OUTPUT_DIR=path/to/synphage/data
>EMAIL=user.email@email.com
>API_KEY=UserApiKey
>```
>**bash**
>``` bash
>export INPUT_DIR=<path_to_data_folder>
>export OUTPUT_DIR=<path_to_synphage_folder>
>export EMAIL=user.email@email.com
>export API_KEY=UserApiKey
>```

>[!NOTE]  
>For docker users, the `INPUT_DIR` is defaulted to `/user_files` and `OUTPUT_DIR` is defaulted to `/data`.  


### Running Synphage

`synphage` uses [Dagster](https://dagster.io). In order to run synphage jobs, you need to start dagster first.


#### Starting Dagster

Set up the environment variable DAGSTER_HOME in order to keep a trace of your previous run (optional). For more information, see [Dagster documentation](https://docs.dagster.io/deployment/dagster-instance). 

```bash
export DAGSTER_HOME=<dagster_home_directory>

dagster dev -h 0.0.0.0 -p 3000 -m synphage
```


#### Running the jobs

synphage pipeline is composed of `four steps` that need to be run `sequencially`.

##### Step 1: Loading the data into the pipeline
Data is loaded into the pipeline from the `input_folder` set by the user `and/or` `downloaded` from the NCBI.  
- `step_1a_get_user_data` : load user's data
- `step_1b_download` : download data from the NCBI

> [!IMPORTANT]
> - Only one of the jobs is required to successfully run step 1.
> - Configuration is require for `step_1b_download` job: `search_key`, that receives the keywords for querying the NCBI database.

> [!TIP]
> Both jobs can be run if the user needs both, local and downloaded files.

##### Step 2: Data validation
Completeness and uniqueness of the data is validated at this step.
- `step_2_make_validation` : perform checks and transformations on the dataset that are required for downstream processing

> [!IMPORTANT]
> This step is required and cannot be skipped.

##### Step 3: Blasting the data
The blast is performed at this step of the pipeline and three different `options` are available:  
- `step_3a_make_blastn` : run a Nucleotide BLAST on the dataset
- `step_3b_make_blastp` : run a Protein BLAST on the dataset
- `step_3c_make_all_blast` : run both, Nucleotide and Protein BLAST simultaneously  

> [!IMPORTANT]
> Only one of the above jobs is required to successfully run step 3.

> [!TIP]
> Both `step_3a_make_blastn` and `step_3b_make_blastp` jobs can be run sequencially, mainly in the case where the user decide to run the second job based on the results obtained for the first one.


##### Step 4: Synteny plot
The graph is created during this last step. The step 4 can be run multiple time with different configurations and different sets of data, as long as the data have been processed once through step1, 2 and 3.
- `step_4_make_plot` : use data generated at step 3 and the genbank files to plot the synteny diagram  
  
> [!IMPORTANT]
> Configuration is require for `step_4_make_plot` job: `graph_type`, that receives either `blastn` or `blastp` as value for specifying what dataset to use for the plot. Default value is set to `blastn`. For more information about the configuration at step 4, check the [documentation](https://vestalisvirginis.github.io/synphage/configurations/).

> [!TIP]
> Different synteny plots can be generated from the same set of genomes. In this case the three first steps only need to be run once and the fourth step, `step_4_make_plot`, can be triggered separately for each graphs.


## Output

synphage's output consists of three main parquet files and the synteny graph. However all the data generated by the synphage pipeline are made available in your workng directory.

### Generated data architecture

```
.
├── <path_to_data_folder>/
│   ├── download/
│   ├── genbank/
│   ├── fs/
│   ├── gene_identity/
│   │   ├── fasta/
│   │   ├── blastn_database/
│   │   └── blastn/
│   ├── tables/
│   │   ├── blastn.parquet
│   │   ├── locus_and_gene.parquet
│   │   └── uniqueness.parquet
│   └── synteny/
│      ├── colour_table.parquet
│      └── synteny_graph.svg
└── ...
```


### Tables

The `tables` folder contains the three main parquet files generated by the `transform` job of synphage.   
1. `blastn.parquet` contains the collection of the best match for each locus tag/gene against each genomes. The percentage of identity between two genes/loci are then used for calculating the plot cross-links between the sequences.
1. `locus_and_gene.parquet` contains the full list of `locus tag` and corresponding `gene` names when available for all the genomes in the genbank folder. If the genbank file only contains `CDS`, the locus tag and gene value are replaced by the protein identifyer `protein_id`.
1. `uniqueness.parquet` combined both previous data tables in one, allowing the user to quickly know how many matches their gene(s) of interest has/have retrieved. These data are then used to compute the colour code used for the synteny plot. The result of the computation is recorded in the `colour_table.parquet`. This file is over-written between each `plot` run. 



#### CSV file

```txt
genome_1.gb,0
genome_2.gb,1
genome_3.gb,0
```

### Synteny plot

The `synteny plot` is generated as `.svg file` and `.png file`, and contains the sequences indicated in the sequences.csv file. The genes are colour-coded according to their abundance (percentage) among the plotted sequences. The cross-links between each consecutive sequence indicates the percentage or similarities between those two sequences.


#### Plotting config options

Field Name | Description | Default Value
 ------- | ----------- | ----
`title` | Generated plot file title | synteny_plot
`colours` | Gene identity colour bar | ["#fde725", "#90d743", "#35b779", "#21918c", "#31688e", "#443983", "#440154"] 
`gradient` | Nucleotide identity colour bar | #B22222
`graph_shape` | Linear or circular representation | linear
`graph_pagesize` | Output document format | A4
`graph_fragments` | Number of fragments | 1
`graph_start` | Sequence start | 1
`graph_end` | Sequence end | length of the longest genome


### Genbank file download

The `download` allow to download sequences of interest into the genbank folder to be subsequently processed by the software.


#### Requirement

Connection to the NCBI databases requires user's `email` and `api_key`.
```bash
export EMAIL=user.email@email.com
export API_KEY=UserApiKey
```

#### Query config options

Field Name | Description | Default Value
 ------- | ----------- | ----
`search_key` | Keyword(s) for NCBI query | Myoalterovirus
`database` | Database identifier | nuccore


## Documentation

- [https://vestalisvirginis.github.io/synphage/](https://vestalisvirginis.github.io/synphage/)

## Support


## Roadmap

- ~~[x] create config options for the plot at run time~~
- ~~[x] integrate the NCBI search~~
- [ ] implement blastp 
- [ ] create possibility to add ref sequence with special colour coding
- [ ] create interactive plot 
- [ ] Help us in a discussion?


## Status

`[2024-01-11]`   ✨ __New feature!__   to simplify the addition of new sequences into the genbank folder
 - `download` : download genomes to be analysed from the NCBI database 


## Contributing 

We accept different types of contributions, including some that don't require you to write a single line of code. For detailed instructions on how to get started with our project, see [CONTRIBUTING](CONTRIBUTING.md) file.


## Authors
- [vestalisvirginis](https://github.com/vestalisvirginis) / Virginie Grosboillot / 🇫🇷 


## Contributors
<a href="https://github.com/vestalisvirginis/synphage/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=vestalisvirginis/synphage" />
</a>

## License
Apache License 2.0
Free for commercial use, modification, distribution, patent use, private use.
Just preserve the copyright and license.


> Made with ❤️ in Ljubljana 🇸🇮