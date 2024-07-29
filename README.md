# synphage

Pipeline to create phage genome synteny graphics from genbank files

This library provides an intuitive tool for creating synteny graphics highlighting the conserved genes between multiple genome sequences.  
This tool is primarily designed to work with phage genomes or other short sequences of interest, although it works with bacterial genomes as well.

Despite numerous synteny tools available on the market, this tool has been conceived because none of the available tools allows to visualise gene conservation in multiple sequences at one glance (as typically cross-links are drawn only between two consecutive sequences for a better readability).

As a result `synphage` was born.  

In addition to show conserved genes across multiple sequences, the originality of this library stands in the fact that when working on the same set of genomes the initial blast and computation need to be run only once. Multiple graphics can then be generated from these data, comparing all the genomes or only a set of genomes from the analysed dataset. Moreover, the generated data is also available to the user as a table, where individual genes or groups of genes can easily be checked by name for conservation or uniqueness.


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
[See complete documention](https://vestalisvirginis.github.io/synphage/installation#/pip-install)

### Via docker
```bash
docker pull vestalisvirginis/synphage:<tag>
```
>[!NOTE]
>Replace `<tag>` with the [latest image tag](https://hub.docker.com/r/vestalisvirginis/synphage/tags).  
[See complete documention](https://vestalisvirginis.github.io/synphage/installation#/docker-install)

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

>[!TIP]
>These variables can be set with a `.env` file located in your working directory (Dagster will automatically load them from the .env file when initialising the pipeline) or can be passed in the terminal before starting to run synphage:  
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
> For more detailed explainations on using `synphage docker image`, check our [documentation](https://vestalisvirginis.github.io/synphage/installation/#run-synphage-container).


### Running Synphage

A step-by-step example, performed on a group of closely related *Lactococcus* phages is available in the [documentation](https://vestalisvirginis.github.io/synphage/phages/).

#### Starting Dagster

`synphage` uses [Dagster](https://dagster.io). In order to run synphage jobs, you need to start dagster first.

Set up the environment variable DAGSTER_HOME in order to keep a trace of your previous run (optional). For more information, see [Dagster documentation](https://docs.dagster.io/deployment/dagster-instance). 

```bash
export DAGSTER_HOME=<dagster_home_directory>

dagster dev -h 0.0.0.0 -p 3000 -m synphage
```

For docker users:
```bash
docker run -p 3000 vestalisvirginis/synphage:<tag>
```
For more information and options, check [running synphage container](https://vestalisvirginis.github.io/synphage/installation/#run-synphage-container).

#### Running the jobs

synphage pipeline is composed of `four steps` that need to be run `sequencially`.
[See complete documention](https://vestalisvirginis.github.io/synphage/pipeline)

##### Step 1: Loading the data into the pipeline
Data is loaded into the pipeline from the `input_folder` set by the user `and/or` `downloaded` from the NCBI.  
- `step_1a_get_user_data` : load user's data
- `step_1b_download` : download data from the NCBI

> [!IMPORTANT]
> - Only one of the jobs is required to successfully run step 1.
> - Configuration is required for `step_1b_download` job: `search_key`, that receives the keywords for querying the NCBI database.  

###### Query config options :
Field Name | Description | Default Value
 ------- | ----------- | ----
`search_key` | Keyword(s) for NCBI query | Myoalterovirus


> [!TIP]
> Both jobs can be run if the user needs both, local and downloaded files.

##### Step 2: Data validation
Completeness of the data is validated at this step.
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
The graph is created during this last step. The step 4 can be run multiple times with different configurations and different sets of data, as long as the data have been processed once through steps 1, 2 and 3.
- `step_4_make_plot` : use data generated at step 3 and the genbank files to plot the synteny diagram  
  
> [!IMPORTANT]
> Configuration is require for `step_4_make_plot` job: `graph_type`, that receives either `blastn` or `blastp` as value for specifying what dataset to use for the plot. Default value is set to `blastn`. For more information about the configuration at step 4, check the [documentation](https://vestalisvirginis.github.io/synphage/configurations/).

> [!TIP]
> Different synteny plots can be generated from the same set of genomes. In this case the three first steps only need to be run once and the fourth step, `step_4_make_plot`, can be triggered separately for each graphs.
> For modifying the sequences to be plotted (selected sequences, order, orientation), the sequences.csv file generated at step3 can be modify and saved under a different name. This new `.csv` can be passed in the job configuration `sequence_file`.
> 
> *sequences.csv*
> ``` txt 
> genome_1.gb,0
> genome_2.gb,1
> genome_3.gb,0
> ```

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
 

## Documentation

Visit [https://vestalisvirginis.github.io/synphage/](https://vestalisvirginis.github.io/synphage/) for complete [installation instruction](https://vestalisvirginis.github.io/synphage/installation/), [pipeline guidelines](https://vestalisvirginis.github.io/synphage/jobs/) and [step-by-step example](https://vestalisvirginis.github.io/synphage/phages/).


## Support

**Where to ask for help?**

Open a [discussion](https://github.com/vestalisvirginis/synphage/discussions).


## Roadmap

- ~~[x] create config options for the plot at run time~~
- ~~[x] integrate the NCBI search~~
- ~~[x] implement blastp~~  
- [ ] create possibility to add ref sequence with special colour coding
- [ ] create interactive plot  
- [ ] Help us in a discussion?


## Status

`[2024-07-20]`   ✨ __New features!__  
- `Checks` : to validate the quality of the data
- `Blastp` is finally implemented

  
`[2024-01-11]`   ✨ __New feature!__   to simplify the addition of new sequences into the genbank folder
 - `download` : download genomes to be analysed from the NCBI database 


## Contributing 

We accept different types of contributions, including some that don't require you to write a single line of code. For detailed instructions on how to get started with our project, see [CONTRIBUTING](CONTRIBUTING.md) file.


## Authors
- [vestalisvirginis](https://github.com/vestalisvirginis) / Virginie Grosboillot / 🇫🇷 


<!-- ## Contributors
<a href="https://github.com/vestalisvirginis/synphage/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=vestalisvirginis/synphage" />
</a> -->

## License
Apache License 2.0
Free for commercial use, modification, distribution, patent use, private use.
Just preserve the copyright and license.


> Made with ❤️ in Ljubljana 🇸🇮