# synphage

Pipeline to create phage genome synteny graphics from genbank files.

This library has been conceived 
The originality of this library is that it provides tabular data in addition to the plot where the user can search/check genes individually.  
The synteny plot colour code represents the aboundance of a gene amount the ploted sequences.


## Stats 
[![PyPI version](https://badge.fury.io/py/synphage.svg)](https://badge.fury.io/py/synphage)
[![](https://img.shields.io/pypi/dm/synphage.svg?style=popout-square)](https://pypi.org/project/synphage/)
[![License](https://img.shields.io/github/license/vestalisvirginis/synphage.svg?style=popout-square)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)



## Visuals 


## Install

Synphage is available via pip install or as docker image.


### Via pip 
```bash
pip install synphage
```


### Via docker
```bash
docker pull vestalisvirginis/synphage:latest
```


### Additional dependencies

Synphage relies on two non-python dependencies that need to be manually installed when Synphage is installed with pip:
- [Blast+](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) >= 2.12.0   
- [OpenJDK](https://openjdk.org/projects/jdk/17/) == 17  


## Usage

### Setup 

Synphage requires:
- to specify a folder path where the  `genbank` folder will be present and where generated data will be stored;
- a `genbank` folder populated with genbank files (`.gb` and  `.gbk` extension are accepted);
- a `sequences.csv` file containing the file name and orientation of the sequences to plot.


**Warning**: Genbank file names should not contain spaces.


#### Path setup

```bash
export PHAGY_DIRECTORY=<path_to_data_folder>
```

**Note:** For docker users, this path is defaulted to `/data`.

#### CSV file

```txt
genome_1.gb,0
genome_2.gb,1
genome_3.gb,0
```


### Running Synphage

`Synphage` uses [Dagster](https://dagster.io). In order to run synphage jobs, you need to start dagster first.


#### Starting Dagster

Set up the environment variable DAGSTER_HOME in order to keep a trace of your previous run. For more information, see [Dagster documentation](https://docs.dagster.io/deployment/dagster-instance). 

```bash
export DAGSTER_HOME=<dagster_home_directory>

dagster dev -h 0.0.0.0 -p 3000 -m synphage
```


#### Running the jobs

The current software is structured in three different jobs.
 - `blasting_job` : create the blastn of each sequences against each sequences (results -> gene_identity folder)
 - `transform` : create three tables from the blastn results and genbank files (results -> tables)
 - `synteny_job` : create the synteny graph (results -> synteny)

**Note:** Different synteny plots can be generated from the same set of genomes. In this case the two first jobs only need to be run once and the third job (`synteny_job`) can be triggered separately for each graphs.


## Output

Synphage's output consists of three main parquet files and the synteny graph. However all the data generated by the synphage pipeline are made available in your workng directory.

### Generated data architecture

```
.
├── <path_to_data_folder>/
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
1. `uniqueness.parquet` combined both previous data tables in one, allowing the user to quickly know how many matches their gene(s) of interest has/have retrieved. These data are then used to compute the colour code used for the synteny plot. The result of the computation is recorded in the `colour_table.parquet`. This file is over-written between each `synteny_job` run. 


### Synteny plot

The `synteny plot` represents the sequences  
## Roadmap

- [ ] create config options for the plot at run time
- [ ] create possibility to add ref sequence with special colour coding
- [ ] integrate the NCBI search asset
- [ ] create interactive plot 
- [ ] Help us in a discussion?


## Contributing 

We accept different types of contributions, including some that don't require you to write a single line of code. For detailed instructions on how to get started with our project, see [CONTRIBUTING](CONTRIBUTING.md) file.


## Authors
- [vestalisvirginis](https://github.com/vestalisvirginis) / Virginie Grosboillot / 🇫🇷 


## License
Apache License 2.0
Free for commercial use, modification, distribution, patent use, private use.
Just preserve the copyright and license.


> Made with ❤️ in Ljubljana 🇸🇮