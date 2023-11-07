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




## Roadmap

- [ ] create config options for the plot at run time
- [ ] create possibility to add ref sequence with special colour coding
- [ ] integrate the NCBI search asset
- [ ] create interactive plot 
- [ ] Help us in a discussion?


## Contributing 



## Authors
- [vestalisvirginis](https://github.com/vestalisvirginis) / Virginie Grosboillot / 🇫🇷 


## License
Apache License 2.0
Free for commercial use, modification, distribution, patent use, private use.
Just preserve the copyright and license.


> Made with ❤️ in Ljubljana 🇸🇮