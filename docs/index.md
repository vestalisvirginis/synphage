# Welcome to `synphage` documentation

`synphage` is a pipeline to create phage genome synteny graphics from genbank files

This library provides an intuitive tool to create synteny graphics highlighting the conserved genes between multiple genome sequences.  
This tool is primarily designed to work with phage genomes or other short sequences of interest, although it works with bacterial genomes as well.

Despite numerous synteny tools available on the market, this tool has been conceived because none of the available tools allows to visualise gene conservation in multiple sequences at one glance (as typically cross-links are drawn only between two consecutive sequences for a better readability).

As a result `synphage` was born. 

In addition to show conserved genes across multiple sequences, the originality of this library stands in the fact that when working on the same set of genomes the initial blast and computation need to be run only once. Multiple graphics can then be generated from these data, comparing all the genomes or only a set of genomes from the analysed dataset. Moreover, the generated data is also available to the user as a table, where individual genes or groups of genes can easily be checked by name for conservation or uniqueness in the other genomes.

## Roadmap

- [x] create config options for the plot at run time
- [x] integrate the NCBI search
- [ ] implement blastp 
- [ ] create possibility to add ref sequence with special colour coding
- [ ] create interactive plot 
- [ ] Help us in a discussion?

## Contributing
We accept different types of contributions, including some that don't require you to write a single line of code. For detailed instructions on how to get started with our project, see [CONTRIBUTING](https://github.com/vestalisvirginis/synphage/blob/main/CONTRIBUTING.md) file.


## Authors
- [vestalisvirginis](https://github.com/vestalisvirginis) / Virginie Grosboillot


## License
[Apache License 2.0](https://github.com/vestalisvirginis/synphage/blob/main/LICENSE)  
Free for commercial use, modification, distribution, patent use, private use.
Just preserve the copyright and license.


> Made with ❤️ in Ljubljana 🇸🇮