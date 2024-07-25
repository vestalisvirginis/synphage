# Welcome to `synphage` documentation

`synphage` is a pipeline to create phage genome synteny graphics from genbank files

This library provides an intuitive tool for creating synteny graphics highlighting the conserved genes between multiple genome sequences.  
This tool is primarily designed to work with phage genomes or other short sequences of interest, although it works with bacterial genomes as well.

Despite numerous synteny tools available on the market, this tool has been conceived because none of the available tools allows to visualise gene conservation in multiple sequences at one glance (as typically cross-links are drawn only between two consecutive sequences for a better readability).

As a result `synphage` was born.  

In addition to show conserved genes across multiple sequences, the originality of this library stands in the fact that when working on the same set of genomes the initial blast and computation need to be run only once. Multiple graphics can then be generated from these data, comparing all the genomes or only a set of genomes from the analysed dataset. Moreover, the generated data is also available to the user as a table, where individual genes or groups of genes can easily be checked by name for conservation or uniqueness.


## Support

**Where to ask for help?**

Open a [discussion](https://github.com/vestalisvirginis/synphage/discussions).


## Roadmap

- [x] create config options for the plot at run time
- [x] integrate the NCBI search
- [x] implement blastp 
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
- [vestalisvirginis](https://github.com/vestalisvirginis) / Virginie Grosboillot

> Made with ❤️ in Ljubljana 🇸🇮