# Configurations

Using Dagster to run synphage pipeline allows the user to easily configure the jobs at run-time. 


## Plotting config options  

To change the plot configations, open the dropdown menu at the right of the `Materialize all` black button. Then go to `Open launchpad`. This will open a pop-up window with the configurations.

List of the supported configurations:

1. Genome selection
    
    Field Name | Description | Default Value  
    ------- | ----------- | ----  
    `sequence_file` | File containing the plot instruction | sequences.csv  

2. Graphic <a id="plot-config"></a>

    Field Name | Description | Default Value
    ------- | ----------- | ----
    `title` | Generated plot file title | synteny_plot
    `colours` | Gene identity colour bar | ["#fde725", "#90d743", "#35b779", "#21918c", "#31688e", "#443983", "#440154"] 
    `gradient` | Nucleotide identity colour bar | #B22222
    `graph_shape` | Linear or circular representation | linear
    `graph_pagesize` | Output document format | A4
    `graph_fragments` | Number of fragments | 1
    `graph_start` | Sequence start | 1
    `graph_end` | Sequence end | length of the longest genome  


## Query config options  

To change the query configations, open the dropdown menu at the right of the `Materialize all` black button. Then go to `Open launchpad`. This will open a pop-up window with the configurations.

Field Name | Description | Default Value
 ------- | ----------- | ----
`search_key` | Keyword(s) for NCBI query | Myoalterovirus
`database` | Database identifier | nuccore  