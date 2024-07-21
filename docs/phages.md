# Synteny diagram of bacteriophage genomes with 'synphage'

For this step-by-step example, a group of closely related Lactococcus phages has been selected based on name from [Bacterial and Viral Bioinformatics Resource Center (BV-BRC)](https://www.bv-brc.org/view/GenomeList/?eq(genome_name,phage)#view_tab=genomes&filter=keyword(Lactococcus)).  

<figure markdown="span">
    ![Selected Lactococcus phages](./images/phages/lactococcus_bv_brc.png)
    <figcaption>Selected *Lactococcus* phages for this example</figcaption>
</figure>  

Lactococcus phage genomes can be mosaic and are relevant for the diary industry.  

## Running 'synphage' pipeline  

To get familiar with `synphage` capabilities, you can reproduce the step-by-step example describe below. To get started, go to [step 1](#step1-phage-example).  

*Prerequisite:* you need to have `synphage` installed in a python environment or in a docker container (see [Installation](installation.md#pip-install)) and running. Open the Dagster UI in your browser to get started.
<figure markdown="span">
    ![Dagster home page](./images/phages/dagster_landing_page.png)
    <figcaption>Dagster UI - landing page</figcaption>
</figure>


### Step 1: Download the data of interest <a id="step1-phage-example"></a>

Go to Dagster_home -> Jobs -> download.  
<!-- #![Job_1](./images/phages/job_1.webm) -->


<iframe width="560" height="315"
src="http://0.0.0.0:8000/synphage/images/phages/job_1.webm" 
frameborder="0" 
allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" 
allowfullscreen></iframe>

<figure markdown="span">
    ![Download job - overview](./images/phages/download_job_overview.png)
    <figcaption>Overview of the download job</figcaption>
</figure>

In order to query our genomes of interest, we need to pass query to the `search-key` in the configuration panel.  
To access the configuration window, open the dropdown menu (white arrow on the right of the black box located on the up right corner, labelled `Materialize all`) and select `Open launchpad`.  
<figure markdown="span">
    ![Warning message - Download Job](./images/phages/download_warning_message.png){ width="200"}
    <figcaption>Warning message - Download job</figcaption>
    ![Lauchpad](./images/phages/launchpad_button.png){ width="200"}
    <figcaption>Access the lauchpad to the job configuration</figcaption>
</figure>

A warning will pop-up, press `Confirm` and configure the `search_key` parameter, changing the default value with the following keywords (Accession names for the genomes previously selected in this case) to query the NCBI database:  

=== "search_key"
    ``` bash
    KP793101[Accession] OR KP793102[Accession] OR KP793103[Accession] OR KP793105[Accession] OR KP793104[Accession] OR KP793107[Accession] OR KP793106[Accession] OR KP793108[Accession] OR KP793109[Accession] OR KP793112[Accession] OR KP793114[Accession] OR KP793113[Accession] OR KP793110[Accession] OR KP793115[Accession] OR KP793117[Accession] OR KP793118[Accession] OR KP793122[Accession] OR KP793116[Accession] OR KP793120[Accession] OR KP793121[Accession] OR KP793123[Accession] OR KP793126[Accession] OR KP793127[Accession] OR KP793125[Accession] OR KP793124[Accession] OR KP793128[Accession] OR KP793130[Accession] OR KP793129[Accession] OR KP793132[Accession] OR KP793131[Accession] OR KP793133[Accession] OR KP793135[Accession] OR KP793134[Accession] OR KP793111[Accession] OR KP793119[Accession]
    ```

<figure markdown="span">
    ![Config panel - Download Job](./images/phages/download_config_windows.png)
    <figcaption>Configuration panel - Download job</figcaption>
</figure>

Select `Materialize`, in the right bottom corner.  

Most of the assets provides metadata after successful complition of the run. These metadata allow the user to easily follow the smooth execution of the job.
<figure markdown="span">
    ![Metadata fetch genome - Download Job](./images/phages/download_metadata_example.png)
    <figcaption>Example of metadata provided for the `fetch_genome` asset. After complition the user can see the number and the name of GenBank files that have been downloaded.</figcaption>
</figure>

When the job finishes its executon, we can move to `step 2` to run the validation on the data.
<figure markdown="span">
    ![Executed - Download Job](./images/phages/download_after_execution.png)
    <figcaption>Job is terminated</figcaption>
</figure>


### Step 2: Run data quality checks on the dataset

(For more detailed information on this step, check the doc(link to doc))  

Go to Dagster_home -> Jobs -> make_validations.  
<figure markdown="span">
    ![Job overview - Validation Job](./images/phages/validation_job_overview.png)
    <figcaption>Overview of the validation job</figcaption>
</figure>
<figure markdown="span">
    ![ZoomIn- Validation Job](./images/phages/validation_zoom_in.png)
    <figcaption>Zoom In </figcaption>
</figure>

Select `Materialize all` (black box located on the up right corner).

This job runs checks on each of the files that have been downloaded. The number of checks that pass or fail are directly visible on the assets and reflects how complete each dataset is.
The result table for the checks is available in the metadata panel and the detailed results for the checks can be accessed either from the right panel under `Checks -> View all check details` or by selecting the asset and then the `Checks` tab.  

<figure markdown="span">
    ![Assets with checks - Validation Job](./images/phages/validation_assets_with_checks.png)
    <figcaption>The assets display the number of failed and passed checks.</figcaption>
    ![Check table - Validation Job](./images/phages/validation_check_table.png)
    <figcaption>Result table for the check, accessible from the metadata panel.</figcaption>
    ![Check result overview - Validation Job](./images/phages/validation_checks_rightpanel.png)
    <figcaption>Check results are accessible via the right panel.</figcaption>
    ![Asset check results - Validation Job](./images/phages/validation_check_results.png)
    <figcaption>Full asset check results.</figcaption>
</figure>

The metadata attached to the second step of the validation, inform the user about the logic apply to the file, more precisely what feature type the software will be using for downstream processing and what attribute will be used as unique identifier for the coding genes.
<figure markdown="span">
    ![Logic - Validation Job](./images/phages/validation_apply_logic.png)
    <figcaption>Step 2 of the validation with metadata.</figcaption>
</figure>

The metadata attached to the last step of the validation, render an overview of the transformed data.
<figure markdown="span">
    ![Transformation - Validation Job](./images/phages/validation_transformation.png)
    <figcaption>Step 3 of the validation with metadata.</figcaption>
</figure>  


### Step 3: Run the blast

For this example, blastn was run on the dataset.  

Go to Dagster_home -> Jobs -> make_blastn  
<figure markdown="span">
    ![Overview - Blastn Job](./images/phages/blastn_job_overview.png)
    <figcaption>Overview of the blastn job</figcaption>
</figure>  

Select `Materialize all` (black box located on the up right corner). 

<figure markdown="span">
    ![Completed - Blastn Job](./images/phages/blastn_after_completion.png)
    <figcaption>Completed job</figcaption>
</figure> 

Checks are run at the beginning of the job to verify that the key and identifier used for each of the coding element are unique over all the sequences.
<figure markdown="span">
    ![Checks - Blastn Job](./images/phages/blastn_checks_df.png)
    <figcaption>The checks confirm the uniqueness of the chosen identifier for each of the coding elements.</figcaption>
</figure> 

Several files are generated during this step.
=== "create_fasta_n"
    ``` title="KP793103_1.fna"
    --8<-- "KP793103_1.fna::7"
    ```

=== "create_blast_n_db"
    ``` title="KP793103_1"
    KP793103_1.ndb
    KP793103_1.nhr
    KP793103_1.nim
    KP793103_1.not
    KP793103_1.nsq
    KP793103_1.ntf
    KP793103_1.nto
    ```

=== "get_blastn"
    ``` title="KP793103_1_vs_KP793107_1"
    --8<-- "KP793103_1_vs_KP793107_1:19:51"
    ```

=== "transform_blastn"
    ``` title="KP793103_1_vs_KP793107_1.parquet"
    --8<-- "blastn_df.md:1:5"
    ```

=== "unified_dataframe"
    ``` title="gene_uniqueness.parquet"
    --8<-- "gene_uniqueness.md"
    ```

The generated `gene_uniqueness.parquet` file is used to generate the downstream graphic.  
This file can be read and manipulated with any DataFrame API the user choose, such as [Pandas](https://pandas.pydata.org/), [Apache Spark](https://spark.apache.org/docs/latest/api/python/index.html), [Polars](https://docs.pola.rs/api/python/stable/reference/index.html), [DuckDB](https://duckdb.org/) but also in a non-programmatic manner using softwares such as [Tad](https://www.tadviewer.com/).  


### Step 4: Generate the plot

Go to Dagster_home -> Jobs -> make_plot
(# ref asset before materialisation)

This job require some configuration:   
To access the configuration window, open the dropdown menu (white arrow on the right of the black box located on the up right corner, labelled Materialize all) and select `Open launchpad`.  
Change title and blastn   

Select the 'Materialize' botton.   
(# ref asset after materialisation)

Show output graph.  


## Analyse the data

Examples of two queries in the data tables as in the paper figure
- unique genes
- conserved genes with protein_id....


Conclusion






'search_target','query_id','query_key','query_len','number_of_hits','source_key','num','bit_score','score','evalue','identity','query_from', 'query_to','query_strand','hit_from','hit_to','hit_strand',"align_len",'gaps', 'percentage_of_identity'