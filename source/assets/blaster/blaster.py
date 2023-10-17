from dagster import (
    asset,
    Field,
    op,
    graph,
    In,
    Out,
    graph_asset,
    multi_asset,
    AssetOut,
    MetadataValue,
    AutoMaterializePolicy,
    EnvVar,
    job,
    Config,
    sensor,
    RunRequest,
    RunConfig,
)

import os
import glob
import re
import shutil

from Bio import SeqIO
from Bio.SeqRecord import SeqRecord
from pathlib import Path
from datetime import datetime
from functools import reduce
from typing import List
from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as F


genbank_folder_config = {
    "phage_download_directory": Field(
        str,
        description="Path to folder containing the genebank sequence files",
        # default_value="/usr/src/data/phage_view_data/genome_download",
        default_value="/usr/src/data_folder/jaka_data/genome_download",
    ),
    "spbetaviruses_directory": Field(
        str,
        description="Path to folder containing the genebank sequence files",
        # default_value="/usr/src/data_folder/phage_view_data/genbank_spbetaviruses",
        default_value="/usr/src/data_folder/jaka_data/genbank",
    ),
}


@asset(
    config_schema={**genbank_folder_config},
    # config_schema={**path_config},
    description="Select for Spbetaviruses with complete genome sequence",
    compute_kind="Biopython",
    metadata={"owner": "Virginie Grosboillot"},
)
def sequence_sorting(context, fetch_genome) -> List[str]:
    context.log.info(f"Number of genomes in download folder: {len(fetch_genome)}")
    # context.log.info(f"Files: {fetch_genome}")

    complete_sequences = []
    for file in fetch_genome:
        for p in SeqIO.parse(file, "gb"):
            if re.search("complete genome", p.description):
                complete_sequences.append(file)

    context.log.info(f"Number of complete sequences: {len(complete_sequences)}")

    bacillus_sub_sequences = []
    for file in complete_sequences:
        for p in SeqIO.parse(file, "gb"):
            for feature in p.features:
                if feature.type == "source":
                    for v in feature.qualifiers.values():
                        if re.search("Bacillus subtilis", v[0]):
                            bacillus_sub_sequences.append(file)

    context.log.info(
        f"Number of Bacillus subtilis sequences: {len(bacillus_sub_sequences)}"
    )

    genes_in_sequences = []
    for file in bacillus_sub_sequences:
        for p in SeqIO.parse(file, "gb"):
            if set(["gene"]).issubset(set([type_f.type for type_f in p.features])):
                genes_in_sequences.append(file)

    context.log.info(
        f"Number of sequences with gene features: {len(genes_in_sequences)}"
    )

    for file in genes_in_sequences:
        shutil.copy2(
            file, f'{context.op_config["spbetaviruses_directory"]}/{Path(file).stem}.gb'
        )

    return list(
        map(
            lambda x: Path(x).stem,
            os.listdir(context.op_config["spbetaviruses_directory"]),
        )
    )


file_config = {
    "fs": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="fs",
    ),
}

sqc_folder_config = {
    "genbank_dir": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="genbank",
    ),
    "fasta_dir": Field(
        str,
        description="Path to folder containing the fasta sequence files",
        default_value="gene_identity/fasta",
    ),
}


@asset(
    config_schema={**sqc_folder_config},
    description="""Parse genebank file and create a file containing every genes in the fasta format.
    Note: The sequence start and stop indexes are `-1` on the fasta file 1::10  --> [0:10] included/excluded.""",
    compute_kind="Biopython",
    io_manager_key="io_manager",
    #auto_materialize_policy=AutoMaterializePolicy.eager(),
    metadata={"owner": "Virginie Grosboillot"},
)
def genbank_to_fasta(context, process_asset) -> str:
    context.log.info(f"The following file {process_asset} is being processed")

    # Paths to read and store the data
    path_in = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["genbank_dir"]]
    )
    path_out = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["fasta_dir"]]
    )
    context.log.info(path_in)
    context.log.info(path_out)

    os.makedirs(path_out, exist_ok=True)

    # Genbank to fasta
    file = f"{path_in}/{Path(process_asset).stem}.gb"
    context.log.info(file)
    output_dir = f"{path_out}/{Path(file).stem}.fna"
    context.log.info(output_dir)
    genome = SeqIO.read(file, "genbank")
    genome_records = list(SeqIO.parse(file, "genbank"))

    with open(output_dir, "w") as f:
        gene_features = list(filter(lambda x: x.type == "gene", genome.features))
        for feature in gene_features:
            for seq_record in genome_records:
                f.write(
                    ">%s | %s | %s | %s | %s | %s\n%s\n"
                    % (
                        seq_record.name,
                        seq_record.id,
                        seq_record.description,
                        feature.qualifiers["gene"][0]
                        if "gene" in feature.qualifiers.keys()
                        else "None",
                        feature.qualifiers["locus_tag"][0],
                        feature.location,
                        seq_record.seq[feature.location.start : feature.location.end],
                    )
                )

    fasta_files = list(
        map(
            lambda x: Path(x).stem,
            os.listdir(path_out),
        )
    )

    time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"The list of genbank files has been updated {time.isoformat()} (UTC).",
            "processed_file": process_asset,
            "path": path_out,
            "num_files": len(fasta_files),
            "preview": fasta_files,
        }
    )

    return output_dir


blastn_folder_config = {
    "blast_db_dir": Field(
        str,
        description="Path to folder containing the database for the blastn",
        default_value="gene_identity/blastn_database",
    ),
    "blastn_dir": Field(
        str,
        description="Path to folder containing the blastn output files",
        default_value="gene_identity/blastn",
    ),
}


@asset(
    config_schema={**sqc_folder_config, **blastn_folder_config},
    description="Receive a fasta file as input and create a database for blast in the output directory",
    compute_kind="Blastn",
    #auto_materialize_policy=AutoMaterializePolicy.eager(),
    metadata={"owner": "Virginie Grosboillot"},
)
def create_blast_db(context, genbank_to_fasta):
    path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["blast_db_dir"]]
    )
    context.log.info(path)
    os.makedirs(path, exist_ok=True)
    # db = []
    # for input in genbank_to_fasta:
    output_dir = f"{path}/{Path(genbank_to_fasta).stem}"
    context.log.info(output_dir)
    os.system(
        f"makeblastdb -in {genbank_to_fasta} -input_type fasta -dbtype nucl -out {output_dir}"
    )
    context.log.info("finished process")
    # db.append(output_dir)

    db = set(map(lambda x: f"{path}/{Path(x).stem}", os.listdir(path)))

    context.log.info(f"list of db: {set([Path(p).stem for p in db])}")

    return db


@asset(
    config_schema={**sqc_folder_config, **blastn_folder_config},
    description="Perform blastn between sequence and database and return results as json",
    compute_kind="Blastn",
    #auto_materialize_policy=AutoMaterializePolicy.eager(),
    metadata={"owner": "Virginie Grosboillot"},
)
def get_blastn(context, genbank_to_fasta, create_blast_db):
    path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["blastn_dir"]]
    )
    context.log.info(path)
    os.makedirs(path, exist_ok=True)

    blastn_files = []
    # for query in genbank_to_fasta:
    context.log.info(f"Query {genbank_to_fasta}")
    for database in create_blast_db:
        context.log.info(f"Database {database}")
        output_dir = f"{path}/{Path(genbank_to_fasta).stem}_vs_{Path(database).stem}"
        context.log.info(f"Output directory {output_dir}")
        os.system(
            f"blastn -query {genbank_to_fasta} -db {database} -evalue 1e-3 -dust no -out {output_dir} -outfmt 15"
        )
        blastn_files.append(output_dir)
        context.log.info(f"{Path(output_dir)} processed successfully")

        full_list = set(map(lambda x: f"{path}/{Path(x).stem}", os.listdir(path)))
    return full_list


table_config = {
    "output_folder": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="table",
    ),
}

blastn_summary_config = {
    "name": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="blastn_summary",
    ),
}


@multi_asset(
    config_schema={**file_config, **table_config, **blastn_summary_config},
    outs={
        "parse_blastn": AssetOut(
            is_required=True,
            description="Extract blastn information and save them into a Dataframe",
            #auto_materialize_policy=AutoMaterializePolicy.eager(),
            io_manager_key="parquet_io_manager",
            metadata={
                "output_folder": "table",
                "name": "blastn_summary",
                "parquet_managment": "append",
                "owner": "Virginie Grosboillot",
            },
        ),
        "history": AssetOut(
            is_required=True,
            description="keep track of processed files",
            #auto_materialize_policy=AutoMaterializePolicy.eager(),
            io_manager_key="io_manager",
            metadata={
                "owner": "Virginie Grosboillot",
            },
        ),
    },
    compute_kind="Pyspark",
)
def parse_blastn(context, get_blastn):  # -> tuple([DataFrame, List[str]]):
    # Check for not yet processed files:
    full_list = get_blastn

    history_path = "/".join(
        [
            os.getenv(EnvVar("PHAGY_DIRECTORY")),
            context.op_config["fs"],
            "history",
        ]
    )
    if os.path.exists(history_path):
        context.log.info("path exist")
        history = pickle.load(open(history_path, "rb"))
    else:
        history = []

    files_to_process = list(set(full_list).difference(history))

    # path = "/".join(
    #     [
    #         os.getenv(EnvVar("PHAGY_DIRECTORY")),
    #         context.op_config["output_folder"],
    #         context.op_config["name"],
    #     ]
    # )
    # Instantiate the SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Parse the json file to return a DataFrame
    # for json_file in get_blastn:
    json_file = files_to_process[0]
    context.log.info(f"File in process: {json_file}")
    df = (
        spark.read.option("multiline", "true")
        .json(json_file)
        .select(F.explode("BlastOutput2.report.results.search").alias("search"))
    )

    keys_list = ["query_id", "query_title", "query_len", "message", "hits"]
    keys_no_message = ["query_id", "query_title", "query_len", "hits"]
    hits_list = ["num", "description", "len", "hsps"]
    description_item = "title"
    hsps_list = [
        "num",
        "evalue",
        "identity",
        "query_from",
        "query_to",
        "query_strand",
        "hit_from",
        "hit_to",
        "hit_strand",
        "align_len",
        "gaps",
    ]

    try:
        df = (
            reduce(
                lambda df, i: df.withColumn(i, F.col("hsps").getItem(i)[0]),
                [i for i in hsps_list],
                reduce(
                    lambda df, i: df.withColumn(i, F.col("hits").getItem(i)[0]),
                    [i for i in hits_list],
                    reduce(
                        lambda df, i: df.withColumn(i, F.col("search").getItem(i)),
                        [i for i in keys_list],
                        df,
                    ).filter(F.col("message").isNull()),
                )
                .withColumn("source", F.col("description").getItem(description_item)[0])
                .withColumnRenamed("num", "number_of_hits"),
            )
            .drop("search", "hits", "description", "hsps")
            .withColumns(
                {
                    "query_genome_name": F.regexp_extract("query_title", r"^\w+", 0),
                    "query_genome_id": F.regexp_extract("query_title", r"\w+\.\d", 0),
                    "query_gene": F.regexp_extract("query_title", r"\| (\w+) \|", 1),
                    "query_locus_tag": F.regexp_extract(
                        "query_title", r" (\w+) \| \[", 1
                    ),
                    "query_start_end": F.regexp_extract(
                        "query_title", r"(\[\d+\:\d+\])", 0
                    ),
                    "query_gene_strand": F.regexp_extract(
                        "query_title", r"(\((\+|\-)\))", 0
                    ),
                }
            )
            .withColumns(
                {
                    "source_genome_name": F.regexp_extract("source", r"^\w+", 0),
                    "source_genome_id": F.regexp_extract("source", r"\w+\.\d", 0),
                    "source_gene": F.regexp_extract("source", r"\| (\w+) \|", 1),
                    "source_locus_tag": F.regexp_extract("source", r" (\w+) \| \[", 1),
                    "source_start_end": F.regexp_extract(
                        "source", r"(\[\d+\:\d+\])", 0
                    ),
                    "source_gene_strand": F.regexp_extract(
                        "source", r"(\((\+|\-)\))", 0
                    ),
                }
            )
            .withColumn(
                "percentage_of_identity",
                F.round(F.col("identity") / F.col("align_len") * 100, 3),
            )
        )
        # .coalesce(1).write.mode("append").parquet(path)
        context.log.info(f"File processed: {json_file}")
        # context.add_output_metadata(
        #     metadata={
        #         "text_metadata": "The blastn_summary parquet file has been updated",
        #         "processed_file": json_file,
        #         "path": "/".join(
        #             [
        #                 os.getenv(EnvVar("PHAGY_DIRECTORY")),
        #                 context.op_config["output_folder"],
        #                 context.op_config["name"],
        #             ]
        #         ),
        #     #         "path2": os.path.abspath(__file__),
        #     #         # "path": path,
        #         "num_records": df.count(),  # Metadata can be any key-value pair
        #         "preview": MetadataValue.md(df.toPandas().head().to_markdown()),
        #     #         # The `MetadataValue` class has useful static methods to build Metadata
        #         }
        #     )
        return df, history.append(json_file)

    except:
        df = (
            reduce(
                lambda df, i: df.withColumn(i, F.col("hsps").getItem(i)[0]),
                [i for i in hsps_list],
                reduce(
                    lambda df, i: df.withColumn(i, F.col("hits").getItem(i)[0]),
                    [i for i in hits_list],
                    reduce(
                        lambda df, i: df.withColumn(i, F.col("search").getItem(i)),
                        [i for i in keys_no_message],
                        df,
                    ),
                )
                .withColumn("source", F.col("description").getItem(description_item)[0])
                .withColumnRenamed("num", "number_of_hits"),
            )
            .drop("search", "hits", "description", "hsps")
            .withColumns(
                {
                    "query_genome_name": F.regexp_extract("query_title", r"^\w+", 0),
                    "query_genome_id": F.regexp_extract("query_title", r"\w+\.\d", 0),
                    "query_gene": F.regexp_extract("query_title", r"\| (\w+) \|", 1),
                    "query_locus_tag": F.regexp_extract(
                        "query_title", r" (\w+) \| \[", 1
                    ),
                    "query_start_end": F.regexp_extract(
                        "query_title", r"(\[\d+\:\d+\])", 0
                    ),
                    "query_gene_strand": F.regexp_extract(
                        "query_title", r"(\((\+|\-)\))", 0
                    ),
                }
            )
            .withColumns(
                {
                    "source_genome_name": F.regexp_extract("source", r"^\w+", 0),
                    "source_genome_id": F.regexp_extract("source", r"\w+\.\d", 0),
                    "source_gene": F.regexp_extract("source", r"\| (\w+) \|", 1),
                    "source_locus_tag": F.regexp_extract("source", r" (\w+) \| \[", 1),
                    "source_start_end": F.regexp_extract(
                        "source", r"(\[\d+\:\d+\])", 0
                    ),
                    "source_gene_strand": F.regexp_extract(
                        "source", r"(\((\+|\-)\))", 0
                    ),
                }
            )
            .withColumn(
                "percentage_of_identity",
                F.round(F.col("identity") / F.col("align_len") * 100, 3),
            )
        )
        # .coalesce(1).write.mode("append").parquet(path)
        context.log.info(f"File processed: {json_file}")

        # context.add_output_metadata(
        #     metadata={
        #         "text_metadata": "The blastn_summary parquet file has been updated",
        #         "processed_file": json_file,
        #         "path": "/".join(
        #             [
        #                 os.getenv(EnvVar("PHAGY_DIRECTORY")),
        #                 context.op_config["output_folder"],
        #                 context.op_config["name"],
        #             ]
        #         ),
        #     #         "path2": os.path.abspath(__file__),
        #     #         # "path": path,
        #         "num_records": df.count(),  # Metadata can be any key-value pair
        #         "preview": MetadataValue.md(df.toPandas().head().to_markdown()),
        #     #         # The `MetadataValue` class has useful static methods to build Metadata
        #         }
        #     )

        return df, history.append(json_file)


# @asset
# def processed_blastn_history(context, history):
#     return history


locus_and_gene_folder_config = {
    "output_folder": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="table",
    ),
    "name": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="locus_and_gene",
    ),
}


@asset(
    config_schema={
        **sqc_folder_config,
    },
    description="Create a dataframe containing the information relative to gene and save it as parquet file",
    #auto_materialize_policy=AutoMaterializePolicy.eager(),
    io_manager_key="parquet_io_manager",
    compute_kind="Biopython",
    metadata={
        "output_folder": "table",
        "name": "locus_and_gene",
        "parquet_managment": "append",
        "owner": "Virginie Grosboillot",
    },
)
def extract_locus_tag_gene(context, process_asset):
    """Create a dataframe containing the information relative to gene and save it as parquet file"""

    def _assess_file_content(genome) -> bool:
        """Assess wether the genbank file contains gene or only CDS"""

        gene_count = 0
        gene_value = False
        for feature in genome.features:
            if feature.type == "gene":
                gene_count = gene_count + 1
                if gene_count > 1:
                    gene_value = True
                    break

        return gene_value

    spark = SparkSession.builder.getOrCreate()

    # output_file = context.op_config["locus_and_gene_directory"]

    # output_file = "/".join(
    #     [
    #         os.getenv(EnvVar("PHAGY_DIRECTORY")),
    #         context.op_config["output_folder"],
    #         context.op_config["name"],
    #     ]
    # )
    path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["genbank_dir"]]
    )

    # for file in glob.glob(f"{path}/*.gb"):
    # for acc in list_genbank_files:
    file = f"{path}/{Path(process_asset).stem}.gb"

    gene_list = []
    locus_tag_list = []

    record = SeqIO.read(file, "genbank")

    gene_value = _assess_file_content(record)

    if gene_value == True:
        for f in record.features:
            if f.type == "gene":
                if "locus_tag" in f.qualifiers:
                    locus_tag_list.append(f.qualifiers["locus_tag"][0])
                else:
                    locus_tag_list.append("")
                if "gene" in f.qualifiers:
                    gene_list.append(f.qualifiers["gene"][0])
                else:
                    gene_list.append("")

        assert len(gene_list) == len(locus_tag_list), "Error Fatal!"

        df = (
            spark.createDataFrame(
                [[record.name, g, l] for g, l in zip(gene_list, locus_tag_list)],
                ["name", "gene", "locus_tag"],
            )
            # .coalesce(1)
            # .write.mode("append")
            # .parquet(output_file)
        )

    else:
        for f in record.features:
            if f.type == "CDS":
                if "protein_id" in f.qualifiers:
                    locus_tag_list.append(f.qualifiers["protein_id"][0][:-2])
                    gene_list.append(f.qualifiers["protein_id"][0][:-2])
                else:
                    locus_tag_list.append("")
                    gene_list.append("")

        assert len(gene_list) == len(locus_tag_list), "Error Fatal!"

        df = (
            spark.createDataFrame(
                [[record.name, g, l] for g, l in zip(gene_list, locus_tag_list)],
                ["name", "gene", "locus_tag"],
            )
            # .coalesce(1)
            # .write.mode("append")
            # .parquet(output_file)
        )

    # return f"{output_file} has been updated"
    return df


# gene_uniqueness_folder_config = {
#     "output_folder": Field(
#         str,
#         description="Path to folder where the files will be saved",
#         default_value="tables",
#     ),
#     "name": Field(
#         str,
#         description="Path to folder where the files will be saved",
#         default_value="gene_uniqueness",
#     ),
# }
gene_uniqueness_folder_config = {
    "output_folder": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="table",
    ),
    "name": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="gene_uniqueness",
    ),
}


@asset(
    config_schema={
        **gene_uniqueness_folder_config,
    },
    description="Create a datframe with all the query to a same genome and save result as parquet file",
    #auto_materialize_policy=AutoMaterializePolicy.eager(),
    io_manager_key="parquet_io_manager",
    compute_kind="Pyspark",
    metadata={
        "output_folder": "table",
        "name": "gene_uniqueness",
        "parquet_managment": "overwrite",
        "owner": "Virginie Grosboillot",
    },
)
def gene_presence_table(context, extract_locus_tag_gene, parse_blastn):  # parse_blastn
    """Create a datframe with all the query to a same genome and save result as parquet"""

    spark = SparkSession.builder.getOrCreate()
    # output_file = context.op_config["gene_uniqueness_directory"]

    # Load data
    # full_locus_df = spark.read.parquet(context.op_config["locus_and_gene_directory"])
    full_locus_df = extract_locus_tag_gene
    context.log.info(f"Loaded: locus_and_gene dataframe")

    # blastn_df = spark.read.parquet(context.op_config["blastn_summary_dataframe"])
    blastn_df = parse_blastn
    context.log.info(f"Loaded: blastn dataframe")

    # Column selector
    _query_col = [c for c in blastn_df.columns if c.startswith("query_")]
    _source_col = [c for c in blastn_df.columns if c.startswith("source_")]
    all_df = full_locus_df.join(
        blastn_df.select(*_query_col, *_source_col)
        .withColumnRenamed("query_genome_name", "name")
        .withColumnRenamed("query_locus_tag", "locus_tag"),
        ["name", "locus_tag"],
        "left",
    )

    # all_df.coalesce(1).write.mode("append").parquet(output_file)
    context.add_output_metadata(
        metadata={
            "text_metadata": "The 'gene_uniqueness' table has been re-computed'",
            "path": "/".join(
                [
                    os.getenv(EnvVar("PHAGY_DIRECTORY")),
                    context.op_config["output_folder"],
                    context.op_config["name"],
                ]
            ),
            "num_records": all_df.count(),
            "preview": MetadataValue.md(all_df.toPandas().head().to_markdown()),
        }
    )

    return all_df
