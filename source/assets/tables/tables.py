from dagster import (
    op,
    Out,
    Config,
    asset,
    Field,
    multi_asset,
    AssetOut,
    MetadataValue,
    EnvVar,
)

import os
import pickle

from Bio import SeqIO
from pathlib import Path
from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as F


dir_config = {
    "fs": Field(
        str,
        description="Path to folder containing the genbank files",
        default_value="fs",
    ),
    "tables": Field(
        str,
        description="Path to folder where the files will be saved",
        default_value="tables",
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
    config_schema={**dir_config, **blastn_summary_config},
    outs={
        "parse_blastn": AssetOut(
            is_required=True,
            description="Extract blastn information from json file and save them into a pyspark Dataframe",
            io_manager_key="parquet_io_manager",
            metadata={
                "tables": "table",
                "name": "blastn_summary",
                "parquet_managment": "append",
                "owner": "Virginie Grosboillot",
            },
        ),
        "history": AssetOut(
            is_required=True,
            description="Keep track of processed files",
            # auto_materialize_policy=AutoMaterializePolicy.eager(),
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
        context.log.info(f"History: {history}")
    else:
        context.log.info("path does not exist")
        history = []

    # context.log.info(f"History: {history}")

    files_to_process = list(set(full_list).difference(history))
    # context.log.info(f"History: {files_to_process}")

    path = "/".join(
        [
            os.getenv(EnvVar("PHAGY_DIRECTORY")),
            context.op_config["tables"],
            context.op_config["name"],
        ]
    )
    # Instantiate the SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Parse the json file to return a DataFrame
    # for json_file in get_blastn:
    if files_to_process:
        #json_file = files_to_process[0]
        new_files = []
        # context.log.info(f"history: {history}")
        # context.log.info(f"type history: {type(history)}")
        
        for json_file in files_to_process:

            context.log.info(f"File in process: {json_file}")

            context.log.info(f"Updated history: {history}")

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
                        .withColumn(
                            "source", F.col("description").getItem(description_item)[0]
                        )
                        .withColumnRenamed("num", "number_of_hits"),
                    )
                    .drop("search", "hits", "description", "hsps")
                    .withColumns(
                        {
                            "query_genome_name": F.regexp_extract(
                                "query_title", r"^\w+", 0
                            ),
                            "query_genome_id": F.regexp_extract(
                                "query_title", r"\w+\.\d", 0
                            ),
                            "query_gene": F.regexp_extract(
                                "query_title", r"\| (\w+) \|", 1
                            ),
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
                            "source_locus_tag": F.regexp_extract(
                                "source", r" (\w+) \| \[", 1
                            ),
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
                ).coalesce(1).write.mode("append").parquet(path)
                # context.log.info(f"File processed: {json_file}")
                # context.add_output_metadata(
                #     metadata={
                #         "text_metadata": "The blastn_summary parquet file has been updated",
                #         "processed_file": json_file,
                #         "path": "/".join(
                #             [
                #                 os.getenv(EnvVar("PHAGY_DIRECTORY")),
                #                 context.op_config["tables"],
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
                # time = datetime.now()
                # context.add_output_metadata(
                #     output_name="parse_blastn",
                #     metadata={
                #         "text_metadata": f"The blastn_summary parquet file has been updated {time.isoformat()} (UTC).",
                #         "processed_file": Path(json_file).stem,
                #         "path": "/".join(
                #             [
                #                 os.getenv(EnvVar("PHAGY_DIRECTORY")),
                #                 context.op_config["tables"],
                #                 context.op_config["name"],
                #             ]
                #         ),
                #         "num_records": df.count(),
                #         "preview": MetadataValue.md(df.toPandas().head().to_markdown()),
                #     },
                # )
                # context.add_output_metadata(
                #     output_name="history",
                #     metadata={
                #         "text_metadata": f"A new json file has been processed {time.isoformat()} (UTC).",
                #         "processed_file": Path(json_file).stem,
                #         "path": history_path,
                #         "num_files": len(history),
                #         "preview": history,
                #     },
                # )
                # return df, history
                # return df
            
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
                        .withColumn(
                            "source", F.col("description").getItem(description_item)[0]
                        )
                        .withColumnRenamed("num", "number_of_hits"),
                    )
                    .drop("search", "hits", "description", "hsps")
                    .withColumns(
                        {
                            "query_genome_name": F.regexp_extract(
                                "query_title", r"^\w+", 0
                            ),
                            "query_genome_id": F.regexp_extract(
                                "query_title", r"\w+\.\d", 0
                            ),
                            "query_gene": F.regexp_extract(
                                "query_title", r"\| (\w+) \|", 1
                            ),
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
                            "source_locus_tag": F.regexp_extract(
                                "source", r" (\w+) \| \[", 1
                            ),
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
                ).coalesce(1).write.mode("append").parquet(path)
                # context.log.info(f"File processed: {json_file}")

                # context.add_output_metadata(
                #     metadata={
                #         "text_metadata": "The blastn_summary parquet file has been updated",
                #         "processed_file": json_file,
                #         "path": "/".join(
                #             [
                #                 os.getenv(EnvVar("PHAGY_DIRECTORY")),
                #                 context.op_config["tables"],
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
                
                # return df, history
                # return df

            context.log.info(f"DataFRame has been generated")


            new_files.append(Path(json_file).stem)

        history = history + new_files

    df = spark.read.parquet('data_folder/experimenting/table/blastn_summary')

    time = datetime.now()
    context.add_output_metadata(
        output_name="parse_blastn",
        metadata={
            "text_metadata": f"The blastn_summary parquet file has been updated {time.isoformat()} (UTC).",
            "processed_files": new_files,
            "path": "/".join(
                [
                    os.getenv(EnvVar("PHAGY_DIRECTORY")),
                    context.op_config["tables"],
                    context.op_config["name"],
                ]
            ),
            "num_records": df.count(),
            "preview": MetadataValue.md(df.toPandas().head().to_markdown()),
        },
    )
    context.add_output_metadata(
        output_name="history",
        metadata={
            "text_metadata": f"A new json file has been processed {time.isoformat()} (UTC).",
            "path": history_path,
            "num_files": len(history),
            "preview": history,
        },
    )

    return 'DataFrame has been updated', history   

        

        

    # @asset
    # def processed_blastn_history(context, history):
    #     return history


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
        **dir_config,
        **sqc_folder_config,
    },
    description="Create a dataframe containing the information relative to gene and save it as parquet file",
    # auto_materialize_policy=AutoMaterializePolicy.eager(),
    io_manager_key="parquet_io_manager",
    compute_kind="Biopython",
    op_tags={"blaster": "compute_intense"},
    metadata={
        "tables": "table",
        "name": "locus_and_gene",
        "parquet_managment": "append",
        "owner": "Virginie Grosboillot",
    },
)
def extract_locus_tag_gene(context, standardised_ext_file):
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
    #         context.op_config["tables"],
    #         context.op_config["name"],
    #     ]
    # )
    # path = "/".join(
    #     [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["genbank_dir"]]
    # )

    # for file in glob.glob(f"{path}/*.gb"):
    # for acc in list_genbank_files:
    file = standardised_ext_file

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
        **dir_config,
        **gene_uniqueness_folder_config,
    },
    description="Create a datframe with all the query to a same genome and save result as parquet file",
    # auto_materialize_policy=AutoMaterializePolicy.eager(),
    io_manager_key="parquet_io_manager",
    compute_kind="Pyspark",
    metadata={
        "tables": "table",
        "name": "gene_uniqueness",
        "parquet_managment": "overwrite",
        "owner": "Virginie Grosboillot",
    },
)
def gene_presence_table(context, extract_locus_tag_gene, parse_blastn):  # parse_blastn
    """Create a datframe with all the query to a same genome and save result as parquet"""

    # spark = SparkSession.builder.getOrCreate()
    # output_file = context.op_config["gene_uniqueness_directory"]

    # Load data
    # full_locus_df = spark.read.parquet(context.op_config["locus_and_gene_directory"])
    full_locus_df = extract_locus_tag_gene
    context.log.info("Loaded: locus_and_gene dataframe")

    # blastn_df = spark.read.parquet(context.op_config["blastn_summary_dataframe"])
    blastn_df = parse_blastn
    context.log.info("Loaded: blastn dataframe")

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
                    context.op_config["tables"],
                    context.op_config["name"],
                ]
            ),
            "num_records": all_df.count(),
            "preview": MetadataValue.md(all_df.toPandas().head().to_markdown()),
        }
    )

    return all_df
