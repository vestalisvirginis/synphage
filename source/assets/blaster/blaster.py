from dagster import (
    asset,
    Field,
    op,
    graph,
    In,
    Out,
    graph_asset,
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


# class FileConfig(Config):
#     filename: str


# @op
# def process_file(context, config: FileConfig):
#     """Print file name on console"""
#     context.log.info(config.filename)
#     return config.filename


# @graph_asset()
# def process_asset():
#     return process_file()


# @asset()
# def downstream_asset(context, process_asset):
#     context.log.info("Downstream processing")
#     return "TEST"

# class FileConfig(Config):
#     filename: str


# @op
# def process_file(context, config: FileConfig):
#     context.log.info(config.filename)


# @job
# def log_file_job():
#     process_file()


# @sensor(job=log_file_job)
# def my_directory_sensor():
#     for filename in os.listdir('./data_folder/experimenting/genbank/'):
#         filepath = os.path.join('./data_folder/experimenting/genbank/', filename)
#         if os.path.isfile(filepath):
#             yield RunRequest(
#             run_key=filename,
#             run_config=RunConfig(
#             ops={"process_file": FileConfig(filename=filename)}
#             ),
#             )

# @sensor(job=log_file_job)
# def my_directory_sensor_cursor(context):
#     last_mtime = float(context.cursor) if context.cursor else 0

#     max_mtime = last_mtime
#     for filename in os.listdir(MY_DIRECTORY):
#         filepath = os.path.join(MY_DIRECTORY, filename)
#         if os.path.isfile(filepath):
#             fstats = os.stat(filepath)
#             file_mtime = fstats.st_mtime
#             if file_mtime <= last_mtime:
#                 continue

#             # the run key should include mtime if we want to kick off new runs based on file modifications
#             run_key = f"{filename}:{file_mtime}"
#             run_config = {"ops": {"process_file": {"config": {"filename": filename}}}}
#             yield RunRequest(run_key=run_key, run_config=run_config)
#             max_mtime = max(max_mtime, file_mtime)

#     context.update_cursor(str(max_mtime))

# @op(
#     name = "assess_file_content",
#     ins={"genome": In(SeqRecord)},
#     out={'gene_value': Out(bool)},
#     description="Assess wether the genbank file contains gene or only CDS",
# )
# def _assess_file_content(genome) -> bool:
#     """Assess wether the genbank file contains gene or only CDS"""

#     gene_count = 0
#     gene_value = False
#     for feature in genome.features:
#             if feature.type == "gene":
#                 gene_count = gene_count+1
#                 if gene_count > 1:
#                     gene_value = True
#                     break

#     return gene_value


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


def _standardise_file_extention(file) -> None:
    """Change file extension when '.gbk' for '.gb'"""
    path = Path(file)
    if path.suffix == ".gbk":
        return path.rename(path.with_suffix(".gb"))


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

file_config = {
    "file": Field(
        str,
        description="File to be processed",
        default_value="genbank",
    ),
}


# @asset(
#     config_schema={**sqc_folder_config},
#     description="""List the sequences available in the genbank folder and return a list""",
#     compute_kind="Python",
#     io_manager_key="io_manager",
#     metadata={"owner": "Virginie Grosboillot"},
# )
# def list_genbank_files(context) -> List[str]:
#     files = list(
#         map(
#             lambda x: Path(x).stem,
#             os.listdir(
#                 "/".join(
#                     [
#                         os.getenv(EnvVar("PHAGY_DIRECTORY")),
#                         context.op_config["genbank_dir"],
#                     ]
#                 )
#             ),
#         )
#     )

#     time = datetime.now()
#     context.add_output_metadata(
#         metadata={
#             "text_metadata": f"List of genbank files {time.isoformat()} (UTC).",
#             "path": "/".join(
#                 [
#                     EnvVar("PHAGY_DIRECTORY"),
#                     context.op_config["genbank_dir"],
#                 ]
#             ),
#             "num_files": len(files),
#             "preview": files,
#         }
#     )

#     return files


# @asset(
#     config_schema={**sqc_folder_config},
#     description="""Keep last updated state of genbank folder""",
#     compute_kind="Python",
#     io_manager_key="io_manager",
#     metadata={"owner": "Virginie Grosboillot"},
# )
# def list_genbank_history(context, list_genbank_files) -> List[str]:
#     files = list_genbank_files

#     time = datetime.now()
#     context.add_output_metadata(
#         metadata={
#             "text_metadata": f"Last history status ({time.isoformat()} (UTC)).",
#             "num_files": len(files),
#             "preview": files,
#         }
#     )

#     return files


# @asset(
#     config_schema={**sqc_folder_config},
#     description="""Give a list of the new files""",
#     compute_kind="Python",
#     io_manager_key="io_manager",
#     metadata={"owner": "Virginie Grosboillot"},
# )
# def new_genbank_files(context, list_genbank_files, list_genbank_history) -> List[str]:

#     new_files = set(list_genbank_files).difference(list_genbank_history)

#     for file in new_files:
#         _rename_file_ext(file)

#     time = datetime.now()
#     context.add_output_metadata(
#         metadata={
#             "text_metadata": f"The list of genbank files has been updated {time.isoformat()} (UTC).",
#             "num_new_files": len(new_files),
#             "new_files": list(new_files),
#             "path": "/".join(
#                 [
#                     EnvVar("PHAGY_DIRECTORY"),
#                     context.op_config["genbank_dir"],
#                 ]
#             ),
#         }
#     )

#     return list(new_files)


@asset(
    config_schema={**sqc_folder_config, **file_config},
    description="""Parse genebank file and create a file containing every genes in the fasta format.
    Note: The sequence start and stop indexes are `-1` on the fasta file 1::10  --> [0:10] included/excluded.""",
    compute_kind="Biopython",
    io_manager_key="io_manager",
    metadata={"owner": "Virginie Grosboillot"},
)
def genbank_to_fasta(context) -> List[str]:
    # Check files that have already been processed
    list_genbank_files = context.op_config["file"]
    # Process new files
    # context.log.info(f"Number of file to process: {len(sequence_sorting)}")

    path_in = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["genbank_dir"]]
    )
    path_out = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["fasta_dir"]]
    )

    fasta_files = []
    for acc in list_genbank_files:
        file = f"{path_in}/{acc}.gb"
        output_dir = f"{path_out}/{Path(file).stem}.fna"
        fasta_files.append(output_dir)

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
                            seq_record.seq[
                                feature.location.start : feature.location.end
                            ],
                        )
                    )
    # fasta_files = [file for file in glob.glob(f'{context.op_config["fasta_directory"]}/*.fna')]
    # context.log.info(f"Number of file processed: {len(fasta_files)}")
    files = list(
        map(
            lambda x: Path(x).stem,
            os.listdir(path_out),
        )
    )

    time = datetime.now()
    context.add_output_metadata(
        metadata={
            "text_metadata": f"The list of genbank files has been updated {time.isoformat()} (UTC).",
            "num_processed_files": len(fasta_files),
            "path": path_in,
            "num_files": len(files),
            "preview": files,
        }
    )
    # return list(map(lambda x: x, os.listdir(context.op_config["fasta_directory"])))
    return fasta_files


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
    metadata={"owner": "Virginie Grosboillot"},
)
def create_blast_db(context, genbank_to_fasta):
    path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["blast_db_dir"]]
    )
    db = []
    for input in genbank_to_fasta:
        output_dir = f"{path}/{Path(input).stem}"
        os.system(
            f"makeblastdb -in {input} -input_type fasta -dbtype nucl -out {output_dir}"
        )
        db.append(output_dir)
    return db


@asset(
    config_schema={**sqc_folder_config, **blastn_folder_config},
    description="Perform blastn between sequence and database and return results as json",
    compute_kind="Blastn",
    metadata={"owner": "Virginie Grosboillot"},
)
def get_blastn(context, genbank_to_fasta, create_blast_db):
    path = "/".join(
        [os.getenv(EnvVar("PHAGY_DIRECTORY")), context.op_config["blastn_dir"]]
    )
    blastn_files = []
    for query in genbank_to_fasta:
        context.log.info(f"Query {query}")
        for database in create_blast_db:
            context.log.info(f"Database {database}")
            output_dir = f"{path}/{Path(query).stem}_vs_{Path(database).stem}"
            context.log.info(f"Output directory {output_dir}")
            os.system(
                f"blastn -query {query} -db {database} -evalue 1e-3 -dust no -out {output_dir} -outfmt 15"
            )
            blastn_files.append(output_dir)
            context.log.info(f"{Path(output_dir)} processed successfully")
    return blastn_files


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


@asset(
    config_schema={**table_config, **blastn_summary_config},
    description="Extract blastn information and save them into a Dataframe",
    # io_manager_key="parquet_io_manager",
    compute_kind="Pyspark",
    metadata={
        "output_folder": "table",
        "name": "blastn_summary",
        "owner": "Virginie Grosboillot",
    },
)
def parse_blastn(context, get_blastn) -> str:
    path = "/".join(
        [
            os.getenv(EnvVar("PHAGY_DIRECTORY")),
            context.op_config["output_folder"],
            context.op_config["name"],
        ]
    )
    # Instantiate the SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Parse the json file to return a DataFrame
    for json_file in get_blastn:
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
            (
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
            context.log.info(f"File processed: {json_file}")
            # context.add_output_metadata(
            #     metadata={
            #         "text_metadata": "The blastn_summary parquet file has been updated",
            #         "path": "/".join(
            #             [
            #                 EnvVar("PHAGY_DIRECTORY"),
            #                 context.op_config["output_folder"],
            #                 context.op_config["name"],
            #             ]
            #         ),
            #         "path2": os.path.abspath(__file__),
            #         # "path": path,
            #         "num_records": df.count(),  # Metadata can be any key-value pair
            #         "preview": MetadataValue.md(df.toPandas().head().to_markdown()),
            #         # The `MetadataValue` class has useful static methods to build Metadata
            #     }
            # )
            # return df

        except:
            (
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
            context.log.info(f"File processed: {json_file}")

        # context.add_output_metadata(
        #     metadata={
        #         "text_metadata": "The blastn_summary parquet file has been updated",
        #         "path": "/".join(
        #             [
        #                 EnvVar("PHAGY_DIRECTORY"),
        #                 context.op_config["output_folder"],
        #                 context.op_config["name"],
        #             ]
        #         ),
        #         "path2": os.path.abspath(__file__),
        #         # "path": path,
        #         "num_records": df.count(),  # Metadata can be any key-value pair
        #         "preview": MetadataValue.md(df.toPandas().head().to_markdown()),
        #         # The `MetadataValue` class has useful static methods to build Metadata
        #     }
        # )

    return f"Done"


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
        **genbank_folder_config,
        **locus_and_gene_folder_config,
        **file_config,
    },
    description="Create a dataframe containing the information relative to gene and save it as parquet file",
    # io_manager_key="parquet_io_manager",
    compute_kind="Biopython",
    metadata={
        "output_folder": "table",
        "name": "locus_and_gene",
        "owner": "Virginie Grosboillot",
    },
)
def extract_locus_tag_gene(context):
    """Create a dataframe containing the information relative to gene and save it as parquet file"""

    list_genbank_files = context.op_config["file"]

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

    output_file = "/".join(
        [
            os.getenv(EnvVar("PHAGY_DIRECTORY")),
            context.op_config["output_folder"],
            context.op_config["name"],
        ]
    )
    path = context.op_config["spbetaviruses_directory"]

    # for file in glob.glob(f"{path}/*.gb"):
    for acc in list_genbank_files:
        file = f"{path}/{acc}.gb"

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
                .coalesce(1)
                .write.mode("append")
                .parquet(output_file)
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
                .coalesce(1)
                .write.mode("append")
                .parquet(output_file)
            )

    return f"{output_file} has been updated"
    # return df


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
    "gene_uniqueness_directory": Field(
        str,
        description="Path to folder containing the parquet files containing the information on the uniqueness of the genes",
        default_value="/usr/src/data_folder/jaka_data/table/gene_uniqueness",
    ),
    "locus_and_gene_directory": Field(
        str,
        description="Path to folder containing the parquet files containing the information on the uniqueness of the genes",
        default_value="/usr/src/data_folder/jaka_data/table/locus_and_gene",
    ),
    "blastn_summary_dataframe": Field(
        str,
        description="Path to folder containing the parquet files containing the information on the uniqueness of the genes",
        default_value="/usr/src/data_folder/jaka_data/table/blastn_summary",
    ),
}


@asset(
    config_schema={
        **blastn_summary_config,
        **locus_and_gene_folder_config,
        **gene_uniqueness_folder_config,
    },
    description="Create a datframe with all the query to a same genome and save result as parquet file",
    compute_kind="Pyspark",
    metadata={"owner": "Virginie Grosboillot"},
)
def gene_presence_table(context, extract_locus_tag_gene, parse_blastn):  # parse_blastn
    """Create a datframe with all the query to a same genome and save result as parquet"""

    spark = SparkSession.builder.getOrCreate()
    output_file = context.op_config["gene_uniqueness_directory"]

    # Load data
    full_locus_df = spark.read.parquet(context.op_config["locus_and_gene_directory"])

    blastn_df = spark.read.parquet(context.op_config["blastn_summary_dataframe"])

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

    all_df.coalesce(1).write.mode("append").parquet(output_file)

    return f"{output_file} has been updated"


# return all_df
