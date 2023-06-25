import os

from Bio import SeqIO
from Bio.SeqRecord import SeqRecord
from pathlib import Path
from functools import reduce

from pyspark.sql import DataFrame

import pyspark.sql.functions as F



def assess_file_content(genome: SeqRecord):
    """Assess wether the genbank file contains gene or only CDS"""

    gene_count = 0
    gene_value = False

    for feature in genome.features:
            if feature.type == "gene":
                gene_count = gene_count+1
                if gene_count > 1:
                    gene_value = True
                    break
    
    return gene_value


def genbank_to_fasta(input: str, output: str='None'):
    """Parse genebank file and create a file containing every genes in the fasta format.
    Note: The sequence start and stop indexes are `-1` on the fasta file 1::10  --> [0:10] included/excluded."""

    if output == 'None':
        output = f'{Path(input).parent}/{Path(input).stem}.fna'
    else:
        output = f'{output}/{Path(input).stem}.fna'

    genome = SeqIO.read(input, "genbank")

    gene_value = assess_file_content(genome)

    if gene_value == True:
        with open(output, "w") as f:
            for feature in genome.features:
                if feature.type == "gene":
                    for seq_record in SeqIO.parse(input, "genbank"):
                        f.write(
                            ">%s | %s | %s | %s | %s | %s\n%s\n"
                            % (
                                seq_record.name,
                                seq_record.id,
                                seq_record.description,
                                feature.qualifiers["gene"][0] if 'gene' in feature.qualifiers.keys() else 'None',
                                feature.qualifiers["locus_tag"][0],
                                feature.location,
                                seq_record.seq[
                                    feature.location.start : feature.location.end
                                ],
                            )
                        )

    else:
        with open(output, "w") as f:
            for feature in genome.features:
                if feature.type == "CDS":
                    for seq_record in SeqIO.parse(input, "genbank"):
                        f.write(
                            ">%s | %s | %s | %s | %s | %s\n%s\n"
                            % (
                                seq_record.name,
                                seq_record.id,
                                seq_record.description,
                                feature.qualifiers["protein_id"][0][:-2],
                                feature.qualifiers["protein_id"][0][:-2],
                                feature.location,
                                seq_record.seq[
                                    feature.location.start : feature.location.end
                                ],
                            )
                        )

    return f"File {output} has been written."


# def multiple_file_conversion(list_of_files: list, output_folder: str):
#     """Convert multiple genome sequences into the desired format"""
#     for file in list_of_files:
#         return genbank_to_fasta(file, f"{output_folder}_{file}.fna")


def create_blast_db(input: str, output_folder: str):
    """Receive a fasta file as input and create a database for blast in the output directory"""
    file_name = Path(input).stem
    return os.system(
        f"makeblastdb -in {input} -input_type fasta -dbtype nucl -out {output_folder}/{file_name}"
    )


def get_blastn(query: str, database: str, output: str):
    """Perform blastn between sequence and database and return results as json"""
    return os.system(
        f"blastn -query {query} -db {database} -evalue 1e-3 -dust no -out {output} -outfmt 15"
    )

#  for file in glob.glob('tests/fixtures/synthetic_data/fasta/*.fna'):
#    db_list = []
#    for db in glob.glob('tests/fixtures/synthetic_data/blast_db/*'):
#        path_db = Path(db).parent/Path(db).stem
#        if path_db  not in db_list:
#            print(path_db)
#            db_list.append(path_db)
#            BLT.get_blastn(file, path_db, f'tests/fixtures/synthetic_data/blast_results/{Path(file).stem}_vs_{Path(path_db).name}')
#        else:
#            print('already done!')
#            continue


def parse_blastn(spark, json_file: str, output: str="blastn_summary"):
    """Extract blastn information and save them into a Dataframe"""

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
        return (
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
                    "query_locus_tag": F.regexp_extract("query_title", r" (\w+) \| \[", 1),
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
                    "source_start_end": F.regexp_extract("source", r"(\[\d+\:\d+\])", 0),
                    "source_gene_strand": F.regexp_extract("source", r"(\((\+|\-)\))", 0),
                }
            )
            .withColumn('percentage_of_identity', F.round(F.col('identity')/F.col('align_len')*100, 3))
        ).coalesce(1).write.mode("append").parquet(output)

    except:
        return (
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
                    "query_locus_tag": F.regexp_extract("query_title", r" (\w+) \| \[", 1),
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
                    "source_start_end": F.regexp_extract("source", r"(\[\d+\:\d+\])", 0),
                    "source_gene_strand": F.regexp_extract("source", r"(\((\+|\-)\))", 0),
                }
            )
            .withColumn('percentage_of_identity', F.round(F.col('identity')/F.col('align_len')*100, 3))
        ).coalesce(1).write.mode("append").parquet(output)



def extract_locus_tag_gene(spark, input_file: str, output_file: str="locus_and_gene"):
    """Create a dataframe containing the information relative to gene and save it as parquet file"""

    gene_list = []
    locus_tag_list = []

    record = SeqIO.read(input_file, 'gb')

    gene_value = assess_file_content(record)

    if gene_value == True:
        for f in record.features:
            if f.type=="gene":
                if "locus_tag" in f.qualifiers:
                    locus_tag_list.append(f.qualifiers['locus_tag'][0])
                else:
                    locus_tag_list.append('')
                if "gene" in f.qualifiers:
                        gene_list.append(f.qualifiers['gene'][0])
                else:
                    gene_list.append('')

        assert len(gene_list) == len(locus_tag_list), 'Error Fatal!'

        return spark.createDataFrame([[record.name, g, l] for g, l in zip(gene_list, locus_tag_list)], ['name', 'gene', 'locus_tag']).coalesce(1).write.mode("append").parquet(output_file)

    else:
        for f in record.features:
            if f.type=="CDS":
                if "protein_id" in f.qualifiers:
                    locus_tag_list.append(f.qualifiers['protein_id'][0][:-2])
                    gene_list.append(f.qualifiers['protein_id'][0][:-2])
                else:
                    locus_tag_list.append('')
                    gene_list.append('')

        assert len(gene_list) == len(locus_tag_list), 'Error Fatal!'

        return spark.createDataFrame([[record.name, g, l] for g, l in zip(gene_list, locus_tag_list)], ['name', 'gene', 'locus_tag']).coalesce(1).write.mode("append").parquet(output_file)
         
                                

def gene_presence_table(spark, locus_input: str= "locus_and_gene", blastn_input: str= 'blastn_summary', output_file: str='gene_uniqueness'):
    """ Create a datframe with all the query to a same genome and save result as parquet"""

    # Load data
    full_locus_df = spark.read.parquet(locus_input)

    blastn_df = spark.read.parquet(blastn_input)

    # Column selector

    _query_col = [c for c in blastn_df.columns if c.startswith('query_')]
    _source_col = [c for c in blastn_df.columns if c.startswith('source_')]
    all_df = full_locus_df.join(blastn_df.select(*_query_col, *_source_col).withColumnRenamed('query_genome_name', 'name').withColumnRenamed('query_locus_tag', 'locus_tag'), ['name', 'locus_tag'], 'left')

    
    return all_df.coalesce(1).write.mode("append").parquet(output_file)

    # .filter(F.col('query_gene_locus').isNull()).count() gives the same results as number of message.notNull


def gene_uniqueness(spark, record_name: list, path_to_dataset: str='gene_uniqueness'):
    """Calculate percentage of the presence of a given gene over the displayed sequences"""

    gene_uniqueness_df = spark.read.parquet(path_to_dataset).filter((F.col('name').isin(record_name)) & (F.col('source_genome_name').isin(record_name)))
    total_seq = gene_uniqueness_df.select(F.count_distinct(F.col('query_genome_id')).alias('count')).collect()[0][0]
    return gene_uniqueness_df.withColumn('total_seq', F.lit(total_seq)).groupby('name', 'gene', 'locus_tag', 'total_seq').count().withColumn('perc_presence', (F.col('count')-1)/(F.col('total_seq')-1)*100)



def _get_match_rna_seq(spark, file: str='gene_uniqueness'):

    no_match_df = file.filter(F.col('query_genome_name').isNull()).select('name', 'locus_tag', 'gene')

    loci = [locus[0] for locus in no_match_df.select('locus_tag').toLocalIterator()]

    df = spark.read.option("delimiter", "|").option("multiligne", "true").option('lineSep', '>').option("ignoreLeadingWhiteSpace", "true").option("ignoreTrailingWhiteSpace", "true").csv('bacillus_genome_comparison/NC_022898.1.fna').filter(F.col('_c4').isin(loci))


    #df.select(F.concat_ws('|', *df.columns)).coalesce(1).write.option("lineSep", "\n").text('bacillus_genome_comparison/txt_trial')
    df.select(F.regexp_replace(F.concat_ws('|', *df.columns), r'^', r'>')).coalesce(1).write.option("lineSep", "\n").text('bacillus_genome_comparison/txt_trial')

    os.system(f'blastn -query {file} -db {database} -word_size 7 -evalue 1e3 -dust no -out {output} -outfmt 15')