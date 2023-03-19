from dagster import asset, Field

import os
import glob
import re
import shutil

from Bio import SeqIO
from pathlib import Path
from functools import reduce
from typing import List


genbank_folder_config = {
    "phage_download_directory": Field(str, description="Path to folder containing the genebank sequence files", default_value="/usr/src/data_folder/genome_download"),
    "spbetaviruses_directory": Field(str, description="Path to folder containing the genebank sequence files", default_value="/usr/src/data_folder/genbank_spbetaviruses"),
}

@asset(
    config_schema={**genbank_folder_config},
    description='Select for Spbetaviruses with complete genome sequence',
    compute_kind="Biopython",
    metadata={"owner" : "Virginie Grosboillot"},
)
def sequence_sorting(context, fetch_genome) -> List[str]:

    context.log.info(f"Number of genomes in download folder: {len(fetch_genome)}")
    #context.log.info(f"Files: {fetch_genome}")

    complete_sequences = []
    for file in fetch_genome:
        for p in SeqIO.parse(file, 'gb'):
            if re.search('complete genome', p.description):
                complete_sequences.append(file)

    context.log.info(f"Number of complete sequences: {len(complete_sequences)}")

    bacillus_sub_sequences = []
    for file in complete_sequences:
        for p in SeqIO.parse(file, 'gb'):
            for feature in p.features:
                if (feature.type == 'source'):
                    for v in feature.qualifiers.values():
                        if re.search('Bacillus subtilis', v[0]):
                            bacillus_sub_sequences.append(file)

    context.log.info(f"Number of Bacillus subtilis sequences: {len(bacillus_sub_sequences)}")

    genes_in_sequences = []
    for file in bacillus_sub_sequences:
        for p in SeqIO.parse(file, 'gb'):
            if set(['gene']).issubset(set([type_f.type for type_f in p.features])):
                genes_in_sequences.append(file)

    context.log.info(f"Number of sequences with gene features: {len(genes_in_sequences)}")

    for file in genes_in_sequences:
        shutil.copy2(file, f'{context.op_config["spbetaviruses_directory"]}/{Path(file).stem}.gb')
    
    return list(map(lambda x: Path(x).stem, os.listdir(context.op_config["spbetaviruses_directory"])))


fasta_folder_config = {
    "fasta_directory": Field(str, description="Path to folder containing the fasta sequence files", default_value="/usr/src/data_folder/gene_identity/fasta"),
}

@asset(
    config_schema={**genbank_folder_config, **fasta_folder_config},
    description="""Parse genebank file and create a file containing every genes in the fasta format.
    Note: The sequence start and stop indexes are `-1` on the fasta file 1::10  --> [0:10] included/excluded.""",
    compute_kind="Biopython",
    metadata={"owner" : "Virginie Grosboillot"},
)
def genbank_to_fasta(context, sequence_sorting) -> List[str]:

    # Check files that have already been processed


    # Process new files
    context.log.info(f"Number of file to process: {len(sequence_sorting)}")

    path = context.op_config["spbetaviruses_directory"]
    fasta_files = []
    for acc in sequence_sorting:
        file = f'{path}/{acc}.gb'
        output = f'{context.op_config["fasta_directory"]}/{Path(file).stem}.fna'
        fasta_files.append(output)

        genome = SeqIO.read(file, "genbank")
        genome_records = list(SeqIO.parse(file, "genbank"))

        with open(output, "w") as f:
            gene_features = list(filter(lambda x: x.type == "gene", genome.features))
            for feature in gene_features:
                for seq_record in genome_records:
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
    #fasta_files = [file for file in glob.glob(f'{context.op_config["fasta_directory"]}/*.fna')]
    context.log.info(f"Number of file processed: {len(fasta_files)}")
    #return list(map(lambda x: x, os.listdir(context.op_config["fasta_directory"])))
    return fasta_files



blastn_folder_config = {
    "blast_db_directory": Field(str, description="Path to folder containing the database for the blastn", default_value="/usr/src/data_folder/gene_identity/blastn_database"),
    "blastn_directory": Field(str, description="Path to folder containing the blastn output files", default_value="/usr/src/data_folder/gene_identity/blastn"),
}

@asset(
    config_schema={**fasta_folder_config, **blastn_folder_config},
    description="Receive a fasta file as input and create a database for blast in the output directory",
    compute_kind="Blastn",
    metadata={"owner" : "Virginie Grosboillot"},
)
def create_blast_db(context, genbank_to_fasta):
    db = []
    for input in genbank_to_fasta:
        output = f'{context.op_config["blast_db_directory"]}/{Path(input).stem}'
        os.system(
            f'makeblastdb -in {input} -input_type fasta -dbtype nucl -out {output}'
        )
        db.append(output)
    return db