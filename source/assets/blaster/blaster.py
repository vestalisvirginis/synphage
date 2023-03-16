from dagster import asset, Field

import os
import glob
import re
import shutil

from Bio import SeqIO
from pathlib import Path
from functools import reduce


genbank_folder_config = {
    "phage_download_directory": Field(str, description="Path to folder containing the genebank sequence files", default_value="/usr/src/data_folder/genome_download"),
    "spbetaviruses_directory": Field(str, description="Path to folder containing the genebank sequence files", default_value="/usr/src/data_folder/genbank_spbetaviruses"),
}

fasta_folder_config = {
    "fasta_directory": Field(str, description="Path to folder containing the fasta sequence files", default_value="/usr/src/data_folder/gene_identity/fasta"),
}

@asset(
    config_schema={**genbank_folder_config},
    description='Select for Spbetaviruses with complete genome sequence',
    compute_kind="Biopython",
    metadata={"owner" : "Virginie Grosboillot"},
)
def sequence_sorting(context):

    complete_sequences = []
    for file in glob.glob(f'{context.op_config["phage_download_directory"]}/*.gb'):
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

    context.log.info(f"Number of complete sequences: {len(bacillus_sub_sequences)}")

    genes_in_sequences = []
    for file in bacillus_sub_sequences:
        for p in SeqIO.parse(file, 'gb'):
            if set(['gene']).issubset(set([type_f.type for type_f in p.features])):
                genes_in_sequences.append(file)

    context.log.info(f"Number of complete sequences: {len(genes_in_sequences)}")

    for file in genes_in_sequences:
        shutil.copy2(file, f'{context.op_config["spbetaviruses_directory"]}{Path(file).stem}.gb')
    
    return list(map(lambda x: f"{context.op_config['spbetaviruses_directory']}/{x}", [Path(file).stem for file in genes_in_sequences]))


@asset(
    config_schema={**genbank_folder_config, **fasta_folder_config},
    description="""Parse genebank file and create a file containing every genes in the fasta format.
    Note: The sequence start and stop indexes are `-1` on the fasta file 1::10  --> [0:10] included/excluded.""",
    compute_kind="Biopython",
    metadata={"owner" : "Virginie Grosboillot"},
)
def genbank_to_fasta(context):

    # Check files that have already been processed


    # Process new files

    files = [file for file in glob.glob(f'{context.op_config["spbetaviruses_directory"]}/*.gb')]
    context.log.info(f"Number of file to process: {len(files)}")

    for file in files:
        output = f'{context.op_config["fasta_directory"]}/{Path(file).stem}.fna'

        genome = SeqIO.read(file, "genbank")

        with open(output, "w") as f:
            for feature in genome.features:
                if feature.type == "gene":
                    for seq_record in SeqIO.parse(file, "genbank"):
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
    fasta_files = [file for file in glob.glob(f'{context.op_config["fasta_directory"]}/*.fna')]
    return context.log.info(f"Number of file processed: {len(fasta_files)}")