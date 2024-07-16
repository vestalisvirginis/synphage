import polars as pl

from Bio import SeqIO
from Bio.Seq import translate
from toolz import compose, juxt, first
from toolz.curried import map as mapc
from operator import itemgetter as it
from operator import attrgetter as at
from operator import methodcaller as mc
from operator import eq
from functools import partial


def genbank_to_dataframe(filename: str):

    # Read file
    genome = SeqIO.read(filename, "genbank")

    # features
    cds_qualifiers = (
        "gene",
        "locus_tag",
        "protein_id",
        "function",
        "product",
        "translation",
        "transl_table",
        "codon_start",
    )
    gene_qualifiers = ("gene", "locus_tag")
    location_attributes = ("start", "end", "strand")

    # annotations
    annotations_attributes = ("topology", "organism", "taxonomy")
    topology, organism, taxonomy = compose(
        juxt(map(lambda x: mc("get", x, None), annotations_attributes)),
        at("annotations"),
    )(genome)
    # topology, organism, taxonomy = compose(juxt(map(it, annotations_attributes)), at("annotations"))(genome)

    # ids
    id_attributes = ("id", "name", "description")
    id, name, description = juxt(map(at, id_attributes))(genome)

    # fn
    _impute_attributes = lambda x: mc("get", x, [""])

    _type_cds = compose(partial(eq, "CDS"), at("type"))
    _type_gene = compose(partial(eq, "gene"), at("type"))

    # process cds features related information
    data_cds = list(
        map(
            compose(
                list,
                mapc(first),
                juxt(map(_impute_attributes, cds_qualifiers)),
                at("qualifiers"),
            ),
            filter(_type_cds, genome.features),
        )
    )
    data_cds_pk = list(
        map(
            compose(
                list,
                mapc(at("real")),
                juxt(map(at, location_attributes)),
                at("location"),
            ),
            filter(_type_cds, genome.features),
        )
    )

    df_cds = pl.DataFrame(data_cds, schema=cds_qualifiers)
    df_cds_pk = pl.DataFrame(data_cds_pk, schema=location_attributes)
    cds_extract = list(
        map(lambda x: str(x.extract(genome.seq)), filter(_type_cds, genome.features))
    )
    df_cds_extract = pl.DataFrame(cds_extract, schema=["extract"])
    # df_cds_translate = pl.DataFrame(map(partial(translate, stop_symbol="", table=11), cds_extract), schema=["translation_fn"])

    cds = pl.concat(items=[df_cds, df_cds_pk, df_cds_extract], how="horizontal").rename(
        mapping={"gene": "cds_gene", "locus_tag": "cds_locus_tag"}
    )

    # process gene features related information
    data_gene = list(
        map(
            compose(
                list,
                mapc(first),
                juxt(map(_impute_attributes, gene_qualifiers)),
                at("qualifiers"),
            ),
            filter(_type_gene, genome.features),
        )
    )
    data_gene_pk = list(
        map(
            compose(
                list,
                mapc(at("real")),
                juxt(map(at, location_attributes)),
                at("location"),
            ),
            filter(_type_gene, genome.features),
        )
    )

    df_gene = pl.DataFrame(data_gene, schema=gene_qualifiers)
    df_gene_pk = pl.DataFrame(data_gene_pk, schema=location_attributes)
    gene_extract = list(
        map(lambda x: str(x.extract(genome.seq)), filter(_type_gene, genome.features))
    )
    df_gene_translate = pl.DataFrame(
        map(partial(translate, stop_symbol="", table=11), gene_extract),
        schema=["translation_fn"],
    )
    df_gene_extract = pl.DataFrame(gene_extract, schema=["extract"])

    gene = pl.concat(
        items=[df_gene, df_gene_pk, df_gene_extract, df_gene_translate],
        how="horizontal",
    )

    # join all the dataframes
    df = cds.join(
        other=gene, on=["start", "end", "strand", "extract"], how="full", coalesce=True
    )
    df = df.with_columns(
        id=pl.lit(id),
        name=pl.lit(name),
        description=pl.lit(description),
        topology=pl.lit(topology),
        organism=pl.lit(organism),
        taxonomy=pl.lit(taxonomy),
        filename=pl.lit(filename),
    )

    return df
