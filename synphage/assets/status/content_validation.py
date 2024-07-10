from dagster import (
    Output,
    AssetCheckSpec,
    AssetCheckResult,

    asset,
    AssetIn,
    AssetExecutionContext,
)

import os
import pickle
import pandas as pd
import polars  as pl
from toolz import compose, juxt
from operator import itemgetter as it
from operator import methodcaller as mc
from pathlib import Path
from cuallee import Check, bio


GENBANK_CHECKS = {'is_complete': {'check_type': 'std', 'check_name': 'is_complete', 'cols': ["gene", "locus_tag", "cds_gene", "cds_locus_tag", "protein_id", "start", "end", "strand", "extract"], 'check_value': None, 'check_description': 'Validate the column completeness'},
                  'is_unique': {'check_type': 'std', 'check_name': 'is_unique', 'cols': ["gene", "locus_tag", "cds_gene", "cds_locus_tag", "protein_id", "start", "end"], 'check_value': None, 'check_description': 'Validate the uniqueness of each value presents in the column'},
                  'is_dna': {'check_type': 'bio', 'check_name': 'is_dna', 'cols': ['extract'], 'check_value': None, 'check_description': 'Validate that the sequence only contains ATCG'},
                  'is_protein': {'check_type': 'bio', 'check_name': 'is_protein', 'cols': ['translation', 'translation_fn'], 'check_value': None, 'check_description': 'Validate that the protein sequence only contains valid amino acids'},
                  'is_contained_in' : {'check_type': 'parameterised', 'check_name': 'is_contained_in', 'cols': ['strand'], 'check_value': (1,-1), 'check_description': 'Validate that the protein sequence only contains valid amino acids'},
                  }

check = Check()

### Check logic to implement

# bio checks:
# levenstein distance
# cols=[('translate', 'translate_fn')]

# for id , name check on the full dataframe --> unique to each file
# are_unique 
# cols = [(file, id), (file, name)]



def create_check_specs(check_dict, asset_name):
    """Create """
    check = Check()
    for key in check_dict:
        check_type, check_name, cols, value, description = compose(juxt(map(it, ["check_type", "check_name", "cols", "check_value", "check_description"])), mc("get", key))(check_dict)
        if check_type == 'bio':
            juxt(map(lambda x: mc(check_name, x), cols))(check.bio)
        elif check_type == 'parameterised':
            juxt(map(lambda x: mc(check_name, x, value), cols))(check)
        else:
            juxt(map(lambda x: mc(check_name, x), cols))(check)
    #check.is_contained_in('strand', (1,-1))

    #check_specs = [AssetCheckSpec(name=f"{item.method}.{item.column}", asset=asset_name, description=check_dict) for item in check.rules]
    check_specs = [AssetCheckSpec(name=f"{item.method}.{item.column}", asset=asset_name) for item in check.rules]
    
    return check, check_specs


def _chiki_1_settings(asset_name: str, asset_check_specs: list[AssetCheckSpec], asset_description: str = "Empty") -> dict:
    """Asset configuration for chiki gb validation"""
    return {
        "name": asset_name.lower(),
        "description": asset_description,
        "io_manager_key": "io_manager",
        # "group_name": "checks",
        "metadata": {"owner": "Virginie Grosboillot"},
        "compute_kind": "validation",
        "check_specs": asset_check_specs,
    }


def _chiki_2_settings(asset_name: str, input_name: str, asset_description: str = "Empty") -> dict:
    """Asset configuration for chiki gb validation"""
    return {
        "name": asset_name.lower(),
        "description": asset_description,
        "ins": {input_name.lower(): AssetIn()},
        "io_manager_key": "io_manager",
        # "group_name": "checks",
        "metadata": {"owner": "Virginie Grosboillot"},
        "compute_kind": "validation",
    }


def chiki_level_1(key, name, check_specs, check, description):
    """Asset factory for df loading"""
    
    @asset(**_chiki_1_settings(asset_name=key + "_validation", asset_check_specs=check_specs, asset_description=description))
    def asset_template(context: AssetExecutionContext):
        entity = name
        path= "temp/development/data/fs/gb_parsing"
        data = pd.read_parquet(f"{path}/{entity}.parquet")
        yield Output(value=data, metadata={"rows": len(data)})

        for item in check.validate(data).itertuples():
            yield AssetCheckResult(
                asset_key=key.lower() + "_validation",
                check_name=f"{item.rule}.{item.column}",
                passed=(item.status == "PASS"),
                metadata={
                    "level": item.level,
                    "rows": int(item.rows),
                    "column": item.column,
                    "value": str(item.value),
                    "violations": int(item.violations),
                    "pass_rate": item.pass_rate,
                },
            )

        #return Output(value=data, metadata={"rows": len(data)})
    
    return asset_template


def chiki_level_2(key, input_name, description):
    """Asset factory for Writing in a new location"""

    @asset(
        **_chiki_2_settings(
            asset_name=key, input_name=input_name, asset_description=description
        )
    )
    def asset_template(**kwargs):
        entity = kwargs[input_name]
        path= "temp/development/data/fs/gb_parsing_copy"
        file_name = "test"
        entity.to_parquet(f"{path}/{file_name}.parquet")

        return Output(value=entity, metadata={"rows": len(entity)})
    
    return asset_template


def load_dynamic():
    """Asset factory generator using crm manifesto"""

    _file = 'temp/development/data/fs/genbank_history'
    entities = pickle.load(open(_file, 'rb'))
    asset_names = entities.history
    assets = []
    for asset_name in asset_names:
        checks, check_specs=create_check_specs(GENBANK_CHECKS, asset_name=asset_name.lower() + "_validation")
        assets.append(
            chiki_level_1(
                key=asset_name,
                name=asset_name,
                check_specs=check_specs,
                check=checks,
                description=f"Load table for {asset_name} and apply the checks",

            )
        )

        assets.append(
            chiki_level_2(
                key=asset_name,
                input_name=asset_name.lower() + "_validation",
                description="Not yet known"
            )
        )

    return assets


my_dynamic_assets = load_dynamic()
