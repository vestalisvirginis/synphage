from dagster import resource, EnvVar

import os

from Bio import Entrez


class NCBIConnection:
    def __init__(self, email: str, api_key: str):
        self.conn = Entrez
        self.conn.email = email
        self.conn.api_key = api_key

    def __repr__(self):
        return f"Email: {self.conn.email}\nAPI: {self.conn.api_key}\n"


# @resource(config_schema={"email": str, "api_key" : str})
@resource
def ncbi_resource(init_context):
    return NCBIConnection(
        os.getenv(EnvVar("EMAIL")),
        os.getenv(EnvVar("API_KEY")),
    )
