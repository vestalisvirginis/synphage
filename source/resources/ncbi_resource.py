from dagster import resource

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
        # init_context.resource_config["email"],
        # init_context.resource_config["api_key"]
        "virginie.grosboillot@bf.uni-lj.si",
        "8a016c7273a14c68d5f72af6f404024dfc08",
    )
