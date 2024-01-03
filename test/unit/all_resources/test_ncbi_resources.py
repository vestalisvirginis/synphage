import pytest

from dagster import build_init_resource_context

from synphage.resources.ncbi_resource import NCBIConnection, ncbi_resource


def test_ncbiconnection():
    assert callable(NCBIConnection)
    configuration = NCBIConnection(
        email="my_email",
        api_key="my_api_key",
    )
    assert hasattr(configuration.conn, "email")
    assert hasattr(configuration.conn, "api_key")
    assert repr(configuration) == "Email: my_email\nAPI: my_api_key\n"


def test_ncbiconnection_values_pos():
    configuration = NCBIConnection(
        email="my_email",
        api_key="my_api_key",
    )
    assert configuration.conn.email == "my_email"
    assert configuration.conn.api_key == "my_api_key"


def test_ncbiconnection_values_missing():
    with pytest.raises(
        TypeError,
        match="missing 1 required positional argument",
    ):
        NCBIConnection("a")


def test_ncbi_connect_resources(mock_env_ncbi_connect):
    context = build_init_resource_context()
    result = ncbi_resource(context)
    assert isinstance(result, NCBIConnection)
    assert result.conn.email == "name@domain.com"
    assert result.conn.api_key == "jhd6hdz778ahjeahj8889"
