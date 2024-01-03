# import os
# import warnings
import pytest
# from pyspark.sql import SparkSession
#import logging

#logger = logging.getLogger(__name__)


@pytest.fixture
def mock_env_ncbi_connect(monkeypatch):
    monkeypatch.setenv("EMAIL", "name@domain.com")
    monkeypatch.setenv("API_KEY", "jhd6hdz778ahjeahj8889")


@pytest.fixture
def mock_env_ncbi_download_pos(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/ncbi_download/positive")


@pytest.fixture
def mock_env_ncbi_download_neg(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/ncbi_download/negative")


@pytest.fixture
def mock_env_ncbi_count(monkeypatch):
    monkeypatch.setenv("DATABASE", "nuccore")
    monkeypatch.setenv("KEYWORD", "Listeria ivanovii")


@pytest.fixture
def mock_env_ncbi_fetch(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/ncbi_download/fetch")


@pytest.fixture
def mock_env_phagy_dir_blasting(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/assets_testing_folder/blasting")

@pytest.fixture
def mock_env_phagy_dir_blasting_with_history(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/assets_testing_folder/blasting_with_history")

@pytest.fixture
def mock_env_phagy_dir_transform(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/assets_testing_folder/transform")
    monkeypatch.setenv("FILE_SYSTEM", "fs")

@pytest.fixture
def mock_env_phagy_dir_transform_step3(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/assets_testing_folder/transform_3")

@pytest.fixture
def mock_env_phagy_dir_synteny(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/assets_testing_folder/synteny")
    monkeypatch.setenv("SEQUENCE_FILE", "sequences.csv")
# @pytest.fixture(scope="session")
# def spark():
#     try:
#         #logger = logging.getLogger("py4j")
#         #ogger.setLevel(logging.ERROR)
#         spark_session = SparkSession.builder.config(
#             "spark.driver.memory", "2g"
#         ).getOrCreate()
#         yield spark_session
#     except:
#         pass
#     finally:
#         spark_session.stop()