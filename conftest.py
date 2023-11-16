# import os
# import warnings
import pytest
from pyspark.sql import SparkSession
#import logging

#logger = logging.getLogger(__name__)


@pytest.fixture
def mock_env_phagy_dir_blasting(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/assets_testing_folder/blasting")


@pytest.fixture
def mock_env_phagy_dir_synteny(monkeypatch):
    monkeypatch.setenv("PHAGY_DIRECTORY", "test/fixtures/assets_testing_folder/synteny")
    monkeypatch.setenv("SEQUENCE_FILE", "sequences.csv")


@pytest.fixture(scope="session")
def spark():
    try:
        #logger = logging.getLogger("py4j")
        #ogger.setLevel(logging.ERROR)
        spark_session = SparkSession.builder.config(
            "spark.driver.memory", "2g"
        ).getOrCreate()
        yield spark_session
    except:
        pass
    finally:
        spark_session.stop()