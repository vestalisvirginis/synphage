# import os
# import warnings
import pytest
from pyspark.sql import SparkSession
#import logging

#logger = logging.getLogger(__name__)


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