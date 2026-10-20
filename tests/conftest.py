import pytest 

from src.common.spark_session import get_spark

@pytest.fixture(scope = "session")
def spark():
    return get_spark()