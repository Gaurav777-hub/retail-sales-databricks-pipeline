import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    active = SparkSession.getActiveSession()
    if active is not None:          # Databricks: reuse the existing session
        yield active
        return

    session = (                     # Laptop / CI: build a small local session
        SparkSession.builder.master("local[1]")
        .appName("retail-sales-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()