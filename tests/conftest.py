import os

import pytest

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_API_KEY", "test-key")
os.environ.setdefault("KAFKA_API_SECRET", "test-secret")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("JWT_SECRET", "test-secret-not-for-production")
os.environ.setdefault("DEMO_API_USERNAME", "demo")
os.environ.setdefault("DEMO_API_PASSWORD", "demo-password")


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("consent-pipeline-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
