import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from main import InstrumentCalculator, load_configuration

@pytest.fixture
def config():
    return load_configuration()

@pytest.fixture
def spark_session():
    return SparkSession.builder \
        .appName("test_instrument_calculator") \
        .master("local[2]") \
        .getOrCreate()

@pytest.fixture
def instrument_calculator(spark_session, config):
    calculator = InstrumentCalculator(spark_session, config)
    calculator.connect_db()
    return calculator

def test_read_data(instrument_calculator):
    data = instrument_calculator.read_data()
    assert data.count() > 0

def test_calculate_mean(spark_session, instrument_calculator):
    data = spark_session.createDataFrame([
        ("INSTRUMENT1", datetime.now(), 2.0, 2.5),
        ("INSTRUMENT1", datetime.now(), 3.0, 4.0),
    ], ["instrument", "date", "value", "multiplied_value"])

    result = instrument_calculator.calculate_mean(data, "INSTRUMENT1")

    mean_value = result.collect()[0]["mean_value_INSTRUMENT1"]
    mean_multiplied_value = result.collect()[0]["mean_multiplied_value_INSTRUMENT1"]

    assert mean_value == 2.5
    assert mean_multiplied_value == 3.25

# Ensure the Spark session is stopped at the end of the tests
def test_cleanup(spark_session):
    spark_session.stop()
