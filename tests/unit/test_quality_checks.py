import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.quality_checks import check_not_null, check_positive, check_no_duplicates, check_row_count

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("tests").getOrCreate()

def test_check_not_null_ok(spark):
    schema = StructType([StructField("id_commande", StringType()), StructField("client", StringType())])
    df = spark.createDataFrame([("CMD001", "client1")], schema)
    check_not_null(df, "id_commande", "test")

def test_check_not_null_fail(spark):
    schema = StructType([StructField("id_commande", StringType()), StructField("client", StringType())])
    df = spark.createDataFrame([(None, "client1")], schema)
    with pytest.raises(AssertionError):
        check_not_null(df, "id_commande", "test")

def test_check_positive_ok(spark):
    schema = StructType([StructField("montant_total_eur", DoubleType())])
    df = spark.createDataFrame([(100.0,), (200.0,)], schema)
    check_positive(df, "montant_total_eur", "test")

def test_check_positive_fail(spark):
    schema = StructType([StructField("montant_total_eur", DoubleType())])
    df = spark.createDataFrame([(-50.0,), (200.0,)], schema)
    with pytest.raises(AssertionError):
        check_positive(df, "montant_total_eur", "test")

def test_check_no_duplicates_ok(spark):
    schema = StructType([StructField("id_commande", StringType())])
    df = spark.createDataFrame([("CMD001",), ("CMD002",)], schema)
    check_no_duplicates(df, "id_commande", "test")

def test_check_no_duplicates_fail(spark):
    schema = StructType([StructField("id_commande", StringType())])
    df = spark.createDataFrame([("CMD001",), ("CMD001",)], schema)
    with pytest.raises(AssertionError):
        check_no_duplicates(df, "id_commande", "test")

def test_check_row_count_ok(spark):
    schema = StructType([StructField("val", StringType())])
    df = spark.createDataFrame([("a",), ("b",), ("c",)], schema)
    check_row_count(df, 1, "test")

def test_check_row_count_fail(spark):
    schema = StructType([StructField("val", StringType())])
    df = spark.createDataFrame([("a",)], schema)
    with pytest.raises(AssertionError):
        check_row_count(df, 10, "test")