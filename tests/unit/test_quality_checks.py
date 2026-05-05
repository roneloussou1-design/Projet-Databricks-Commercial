import pytest
from pyspark.sql import SparkSession
from src.quality_checks import check_not_null, check_positive, check_no_duplicates, check_row_count

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("tests").getOrCreate()

def test_check_not_null_ok(spark):
    df = spark.createDataFrame([("CMD001", "client1")], ["id_commande", "client"])
    check_not_null(df, "id_commande", "test")

def test_check_not_null_fail(spark):
    df = spark.createDataFrame([(None, "client1")], ["id_commande", "client"])
    with pytest.raises(AssertionError):
        check_not_null(df, "id_commande", "test")

def test_check_positive_ok(spark):
    df = spark.createDataFrame([(100.0,), (200.0,)], ["montant_total_eur"])
    check_positive(df, "montant_total_eur", "test")

def test_check_positive_fail(spark):
    df = spark.createDataFrame([(-50.0,), (200.0,)], ["montant_total_eur"])
    with pytest.raises(AssertionError):
        check_positive(df, "montant_total_eur", "test")

def test_check_no_duplicates_ok(spark):
    df = spark.createDataFrame([("CMD001",), ("CMD002",)], ["id_commande"])
    check_no_duplicates(df, "id_commande", "test")

def test_check_no_duplicates_fail(spark):
    df = spark.createDataFrame([("CMD001",), ("CMD001",)], ["id_commande"])
    with pytest.raises(AssertionError):
        check_no_duplicates(df, "id_commande", "test")

def test_check_row_count_ok(spark):
    df = spark.createDataFrame([("a",), ("b",), ("c",)], ["val"])
    check_row_count(df, 1, "test")

def test_check_row_count_fail(spark):
    df = spark.createDataFrame([("a",)], ["val"])
    with pytest.raises(AssertionError):
        check_row_count(df, 10, "test")