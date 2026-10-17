from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType

from retail_sales.silver import clean_sales

SCHEMA = StructType(
    [StructField(c, StringType(), True)
     for c in ["Invoice", "StockCode", "Quantity", "InvoiceDate", "Price", "CustomerID"]]
)


def run(spark, rows):
    """Clean the rows and return them keyed by Invoice."""
    df = spark.createDataFrame(rows, SCHEMA)
    return {r["Invoice"]: r for r in clean_sales(df).collect()}


def test_invoice_date_is_parsed(spark):
    out = run(spark, [("536365", "85123A", "6", "01-12-10 8:26", "2.55", "17850")])
    assert out["536365"]["InvoiceDate"] == datetime(2010, 12, 1, 8, 26)


def test_malformed_date_becomes_null_not_error(spark):
    out = run(spark, [("536366", "22633", "6", "not-a-date", "1.85", "17850")])
    assert out["536366"]["InvoiceDate"] is None


def test_cancellation_flag(spark):
    out = run(spark, [
        ("C536379", "D", "-1", "01-12-10 9:41", "27.50", "14527"),
        ("536365", "85123A", "6", "01-12-10 8:26", "2.55", "17850"),
    ])
    assert out["C536379"]["is_cancelled"] is True
    assert out["536365"]["is_cancelled"] is False


def test_missing_customer_becomes_unknown_and_is_flagged(spark):
    out = run(spark, [("536414", "22139", "56", "01-12-10 11:52", "0.00", None)])
    assert out["536414"]["CustomerID"] == "UNKNOWN"
    assert out["536414"]["has_customer_id"] is False


def test_types_and_unitprice(spark):
    out = run(spark, [("536365", "85123A", "6", "01-12-10 8:26", "2.55", "17850")])
    assert out["536365"]["Quantity"] == 6
    assert out["536365"]["UnitPrice"] == 2.55