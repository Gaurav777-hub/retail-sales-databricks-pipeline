from datetime import datetime

from retail_sales.silver import DEDUP_KEYS, deduplicate_sales

COLUMNS = ["Invoice", "StockCode", "CustomerID", "Quantity", "InvoiceDate"]
TS = datetime(2010, 12, 1, 8, 26)


def test_exact_duplicates_collapse_to_one_row(spark):
    rows = [
        ("536365", "85123A", "17850", 6, TS),
        ("536365", "85123A", "17850", 6, TS),   # exact repeat
        ("536365", "71053", "17850", 6, TS),    # different StockCode
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    assert deduplicate_sales(df).count() == 2


def test_rows_differing_in_a_key_column_are_kept(spark):
    rows = [
        ("536365", "85123A", "17850", 6, TS),
        ("536365", "85123A", "17850", 7, TS),   # only Quantity differs
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    assert deduplicate_sales(df).count() == 2


def test_dedup_key_definition_has_not_changed():
    # Guards the business rule: changing this list changes what counts as a duplicate
    assert DEDUP_KEYS == ["Invoice", "StockCode", "CustomerID", "Quantity", "InvoiceDate"]