from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, try_to_timestamp, when

INVOICE_DATE_FORMAT = "dd-MM-yy H:mm"


def clean_sales(df: DataFrame) -> DataFrame:
    """Silver cleaning rules for sales. Pure DataFrame in, DataFrame out (no I/O)."""
    return (
        df
        .withColumn("InvoiceDate", try_to_timestamp(col("InvoiceDate"), lit(INVOICE_DATE_FORMAT)))
        .withColumn("Quantity", col("Quantity").cast("int"))
        .withColumn("UnitPrice", col("Price").cast("double"))
        .withColumn("is_cancelled", col("Invoice").startswith("C"))
        .withColumn("has_customer_id", col("CustomerID").isNotNull())
        .withColumn(
            "CustomerID",
            when(col("CustomerID").isNull(), "UNKNOWN").otherwise(col("CustomerID")),
        )
    )


DEDUP_KEYS = ["Invoice", "StockCode", "CustomerID", "Quantity", "InvoiceDate"]
WATERMARK_DELAY = "3 days"


def deduplicate_sales(df: DataFrame) -> DataFrame:
    """Streaming-safe dedupe: watermark first, then dropDuplicates on the business key."""
    return (
        df
        .withWatermark("InvoiceDate", WATERMARK_DELAY)
        .dropDuplicates(DEDUP_KEYS)
    )