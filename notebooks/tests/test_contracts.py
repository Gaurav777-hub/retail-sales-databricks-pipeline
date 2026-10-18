from retail_sales.contracts import BRONZE_SALES_SCHEMA, SILVER_SALES_SCHEMA, schema_problems
from retail_sales.silver import clean_sales


def ddl(schema: dict) -> str:
    return ", ".join(f"`{name}` {dtype}" for name, dtype in schema.items())


def test_clean_sales_output_matches_silver_contract(spark):
    bronze_like = spark.createDataFrame([], ddl(BRONZE_SALES_SCHEMA))

    out = clean_sales(bronze_like)

    assert schema_problems(out, SILVER_SALES_SCHEMA) == []


def test_renamed_header_is_detected(spark):
    # Replays the "Customer ID" incident: the header drifted from CustomerID
    drifted = {k: v for k, v in BRONZE_SALES_SCHEMA.items() if k != "CustomerID"}
    drifted["Customer ID"] = "double"
    df = spark.createDataFrame([], ddl(drifted))

    problems = schema_problems(df, BRONZE_SALES_SCHEMA)

    assert "missing column: CustomerID" in problems
    assert "unexpected column: Customer ID" in problems


def test_type_change_is_detected(spark):
    changed = {**BRONZE_SALES_SCHEMA, "Quantity": "string"}
    df = spark.createDataFrame([], ddl(changed))

    assert schema_problems(df, BRONZE_SALES_SCHEMA) == [
        "type mismatch: Quantity expected int, got string"
    ]