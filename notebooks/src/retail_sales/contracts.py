from pyspark.sql import DataFrame

BRONZE_SALES_SCHEMA = {
    "Invoice": "string",
    "StockCode": "string",
    "Description": "string",
    "Quantity": "int",
    "InvoiceDate": "string",
    "Price": "double",
    "CustomerID": "string",
    "Country": "string",
    "_rescued_data": "string",
    "_source_file": "string",
    "_ingested_at": "timestamp",
}

# What clean_sales changes and adds on top of Bronze
SILVER_SALES_SCHEMA = {
    **BRONZE_SALES_SCHEMA,
    "InvoiceDate": "timestamp",
    "UnitPrice": "double",
    "is_cancelled": "boolean",
    "has_customer_id": "boolean",
}


def schema_problems(df: DataFrame, expected: dict) -> list:
    """Return a sorted list of differences between df's schema and the contract. Empty list = OK."""
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    problems = []
    for name, dtype in expected.items():
        if name not in actual:
            problems.append(f"missing column: {name}")
        elif actual[name] != dtype:
            problems.append(f"type mismatch: {name} expected {dtype}, got {actual[name]}")
    for name in actual.keys() - expected.keys():
        problems.append(f"unexpected column: {name}")
    return sorted(problems)