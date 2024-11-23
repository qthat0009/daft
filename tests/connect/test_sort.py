from __future__ import annotations

from pyspark.sql.functions import col


def test_sort(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Sort the DataFrame by 'id' column in descending order
    df_sorted = df.sort(col("id").desc())

    # Verify the DataFrame is sorted correctly
    actual =  df.to_arrow().to_pydict() # or df.toPandas().to_dict()
    expected = daft.from_pydict({"id": list(range(10))}).sort("id", descending=True).to_pydict()
    assert actual == expected
