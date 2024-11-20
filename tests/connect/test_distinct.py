from __future__ import annotations

from pyspark.sql.functions import col


def test_distinct(spark_session):
    # Create DataFrame with duplicates
    data = [(1,), (1,), (2,), (2,), (3,)]
    df = spark_session.createDataFrame(data, ["id"])

    # Get distinct rows
    df_distinct = df.distinct()

    # Verify distinct operation removed duplicates
    df_distinct_pandas = df_distinct.toPandas()
    assert len(df_distinct_pandas) == 3, "Distinct should remove duplicates"
    assert set(df_distinct_pandas["id"]) == {1, 2, 3}, "Distinct values should be preserved"
