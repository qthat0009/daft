from __future__ import annotations


def test_create_df(spark_session):
    # Create simple DataFrame
    data = [(1,), (2,), (3,)]
    df = spark_session.createDataFrame(data, ["id"])

    # Convert to pandas
    df_pandas = df.toPandas()
    assert len(df_pandas) == 3, "DataFrame should have 3 rows"
    assert list(df_pandas["id"]) == [1, 2, 3], "DataFrame should contain expected values"
