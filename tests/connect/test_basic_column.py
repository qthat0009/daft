from __future__ import annotations

from pyspark.sql.functions import col
from pyspark.sql.types import StringType


def test_column_operations(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)
    
    # Test __getattr__
    df_attr = df.select(col("id").desc())  # Fix: call desc() as method
    assert df_attr.toPandas()["id"].iloc[0] == 9, "desc should sort in descending order"
    
    # Test __getitem__ 
    # df_item = df.select(col("id")[0])
    # assert df_item.toPandas()["id"].iloc[0] == 0, "getitem should return first element"
    
    # Test alias
    df_alias = df.select(col("id").alias("my_number"))
    assert "my_number" in df_alias.columns, "alias should rename column"
    assert df_alias.toPandas()["my_number"].equals(df.toPandas()["id"]), "data should be unchanged"
    
    # Test cast
    df_cast = df.select(col("id").cast(StringType()))
    assert df_cast.schema.fields[0].dataType == StringType(), "cast should change data type"
    
    # Test isNotNull/isNull
    df_null = df.select(col("id").isNotNull().alias("not_null"), col("id").isNull().alias("is_null"))
    assert df_null.toPandas()["not_null"].iloc[0] == True, "isNotNull should be True for non-null values"
    assert df_null.toPandas()["is_null"].iloc[0] == False, "isNull should be False for non-null values"
    
    # Test name
    df_name = df.select(col("id").name("renamed_id"))
    assert "renamed_id" in df_name.columns, "name should rename column"
    assert df_name.toPandas()["renamed_id"].equals(df.toPandas()["id"]), "data should be unchanged"
