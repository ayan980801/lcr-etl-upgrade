import pyspark.sql.types as T
from pyspark.sql import SparkSession
from ingest import transform_column, rename_and_add_columns, table_schemas

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_boolean_string_column():
    df = spark.createDataFrame(
        [
            ("true",),
            ("false",),
            (None,),
        ],
        ["IS_DELETED_SOURCE"],
    )
    result = transform_column(
        df, "IS_DELETED_SOURCE", T.StringType(), "lead_assignment"
    )
    collected = [row[0] for row in result.collect()]
    assert collected == ["TRUE", "FALSE", None]


def test_rename_and_add_columns():
    df = spark.createDataFrame(
        [("id1", "guid1")], ["leadassignmentguid", "leadxrefguid"]
    )
    result = rename_and_add_columns(df, "lead_assignment")
    assert "LEAD_ASSIGNMENT_GUID" in result.columns
    assert "LEAD_XREF_GUID" in result.columns
    for field in table_schemas["lead_assignment"].fields:
        assert field.name in result.columns
