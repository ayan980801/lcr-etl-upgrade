from pyspark.sql import SparkSession
from ingest import (
    rename_and_add_columns,
    transform_columns,
    add_metadata_columns,
    table_schemas,
)

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_full_transformation():
    df = spark.createDataFrame(
        [("id1", "guid1", "yes")],
        ["leadassignmentguid", "leadxrefguid", "isdeletedsource"],
    )
    df1 = rename_and_add_columns(df, "lead_assignment")
    target_schema = table_schemas["lead_assignment"]
    df2 = transform_columns(df1, target_schema, "lead_assignment")
    df3 = add_metadata_columns(df2, target_schema)
    assert df3.count() == 1
    for field in target_schema.fields:
        assert field.name in df3.columns
