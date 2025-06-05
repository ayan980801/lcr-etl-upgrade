import pyspark.sql.types as T
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F
from ingest import (
    transform_column,
    rename_and_add_columns,
    table_schemas,
    process_table,
)

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


def test_update_last_runtime_called_with_max(monkeypatch):
    df = (
        spark.createDataFrame(
            [
                ("2024-01-01", "2024-01-01"),
                ("2024-02-01", "2024-02-01"),
            ],
            ["MODIFY_DATE", "CREATE_DATE"],
        )
        .withColumn("MODIFY_DATE", F.to_timestamp("MODIFY_DATE"))
        .withColumn("CREATE_DATE", F.to_timestamp("CREATE_DATE"))
    )

    monkeypatch.setattr("ingest.load_raw_data", lambda _: df)
    monkeypatch.setattr("ingest.rename_and_add_columns", lambda d, _: d)
    monkeypatch.setattr("ingest.transform_columns", lambda d, s, t: d)
    monkeypatch.setattr("ingest.add_metadata_columns", lambda d, s: d)
    monkeypatch.setattr("ingest.validate_dataframe", lambda d, s: None)
    monkeypatch.setattr("ingest.clean_invalid_timestamps", lambda d: d)
    monkeypatch.setattr("ingest.get_last_runtime", lambda _: datetime(1900, 1, 1))

    from ingest import table_schemas

    table_schemas["lead"] = T.StructType(
        [
            T.StructField("MODIFY_DATE", T.TimestampType(), True),
            T.StructField("CREATE_DATE", T.TimestampType(), True),
        ]
    )

    captured = {}

    def fake_update(table, ts):
        captured["ts"] = ts

    monkeypatch.setattr("ingest.update_last_runtime", fake_update)

    monkeypatch.setattr(
        "pyspark.sql.readwriter.DataFrameWriter.save", lambda self, path=None: None
    )

    import ingest

    ingest.write_mode = "delta_insert"
    ingest.historical_load = False
    ingest.snowflake_config = {}

    process_table("lead")

    expected = df.agg(F.max("MODIFY_DATE")).first()[0]
    assert captured["ts"] == expected
