from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from psycopg2 import OperationalError
import logging
import psycopg2
import traceback
from pyspark.sql.functions import current_timestamp, lit

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

try:
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
except Exception:
    import dbutils  # type: ignore


# Postgres Handler
class PostgresDataHandler:
    def __init__(self, pg_pool):
        self.pg_pool = pg_pool

    @staticmethod
    def connect_to_postgres(pg_config: dict) -> psycopg2.extensions.connection:
        try:
            return psycopg2.pool.ThreadedConnectionPool(1, 500, **pg_config)
        except OperationalError as e:
            logging.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    def is_connection_alive(self):
        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except OperationalError:
            return False
        finally:
            self.pg_pool.putconn(conn)

    def get_table_count(self, table: str) -> int:
        """Get actual row count from PostgreSQL table"""
        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logging.info(f"PostgreSQL count for {table}: {count}")
                return count
        finally:
            self.pg_pool.putconn(conn)

    def export_table_to_delta(self, table: str, stage: str, db: str) -> None:
        """
        Export a table directly from PostgreSQL to Delta Lake using JDBC
        This bypasses CSV completely and avoids all the row count issues
        """
        try:
            # Get actual row count from PostgreSQL for verification
            pg_count = self.get_table_count(table)
            logging.info(f"Starting direct JDBC export of {pg_count} rows from {table}")

            # Create JDBC URL and properties
            jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
            properties = {
                "user": pg_config["user"],
                "password": pg_config["password"],
                "driver": "org.postgresql.Driver",
                # Increase fetch size for better performance
                "fetchsize": "10000",
            }

            # Use Spark's JDBC reader to load directly from PostgreSQL
            # This completely avoids any CSV intermediate step
            df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)

            # Log the schema to verify correct data types
            logging.info(f"JDBC schema for {table}:")
            for field in df.schema.fields:
                logging.info(f"  {field.name}: {field.dataType}")

            # Count rows to verify
            jdbc_count = df.count()
            logging.info(f"JDBC read {jdbc_count} rows from {table}")

            if jdbc_count != pg_count:
                logging.warning(
                    f"Row count mismatch: PostgreSQL={pg_count}, JDBC={jdbc_count}"
                )

            # Add metadata columns
            df = df.withColumns(
                {
                    "ETL_CREATED_DATE": current_timestamp(),
                    "ETL_LAST_UPDATE_DATE": current_timestamp(),
                    "CREATED_BY": lit("ETL_PROCESS"),
                    "TO_PROCESS": lit(True),
                    "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
                }
            )

            # Calculate Delta Lake path
            clean_table = table.replace('public."', "").replace('"', "")
            flp = f"abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/{stage}/{db}/{clean_table}"

            # Write to Delta Lake
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).save(flp)

            # Verify Delta file row count
            delta_df = spark.read.format("delta").load(flp)
            delta_count = delta_df.count()
            logging.info(f"Delta file count for {table}: {delta_count}")

            if delta_count != jdbc_count:
                logging.warning(
                    f"Delta count ({delta_count}) doesn't match JDBC count ({jdbc_count})"
                )
            else:
                logging.info(
                    f"Successfully exported {delta_count} rows from {table} to Delta"
                )

        except Exception as e:
            logging.error(f"Failed to export table '{table}': {str(e)}")
            logging.error(traceback.format_exc())
            raise


# Azure Data Handler
class AzureDataHandler:
    def __init__(self, blob_service_client):
        self.blob_service_client = blob_service_client

    @staticmethod
    def connect_to_azure_storage(storage_config: dict) -> BlobServiceClient:
        try:
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_config['account_name']};AccountKey={storage_config['account_key']};EndpointSuffix=core.windows.net"
            return BlobServiceClient.from_connection_string(connection_string)
        except Exception as e:
            logging.error(f"Failed to connect to Azure Storage: {str(e)}")
            raise

    def ensure_directory_exists(self, stage: str, db: str) -> None:
        try:
            db_path = f"abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/{stage}/{db}"
            dbutils.fs.ls(db_path)
        except Exception:
            dbutils.fs.mkdirs(db_path)


# Data Sync
class PostgresAzureDataSync:
    def __init__(
        self, postgres_handler: PostgresDataHandler, azure_handler: AzureDataHandler
    ):
        self.postgres_handler = postgres_handler
        self.azure_handler = azure_handler

    def perform_operation(self, db: str, tables_to_copy: list) -> None:
        if not self.postgres_handler.is_connection_alive():
            logging.error("PostgreSQL connection is not alive. Aborting operation.")
            return

        for table in tables_to_copy:
            try:
                logging.info(f"Processing table {table}")
                # Ensure the directory exists
                self.azure_handler.ensure_directory_exists("RAW", db)
                # Export directly to Delta
                self.postgres_handler.export_table_to_delta(table, "RAW", db)
                logging.info(f"Successfully processed table {table}")
            except Exception as e:
                logging.error(f"Failed to process table {table}: {str(e)}")
                logging.error(traceback.format_exc())


# PostgreSQL Configurations
pg_config = {
    "host": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-LCR-Host-PROD"
    ),
    "port": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-LCR-Port-PROD"
    ),
    "database": "LeadCustodyRepository",
    "user": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-LCR-User-PROD"
    ),
    "password": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-LCR-Pass-PROD"
    ),
}

# Azure Configurations
storage_config = {
    "account_name": "quilitydatabricks",
    "account_key": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-ADLS-Key"
    ),
    "container_name": "dataarchitecture",
}

# Table Configurations
tables_to_copy = [
    'public."leadassignment"',
    'public."leadxref"',
    'public."lead"',
]

# Execution
try:
    pg_pool = PostgresDataHandler.connect_to_postgres(pg_config)
    postgres_handler = PostgresDataHandler(pg_pool)
    blob_service_client = AzureDataHandler.connect_to_azure_storage(storage_config)
    azure_handler = AzureDataHandler(blob_service_client)
    sync = PostgresAzureDataSync(postgres_handler, azure_handler)
    sync.perform_operation(
        pg_config["database"],
        tables_to_copy,
    )
finally:
    sync.postgres_handler.pg_pool.closeall()
