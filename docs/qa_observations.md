# QA Observations

During initial QA the following issues were found:

* **Row count mismatch** - exporting PostgreSQL tables through CSV resulted in duplicate rows. The upgraded sync script now uses direct JDBC export to Delta Lake and performs row count reconciliation between PostgreSQL, JDBC and Delta outputs.
* **Timestamp precision** - several timestamp columns contained invalid or future dates. Additional parsing and cleansing logic was added in `ingest.py` to normalise timestamps and cap future dates at the current time.
* **JSON flattening** - ingestion of `lead_assignment` incorrectly flattened JSON metadata. Special handling keeps JSON columns intact.

Row count checks are performed before and after every transformation step to ensure data quality.
