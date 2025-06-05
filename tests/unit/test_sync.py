from sync import PostgresAzureDataSync


class DummyPostgresHandler:
    def __init__(self) -> None:
        self.called = []

    def is_connection_alive(self) -> bool:
        return True

    def export_table_to_delta(self, table: str, stage: str, db: str) -> None:
        self.called.append((table, stage, db))


class DummyAzureHandler:
    def ensure_directory_exists(self, stage: str, db: str) -> None:
        pass


def test_perform_operation_calls_handlers():
    pg = DummyPostgresHandler()
    az = DummyAzureHandler()
    sync = PostgresAzureDataSync(pg, az)
    sync.perform_operation("db", ["t1", "t2"])
    assert pg.called == [("t1", "RAW", "db"), ("t2", "RAW", "db")]
