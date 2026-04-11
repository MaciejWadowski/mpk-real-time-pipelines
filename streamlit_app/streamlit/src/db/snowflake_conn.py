from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine


class SnowflakeDB:
    def __init__(self, user, password, account, database, schema, warehouse):
        self.user = user
        self.password = password
        self.account = account
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self._engine = None

    def get_engine(self):
        if self._engine is None:
            self._engine = create_engine(
                f"snowflake://{self.user}:{quote_plus(self.password)}@"
                f"{self.account}/{self.database}/{self.schema}"
                f"?warehouse={self.warehouse}"
            )
        return self._engine

    def query(self, sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, self.get_engine())
