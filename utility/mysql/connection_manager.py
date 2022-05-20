from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import text
from typing import Dict
import atexit
from utility.hashicorp.vault import Vault


class ConnectionManager:
    def __init__(self, role: str, host: str = 'mysql.veloslab.lan', port: int = 3306):
        self.role: str = role
        self.host: str = host
        self.port: int = port
        self.temp_tables: list = []
        self.engine = self.get_engine()
        atexit.register(self.__cleanup)

    def get_engine(self):
        creds = Vault.get_static_database_credentials(self.role)
        url = f'mysql+pymysql://{creds["username"]}:{creds["password"]}@{self.host}:{self.port}/?utf8bm4&binary_prefix=true'
        return create_engine(url, poolclass=NullPool)

    def query(self, query, query_args: Dict = None):
        with self.engine.connect() as conn:
            if query_args:
                result_proxy = conn.execute(text(query).execution_options(auto_commit=True), query_args)
            else:
                result_proxy = conn.execute(text(query).execution_options(auto_commit=True))
        return result_proxy

    def drop_temp_tables(self):
        for table in self.temp_tables:
            self.query(f"DROP TABLE IF EXISTS {table}")

    def add_temp_table(self, table: str):
        self.temp_tables.append(table)

    def __cleanup(self):
        self.drop_temp_tables()
        self.engine.dispose()
