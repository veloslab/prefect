from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import text
from datetime import datetime
from typing import Dict
import atexit
from utility.hashicorp.vault import Vault


class ConnectionManager:
    def __init__(self, role: str, host: str = 'mysql.veloslab.lan', port: int = 3306):
        credentials = Vault.get_static_database_credentials(role=role)
        self.engine = self.get_engine(host=host,
                                      user=credentials['username'],
                                      password=credentials['password'],
                                      port=port)
        self.temp_tables = []
        atexit.register(self.__cleanup)

    @staticmethod
    def get_engine(host: str, user: str, password: str, port: int):
        url = f'mysql+pymysql://{user}:{password}@{host}:{port}/?utf8bm4&binary_prefix=true'
        return create_engine(url, poolclass=NullPool)

    def query(self, query, query_args: Dict = None):
        with self.engine.connect() as conn:
            if query_args:
                result_proxy = conn.execute(text(query).execution_options(auto_commit=True), query_args)
            else:
                result_proxy = conn.execute(text(query).execution_options(auto_commit=True))
        return result_proxy

    def add_temp_table(self, table: str):
        self.temp_tables.append(table)

    def __cleanup(self):
        self.engine.dispose()
