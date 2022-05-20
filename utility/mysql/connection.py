import pymysql.cursors
from dataclasses import dataclass, field
from typing import Union, List, Dict, Tuple
import atexit
from utility.hashicorp.vault import Vault


@dataclass
class Result:
    affected: int
    data: Union[List, Dict, Tuple]

    def __post_init__(self):
        self.columns: List = self.data[0].keys() if self.data else []


@dataclass
class Connection:
    role: str
    host: str
    port: int = 3306
    temp_tables: List = field(default_factory=list)
    _connection: pymysql.Connection = None

    def __post_init__(self):
        atexit.register(self.drop_temp_tables)

    def _create_connection(self) -> pymysql.Connection:
        credentials = Vault.get_static_database_credentials(self.role)
        return pymysql.Connection(
            host=self.host,
            user=credentials['username'],
            password=credentials['password'],
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor
        )

    def _cleanup(self):
        self.drop_temp_tables()

    @property
    def connect(self):
        if self._connection is None:
            self._connection = self._create_connection()
        if self._connection.open:
            self._connection.close()
        return self._connection

    def execute(self, query: str) -> Result:
        with self.connect as connection:
            connection.ping(reconnect=True)
            with connection.cursor() as cursor:
                cursor.execute(query)
                r = Result(data=cursor.fetchall(), affected=cursor.rowcount)
        return r

    def drop_temp_tables(self):
        for table in self.temp_tables:
            self.execute(f"DROP TABLE IF EXISTS {table}")

    def add_temp_table(self, table: str):
        self.temp_tables.append(table)
