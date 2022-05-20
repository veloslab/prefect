from .connection_manager import ConnectionManager
from pandas import DataFrame
from datetime import datetime
from uuid import uuid4
from typing import Union, Dict, List


class MySql:
    connection_managers = {}

    def __init__(self, role: str):
        self.cm_key = role

        if self.connection_managers.get(self.cm_key, None) is None:
            self.connection_managers[self.cm_key] = ConnectionManager(role=role)

        self.connection_manager = self.connection_managers[self.cm_key]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def query(self,
              query: str,
              query_args: Dict = None,
              pandas: bool = False,
              one_column: bool = False,
              one_row: bool = False,
              one_value: bool = False):

        if sum([one_column, one_row, one_value]) > 1:
            raise ValueError("You can only set one of these true: one_column, one_row, one_value")

        result_proxy = self.connection_manager.query(query, query_args=query_args)

        if pandas:
            if not query.lower().strip().startswith("select"):
                raise ValueError("Pandas=True only works when doing a select query!")
            df = DataFrame(result_proxy)
            df.columns = result_proxy.keys()
            return df

        if result_proxy.returns_rows:
            row_count = result_proxy.rowcount
            column_count = len(result_proxy.keys())

            if (one_row or one_value) and row_count > 1:
                raise ValueError(f"Query resulted in {row_count} rows "
                                 f"but one_{'row' if one_row else 'value'} set to True")

            if (one_column or one_value) and column_count > 1:
                raise ValueError(f"Query resulted in {column_count} columns "
                                 f"but one_{'row' if one_row else 'value'} set to True")
            if one_value:
                return result_proxy.fetchone()[0]
            elif one_row:
                return result_proxy.fetchone()
            else:
                return result_proxy.fetchall()
        else:
            return None

    def insert(self,
               table: str, data: Union[List[Dict], Dict],
               insert_type: str = 'INSERT',
               odku: str = None):
        """
        Insert data in table
        :param table: Table
        :param data: Data that should be inserted, must be dict or list dicts where key is column that value should be inserted into
        :param insert_type: Default is 'INSERT', can be 'REPLACE' or 'INSERT IGNORE' as well
        :param odku: Add 'ON DUPLICATE KEY UPDATE' statement to insert, value passed will be added after statement
        :return:
        """

        data = [data] if isinstance(data, dict) else data
        columns = data[0].keys()
        column_statement = "(`" + "`, `".join(columns) + "`)"
        insert_statement = f"{insert_type} INTO {table}{column_statement} VALUES "
        odku_statement = f"\nON DUPLICATE KEY UPDATE {odku}" if odku else ""
        row_statements = []

        for row in data:
            if list(row.keys()) != list(columns):
                raise Exception(f"Entry {row} does not have exact columns/correct order, expected {columns}")
            row_statements.append("('" + "', '".join(str(i) for i in row.values()) + "')")
        insert_query = insert_statement + "\n" + ",\n".join(row_statements) + odku_statement

        return self.connection_manager.query(insert_query)

    def temp_table(self, structure: str):
        """
        Create temp table that will be dropped after process ends
        :param structure: Stucture of temp table, Ex. '(id INT, PRIMARY KEY(id))'
        :return: Temp Table that was created
        """
        table = f"`tmp`.`py_{datetime.now().strftime('%Y%m%d')}_{uuid4().hex}`"
        query = f"CREATE TABLE IF NOT EXISTS {table} {structure}"
        self.connection_manager.query(query)
        self.connection_manager.add_temp_table(table)
        return table
