from .connection import Connection, Result
from pandas import DataFrame
from datetime import datetime
from uuid import uuid4
from typing import Union, Dict, List


class MySql:
    connections = {}

    def __init__(self, role: str, host: str):
        self.role = role
        if MySql.connections.get(role, None) is None:
            MySql.connections[role] = Connection(role=role, host=host)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    @property
    def connection(self) -> Connection:
        return MySql.connections[self.role]

    def query(self,
              query: str,
              pandas: bool = False,
              one_column: bool = False,
              one_row: bool = False,
              one_value: bool = False) -> Union[Result, Dict, List, DataFrame]:
        """
        Query MySql database, preferably using SELECT
        :param query: Query
        :param pandas: Default False, if True will return results as pandas.Dataframe
        :param one_column: Default False, if True it will return result as List
        :param one_row: Default True, if True will return result as Dictionary
        :param one_value: Default False, if True will return as a value that depends on mysql column structure
        :return:  Query Result
        """
        if sum([one_column, one_row, one_value]) > 1:
            raise ValueError("You can only set one of these true: one_column, one_row, one_value")

        result = self.connection.execute(query)

        if pandas:
            if not query.lower().strip().startswith("select"):
                raise ValueError("Pandas=True only works when doing a select query!")
            df = DataFrame(result.data)
            df.columns = result.columns
            return df

        if result.affected:
            if (one_row or one_value) and result.affected > 1:
                raise ValueError(f"Query resulted in {result.affected} rows "
                                 f"but one_{'row' if one_row else 'value'} set to True")

            if (one_column or one_value) and len(result.columns) > 1:
                raise ValueError(f"Query resulted in {len(result.columns)} columns "
                                 f"but one_{'column' if one_row else 'value'} set to True")
            if one_value:
                return result.data[0].values()[0]
            elif one_row:
                return result.data[0]
            elif one_column:
                return [i.values()[0] for i in result.data]
            else:
                return result.data
        else:
            return result

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

        return self.connection.execute(insert_query)

    def temp_table(self, structure: str):
        """
        Create temp table that will be dropped after process ends
        :param structure: Stucture of temp table, Ex. '(id INT, PRIMARY KEY(id))'
        :return: Temp Table that was created
        """
        table = f"`tmp`.`py_{datetime.now().strftime('%Y%m%d')}_{uuid4().hex}`"
        query = f"CREATE TABLE IF NOT EXISTS {table} {structure}"
        self.connection.connect.query(query)
        self.connection.add_temp_table(table)
        return table
