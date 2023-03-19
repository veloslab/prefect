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
              one_value: bool = False) -> Union[Result, Dict, List, DataFrame, None]:
        """
        Query MySql database, preferably using SELECT
        :param query: Query
        :param pandas: Default False, if True will return select results as pandas.Dataframe
        :param one_column: Default False, if True it will select return result as List
        :param one_row: Default True, if True will return select result as Dictionary
        :param one_value: Default False, if True will return sole value from select result
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

        if query.strip().lower().startswith('select'):
            if result.affected:
                if (one_row or one_value) and result.affected > 1:
                    raise ValueError(f"Query resulted in {result.affected} rows "
                                     f"but one_{'row' if one_row else 'value'} set to True")

                if (one_column or one_value) and len(result.columns) > 1:
                    raise ValueError(f"Query resulted in {len(result.columns)} columns "
                                     f"but one_{'column' if one_row else 'value'} set to True")
                if one_value:
                    return list(result.data[0].values())[0]
                elif one_row:
                    return result.data[0]
                elif one_column:
                    return [list(i.values())[0] for i in result.data]
                else:
                    return result.data
            else:
                return None
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
        columns = list(data[0].keys())
        column_statement = "`" + "`, `".join(columns) + "`"
        insert_statement = f"{insert_type} INTO {table}({column_statement}) VALUES "
        values_statement = "(" + ", ".join(["%s"] * len(columns)) + ")"
        odku_statement = f"\nON DUPLICATE KEY UPDATE {odku}" if odku else ""

        insert_query = f"{insert_statement}\n{values_statement}\n{odku_statement}"

        rows = []
        for row in data:
            if list(row.keys()) != columns:
                raise Exception(f"Entry {row} does not have exact columns/correct order, expected {columns}")
            rows.append(list(row.values()))

        return self.connection.execute(insert_query, insert_args={'columns': columns, 'values': rows})

    def temp_table(self, structure: str):
        """
        Create temp table that will be dropped after process ends
        :param structure: Stucture of temp table, Ex. '(id INT, PRIMARY KEY(id))'
        :return: Temp Table that was created
        """
        table = f"`tmp`.`py_{datetime.now().strftime('%Y%m%d')}_{uuid4().hex}`"
        query = f"CREATE TABLE IF NOT EXISTS {table} {structure}"
        self.connection.execute(query)
        self.connection.add_temp_table(table)
        return table

    def insert_normalize(self,
                         source_table: str,
                         destination_table: str,
                         unique_columns: Union[str|List],
                         additional_columns: Union[str|List] = None,
                         odku: str = None
                         ):
        """
        Insert data from temp table to destination table
        :param source_table: Temp Table
        :param destination_table: Destination Table
        :param unique_columns: Columns from temp table that should be inserted into destination table's unique columns
        :param additional_columns: Columns from temp table that should also be inserted besides unique columns
        :param odku: Additional odku statements for additional columns, like 'ca1=VALUES(ca1), ca2=VALUES(ca2)'
        :return:
        """
        unique_columns = [unique_columns] if isinstance(unique_columns, str) else unique_columns
        if additional_columns is None:
            additional_columns = []
        else:
            additional_columns = [unique_columns] if isinstance(additional_columns, str) else additional_columns
        insert_columns = unique_columns + additional_columns

        # Create insert query
        columns_statement = "`" + "`, `".join(insert_columns) + "`"
        odku_statement = f", {odku}" if odku else ""
        insert_query = f"""
            INSERT INTO {destination_table} ({columns_statement})
            SELECT {columns_statement}
            FROM {source_table}
            ON DUPLICATE KEY UPDATE id = id {odku_statement}
        """
        self.connection.execute(insert_query)

        # Get inserted/corresponding rows
        columns_statement = "`" + "`, `".join(unique_columns) + "`"
        select_query = f"""
            SELECT DISTINCT destination.id
            FROM {source_table} source
            INNER JOIN {destination_table} destination
            USING({columns_statement})
        """

        return self.query(select_query, one_column=True)
