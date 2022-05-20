from .mysql import MySql
from typing import Union, List, Dict


def query(q: str, query_args=None):
    with MySql('prefect') as mysql:
        return mysql.query(q, query_args=query_args)

def query(q: str, query_args=None):
    with MySql('prefect') as mysql:
        return mysql.query(q, query_args=query_args)

def insert(table: str,
           data: Union[List[Dict], Dict],
           insert_type: str = 'INSERT',
           odku: str = ""):
    with MySql('prefect') as mysql:
        return mysql.insert(table, data, insert_type, odku)

