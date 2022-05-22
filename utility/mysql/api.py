from .mysql import MySql
from typing import Union, List, Dict


def query(q: str, **kwargs):
    with MySql('prefect', host='mysql.veloslab.lan') as mysql:
        return mysql.query(q, **kwargs)


def insert(table: str,
           data: Union[List[Dict], Dict],
           insert_type: str = 'INSERT',
           odku: str = ""):
    with MySql('prefect', host='mysql.veloslab.lan') as mysql:
        return mysql.insert(table, data, insert_type, odku)
