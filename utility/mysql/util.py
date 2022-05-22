from datetime import datetime, date
from typing import Union


def format_datetime(dt: datetime):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def format_date(dt: Union[date, datetime]):
    return dt.strftime('%Y-%m-%d')


def format_number(number: Union[str, float, int], precision: int = 0):
    if isinstance(number, str):
        number = float(number.replace(",", ""))
    return number
