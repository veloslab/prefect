from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
import requests
import tasks
from parsel import Selector
from dateutil import parser, tz
from datetime import datetime, timedelta
from utility.mysql import MySql, format_number
import json
from flows.etl.microcenter.constants import MICROCENTER_CATEGORIES, MICROCENTER_STORES
from typing import List, Dict


@task
def parse_search_results(response: requests.Response):
    logger = get_run_logger()
    selector = Selector(response.text.replace('\r', '').replace('\n', ' '))
    raw_results = selector.re_first(r'<div id="productImpressions" class="hidden">(.*position.?: \d+..})')
    results = json.loads('[' + raw_results.replace("\'", "\"").strip() + ']')
    logger.info(f"Found {len(results)} results")
    return {
        i['id']: i
        for i in results
    }


@task
def persist_search_results(results: Dict, store: str, category: str):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    items_persisted = mysql.query(f"SELECT id FROM prefect.microcenter_open_box "
                                  f"WHERE store = '{store}' and category = '{category}' and available = 1",
                                  one_column=True)
    items_persisted = set(items_persisted) if items_persisted else set()

    items_current = set(list(results.keys()))
    items_not_available = items_persisted - items_current
    items_new = items_current - items_persisted

    if items_not_available:
        logger.info(f"{len(items_not_available)} items no longer available")
        mysql.query(f"UPDATE prefect.microcenter_open_box "
                    f"SET available = 0 "
                    f"WHERE id in ({','.join([str(i) for i in items_not_available])}) and store = '{store}' and "
                    f"category = '{category}'")
        logger.info(f"Items marked as not available in database")

    if items_new:
        data = [
            {
                'id': item_id,
                'store': store,
                'category': category,
                'name': results[item_id]['name'],
                'price': format_number(results[item_id]['price'], 2),
                'notify': 0,
                'available': 1
            }
            for item_id in items_new
        ]
        mysql.insert(table='prefect.microcenter_open_box',
                     data=data,
                     odku="notify=VALUES(notify), available=VALUES(available)")

        logger.info(f"Persisted/Updated {len(items_new)} items")


@flow(task_runner=SequentialTaskRunner())
def microcenter_open_box_flow(category: str, store: str):
    category_id = MICROCENTER_CATEGORIES[category]
    store_id = MICROCENTER_STORES[store]
    url = f"https://www.microcenter.com/search/search_results.aspx?N={category_id}&prt=clearance&NTK=all&sortby=match" \
          f"&rpp=96&storeid={store_id}"
    response = tasks.request(url=url)
    results = parse_search_results(response=response)
    persist_search_results(results, store, category)


if __name__ == "__main__":
    flow_state = microcenter_open_box_flow('Graphic Cards', 'Fairfax')

