from datetime import datetime

from prefect import flow, task, get_run_logger
import requests
import tasks
from parsel import Selector
from utility.mysql import MySql, format_number
from utility.notify import Slack
import json
from flows.etl.microcenter.constants import MICROCENTER_CATEGORIES, MICROCENTER_STORES
from typing import Dict


@task
def parse_nfcu(response: requests.Response):
    logger = get_run_logger()
    logger.info(f"Processing {response.request.url}")
    selector = Selector(response.text)
    results = []
    posted = datetime.now()
    for row in selector.xpath("//caption[contains(text(), 'Rates Table')]/../tbody/tr"):
        term = row.xpath("./th[contains(@data-th, 'Term')]/text()").get()
        rate = row.xpath("./td[contains(@data-th, 'Interest')]/text()").get()
        discount_points = row.xpath("./td[contains(@data-th, 'Discount Points')]/text()").get()
        if term is None or rate is None or discount_points is None:
            raise Exception(f"Could not extract all data from {row.get()}")
        results.append({
            'posted': posted,
            'bank': 'NFCU',
            'term': term,
            'rate': rate.replace("%", ""),
            'discount_points': discount_points
        })
    logger.info(f"Found {len(results)} results")
    return results

@task
def persist_results(results: Dict):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    mysql.insert(table='prefect.mortgage_rates',
                 data=results)
    logger.info(f"Persisted {len(results)} items")


@task
def notify_search_results(store: str, category: str):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    items = mysql.query(
        f"SELECT * FROM prefect.microcenter_open_box "
        f"WHERE store = '{store}' and category = '{category}' and notify = 0"
    )

    if items:
        logger.info(f"Found {len(items)} item(s) pending notification")
        for item in items:
            logger.info(f"Sending notification for {item['id']}")
            content = f"*Microcenter* [Open Box]\n{item['name']}\n" \
                      f"```Store: {item['store']}\n" \
                      f"Category: {item['category']}\n" \
                      f"<{item['url']}|Link>```"
            fallback = f"Microcenter [Open Box]: {item['name']}"
            response = Slack.post_formatted_message(
                bot_user='prefect',
                channel='deals',
                fallback=fallback,
                content=content,
                color='black'
            )

            if response.data['ok']:
                mysql.query(f"""
                    UPDATE prefect.microcenter_open_box
                    SET notify = 1
                    WHERE id = '{item['id']}' and store = '{store}' and category = '{category}'
                """)
                logger.info(f"Notification for {item['id']} successful")
            else:
                logger.info(f"Notification for {item['id']} failed:\n{response.text}")
                raise requests.exceptions.RequestException(f"Received {item}")
    else:
        logger.info(f"No items pending notification")


@flow()
def mortgage_rates_flow():
    nfcu_url = 'https://www.navyfederal.org/loans-cards/mortgage/mortgage-rates/conventional-fixed-rate-mortgages.html'
    response = tasks.request(url=nfcu_url)
    results = parse_nfcu(response=response)
    persist_results(results)


if __name__ == "__main__":
    mortgage_rates_flow()


