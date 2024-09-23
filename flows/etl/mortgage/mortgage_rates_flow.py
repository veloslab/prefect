from datetime import datetime
import pandas as pd
from prefect import flow, task, get_run_logger
import requests
import tasks
from parsel import Selector
from utility.mysql import MySql, format_number
from utility.notify import Slack
import json
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
            'rate': float(rate.replace("%", "")),
            'discount_points': float(discount_points)
        })
    logger.info(f"Found {len(results)} results")
    return results

@task
def persist_results_history(results: Dict):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    mysql.insert(table='prefect.mortgage_rates_history',
                 data=results)
    logger.info(f"Persisted {len(results)} items")


@task
def persist_results_latest(results: Dict):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    mysql.insert(table='prefect.mortgage_rates_latest',
                 insert_type='REPLACE',
                 data=results)
    logger.info(f"Persisted {len(results)} items")


@task
def notify_change(results: Dict):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    df_current = pd.DataFrame(results)
    df_persisted = mysql.query(
        "SELECT bank, term, rate previous_rate, discount_points previous_discount_points FROM prefect.mortgage_rates_latest", pandas=True)
    if len(df_persisted) == 0:
        df_persisted = pd.DataFrame(columns=['bank', 'term', 'previous_rate', 'previous_discount_points'])
    df_diff = pd.merge(df_current, df_persisted, how='left', on=['bank', 'term']).query("rate != previous_rate or discount_points != previous_discount_points")
    if len(df_diff) > 0:
        message = ""
        for record in df_diff.to_dict('records'):
            message += (f"{record['bank']} - {record['term']}\nRate: {record['previous_rate']}->{record['rate']}\n"
                        f"Discount Points: {record['previous_discount_points']}->{record['discount_points']}\n\n")
        logger.info(f"Sending notification for rate changes found")
        content = f"*Mortgage Rates Update*\n" \
                  f"```{message.strip()}```"
        fallback = f"Mortgage Rates Update"
        response = Slack.post_formatted_message(
            bot_user='prefect',
            channel='mortgage-rates',

            fallback=fallback,
            content=content,
            color='dusty_blue'
        )
        if response.data['ok']:
            logger.info(f"Notification Successful")
        else:
            logger.error(f"Notification Failed")
            raise Exception(f"Notification Failed")
    else:
        logger.info(f"No rate changes found")


@flow()
def mortgage_rates_flow():
    nfcu_url = 'https://www.navyfederal.org/loans-cards/mortgage/mortgage-rates/conventional-fixed-rate-mortgages.html'
    response = tasks.request(url=nfcu_url)
    results = parse_nfcu(response=response)
    persist_results_history(results)
    notify_change(results)
    persist_results_latest(results)


if __name__ == "__main__":
    mortgage_rates_flow()


