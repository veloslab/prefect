from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
import requests
import tasks
from parsel import Selector
from utility.mysql import MySql
from utility.notify import Slack
from flows.etl.microcenter.constants import MICROCENTER_STORES
from typing import Dict


@task
def parse_response(response: requests.Response) -> Dict:
    logger = get_run_logger()
    logger.info(f"Processing {response.request.url}")
    selector = Selector(response.text.replace('\r', '').replace('\n', ' '))

    # Extract Item Details
    item_metadata = selector.xpath("//div[@id='product-details-control']//div[@class='product-header']//span").attrib
    if not item_metadata:
        raise ValueError("Unable to find xpath //div[@id='product-details-control']//div[@class='product-header']//span in response")
    logger.info(f"Found following item metadata: {item_metadata}")

    # Extract Item Status
    html_status = selector.xpath("//div[@id='pnlInventory']")
    if html_status is None:
        raise ValueError('Unable to find xpath //div[@id=\'pnlInventory\'] in response')
    item_status = html_status.re_first("(IN STOCK|SOLD OUT)")

    if item_status:
        logger.info(f"Item has '{item_status.title()}' status")
        status = item_status.title()
    else:
        logger.warning(f"Unable to extract status from following:\n{html_status}")
        raise ValueError('Didn\'t find expected status of \'IN STOCK\' or \'SOLD OUT\'')

    return {
        'id': item_metadata['data-id'],
        'name': item_metadata['data-name'],
        'status': status,
    }


@task
def persist_item_status(store: str, url: str, data: Dict):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    status_persisted = mysql.query(f"SELECT status FROM prefect.microcenter_in_stock "
                                   f"WHERE store = '{store}' and id = '{data['id']}'",
                                   one_value=True)

    if status_persisted == data['status']:
        logger.info(f"Item status ({status_persisted}) has not changed, no need to update table")
    else:
        data = {
            'id': data['id'],
            'store': store,
            'name': data['name'],
            'url': url,
            'status': data['status'],
            'notify': 0,
        }
        mysql.insert(table='prefect.microcenter_in_stock',
                     data=data,
                     odku="notify=VALUES(notify), status=VALUES(status)")

        logger.info(f"Persisted/Updated {data}")


def notify_item_status(store: str, item_id: str):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')

    items = mysql.query(
        f"SELECT * FROM prefect.microcenter_in_stock "
        f"WHERE store = '{store}' and id = '{item_id}' and notify = 0 and status != 'Sold Out'"
    )

    if items:
        logger.info(f"Found {len(items)} items(s) pending notification")
        for item in items:
            logger.info(f"Sending notification for {item['id']}")
            content = f"*Microcenter* [In Stock]\n{item['name']}\n" \
                      f"```Store: {item['store']}\n" \
                      f"Status: {item['status']}\n" \
                      f"<{item['url']}|Link>```"
            fallback = f"Microcenter [In Stock]: {item['name']}"
            response = Slack.post_formatted_message(
                bot_user='prefect',
                channel='deals',
                fallback=fallback,
                content=content,
                color='black'
            )

            if response.data['ok']:
                mysql.query(f"""
                    UPDATE prefect.microcenter_in_stock
                    SET notify = 1
                    WHERE id = '{item['id']}' and store = '{store}'
                """)
                logger.info(f"Notification for {item['id']} successful")
            else:
                logger.info(f"Notification for {item['id']} failed:\n{response.text}")
                raise requests.exceptions.RequestException(f"Received {item}")
    else:
        logger.info(f"No items pending notification")


@flow(task_runner=SequentialTaskRunner())
def microcenter_in_stock_flow(store: str, url: str):
    store_id = MICROCENTER_STORES[store]
    url = f"{url}?storeid={store_id}"
    response = tasks.request(url=url)
    item_data = parse_response(response=response)
    persist_item_status(store, url, item_data)
    notify_item_status(store, item_data['id'])


if __name__ == "__main__":
    item_url = "https://www.microcenter.com/product/651471/asus-x670e-e-rog-strix-gaming-wifi-amd-am5-atx-motherboard"
    flow_state = microcenter_in_stock_flow('Fairfax', item_url)

