from typing import Dict
from prefect import task, get_run_logger
import re
import requests


@task(name='request', retries=3, retry_delay_seconds=5)
def request(
        url: str,
        method: str = 'get',
        headers: Dict = None,
        params: Dict = None,
        data: Dict = None,
        auth: Dict = None,
        cert: str = None
) -> requests.Response:
    """
    Get URL
    :param url:
    :param method:
    :param headers:
    :param params:
    :param data:
    :param auth:
    :param cert:
    :return:
    """

    logger = get_run_logger()

    if url is None:
        raise ValueError("Must provide url")
    url_redacted = re.sub(r"(?<=\=)[^&]+", "REDACTED", url)
    request_params = {}
    if headers:
        request_params['headers'] = headers
    if params:
        request_params['params'] = params
    if data:
        request_params['data'] = data
    if auth:
        request_params['auth'] = (auth['username'], auth['password'])
    if cert:
        request_params['cert'] = cert

    logger.info(f"Starting [{method}] to {url_redacted}")
    response = requests.request(method, url=url, **request_params)
    logger.info(f"Retrieved [{response.status_code}] from {url_redacted}")

    if response.status_code == 200:
        return response
    else:
        raise Exception(f"Received {response.status_code} response code for {url_redacted}")
