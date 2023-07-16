from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
import tasks
from requests import Response
from utility.hashicorp import Vault
from utility.server import remote_ls, scp, tmp_file
import fabric
from typing import Dict
import time


@task
def get_latest_backup(response: Response):
    existing_backups = response.json()
    if not existing_backups:
        raise Exception("No backups found on ElephantSql")
    else:
        latest_backup = existing_backups[0]
        filename = latest_backup['file_name'].split('/')[-1]
        return {
            'url': latest_backup['url'],
            'filename_lzo': filename,
            'filename_sql': filename.replace('.lzo', ''),
        }


@task
def store_backup(latest_backup: Dict, file_response: Response):
    logger = get_run_logger()
    with tmp_file(file_response.content, suffix=latest_backup['filename_lzo']) as file_lzo:
        logger.info('Saved file response to lzo file')
        with tmp_file() as file_sql:
            conn = fabric.Connection('localhost')
            result = conn.local(f"lzop -df {file_lzo} -o {file_sql}", hide=True)
            if result.failed:
                logger.error(result.stderr)
                raise Exception("Failed to uncompress lzop file")
            logger.info("Uncompressed lzo file into sql file")
            scp(file_sql,
                f"/mnt/storage-ssd/prefect/elephantsql/{latest_backup['filename_sql']}",
                'ssh/truenas/prefect')
            logger.info('Saved sql file to storage')
            return file_response


@flow(task_runner=SequentialTaskRunner())
def elephantsql_backup_flow():
    logger = get_run_logger()
    db_info = Vault.get_secret('elephantsql/alerta')

    logger.info("Requesting ElephantSQL to generated backup")
    tasks.request(
        url='https://api.elephantsql.com/api/backup',
        method='POST',
        auth={'username': None, 'password': db_info['key']},
        data={'database': db_info['database']}
    )

    logger.info("Sleeping 300 seconds, waiting for backup to be generated")
    time.sleep(300)

    logger.info("Getting latest backup from ElephantSql")
    response = tasks.request(
        url='https://api.elephantsql.com/api/backup',
        method='GET',
        auth={'username': None, 'password': db_info['key']},
    )
    latest_backup = get_latest_backup(response)
    persisted_files = remote_ls('/mnt/storage-hdd/elephantsql', 'ssh/truenas/prefect')
    if latest_backup['filename_sql'] in persisted_files:
        raise Exception("No new backup found in ElephantSql")
    file_response = tasks.request(
        url=latest_backup['url']
    )
    persisted_backup = store_backup(latest_backup, file_response)
    logger.info(f"File {persisted_backup} was saved")


if __name__ == "__main__":
    flow_state = elephantsql_backup_flow()
