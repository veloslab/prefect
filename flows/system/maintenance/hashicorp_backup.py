from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from datetime import datetime
from utility.hashicorp import Vault
from utility.server import scp, tmp_file


@flow(task_runner=SequentialTaskRunner())
def hashicorp_vault_backup_flow():
    logger = get_run_logger()
    logger.info("Initiated Hashicorp Vault Raft Backup")
    response = Vault.generate_backup()
    dt = datetime.now()
    logger.info("Retrieved Hashicorp Vault Raft Backup")
    with tmp_file(response.content, prefix='hc_vault') as file:
        logger.info(f"Persisted Response to localhost: {file}")
        scp(file,
            f"/mnt/storage-ssd/prefect/hashicorp/vault/{dt.strftime('%Y-%m-%dT%H%M%S')}.backup.snap",
            'ssh/truenas/prefect')
        logger.info(f"Transferred Backup to Truenas: {dt.strftime('%Y-%m-%dT%H%M%S')}.backup.snap")


if __name__ == "__main__":
    flow_state = hashicorp_vault_backup_flow()
