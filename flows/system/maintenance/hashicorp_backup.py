from prefect import flow, get_run_logger
from datetime import datetime
from utility.hashicorp import Vault
from utility.server import tmp_file
from utility.backblaze import Bucket


@flow()
def hashicorp_vault_backup_flow():
    logger = get_run_logger()
    logger.info("Initiated Hashicorp Vault Raft Backup")
    response = Vault.generate_backup()
    dt = datetime.now()
    logger.info("Retrieved Hashicorp Vault Raft Backup")
    with tmp_file(response.content, prefix='hc_vault') as file:
        logger.info(f"Persisted Response to localhost: {file}")
        bucket = Bucket('prefect-vault')
        bucket.upload_file(file, f"{dt.strftime('%Y-%m-%dT%H%M%S')}.backup.snap")
        logger.info(f"Transferred Backup to BackBlaze: {dt.strftime('%Y-%m-%dT%H%M%S')}.backup.snap")


if __name__ == "__main__":
    flow_state = hashicorp_vault_backup_flow()
