from flows.system.maintenance.hashicorp_backup import hashicorp_vault_backup_flow
from prefect.deployments import Deployment
from prefect.blocks.core import Block
from prefect.server.schemas.schedules import CronSchedule

storage = Block.load("github/veloslab-prefect")

hashicorp_vault_backup = Deployment.build_from_flow(
    flow=hashicorp_vault_backup_flow,
    name='hashicorp_vault_backup',
    work_pool_name='veloslab',
    work_queue_name='system-maintenance',
    storage=storage,
    schedule=(CronSchedule(cron="6 6 * * *"))
)

if __name__ == '__main__':
    hashicorp_vault_backup.apply()

