from flows.system.maintenance.elephantsql_backup_flow import elephantsql_backup_flow
from prefect.deployments import Deployment
from prefect.blocks.core import Block
from prefect.server.schemas.schedules import IntervalSchedule, CronSchedule

storage = Block.load("github/veloslab-prefect")

elephantsql_backup = Deployment.build_from_flow(
    flow=elephantsql_backup_flow,
    name='elephantsql_backup',
    work_pool_name='veloslab',
    work_queue_name='reddit_new_submissions',
    storage=storage,
    schedule=(CronSchedule(cron="15 6 * * 0,3"))
)

if __name__ == '__main__':
    elephantsql_backup.apply()

