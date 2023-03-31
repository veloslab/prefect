from flows.etl.microcenter.in_stock_flow import microcenter_in_stock_flow
from prefect.deployments import Deployment
from prefect.blocks.core import Block
from prefect.server.schemas.schedules import CronSchedule

storage = Block.load("github/veloslab-prefect")

fairfax_z690i = Deployment.build_from_flow(
    flow=microcenter_in_stock_flow,
    name="fairfax_z690i",
    work_pool_name='veloslab-remote',
    work_queue_name='microcenter_in_stock',
    parameters={
        "store": "Fairfax",
        "url": "https://www.microcenter.com/product/644627/msi-z690i-meg-unify-ddr5-intel-lga-1700-mini-itx-motherboard"},
    storage=storage,
    schedule=CronSchedule(cron='15 */4 * * *')
)

rockville_z690i = Deployment.build_from_flow(
    flow=microcenter_in_stock_flow,
    name="rockville_z690i",
    work_pool_name='veloslab-remote',
    work_queue_name='microcenter_in_stock',
    parameters={
        "store": "Rockville",
        "url": "https://www.microcenter.com/product/644627/msi-z690i-meg-unify-ddr5-intel-lga-1700-mini-itx-motherboard"},
    storage=storage,
    schedule=CronSchedule(cron='20 */4 * * *')
)

if __name__ == '__main__':
    fairfax_z690i.apply()
    rockville_z690i.apply()

