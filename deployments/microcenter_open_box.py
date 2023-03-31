from flows.etl.microcenter.open_box_flow import microcenter_open_box_flow
from prefect.deployments import Deployment
from prefect.blocks.core import Block
from prefect.server.schemas.schedules import CronSchedule

storage = Block.load("github/veloslab-prefect")

fairfax_gpu = Deployment.build_from_flow(
    flow=microcenter_open_box_flow,
    name="fairfax_gpu",
    work_pool_name='veloslab-remote',
    work_queue_name='microcenter_open_box',
    parameters={"store": "Fairfax", "category": "Graphic Cards"},
    storage=storage,
    schedule=(CronSchedule(cron='15,30 0,12 * * *'))
)
rockville_gpu = Deployment.build_from_flow(
    flow=microcenter_open_box_flow,
    name="rockville_gpu",
    work_pool_name='veloslab-remote',
    work_queue_name='microcenter_open_box',
    parameters={"store": "Rockville", "category": "Graphic Cards"},
    storage=storage,
    schedule=CronSchedule(cron='20,30 0,12 * * *')
)


if __name__ == '__main__':
    fairfax_gpu.apply()
    rockville_gpu.apply()

