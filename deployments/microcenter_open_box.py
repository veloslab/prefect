from flows.etl.microcenter.open_box_flow import microcenter_open_box_flow
from prefect.deployments import Deployment
from prefect.blocks.core import Block
from prefect.server.schemas.schedules import IntervalSchedule


storage = Block.load("github/veloslab-prefect")

fairfax_gpu = Deployment.build_from_flow(
    flow=microcenter_open_box_flow,
    name="fairfax_gpu",
    work_pool_name='veloslab',
    work_queue_name='microcenter_open_box',
    parameters={"store": "Fairfax", "category": "Graphic Cards"},
    storage=storage,
    schedule=(IntervalSchedule(interval=600))
)
