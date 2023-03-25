from flows.etl.microcenter.open_box_flow import microcenter_open_box_flow
from flows.etl.microcenter.in_stock_flow import microcenter_in_stock_flow
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
rockville_gpu = Deployment.build_from_flow(
    flow=microcenter_open_box_flow,
    name="rockville_gpu",
    work_pool_name='veloslab',
    work_queue_name='microcenter_open_box',
    parameters={"store": "Rockville", "category": "Graphic Cards"},
    storage=storage,
    schedule=(IntervalSchedule(interval=600))
)

fairfax_z790i = Deployment.build_from_flow(
    flow=microcenter_in_stock_flow,
    name="fairfax_z790i",
    work_pool_name='veloslab',
    work_queue_name='microcenter_in_stock',
    parameters={
        "store": "Fairfax",
        "url": "https://www.microcenter.com/product/664308/msi-z790i-mpg-edge-wifi-intel-lga-1700-mini-itx-motherboard"},
    storage=storage,
    schedule=(IntervalSchedule(interval=600))
)

rockville_z790i = Deployment.build_from_flow(
    flow=microcenter_in_stock_flow,
    name="rockville_z790i",
    work_pool_name='veloslab',
    work_queue_name='microcenter_in_stock',
    parameters={
        "store": "Rockville",
        "url": "https://www.microcenter.com/product/664308/msi-z790i-mpg-edge-wifi-intel-lga-1700-mini-itx-motherboard"},
    storage=storage,
    schedule=(IntervalSchedule(interval=600))
)

fairfax_z690i = Deployment.build_from_flow(
    flow=microcenter_in_stock_flow,
    name="fairfax_z790i",
    work_pool_name='veloslab',
    work_queue_name='microcenter_in_stock',
    parameters={
        "store": "Fairfax",
        "url": "https://www.microcenter.com/product/644627/msi-z690i-meg-unify-ddr5-intel-lga-1700-mini-itx-motherboard"},
    storage=storage,
    schedule=(IntervalSchedule(interval=600))
)

rockville_z690i = Deployment.build_from_flow(
    flow=microcenter_in_stock_flow,
    name="rockville_z690i",
    work_pool_name='veloslab',
    work_queue_name='microcenter_in_stock',
    parameters={
        "store": "Rockville",
        "url": "https://www.microcenter.com/product/644627/msi-z690i-meg-unify-ddr5-intel-lga-1700-mini-itx-motherboard"},
    storage=storage,
    schedule=(IntervalSchedule(interval=600))
)

if __name__ == '__main__':
    fairfax_z690i.apply()
    fairfax_z790i.apply()
    fairfax_gpu.apply()
    rockville_z690i.apply()
    rockville_z790i.apply()
    rockville_gpu.apply()

