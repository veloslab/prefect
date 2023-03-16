from flows.etl.reddit.new_submissions_flow import new_submissions_flow
from prefect.deployments import Deployment
from prefect.blocks.core import Block
from prefect.server.schemas.schedules import IntervalSchedule


storage = Block.load("github/veloslab-prefect")

buildapcsales = Deployment.build_from_flow(
    flow=new_submissions_flow,
    name="buildapcsales",
    work_pool_name='veloslab',
    work_queue_name='reddit_new_submissions',
    parameters={"subreddit": "buildapcsales"},
    storage=storage,
    schedule=(IntervalSchedule(interval=60)),
)

if __name__ == "__main__":
    buildapcsales.apply()
