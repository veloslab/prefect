from flows.etl.reddit.new_submissions_flow import new_submissions_flow
from prefect.deployments import Deployment
from prefect.blocks.core import Block


storage = Block.load("github/veloslab-prefect")

buildapcsales = Deployment.build_from_flow(
    flow=new_submissions_flow,
    name="buildapcsales",
    work_pool_name='veloslab',
    parameters={"subreddit": "buildapcsales"},
    storage=storage,
)

if __name__ == "__main__":
    buildapcsales.apply()
