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
    parameters={"subreddit": "buildapcsales", "search_title": "(ssd|nvme|microcenter|itx|4080|4090|cpu|keyboard)"},
    storage=storage,
    schedule=(IntervalSchedule(interval=60))
)
hardwareswap = Deployment.build_from_flow(
    flow=new_submissions_flow,
    name="hardwareswap",
    work_pool_name='veloslab',
    work_queue_name='reddit_new_submissions',
    parameters={"subreddit": "hardwareswap",
                "search_title": r"\[USA-\w+\]\s*.*(itx|3080|4080|3070|4070).*\[W\]",
                "search_selftext": r"(z690|z690i)",
                },
    storage=storage,
    schedule=(IntervalSchedule(interval=60))
)

homelabsales = Deployment.build_from_flow(
    flow=new_submissions_flow,
    name="homelabsales",
    work_pool_name='veloslab',
    work_queue_name='reddit_new_submissions',
    parameters={"subreddit": "homelabsales", "search_title":  r"\[FS\]\s?\[us.(nova|va)\]"},
    storage=storage,
    schedule=(IntervalSchedule(interval=900))
)


if __name__ == "__main__":
    buildapcsales.apply()
    hardwareswap.apply()
