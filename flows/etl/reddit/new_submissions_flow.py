import requests.exceptions
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from praw import Reddit
from praw.models import Submission
from datetime import datetime
from utility.mysql import MySql, format_datetime
from utility.hashicorp import Vault
from utility.notify import Pushover
from typing import Iterator
import re


@task
def get_new_submissions(subreddit: str,
                        search_title: str = None,
                        search_selftext: str = None,
                        limit: int = 5) -> Iterator[Submission]:
    """
    Get 25 latest submissions for subreddit, submissions found can be further filtered by regexes performed on title
    or selftext
    :param subreddit:
    :param search_title:
    :param search_selftext:
    :param limit:
    :return:
    """
    logger = get_run_logger()
    credentials = Vault.get_secret('reddit/api')
    post_count_total = 0
    post_count_accepted = 0
    for submission in Reddit(**credentials).subreddit(subreddit).new(limit=limit):
        post_count_total += 1
        if search_title:
            if re.search(search_title, submission.title, re.IGNORECASE):
                post_count_accepted += 1
                yield submission
        elif search_selftext:
            if re.search(search_selftext, submission.selftext, re.IGNORECASE):
                post_count_accepted += 1
                yield submission
        else:
            post_count_accepted += 1
            yield submission
    logger.info(f"Retrieved {post_count_total} submissions post(s) from /r/{subreddit}, "
                f"{post_count_accepted} meet criteria")

@task
def persist_submissions(submissions: Iterator[Submission]):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    data = []
    for submission in submissions:
        data.append({
            'id': submission.id,
            'subreddit': submission.subreddit.display_name,
            'title': submission.title,
            'posted': format_datetime(datetime.fromtimestamp(submission.created_utc))  # Converts UTC to Local
        })
    if data:
        r = mysql.insert('prefect.reddit_new_submissions', data=data, insert_type='INSERT IGNORE')
        if r.affected == 0:
            logger.info(f"No new submissions found")
            return None
        else:
            logger.info(f"Inserted {r.affected} new submissions")
            return r.affected
    else:
        logger.info(f"No submissions passed regex filters")
        return None

@task
def notify_new_submissions():
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    submissions = mysql.query("""
        SELECT * FROM prefect.reddit_new_submissions
        WHERE notify = 0 
    """)
    logger.info(f"Found {len(submissions)} submission(s) that haven't had a notification sent for yet")
    for submission in submissions:
        response = Pushover.send(app='prefect',
                                 message="New Post in /r/{submission['subreddit']}",
                                 url=f"https://redd.it/{submission['id']}",
                                 url_title=submission['title'])
        logger.info(f"Sending notification for {submission['id']}")
        if response.status_code == 200:
            mysql.query(f"""
                UPDATE prefect.reddit_new_submissions
                SET notify = 1
                WHERE id = '{submission}'
            """)
            logger.info(f"Notification for {submission['id']} successful")
        else:
            logger.info(f"Notification for {submission['id']} failed:\n{response.text}")
            raise requests.exceptions.RequestException(f"Received {response.status_code}")


@flow(task_runner=SequentialTaskRunner())
def new_submissions_flow():
    submissions = get_new_submissions()
    persist_submissions(submissions)
    notify_new_submissions()


if __name__ == "__main__":
    new_submissions_flow()