import requests.exceptions
from prefect import flow, task, get_run_logger
from praw import Reddit
from praw.models import Submission
from datetime import datetime
import pytz
from utility.mysql import MySql, format_datetime
from utility.hashicorp import Vault
from utility.notify import Slack
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
    logger.info(search_title)
    for submission in Reddit(**credentials, check_for_async=False).subreddit(subreddit).new(limit=limit):
        post_count_total += 1
        if search_title or search_selftext:
            if search_title:
                if re.search(search_title, submission.title, re.IGNORECASE):
                    logger.info(submission.title)
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
            'posted': format_datetime(datetime.fromtimestamp(submission.created_utc))  # In UTC
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
def notify_new_submissions(subreddit: str):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')
    submissions = mysql.query(
        f"SELECT * FROM prefect.reddit_new_submissions WHERE notify = 0  and subreddit = '{subreddit}'"
    )
    if submissions:
        logger.info(f"Found {len(submissions)} submission(s) pending notification")
        for submission in submissions:
            logger.info(f"Sending notification for {submission['id']}")
            url = f"https://redd.it/{submission['id']}"
            fallback = f"/r/{subreddit} [New]: {submission['title']}"
            posted_utc = submission['posted'].replace(tzinfo=pytz.UTC)
            content = f"*/r/{subreddit}* [New]\n" \
                      f"{submission['title']}\n" \
                      f"Posted: {posted_utc.astimezone(pytz.timezone('US/Eastern'))}\n" \
                      f"<{url}|Link>"
            response = Slack.post_formatted_message(
                bot_user='prefect',
                channel='deals',
                fallback=fallback,
                content=content,
                color='orange'
            )

            if response.data['ok']:
                mysql.query(f"""
                    UPDATE prefect.reddit_new_submissions
                    SET notify = 1
                    WHERE id = '{submission['id']}'
                """)
                logger.info(f"Notification for {submission['id']} successful")
            else:
                logger.info(f"Notification for {submission['id']} failed:\n{response.text}")
                raise requests.exceptions.RequestException(f"Received {response.status_code}")
    else:
        logger.info(f"No submissions pending notification")


@flow(name="reddit-new-submissions")
def new_submissions_flow(subreddit: str, search_title: str = "", search_selftext: str = "", limit: int = 5):
    submissions = get_new_submissions(subreddit=subreddit,
                                      search_title=search_title,
                                      search_selftext=search_selftext,
                                      limit=limit)
    persist_submissions(submissions)
    notify_new_submissions(subreddit)


if __name__ == "__main__":
    new_submissions_flow('homelabsales', r'\[FS\]\s?\[us.(nova|va)\]')
