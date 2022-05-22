from prefect import flow, task, get_run_logger
import requests
import tasks
from parsel import Selector
from dateutil import parser, tz
from datetime import datetime, timedelta
from utility.mysql import MySql, format_datetime, format_number
from typing import List, Dict


@task
def parse_posts(response: requests.Response):
    selector = Selector(response.text)
    # Guest users default to pacific time...will need to check DST behavior
    dt_now = datetime.now(tz.gettz('US/Pacific'))
    date_today = dt_now.date()
    date_yesterday = date_today - timedelta(days=1)
    posts = []

    # Filter out posts older than a week
    date_limit = date_today - timedelta(days=7)

    for selector_post in selector.xpath("//tbody[@id='threadbits_forum_9']/tr"):
        if selector_post.re(r"\s*Moved:"):
            continue
        # Parse thread information from webpage
        thread_id = selector_post.re_first(r"td_threadstatusicon_(\d+)")
        category = selector_post.xpath("td[contains(@class, 'tlv3_cat')]//button/text()").get()
        title = selector_post.xpath("td/div[@class='threadtitleline']/span[@class='blueprint']/a/text()").get()
        comments, views = selector_post.re(r"Replies: (\d+), Views: ([\d,]*)")
        raw_vote_scores = selector_post.re(r"Votes: ([\d,]+) Score: ([\d,]+)")
        raw_date = selector_post.xpath(f"td[@id='td_postdate_{thread_id}']").re_first(
            r"(Today|Yesterday|\d{2}-\d{2}-\d{4})")
        raw_time = selector_post.xpath(f"td[@id='td_postdate_{thread_id}']//span[@class='time']/text()").get()
        if raw_date == 'Today':
            posted = parser.parse(f"{date_today} {raw_time}")
        elif raw_date == 'Yesterday':
            posted = parser.parse(f"{date_yesterday} {raw_time}")
        else:
            posted = parser.parse(f"{raw_date} {raw_time}")

        if posted.date() < date_limit:
            continue

        posted = posted.replace(tzinfo=tz.gettz('US/Pacific'))

        posts.append({
            'thread': thread_id,
            'category': category,
            'title': title,
            'comments': format_number(comments),
            'views': format_number(views),
            'votes': format_number(raw_vote_scores[0]) if raw_vote_scores else 0,
            'score': format_number(raw_vote_scores[1]) if raw_vote_scores else 0,
            'posted': format_datetime(posted),
            'acquired': format_datetime(dt_now),
            'age': (dt_now - posted).seconds
        })

    if not posts:
        raise Exception("Failed to extract posts from response")
    return posts


@task
def persist_posts(posts: List[Dict]):
    logger = get_run_logger()
    mysql = MySql('prefect', 'mysql.veloslab.lan')

    # Insert into temp table
    temp_table = mysql.temp_table("""(
        `thread` INT,
        category VARCHAR(255),
        title TEXT,
        comments MEDIUMINT,
        views MEDIUMINT,
        votes MEDIUMINT,
        score MEDIUMINT,
        posted DATETIME,
        age INT,
        `acquired` DATETIME,
        PRIMARY KEY (`thread`)
    )""")

    logger.info(f"Created temp table {temp_table}")
    mysql.insert(temp_table, posts)

    # Insert into report table, unique key on acquired
    mysql.query(f"""
        INSERT INTO slickdeals.report_post_meta(acquired)
        SELECT DISTINCT acquired FROM {temp_table}
        ON DUPLICATE KEY UPDATE id=VALUES(id)
    """)
    reports = mysql.query(f"""
        SELECT distinct report.id
        FROM {temp_table} temp
        INNER JOIN slickdeals.report_post_meta report
            ON temp.acquired = report.acquired
    """, one_column=True)
    logger.info(f"Created report(s) {reports}")

    # Insert into post table, unique key on thread
    mysql.query(f"""
        INSERT INTO slickdeals.post (thread, category, title, posted)
        SELECT thread, category, title, posted FROM {temp_table}
        ON DUPLICATE KEY UPDATE category=VALUES(category), title=VALUES(title)
    """)

    # Insert into post_meta table
    result = mysql.query(f"""
        INSERT INTO slickdeals.post_meta(report, post, age, comments, views, votes, score) 
        SELECT report.id, post.id, temp.age, temp.comments, temp.views, temp.votes, temp.score
        FROM {temp_table} temp
        INNER JOIN slickdeals.report_post_meta report
            ON temp.acquired = report.acquired
        INNER JOIN slickdeals.post 
            on temp.thread = post.thread
    """)
    if result.affected != len(posts):
        raise Exception(f"Row count mismatch, expected {len(posts)} but inserted {result.affected}")
    logger.info(f"Inserted {result.affected} rows")
    return reports


@flow
def slickdeals_flow():
    url = "https://slickdeals.net/forums/filtered/?f=9&sortfield=threadstarted&sortorder=desc&perpage=50"
    response = tasks.request(url=url)
    posts = parse_posts(response)
    return persist_posts(posts)


if __name__ == "__main__":
    flow_state = slickdeals_flow()
