# prefect
Scraping Infrastructure for Homelab


### Utility
Directory holds various clients to interact with services (e.g. hashicorp vault, mysql, alerta)

### Flows
| directory      | flow                   | description                                                                                                          |
|----------------|------------------------|----------------------------------------------------------------------------------------------------------------------|
| etl/slickdeals | slickdeals_flow.py     | scrape and persist 50 latest slickdeals posts and metadata                                                           |
| etl/reddit     | new_submissions_flow.py | scrape and notify of new posts made on a subreddit. Post can be filtered through regexes on title or content of post |
| etl/microcenter| in_stock_flow.py       | scrapes item page for a specific store and notifies when item is in stock                                            |
| etl/microcenter| open_box_flow.py       | scrape and notify of open box items for a category of items at specific microcenter store                            |


