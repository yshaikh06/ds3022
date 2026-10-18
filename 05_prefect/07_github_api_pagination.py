"""
Work with Claude Code (CC) to complete this prefect flow.

The best way to do this is to open CC in the root dir of this
project, and your prompt can reference @05_prefect/07_github_api_pagination.py
to ask for a plan for how to complete the steps. the instructors
inline comments already add to the prompt - i.e. to paginate over that
API endpoint for a certain number of pages, and to store the output
in a single duckdb table. Ask CC to enumerate the steps you will need
to do this, and REQUIRE it to work with you on each task, one by one.

Read what CC generates/suggests with each step, and ask yourself if you
understand the python it has generated. IF YOU DO NOT UNDERSTAND IT WELL,
this is your chance to learn a bit more python! Ask CC to explain the specific
lines that are unclear to you, and have a back and forth with CC until you
get a sense for what is going on. You know about loops, and you know about
fetching remote web content, and so the growth here is understanding how
to cycle through a series of related links to build up all 1000 records.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import httpx
import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner


# commit history for the git/git repository
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/repos/git/git/commits"
PER_PAGE = 100
PAGES = 10


# ------ only one task ------
# create a single prefect task that loops through 10 pages
# of git/git commits and stores each one into a duckdb table.
# add more tasks and more data if you like, and do more with it.
# if successful you should have 1000 records in your db.

# local duckdb file next to this script, created if missing
DB_PATH = Path(__file__).parent / "actions.duckdb"
COLUMNS = ["sha", "author_name", "author_email", "author_date", "committer_name",
           "message", "html_url", "ingested_at", "raw"]

# local log file next to this script (in addition to Prefect's own logs).
# a dedicated handler is used because Prefect already configures the root logger,
# which makes logging.basicConfig() a silent no-op
log = logging.getLogger("github_pagination")
log.setLevel(logging.INFO)
_handler = logging.FileHandler(Path(__file__).with_suffix(".log"))
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(_handler)


def flatten(c, ingested_at):
    """Turn one nested commit object into a flat row tuple, keeping the full raw JSON too."""
    author, committer = c["commit"]["author"], c["commit"]["committer"]
    return (
        c["sha"],
        author["name"],
        author["email"],
        author["date"],
        committer["name"],
        c["commit"]["message"].splitlines()[0] if c["commit"]["message"] else "",  # subject line only
        c["html_url"],
        ingested_at,
        json.dumps(c),  # full original commit, queryable later via DuckDB JSON functions
    )


@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    # work with claude to fetch 100 records for each page
    # and pull 10 pages total. This could be done in series,
    # or if you want to look at previous prefect .map examples
    # you could pull some/all pages concurrently.
    #
    # for help with github api pagination see
    # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2026-03-10
    #
    # httpx parses it into response.links. The "next" URL already includes 
    # per_page and page, so params are only needed on the first request. 
    # The loop ends when there is no "next" link (last page).

    # this task already comes with a name, retry settings, and anything
    # you print() is written to the log.
    # visit https://sds-prefect.pods.uvarc.io/v2/runs to review your flows

    # if you need to see a working example of this, see this file in this repo
    # 04_apis/05_pagination.py

    try:
        # follow GitHub's "next" Link header page by page until PAGES is reached
        url, params, page = URL, {"per_page": PER_PAGE}, 1
        commits = []
        ingested_at = datetime.now(timezone.utc)  # one timestamp for the whole run
        while url and page <= PAGES:
            response = httpx.get(url, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()  # HTTP errors fail the task so retries kick in
            batch = response.json()
            commits.extend(flatten(c, ingested_at) for c in batch)
            print(f"page {page}: {len(batch)} commits (total {len(commits)})")
            log.info("page %d: %d commits (total %d)", page, len(batch), len(commits))

            url = response.links.get("next", {}).get("url")  # None on the last page
            params = None  # the next URL already carries per_page and page
            page += 1

        # bulk load: duckdb reads the DataFrame `df` directly by its variable name,
        # one INSERT for all rows instead of 1000 row-by-row inserts
        df = pd.DataFrame(commits, columns=COLUMNS)
        with duckdb.connect(str(DB_PATH)) as conn:
            # replace the table each run so it always holds exactly this run's commits
            conn.execute("""
                CREATE OR REPLACE TABLE commits (
                    sha            VARCHAR PRIMARY KEY,
                    author_name    VARCHAR,
                    author_email   VARCHAR,
                    author_date    TIMESTAMPTZ,
                    committer_name VARCHAR,
                    message        VARCHAR,
                    html_url       VARCHAR,
                    ingested_at    TIMESTAMPTZ,
                    raw            JSON
                )
            """)
            conn.execute("INSERT INTO commits SELECT * FROM df")
            count = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]

        print(f"stored {count} commits in {DB_PATH.name}")
        log.info("stored %d commits in %s", count, DB_PATH)
        return count
    except Exception:
        # write the full traceback to the local log, then re-raise so Prefect marks
        # the task failed and applies its retries
        log.exception("ingest failed on page %s", page if "page" in locals() else "?")
        raise


# name the flow something you can identify as yourself
@flow(name="vfd6jb-flow")
def main_flow():
    ingest_data()

# it doesn't seem this flow executes when you run it. Why?
# fix it ;^)

# defining a flow doesn't run it - call it only when this file is executed directly
if __name__ == "__main__":
    main_flow()
