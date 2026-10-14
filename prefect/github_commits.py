"""
Fetches the last 12 complete months of commits/PRs/merges for selected python library
repos, plots trendlines of time (x-axis) vs. commit activity (y-axis), and prints a
per-repo activity summary.

Task pipeline:
  extract   -> GET /repos/{repo}/commits?since=..&until=.., paginated 100/page until
               exhausted (fanned out per repo); raw JSON landed in ./github_data/raw/
  transform -> flatten each commit to sha, repo, author, date, is_merge, message
  load      -> all repos into DuckDB table `commits` (./github_data/github_commits.duckdb)
  plot      -> monthly commit counts per repo -> ./github_data/commit_activity.png
  summarize -> total, avg/month, high/low month, and 1-5 activity score per repo

Requires a GH_PAT environment variable (authenticated limit: 5,000 req/hr).
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import matplotlib
import matplotlib.dates
matplotlib.use("Agg")  # render to file only, no GUI window
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

API = "https://api.github.com/repos/{repo}/commits"
PER_PAGE = 100          # GitHub's max page size
MONTHS = 24
MAX_PAGES = 200         # safety cap: 20,000 commits per repo
# activity score = 1 + number of these avg-commits/month thresholds a repo meets
SCORE_THRESHOLDS = [25, 75, 150, 300]
SMOOTH_WINDOW = 3       # months in the centered rolling mean used for plot trendlines
DATA_DIR = Path(__file__).parent / "github_data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "github_commits.duckdb"
PLOT_PATH = DATA_DIR / "commit_activity.png"


def month_window(months: int = MONTHS) -> tuple[datetime, datetime]:
  """Return [start, end) covering the last `months` complete calendar months, in UTC."""
  end = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
  y, m = divmod(end.year * 12 + end.month - 1 - months, 12)
  return end.replace(year=y, month=m + 1), end


@task(retries=3, retry_delay_seconds=10, name="extract")
def extract(repo: str, since: datetime, until: datetime) -> list[dict]:
  """Page through all commits in [since, until) for one repo and land the raw JSON on disk."""
  logger = get_run_logger()
  headers = {
    "Authorization": f"Bearer {os.environ['GH_PAT']}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
  }
  params = {"per_page": PER_PAGE, "since": since.isoformat(), "until": until.isoformat()}
  commits = []
  for page in range(1, MAX_PAGES + 1):
    resp = requests.get(API.format(repo=repo), headers=headers,
                        params={**params, "page": page}, timeout=30)
    try:
      resp.raise_for_status()
    except requests.HTTPError:
      # 403/429 with 0 remaining = rate limited; 404 = repo renamed/deleted/typo
      if resp.status_code in (403, 429) and resp.headers.get("X-RateLimit-Remaining") == "0":
        reset = datetime.fromtimestamp(int(resp.headers["X-RateLimit-Reset"]), timezone.utc)
        logger.error(f"{repo}: GitHub rate limit exhausted; resets at {reset:%H:%M} UTC")
      else:
        logger.error(f"{repo}: HTTP {resp.status_code} on page {page}: {resp.text[:200]}")
      raise  # network errors (timeouts, DNS) propagate as-is; both trigger task retries
    batch = resp.json()
    commits.extend(batch)
    if len(batch) < PER_PAGE:  # last page reached
      break
  else:
    logger.warning(f"{repo}: hit MAX_PAGES={MAX_PAGES}; results truncated")
  logger.info(f"{repo}: {len(commits)} commits over {page} pages "
              f"(rate limit remaining: {resp.headers.get('X-RateLimit-Remaining')})")

  # keep the untouched API response so we can re-transform without re-fetching
  try:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / f"{repo.replace('/', '__')}.json").write_text(json.dumps(commits))
  except OSError as e:
    logger.warning(f"{repo}: could not write raw JSON ({e}); continuing without it")
  return commits


@task(name="transform")
def transform(repo: str, raw: list[dict]) -> list[dict]:
  """Flatten nested commit JSON into one flat row per commit; skip malformed records."""
  rows, skipped = [], 0
  for c in raw:
    try:
      rows.append({
        "repo": repo,
        "sha": c["sha"],
        "author": c["commit"]["author"]["name"],
        "committed_at": c["commit"]["committer"]["date"],
        "is_merge": len(c["parents"]) > 1,
        "message": c["commit"]["message"].splitlines()[0] if c["commit"]["message"] else "",
      })
    except (KeyError, TypeError):  # missing field or unexpected null in the API payload
      skipped += 1
  if skipped:
    get_run_logger().warning(f"{repo}: skipped {skipped} malformed commit records")
  return rows


@task(name="load")
def load(rows: list[dict]) -> None:
  """Replace the DuckDB `commits` table with the combined rows from all repos."""
  logger = get_run_logger()
  if not rows:
    raise ValueError("No commit rows to load")
  df = pd.DataFrame(rows)
  df["committed_at"] = pd.to_datetime(df["committed_at"], utc=True)
  try:
    with duckdb.connect(str(DB_PATH)) as con:
      con.execute("CREATE OR REPLACE TABLE commits AS SELECT * FROM df")
      n = con.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
  except duckdb.IOException as e:
    # most often the file is held open by another process (DBeaver, a notebook, the duckdb CLI)
    logger.error(f"Could not open {DB_PATH.name}; is it open elsewhere? ({e})")
    raise
  logger.info(f"Loaded {n:,} rows into {DB_PATH.name}:commits")


@task(name="monthly_counts")
def monthly_counts(since: datetime, until: datetime) -> pd.DataFrame:
  """Commits per repo per month, including zero-commit months, bucketed in UTC."""
  with duckdb.connect(str(DB_PATH), read_only=True) as con:
    con.execute("SET TimeZone = 'UTC'")  # match the UTC since/until window
    return con.execute("""
      WITH months AS (
        SELECT unnest(range(?::TIMESTAMP, ?::TIMESTAMP, INTERVAL 1 MONTH)) AS month
      ),
      repos AS (SELECT DISTINCT repo FROM commits),
      counts AS (
        SELECT repo, date_trunc('month', committed_at)::TIMESTAMP AS month, COUNT(*) AS commits
        FROM commits GROUP BY ALL
      )
      SELECT r.repo, m.month, coalesce(c.commits, 0) AS commits
      FROM repos r CROSS JOIN months m
      LEFT JOIN counts c USING (repo, month)
      ORDER BY r.repo, m.month
    """, [since.replace(tzinfo=None), until.replace(tzinfo=None)]).df()


@task(name="plot")
def plot(monthly: pd.DataFrame) -> Path:
  """Small multiples, one panel per repo (busiest first): smoothed trendline + raw monthly
  dots, with every other repo drawn faintly behind for context. Shared axes so panels compare."""
  # centered rolling mean per repo; min_periods=1 keeps the first/last months
  monthly = monthly.assign(smoothed=monthly.groupby("repo")["commits"].transform(
    lambda s: s.rolling(SMOOTH_WINDOW, center=True, min_periods=1).mean()))
  order = monthly.groupby("repo")["commits"].sum().sort_values(ascending=False)
  by_repo = {r: g for r, g in monthly.groupby("repo")}

  # one accent hue + neutral chrome; too many repos for distinguishable per-repo colors
  accent, context, ink, muted, grid = "#2a78d6", "#d6d5ce", "#0b0b0b", "#898781", "#e1e0d9"
  ncols = 5
  nrows = -(-len(order) // ncols)  # ceiling division
  fig, axes = plt.subplots(nrows, ncols, figsize=(3 * ncols, 2.4 * nrows),
                           sharex=True, sharey=True, facecolor="white")
  for ax, (repo, total) in zip(axes.flat, order.items()):
    for other, g in by_repo.items():
      if other != repo:
        ax.plot(g["month"], g["smoothed"], color=context, lw=0.8, zorder=1)
    g = by_repo[repo]
    ax.scatter(g["month"], g["commits"], color=accent, s=10, alpha=0.35, lw=0, zorder=2)
    ax.plot(g["month"], g["smoothed"], color=accent, lw=2, zorder=3)
    ax.text(0, 1.0, f"{total:,} commits", transform=ax.transAxes, ha="left", va="bottom",
            fontsize=8, color=muted)
    ax.set_title(repo, loc="left", fontsize=10, color=ink, fontweight="bold", pad=14)
    # recessive chrome: horizontal hairline grid, no box
    ax.grid(axis="y", color=grid, lw=0.6)
    ax.set_axisbelow(True)
    sns.despine(ax=ax, left=True)
    ax.spines["bottom"].set_color("#c3c2b7")
    ax.tick_params(colors=muted, labelsize=8, length=0)
  for i, ax in enumerate(axes.flat[len(order):], start=len(order)):  # hide unused grid slots
    ax.set_visible(False)
    axes.flat[i - ncols].tick_params(labelbottom=True)  # panel above now needs its own x labels

  axes.flat[0].set_ylim(0, None)
  axes.flat[0].xaxis.set_major_locator(matplotlib.dates.MonthLocator(bymonth=[1, 4, 7, 10]))
  axes.flat[0].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b '%y"))
  first, last = monthly["month"].min(), monthly["month"].max()
  fig.suptitle("Monthly commits per repo", x=0.01, y=0.99, ha="left", va="top", fontsize=14,
               fontweight="bold", color=ink)
  fig.text(0.01, 0.935, f"{first:%b %Y} - {last:%b %Y} · line = {SMOOTH_WINDOW}-month rolling "
           "mean, dots = raw monthly counts, gray = other repos", fontsize=9, color=muted)
  fig.tight_layout(rect=(0, 0, 1, 0.93))
  fig.savefig(PLOT_PATH, dpi=150)
  plt.close(fig)
  return PLOT_PATH


@task(name="summarize")
def summarize(monthly: pd.DataFrame) -> pd.DataFrame:
  """Per-repo total, avg/month, high/low month, and a 1-5 activity score."""
  rows = []
  for repo, g in monthly.groupby("repo"):
    hi, lo = g.loc[g["commits"].idxmax()], g.loc[g["commits"].idxmin()]
    avg = g["commits"].mean()
    rows.append({
      "repo": repo,
      "total": int(g["commits"].sum()),
      "avg/month": round(avg, 1),
      "high month": f"{hi['month']:%b %Y} ({hi['commits']})",
      "low month": f"{lo['month']:%b %Y} ({lo['commits']})",
      "score (1-5)": 1 + sum(avg >= t for t in SCORE_THRESHOLDS),
    })
  return pd.DataFrame(rows).sort_values("total", ascending=False)


@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=16), name="commit-etl")
def etl(repos: list[str]):
  logger = get_run_logger()
  # fail fast rather than burning task retries on a missing token
  if not os.environ.get("GH_PAT"):
    raise RuntimeError("GH_PAT environment variable is not set")
  since, until = month_window()
  try:
    # fan out: one extract per repo, all submitted concurrently
    futures = extract.map(repos, since=since, until=until)
    # collect results; one bad repo (renamed, deleted, out of retries) shouldn't sink the rest
    fetched = {}
    for repo, fut in zip(repos, futures):
      try:
        fetched[repo] = fut.result()
      except Exception as e:
        logger.warning(f"Skipping {repo}: extract failed ({e})")
    if not fetched:
      raise RuntimeError("Every repo failed to extract; nothing to load")

    clean = transform.map(list(fetched), list(fetched.values()))
    load([row for rows in clean.result() for row in rows])

    monthly = monthly_counts(since, until)
    try:
      print(f"Plot saved to {plot(monthly)}")
    except Exception as e:
      logger.warning(f"Plot failed ({e}); continuing to summary")
    summary = summarize(monthly)
    print(f"\nCommit activity {since:%b %Y} - {until:%b %Y} (exclusive)\n"
          f"Score thresholds (avg commits/month): {SCORE_THRESHOLDS}\n"
          f"{summary.to_string(index=False)}\n")
  except Exception as e:
    logger.error(f"Flow failed: {e}")
    raise


if __name__ == "__main__":
  # packages
  etl([
    "pandas-dev/pandas",
    "pola-rs/polars",
    "scikit-learn/scikit-learn",
    "matplotlib/matplotlib",
    "boto/boto3",
    "numpy/numpy",
    "scipy/scipy",
    "PrefectHQ/prefect",
    "apache/airflow",
    "astral-sh/uv",
    "pypa/pipenv",
    "mwaskom/seaborn",
    "psf/requests",
    "encode/httpx",
  ])
