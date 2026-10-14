"""
Fetches the last 24 complete months of commits, merged PRs, and closed issues for selected
python library repos, plots per-repo trendlines for all three dimensions, and prints a
per-repo activity summary with year-over-year change.

Task pipeline:
  extract_commits -> GET /repos/{repo}/commits?since=..&until=..   (paginated, per repo)
  extract_issues  -> GET /repos/{repo}/issues?state=closed&since=.. (paginated, per repo)
                     The issues endpoint returns both issues and PRs; PR items carry
                     pull_request.merged_at, so one call yields merged PRs AND closed issues.
                     Raw JSON for both lands in ./github_data/raw/{commits,issues}/
  transform       -> commits: sha, repo, author, date, is_merge, message
                     items:   repo, number, kind (merged_pr | closed_issue), event_at, title
  load            -> DuckDB tables `commits` and `items` (./github_data/github_activity.duckdb)
  plot            -> monthly counts per repo, one colored line per dimension
                     -> ./github_data/activity_trends.png
  plot_vitality   -> bubble timeline: area = human contributors/month, color = merged-PR
                     momentum vs. repo's own average -> ./github_data/vitality_bubbles.png
  summarize       -> totals, avg/month, YoY % change per dimension, 1-5 commit activity score

Requires a GH_PAT environment variable (authenticated limit: 5,000 req/hr).
Env flags: REFRESH=0 reuses the local DuckDB (skips extract/transform/load);
           MONTHS=N sets the window (default 12).
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
import numpy as np
import pandas as pd
import requests
import seaborn as sns
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

COMMITS_API = "https://api.github.com/repos/{repo}/commits"
ISSUES_API = "https://api.github.com/repos/{repo}/issues"
PER_PAGE = 100          # GitHub's max page size
MONTHS = int(os.environ.get("MONTHS", 12))  # override for quick test runs, e.g. MONTHS=3
# REFRESH=1 (default) re-pulls from the GitHub API; REFRESH=0 skips extract/transform/load
# and re-plots/summarizes from the existing local DuckDB file (no API calls, no GH_PAT needed)
REFRESH = os.environ.get("REFRESH", "1").strip().lower() not in ("0", "false", "no")
MAX_PAGES = 500         # safety cap: 50,000 records per repo per endpoint
# activity score = 1 + number of these avg-commits/month thresholds a repo meets
SCORE_THRESHOLDS = [25, 75, 150, 300]
SMOOTH_WINDOW = 3       # months in the centered rolling mean used for plot trendlines
# dimension -> (legend label, color); fixed order so each dimension keeps its hue in every panel
METRICS = {
  "commits":       ("Commits",       "#2a78d6"),
  "merged_prs":    ("Merged PRs",    "#eb6834"),
  "closed_issues": ("Closed issues", "#1baf7a"),
}
DATA_DIR = Path(__file__).parent / "github_data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "github_activity.duckdb"
PLOT_PATH = DATA_DIR / "activity_trends.png"
VITALITY_PATH = DATA_DIR / "vitality_bubbles.png"
# vitality bubbles: diverging red (below repo's own avg) -> gray (at avg) -> blue (above)
DIVERGING = ["#c43d3d", "#c9c8c0", "#1c5cab"]
MOMENTUM_CLIP = 1.0     # log2 color range: +/-1 = 0.5x .. 2x the repo's average


def month_window(months: int = MONTHS) -> tuple[datetime, datetime]:
  """Return [start, end) covering the last `months` complete calendar months, in UTC."""
  end = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
  y, m = divmod(end.year * 12 + end.month - 1 - months, 12)
  return end.replace(year=y, month=m + 1), end


def fetch_all(url: str, params: dict, repo: str, label: str) -> list[dict]:
  """Page through a GitHub list endpoint until a short page; log rate-limit / HTTP errors."""
  logger = get_run_logger()
  headers = {
    "Authorization": f"Bearer {os.environ['GH_PAT']}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
  }
  records = []
  for page in range(1, MAX_PAGES + 1):
    resp = requests.get(url, headers=headers,
                        params={**params, "per_page": PER_PAGE, "page": page}, timeout=30)
    try:
      resp.raise_for_status()
    except requests.HTTPError:
      # 403/429 with 0 remaining = rate limited; 404 = repo renamed/deleted/typo
      if resp.status_code in (403, 429) and resp.headers.get("X-RateLimit-Remaining") == "0":
        reset = datetime.fromtimestamp(int(resp.headers["X-RateLimit-Reset"]), timezone.utc)
        logger.error(f"{repo}: GitHub rate limit exhausted; resets at {reset:%H:%M} UTC")
      else:
        logger.error(f"{repo} {label}: HTTP {resp.status_code} on page {page}: {resp.text[:200]}")
      raise  # network errors (timeouts, DNS) propagate as-is; both trigger task retries
    batch = resp.json()
    records.extend(batch)
    if len(batch) < PER_PAGE:  # last page reached
      break
  else:
    logger.warning(f"{repo} {label}: hit MAX_PAGES={MAX_PAGES}; results truncated")
  logger.info(f"{repo}: {len(records)} {label} over {page} pages "
              f"(rate limit remaining: {resp.headers.get('X-RateLimit-Remaining')})")

  # keep the untouched API response so we can re-transform without re-fetching
  try:
    out = RAW_DIR / label
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{repo.replace('/', '__')}.json").write_text(json.dumps(records))
  except OSError as e:
    logger.warning(f"{repo}: could not write raw {label} JSON ({e}); continuing without it")
  return records


@task(retries=3, retry_delay_seconds=10, name="extract_commits")
def extract_commits(repo: str, since: datetime, until: datetime) -> list[dict]:
  """All commits on the default branch in [since, until)."""
  return fetch_all(COMMITS_API.format(repo=repo),
                   {"since": since.isoformat(), "until": until.isoformat()}, repo, "commits")


@task(retries=3, retry_delay_seconds=10, name="extract_issues")
def extract_issues(repo: str, since: datetime) -> list[dict]:
  """Closed issues + PRs updated since `since`. `since` filters on updated_at, which is always
  >= closed_at, so this is a superset of everything closed in the window (trimmed in SQL)."""
  return fetch_all(ISSUES_API.format(repo=repo),
                   {"state": "closed", "since": since.isoformat(),
                    "sort": "updated", "direction": "desc"}, repo, "issues")


@task(name="transform_commits")
def transform_commits(repo: str, raw: list[dict]) -> list[dict]:
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


@task(name="transform_issues")
def transform_issues(repo: str, raw: list[dict]) -> list[dict]:
  """Split issue-endpoint items into merged PRs (by merged_at) and closed issues (by closed_at).
  PRs closed without merging are dropped."""
  rows, skipped = [], 0
  for it in raw:
    try:
      pr = it.get("pull_request")
      if pr:
        if not pr.get("merged_at"):
          continue  # closed, never merged
        kind, event_at = "merged_pr", pr["merged_at"]
      else:
        kind, event_at = "closed_issue", it["closed_at"]
      rows.append({"repo": repo, "number": it["number"], "kind": kind,
                   "event_at": event_at, "title": it["title"]})
    except (KeyError, TypeError):
      skipped += 1
  if skipped:
    get_run_logger().warning(f"{repo}: skipped {skipped} malformed issue/PR records")
  return rows


@task(name="load")
def load(commits: list[dict], items: list[dict]) -> None:
  """Replace the DuckDB `commits` and `items` tables with combined rows from all repos."""
  logger = get_run_logger()
  if not commits and not items:
    raise ValueError("No rows to load")
  cdf = pd.DataFrame(commits, columns=["repo", "sha", "author", "committed_at", "is_merge", "message"])
  idf = pd.DataFrame(items, columns=["repo", "number", "kind", "event_at", "title"])
  cdf["committed_at"] = pd.to_datetime(cdf["committed_at"], utc=True)
  idf["event_at"] = pd.to_datetime(idf["event_at"], utc=True)
  try:
    with duckdb.connect(str(DB_PATH)) as con:
      con.execute("CREATE OR REPLACE TABLE commits AS SELECT * FROM cdf")
      con.execute("CREATE OR REPLACE TABLE items AS SELECT * FROM idf")
  except duckdb.IOException as e:
    # most often the file is held open by another process (DBeaver, a notebook, the duckdb CLI)
    logger.error(f"Could not open {DB_PATH.name}; is it open elsewhere? ({e})")
    raise
  logger.info(f"Loaded {len(cdf):,} commits and {len(idf):,} items into {DB_PATH.name}")


@task(name="monthly_counts")
def monthly_counts(since: datetime, until: datetime) -> pd.DataFrame:
  """Long table (repo, month, metric, n) for every repo x month x metric, zero-filled, UTC."""
  with duckdb.connect(str(DB_PATH), read_only=True) as con:
    con.execute("SET TimeZone = 'UTC'")  # match the UTC since/until window
    return con.execute("""
      WITH months AS (
        SELECT unnest(range(?::TIMESTAMP, ?::TIMESTAMP, INTERVAL 1 MONTH)) AS month
      ),
      events AS (
        SELECT repo, 'commits' AS metric, committed_at AS ts FROM commits
        UNION ALL
        SELECT repo, CASE kind WHEN 'merged_pr' THEN 'merged_prs' ELSE 'closed_issues' END, event_at
        FROM items
      ),
      counts AS (
        SELECT repo, metric, date_trunc('month', ts)::TIMESTAMP AS month, COUNT(*) AS n
        FROM events GROUP BY ALL
      ),
      grid AS (
        SELECT * FROM (SELECT DISTINCT repo FROM events)
        CROSS JOIN (SELECT unnest(['commits', 'merged_prs', 'closed_issues']) AS metric)
        CROSS JOIN months
      )
      -- the LEFT JOIN onto the month grid also drops items closed before the window
      SELECT g.repo, g.month, g.metric, coalesce(c.n, 0) AS n
      FROM grid g LEFT JOIN counts c USING (repo, metric, month)
      ORDER BY g.repo, g.metric, g.month
    """, [since.replace(tzinfo=None), until.replace(tzinfo=None)]).df()


@task(name="plot")
def plot(monthly: pd.DataFrame) -> Path:
  """Small multiples, one panel per repo (busiest by commits first), one smoothed trendline
  per dimension plus faint raw monthly dots. Y-axes are independent per panel because repo
  volumes differ by ~100x; compare repos via the totals printed above each panel."""
  # centered rolling mean per repo+metric; min_periods=1 keeps the first/last months
  monthly = monthly.assign(smoothed=monthly.groupby(["repo", "metric"])["n"].transform(
    lambda s: s.rolling(SMOOTH_WINDOW, center=True, min_periods=1).mean()))
  totals = monthly.pivot_table(index="repo", columns="metric", values="n", aggfunc="sum")
  # fixed 3x4 grid: plot the 12 busiest repos by commits; the rest stay in the summary table
  nrows, ncols = 3, 4
  ranked = totals.sort_values("commits", ascending=False)
  order, omitted = ranked.head(nrows * ncols), list(ranked.index[nrows * ncols:])
  if omitted:
    get_run_logger().info(f"Plot shows top {nrows * ncols} repos; omitted: {', '.join(omitted)}")

  ink, muted, grid = "#0b0b0b", "#898781", "#e1e0d9"
  fig, axes = plt.subplots(nrows, ncols, figsize=(4.6 * ncols, 3.6 * nrows),
                           sharex=True, sharey=False, facecolor="white", squeeze=False)
  for ax, (repo, t) in zip(axes.flat, order.iterrows()):
    g = monthly[monthly["repo"] == repo]
    for metric, (label, color) in METRICS.items():
      m = g[g["metric"] == metric]
      ax.scatter(m["month"], m["n"], color=color, s=8, alpha=0.25, lw=0, zorder=2)
      ax.plot(m["month"], m["smoothed"], color=color, lw=2, zorder=3, label=label)
    ax.text(0, 1.0, f"{t['commits']:,.0f} commits · {t['merged_prs']:,.0f} PRs · "
            f"{t['closed_issues']:,.0f} issues", transform=ax.transAxes, ha="left",
            va="bottom", fontsize=9, color=muted)
    ax.set_title(repo, loc="left", fontsize=12, color=ink, fontweight="bold", pad=16)
    ax.set_ylim(0, None)
    # recessive chrome: horizontal hairline grid, no box
    ax.grid(axis="y", color=grid, lw=0.6)
    ax.set_axisbelow(True)
    sns.despine(ax=ax, left=True)
    ax.spines["bottom"].set_color("#c3c2b7")
    ax.tick_params(colors=muted, labelsize=9, length=0)
  for i, ax in enumerate(axes.flat[len(order):], start=len(order)):  # hide unused grid slots
    ax.set_visible(False)
    axes.flat[i - ncols].tick_params(labelbottom=True)  # panel above now needs its own x labels

  # tick density by window: monthly (<=6 mo), quarterly (<=12 mo), else Jan/Jul
  ticks = range(1, 13) if MONTHS <= 6 else [1, 4, 7, 10] if MONTHS <= 12 else [1, 7]
  axes.flat[0].xaxis.set_major_locator(matplotlib.dates.MonthLocator(bymonth=ticks))
  axes.flat[0].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b '%y"))
  first, last = monthly["month"].min(), monthly["month"].max()
  fig.suptitle("Monthly commits, merged PRs, and closed issues per repo", x=0.01, y=0.99,
               ha="left", va="top", fontsize=16, fontweight="bold", color=ink)
  fig.text(0.01, 0.95, f"{first:%b %Y} - {last:%b %Y} · lines = {SMOOTH_WINDOW}-month rolling "
           "mean, dots = raw monthly counts · y-axis scaled per repo"
           + (f" · not shown: {', '.join(omitted)}" if omitted else ""), fontsize=10, color=muted)
  # one shared legend, top right, text in ink rather than series color
  handles, labels = axes.flat[0].get_legend_handles_labels()
  fig.legend(handles, labels, loc="upper right", bbox_to_anchor=(0.995, 0.995), ncol=3,
             frameon=False, fontsize=11, labelcolor=ink)
  fig.tight_layout(rect=(0, 0, 1, 0.93))
  fig.savefig(PLOT_PATH, dpi=150)
  plt.close(fig)
  return PLOT_PATH


@task(name="vitality")
def vitality(since: datetime, until: datetime) -> pd.DataFrame:
  """Per repo-month: distinct human commit authors (bots excluded) and merged PRs, zero-filled,
  plus momentum = log2(merged PRs / the repo's own monthly average over the window)."""
  with duckdb.connect(str(DB_PATH), read_only=True) as con:
    con.execute("SET TimeZone = 'UTC'")
    df = con.execute("""
      WITH months AS (
        SELECT unnest(range(?::TIMESTAMP, ?::TIMESTAMP, INTERVAL 1 MONTH)) AS month
      ),
      repos AS (SELECT DISTINCT repo FROM commits UNION SELECT DISTINCT repo FROM items),
      contrib AS (
        SELECT repo, date_trunc('month', committed_at)::TIMESTAMP AS month,
               COUNT(DISTINCT author) AS contributors
        FROM commits WHERE author NOT ILIKE '%[bot]%' GROUP BY ALL
      ),
      prs AS (
        SELECT repo, date_trunc('month', event_at)::TIMESTAMP AS month, COUNT(*) AS merged_prs
        FROM items WHERE kind = 'merged_pr' GROUP BY ALL
      )
      SELECT r.repo, m.month, coalesce(c.contributors, 0) AS contributors,
             coalesce(p.merged_prs, 0) AS merged_prs
      FROM repos r CROSS JOIN months m
      LEFT JOIN contrib c USING (repo, month)
      LEFT JOIN prs p USING (repo, month)
      ORDER BY r.repo, m.month
    """, [since.replace(tzinfo=None), until.replace(tzinfo=None)]).df()
  # each repo is measured against itself, so small and large repos share one color scale;
  # repos with no PRs in the window get NaN (drawn neutral gray)
  avg = df.groupby("repo")["merged_prs"].transform("mean")
  ratio = (df["merged_prs"] / avg.where(avg > 0)).clip(lower=2 ** -MOMENTUM_CLIP)
  df["momentum"] = np.log2(ratio).clip(-MOMENTUM_CLIP, MOMENTUM_CLIP)
  return df


@task(name="plot_vitality")
def plot_vitality(df: pd.DataFrame) -> Path:
  """Bubble timeline: one row per repo, one bubble per month. Area = active human contributors,
  color = merged-PR momentum vs. the repo's own average (diverging, gray = typical month)."""
  ink, muted, grid = "#0b0b0b", "#898781", "#e1e0d9"
  # largest contributor base at the top
  order = df.groupby("repo")["contributors"].mean().sort_values().index.tolist()
  ypos = {r: i for i, r in enumerate(order)}
  # area proportional to contributors; cap at the 95th pct so one huge repo doesn't swamp the rest
  cap = max(df["contributors"].quantile(0.95), 1)
  smax = 900
  sizes = lambda v: np.clip(np.minimum(v, cap) / cap * smax, 6, None)
  cmap = matplotlib.colors.LinearSegmentedColormap.from_list("div", DIVERGING)
  norm = matplotlib.colors.Normalize(-MOMENTUM_CLIP, MOMENTUM_CLIP)

  fig, ax = plt.subplots(figsize=(15, 0.62 * len(order) + 2.6), facecolor="white")
  for r in order:  # faint row guides
    ax.axhline(ypos[r], color=grid, lw=0.6, zorder=1)
  colors = [cmap(norm(m)) if pd.notna(m) else DIVERGING[1] for m in df["momentum"]]
  # white edge = surface ring so overlapping bubbles stay separable
  ax.scatter(df["month"], df["repo"].map(ypos), s=sizes(df["contributors"]), c=colors,
             alpha=0.75, edgecolors="white", linewidths=0.8, zorder=2)

  ax.set_yticks(range(len(order)), order)
  ax.set_ylim(-0.7, len(order) - 0.3)
  ticks = range(1, 13) if MONTHS <= 12 else [1, 4, 7, 10]
  ax.xaxis.set_major_locator(matplotlib.dates.MonthLocator(bymonth=ticks))
  ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b '%y"))
  ax.margins(x=0.03)
  sns.despine(ax=ax, left=True, bottom=True)
  ax.tick_params(colors=ink, labelsize=10, length=0)
  ax.tick_params(axis="x", colors=muted, labelsize=9)

  # size legend: three reference bubbles
  refs = sorted({max(1, round(cap * f)) for f in (0.1, 0.4, 1.0)})
  handles = [ax.scatter([], [], s=sizes(np.array([v]))[0], color=DIVERGING[1], alpha=0.75,
                        edgecolors="white") for v in refs]
  labels = [f"{v}{'+' if v == refs[-1] else ''} contributors" for v in refs]
  fig.legend(handles, labels, loc="upper right", bbox_to_anchor=(0.995, 0.995), ncol=len(refs),
             frameon=False, fontsize=10, labelcolor=ink, columnspacing=2.2, handletextpad=0.8)
  # color legend
  cax = fig.add_axes([0.70, 0.905, 0.22, 0.018])
  cb = fig.colorbar(matplotlib.cm.ScalarMappable(norm=norm, cmap=cmap), cax=cax,
                    orientation="horizontal")
  cb.set_ticks([-MOMENTUM_CLIP, 0, MOMENTUM_CLIP],
               labels=[f"≤{2 ** -MOMENTUM_CLIP:g}× avg", "repo avg", f"≥{2 ** MOMENTUM_CLIP:g}× avg"])
  cb.ax.tick_params(labelsize=9, colors=muted, length=0)
  cb.outline.set_visible(False)
  cb.ax.set_title("merged PRs vs. repo's own monthly average", fontsize=9, color=muted, loc="left")

  first, last = df["month"].min(), df["month"].max()
  fig.suptitle("Repo vitality over time", x=0.01, y=0.99, ha="left", va="top", fontsize=16,
               fontweight="bold", color=ink)
  fig.text(0.01, 0.945, f"{first:%b %Y} - {last:%b %Y} · bubble area = distinct human commit "
           f"authors per month (bots excluded, capped at {cap:.0f}) · color = merged-PR momentum",
           fontsize=10, color=muted, va="top")
  fig.tight_layout(rect=(0, 0, 1, 0.885))
  fig.savefig(VITALITY_PATH, dpi=150)
  plt.close(fig)
  return VITALITY_PATH


@task(name="summarize")
def summarize(monthly: pd.DataFrame) -> pd.DataFrame:
  """Per repo: totals and avg/month for each dimension, YoY % change (last 12 months vs. the
  12 before), commits per merged PR, and a 1-5 commit activity score."""
  def yoy(s: pd.Series) -> str:
    prev, curr = s.iloc[:-12].sum(), s.iloc[-12:].sum()
    return f"{(curr - prev) / prev:+.0%}" if prev else "n/a"

  rows = []
  for repo, g in monthly.groupby("repo"):
    w = g.pivot(index="month", columns="metric", values="n").sort_index()
    avg = w["commits"].mean()
    rows.append({
      "repo": repo,
      "commits": int(w["commits"].sum()),
      "merged PRs": int(w["merged_prs"].sum()),
      "closed issues": int(w["closed_issues"].sum()),
      "commits/mo": round(avg, 1),
      "PRs/mo": round(w["merged_prs"].mean(), 1),
      "issues/mo": round(w["closed_issues"].mean(), 1),
      "commits YoY": yoy(w["commits"]),
      "PRs YoY": yoy(w["merged_prs"]),
      "issues YoY": yoy(w["closed_issues"]),
      # ~1 means squash-merge workflow (1 commit per PR); >1 suggests direct pushes or merge commits
      "commits/PR": round(w["commits"].sum() / w["merged_prs"].sum(), 2) if w["merged_prs"].sum() else None,
      "score (1-5)": 1 + sum(avg >= t for t in SCORE_THRESHOLDS),
    })
  return pd.DataFrame(rows).sort_values("commits", ascending=False)


def extract_and_load(repos: list[str], since: datetime, until: datetime) -> None:
  """Pull commits + issues for every repo from the API and replace the DuckDB tables."""
  logger = get_run_logger()
  # fail fast rather than burning task retries on a missing token
  if not os.environ.get("GH_PAT"):
    raise RuntimeError("GH_PAT environment variable is not set")
  # fan out: one commits + one issues extract per repo, all submitted concurrently
  c_futs = extract_commits.map(repos, since=since, until=until)
  i_futs = extract_issues.map(repos, since=since)
  # collect results; one bad repo (renamed, deleted, out of retries) shouldn't sink the rest
  fetched = {}
  for repo, cf, itf in zip(repos, c_futs, i_futs):
    try:
      fetched[repo] = (cf.result(), itf.result())
    except Exception as e:
      logger.warning(f"Skipping {repo}: extract failed ({e})")
  if not fetched:
    raise RuntimeError("Every repo failed to extract; nothing to load")

  names = list(fetched)
  commits = transform_commits.map(names, [c for c, _ in fetched.values()])
  items = transform_issues.map(names, [i for _, i in fetched.values()])
  load([r for rows in commits.result() for r in rows],
       [r for rows in items.result() for r in rows])


@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=16), name="activity-etl")
def etl(repos: list[str]):
  logger = get_run_logger()
  since, until = month_window()
  try:
    if REFRESH:
      extract_and_load(repos, since, until)
    elif not DB_PATH.exists():
      raise RuntimeError(f"REFRESH=0 but {DB_PATH} does not exist; run once with REFRESH=1")
    else:
      # cached DB reflects whatever repos/window it was last loaded with, not the current list
      logger.info(f"REFRESH=0: using cached {DB_PATH.name}; skipping API pull")

    monthly = monthly_counts(since, until)
    try:
      print(f"Plot saved to {plot(monthly)}")
    except Exception as e:
      logger.warning(f"Plot failed ({e}); continuing to summary")
    try:
      print(f"Vitality plot saved to {plot_vitality(vitality(since, until))}")
    except Exception as e:
      logger.warning(f"Vitality plot failed ({e}); continuing to summary")
    summary = summarize(monthly)
    with pd.option_context("display.width", 250, "display.max_columns", None):
      print(f"\nRepo activity {since:%b %Y} - {until:%b %Y} (exclusive)\n"
            f"YoY = last 12 months vs. prior 12 · score thresholds (avg commits/month): "
            f"{SCORE_THRESHOLDS}\n{summary.to_string(index=False)}\n")
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
    "astral-sh/uv",
    "pypa/pipenv",
    "mwaskom/seaborn",
    "psf/requests",
  ])
