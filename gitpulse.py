#!/usr/bin/env python3
"""GitPulse: a single-file GitHub terminal dashboard powered by gh."""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


CACHE_VERSION = 2
CACHE_DIR = Path.home() / ".gitpulse"
CACHE_PATH = CACHE_DIR / "cache.json"

DEFAULT_INTERVAL = 60
STREAK_DAYS = 365
HEATMAP_WEEKS = 12
SEARCH_LIMIT_MULTIPLIER = 4
REPO_FETCH_FLOOR = 60
PR_FETCH_FLOOR = 40
ISSUE_FETCH_FLOOR = 40
ATTENTION_LIMIT = 8
STALE_DAYS = 3
FAILING_RISK_HOURS = 24

BLOCKS = {
    "unicode": {
        "box": ("╭", "╮", "╰", "╯", "│", "─"),
        "heavy": ("╔", "╗", "╚", "╝", "║", "═"),
        "heat": {
            "NONE": "·",
            "FIRST_QUARTILE": "░",
            "SECOND_QUARTILE": "▒",
            "THIRD_QUARTILE": "▓",
            "FOURTH_QUARTILE": "█",
        },
        "spark": "▁▂▃▄▅▆▇█",
        "bullet": "•",
    },
    "ascii": {
        "box": ("+", "+", "+", "+", "|", "-"),
        "heavy": ("#", "#", "#", "#", "#", "="),
        "heat": {
            "NONE": ".",
            "FIRST_QUARTILE": ":",
            "SECOND_QUARTILE": "*",
            "THIRD_QUARTILE": "O",
            "FOURTH_QUARTILE": "@",
        },
        "spark": "._-~=*#",
        "bullet": "*",
    },
}

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
REPO_RE = re.compile(r"^[^/\s]+/[^/\s]+$")

PR_FIELDS = """
fragment PrFields on PullRequest {
  id
  number
  title
  url
  isDraft
  createdAt
  updatedAt
  mergeable
  mergeStateStatus
  reviewDecision
  repository {
    nameWithOwner
  }
  labels(first: 8) {
    nodes {
      name
    }
  }
  reviewRequests(first: 10) {
    nodes {
      requestedReviewer {
        __typename
        ... on User {
          login
        }
        ... on Team {
          slug
        }
      }
    }
  }
  commits(last: 1) {
    nodes {
      commit {
        statusCheckRollup {
          state
        }
      }
    }
  }
}
"""

ISSUE_FIELDS = """
fragment IssueFields on Issue {
  id
  number
  title
  url
  createdAt
  updatedAt
  repository {
    nameWithOwner
  }
  labels(first: 8) {
    nodes {
      name
    }
  }
}
"""

DASHBOARD_QUERY = (
    """
query Dashboard(
  $repoLimit: Int!,
  $prLimit: Int!,
  $issueLimit: Int!,
  $reviewQuery: String!,
  $authoredQuery: String!,
  $issueQuery: String!,
  $from: DateTime!,
  $to: DateTime!
) {
  viewer {
    login
    name
    repositories(
      first: $repoLimit,
      affiliations: [OWNER, COLLABORATOR, ORGANIZATION_MEMBER],
      isFork: false,
      orderBy: {field: PUSHED_AT, direction: DESC}
    ) {
      nodes {
        id
        nameWithOwner
        description
        url
        isPrivate
        pushedAt
        updatedAt
        stargazerCount
        primaryLanguage {
          name
        }
        issues(states: OPEN) {
          totalCount
        }
        pullRequests(states: OPEN) {
          totalCount
        }
      }
    }
    contributionsCollection(from: $from, to: $to) {
      contributionCalendar {
        totalContributions
        weeks {
          firstDay
          contributionDays {
            date
            contributionCount
            contributionLevel
            weekday
          }
        }
      }
    }
  }
  reviewRequested: search(query: $reviewQuery, type: ISSUE, first: $prLimit) {
    issueCount
    nodes {
      ...PrFields
    }
  }
  authored: search(query: $authoredQuery, type: ISSUE, first: $prLimit) {
    issueCount
    nodes {
      ...PrFields
    }
  }
  assignedIssues: search(query: $issueQuery, type: ISSUE, first: $issueLimit) {
    issueCount
    nodes {
      ...IssueFields
    }
  }
}
"""
    + PR_FIELDS
    + ISSUE_FIELDS
)


class GhError(RuntimeError):
    """Raised when a gh command fails."""


@dataclass
class Repo:
    id: str
    name: str
    description: str
    url: str
    pushed_at: datetime | None
    updated_at: datetime | None
    is_private: bool
    stars: int
    language: str
    open_issues: int
    open_prs: int


@dataclass
class PullRequest:
    id: str
    number: int
    title: str
    url: str
    repository: str
    created_at: datetime | None
    updated_at: datetime | None
    is_draft: bool
    mergeable: str
    merge_state_status: str
    review_decision: str
    check_state: str
    review_requested: bool
    labels: list[str]

    @property
    def key(self) -> str:
        return f"{self.repository}#{self.number}"

    @property
    def command_ref(self) -> str:
        return self.key


@dataclass
class Issue:
    id: str
    number: int
    title: str
    url: str
    repository: str
    created_at: datetime | None
    updated_at: datetime | None
    labels: list[str]

    @property
    def key(self) -> str:
        return f"{self.repository}#{self.number}"

    @property
    def command_ref(self) -> str:
        return self.key


@dataclass
class ActionItem:
    key: str
    kind: str
    repository: str
    title: str
    url: str
    number: int
    created_at: datetime | None
    updated_at: datetime | None
    badges: list[str]
    score: int
    check_state: str
    reason: str
    next_step: str
    age_bucket: str
    bucket: str


@dataclass
class DashboardData:
    generated_at: datetime
    viewer_login: str
    viewer_name: str
    repos: list[Repo]
    review_prs: list[PullRequest]
    authored_prs: list[PullRequest]
    failing_prs: list[PullRequest]
    ready_prs: list[PullRequest]
    assigned_issues: list[Issue]
    attention_items: list[ActionItem]
    contribution_weeks: list[list[dict[str, Any]]]
    contribution_days: list[dict[str, Any]]
    current_streak: int
    longest_streak: int
    contribution_total: int
    repo_activity_series: list[int]
    open_work_series: list[int]
    daily_brief: str
    changes: list[str]
    summary: dict[str, int]
    focus_label: str
    subtitle: str
    cache_used: bool
    watch_iteration: int = 1
    watch_total: int | None = None
    last_refresh_at: datetime | None = None
    next_refresh_seconds: int | None = None
    watch_mode: bool = False


class Style:
    def __init__(self) -> None:
        self.color_enabled = sys.stdout.isatty() and not os.environ.get("NO_COLOR")
        encoding = (sys.stdout.encoding or "").lower()
        self.charset = BLOCKS["unicode"] if "utf" in encoding else BLOCKS["ascii"]

    def color(self, text: str, *codes: str) -> str:
        if not self.color_enabled or not codes:
            return text
        return f"\033[{';'.join(codes)}m{text}\033[0m"

    def dim(self, text: str) -> str:
        return self.color(text, "2")

    def bold(self, text: str) -> str:
        return self.color(text, "1")

    def accent(self, text: str) -> str:
        return self.color(text, "1", "36")

    def good(self, text: str) -> str:
        return self.color(text, "1", "32")

    def warn(self, text: str) -> str:
        return self.color(text, "1", "33")

    def bad(self, text: str) -> str:
        return self.color(text, "1", "31")

    def cool(self, text: str) -> str:
        return self.color(text, "1", "34")

    def magenta(self, text: str) -> str:
        return self.color(text, "1", "35")

    def border(self, text: str) -> str:
        return self.color(text, "36")

    def badge(self, label: str) -> str:
        styles = {
            "REVIEW": ("30", "46"),
            "FAILING": ("37", "41"),
            "STALE": ("30", "43"),
            "READY": ("30", "42"),
            "ASSIGNED": ("30", "45"),
            "MERGE": ("30", "42"),
            "BEHIND": ("30", "43"),
            "DO NOW": ("37", "41"),
            "AT RISK": ("30", "43"),
            "WAITING": ("30", "47"),
        }
        fg, bg = styles.get(label, ("30", "47"))
        return self.color(f" {label} ", "1", fg, bg)

    def delta(self, text: str, sign: str) -> str:
        if sign == "+":
            return self.good(text)
        if sign == "-":
            return self.bad(text)
        if sign == "~":
            return self.warn(text)
        return self.dim(text)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="gitpulse.py",
        description="Render a GitHub terminal dashboard powered by gh.",
    )
    parser.add_argument("--limit", type=int, default=6, help="Rows to show per section (default: 6).")
    parser.add_argument(
        "--width",
        choices=["auto", "full"],
        default="auto",
        help="Clamp layout for readability or use the full terminal width.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore the previous disk cache snapshot but write a new one at the end.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable disk cache reads and writes for this run.",
    )
    parser.add_argument("--watch", action="store_true", help="Refresh the dashboard continuously.")
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help=f"Refresh interval in watch mode (default: {DEFAULT_INTERVAL}).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        help="Stop after N watch iterations. Useful for tests.",
    )
    parser.add_argument("--export-md", metavar="PATH", help="Write a Markdown standup export.")
    parser.add_argument("--export-html", metavar="PATH", help="Write a self-contained HTML export.")
    parser.add_argument("--reviews", action="store_true", help="Focus on review-requested PRs.")
    parser.add_argument("--failing", action="store_true", help="Focus on authored PRs with failing checks.")
    parser.add_argument("--stale", action="store_true", help="Focus on work stale for 3+ days.")
    parser.add_argument("--repo", metavar="OWNER/NAME", help="Filter to a single repository.")
    parser.add_argument("--org", metavar="ORGNAME", help="Filter to repositories owned by one org.")
    args = parser.parse_args(argv)

    if args.limit < 1:
        parser.error("--limit must be at least 1.")
    if args.interval < 1:
        parser.error("--interval must be at least 1.")
    if args.iterations is not None and args.iterations < 1:
        parser.error("--iterations must be at least 1.")
    if not args.watch and args.interval != DEFAULT_INTERVAL:
        parser.error("--interval requires --watch.")
    if not args.watch and args.iterations is not None:
        parser.error("--iterations requires --watch.")
    if args.repo and not REPO_RE.match(args.repo):
        parser.error("--repo must look like OWNER/NAME.")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    style = Style()
    try:
        if args.watch:
            return run_watch_mode(args, style)

        previous_snapshot = None if (args.no_cache or args.refresh) else read_cache()
        data = build_dashboard(args, previous_snapshot=previous_snapshot, cache_used=previous_snapshot is not None)
        render_dashboard(data, args, style)
        export_outputs(data, args)
        if not args.no_cache:
            write_cache(snapshot_from_dashboard(data))
        return 0
    except GhError as exc:
        print(style.bad(f"GitPulse error: {exc}"), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


def run_watch_mode(args: argparse.Namespace, style: Style) -> int:
    previous_snapshot = None if (args.no_cache or args.refresh) else read_cache()
    cache_used = previous_snapshot is not None
    iteration = 0

    while True:
        iteration += 1
        data = build_dashboard(args, previous_snapshot=previous_snapshot, cache_used=cache_used)
        data.watch_mode = True
        data.watch_iteration = iteration
        data.watch_total = args.iterations
        data.last_refresh_at = parse_dt((previous_snapshot or {}).get("generated_at")) or data.generated_at
        data.next_refresh_seconds = args.interval if (args.iterations is None or iteration < args.iterations) else None

        clear_screen_for_watch(iteration)
        render_dashboard(data, args, style)
        export_outputs(data, args)

        current_snapshot = snapshot_from_dashboard(data)
        previous_snapshot = current_snapshot
        cache_used = False

        if not args.no_cache:
            write_cache(current_snapshot)

        if args.iterations is not None and iteration >= args.iterations:
            return 0

        try:
            time_module.sleep(args.interval)
        except KeyboardInterrupt:
            return 130


def build_dashboard(
    args: argparse.Namespace,
    previous_snapshot: dict[str, Any] | None,
    cache_used: bool,
) -> DashboardData:
    ensure_gh_exists()
    generated_at = datetime.now(timezone.utc)

    viewer = run_gh_json(["api", "user"])
    viewer_login = viewer.get("login")
    if not viewer_login:
        raise GhError("Unable to determine the authenticated GitHub user.")

    viewer_name = viewer.get("name") or viewer_login
    repo_limit = min(max(args.limit * SEARCH_LIMIT_MULTIPLIER, REPO_FETCH_FLOOR), 100)
    pr_limit = min(max(args.limit * SEARCH_LIMIT_MULTIPLIER, PR_FETCH_FLOOR), 100)
    issue_limit = min(max(args.limit * SEARCH_LIMIT_MULTIPLIER, ISSUE_FETCH_FLOOR), 100)

    day_end = generated_at.date()
    day_start = day_end - timedelta(days=STREAK_DAYS - 1)
    payload = run_graphql(
        DASHBOARD_QUERY,
        {
            "repoLimit": repo_limit,
            "prLimit": pr_limit,
            "issueLimit": issue_limit,
            "reviewQuery": f"is:open is:pr archived:false sort:updated-desc review-requested:{viewer_login}",
            "authoredQuery": f"is:open is:pr archived:false sort:updated-desc author:{viewer_login}",
            "issueQuery": f"is:open is:issue archived:false sort:updated-desc assignee:{viewer_login}",
            "from": iso_datetime(datetime.combine(day_start, time.min, tzinfo=timezone.utc)),
            "to": iso_datetime(generated_at),
        },
    )

    viewer_data = payload["viewer"]
    repos = parse_repos(viewer_data.get("repositories", {}).get("nodes", []))
    review_prs = parse_prs(payload.get("reviewRequested", {}).get("nodes", []), viewer_login)
    authored_prs = parse_prs(payload.get("authored", {}).get("nodes", []), viewer_login)
    assigned_issues = parse_issues(payload.get("assignedIssues", {}).get("nodes", []))

    repos, review_prs, authored_prs, assigned_issues = apply_filters(
        repos, review_prs, authored_prs, assigned_issues, args, generated_at
    )

    attention_items = build_attention_items(review_prs, authored_prs, assigned_issues, generated_at, args.limit)
    failing_prs = [pr for pr in authored_prs if pr.check_state in {"FAILURE", "ERROR"}]
    ready_prs = [pr for pr in authored_prs if is_ready_pr(pr)]

    contribution_weeks = parse_contribution_weeks(
        viewer_data.get("contributionsCollection", {}).get("contributionCalendar", {}).get("weeks", [])
    )
    contribution_days = [day for week in contribution_weeks for day in week]
    # Use the local calendar date for streak calculations so the current day
    # is not dropped for users whose local time is ahead of UTC.
    current_streak, longest_streak = compute_streaks(contribution_days, generated_at.astimezone().date())
    contribution_total = (
        viewer_data.get("contributionsCollection", {}).get("contributionCalendar", {}).get("totalContributions", 0)
    )

    repo_activity_series = bucket_weekly(
        [repo.pushed_at for repo in repos if repo.pushed_at],
        generated_at,
        HEATMAP_WEEKS,
    )
    open_work_series = bucket_weekly(
        [item.updated_at for item in attention_items if item.updated_at],
        generated_at,
        HEATMAP_WEEKS,
    )

    summary = {
        "active_repos": count_recent_repos(repos, generated_at, days=14),
        "reviews_waiting": len(review_prs),
        "failing_prs": len(failing_prs),
        "assigned_issues": len(assigned_issues),
        "attention_now": sum(1 for item in attention_items if item.bucket == "DO NOW"),
        "attention_risk": sum(1 for item in attention_items if item.bucket == "AT RISK"),
        "current_streak": current_streak,
    }
    focus_label = build_focus_label(args)
    subtitle = build_subtitle(args)
    daily_brief = build_daily_brief(summary, repos, attention_items, focus_label)
    changes = build_changes(previous_snapshot, repos, review_prs, authored_prs, assigned_issues, current_streak, generated_at)

    return DashboardData(
        generated_at=generated_at,
        viewer_login=viewer_login,
        viewer_name=viewer_name,
        repos=repos,
        review_prs=review_prs,
        authored_prs=authored_prs,
        failing_prs=failing_prs,
        ready_prs=ready_prs,
        assigned_issues=assigned_issues,
        attention_items=attention_items,
        contribution_weeks=contribution_weeks,
        contribution_days=contribution_days,
        current_streak=current_streak,
        longest_streak=longest_streak,
        contribution_total=contribution_total,
        repo_activity_series=repo_activity_series,
        open_work_series=open_work_series,
        daily_brief=daily_brief,
        changes=changes,
        summary=summary,
        focus_label=focus_label,
        subtitle=subtitle,
        cache_used=cache_used,
    )


def ensure_gh_exists() -> None:
    gh_bin = os.environ.get("GITPULSE_GH_BIN", "gh")
    if shutil.which(gh_bin) is None:
        raise GhError("`gh` was not found in PATH. Install GitHub CLI and authenticate first.")


def run_gh_json(args: list[str], stdin_text: str | None = None) -> dict[str, Any]:
    raw = run_gh(args, stdin_text=stdin_text)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise GhError(f"`gh {' '.join(args)}` did not return valid JSON.") from exc


def run_graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    args = ["api", "graphql", "-f", f"query={query}"]
    for key, value in variables.items():
        args.extend(["-F", f"{key}={value}"])
    payload = run_gh_json(args)
    if "data" not in payload:
        raise GhError("GraphQL query returned no data.")
    return payload["data"]


def run_gh(args: list[str], stdin_text: str | None = None) -> str:
    gh_bin = os.environ.get("GITPULSE_GH_BIN", "gh")
    command = [gh_bin, *args]
    try:
        completed = subprocess.run(
            command,
            input=stdin_text,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        raise GhError(f"Unable to execute `{' '.join(command)}`: {exc}.") from exc

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        if "authentication" in stderr.lower() or "not logged into any hosts" in stderr.lower():
            raise GhError("GitHub CLI is not authenticated. Run `gh auth login` and try again.")
        raise GhError(stderr or f"`{' '.join(command)}` exited with status {completed.returncode}.")
    return completed.stdout


def parse_repos(nodes: list[dict[str, Any]]) -> list[Repo]:
    repos: list[Repo] = []
    for node in nodes:
        if not node:
            continue
        repos.append(
            Repo(
                id=node.get("id", ""),
                name=node.get("nameWithOwner", "unknown/unknown"),
                description=(node.get("description") or "").strip(),
                url=node.get("url", ""),
                pushed_at=parse_dt(node.get("pushedAt")),
                updated_at=parse_dt(node.get("updatedAt")),
                is_private=bool(node.get("isPrivate")),
                stars=int(node.get("stargazerCount") or 0),
                language=((node.get("primaryLanguage") or {}).get("name") or "n/a"),
                open_issues=int((node.get("issues") or {}).get("totalCount") or 0),
                open_prs=int((node.get("pullRequests") or {}).get("totalCount") or 0),
            )
        )
    repos.sort(key=lambda repo: repo.pushed_at or epoch_utc(), reverse=True)
    return repos


def parse_prs(nodes: list[dict[str, Any]], viewer_login: str) -> list[PullRequest]:
    prs: list[PullRequest] = []
    for node in nodes:
        if not node or node.get("id") is None:
            continue
        review_requested = False
        for request in (node.get("reviewRequests") or {}).get("nodes", []):
            reviewer = (request or {}).get("requestedReviewer") or {}
            if reviewer.get("__typename") == "User" and reviewer.get("login") == viewer_login:
                review_requested = True
                break
        prs.append(
            PullRequest(
                id=node.get("id", ""),
                number=int(node.get("number") or 0),
                title=node.get("title", ""),
                url=node.get("url", ""),
                repository=((node.get("repository") or {}).get("nameWithOwner") or "unknown/unknown"),
                created_at=parse_dt(node.get("createdAt")),
                updated_at=parse_dt(node.get("updatedAt")),
                is_draft=bool(node.get("isDraft")),
                mergeable=node.get("mergeable") or "UNKNOWN",
                merge_state_status=node.get("mergeStateStatus") or "UNKNOWN",
                review_decision=node.get("reviewDecision") or "REVIEW_REQUIRED",
                check_state=parse_check_state(node),
                review_requested=review_requested,
                labels=[label.get("name", "") for label in (node.get("labels") or {}).get("nodes", []) if label],
            )
        )
    prs.sort(key=lambda pr: pr.updated_at or epoch_utc(), reverse=True)
    return prs


def parse_issues(nodes: list[dict[str, Any]]) -> list[Issue]:
    issues: list[Issue] = []
    for node in nodes:
        if not node or node.get("id") is None:
            continue
        issues.append(
            Issue(
                id=node.get("id", ""),
                number=int(node.get("number") or 0),
                title=node.get("title", ""),
                url=node.get("url", ""),
                repository=((node.get("repository") or {}).get("nameWithOwner") or "unknown/unknown"),
                created_at=parse_dt(node.get("createdAt")),
                updated_at=parse_dt(node.get("updatedAt")),
                labels=[label.get("name", "") for label in (node.get("labels") or {}).get("nodes", []) if label],
            )
        )
    issues.sort(key=lambda issue: issue.updated_at or epoch_utc(), reverse=True)
    return issues


def parse_contribution_weeks(weeks: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    parsed = []
    for week in weeks:
        week_days = []
        for day in week.get("contributionDays", []):
            day_copy = dict(day)
            day_copy["date"] = parse_date(day.get("date"))
            week_days.append(day_copy)
        if week_days:
            parsed.append(week_days)
    return parsed


def parse_check_state(node: dict[str, Any]) -> str:
    commits = (node.get("commits") or {}).get("nodes", [])
    if not commits:
        return "UNKNOWN"
    commit = (commits[-1] or {}).get("commit") or {}
    rollup = commit.get("statusCheckRollup") or {}
    return rollup.get("state") or "UNKNOWN"


def apply_filters(
    repos: list[Repo],
    review_prs: list[PullRequest],
    authored_prs: list[PullRequest],
    assigned_issues: list[Issue],
    args: argparse.Namespace,
    now: datetime,
) -> tuple[list[Repo], list[PullRequest], list[PullRequest], list[Issue]]:
    if args.repo:
        repos = [repo for repo in repos if repo.name == args.repo]
        review_prs = [pr for pr in review_prs if pr.repository == args.repo]
        authored_prs = [pr for pr in authored_prs if pr.repository == args.repo]
        assigned_issues = [issue for issue in assigned_issues if issue.repository == args.repo]

    if args.org:
        prefix = f"{args.org}/"
        repos = [repo for repo in repos if repo.name.startswith(prefix)]
        review_prs = [pr for pr in review_prs if pr.repository.startswith(prefix)]
        authored_prs = [pr for pr in authored_prs if pr.repository.startswith(prefix)]
        assigned_issues = [issue for issue in assigned_issues if issue.repository.startswith(prefix)]

    if args.reviews:
        review_prs = sorted(
            review_prs,
            key=lambda pr: (
                0 if pr.review_requested else 1,
                -(pr.updated_at.timestamp() if pr.updated_at else 0),
            ),
        )

    if args.failing:
        authored_prs = sort_prs_for_failing_focus(authored_prs, now)

    if args.stale:
        review_prs = sort_by_staleness(review_prs, now)
        authored_prs = sort_by_staleness(authored_prs, now)
        assigned_issues = sort_by_staleness(assigned_issues, now)
        repos = sorted(repos, key=lambda repo: age_key(repo.pushed_at, now), reverse=True)

    return repos, review_prs, authored_prs, assigned_issues


def build_attention_items(
    review_prs: list[PullRequest],
    authored_prs: list[PullRequest],
    assigned_issues: list[Issue],
    now: datetime,
    limit: int,
) -> list[ActionItem]:
    items: dict[str, ActionItem] = {}

    for pr in review_prs:
        item = action_for_review_pr(pr, now)
        items[pr.id] = item

    for pr in authored_prs:
        item = action_for_authored_pr(pr, now)
        if not item:
            continue
        current = items.get(pr.id)
        if current is None or item.score > current.score:
            items[pr.id] = item

    for issue in assigned_issues:
        item = action_for_issue(issue, now)
        items[f"issue:{issue.id}"] = item

    ranked = sorted(
        items.values(),
        key=lambda item: (
            bucket_rank(item.bucket),
            -item.score,
            -(item.updated_at.timestamp() if item.updated_at else 0),
            item.repository.lower(),
        ),
    )
    return ranked[: max(ATTENTION_LIMIT, limit)]


def action_for_review_pr(pr: PullRequest, now: datetime) -> ActionItem:
    badges = ["REVIEW"]
    bucket = "DO NOW"
    reason = f"review requested {relative_time_long(pr.updated_at, now)} ago"
    next_step = f"gh pr view {pr.key} --web"
    score = 120 + stale_score(pr.updated_at, now)
    if is_stale(pr.updated_at, now):
        badges.append("STALE")
    if pr.check_state in {"FAILURE", "ERROR"}:
        badges.append("FAILING")
        reason = f"review requested, checks failing {relative_time_long(pr.updated_at, now)} old"
        score += 10
    return ActionItem(
        key=pr.key,
        kind="pr",
        repository=pr.repository,
        title=pr.title,
        url=pr.url,
        number=pr.number,
        created_at=pr.created_at,
        updated_at=pr.updated_at,
        badges=unique_preserving_order([bucket, *badges])[:3],
        score=score,
        check_state=pr.check_state,
        reason=reason,
        next_step=next_step,
        age_bucket=age_bucket(pr.updated_at, now),
        bucket=bucket,
    )


def action_for_authored_pr(pr: PullRequest, now: datetime) -> ActionItem | None:
    badges: list[str] = []
    bucket = "WAITING"
    reason = f"updated {relative_time_long(pr.updated_at, now)} ago"
    next_step = f"gh pr view {pr.key} --web"
    score = 30 + recent_boost(pr.updated_at, now)

    if pr.check_state in {"FAILURE", "ERROR"}:
        badges.append("FAILING")
        reason = "checks failing"
        next_step = f"gh pr checkout {pr.number}"
        if is_older_than(pr.updated_at, now, hours=FAILING_RISK_HOURS):
            bucket = "AT RISK"
            reason = f"checks failing for {relative_time_long(pr.updated_at, now)}"
            score = 105
        else:
            bucket = "DO NOW"
            score = 115
    elif is_ready_pr(pr):
        badges.extend(["READY", "MERGE"])
        bucket = "DO NOW"
        reason = "approved and mergeable"
        score = 112
    elif pr.merge_state_status == "BEHIND":
        badges.append("BEHIND")
        bucket = "AT RISK"
        reason = "behind base branch"
        next_step = f"gh pr checkout {pr.number}"
        score = 88
    elif is_stale(pr.updated_at, now):
        badges.append("STALE")
        bucket = "AT RISK"
        reason = f"stale for {relative_time_long(pr.updated_at, now)}"
        score = 82
    elif not pr.is_draft:
        bucket = "WAITING"
        reason = "waiting on review"
        score = 42
    else:
        return None

    return ActionItem(
        key=pr.key,
        kind="pr",
        repository=pr.repository,
        title=pr.title,
        url=pr.url,
        number=pr.number,
        created_at=pr.created_at,
        updated_at=pr.updated_at,
        badges=unique_preserving_order([bucket, *badges])[:3],
        score=score + stale_score(pr.updated_at, now),
        check_state=pr.check_state,
        reason=reason,
        next_step=next_step,
        age_bucket=age_bucket(pr.updated_at, now),
        bucket=bucket,
    )


def action_for_issue(issue: Issue, now: datetime) -> ActionItem:
    stale = is_stale(issue.updated_at, now)
    bucket = "AT RISK" if stale else "WAITING"
    reason = f"stale for {relative_time_long(issue.updated_at, now)}" if stale else "assigned issue, no escalation yet"
    score = 74 if stale else 38
    return ActionItem(
        key=issue.key,
        kind="issue",
        repository=issue.repository,
        title=issue.title,
        url=issue.url,
        number=issue.number,
        created_at=issue.created_at,
        updated_at=issue.updated_at,
        badges=unique_preserving_order([bucket, "ASSIGNED", "STALE" if stale else ""])[:3],
        score=score,
        check_state="",
        reason=reason,
        next_step=f"gh issue view {issue.key} --web",
        age_bucket=age_bucket(issue.updated_at, now),
        bucket=bucket,
    )


def is_ready_pr(pr: PullRequest) -> bool:
    if pr.is_draft:
        return False
    if pr.review_decision != "APPROVED":
        return False
    if pr.mergeable != "MERGEABLE":
        return False
    return pr.merge_state_status in {"CLEAN", "HAS_HOOKS", "UNSTABLE"}


def score_issue(issue: Issue, now: datetime) -> int:
    score = 55
    score += stale_score(issue.updated_at, now)
    score += recent_boost(issue.updated_at, now)
    return score


def stale_score(updated_at: datetime | None, now: datetime) -> int:
    if updated_at is None:
        return 0
    days = max((now - updated_at).days, 0)
    if days < STALE_DAYS:
        return 0
    return min(30, 8 + (days - STALE_DAYS) * 3)


def recent_boost(updated_at: datetime | None, now: datetime) -> int:
    if updated_at is None:
        return 0
    age = now - updated_at
    if age <= timedelta(hours=6):
        return 6
    if age <= timedelta(hours=24):
        return 3
    return 0


def is_stale(updated_at: datetime | None, now: datetime) -> bool:
    return updated_at is not None and (now - updated_at) >= timedelta(days=STALE_DAYS)


def is_older_than(updated_at: datetime | None, now: datetime, hours: int) -> bool:
    return updated_at is not None and (now - updated_at) >= timedelta(hours=hours)


def age_bucket(updated_at: datetime | None, now: datetime) -> str:
    if updated_at is None:
        return "UNKNOWN"
    delta = now - updated_at
    if delta < timedelta(hours=12):
        return "FRESH"
    if delta < timedelta(days=3):
        return "RECENT"
    if delta < timedelta(days=7):
        return "STALE"
    return "OLD"


def bucket_rank(bucket: str) -> int:
    order = {"DO NOW": 0, "AT RISK": 1, "WAITING": 2}
    return order.get(bucket, 9)


def unique_preserving_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered = []
    for item in items:
        if item and item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


def compute_streaks(days: list[dict[str, Any]], today: date) -> tuple[int, int]:
    contributions = {day["date"]: int(day.get("contributionCount") or 0) for day in days if day.get("date")}
    if not contributions:
        return 0, 0

    longest = 0
    current = 0
    run = 0
    previous: date | None = None
    for current_day in sorted(contributions):
        count = contributions[current_day]
        if count > 0:
            if previous and current_day == previous + timedelta(days=1):
                run += 1
            else:
                run = 1
            longest = max(longest, run)
        else:
            run = 0
        previous = current_day

    pointer = today
    last_active = max((day for day, count in contributions.items() if count > 0), default=None)
    if last_active is None or (today - last_active) > timedelta(days=1):
        return 0, longest

    while contributions.get(pointer, 0) > 0:
        current += 1
        pointer -= timedelta(days=1)

    if current == 0 and contributions.get(today - timedelta(days=1), 0) > 0:
        pointer = today - timedelta(days=1)
        while contributions.get(pointer, 0) > 0:
            current += 1
            pointer -= timedelta(days=1)
    return current, longest


def count_recent_repos(repos: list[Repo], now: datetime, days: int) -> int:
    cutoff = now - timedelta(days=days)
    return sum(1 for repo in repos if repo.pushed_at and repo.pushed_at >= cutoff)


def build_focus_label(args: argparse.Namespace) -> str:
    bits = []
    if args.repo:
        bits.append(f"repo {args.repo}")
    if args.org:
        bits.append(f"org {args.org}")
    if args.reviews:
        bits.append("reviews")
    if args.failing:
        bits.append("failing")
    if args.stale:
        bits.append("stale")
    return ", ".join(bits) if bits else "all activity"


def build_subtitle(args: argparse.Namespace) -> str:
    if not any([args.repo, args.org, args.reviews, args.failing, args.stale]):
        return "GitHub activity at a glance"
    return f"Focus: {build_focus_label(args)}"


def build_daily_brief(summary: dict[str, int], repos: list[Repo], attention_items: list[ActionItem], focus_label: str) -> str:
    if attention_items:
        standout = f"Top action: {attention_items[0].repository} ({attention_items[0].reason})."
    elif repos:
        standout = f"Biggest movement: {repos[0].name}."
    else:
        standout = "No matching GitHub activity surfaced."
    return (
        f"Focus: {focus_label}. "
        f"{summary['active_repos']} active repos, "
        f"{summary['reviews_waiting']} reviews waiting, "
        f"{summary['failing_prs']} failing PRs, "
        f"{summary['assigned_issues']} assigned issues, "
        f"{summary['attention_now']} do-now items, "
        f"streak {summary['current_streak']} days. "
        f"{standout}"
    )


def build_changes(
    previous_snapshot: dict[str, Any] | None,
    repos: list[Repo],
    review_prs: list[PullRequest],
    authored_prs: list[PullRequest],
    assigned_issues: list[Issue],
    current_streak: int,
    now: datetime,
) -> list[str]:
    if previous_snapshot is None:
        return ["First run snapshot will be created after this run."]

    previous_at = parse_dt(previous_snapshot.get("generated_at"))
    previous_review = set(previous_snapshot.get("review_pr_ids", []))
    current_review = {pr.key for pr in review_prs}
    previous_issues = set(previous_snapshot.get("issue_ids", []))
    current_issues = {issue.key for issue in assigned_issues}
    previous_repo_pushes = previous_snapshot.get("repo_pushes", {})
    current_checks = {pr.key: pr.check_state for pr in authored_prs}
    previous_checks = previous_snapshot.get("pr_check_states", {})
    previous_ready = set(previous_snapshot.get("ready_pr_ids", []))
    current_ready = {pr.key for pr in authored_prs if is_ready_pr(pr)}

    changes = []

    new_reviews = sorted(current_review - previous_review)
    cleared_reviews = sorted(previous_review - current_review)
    new_issues = sorted(current_issues - previous_issues)
    cleared_issues = sorted(previous_issues - current_issues)

    fresh_repos = []
    for repo in repos:
        if repo.pushed_at is None:
            continue
        current_push = iso_datetime(repo.pushed_at)
        previous_push = previous_repo_pushes.get(repo.name)
        if previous_push and previous_push != current_push:
            fresh_repos.append(repo.name)
        elif previous_push is None and previous_at is not None and repo.pushed_at > previous_at:
            fresh_repos.append(repo.name)

    check_changes = []
    for key, state in current_checks.items():
        previous_state = previous_checks.get(key)
        if previous_state and previous_state != state:
            check_changes.append(f"{key} {previous_state.lower()}→{state.lower()}")

    newly_ready = sorted(current_ready - previous_ready)
    streak_delta = current_streak - int(previous_snapshot.get("current_streak") or 0)

    if new_reviews:
        changes.append(f"+{len(new_reviews)} new review requests")
    if cleared_reviews:
        changes.append(f"-{len(cleared_reviews)} review requests cleared")
    if new_issues:
        changes.append(f"+{len(new_issues)} assigned issues added")
    if cleared_issues:
        changes.append(f"-{len(cleared_issues)} assigned issues cleared")
    if fresh_repos:
        changes.append(f"+{len(fresh_repos)} repos received fresh pushes")
    if check_changes:
        changes.append(f"~{len(check_changes)} PR check states changed")
    if newly_ready:
        changes.append(f"+{len(newly_ready)} PRs became ready to merge")
    if streak_delta > 0:
        changes.append(f"+{streak_delta} streak day")
    elif streak_delta < 0:
        changes.append(f"{streak_delta} streak days")
    if not changes:
        changes.append(f"No changes since {format_timestamp(previous_at or now)}.")
    return changes


def snapshot_from_dashboard(data: DashboardData) -> dict[str, Any]:
    return {
        "schema_version": CACHE_VERSION,
        "generated_at": iso_datetime(data.generated_at),
        "viewer_login": data.viewer_login,
        "review_pr_ids": [pr.key for pr in data.review_prs],
        "issue_ids": [issue.key for issue in data.assigned_issues],
        "repo_pushes": {repo.name: iso_datetime(repo.pushed_at) for repo in data.repos if repo.pushed_at is not None},
        "pr_check_states": {pr.key: pr.check_state for pr in data.authored_prs},
        "ready_pr_ids": [pr.key for pr in data.ready_prs],
        "current_streak": data.current_streak,
    }


def read_cache() -> dict[str, Any] | None:
    if not CACHE_PATH.exists():
        return None
    try:
        payload = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("schema_version") != CACHE_VERSION:
        return None
    return payload


def write_cache(snapshot: dict[str, Any]) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    except OSError as exc:
        raise GhError(f"Unable to write cache file {CACHE_PATH}: {exc}.") from exc


def render_dashboard(data: DashboardData, args: argparse.Namespace, style: Style) -> None:
    width = resolve_width(args.width)
    print(render_title(data, width, style))
    if data.subtitle:
        print(style.dim(truncate_ansi(data.subtitle, width)))
    if data.watch_mode:
        print(style.dim(truncate_ansi(render_status_line(data, style), width)))
    print()

    print_box("Daily Brief", wrap_lines(data.daily_brief, width - 4), width, style)
    print()
    print_box(
        "Attention Radar",
        render_attention_lines(data.attention_items, args.limit, data.generated_at, width, style),
        width,
        style,
    )
    print()
    print_box(
        "Changes Since Last Run",
        render_changes_lines(data.changes, style, highlight=data.watch_mode),
        width,
        style,
        heavy=data.watch_mode,
    )
    print()
    print_box("Contributions", render_contribution_lines(data, width, style), width, style)
    print()
    print_box("Repos By Recent Activity", render_repo_lines(data.repos, args.limit, data.generated_at, width, style), width, style)
    print()
    print_box("Open PRs Waiting On You", render_review_lines(data.review_prs, args.limit, data.generated_at, width, style), width, style)
    print()
    print_box("Failing Or Ready PRs", render_authored_pr_lines(data, args.limit, data.generated_at, width, style), width, style)
    print()
    print_box("Issues Assigned To You", render_issue_lines(data.assigned_issues, args.limit, data.generated_at, width, style), width, style)


def render_title(data: DashboardData, width: int, style: Style) -> str:
    left = f" GitPulse "
    right = f" {data.viewer_name} ({data.viewer_login}) "
    if data.focus_label != "all activity":
        right = f" {data.focus_label} | {data.viewer_login} "
    bar_width = max(width - visible_len(left) - visible_len(right), 6)
    mid = style.border("═" * bar_width)
    return f"{style.accent(left)}{mid}{style.cool(right)}"


def render_status_line(data: DashboardData, style: Style) -> str:
    parts = [
        f"now {datetime.now().astimezone().strftime('%H:%M:%S')}",
        f"last {format_timestamp(data.last_refresh_at or data.generated_at, short=True)}",
        f"next {str(data.next_refresh_seconds) + 's' if data.next_refresh_seconds is not None else 'stop'}",
        f"cache {'disk' if data.cache_used else 'memory/off'}",
    ]
    if data.watch_total is not None:
        parts.append(f"iter {data.watch_iteration}/{data.watch_total}")
    else:
        parts.append(f"iter {data.watch_iteration}")
    return " | ".join(parts)


def render_changes_lines(changes: list[str], style: Style, highlight: bool = False) -> list[str]:
    bullet = style.charset["bullet"]
    lines = []
    for change in changes:
        sign = change[0] if change and change[0] in "+-~" else ""
        prefix = bullet if not highlight else {"+" : "+", "-" : "-", "~" : "~"}.get(sign, bullet)
        lines.append(style.delta(f"{prefix} {change.lstrip('+-~') if highlight and sign else change}", sign))
    return lines


def render_attention_lines(
    items: list[ActionItem],
    limit: int,
    now: datetime,
    width: int,
    style: Style,
) -> list[str]:
    if not items:
        return [style.dim("No attention items in the current focus.")]

    compact = width < 96
    lines = []
    for item in items[: min(len(items), max(5, min(limit + 2, ATTENTION_LIMIT)))]:
        head = (
            f"{style.badge(item.bucket)} "
            f"{style.bold(truncate_plain(item.repository, 30 if compact else 36))} "
            f"{style.dim('#' + str(item.number)) if item.kind == 'pr' else style.dim('#' + str(item.number))}"
        )
        lines.append(truncate_ansi(head, width - 4))
        lines.append(truncate_ansi(f"  {truncate_plain(item.title, max(20, width - 8))}", width - 4))
        reason = f"  {item.reason} | {item.age_bucket.lower()} | {item.next_step}"
        lines.append(truncate_ansi(reason, width - 4))
        if compact:
            lines.append(style.dim("  "))
    return trim_blank_tail(lines)


def render_contribution_lines(data: DashboardData, width: int, style: Style) -> list[str]:
    lines = render_heatmap(data.contribution_weeks, width, style)
    lines.append(
        f"Current streak {style.good(str(data.current_streak))}d  "
        f"Longest {style.cool(str(data.longest_streak))}d  "
        f"Total 365d {style.magenta(str(data.contribution_total))}"
    )
    lines.append(
        f"Repo activity {style.cool(render_sparkline(data.repo_activity_series, style))}  "
        f"Open work {style.warn(render_sparkline(data.open_work_series, style))}"
    )
    return lines


def render_repo_lines(repos: list[Repo], limit: int, now: datetime, width: int, style: Style) -> list[str]:
    if not repos:
        return [style.dim("No repositories matched the current focus.")]

    inner = max(width - 4, 20)
    repo_width = min(28, max(16, inner // 4))
    push_width = 10
    lang_width = 12
    load_width = 10
    desc_width = max(inner - repo_width - push_width - lang_width - load_width - 8, 10)
    lines = [
        f"{style.dim(pad_plain('Repo', repo_width))}  "
        f"{style.dim(pad_plain('Last Push', push_width))}  "
        f"{style.dim(pad_plain('Lang', lang_width))}  "
        f"{style.dim(pad_plain('Open', load_width))}  "
        f"{style.dim('Description')}"
    ]
    for repo in repos[:limit]:
        last_push = relative_time(repo.pushed_at, now)
        open_load = f"{repo.open_prs}/{repo.open_issues}"
        desc = repo.description or ("Private repository" if repo.is_private else "")
        lines.append(
            f"{pad_plain(truncate_plain(repo.name, repo_width), repo_width)}  "
            f"{pad_plain(last_push, push_width)}  "
            f"{pad_plain(truncate_plain(repo.language, lang_width), lang_width)}  "
            f"{pad_plain(open_load, load_width)}  "
            f"{truncate_plain(desc, desc_width)}"
        )
    append_more_line(lines, len(repos), limit, style)
    return lines


def render_review_lines(prs: list[PullRequest], limit: int, now: datetime, width: int, style: Style) -> list[str]:
    if not prs:
        return [style.dim("Nothing is currently waiting on your review.")]

    inner = max(width - 4, 20)
    repo_width = min(24, max(15, inner // 4))
    title_width = max(inner - repo_width - 27, 16)
    lines = [
        f"{style.dim(pad_plain('Repo', repo_width))}  "
        f"{style.dim(pad_plain('PR', title_width))}  "
        f"{style.dim(pad_plain('Updated', 7))}  "
        f"{style.dim('Checks')}"
    ]
    for pr in prs[:limit]:
        lines.append(
            f"{pad_plain(truncate_plain(pr.repository, repo_width), repo_width)}  "
            f"{pad_plain(truncate_plain(f'#{pr.number} {pr.title}', title_width), title_width)}  "
            f"{pad_plain(relative_time(pr.updated_at, now), 7)}  "
            f"{format_check_state(pr.check_state, style)}"
        )
    append_more_line(lines, len(prs), limit, style)
    return lines


def render_authored_pr_lines(data: DashboardData, limit: int, now: datetime, width: int, style: Style) -> list[str]:
    prs = unique_pr_list(data.failing_prs + data.ready_prs)
    if not prs:
        return [style.dim("No failing or merge-ready authored PRs right now.")]

    inner = max(width - 4, 20)
    repo_width = min(24, max(15, inner // 4))
    title_width = max(inner - repo_width - 28, 16)
    lines = [
        f"{style.dim(pad_plain('Repo', repo_width))}  "
        f"{style.dim(pad_plain('PR', title_width))}  "
        f"{style.dim(pad_plain('Updated', 7))}  "
        f"{style.dim('State')}"
    ]
    for pr in prs[:limit]:
        state = "ready" if is_ready_pr(pr) else pr.check_state.lower()
        if is_ready_pr(pr):
            state_text = style.good(state)
        elif pr.check_state in {"FAILURE", "ERROR"}:
            state_text = style.bad(state)
        else:
            state_text = style.dim(state)
        lines.append(
            f"{pad_plain(truncate_plain(pr.repository, repo_width), repo_width)}  "
            f"{pad_plain(truncate_plain(f'#{pr.number} {pr.title}', title_width), title_width)}  "
            f"{pad_plain(relative_time(pr.updated_at, now), 7)}  "
            f"{state_text}"
        )
    append_more_line(lines, len(prs), limit, style)
    return lines


def render_issue_lines(issues: list[Issue], limit: int, now: datetime, width: int, style: Style) -> list[str]:
    if not issues:
        return [style.dim("No assigned issues right now.")]

    inner = max(width - 4, 20)
    repo_width = min(24, max(15, inner // 4))
    title_width = max(inner - repo_width - 13, 16)
    lines = [
        f"{style.dim(pad_plain('Repo', repo_width))}  "
        f"{style.dim(pad_plain('Issue', title_width))}  "
        f"{style.dim('Updated')}"
    ]
    for issue in issues[:limit]:
        lines.append(
            f"{pad_plain(truncate_plain(issue.repository, repo_width), repo_width)}  "
            f"{pad_plain(truncate_plain(f'#{issue.number} {issue.title}', title_width), title_width)}  "
            f"{relative_time(issue.updated_at, now)}"
        )
    append_more_line(lines, len(issues), limit, style)
    return lines


def print_box(title: str, lines: list[str], width: int, style: Style, heavy: bool = False) -> None:
    for line in box_lines(title, lines, width, style, heavy=heavy):
        print(line)


def box_lines(title: str, lines: list[str], width: int, style: Style, heavy: bool = False) -> list[str]:
    charset = style.charset["heavy"] if heavy else style.charset["box"]
    tl, tr, bl, br, vertical, horizontal = charset
    inner_width = max(width - 2, 20)
    title_text = f" {title} "
    title_fill = max(inner_width - visible_len(title_text), 0)
    top = f"{style.border(tl + title_text + horizontal * title_fill + tr)}"
    boxed = [top]
    for line in lines or [""]:
        boxed.extend(wrap_box_line(line, inner_width, vertical, style))
    boxed.append(style.border(bl + horizontal * inner_width + br))
    return boxed


def wrap_box_line(line: str, inner_width: int, vertical: str, style: Style) -> list[str]:
    if visible_len(line) <= inner_width:
        return [f"{style.border(vertical)}{pad_ansi(line, inner_width)}{style.border(vertical)}"]
    plain = strip_ansi(line)
    wrapped = textwrap.wrap(plain, width=inner_width) or [""]
    return [f"{style.border(vertical)}{pad_plain(part, inner_width)}{style.border(vertical)}" for part in wrapped]


def wrap_lines(text: str, width: int) -> list[str]:
    return textwrap.wrap(text, width=max(width, 20)) or [text]


def render_heatmap(weeks: list[list[dict[str, Any]]], width: int, style: Style) -> list[str]:
    if not weeks:
        return [style.dim("No contribution history returned by GitHub.")]
    recent_weeks = weeks[-HEATMAP_WEEKS:]
    if not recent_weeks:
        return [style.dim("No recent contribution history returned by GitHub.")]

    month_line = ["   "]
    last_month = None
    for week in recent_weeks:
        first = week[0]["date"]
        label = first.strftime("%b")[0] if first and first.month != last_month else " "
        month_line.append(label + " ")
        if first:
            last_month = first.month
    lines = [style.dim("".join(month_line).rstrip())]

    weekday_labels = ["M", "T", "W", "T", "F", "S", "S"]
    rows = [[] for _ in range(7)]
    for week in recent_weeks:
        normalized = {}
        for day in week:
            weekday_index = day["date"].weekday() if day.get("date") else len(normalized)
            normalized[weekday_index] = day
        for index in range(7):
            rows[index].append(format_heat_cell(normalized.get(index), style))

    for label, row in zip(weekday_labels, rows):
        lines.append(f"{label}  {' '.join(row)}")
    if width < 86:
        return [truncate_ansi(line, width - 4) for line in lines]
    return lines


def format_heat_cell(day: dict[str, Any] | None, style: Style) -> str:
    if not day:
        return style.dim(style.charset["heat"]["NONE"])
    level = day.get("contributionLevel") or "NONE"
    char = style.charset["heat"].get(level, style.charset["heat"]["NONE"])
    if level == "FOURTH_QUARTILE":
        return style.good(char)
    if level == "THIRD_QUARTILE":
        return style.color(char, "32")
    if level == "SECOND_QUARTILE":
        return style.color(char, "36")
    if level == "FIRST_QUARTILE":
        return style.color(char, "34")
    return style.dim(char)


def render_sparkline(values: list[int], style: Style) -> str:
    if not values:
        return style.dim("n/a")
    ticks = style.charset["spark"]
    max_value = max(values) if values else 0
    if max_value <= 0:
        return ticks[0] * len(values)
    out = []
    steps = len(ticks) - 1
    for value in values:
        index = int(round((value / max_value) * steps))
        out.append(ticks[max(0, min(index, steps))])
    return "".join(out)


def bucket_weekly(points: list[datetime], now: datetime, weeks: int) -> list[int]:
    buckets = [0 for _ in range(weeks)]
    start = now - timedelta(weeks=weeks)
    for point in points:
        if point < start:
            continue
        delta = now - point
        week_index = weeks - 1 - min(int(delta.days // 7), weeks - 1)
        buckets[week_index] += 1
    return buckets


def resolve_width(mode: str) -> int:
    columns = shutil.get_terminal_size(fallback=(110, 40)).columns
    if mode == "full":
        return max(columns, 40)
    if columns < 72:
        return columns
    return min(columns, 112)


def relative_time(value: datetime | None, now: datetime) -> str:
    if value is None:
        return "n/a"
    delta = now - value
    if delta.total_seconds() < 0:
        delta = timedelta(0)
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    days = hours // 24
    if days < 7:
        return f"{days}d"
    weeks = days // 7
    if weeks < 8:
        return f"{weeks}w"
    months = max(days // 30, 1)
    return f"{months}mo"


def relative_time_long(value: datetime | None, now: datetime) -> str:
    if value is None:
        return "unknown"
    delta = now - value
    if delta.total_seconds() < 0:
        delta = timedelta(0)
    if delta < timedelta(minutes=1):
        return "just now"
    if delta < timedelta(hours=1):
        minutes = int(delta.total_seconds() // 60)
        return f"{minutes}m"
    if delta < timedelta(days=1):
        hours = int(delta.total_seconds() // 3600)
        return f"{hours}h"
    days = delta.days
    if days < 7:
        return f"{days}d"
    weeks = max(days // 7, 1)
    if weeks < 8:
        return f"{weeks}w"
    return f"{max(days // 30, 1)}mo"


def format_check_state(state: str, style: Style) -> str:
    if state in {"SUCCESS"}:
        return style.good(state.lower())
    if state in {"FAILURE", "ERROR"}:
        return style.bad(state.lower())
    if state in {"PENDING", "EXPECTED"}:
        return style.warn(state.lower())
    return style.dim(state.lower())


def pad_plain(text: str, width: int) -> str:
    text = text[:width]
    return text + " " * max(width - len(text), 0)


def pad_ansi(text: str, width: int) -> str:
    return text + " " * max(width - visible_len(text), 0)


def truncate_plain(text: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def truncate_ansi(text: str, width: int) -> str:
    if visible_len(text) <= width:
        return pad_ansi(text, width)
    plain = strip_ansi(text)
    return truncate_plain(plain, width)


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def visible_len(text: str) -> int:
    return len(strip_ansi(text))


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def iso_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def format_timestamp(value: datetime, short: bool = False) -> str:
    fmt = "%H:%M:%S" if short else "%Y-%m-%d %H:%M"
    return value.astimezone().strftime(fmt)


def sort_by_staleness(items: list[Any], now: datetime) -> list[Any]:
    return sorted(items, key=lambda item: age_key(item.updated_at, now), reverse=True)


def age_key(updated_at: datetime | None, now: datetime) -> int:
    if updated_at is None:
        return -1
    return int((now - updated_at).total_seconds())


def sort_prs_for_failing_focus(prs: list[PullRequest], now: datetime) -> list[PullRequest]:
    return sorted(
        prs,
        key=lambda pr: (
            0 if pr.check_state in {"FAILURE", "ERROR"} else 1,
            -age_key(pr.updated_at, now),
            -(pr.updated_at.timestamp() if pr.updated_at else 0),
        ),
    )


def unique_pr_list(prs: list[PullRequest]) -> list[PullRequest]:
    seen: set[str] = set()
    ordered = []
    for pr in prs:
        if pr.key in seen:
            continue
        seen.add(pr.key)
        ordered.append(pr)
    return ordered


def append_more_line(lines: list[str], total: int, limit: int, style: Style) -> None:
    remaining = total - limit
    if remaining > 0:
        lines.append(style.dim(f"+{remaining} more"))


def trim_blank_tail(lines: list[str]) -> list[str]:
    out = list(lines)
    while out and strip_ansi(out[-1]).strip() == "":
        out.pop()
    return out


def clear_screen_for_watch(iteration: int) -> None:
    if sys.stdout.isatty():
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()
        return
    if iteration > 1:
        print("\n" + "=" * 72 + "\n")


def export_outputs(data: DashboardData, args: argparse.Namespace) -> None:
    if args.export_md:
        path = Path(args.export_md)
        path.write_text(render_markdown_export(data, args), encoding="utf-8")
        print(f"Wrote Markdown export to {path}")
    if args.export_html:
        path = Path(args.export_html)
        path.write_text(render_html_export(data, args), encoding="utf-8")
        print(f"Wrote HTML export to {path}")


def render_markdown_export(data: DashboardData, args: argparse.Namespace) -> str:
    lines = [
        "# GitPulse Standup",
        "",
        f"Generated: {format_timestamp(data.generated_at)}",
        f"Viewer: {data.viewer_name} (`{data.viewer_login}`)",
        f"Focus: {data.focus_label}",
        "",
        "## Summary",
        "",
        f"- Active repos: {data.summary['active_repos']}",
        f"- Reviews waiting: {data.summary['reviews_waiting']}",
        f"- Failing PRs: {data.summary['failing_prs']}",
        f"- Assigned issues: {data.summary['assigned_issues']}",
        f"- Do now: {data.summary['attention_now']}",
        f"- At risk: {data.summary['attention_risk']}",
        f"- Streak: {data.current_streak} days",
        "",
        "## Daily Brief",
        "",
        data.daily_brief,
        "",
        "## Attention Radar",
        "",
    ]
    for item in data.attention_items[: min(len(data.attention_items), max(5, min(args.limit + 2, ATTENTION_LIMIT)))]:
        lines.extend(
            [
                f"- [{item.bucket}] `{item.key}` {item.title}",
                f"  Reason: {item.reason}",
                f"  Next: `{item.next_step}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Active Repos",
            "",
            "| Repo | Last Push | Language | Open PRs/Issues |",
            "| --- | --- | --- | --- |",
        ]
    )
    for repo in data.repos[: args.limit]:
        lines.append(f"| {repo.name} | {relative_time(repo.pushed_at, data.generated_at)} | {repo.language} | {repo.open_prs}/{repo.open_issues} |")

    lines.extend(section_pr_markdown("## Review Queue", data.review_prs, data.generated_at, args.limit))
    lines.extend(section_pr_markdown("## Failing Or Ready PRs", unique_pr_list(data.failing_prs + data.ready_prs), data.generated_at, args.limit))
    lines.extend(section_issue_markdown("## Assigned Issues", data.assigned_issues, data.generated_at, args.limit))
    lines.extend(
        [
            "",
            "## Streak Stats",
            "",
            f"- Current streak: {data.current_streak} days",
            f"- Longest streak: {data.longest_streak} days",
            f"- Contributions in last 365 days: {data.contribution_total}",
            "",
            "## Recent Changes",
            "",
        ]
    )
    for change in data.changes:
        lines.append(f"- {change}")
    lines.append("")
    return "\n".join(lines)


def section_pr_markdown(title: str, prs: list[PullRequest], now: datetime, limit: int) -> list[str]:
    lines = ["", title, ""]
    if not prs:
        lines.append("- None")
        return lines
    for pr in prs[:limit]:
        lines.append(
            f"- `{pr.key}` {pr.title} ({relative_time(pr.updated_at, now)}, checks: {pr.check_state.lower()}, merge: {pr.merge_state_status.lower()})"
        )
    return lines


def section_issue_markdown(title: str, issues: list[Issue], now: datetime, limit: int) -> list[str]:
    lines = ["", title, ""]
    if not issues:
        lines.append("- None")
        return lines
    for issue in issues[:limit]:
        lines.append(f"- `{issue.key}` {issue.title} ({relative_time(issue.updated_at, now)})")
    return lines


def render_html_export(data: DashboardData, args: argparse.Namespace) -> str:
    attention_cards = "".join(render_attention_card_html(item) for item in data.attention_items[: min(len(data.attention_items), max(5, min(args.limit + 2, ATTENTION_LIMIT)))])
    repo_rows = "".join(
        f"<tr><td>{h(repo.name)}</td><td>{h(relative_time(repo.pushed_at, data.generated_at))}</td><td>{h(repo.language)}</td><td>{repo.open_prs}/{repo.open_issues}</td><td>{h(repo.description or '')}</td></tr>"
        for repo in data.repos[: args.limit]
    )
    review_rows = "".join(render_pr_row_html(pr, data.generated_at) for pr in data.review_prs[: args.limit])
    authored_rows = "".join(render_pr_row_html(pr, data.generated_at) for pr in unique_pr_list(data.failing_prs + data.ready_prs)[: args.limit])
    issue_rows = "".join(
        f"<tr><td>{h(issue.key)}</td><td>{h(issue.title)}</td><td>{h(relative_time(issue.updated_at, data.generated_at))}</td></tr>"
        for issue in data.assigned_issues[: args.limit]
    )
    changes = "".join(f"<li class='{change_class(change)}'>{h(change)}</li>" for change in data.changes)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GitPulse Standup</title>
<style>
body {{
  margin: 0;
  font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: linear-gradient(180deg, #f4f7fb 0%, #eef3f8 100%);
  color: #102033;
}}
.wrap {{ max-width: 1180px; margin: 0 auto; padding: 32px 20px 48px; }}
.hero {{
  background: linear-gradient(135deg, #14324d, #1d5a73);
  color: #fff;
  border-radius: 20px;
  padding: 28px;
  box-shadow: 0 18px 48px rgba(10, 37, 64, 0.18);
}}
.hero h1 {{ margin: 0 0 8px; font-size: 34px; }}
.hero p {{ margin: 0; opacity: 0.92; }}
.chips {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 18px; }}
.chip {{
  background: rgba(255,255,255,0.14);
  border: 1px solid rgba(255,255,255,0.18);
  padding: 8px 12px;
  border-radius: 999px;
  font-weight: 600;
}}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 18px; margin-top: 20px; }}
.card {{
  background: #fff;
  border-radius: 18px;
  padding: 18px;
  box-shadow: 0 12px 32px rgba(12, 31, 54, 0.08);
}}
.card h2 {{ margin: 0 0 12px; font-size: 20px; }}
.metrics {{ display: flex; flex-wrap: wrap; gap: 10px; }}
.metric {{
  border-radius: 14px;
  padding: 10px 12px;
  background: #f2f7fb;
  min-width: 120px;
}}
.metric strong {{ display: block; font-size: 22px; }}
.badge {{
  display: inline-block;
  border-radius: 999px;
  padding: 5px 10px;
  font-size: 12px;
  font-weight: 700;
  letter-spacing: .04em;
}}
.badge.red {{ background: #ffe1df; color: #9d1d14; }}
.badge.yellow {{ background: #fff1c7; color: #7a5a00; }}
.badge.green {{ background: #d9f6de; color: #166534; }}
.badge.gray {{ background: #e7edf3; color: #445566; }}
.attention {{ display: grid; gap: 12px; }}
.attention .item {{
  border: 1px solid #e6edf4;
  border-radius: 14px;
  padding: 14px;
  background: #fbfdff;
}}
.attention h3 {{ margin: 8px 0 6px; font-size: 17px; }}
.cmd {{
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  background: #102033;
  color: #e7f4ff;
  border-radius: 999px;
  display: inline-block;
  padding: 6px 10px;
  font-size: 12px;
}}
table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid #e8eef5; vertical-align: top; }}
th {{ color: #4d6277; font-weight: 700; }}
ul.changes {{ padding-left: 18px; margin: 0; }}
ul.changes li.plus {{ color: #166534; }}
ul.changes li.minus {{ color: #9d1d14; }}
ul.changes li.tilde {{ color: #8a6400; }}
</style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>GitPulse</h1>
      <p>{h(data.viewer_name)} ({h(data.viewer_login)}) • {h(format_timestamp(data.generated_at))}</p>
      <p>{h(data.subtitle)}</p>
      <div class="chips">
        <span class="chip">Focus: {h(data.focus_label)}</span>
        <span class="chip">Reviews {data.summary['reviews_waiting']}</span>
        <span class="chip">Failing {data.summary['failing_prs']}</span>
        <span class="chip">Assigned {data.summary['assigned_issues']}</span>
        <span class="chip">Streak {data.current_streak}d</span>
      </div>
    </section>

    <div class="grid">
      <section class="card">
        <h2>Daily Brief</h2>
        <p>{h(data.daily_brief)}</p>
      </section>
      <section class="card">
        <h2>Summary Metrics</h2>
        <div class="metrics">
          <div class="metric"><span>Active repos</span><strong>{data.summary['active_repos']}</strong></div>
          <div class="metric"><span>Do now</span><strong>{data.summary['attention_now']}</strong></div>
          <div class="metric"><span>At risk</span><strong>{data.summary['attention_risk']}</strong></div>
          <div class="metric"><span>Longest streak</span><strong>{data.longest_streak}</strong></div>
        </div>
      </section>
    </div>

    <section class="card">
      <h2>Attention Radar</h2>
      <div class="attention">{attention_cards or "<p>No attention items.</p>"}</div>
    </section>

    <div class="grid">
      <section class="card">
        <h2>Recent Changes</h2>
        <ul class="changes">{changes}</ul>
      </section>
      <section class="card">
        <h2>Streak Stats</h2>
        <div class="metrics">
          <div class="metric"><span>Current streak</span><strong>{data.current_streak}</strong></div>
          <div class="metric"><span>Longest streak</span><strong>{data.longest_streak}</strong></div>
          <div class="metric"><span>365d contributions</span><strong>{data.contribution_total}</strong></div>
        </div>
      </section>
    </div>

    <section class="card">
      <h2>Active Repos</h2>
      <table><thead><tr><th>Repo</th><th>Last Push</th><th>Language</th><th>Open</th><th>Description</th></tr></thead><tbody>{repo_rows or "<tr><td colspan='5'>None</td></tr>"}</tbody></table>
    </section>
    <section class="card">
      <h2>Review Queue</h2>
      <table><thead><tr><th>PR</th><th>Title</th><th>Updated</th><th>Checks</th></tr></thead><tbody>{review_rows or "<tr><td colspan='4'>None</td></tr>"}</tbody></table>
    </section>
    <section class="card">
      <h2>Failing Or Ready PRs</h2>
      <table><thead><tr><th>PR</th><th>Title</th><th>Updated</th><th>Checks</th></tr></thead><tbody>{authored_rows or "<tr><td colspan='4'>None</td></tr>"}</tbody></table>
    </section>
    <section class="card">
      <h2>Assigned Issues</h2>
      <table><thead><tr><th>Issue</th><th>Title</th><th>Updated</th></tr></thead><tbody>{issue_rows or "<tr><td colspan='3'>None</td></tr>"}</tbody></table>
    </section>
  </div>
</body>
</html>
"""


def render_attention_card_html(item: ActionItem) -> str:
    return (
        "<div class='item'>"
        f"{badge_html(item.bucket)} "
        f"<h3>{h(item.key)} {h(item.title)}</h3>"
        f"<p>{h(item.reason)}</p>"
        f"<p><span class='cmd'>{h(item.next_step)}</span></p>"
        "</div>"
    )


def render_pr_row_html(pr: PullRequest, now: datetime) -> str:
    state = "ready" if is_ready_pr(pr) else pr.check_state.lower()
    return f"<tr><td>{h(pr.key)}</td><td>{h(pr.title)}</td><td>{h(relative_time(pr.updated_at, now))}</td><td>{h(state)}</td></tr>"


def badge_html(label: str) -> str:
    css = {
        "DO NOW": "red",
        "AT RISK": "yellow",
        "WAITING": "gray",
        "READY": "green",
    }.get(label, "gray")
    return f"<span class='badge {css}'>{h(label)}</span>"


def change_class(change: str) -> str:
    if change.startswith("+"):
        return "plus"
    if change.startswith("-"):
        return "minus"
    if change.startswith("~"):
        return "tilde"
    return ""


def h(value: str) -> str:
    return html.escape(value, quote=True)


def epoch_utc() -> datetime:
    return datetime.fromtimestamp(0, tz=timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
