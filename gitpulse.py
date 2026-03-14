#!/usr/bin/env python3
"""GitPulse: a single-file GitHub terminal dashboard powered by gh."""

from __future__ import annotations

import argparse
import contextlib
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


CACHE_VERSION = 3
CACHE_DIR = Path.home() / ".gitpulse"
CACHE_PATH = CACHE_DIR / "cache.json"
HISTORY_PATH = CACHE_DIR / "history.jsonl"
DEFAULT_CONFIG_PATH = CACHE_DIR / "config.json"

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
HISTORY_LIMIT = 90
NOTIFICATION_FETCH_LIMIT = 25
REPO_WORKFLOW_LIMIT = 8
WEEKLY_WINDOW_DAYS = 7
REPO_HEALTH_MATRIX_LIMIT = 5
RECENT_WINS_LIMIT = 6

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

BUILTIN_DEFAULTS: dict[str, Any] = {
    "limit": 6,
    "width": "auto",
    "refresh": False,
    "no_cache": False,
    "watch": False,
    "interval": DEFAULT_INTERVAL,
    "iterations": None,
    "export_md": None,
    "export_html": None,
    "export_update": None,
    "reviews": False,
    "failing": False,
    "stale": False,
    "inbox": False,
    "commands": False,
    "digest": None,
    "standup": False,
    "repo": None,
    "org": None,
}

CONFIGURABLE_OPTIONS = set(BUILTIN_DEFAULTS)

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
class NotificationItem:
    id: str
    repository: str
    subject_type: str
    title: str
    reason: str
    updated_at: datetime | None
    url: str
    unread: bool
    last_read_at: datetime | None
    score: int
    reason_label: str
    next_step: str

    @property
    def key(self) -> str:
        return f"{self.repository}:{self.id}"


@dataclass
class DigestDelta:
    value: int
    previous: int
    delta: int


@dataclass
class DigestMetrics:
    mode: str
    period_label: str
    comparison_label: str
    merged_authored_prs: DigestDelta
    reviews_completed: DigestDelta
    issues_closed: DigestDelta
    active_repos_touched: DigestDelta
    streak_change: DigestDelta
    narrative: str


@dataclass
class RepoHealth:
    repository: str
    workflow_runs: list[dict[str, Any]]
    failing_runs: int
    successful_runs: int
    oldest_open_pr: PullRequest | None
    oldest_open_issue: Issue | None
    merge_ready_prs: list[PullRequest]
    latest_release: dict[str, Any] | None
    next_steps: list[str]
    notes: list[str]


@dataclass
class RecentWinItem:
    kind: str
    repository: str
    number: int
    title: str
    url: str
    closed_at: datetime | None

    @property
    def key(self) -> str:
        return f"{self.repository}#{self.number}"


@dataclass
class RecentWins:
    merged_prs: list[RecentWinItem]
    closed_issues: list[RecentWinItem]
    merged_pr_count: int
    closed_issue_count: int
    narrative: str

    @property
    def total_count(self) -> int:
        return self.merged_pr_count + self.closed_issue_count

    @property
    def top_items(self) -> list[RecentWinItem]:
        return sorted(
            [*self.merged_prs, *self.closed_issues],
            key=lambda item: item.closed_at or epoch_utc(),
            reverse=True,
        )


@dataclass
class HistoryEntry:
    generated_at: datetime
    review_queue_count: int
    assigned_issue_count: int
    failing_pr_count: int
    active_repo_count: int
    review_pr_ids: set[str]
    issue_ids: set[str]
    ready_pr_ids: set[str]
    repo_pushes: dict[str, str]
    pr_check_states: dict[str, str]


@dataclass
class TimelineMetric:
    label: str
    values: list[int]
    current: int
    previous: int
    delta: int


@dataclass
class MomentumTimeline:
    metrics: list[TimelineMetric]
    narrative: str
    sample_count: int
    span_days: int
    fallback: str | None = None


@dataclass
class CommandSuggestion:
    label: str
    command: str
    reason: str
    priority: int


@dataclass
class ChangeEvent:
    badge: str
    summary: str
    sign: str
    priority: int
    command: str | None = None


@dataclass
class DailyPlanItem:
    summary: str
    urgency: str
    reason: str
    command: str | None
    priority: int


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
    notifications: list[NotificationItem]
    inbox_items: list[NotificationItem]
    repo_health_matrix: list[RepoHealth]
    repo_health: RepoHealth | None
    recent_wins: RecentWins
    momentum_timeline: MomentumTimeline
    digest: DigestMetrics | None
    daily_plan: list[DailyPlanItem]
    command_suggestions: list[CommandSuggestion]
    command_catalog: list[CommandSuggestion]
    change_feed: list[ChangeEvent]
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
    share_update: str
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
    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument("--config", metavar="PATH")
    config_parser.add_argument("--profile", metavar="NAME")
    early_args, _ = config_parser.parse_known_args(argv)

    config_path = Path(os.path.expanduser(early_args.config)) if early_args.config else DEFAULT_CONFIG_PATH
    try:
        config_payload = load_config(config_path)
    except GhError as exc:
        config_parser.error(str(exc))

    merged_defaults = dict(BUILTIN_DEFAULTS)
    merged_defaults.update(config_payload["defaults"])
    if early_args.profile:
        profiles = config_payload["profiles"]
        if early_args.profile not in profiles:
            available = ", ".join(sorted(profiles)) or "none"
            config_parser.error(f"Unknown profile '{early_args.profile}'. Available profiles: {available}.")
        merged_defaults.update(profiles[early_args.profile])

    parser = build_parser(merged_defaults)
    args = parser.parse_args(argv)
    args.config_path = config_path

    explicit_interval = has_flag(argv, "--interval")
    explicit_iterations = has_flag(argv, "--iterations")

    if args.limit < 1:
        parser.error("--limit must be at least 1.")
    if args.interval < 1:
        parser.error("--interval must be at least 1.")
    if args.iterations is not None and args.iterations < 1:
        parser.error("--iterations must be at least 1.")
    if not args.watch and explicit_interval and args.interval != DEFAULT_INTERVAL:
        parser.error("--interval requires --watch.")
    if not args.watch and explicit_iterations and args.iterations is not None:
        parser.error("--iterations requires --watch.")
    if args.repo and not REPO_RE.match(args.repo):
        parser.error("--repo must look like OWNER/NAME.")
    if args.standup and args.watch:
        parser.error("--standup is not supported with --watch.")
    return args


def build_parser(defaults: dict[str, Any]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gitpulse.py",
        description="Render a GitHub terminal dashboard powered by gh.",
    )
    parser.add_argument("--config", metavar="PATH", default=str(DEFAULT_CONFIG_PATH), help="Override the config file path.")
    parser.add_argument("--profile", metavar="NAME", help="Use a named profile from the config file.")
    parser.add_argument(
        "--limit",
        type=int,
        default=defaults["limit"],
        help=f"Rows to show per section (default: {defaults['limit']}).",
    )
    parser.add_argument(
        "--width",
        choices=["auto", "full"],
        default=defaults["width"],
        help="Clamp layout for readability or use the full terminal width.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        default=bool(defaults["refresh"]),
        help="Ignore the previous disk cache snapshot but write a new one at the end.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=bool(defaults["no_cache"]),
        help="Disable disk cache reads and writes for this run.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        default=bool(defaults["watch"]),
        help="Refresh the dashboard continuously.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=defaults["interval"],
        help=f"Refresh interval in watch mode (default: {defaults['interval']}).",
    )
    parser.add_argument(
        "--iterations",
        default=defaults["iterations"],
        type=int,
        help="Stop after N watch iterations. Useful for tests.",
    )
    parser.add_argument("--export-md", metavar="PATH", default=defaults["export_md"], help="Write a Markdown standup export.")
    parser.add_argument("--export-html", metavar="PATH", default=defaults["export_html"], help="Write a self-contained HTML export.")
    parser.add_argument("--export-update", metavar="PATH", default=defaults["export_update"], help="Write a compact team update in plain text.")
    parser.add_argument("--reviews", action="store_true", default=bool(defaults["reviews"]), help="Focus on review-requested PRs.")
    parser.add_argument("--failing", action="store_true", default=bool(defaults["failing"]), help="Focus on authored PRs with failing checks.")
    parser.add_argument("--stale", action="store_true", default=bool(defaults["stale"]), help="Focus on work stale for 3+ days.")
    parser.add_argument("--inbox", action="store_true", default=bool(defaults["inbox"]), help="Focus on unread mentions, assignments, and review requests.")
    parser.add_argument(
        "--commands",
        action="store_true",
        default=bool(defaults["commands"]),
        help="Show a larger actionable command section.",
    )
    parser.add_argument(
        "--digest",
        choices=["daily", "weekly"],
        default=defaults["digest"],
        help="Show a digest section for the current snapshot or a trailing weekly comparison.",
    )
    parser.add_argument("--standup", action="store_true", default=bool(defaults["standup"]), help="Print a compact paste-ready team update to stdout.")
    parser.add_argument("--repo", metavar="OWNER/NAME", default=defaults["repo"], help="Filter to a single repository.")
    parser.add_argument("--org", metavar="ORGNAME", default=defaults["org"], help="Filter to repositories owned by one org.")
    return parser


def has_flag(argv: list[str], flag: str) -> bool:
    return any(token == flag or token.startswith(f"{flag}=") for token in argv)


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"defaults": {}, "profiles": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise GhError(f"Unable to read config file {path}: {exc}.") from exc
    except json.JSONDecodeError as exc:
        raise GhError(f"Config file {path} does not contain valid JSON: {exc}.") from exc

    if not isinstance(payload, dict):
        raise GhError(f"Config file {path} must contain a JSON object.")

    unknown_top_level = sorted(set(payload) - {"defaults", "profiles"} - CONFIGURABLE_OPTIONS)
    if unknown_top_level:
        raise GhError(f"Unknown config keys: {', '.join(unknown_top_level)}.")

    raw_defaults = payload.get("defaults")
    if raw_defaults is not None and not isinstance(raw_defaults, dict):
        raise GhError("Config `defaults` must be an object.")
    defaults = dict(raw_defaults or {})
    inline_defaults = {key: value for key, value in payload.items() if key in CONFIGURABLE_OPTIONS}
    defaults = {**defaults, **inline_defaults}
    raw_profiles = payload.get("profiles")
    if raw_profiles is not None and not isinstance(raw_profiles, dict):
        raise GhError("Config `profiles` must be an object keyed by profile name.")
    profiles = raw_profiles or {}

    validate_config_values(defaults, "defaults")
    for profile_name, values in profiles.items():
        if not isinstance(profile_name, str) or not profile_name.strip():
            raise GhError("Profile names must be non-empty strings.")
        if not isinstance(values, dict):
            raise GhError(f"Profile '{profile_name}' must be a JSON object.")
        validate_config_values(values, f"profile '{profile_name}'")
    return {"defaults": defaults, "profiles": profiles}


def validate_config_values(values: dict[str, Any], label: str) -> None:
    unknown_keys = sorted(set(values) - CONFIGURABLE_OPTIONS)
    if unknown_keys:
        raise GhError(f"Unknown keys in {label}: {', '.join(unknown_keys)}.")

    for key, value in values.items():
        if key in {"limit", "interval"}:
            if not is_positive_int(value):
                raise GhError(f"{label} field `{key}` must be a positive integer.")
        elif key == "iterations":
            if value is not None and not is_positive_int(value):
                raise GhError(f"{label} field `{key}` must be null or a positive integer.")
        elif key == "width":
            if value not in {"auto", "full"}:
                raise GhError(f"{label} field `width` must be `auto` or `full`.")
        elif key == "digest":
            if value is not None and value not in {"daily", "weekly"}:
                raise GhError(f"{label} field `digest` must be `daily`, `weekly`, or null.")
        elif key == "repo":
            if value is not None and (not isinstance(value, str) or not REPO_RE.match(value)):
                raise GhError(f"{label} field `repo` must look like OWNER/NAME.")
        elif key in {"org", "export_md", "export_html", "export_update"}:
            if value is not None and not isinstance(value, str):
                raise GhError(f"{label} field `{key}` must be a string or null.")
        elif key in {"refresh", "no_cache", "watch", "reviews", "failing", "stale", "inbox", "commands", "standup"}:
            if not isinstance(value, bool):
                raise GhError(f"{label} field `{key}` must be true or false.")


def is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


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
        persist_run_state(data, args, write_history=True)
        if args.standup:
            print(data.share_update)
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

        persist_run_state(
            data,
            args,
            write_history=bool(args.iterations is not None and iteration >= args.iterations),
            snapshot=current_snapshot,
        )

        if args.iterations is not None and iteration >= args.iterations:
            return 0

        try:
            time_module.sleep(args.interval)
        except KeyboardInterrupt:
            return 130


def persist_run_state(
    data: DashboardData,
    args: argparse.Namespace,
    write_history: bool,
    snapshot: dict[str, Any] | None = None,
) -> None:
    if args.no_cache:
        return
    snapshot = snapshot or snapshot_from_dashboard(data)
    write_cache(snapshot)
    if write_history:
        append_history(snapshot)


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
    notifications = fetch_notifications(args, generated_at)

    repos, review_prs, authored_prs, assigned_issues, notifications = apply_filters(
        repos, review_prs, authored_prs, assigned_issues, notifications, args, generated_at
    )

    history = read_history()
    history_entries = read_history_entries(generated_at)
    recent_wins = fetch_recent_wins(viewer_login, generated_at, RECENT_WINS_LIMIT)
    attention_items = build_attention_items(
        review_prs, authored_prs, assigned_issues, notifications, generated_at, args.limit
    )
    failing_prs = [pr for pr in authored_prs if pr.check_state in {"FAILURE", "ERROR"}]
    ready_prs = [pr for pr in authored_prs if is_ready_pr(pr)]
    inbox_items = notifications[: args.limit]
    repo_health_targets = [repo.name for repo in repos[:REPO_HEALTH_MATRIX_LIMIT]]
    if args.repo and args.repo not in repo_health_targets:
        repo_health_targets.insert(0, args.repo)
    repo_health_lookup = fetch_repo_health_collection(
        repo_health_targets,
        viewer_login,
        limit=REPO_HEALTH_MATRIX_LIMIT + (1 if args.repo else 0),
        detailed_repo=args.repo,
    )
    repo_health_matrix = [repo_health_lookup[name] for name in repo_health_targets if name in repo_health_lookup][:REPO_HEALTH_MATRIX_LIMIT]
    repo_health = repo_health_lookup.get(args.repo) if args.repo else None

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
        "inbox_unread": sum(1 for item in notifications if item.unread),
        "mentions": sum(1 for item in notifications if item.reason == "mention"),
        "review_requests": sum(1 for item in notifications if item.reason == "review_requested"),
        "attention_now": sum(1 for item in attention_items if item.bucket == "DO NOW"),
        "attention_risk": sum(1 for item in attention_items if item.bucket == "AT RISK"),
        "current_streak": current_streak,
        "recent_win_total": recent_wins.total_count,
        "recent_merged_prs": recent_wins.merged_pr_count,
        "recent_closed_issues": recent_wins.closed_issue_count,
    }
    focus_label = build_focus_label(args)
    subtitle = build_subtitle(args)
    daily_brief = build_daily_brief(summary, repos, attention_items, recent_wins, focus_label)
    current_history = make_history_entry_from_snapshot(
        {
            "generated_at": iso_datetime(generated_at),
            "reviews_waiting": summary["reviews_waiting"],
            "assigned_issues": summary["assigned_issues"],
            "failing_prs": summary["failing_prs"],
            "active_repos": summary["active_repos"],
            "review_pr_ids": [pr.key for pr in review_prs],
            "issue_ids": [issue.key for issue in assigned_issues],
            "ready_pr_ids": [pr.key for pr in ready_prs],
            "repo_pushes": {repo.name: iso_datetime(repo.pushed_at) for repo in repos if repo.pushed_at},
            "pr_check_states": {pr.key: pr.check_state for pr in authored_prs},
        }
    )
    if current_history:
        history_entries = [*history_entries, current_history]
    changes = build_changes(
        previous_snapshot,
        repos,
        review_prs,
        authored_prs,
        assigned_issues,
        current_streak,
        generated_at,
    )
    change_feed = build_change_feed(
        previous_snapshot,
        repos,
        review_prs,
        authored_prs,
        assigned_issues,
        generated_at,
        limit=max(4, min(args.limit + 2, 8)),
    )
    momentum_timeline = build_momentum_timeline(history_entries)
    digest = build_digest(args, history, generated_at, viewer_login, current_streak)
    command_suggestions, command_catalog = build_command_suggestions(
        args,
        attention_items,
        review_prs,
        authored_prs,
        failing_prs,
        assigned_issues,
        inbox_items,
        repo_health,
        repos,
    )
    daily_plan = build_daily_plan(
        attention_items,
        inbox_items,
        repo_health_matrix,
        failing_prs,
        assigned_issues,
        limit=min(max(3, args.limit), 5),
    )
    share_update = build_share_update(
        summary=summary,
        attention_items=attention_items,
        notifications=notifications,
        digest=digest,
        recent_wins=recent_wins,
        repo_health=repo_health,
        focus_label=focus_label,
        daily_plan=daily_plan,
        command_suggestions=command_suggestions,
        momentum_timeline=momentum_timeline,
    )

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
        notifications=notifications,
        inbox_items=inbox_items,
        repo_health_matrix=repo_health_matrix,
        repo_health=repo_health,
        recent_wins=recent_wins,
        momentum_timeline=momentum_timeline,
        digest=digest,
        daily_plan=daily_plan,
        command_suggestions=command_suggestions,
        command_catalog=command_catalog,
        change_feed=change_feed,
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
        share_update=share_update,
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


def fetch_notifications(args: argparse.Namespace, now: datetime) -> list[NotificationItem]:
    with contextlib.suppress(GhError):
        payload = run_gh_json(
            [
                "api",
                f"notifications?all=false&participating=false&per_page={NOTIFICATION_FETCH_LIMIT}",
            ]
        )
        if isinstance(payload, list):
            notifications = [notification_from_api(node, now) for node in payload]
            ranked = [item for item in notifications if item is not None]
            ranked.sort(
                key=lambda item: (
                    -item.score,
                    0 if item.unread else 1,
                    -(item.updated_at.timestamp() if item.updated_at else 0),
                    item.repository.lower(),
                )
            )
            return ranked
    return []


def notification_from_api(node: dict[str, Any], now: datetime) -> NotificationItem | None:
    if not node:
        return None
    repo_name = ((node.get("repository") or {}).get("full_name") or "unknown/unknown").strip()
    updated_at = parse_dt(node.get("updated_at"))
    last_read_at = parse_dt(node.get("last_read_at"))
    subject = node.get("subject") or {}
    subject_type = (subject.get("type") or "Notification").strip()
    reason = (node.get("reason") or "unknown").strip()
    metadata = notification_reason_metadata(reason)
    title = (subject.get("title") or f"{subject_type} update").strip()
    url, next_step = resolve_notification_target(repo_name, subject_type, subject.get("url"), subject.get("latest_comment_url"))
    score = metadata["score"] + stale_score(updated_at, now)
    if node.get("unread", False):
        score += 8
    return NotificationItem(
        id=str(node.get("id") or title),
        repository=repo_name,
        subject_type=subject_type,
        title=title,
        reason=reason,
        updated_at=updated_at,
        url=url,
        unread=bool(node.get("unread")),
        last_read_at=last_read_at,
        score=score,
        reason_label=metadata["label"],
        next_step=next_step,
    )


def notification_reason_metadata(reason: str) -> dict[str, Any]:
    mapping = {
        "review_requested": {"label": "Review Request", "score": 130},
        "mention": {"label": "Mention", "score": 126},
        "assign": {"label": "Assigned", "score": 118},
        "author": {"label": "Author Update", "score": 86},
        "comment": {"label": "Comment", "score": 82},
        "subscribed": {"label": "Subscribed", "score": 72},
    }
    return mapping.get(reason, {"label": reason.replace("_", " ").title() or "Notification", "score": 64})


def resolve_notification_target(
    repository: str,
    subject_type: str,
    subject_url: str | None,
    latest_comment_url: str | None,
) -> tuple[str, str]:
    candidate = latest_comment_url or subject_url or ""
    if not candidate:
        return "", f"gh notification view --repo {repository}"
    if candidate.startswith("https://github.com/"):
        return candidate, command_for_subject_url(repository, subject_type, candidate)

    patterns = [
        (r"/repos/([^/]+/[^/]+)/pulls/(\d+)$", lambda repo, num: (f"https://github.com/{repo}/pull/{num}", f"gh pr view {num} -R {repo} --web")),
        (r"/repos/([^/]+/[^/]+)/issues/(\d+)$", lambda repo, num: (f"https://github.com/{repo}/issues/{num}", f"gh issue view {num} -R {repo} --comments")),
        (
            r"/repos/([^/]+/[^/]+)/pulls/comments/(\d+)$",
            lambda repo, num: (f"https://github.com/{repo}/pulls", f"gh repo view {repo} --web"),
        ),
        (
            r"/repos/([^/]+/[^/]+)/commits/([0-9a-fA-F]+)$",
            lambda repo, sha: (f"https://github.com/{repo}/commit/{sha}", f"gh browse {repo} -- commit/{sha}"),
        ),
        (
            r"/repos/([^/]+/[^/]+)/releases/(\d+)$",
            lambda repo, rel_id: (f"https://github.com/{repo}/releases", f"gh release view --repo {repo} {rel_id}"),
        ),
    ]
    for pattern, builder in patterns:
        match = re.search(pattern, candidate)
        if match:
            return builder(*match.groups())
    return candidate, command_for_subject_url(repository, subject_type, candidate)


def command_for_subject_url(repository: str, subject_type: str, url: str) -> str:
    lowered = subject_type.lower()
    if "pull" in lowered:
        return f"gh pr list -R {repository} --limit 10"
    if "issue" in lowered:
        return f"gh issue list -R {repository} --limit 10"
    if "release" in lowered:
        return f"gh release list --repo {repository} --limit 1"
    return f"gh repo view {repository} --web"


def fetch_repo_health(repository: str, viewer_login: str) -> RepoHealth | None:
    return fetch_repo_health_summary(repository, viewer_login, verify_exists=True)


def fetch_repo_health_collection(
    repositories: list[str],
    viewer_login: str,
    limit: int,
    detailed_repo: str | None = None,
) -> dict[str, RepoHealth]:
    collected: dict[str, RepoHealth] = {}
    for repository in unique_preserving_order(repositories):
        if len(collected) >= limit and repository != detailed_repo:
            break
        health = fetch_repo_health_summary(
            repository,
            viewer_login,
            verify_exists=repository == detailed_repo,
        )
        if health is not None:
            collected[repository] = health
    return collected


def fetch_repo_health_summary(
    repository: str,
    viewer_login: str,
    verify_exists: bool = False,
) -> RepoHealth | None:
    notes: list[str] = []
    workflow_runs: list[dict[str, Any]] = []
    failing_runs = 0
    successful_runs = 0
    oldest_open_pr: PullRequest | None = None
    oldest_open_issue: Issue | None = None
    merge_ready_prs: list[PullRequest] = []
    latest_release: dict[str, Any] | None = None

    if verify_exists:
        repo_payload = run_gh_json(["api", f"repos/{repository}"])
        if repo_payload.get("full_name") != repository:
            raise GhError(f"Repository {repository} was not found.")

    try:
        workflows_payload = run_gh_json(["api", f"repos/{repository}/actions/runs?per_page={REPO_WORKFLOW_LIMIT}"])
        workflow_runs = workflows_payload.get("workflow_runs", [])[:REPO_WORKFLOW_LIMIT]
        for run in workflow_runs:
            conclusion = (run.get("conclusion") or "").lower()
            status = (run.get("status") or "").lower()
            if conclusion == "success":
                successful_runs += 1
            elif conclusion in {"failure", "timed_out", "cancelled", "action_required", "startup_failure"}:
                failing_runs += 1
            elif status == "completed":
                notes.append("Some recent workflow runs completed without a clear conclusion.")
    except GhError as exc:
        notes.append(f"Workflow data unavailable: {exc}")

    try:
        prs_payload = run_gh_json(
            [
                "api",
                "graphql",
                "-f",
                "query="
                + repo_health_query(),
                "-F",
                f"repo={repository.split('/')[0]}",
                "-F",
                f"name={repository.split('/')[1]}",
                "-F",
                f"viewer={viewer_login}",
            ]
        )
        repo_data = ((prs_payload.get("data") or {}).get("repository") or {})
        oldest_open_pr = parse_repo_pr_node(((repo_data.get("oldestPrs") or {}).get("nodes") or [None])[0], viewer_login)
        oldest_open_issue = parse_repo_issue_node(((repo_data.get("oldestIssues") or {}).get("nodes") or [None])[0])
        merge_ready_prs = [
            pr for pr in (parse_repo_pr_node(node, viewer_login) for node in ((repo_data.get("mergeReadyPrs") or {}).get("nodes") or []))
            if pr and is_ready_pr(pr)
        ]
        release = repo_data.get("latestRelease")
        if release:
            latest_release = {
                "name": release.get("name") or release.get("tagName") or "latest",
                "tag_name": release.get("tagName") or "",
                "published_at": parse_dt(release.get("publishedAt")),
                "url": release.get("url") or "",
                "is_draft": bool(release.get("isDraft")),
                "is_prerelease": bool(release.get("isPrerelease")),
            }
    except GhError as exc:
        notes.append(f"Repo deep-dive data unavailable: {exc}")

    next_steps = build_repo_health_next_steps(repository, workflow_runs, merge_ready_prs, oldest_open_pr, oldest_open_issue)
    return RepoHealth(
        repository=repository,
        workflow_runs=workflow_runs,
        failing_runs=failing_runs,
        successful_runs=successful_runs,
        oldest_open_pr=oldest_open_pr,
        oldest_open_issue=oldest_open_issue,
        merge_ready_prs=merge_ready_prs,
        latest_release=latest_release,
        next_steps=next_steps,
        notes=notes,
    )


def fetch_recent_wins(viewer_login: str, now: datetime, limit: int) -> RecentWins:
    start = now - timedelta(days=WEEKLY_WINDOW_DAYS)
    merged_query = (
        f"is:pr is:merged archived:false author:{viewer_login} "
        f"sort:updated-desc merged:{start.date()}..{now.date()}"
    )
    closed_query = (
        f"is:issue is:closed archived:false assignee:{viewer_login} "
        f"sort:updated-desc closed:{start.date()}..{now.date()}"
    )
    merged_payload = run_gh_json(["api", "search/issues", "-f", f"q={merged_query}", "-f", f"per_page={limit}"])
    closed_payload = run_gh_json(["api", "search/issues", "-f", f"q={closed_query}", "-f", f"per_page={limit}"])
    merged_prs = parse_recent_win_items(merged_payload.get("items", []), kind="Merged PR", timestamp_key="closed_at")
    closed_issues = parse_recent_win_items(closed_payload.get("items", []), kind="Closed issue", timestamp_key="closed_at")
    merged_count = int(merged_payload.get("total_count") or 0)
    closed_count = int(closed_payload.get("total_count") or 0)
    return RecentWins(
        merged_prs=merged_prs,
        closed_issues=closed_issues,
        merged_pr_count=merged_count,
        closed_issue_count=closed_count,
        narrative=build_recent_wins_narrative(merged_count, closed_count),
    )


def parse_recent_win_items(items: list[dict[str, Any]], kind: str, timestamp_key: str) -> list[RecentWinItem]:
    parsed: list[RecentWinItem] = []
    for item in items:
        repo_url = item.get("repository_url") or ""
        repository = repo_url.rsplit("/repos/", 1)[-1] if "/repos/" in repo_url else "unknown/unknown"
        parsed.append(
            RecentWinItem(
                kind=kind,
                repository=repository,
                number=int(item.get("number") or 0),
                title=item.get("title", ""),
                url=item.get("html_url", ""),
                closed_at=parse_dt(item.get(timestamp_key)),
            )
        )
    return parsed


def build_recent_wins_narrative(merged_count: int, closed_count: int) -> str:
    if merged_count == 0 and closed_count == 0:
        return "No completed work landed in the last 7 days."
    parts = []
    if merged_count:
        parts.append(f"merged {merged_count} PR{'s' if merged_count != 1 else ''}")
    if closed_count:
        parts.append(f"closed {closed_count} issue{'s' if closed_count != 1 else ''}")
    joined = " and ".join(parts)
    return f"In the last 7 days you {joined}."


def repo_health_query() -> str:
    return """
query RepoHealth($repo: String!, $name: String!, $viewer: String!) {
  repository(owner: $repo, name: $name) {
    oldestPrs: pullRequests(first: 1, states: OPEN, orderBy: {field: CREATED_AT, direction: ASC}) {
      nodes {
        ...PrFields
      }
    }
    mergeReadyPrs: pullRequests(first: 8, states: OPEN, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        ...PrFields
      }
    }
    oldestIssues: issues(first: 1, states: OPEN, orderBy: {field: CREATED_AT, direction: ASC}) {
      nodes {
        ...IssueFields
      }
    }
    latestRelease {
      name
      tagName
      publishedAt
      url
      isDraft
      isPrerelease
    }
  }
}
""" + PR_FIELDS + ISSUE_FIELDS


def parse_repo_pr_node(node: dict[str, Any] | None, viewer_login: str) -> PullRequest | None:
    if not node:
        return None
    prs = parse_prs([node], viewer_login)
    return prs[0] if prs else None


def parse_repo_issue_node(node: dict[str, Any] | None) -> Issue | None:
    if not node:
        return None
    issues = parse_issues([node])
    return issues[0] if issues else None


def build_repo_health_next_steps(
    repository: str,
    workflow_runs: list[dict[str, Any]],
    merge_ready_prs: list[PullRequest],
    oldest_open_pr: PullRequest | None,
    oldest_open_issue: Issue | None,
) -> list[str]:
    steps: list[str] = []
    if workflow_runs and any((run.get("conclusion") or "").lower() == "failure" for run in workflow_runs):
        steps.append(f"gh run list --repo {repository} --limit {REPO_WORKFLOW_LIMIT}")
    if merge_ready_prs:
        steps.append(pr_web_command(merge_ready_prs[0]))
    if oldest_open_pr:
        steps.append(pr_web_command(oldest_open_pr))
    if oldest_open_issue:
        steps.append(issue_comments_command(oldest_open_issue))
    if not steps:
        steps.append(f"gh repo view {repository} --web")
    return unique_preserving_order(steps)[:4]


def build_digest(
    args: argparse.Namespace,
    history: list[dict[str, Any]],
    now: datetime,
    viewer_login: str,
    current_streak: int,
) -> DigestMetrics | None:
    if not args.digest:
        return None
    if args.digest == "daily":
        local_now = now.astimezone()
        current_start = datetime.combine(local_now.date(), time.min, tzinfo=local_now.tzinfo).astimezone(timezone.utc)
        current = fetch_digest_window(viewer_login, current_start, now)
        return DigestMetrics(
            mode="daily",
            period_label=local_now.strftime("%Y-%m-%d"),
            comparison_label="today",
            merged_authored_prs=DigestDelta(current["merged_authored_prs"], current["merged_authored_prs"], 0),
            reviews_completed=DigestDelta(current["reviews_completed"], current["reviews_completed"], 0),
            issues_closed=DigestDelta(current["issues_closed"], current["issues_closed"], 0),
            active_repos_touched=DigestDelta(current["active_repos_touched"], current["active_repos_touched"], 0),
            streak_change=DigestDelta(current_streak, current_streak, 0),
            narrative=build_digest_narrative(
                "daily",
                current["merged_authored_prs"],
                current["reviews_completed"],
                current["issues_closed"],
                current["active_repos_touched"],
                0,
            ),
        )

    current_start = now - timedelta(days=WEEKLY_WINDOW_DAYS)
    previous_start = now - timedelta(days=WEEKLY_WINDOW_DAYS * 2)
    current = fetch_digest_window(viewer_login, current_start, now)
    previous = fetch_digest_window(viewer_login, previous_start, current_start)
    streak_previous = history_streak_at_or_before(history, current_start) or current_streak
    return DigestMetrics(
        mode="weekly",
        period_label=f"{current_start.astimezone().date()} to {now.astimezone().date()}",
        comparison_label=f"vs {previous_start.astimezone().date()} to {current_start.astimezone().date()}",
        merged_authored_prs=make_delta(current["merged_authored_prs"], previous["merged_authored_prs"]),
        reviews_completed=make_delta(current["reviews_completed"], previous["reviews_completed"]),
        issues_closed=make_delta(current["issues_closed"], previous["issues_closed"]),
        active_repos_touched=make_delta(current["active_repos_touched"], previous["active_repos_touched"]),
        streak_change=make_delta(current_streak, streak_previous),
        narrative=build_digest_narrative(
            "weekly",
            current["merged_authored_prs"],
            current["reviews_completed"],
            current["issues_closed"],
            current["active_repos_touched"],
            current_streak - streak_previous,
        ),
    )


def fetch_digest_window(viewer_login: str, start: datetime, end: datetime) -> dict[str, int]:
    merged = search_issue_count(
        f"is:pr is:merged archived:false author:{viewer_login} merged:{start.date()}..{end.date()}"
    )
    reviewed = search_issue_count(
        f"is:pr archived:false reviewed-by:{viewer_login} updated:{start.date()}..{end.date()}"
    )
    closed = search_issue_count(
        f"is:issue archived:false assignee:{viewer_login} closed:{start.date()}..{end.date()}"
    )
    touched_repos = search_repositories_touched(viewer_login, start, end)
    return {
        "merged_authored_prs": merged,
        "reviews_completed": reviewed,
        "issues_closed": closed,
        "active_repos_touched": touched_repos,
    }


def search_issue_count(query: str) -> int:
    payload = run_gh_json(["api", "search/issues", "-f", f"q={query}", "-f", "per_page=1"])
    return int(payload.get("total_count") or 0)


def search_repositories_touched(viewer_login: str, start: datetime, end: datetime) -> int:
    queries = [
        f"is:pr archived:false author:{viewer_login} updated:{start.date()}..{end.date()}",
        f"is:pr archived:false reviewed-by:{viewer_login} updated:{start.date()}..{end.date()}",
        f"is:issue archived:false assignee:{viewer_login} updated:{start.date()}..{end.date()}",
    ]
    repos: set[str] = set()
    for query in queries:
        payload = run_gh_json(["api", "search/issues", "-f", f"q={query}", "-f", "per_page=25"])
        for item in payload.get("items", []):
            repo_url = item.get("repository_url") or ""
            if "/repos/" in repo_url:
                repos.add(repo_url.rsplit("/repos/", 1)[-1])
    return len(repos)


def make_delta(current: int, previous: int) -> DigestDelta:
    return DigestDelta(value=current, previous=previous, delta=current - previous)


def build_digest_narrative(
    mode: str,
    merged_authored_prs: int,
    reviews_completed: int,
    issues_closed: int,
    active_repos_touched: int,
    streak_delta: int,
) -> str:
    streak_phrase = "held steady" if streak_delta == 0 else ("extended" if streak_delta > 0 else "slipped")
    period = "Today" if mode == "daily" else "This week"
    return (
        f"{period} you merged {merged_authored_prs} PRs you authored and completed {reviews_completed} review cycles. "
        f"You closed {issues_closed} assigned issues across {active_repos_touched} active repos. "
        f"Your contribution streak {streak_phrase}{'' if streak_delta == 0 else f' by {abs(streak_delta)} day' + ('s' if abs(streak_delta) != 1 else '')}."
    )


def build_share_update(
    summary: dict[str, int],
    attention_items: list[ActionItem],
    notifications: list[NotificationItem],
    digest: DigestMetrics | None,
    recent_wins: RecentWins,
    repo_health: RepoHealth | None,
    focus_label: str,
    daily_plan: list[DailyPlanItem],
    command_suggestions: list[CommandSuggestion],
    momentum_timeline: MomentumTimeline,
) -> str:
    wins = [recent_wins.narrative]
    if digest:
        wins.append(digest.narrative)
    elif summary["active_repos"]:
        wins.append(f"Touched {summary['active_repos']} active repos with a {summary['current_streak']}-day contribution streak.")
    blockers = [item for item in attention_items if item.bucket == "AT RISK"][:2]
    asks = [item for item in notifications if item.reason in {"review_requested", "mention", "assign"}][:2]
    next_items = attention_items[:2]
    if repo_health and repo_health.merge_ready_prs:
        merge_item = action_for_authored_pr(repo_health.merge_ready_prs[0], datetime.now(timezone.utc))
        if merge_item:
            next_items = [merge_item, *next_items][:2]

    lines = [f"Focus: {focus_label}"]
    lines.append("Wins:")
    lines.append(f"- {wins[0] if wins else 'No standout wins surfaced from the current snapshot.'}")
    lines.append("Blockers:")
    if blockers:
        for item in blockers:
            lines.append(f"- {item.repository} {item.key}: {item.reason}")
    else:
        lines.append("- No major blockers surfaced.")
    lines.append("Review asks:")
    if asks:
        for note in asks:
            lines.append(f"- {note.repository}: {note.reason_label} on {note.title}")
    else:
        lines.append("- No urgent review asks right now.")
    lines.append("Next:")
    for item in next_items:
        lines.append(f"- {item.repository}: {item.next_step}")
    if daily_plan:
        lines.append("Plan:")
        for item in daily_plan[:3]:
            lines.append(f"- [{item.urgency}] {item.summary}")
    lines.append("Momentum:")
    lines.append(f"- {momentum_timeline.narrative}")
    if command_suggestions:
        lines.append("Commands:")
        for item in command_suggestions[:3]:
            lines.append(f"- `{item.command}`")
    return "\n".join(lines)


def apply_filters(
    repos: list[Repo],
    review_prs: list[PullRequest],
    authored_prs: list[PullRequest],
    assigned_issues: list[Issue],
    notifications: list[NotificationItem],
    args: argparse.Namespace,
    now: datetime,
) -> tuple[list[Repo], list[PullRequest], list[PullRequest], list[Issue], list[NotificationItem]]:
    if args.repo:
        repos = [repo for repo in repos if repo.name == args.repo]
        review_prs = [pr for pr in review_prs if pr.repository == args.repo]
        authored_prs = [pr for pr in authored_prs if pr.repository == args.repo]
        assigned_issues = [issue for issue in assigned_issues if issue.repository == args.repo]
        notifications = [item for item in notifications if item.repository == args.repo]

    if args.org:
        prefix = f"{args.org}/"
        repos = [repo for repo in repos if repo.name.startswith(prefix)]
        review_prs = [pr for pr in review_prs if pr.repository.startswith(prefix)]
        authored_prs = [pr for pr in authored_prs if pr.repository.startswith(prefix)]
        assigned_issues = [issue for issue in assigned_issues if issue.repository.startswith(prefix)]
        notifications = [item for item in notifications if item.repository.startswith(prefix)]

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
        notifications = sort_by_staleness(notifications, now)
        repos = sorted(repos, key=lambda repo: age_key(repo.pushed_at, now), reverse=True)

    if args.inbox:
        notifications = sorted(
            notifications,
            key=lambda item: (-item.score, 0 if item.unread else 1, -(item.updated_at.timestamp() if item.updated_at else 0)),
        )

    return repos, review_prs, authored_prs, assigned_issues, notifications


def build_attention_items(
    review_prs: list[PullRequest],
    authored_prs: list[PullRequest],
    assigned_issues: list[Issue],
    notifications: list[NotificationItem],
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

    for notification in notifications[: max(limit + 2, 6)]:
        item = action_for_notification(notification, now)
        items[f"notification:{notification.id}"] = item

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


def action_for_notification(notification: NotificationItem, now: datetime) -> ActionItem:
    bucket = "DO NOW" if notification.reason in {"review_requested", "mention", "assign"} else "WAITING"
    if is_stale(notification.updated_at, now) and bucket != "DO NOW":
        bucket = "AT RISK"
    badges = unique_preserving_order([bucket, notification.reason_label.upper()[:12], "UNREAD" if notification.unread else ""])
    return ActionItem(
        key=notification.key,
        kind="notification",
        repository=notification.repository,
        title=notification.title,
        url=notification.url,
        number=0,
        created_at=notification.updated_at,
        updated_at=notification.updated_at,
        badges=badges[:3],
        score=notification.score,
        check_state="",
        reason=f"{notification.reason_label.lower()} {relative_time_long(notification.updated_at, now)} ago",
        next_step=notification.next_step,
        age_bucket=age_bucket(notification.updated_at, now),
        bucket=bucket,
    )


def action_for_review_pr(pr: PullRequest, now: datetime) -> ActionItem:
    badges = ["REVIEW"]
    bucket = "DO NOW"
    reason = f"review requested {relative_time_long(pr.updated_at, now)} ago"
    next_step = pr_web_command(pr)
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
    next_step = pr_web_command(pr)
    score = 30 + recent_boost(pr.updated_at, now)

    if pr.check_state in {"FAILURE", "ERROR"}:
        badges.append("FAILING")
        reason = "checks failing"
        next_step = pr_checkout_command(pr)
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
        next_step = pr_checkout_command(pr)
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
        next_step=issue_comments_command(issue),
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


def build_daily_brief(
    summary: dict[str, int],
    repos: list[Repo],
    attention_items: list[ActionItem],
    recent_wins: RecentWins | None,
    focus_label: str,
) -> str:
    if attention_items:
        standout = f"Top action: {attention_items[0].repository} ({attention_items[0].reason})."
    elif repos:
        standout = f"Biggest movement: {repos[0].name}."
    else:
        standout = "No matching GitHub activity surfaced."
    wins_sentence = recent_wins.narrative if recent_wins else "No recent wins summary available."
    return (
        f"Focus: {focus_label}. "
        f"{summary['active_repos']} active repos, "
        f"{summary['reviews_waiting']} reviews waiting, "
        f"{summary['failing_prs']} failing PRs, "
        f"{summary['assigned_issues']} assigned issues, "
        f"{summary['attention_now']} do-now items, "
        f"streak {summary['current_streak']} days. "
        f"{wins_sentence} "
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
        "reviews_waiting": data.summary["reviews_waiting"],
        "assigned_issues": data.summary["assigned_issues"],
        "failing_prs": data.summary["failing_prs"],
        "active_repos": data.summary["active_repos"],
        "review_pr_ids": [pr.key for pr in data.review_prs],
        "issue_ids": [issue.key for issue in data.assigned_issues],
        "repo_pushes": {repo.name: iso_datetime(repo.pushed_at) for repo in data.repos if repo.pushed_at is not None},
        "pr_check_states": {pr.key: pr.check_state for pr in data.authored_prs},
        "ready_pr_ids": [pr.key for pr in data.ready_prs],
        "current_streak": data.current_streak,
        "merged_authored_prs": data.recent_wins.merged_pr_count,
        "reviews_completed": data.digest.reviews_completed.value if data.digest else 0,
        "issues_closed": data.recent_wins.closed_issue_count,
        "active_repos_touched": data.summary["active_repos"],
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


def append_history(snapshot: dict[str, Any]) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with HISTORY_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(snapshot, sort_keys=True) + "\n")
    except OSError as exc:
        raise GhError(f"Unable to write history file {HISTORY_PATH}: {exc}.") from exc


def read_history() -> list[dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    history: list[dict[str, Any]] = []
    try:
        with HISTORY_PATH.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                with contextlib.suppress(json.JSONDecodeError):
                    history.append(json.loads(line))
    except OSError:
        return []
    return history[-HISTORY_LIMIT:]


def history_streak_at_or_before(history: list[dict[str, Any]], target: datetime) -> int | None:
    for entry in reversed(history):
        generated_at = parse_dt(entry.get("generated_at"))
        if generated_at and generated_at <= target:
            return int(entry.get("current_streak") or 0)
    return None


def read_history_entries(now: datetime, max_days: int = 14) -> list[HistoryEntry]:
    raw_history = read_history()
    cutoff = now - timedelta(days=max_days)
    parsed: list[HistoryEntry] = []
    for row in raw_history:
        entry = parse_history_entry(row)
        if entry is None or entry.generated_at < cutoff:
            continue
        parsed.append(entry)
    parsed.sort(key=lambda item: item.generated_at)
    return parsed


def parse_history_entry(row: dict[str, Any]) -> HistoryEntry | None:
    generated_at = normalize_history_timestamp(row.get("generated_at"))
    if generated_at is None:
        return None
    return HistoryEntry(
        generated_at=generated_at,
        review_queue_count=safe_int(row.get("reviews_waiting"), fallback=len(row.get("review_pr_ids") or [])),
        assigned_issue_count=safe_int(row.get("assigned_issues"), fallback=len(row.get("issue_ids") or [])),
        failing_pr_count=safe_int(row.get("failing_prs"), fallback=count_failing_states(row.get("pr_check_states"))),
        active_repo_count=safe_int(row.get("active_repos"), fallback=safe_int(row.get("active_repos_touched"))),
        review_pr_ids={item for item in (row.get("review_pr_ids") or []) if isinstance(item, str) and item},
        issue_ids={item for item in (row.get("issue_ids") or []) if isinstance(item, str) and item},
        ready_pr_ids={item for item in (row.get("ready_pr_ids") or []) if isinstance(item, str) and item},
        repo_pushes={
            name: stamp
            for name, stamp in (row.get("repo_pushes") or {}).items()
            if isinstance(name, str) and isinstance(stamp, str)
        },
        pr_check_states={
            name: normalize_check_state(state)
            for name, state in (row.get("pr_check_states") or {}).items()
            if isinstance(name, str)
        },
    )


def make_history_entry_from_snapshot(snapshot: dict[str, Any]) -> HistoryEntry | None:
    return parse_history_entry(snapshot)


def normalize_history_timestamp(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        with contextlib.suppress(OverflowError, OSError, ValueError):
            return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        return parse_dt(value)
    return None


def safe_int(value: Any, fallback: int = 0) -> int:
    if isinstance(value, bool):
        return fallback
    with contextlib.suppress(TypeError, ValueError):
        return int(value)
    return fallback


def count_failing_states(pr_check_states: Any) -> int:
    if not isinstance(pr_check_states, dict):
        return 0
    return sum(1 for state in pr_check_states.values() if normalize_check_state(state) in {"FAILURE", "ERROR"})


def normalize_check_state(state: Any) -> str:
    if not isinstance(state, str):
        return "UNKNOWN"
    normalized = state.strip().upper()
    return normalized or "UNKNOWN"


def build_momentum_timeline(history_entries: list[HistoryEntry]) -> MomentumTimeline:
    if len(history_entries) < 3:
        return MomentumTimeline(
            metrics=[],
            narrative="Momentum needs a few more saved snapshots before a trendline is honest.",
            sample_count=len(history_entries),
            span_days=0,
            fallback="Need at least 3 valid snapshots from the last 7-14 days.",
        )

    metrics = [
        timeline_metric("Review queue", [entry.review_queue_count for entry in history_entries]),
        timeline_metric("Assigned issues", [entry.assigned_issue_count for entry in history_entries]),
        timeline_metric("Failing PRs", [entry.failing_pr_count for entry in history_entries]),
        timeline_metric("Active repos", [entry.active_repo_count for entry in history_entries]),
    ]
    burden_delta = sum(metric.delta for metric in metrics[:3]) - metrics[3].delta
    if burden_delta <= -2:
        narrative = "Workload is improving: queues are easing relative to repo activity."
    elif burden_delta >= 2:
        narrative = "Workload is worsening: incoming work is outpacing the current burn-down."
    else:
        narrative = "Workload is mostly stable across the recent snapshot window."
    span_days = max((history_entries[-1].generated_at - history_entries[0].generated_at).days, 0)
    return MomentumTimeline(
        metrics=metrics,
        narrative=narrative,
        sample_count=len(history_entries),
        span_days=span_days,
        fallback=None,
    )


def timeline_metric(label: str, values: list[int]) -> TimelineMetric:
    current = values[-1] if values else 0
    midpoint = max(len(values) // 2, 1)
    previous_slice = values[:midpoint]
    previous = round(sum(previous_slice) / len(previous_slice)) if previous_slice else current
    return TimelineMetric(
        label=label,
        values=values,
        current=current,
        previous=previous,
        delta=current - previous,
    )


def build_change_feed(
    previous_snapshot: dict[str, Any] | None,
    repos: list[Repo],
    review_prs: list[PullRequest],
    authored_prs: list[PullRequest],
    assigned_issues: list[Issue],
    now: datetime,
    limit: int,
) -> list[ChangeEvent]:
    if previous_snapshot is None:
        return [ChangeEvent("FIRST RUN", "First watch refresh: collecting a baseline snapshot.", "~", 999, None)]

    previous = parse_history_entry(previous_snapshot)
    if previous is None:
        return [ChangeEvent("FIRST RUN", "Baseline snapshot is not readable yet; next refresh will show deltas.", "~", 999, None)]

    events: list[ChangeEvent] = []
    review_lookup = {pr.key: pr for pr in review_prs}
    issue_lookup = {issue.key: issue for issue in assigned_issues}
    repo_pushes = {repo.name: repo.pushed_at for repo in repos if repo.pushed_at}
    authored_lookup = {pr.key: pr for pr in authored_prs}

    for key in sorted(set(review_lookup) - previous.review_pr_ids):
        pr = review_lookup[key]
        events.append(ChangeEvent("REVIEW", f"New review request on {key}", "+", 10, pr_web_command(pr)))
    for key in sorted(set(issue_lookup) - previous.issue_ids):
        issue = issue_lookup[key]
        events.append(ChangeEvent("ISSUE", f"New assigned issue {key}", "+", 20, issue_comments_command(issue)))
    for key, pr in authored_lookup.items():
        old_state = previous.pr_check_states.get(key)
        new_state = normalize_check_state(pr.check_state)
        if old_state in {"SUCCESS", "PENDING", "EXPECTED", "UNKNOWN"} and new_state in {"FAILURE", "ERROR"}:
            events.append(ChangeEvent("CHECKS", f"{key} checks flipped to failing", "-", 15, pr_checks_command(pr)))
        elif old_state in {"FAILURE", "ERROR"} and new_state == "SUCCESS":
            events.append(ChangeEvent("CHECKS", f"{key} checks recovered", "+", 35, pr_web_command(pr)))
    for repo_name, pushed_at in repo_pushes.items():
        previous_stamp = previous.repo_pushes.get(repo_name)
        if pushed_at and previous_stamp != iso_datetime(pushed_at):
            events.append(ChangeEvent("PUSH", f"Fresh push detected in {repo_name}", "+", 40, repo_view_command(repo_name)))
    resolved_reviews = sorted(previous.review_pr_ids - set(review_lookup))
    for key in resolved_reviews[:2]:
        events.append(ChangeEvent("RESOLVED", f"Review request cleared for {key}", "+", 55, pr_key_view_command(key)))
    resolved_issues = sorted(previous.issue_ids - set(issue_lookup))
    for key in resolved_issues[:2]:
        events.append(ChangeEvent("RESOLVED", f"Assigned issue cleared for {key}", "+", 60, issue_key_view_command(key)))
    if not events:
        events.append(ChangeEvent("STABLE", f"No material changes since {format_timestamp(previous.generated_at or now)}.", "~", 999, None))
    events.sort(key=lambda event: (event.priority, event.summary))
    return events[:limit]


def build_command_suggestions(
    args: argparse.Namespace,
    attention_items: list[ActionItem],
    review_prs: list[PullRequest],
    authored_prs: list[PullRequest],
    failing_prs: list[PullRequest],
    assigned_issues: list[Issue],
    inbox_items: list[NotificationItem],
    repo_health: RepoHealth | None,
    repos: list[Repo],
) -> tuple[list[CommandSuggestion], list[CommandSuggestion]]:
    suggestions: list[CommandSuggestion] = []
    seen: set[str] = set()

    def add(label: str, command: str, reason: str, priority: int) -> None:
        if not command or command in seen:
            return
        suggestions.append(CommandSuggestion(label=label, command=command, reason=reason, priority=priority))
        seen.add(command)

    for pr in review_prs[:2]:
        add(f"Review {pr.key}", pr_web_command(pr), "review-requested PR", 10)
        add(f"Checkout {pr.key}", pr_checkout_command(pr), "pull branch locally", 20)
    for pr in failing_prs[:2]:
        add(f"Fix {pr.key}", pr_checkout_command(pr), "authored PR has failing checks", 12)
        add(f"Check CI {pr.key}", pr_checks_command(pr), "inspect failing checks", 13)
    for pr in authored_prs:
        if is_ready_pr(pr):
            add(f"Merge-ready {pr.key}", pr_web_command(pr), "approved and mergeable", 25)
            break
    for issue in assigned_issues[:2]:
        add(f"Issue {issue.key}", issue_comments_command(issue), "assigned issue needs context", 30)
    for note in inbox_items[:2]:
        add(f"Inbox {note.repository}", note.next_step, f"{note.reason_label.lower()} notification", 35)
    if repo_health:
        add(f"Repo {repo_health.repository}", repo_view_command(repo_health.repository), "open repo drilldown in browser", 45)
        if repo_health.next_steps:
            add("Repo next step", repo_health.next_steps[0], "follow repo health recommendation", 46)
    for repo in repos[:1]:
        add(f"Inspect {repo.name}", repo_view_command(repo.name), "active repo drilldown", 50)

    suggestions.sort(key=lambda item: (item.priority, item.label))
    compact_limit = max(4, min(8, args.limit if args.commands else 5))
    compact = suggestions[:compact_limit]
    catalog = suggestions[: max(compact_limit, min(len(suggestions), max(args.limit + 6, 10)))]
    return compact, catalog


def build_daily_plan(
    attention_items: list[ActionItem],
    inbox_items: list[NotificationItem],
    repo_health_matrix: list[RepoHealth],
    failing_prs: list[PullRequest],
    assigned_issues: list[Issue],
    limit: int,
) -> list[DailyPlanItem]:
    plan: list[DailyPlanItem] = []

    for pr in failing_prs[:2]:
        plan.append(
            DailyPlanItem(
                summary=f"Fix failing checks on {pr.key}",
                urgency="DO NOW",
                reason="CI is failing on authored work",
                command=pr_checks_command(pr),
                priority=10,
            )
        )
    for item in attention_items[:4]:
        summary = item.title and f"{action_summary_verb(item)} {item.key}"
        plan.append(
            DailyPlanItem(
                summary=summary or f"Triage {item.key}",
                urgency=item.bucket,
                reason=item.reason,
                command=item.next_step,
                priority=15 + bucket_rank(item.bucket) * 10,
            )
        )
    for note in inbox_items[:2]:
        plan.append(
            DailyPlanItem(
                summary=f"Reply on {note.repository} {notification_subject_label(note)}",
                urgency="DO NOW" if note.unread else "WAITING",
                reason=f"{note.reason_label} in inbox",
                command=note.next_step,
                priority=22 if note.unread else 38,
            )
        )
    for issue in assigned_issues[:2]:
        plan.append(
            DailyPlanItem(
                summary=f"Review assigned issue {issue.key}",
                urgency="AT RISK" if issue.updated_at else "WAITING",
                reason="assigned issue queue needs a decision",
                command=issue_comments_command(issue),
                priority=32,
            )
        )
    for health in repo_health_matrix[:2]:
        if health.failing_runs:
            plan.append(
                DailyPlanItem(
                    summary=f"Triage repo health in {health.repository}",
                    urgency="AT RISK",
                    reason="recent CI failures are accumulating",
                    command=f"gh run list --repo {health.repository} --limit {REPO_WORKFLOW_LIMIT}",
                    priority=28,
                )
            )

    deduped: list[DailyPlanItem] = []
    seen: set[str] = set()
    for item in sorted(plan, key=lambda entry: (entry.priority, entry.summary)):
        if item.summary in seen:
            continue
        seen.add(item.summary)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


def action_summary_verb(item: ActionItem) -> str:
    if item.kind == "pr" and "REVIEW" in item.badges:
        return "Review"
    if item.kind == "issue":
        return "Reply on"
    if item.kind == "notification":
        return "Check"
    return "Triage"


def notification_subject_label(note: NotificationItem) -> str:
    if note.subject_type.lower() == "issue":
        return f"issue update: {note.title}"
    if note.subject_type.lower() == "pullrequest":
        return f"PR thread: {note.title}"
    return note.title


def pr_web_command(pr: PullRequest) -> str:
    return f"gh pr view {pr.number} -R {pr.repository} --web"


def pr_checkout_command(pr: PullRequest) -> str:
    return f"gh pr checkout {pr.number} -R {pr.repository}"


def pr_checks_command(pr: PullRequest) -> str:
    return f"gh pr checks {pr.number} -R {pr.repository}"


def issue_comments_command(issue: Issue) -> str:
    return f"gh issue view {issue.number} -R {issue.repository} --comments"


def repo_view_command(repository: str) -> str:
    return f"gh repo view {repository} --web"


def pr_key_view_command(key: str) -> str:
    repository, _, number = key.partition("#")
    if repository and number.isdigit():
        return f"gh pr view {number} -R {repository} --web"
    return ""


def issue_key_view_command(key: str) -> str:
    repository, _, number = key.partition("#")
    if repository and number.isdigit():
        return f"gh issue view {number} -R {repository} --comments"
    return ""


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
    if data.watch_mode:
        print_box("Change Feed", render_change_feed_lines(data.change_feed, width, style), width, style, heavy=True)
        print()
    print_box("Daily Plan", render_daily_plan_lines(data.daily_plan, width, style), width, style)
    print()
    print_box("Recent Wins", render_recent_wins_lines(data.recent_wins, data.generated_at, width, style), width, style)
    print()
    print_box(
        "Attention Radar",
        render_attention_lines(data.attention_items, args.limit, data.generated_at, width, style),
        width,
        style,
    )
    print()
    print_box(
        "Actionable Next Commands",
        render_command_lines(data.command_catalog if args.commands else data.command_suggestions, width, style),
        width,
        style,
    )
    print()
    print_box("Momentum Timeline", render_momentum_lines(data.momentum_timeline, width, style), width, style)
    print()
    print_box(
        "Changes Since Last Run",
        render_changes_lines(data.changes, style, highlight=data.watch_mode),
        width,
        style,
        heavy=data.watch_mode,
    )
    print()
    if data.digest:
        print_box("Digest", render_digest_lines(data.digest, style), width, style)
        print()
    print_box("Contributions", render_contribution_lines(data, width, style), width, style)
    print()
    print_box("Repo Health Matrix", render_repo_health_matrix_lines(data.repo_health_matrix, data.generated_at, width, style), width, style)
    print()
    print_box("Repos By Recent Activity", render_repo_lines(data.repos, args.limit, data.generated_at, width, style), width, style)
    print()
    print_box("Open PRs Waiting On You", render_review_lines(data.review_prs, args.limit, data.generated_at, width, style), width, style)
    print()
    print_box("Failing Or Ready PRs", render_authored_pr_lines(data, args.limit, data.generated_at, width, style), width, style)
    print()
    print_box("Issues Assigned To You", render_issue_lines(data.assigned_issues, args.limit, data.generated_at, width, style), width, style)
    if data.repo_health:
        print()
        print_box(f"Repo Health Drilldown: {data.repo_health.repository}", render_repo_health_detail_lines(data.repo_health, data.generated_at, width, style), width, style)


def render_title(data: DashboardData, width: int, style: Style) -> str:
    left = " GitPulse "
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


def render_change_feed_lines(events: list[ChangeEvent], width: int, style: Style) -> list[str]:
    lines: list[str] = []
    for event in events:
        head = style.delta(f"{style.badge(event.badge)} {event.summary}", event.sign)
        lines.append(truncate_ansi(head, width - 4))
        if event.command:
            lines.append(truncate_ansi(f"  {event.command}", width - 4))
    return lines or [style.dim("No watch-mode changes to display.")]


def render_daily_plan_lines(plan: list[DailyPlanItem], width: int, style: Style) -> list[str]:
    if not plan:
        return [style.dim("No concrete plan items were ranked from the current snapshot.")]
    lines: list[str] = []
    for index, item in enumerate(plan, start=1):
        lines.append(truncate_ansi(f"{index}. {style.badge(item.urgency)} {item.summary}", width - 4))
        detail = f"   {item.reason}"
        if item.command:
            detail += f" | {item.command}"
        lines.append(truncate_ansi(detail, width - 4))
    return lines


def render_command_lines(commands: list[CommandSuggestion], width: int, style: Style) -> list[str]:
    if not commands:
        return [style.dim("No command recommendations surfaced from the current focus.")]
    lines: list[str] = []
    for item in commands:
        lines.append(truncate_ansi(f"{style.badge('DO NOW' if item.priority < 20 else 'WAITING')} {item.label}", width - 4))
        lines.append(truncate_ansi(f"  {item.command}", width - 4))
        lines.append(truncate_ansi(f"  {item.reason}", width - 4))
    return lines


def render_momentum_lines(momentum: MomentumTimeline, width: int, style: Style) -> list[str]:
    if momentum.fallback:
        return [style.dim(momentum.fallback), style.dim(momentum.narrative)]

    label_width = min(16, max(12, (width - 28) // 3))
    lines = [momentum.narrative]
    for metric in momentum.metrics:
        spark = render_sparkline(metric.values, style)
        sign = "+" if metric.delta > 0 else ("-" if metric.delta < 0 else "~")
        delta = style.delta(f"{metric.delta:+d}", sign) if metric.delta else style.dim("0")
        lines.append(
            truncate_ansi(
                f"{pad_plain(metric.label, label_width)} {spark}  now {metric.current}  prev {metric.previous}  delta {delta}",
                width - 4,
            )
        )
    lines.append(style.dim(f"{momentum.sample_count} snapshots across {momentum.span_days} days"))
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


def render_recent_wins_lines(wins: RecentWins, now: datetime, width: int, style: Style) -> list[str]:
    lines = wrap_lines(wins.narrative, width - 4)
    if wins.total_count == 0:
        return lines
    lines.append(
        f"Merged PRs {style.good(str(wins.merged_pr_count))}  "
        f"Closed issues {style.cool(str(wins.closed_issue_count))}"
    )
    for item in wins.top_items[:RECENT_WINS_LIMIT]:
        lines.append(
            truncate_ansi(
                f"{style.badge('READY' if item.kind == 'Merged PR' else 'ASSIGNED')} "
                f"{item.key} {truncate_plain(item.title, max(width - 26, 18))} "
                f"{style.dim(relative_time(item.closed_at, now))}",
                width - 4,
            )
        )
    return lines


def render_digest_lines(digest: DigestMetrics, style: Style) -> list[str]:
    return [
        digest.narrative,
        (
            f"Merged {format_delta(digest.merged_authored_prs, style)}  "
            f"Reviews {format_delta(digest.reviews_completed, style)}  "
            f"Issues {format_delta(digest.issues_closed, style)}  "
            f"Repos {format_delta(digest.active_repos_touched, style)}  "
            f"Streak {format_delta(digest.streak_change, style, suffix='d')}"
        ),
        f"{digest.period_label} {style.dim(digest.comparison_label)}",
    ]


def format_delta(delta: DigestDelta, style: Style, suffix: str = "") -> str:
    sign = "+" if delta.delta > 0 else ("-" if delta.delta < 0 else "~")
    base = f"{delta.value}{suffix}"
    if delta.delta == 0:
        return style.delta(base, sign)
    return style.delta(f"{base} ({delta.delta:+d})", sign)


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


def render_repo_health_matrix_lines(health_rows: list[RepoHealth], now: datetime, width: int, style: Style) -> list[str]:
    if not health_rows:
        return [style.dim("No repositories were available for a health scan in the current focus.")]

    inner = max(width - 4, 20)
    repo_width = min(24, max(14, inner // 5))
    ci_width = 16
    age_width = 9
    release_width = 18
    note_width = max(inner - repo_width - ci_width - age_width - age_width - release_width - 10, 16)
    lines = [
        f"{style.dim(pad_plain('Repo', repo_width))}  "
        f"{style.dim(pad_plain('CI', ci_width))}  "
        f"{style.dim(pad_plain('Oldest PR', age_width))}  "
        f"{style.dim(pad_plain('Oldest Issue', age_width))}  "
        f"{style.dim(pad_plain('Release', release_width))}  "
        f"{style.dim('Next')}"
    ]
    for health in health_rows:
        lines.append(
            f"{pad_plain(truncate_plain(health.repository, repo_width), repo_width)}  "
            f"{pad_plain(truncate_plain(repo_health_workflow_summary(health), ci_width), ci_width)}  "
            f"{pad_plain(repo_health_item_age(health.oldest_open_pr, now), age_width)}  "
            f"{pad_plain(repo_health_item_age(health.oldest_open_issue, now), age_width)}  "
            f"{pad_plain(truncate_plain(repo_health_release_label(health, now), release_width), release_width)}  "
            f"{truncate_plain(repo_health_warning(health, now), note_width)}"
        )
    return lines


def render_repo_health_detail_lines(health: RepoHealth, now: datetime, width: int, style: Style) -> list[str]:
    lines = [
        f"Workflow runs: {repo_health_workflow_summary(health)}",
        f"Oldest open PR: {repo_health_detail_label(health.oldest_open_pr, now, empty='No open PRs.')}",
        f"Oldest open issue: {repo_health_detail_label(health.oldest_open_issue, now, empty='No open issues.')}",
        f"Latest release: {repo_health_release_label(health, now)}",
        f"Suggested next step: {repo_health_warning(health, now)}",
    ]
    if health.next_steps:
        lines.append(f"Command: {health.next_steps[0]}")
    for note in health.notes:
        lines.extend(wrap_lines(f"Note: {note}", width - 4))
    return lines


def repo_health_workflow_summary(health: RepoHealth) -> str:
    if not health.workflow_runs:
        return "n/a"
    pending = sum(1 for run in health.workflow_runs if (run.get("status") or "").lower() != "completed")
    parts = []
    if health.failing_runs:
        parts.append(f"{health.failing_runs} fail")
    if health.successful_runs:
        parts.append(f"{health.successful_runs} ok")
    if pending:
        parts.append(f"{pending} running")
    return ", ".join(parts) if parts else "recent runs"


def repo_health_item_age(item: PullRequest | Issue | None, now: datetime) -> str:
    if item is None:
        return "none"
    return relative_time(item.created_at or item.updated_at, now)


def repo_health_release_label(health: RepoHealth, now: datetime) -> str:
    if not health.latest_release:
        return "none"
    tag = health.latest_release.get("tag_name") or health.latest_release.get("name") or "latest"
    published_at = health.latest_release.get("published_at")
    return f"{tag} {relative_time(published_at, now)}"


def repo_health_warning(health: RepoHealth, now: datetime) -> str:
    if health.failing_runs:
        return "CI failures need triage"
    if health.merge_ready_prs:
        return f"Merge-ready PR waiting: {health.merge_ready_prs[0].key}"
    if health.oldest_open_pr and is_older_than(health.oldest_open_pr.created_at, now, hours=24 * 7):
        return "Old PR is aging"
    if health.oldest_open_issue and is_older_than(health.oldest_open_issue.created_at, now, hours=24 * 14):
        return "Old issue needs a decision"
    if health.notes:
        return truncate_plain(health.notes[0], 38)
    return "Healthy"


def repo_health_detail_label(item: PullRequest | Issue | None, now: datetime, empty: str) -> str:
    if item is None:
        return empty
    return f"{item.key} {truncate_plain(item.title, 48)} ({repo_health_item_age(item, now)} old)"


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
    if args.export_update:
        path = Path(args.export_update)
        path.write_text(data.share_update + "\n", encoding="utf-8")
        print(f"Wrote text update to {path}")


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
        f"- Recent wins: {data.summary['recent_win_total']}",
        f"- Merged PRs this week: {data.summary['recent_merged_prs']}",
        f"- Closed issues this week: {data.summary['recent_closed_issues']}",
        f"- Do now: {data.summary['attention_now']}",
        f"- At risk: {data.summary['attention_risk']}",
        f"- Streak: {data.current_streak} days",
        "",
        "## Daily Brief",
        "",
        data.daily_brief,
        "",
        "## Daily Plan",
        "",
    ]
    for item in data.daily_plan:
        lines.append(f"- [{item.urgency}] {item.summary}")
        lines.append(f"  Reason: {item.reason}")
        if item.command:
            lines.append(f"  Command: `{item.command}`")

    lines.extend(
        [
            "",
            "## Momentum Timeline",
            "",
            f"- {data.momentum_timeline.narrative}",
        ]
    )
    if data.momentum_timeline.fallback:
        lines.append(f"- {data.momentum_timeline.fallback}")
    else:
        for metric in data.momentum_timeline.metrics:
            lines.append(
                f"- {metric.label}: {render_sparkline(metric.values, Style())} now {metric.current}, prev {metric.previous}, delta {metric.delta:+d}"
            )

    lines.extend(
        [
            "",
            "## Actionable Next Commands",
            "",
        ]
    )
    for item in data.command_catalog[: max(5, min(len(data.command_catalog), args.limit + 4))]:
        lines.append(f"- {item.label}: `{item.command}`")
        lines.append(f"  Why: {item.reason}")

    lines.extend(
        [
            "",
        "## Recent Wins",
        "",
        data.recent_wins.narrative,
        "",
        ]
    )
    for item in data.recent_wins.top_items[:RECENT_WINS_LIMIT]:
        lines.append(f"- `{item.kind}` `{item.key}` {item.title} ({relative_time(item.closed_at, data.generated_at)})")

    lines.extend(
        [
            "",
            "## Attention Radar",
            "",
        ]
    )

    for item in data.attention_items[: min(len(data.attention_items), max(5, min(args.limit + 2, ATTENTION_LIMIT)))]:
        lines.extend(
            [
                f"- [{item.bucket}] `{item.key}` {item.title}",
                f"  Reason: {item.reason}",
                f"  Next: `{item.next_step}`",
            ]
        )

    lines.extend(section_repo_health_markdown("## Repo Health Matrix", data.repo_health_matrix, data.generated_at))
    if data.digest:
        lines.extend(
            [
                "",
                "## Digest",
                "",
                f"- {data.digest.narrative}",
                f"- Period: {data.digest.period_label} ({data.digest.comparison_label})",
            ]
        )
    if data.watch_mode:
        lines.extend(["", "## Change Feed", ""])
        for event in data.change_feed:
            lines.append(f"- [{event.badge}] {event.summary}")
            if event.command:
                lines.append(f"  Command: `{event.command}`")
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
    if data.repo_health:
        lines.extend(section_repo_health_detail_markdown(data.repo_health, data.generated_at))
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


def section_repo_health_markdown(title: str, health_rows: list[RepoHealth], now: datetime) -> list[str]:
    lines = ["", title, ""]
    if not health_rows:
        lines.append("- None")
        return lines
    lines.append("| Repo | CI | Oldest PR | Oldest Issue | Release | Next |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for health in health_rows:
        lines.append(
            f"| {health.repository} | {repo_health_workflow_summary(health)} | {repo_health_item_age(health.oldest_open_pr, now)} | "
            f"{repo_health_item_age(health.oldest_open_issue, now)} | {repo_health_release_label(health, now)} | {repo_health_warning(health, now)} |"
        )
    return lines


def section_repo_health_detail_markdown(health: RepoHealth, now: datetime) -> list[str]:
    lines = [
        "",
        f"## Repo Health Drilldown: {health.repository}",
        "",
        f"- Workflow runs: {repo_health_workflow_summary(health)}",
        f"- Oldest open PR: {repo_health_detail_label(health.oldest_open_pr, now, empty='No open PRs.')}",
        f"- Oldest open issue: {repo_health_detail_label(health.oldest_open_issue, now, empty='No open issues.')}",
        f"- Latest release: {repo_health_release_label(health, now)}",
        f"- Next step: {repo_health_warning(health, now)}",
    ]
    if health.next_steps:
        lines.append(f"- Command: `{health.next_steps[0]}`")
    for note in health.notes:
        lines.append(f"- Note: {note}")
    return lines


def render_html_export(data: DashboardData, args: argparse.Namespace) -> str:
    wins_items = "".join(
        f"<li><strong>{h(item.kind)}</strong> {h(item.key)} {h(item.title)} <span class='muted'>{h(relative_time(item.closed_at, data.generated_at))}</span></li>"
        for item in data.recent_wins.top_items[:RECENT_WINS_LIMIT]
    )
    attention_cards = "".join(render_attention_card_html(item) for item in data.attention_items[: min(len(data.attention_items), max(5, min(args.limit + 2, ATTENTION_LIMIT)))])
    plan_items = "".join(
        f"<li>{badge_html(item.urgency)} <strong>{h(item.summary)}</strong><br><span class='muted'>{h(item.reason)}</span>{render_cmd_html(item.command)}</li>"
        for item in data.daily_plan
    )
    command_items = "".join(
        f"<li><strong>{h(item.label)}</strong><br>{render_cmd_html(item.command)}<br><span class='muted'>{h(item.reason)}</span></li>"
        for item in (data.command_catalog if args.commands else data.command_suggestions)
    )
    momentum_items = render_momentum_html(data.momentum_timeline)
    watch_changes = "".join(
        f"<li>{badge_html(event.badge)} {h(event.summary)}{render_cmd_html(event.command)}</li>"
        for event in data.change_feed
    )
    repo_rows = "".join(
        f"<tr><td>{h(repo.name)}</td><td>{h(relative_time(repo.pushed_at, data.generated_at))}</td><td>{h(repo.language)}</td><td>{repo.open_prs}/{repo.open_issues}</td><td>{h(repo.description or '')}</td></tr>"
        for repo in data.repos[: args.limit]
    )
    repo_health_rows = "".join(
        f"<tr><td>{h(health.repository)}</td><td>{h(repo_health_workflow_summary(health))}</td><td>{h(repo_health_item_age(health.oldest_open_pr, data.generated_at))}</td><td>{h(repo_health_item_age(health.oldest_open_issue, data.generated_at))}</td><td>{h(repo_health_release_label(health, data.generated_at))}</td><td>{h(repo_health_warning(health, data.generated_at))}</td></tr>"
        for health in data.repo_health_matrix
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
.muted {{ color: #5d7084; }}
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
        <h2>Daily Plan</h2>
        <ul>{plan_items or "<li>No plan items.</li>"}</ul>
      </section>
      <section class="card">
        <h2>Momentum Timeline</h2>
        {momentum_items}
      </section>
      <section class="card">
        <h2>Recent Wins</h2>
        <p>{h(data.recent_wins.narrative)}</p>
        <div class="metrics">
          <div class="metric"><span>Merged PRs</span><strong>{data.recent_wins.merged_pr_count}</strong></div>
          <div class="metric"><span>Closed issues</span><strong>{data.recent_wins.closed_issue_count}</strong></div>
        </div>
        <ul>{wins_items or "<li>No recent wins.</li>"}</ul>
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

    <section class="card">
      <h2>Actionable Next Commands</h2>
      <ul>{command_items or "<li>No command suggestions.</li>"}</ul>
    </section>

    {render_digest_html(data.digest) if data.digest else ""}

    <div class="grid">
      {f"<section class='card'><h2>Change Feed</h2><ul>{watch_changes}</ul></section>" if data.watch_mode else ""}
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
      <h2>Repo Health Matrix</h2>
      <table><thead><tr><th>Repo</th><th>CI</th><th>Oldest PR</th><th>Oldest issue</th><th>Release</th><th>Next</th></tr></thead><tbody>{repo_health_rows or "<tr><td colspan='6'>None</td></tr>"}</tbody></table>
    </section>
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
    {render_repo_health_detail_html(data.repo_health, data.generated_at) if data.repo_health else ""}
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


def render_digest_html(digest: DigestMetrics) -> str:
    return (
        "<section class='card'>"
        "<h2>Digest</h2>"
        f"<p>{h(digest.narrative)}</p>"
        f"<p class='muted'>{h(digest.period_label)} • {h(digest.comparison_label)}</p>"
        "</section>"
    )


def render_cmd_html(command: str | None) -> str:
    if not command:
        return ""
    return f"<div><span class='cmd'>{h(command)}</span></div>"


def render_momentum_html(momentum: MomentumTimeline) -> str:
    if momentum.fallback:
        return f"<p>{h(momentum.narrative)}</p><p class='muted'>{h(momentum.fallback)}</p>"
    items = "".join(
        f"<li><strong>{h(metric.label)}</strong> {h(render_sparkline(metric.values, Style()))} "
        f"<span class='muted'>now {metric.current}, prev {metric.previous}, delta {metric.delta:+d}</span></li>"
        for metric in momentum.metrics
    )
    return f"<p>{h(momentum.narrative)}</p><ul>{items}</ul><p class='muted'>{momentum.sample_count} snapshots across {momentum.span_days} days</p>"


def render_repo_health_detail_html(health: RepoHealth, now: datetime) -> str:
    notes = "".join(f"<li>{h(note)}</li>" for note in health.notes)
    commands = h(health.next_steps[0]) if health.next_steps else ""
    return (
        "<section class='card'>"
        f"<h2>Repo Health Drilldown: {h(health.repository)}</h2>"
        f"<p>Workflow runs: {h(repo_health_workflow_summary(health))}</p>"
        f"<p>Oldest open PR: {h(repo_health_detail_label(health.oldest_open_pr, now, empty='No open PRs.'))}</p>"
        f"<p>Oldest open issue: {h(repo_health_detail_label(health.oldest_open_issue, now, empty='No open issues.'))}</p>"
        f"<p>Latest release: {h(repo_health_release_label(health, now))}</p>"
        f"<p>Next step: {h(repo_health_warning(health, now))}</p>"
        f"{f'<p><span class=\"cmd\">{commands}</span></p>' if commands else ''}"
        f"{f'<ul>{notes}</ul>' if notes else ''}"
        "</section>"
    )


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
