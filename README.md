# GitPulse

GitPulse is a single-file Python terminal dashboard for GitHub activity. It uses the authenticated `gh` CLI as its only data source, renders with plain `print()` plus ANSI colors, and keeps a small cache at `~/.gitpulse/cache.json` for live deltas between runs.

## What You Get

- A colorful terminal dashboard with no dependencies and no curses
- Repos sorted by recent activity
- PRs waiting on your review
- Authored PRs that are failing or ready to merge
- Issues assigned to you
- Contribution streak and recent changes
- Attention Radar with reasons and suggested `gh` commands
- Watch mode with live diffs
- Shareable Markdown and HTML exports

## Requirements

- Python 3.10+
- GitHub CLI installed and authenticated: `gh auth status`

## Run

```bash
python3 gitpulse.py
```

## Common Examples

```bash
python3 gitpulse.py --limit 8
python3 gitpulse.py --refresh
python3 gitpulse.py --no-cache
python3 gitpulse.py --reviews
python3 gitpulse.py --failing --repo owner/repo
python3 gitpulse.py --stale --org my-org
python3 gitpulse.py --watch
python3 gitpulse.py --watch --interval 30
python3 gitpulse.py --watch --interval 10 --iterations 3
python3 gitpulse.py --export-md standup.md
python3 gitpulse.py --export-html standup.html
python3 gitpulse.py --reviews --limit 5 --export-md reviews.md --export-html reviews.html
```

## Flags

```text
--limit N            Rows to show per section
--width auto|full    Clamp for readability or use full terminal width
--refresh            Ignore the previous disk cache snapshot
--no-cache           Disable disk cache reads and writes
--watch              Refresh continuously
--interval SECONDS   Watch refresh interval, default 60
--iterations N       Stop watch mode after N refreshes
--export-md PATH     Write a Markdown standup export
--export-html PATH   Write a self-contained HTML export
--reviews            Focus on review-requested PRs
--failing            Focus on authored PRs with failing checks
--stale              Focus on items stale for 3+ days
--repo OWNER/NAME    Filter to one repository
--org ORGNAME        Filter to repositories owned by one org
```

## Watch Mode

`--watch` reruns the same dashboard fetch and redraw loop. In a TTY it clears and redraws the screen each cycle. In non-TTY output it prints a compact separator between iterations. GitPulse keeps an in-memory previous snapshot during watch mode even if `--no-cache` is set, so change detection still works within the session.

Examples:

```bash
python3 gitpulse.py --watch
python3 gitpulse.py --watch --interval 15
python3 gitpulse.py --watch --interval 5 --iterations 2 --reviews
```

## Export Mode

Exports use the same fetched dashboard data as the terminal render. They do not perform a second fetch.

Markdown export includes:

- Title and generated timestamp
- Viewer name and login
- Summary metrics and daily brief
- Attention Radar actions with suggested commands
- Active repos, review queue, failing or ready PRs, assigned issues
- Streak stats and recent changes

HTML export is a single static file with inline CSS and no JavaScript.

Examples:

```bash
python3 gitpulse.py --export-md standup.md
python3 gitpulse.py --export-html standup.html
python3 gitpulse.py --org my-org --limit 10 --export-md org.md --export-html org.html
```

## Focus Filters

Filters combine sensibly with `--limit`, `--watch`, and both export flags.

- `--reviews` prioritizes review-requested PRs and related action items
- `--failing` brings failing authored PRs to the front
- `--stale` surfaces items stale for 3 or more days
- `--repo OWNER/NAME` limits repos, PRs, issues, and radar items to one repo
- `--org ORGNAME` limits output to repositories under that org

Examples:

```bash
python3 gitpulse.py --reviews --limit 4
python3 gitpulse.py --failing --watch --interval 20
python3 gitpulse.py --stale --org my-org --export-md stale.md
python3 gitpulse.py --repo owner/repo --export-html repo.html
```

## Notes

- Default behavior remains: `python3 gitpulse.py` prints the dashboard once.
- Cache schema changes are handled by ignoring incompatible cache files.
- Ctrl+C exits watch mode cleanly with status code `130`.
