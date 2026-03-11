# gh-pr-analytics

`gh-pr-analytics` is a GitHub command line extension for pull request process analytics over time.

<img width="1400" height="900" alt="openclaw-trends" src="https://github.com/user-attachments/assets/b775db37-eeb0-460b-b2da-1ce30b32ad58" />

## Requirements

- macOS 14 or newer
- GitHub command line tool (`gh`) installed and authenticated
- Network access to `api.github.com`

## Installation

Install the extension directly from GitHub:

```bash
gh extension install nicholascross/gh-pr-analytics
```

Verify the extension command is available:

```bash
gh pr-analytics --help
```

If the `gh-pr-analytics` launcher script runs outside a checked-out source tree, it bootstraps itself on first run by cloning `https://github.com/nicholascross/gh-pr-analytics.git` into `~/.gh-pr-analytics/source` before building. On later runs, it rebuilds automatically whenever `Package.swift`, `Package.resolved`, or files in `Sources/` are newer than the compiled binary.

Authentication resolution order:

1. `GH_TOKEN`
2. `GITHUB_TOKEN`
3. `gh auth token`

## What it measures

- Pull requests opened by week or month
- Pull requests merged by week or month
- Time to merge (`merged_at - created_at`)
- Time to first approval (`earliest APPROVED review submitted_at - created_at`)

## Quick start

Set up a workspace for one repository:

```bash
gh pr-analytics setup --repo owner/name
```

Run initial collection:

```bash
gh pr-analytics sync --repo owner/name --phase metadata --backfill
gh pr-analytics sync --repo owner/name --phase reviews --backfill --skip-closed-unmerged
```

Generate outputs:

```bash
gh pr-analytics report trends --repo owner/name --granularity month --format json
gh pr-analytics report charts --repo owner/name --granularity month --output-path trend-progression.png
gh pr-analytics report charts --repo owner/name --granularity month --chart-style insights --output-path pull-request-insights.png
gh pr-analytics export pull-requests --repo owner/name --format csv
```

Check state:

```bash
gh pr-analytics status --repo owner/name
```

## Command reference

```bash
gh pr-analytics setup --repo owner/name [--workspace-root path] [--database-path path]

gh pr-analytics init [--repo owner/name] [--database-path path]

gh pr-analytics sync \
  [--repo owner/name] \
  [--database-path path] \
  [--phase metadata|reviews|all] \
  [--backfill] \
  [--no-resume] \
  [--skip-closed-unmerged] \
  [--from-date YYYY-MM-DD] \
  [--to-date YYYY-MM-DD] \
  [--batch-size N] \
  [--max-pages N]

gh pr-analytics report trends \
  [--repo owner/name] \
  [--database-path path] \
  [--granularity week|month] \
  [--format csv|json] \
  [--from-date YYYY-MM-DD] \
  [--to-date YYYY-MM-DD]

gh pr-analytics report charts \
  [--repo owner/name] \
  [--database-path path] \
  [--granularity week|month] \
  [--chart-style trend|insights] \
  [--from-date YYYY-MM-DD] \
  [--to-date YYYY-MM-DD] \
  [--output-path path/to/chart.png] \
  [--width N] \
  [--height N]

gh pr-analytics export pull-requests [--repo owner/name] [--database-path path] [--format csv|json]

gh pr-analytics status [--repo owner/name] [--database-path path]
```

## Storage and workspace layout

Default workspace root:

- `.gh-pr-analytics/workspaces`

Default repository workspace:

- `.gh-pr-analytics/workspaces/<owner>__<repo>`

Default store path:

- `.gh-pr-analytics/workspaces/<owner>__<repo>/analytics.swiftdata`

`setup` creates:

- `.../repository` (cloned source repository)
- `.../analytics.swiftdata` (analytics store)

The analytics store uses SwiftData at the configured `.swiftdata` path.

## Synchronization behavior

### Metadata phase

- Fetches pull request lifecycle fields in pages
- Supports incremental mode and backfill mode
- Tracks cursor and watermark in `sync_state`

### Review phase

- Fetches review pages per pull request
- Computes earliest approval event
- Stores review records and first approval fields
- Marks review scan state as complete
- Uses checkpoint cursor for continuation by default

Performance-related options:

- Resume behavior is enabled by default
- `--no-resume` starts synchronization from the beginning and ignores checkpoints
- `--skip-closed-unmerged` skips review fetches for pull requests closed without merge
- Pagination stops early when a review page is not full

## Rate control and reliability

The tool applies request pacing and retries for GitHub REST calls.

Environment variables:

- `GH_PR_ANALYTICS_REQUEST_DELAY_MS` (default `600`)
- `GH_PR_ANALYTICS_REQUEST_TIMEOUT_MS` (default `30000`)
- `GH_PR_ANALYTICS_MAX_ATTEMPTS` (default `2`)

The network client is non-interactive and fails fast if authentication is missing or network access fails.

## Reports

### `report trends`

Outputs period rows with:

- `period_start`
- `period_granularity`
- `pull_requests_opened`
- `pull_requests_merged`
- `time_to_merge_p50_hours`
- `time_to_merge_p90_hours`
- `time_to_first_approval_p50_hours`
- `time_to_first_approval_p90_hours`
- `merge_sample_size`
- `approval_sample_size`

### `report charts`

Writes a PNG dashboard.

`--chart-style trend` (default) renders:

- Pull request volume by period (opened and merged)
- Median durations by period (merge and first approval)

`--chart-style insights` renders:

- Funnel by created cohort period (opened, first approved, merged, closed without merge)
- Cycle time distribution trend by period (p10, median, p90 merge hours)
- Approval lag versus merge lag scatter plot
- Open pull request age bucket snapshot

### `export pull-requests`

Outputs one row per pull request with lifecycle timestamps, first approval timestamp, and derived durations.

## Operational guidance

- Use `Ctrl+C` to stop synchronization safely.
- Resume continues from checkpoints by default.
- Use `--no-resume` to restart a synchronization phase from the beginning.
- Avoid force-killing unless necessary.

## Assumptions

- First approval means the earliest observed `APPROVED` review submission timestamp.
- If no approval exists, approval metrics are `null`.
- Reopened pull requests keep original creation time for duration calculations.
