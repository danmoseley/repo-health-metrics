# Repo Health Metrics — Analysis

*Data collected March 15, 2026. Charts regenerated from ~750K issues and PRs across 6 repos.*

## Repos Analyzed

| Repo | Issues | PRs | Total | Date Range |
|------|--------|-----|-------|------------|
| dotnet/runtime | 70,484 | 51,152 | 121,636 | Sep 2014 – present* |
| dotnet/roslyn | 35,836 | 45,107 | 80,943 | Jan 2015 – present |
| dotnet/maui | 17,224 | 13,526 | 30,750 | May 2020 – present |
| microsoft/vscode | 233,877 | 52,242 | 286,119 | Oct 2015 – present |
| rust-lang/rust | 61,720 | 91,619 | 153,339 | Jun 2010 – present |
| golang/go | 70,580 | 4,921 | 75,501 | Oct 2009 – present |

\* *runtime has pre-2020 issues inherited from coreclr/corefx repos. PRs pre-2020 are missing — coreclr+corefx data would fill this gap. See "Known Artifacts" below.*

**Note on golang/go**: Go uses Gerrit for code review, not GitHub PRs. The 4,921 PRs are mostly bot-generated or mirror artifacts. Go is excluded from all PR-based charts but included in issue charts.

---

## Key Findings

### 1. Open Issue Backlogs

**Chart: `open_issues_comparison.png`**

All repos show growing issue backlogs, but at very different rates:

- **vscode** has the largest absolute backlog (~14K open issues) and it's climbing steeply. This is partly a scale effect — vscode gets enormous community issue volume.
- **go** and **rust** are both around ~9-11K and climbing steadily. Go's philosophy is to keep issues open by design; rust has been actively triaging but the pipeline is growing.
- **runtime** sits at ~8K after the 2020 consolidation. The trajectory is gently upward.
- **roslyn** is the most stable at ~5K, suggesting effective triage relative to inflow.
- **maui** is worth watching for rapid change given the recent AI-assisted PR acceleration.

### 2. Open PR Backlogs

**Chart: `open_prs_comparison.png`** *(Go excluded)*

- **vscode** shows an alarming hockey-stick growth to ~1.1K open PRs. This is the most concerning signal across all repos.
- **rust** has a steady climb to ~1K open PRs — consistent with its very high PR volume (91K total).
- **runtime** is actually one of the healthiest here at ~350-430 open PRs, despite having 51K total PRs.
- **roslyn** is climbing slowly to ~630.
- **maui** data will be interesting to compare quarter-over-quarter given the AI acceleration claims.

### 3. Net Issue Flow (Sustainability)

**Chart: `net_issue_flow_comparison.png`** *(Y-axis clamped to ±200)*

The "sustainability number" — when this is consistently positive, the backlog grows; consistently negative means the team is closing faster than issues arrive.

- **vscode** shows persistent positive flow (more issues opened than closed) — consistent with its growing backlog.
- **runtime** oscillates around zero with seasonal patterns aligned to .NET release cycles.
- **rust** and **go** hover slightly positive — backlogs growing slowly.
- **roslyn** is notably often negative — the team is actively working down its backlog.

### 4. PR Merge Rate

**Chart: `pr_merge_rate_comparison.png`** *(Go excluded)*

- **rust** leads at ~130-150 PRs merged/week, remarkably consistent over years.
- **runtime** averages ~120-150/week, also consistent — a healthy sign.
- **vscode** has a recent explosive spike to 300+/week, likely driven by bot PRs or automation.
- **roslyn** appears to be declining from ~100/week to ~60/week — potential concern if this reflects reduced investment.
- **maui** — to be evaluated for the AI acceleration effect.

### 5. Time to Merge

**Chart: `time_to_merge_comparison.png`** *(Go excluded)*

Median days from PR opened to merged, per month. A rising trend means review is getting slower.

*(Analysis will be updated once this chart is generated with the full dataset.)*

### 6. Sustainability Score

**Chart: `sustainability_score.png`**

Open issues as a percentage of all issues ever opened. Lower = more issues closed relative to inflow.

- **vscode** is the most aggressive closer (~5% still open) — but this may include "closed as duplicate" and "closed as won't fix."
- **runtime** is at ~11% after the 2020 mass closure.
- **rust** and **go** sit at ~15-18%, reflecting more conservative closure policies.

### 7. Maintainer Activity & PRs per Maintainer

**Charts: `active_maintainers_comparison.png`, `prs_per_maintainer_comparison.png`** *(Go excluded)*

"Active maintainer" = distinct person who merged at least 1 PR in the current or prior month.

This is a proxy for funding/attention:
- If the maintainer count drops while PRs stay flat, remaining people are overloaded.
- If PRs-per-maintainer rises, each person is doing more work — potentially unsustainable.
- If both drop, the project may be losing investment.

*(Analysis will be updated once maintainer data fetch completes.)*

### 8. Issue Responsiveness

**Chart: `issue_responsiveness_comparison.png`**

Percentage of issues closed within 30 days of opening. Higher = faster triage/resolution.

This measures how quickly a project responds to new issues — a signal of active maintenance and community health.

### 9. Contributor Diversity

**Chart: `contributor_diversity_comparison.png`** *(Go excluded)*

Distinct PR authors per month. Higher = broader community engagement.

A declining trend here could indicate the project is becoming harder to contribute to, or that community interest is waning.

---

## Known Artifacts & Limitations

### The 2020 Runtime Consolidation Cliff
dotnet/runtime was created in late 2019 by merging dotnet/coreclr and dotnet/corefx. The massive cliff in the open-issues chart around 2020 is NOT a real triage event — it's an artifact:
- Only **open** issues were transferred to the new repo
- **Closed** issues stayed behind in coreclr/corefx
- **PRs** pre-2020 are entirely missing from the runtime data

**Fix (planned)**: Fetch coreclr and corefx data and treat the three repos as a single lineage for runtime's history. This will fill the pre-2020 PR gap and eliminate the artificial cliff.

### golang/go Uses Gerrit
Go's code review happens on Gerrit, not GitHub. The ~5K GitHub PRs are mostly mirror artifacts. Go is excluded from all PR-based charts (merge rate, time-to-merge, maintainer stats) but included in issue-based charts.

### Reopen Events Not Tracked
If an issue was closed then reopened, we only see the final state. This slightly understates historical closure rates. The effect is small for the sustainability questions we're asking.

### Label History Not Available
We see current labels, not historical. Can't accurately track "how many bugs were open in 2022" if labels have changed since then.

### CSV Null Handling
When data is exported to CSV and reimported, empty strings replace SQL NULLs. The analysis code handles this gracefully (empty date strings parse as None), but raw SQL queries on the reimported DB should use `WHERE col != '' AND col IS NOT NULL` instead of just `IS NOT NULL`.

---

## Data & Scripts

| File | Purpose |
|------|---------|
| `fetch.py` | Paginated REST API fetcher with checkpoint/resume |
| `analyze.py` | Time series computation and chart generation |
| `load_csv.py` | Rebuild SQLite DB from compressed CSV |
| `data/items.csv.gz` | Compressed data export (~12MB) |
| `charts/` | Generated PNG charts |
| `plan.md` | Original feasibility proposal and design notes |

### Reproducing

```bash
pip install -r requirements.txt

# Option A: Rebuild from exported data
python load_csv.py
python analyze.py

# Option B: Fetch fresh from GitHub API (~2 hours)
python fetch.py
python analyze.py
```
