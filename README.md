# Repo Health Metrics

Sustainability and health metrics for large GitHub repositories, comparing dotnet/runtime, dotnet/roslyn, dotnet/maui, microsoft/vscode, microsoft/vcpkg, microsoft/aspire, Azure/azure-sdk-for-js, rust-lang/rust, and golang/go.

**[View the charts](https://danmoseley.github.io/repo-health-metrics/)**

**[View the last known good charts](https://danmoseley.github.io/repo-health-metrics/last-known-good/)** — a frozen snapshot that stays valid even if an automated refresh ever goes wrong.

To restore data and generate charts on a fresh clone: `python load_csv.py && python analyze.py`. See `.github/copilot-instructions.md` for data preservation workflow details.
