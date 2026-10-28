"""Public code allowed in each private account app and source-only checks."""
APP_VERSION = "V2.24"
BUSINESS_FILES = (
    "web_app.py", "stock_agent.py", "charts.py", "portfolio.py", "holdings.py",
    "watchlist.py", "market.py", "config.py", "screener.py", "paper.py",
    "outlook.py", "entry_signals.py", "exit_signals.py", "decision.py",
    "signal_snapshot.py", "source_check.py", "runtime_metrics.py", "rule_contract.py",
    "trade_constraints.py", "app_manifest.py", "runtime_identity.py", "rule_inputs.py", "private_storage.py", "public_daily_cache.py",
    "historical_policy.py", "historical_data.py", "research_execution.py", "research_job.py", "market_clock.py", "sina_daily.py", "brief_snapshot.py", "public_quote_http.py",
)
APP_FILES = (*BUSINESS_FILES, "web/index.html")

# Owner-only optional research service. Never copied into per-account apps.
OWNER_MODULE_FILES = (
    "announcement_lab/__init__.py", "announcement_lab/contracts.py", "announcement_lab/sources.py",
    "announcement_lab/extraction.py", "announcement_lab/storage.py", "announcement_lab/pipeline.py",
    "announcement_lab/api.py", "announcement_lab/serve.py", "announcement_lab/index.html",
    "announcement_lab/samples/official_listing.json", "announcement_lab/README.md",
    "requirements-announcements.txt", "启动公告研究.bat",
)
