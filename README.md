# 🏦 FinTx AI Agent

An **AI-powered SQL Operations Agent** that monitors SQL Server 2019 financial transactions, detects database errors, and uses OpenAI GPT models to generate professional error analysis and email alerts.

---

##  Features
- **SQL Server 2019 stored procedures** for deposit, withdraw, and transfer operations.
- **ErrorLog & HealthCheck** system to capture failures and integrity issues.
- **Python CLI** (`main.py`) for simulating transactions with robust error handling.
- **Monitoring script** (`monitor_errors.py`) to detect new errors and append audit logs.
- **AI Agent** (`ai_agent.py`) that:
  - Fetches new errors + health results.
  - Runs **diagnostic probes** (deadlocks, duplicate keys, blocking sessions).
  - Summarizes issues into **actionable recommendations** using OpenAI GPT.
  - Sends **professional HTML email alerts** with context tables and AI insights.
- **Loop mode** for continuous monitoring (`--loop --interval 300`).

---

##  Project Structure
