import os, sys, csv, time, logging, sqlite3, datetime as dt
from pathlib import Path
from typing import List, Tuple, Optional

import pyodbc
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

HERE = Path(__file__).parent
load_dotenv(HERE / ".env")

# ---------- Logging ----------
LOGS = HERE / "logs"
LOGS.mkdir(exist_ok=True)
logging.basicConfig(
    filename=LOGS / "agent.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------- DB ----------
CONN_STR = os.getenv("SQL_CONNECTION_STRING")

def conn():
    c = pyodbc.connect(CONN_STR, autocommit=False)
    c.timeout = 30
    return c

# ---------- State (sqlite) ----------
STATE = HERE / ".agent_state.sqlite"
def _state_conn():
    con = sqlite3.connect(STATE)
    con.execute("""CREATE TABLE IF NOT EXISTS s(
        k TEXT PRIMARY KEY, v TEXT
    )""")
    return con

def get_state(key: str, default: str = "0") -> str:
    con = _state_conn()
    row = con.execute("SELECT v FROM s WHERE k=?", (key,)).fetchone()
    return row[0] if row else default

def set_state(key: str, value: str) -> None:
    con = _state_conn()
    con.execute(
        "INSERT INTO s(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )
    con.commit()

# ---------- Email ----------
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_STARTTLS = os.getenv("SMTP_STARTTLS", "true").lower() == "true"
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_TO = [x.strip() for x in os.getenv("SMTP_TO", SMTP_USER or "").split(";") if x.strip()]
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "noreply@example.com")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "FinTx Agent")

def send_email(subject: str, html_body: str) -> None:
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and SMTP_TO):
        logging.warning("SMTP not fully configured; skipping email")
        return
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{SMTP_FROM_NAME} <{SMTP_FROM}>"
    msg["To"] = ", ".join(SMTP_TO)
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        if SMTP_STARTTLS:
            s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_FROM, SMTP_TO, msg.as_string())

# ---------- Data access ----------
def fetch_new_errors(since_id: int):
    with conn() as c:
        cur = c.cursor()
        cur.execute("""
            SELECT ErrorID, ProcName, ErrorNumber, ErrorMessage, OccurredAt
            FROM dbo.ErrorLog
            WHERE ErrorID > ?
            ORDER BY ErrorID
        """, since_id)
        return cur.fetchall()

def run_health_check() -> List[Tuple]:
    issues = []
    with conn() as c:
        cur = c.cursor()
        cur.execute("EXEC dbo.usp_HealthCheck")
        while True:
            if cur.description:
                rows = cur.fetchall()
                for r in rows:
                    issues.append(tuple(r))
            if not cur.nextset():
                break
    return issues

# ---------- “Agentic” planning (rule-based; zero cost) ----------
def classify_and_plan(err_row) -> Tuple[str, str]:
    """
    Return (severity, plan_text).
    Severity: P1 (critical), P2, P3 (business), INFO
    """
    _, proc, num, msg, when = err_row
    num = int(num) if isinstance(num, (int,)) or (str(num).isdigit()) else None
    proc = proc or "unknown"

    if num in (2601, 2627):
        return ("P2",
                "Duplicate reference detected. Action: verify the ref, keep the earliest TransactionID, "
                "void/cancel the duplicates. Suggested SQL:\n"
                "  ;WITH c AS (\n"
                "    SELECT TransactionID, ROW_NUMBER() OVER (PARTITION BY Ref ORDER BY TransactionID) rn\n"
                "    FROM dbo.Transactions WHERE Ref = @Ref\n"
                "  )\n"
                "  DELETE T FROM dbo.Transactions T JOIN c ON c.TransactionID=T.TransactionID WHERE c.rn > 1;\n")
    if num == 50003:
        return ("P3",
                "Insufficient funds. Action: deposit to the source account, reduce transfer amount, or retry later.")
    if num in (1205, 1222):  # deadlock / lock timeout
        return ("P2",
                "Transient concurrency issue. Action: retry with backoff. Our Python client already retries (A2).")
    # default
    return ("P2", f"Investigate in SSMS. Procedure={proc}, Message={msg}")

# ---------- HTML email builder ----------
def html_report(new_errors: List[Tuple], health_issues: List[Tuple]) -> Optional[str]:
    if not new_errors and not health_issues:
        return None

    style = """
    <style>
      body { font-family: Segoe UI, Arial, sans-serif; color:#0f172a; }
      .card { border:1px solid #e2e8f0; border-radius:10px; padding:16px; margin-bottom:16px; }
      .h { font-size:16px; font-weight:600; margin:0 0 10px 0;}
      table { border-collapse: collapse; width:100%; }
      th, td { border-bottom:1px solid #e2e8f0; padding:8px; text-align:left; font-size:13px; }
      th { background:#f8fafc; }
      .pill { padding:2px 8px; border-radius:999px; color:white; font-size:11px; }
      .P1 { background:#dc2626; } .P2 { background:#d97706; } .P3 { background:#059669; } .INFO { background:#64748b; }
      .mono { font-family: Consolas, monospace; }
      .footer { color:#64748b; font-size:12px; margin-top:8px; }
    </style>
    """
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    out = [f"<!doctype html><html><head>{style}</head><body>"]
    out.append(f"<div class='card'><div class='h'>FinTx Agent Report <span class='mono'>{now}</span></div>")

    if new_errors:
        out.append("<div class='h'>New errors</div>")
        out.append("<table><tr><th>ID</th><th>When (UTC)</th><th>Proc</th><th>Err#</th><th>Plan</th></tr>")
        for e in new_errors:
            eid, proc, num, msg, when = e
            sev, plan = classify_and_plan(e)
            out.append(
                f"<tr><td>{eid}</td><td class='mono'>{when}</td><td>{proc}</td>"
                f"<td><span class='pill {sev}'>{sev}</span> <span class='mono'>{num}</span></td>"
                f"<td><pre class='mono' style='white-space:pre-wrap'>{plan}</pre></td></tr>"
            )
        out.append("</table>")

    if health_issues:
        out.append("<div class='h' style='margin-top:14px'>Health findings</div>")
        out.append("<table><tr><th>Check</th><th>Key</th><th>Value</th></tr>")
        for it in health_issues:
            # Your usp_HealthCheck returns tuples; render them as generic columns
            cells = "".join(f"<td class='mono'>{str(x)}</td>" for x in it)
            out.append(f"<tr>{cells}</tr>")
        out.append("</table>")

    out.append("<div class='footer'>This message was generated by FinTx Agent.</div></div></body></html>")
    return "".join(out)

# ---------- Main one-shot run ----------
def main():
    logging.info("Agent start")
    last_id = int(get_state("last_error_id", "0"))
    rows = fetch_new_errors(last_id)
    issues = run_health_check()
    html = html_report(rows, issues)
    if html:
        subject = f"[FinTx] {len(rows)} new error(s), health {'OK' if not issues else 'has findings'}"
        send_email(subject, html)
    if rows:
        set_state("last_error_id", str(rows[-1][0]))
    logging.info("Agent done")

if __name__ == "__main__":
    try:
        main()
        print("Agent run complete.")
    except Exception as e:
        logging.exception("Agent crashed")
        print("Agent crashed:", e, file=sys.stderr)
        sys.exit(1)
