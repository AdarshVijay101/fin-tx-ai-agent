# ai_agent.py
# FinTxOps agent — polls SQL error log, summarizes with OpenAI (with fallback),
# and emails a professional HTML report.

import os
import time
import sqlite3
import socket
import smtplib
import ssl
import traceback
from pathlib import Path
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pyodbc
from dotenv import load_dotenv

# ------------- .env -------------
HERE = Path(__file__).parent
load_dotenv(HERE / ".env")

SQL_CONN_STR = os.getenv("SQL_CONNECTION_STRING", "")
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_STARTTLS = os.getenv("SMTP_STARTTLS", "true").lower() == "true"
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_TO = [e.strip() for e in os.getenv("SMTP_TO", SMTP_USER).split(";") if e.strip()]

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # must be set in your environment

STATE_DB = HERE / ".agent_state.sqlite"

# ------------- DB helpers -------------
def get_conn():
    if not SQL_CONN_STR:
        raise RuntimeError("SQL_CONNECTION_STRING missing in .env")
    # Autocommit= True because we only read
    return pyodbc.connect(SQL_CONN_STR, autocommit=True, timeout=30)

def fetch_new_errors(last_id: int):
    with get_conn() as c:
        cur = c.cursor()
        cur.execute("""
            SELECT ErrorID, ProcName, ErrorNumber, ErrorMessage, OccurredAt
            FROM dbo.ErrorLog
            WHERE ErrorID > ?
            ORDER BY ErrorID
        """, last_id)
        return cur.fetchall()

def run_health_check():
    issues = []
    with get_conn() as c:
        cur = c.cursor()
        cur.execute("EXEC dbo.usp_HealthCheck")
        while True:
            if cur.description:
                rows = cur.fetchall()
                issues.extend([tuple(r) for r in rows])
            if not cur.nextset():
                break
    return issues  # [] means OK

# ------------- state (last seen ErrorID) -------------
def state_init():
    con = sqlite3.connect(STATE_DB)
    con.execute("CREATE TABLE IF NOT EXISTS s(k TEXT PRIMARY KEY, v TEXT)")
    con.commit()
    con.close()

def state_get_last_id() -> int:
    con = sqlite3.connect(STATE_DB)
    row = con.execute("SELECT v FROM s WHERE k='last_error_id'").fetchone()
    con.close()
    return int(row[0]) if row else 0

def state_set_last_id(v: int):
    con = sqlite3.connect(STATE_DB)
    con.execute("INSERT INTO s(k,v) VALUES('last_error_id', ?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(v),))
    con.commit()
    con.close()

# ------------- OpenAI summary with fallback -------------
def ai_summary(prompt: str) -> str | None:
    """Try to get a summary from OpenAI. Returns None if anything fails."""
    try:
        if not OPENAI_API_KEY:
            return None
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        resp = client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt}
                    ],
                }
            ],
        )
        text = (resp.output_text or "").strip()
        return text or None
    except Exception:
        # Don’t crash the agent if the AI call fails
        return None

def fallback_summary(error_rows, health_rows):
    """Rule-based summary when the AI gives no answer."""
    if not error_rows and not health_rows:
        return "No new errors; health OK."
    lines = []
    if error_rows:
        first_ts = error_rows[0].OccurredAt
        last_ts  = error_rows[-1].OccurredAt
        try:
            # pyodbc datetimes can be naive; normalize to UTC string
            def fmt(dt):
                if hasattr(dt, "tzinfo") and dt.tzinfo:
                    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            span = f"{fmt(first_ts)} → {fmt(last_ts)}"
        except Exception:
            span = "see table"
        total = len(error_rows)
        by_proc = {}
        by_code = {}
        for r in error_rows:
            by_proc[r.ProcName] = by_proc.get(r.ProcName, 0) + 1
            by_code[r.ErrorNumber] = by_code.get(r.ErrorNumber, 0) + 1
        proc_part = ", ".join(f"{k}:{v}" for k,v in by_proc.items())
        code_part = ", ".join(f"{k}:{v}" for k,v in by_code.items())
        lines.append(f"{total} new error(s) during {span}. By proc [{proc_part}]. By code [{code_part}].")

        # Common guidance for known codes
        if 50003 in by_code:
            lines.append("Err 50003 (Insufficient funds): treat as business rejection; notify support/finance; ensure UI/caller validates balance and avoids retry loops.")
    if health_rows:
        lines.append(f"HealthCheck reported {len(health_rows)} issue(s) — see details below.")
    else:
        lines.append("HealthCheck: OK.")
    return " ".join(lines)

def build_prompt(error_rows, health_rows):
    host = socket.gethostname()
    lines = [
        "You are FinTxOps Assistant.",
        "Summarize the new ErrorLog rows and health check for a payments system.",
        "Focus on what happened, likely cause, and action items for ops/engineering.",
        "Be concise (5–10 lines max). Use bullets.",
        f"Host: {host}",
        "",
        "New errors (CSV):",
        "ErrorID,Proc,Err,Message,WhenUTC"
    ]
    for r in error_rows:
        when = r.OccurredAt
        try:
            # format to UTC-ish text
            s = when.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            s = str(when)
        lines.append(f"{r.ErrorID},{r.ProcName},{r.ErrorNumber},{r.ErrorMessage},{s}")
    lines.append("")
    if health_rows:
        lines.append("Health issues (raw tuples):")
        for h in health_rows:
            lines.append(str(h))
    else:
        lines.append("Health issues: none")
    return "\n".join(lines)

# ------------- email -------------
def html_escape(s: str) -> str:
    return (s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            .replace('"',"&quot;").replace("'","&#39;"))

def render_email_html(summary, error_rows, health_rows):
    # summary can be None; show placeholder
    summary_html = html_escape(summary or "(no summary)")
    # error table
    if error_rows:
        rows = []
        for r in error_rows:
            when = r.OccurredAt
            try:
                w = when.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
            except Exception:
                try:
                    w = when.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    w = str(when)
            rows.append(
                f"<tr>"
                f"<td>{r.ErrorID}</td>"
                f"<td>{html_escape(str(r.ProcName))}</td>"
                f"<td>{r.ErrorNumber}</td>"
                f"<td>{html_escape(str(r.ErrorMessage))}</td>"
                f"<td>{w}</td>"
                f"</tr>"
            )
        error_table = (
            "<h3>New ErrorLog rows</h3>"
            "<table border='1' cellspacing='0' cellpadding='6'>"
            "<tr><th>ErrorID</th><th>Proc</th><th>Err #</th><th>Message</th><th>When (UTC)</th></tr>"
            + "".join(rows) + "</table>"
        )
    else:
        error_table = "<p>No new ErrorLog rows.</p>"

    # health
    if health_rows:
        hrows = "".join(f"<li>{html_escape(str(x))}</li>" for x in health_rows)
        health_html = f"<h3>Health issues</h3><ul>{hrows}</ul>"
    else:
        health_html = "<p>HealthCheck: OK</p>"

    host = html_escape(socket.gethostname())
    return f"""
    <html>
      <body>
        <h2>[FinTxOps] Report — host: {host}</h2>
        <p><pre>{summary_html}</pre></p>
        {error_table}
        {health_html}
        <br/>
        <p style="color:#888;font-size:12px">Automated by FinTxOps agent.</p>
      </body>
    </html>
    """

def send_email(subject: str, html_body: str):
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS or not SMTP_TO:
        print("Email not sent: SMTP env not fully configured.")
        return
    msg = MIMEMultipart("alternative")
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(SMTP_TO)
    msg["Subject"] = subject
    # Plain part (very short)
    msg.attach(MIMEText("See HTML version.", "plain"))
    msg.attach(MIMEText(html_body, "html"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        if SMTP_STARTTLS:
            s.starttls(context=ctx)
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_USER, SMTP_TO, msg.as_string())

# ------------- main loop -------------
def main_loop(interval: int = 300, send_when_no_changes=False):
    state_init()
    print("FinTxOps agent running in a loop. Ctrl+C to stop.")
    while True:
        try:
            run_once(send_when_no_changes=send_when_no_changes)
        except Exception as e:
            print("Agent error:", e)
            traceback.print_exc()
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            break

def run_once(send_when_no_changes=False):
    state_init()
    last = state_get_last_id()
    err_rows = fetch_new_errors(last)
    health_rows = run_health_check()
    new_last = last

    if err_rows:
        new_last = max(r.ErrorID for r in err_rows)

    if not err_rows and not health_rows and not send_when_no_changes:
        print("Agent: no new errors and health OK — skipping OpenAI/email.")
        if new_last != last:
            state_set_last_id(new_last)
        return

    # Build prompt and call AI (with fallback)
    prompt = build_prompt(err_rows, health_rows)
    summary = ai_summary(prompt)
    if not summary:
        summary = fallback_summary(err_rows, health_rows)

    # Subject
    host = socket.gethostname()
    err_count = len(err_rows)
    health_ok = "OK" if not health_rows else "issues"
    subject = f"[FinTxOps] Report - {err_count} errors; health {health_ok}."

    # Email
    html = render_email_html(summary, err_rows, health_rows)
    send_email(subject, html)
    print("\n--- Agent Summary ---")
    print(summary or "(no summary)")
    print("\nEmail sent.\n")

    if new_last != last:
        state_set_last_id(new_last)

# ------------- CLI -------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="FinTxOps Agent")
    ap.add_argument("--loop", action="store_true", help="Run forever")
    ap.add_argument("--interval", type=int, default=300, help="Seconds between checks when --loop")
    ap.add_argument("--send-when-idle", action="store_true",
                    help="Email even if there are no new errors and health is OK")
    args = ap.parse_args()

    if args.loop:
        main_loop(interval=args.interval, send_when_no_changes=args.send_when_idle)
    else:
        run_once(send_when_no_changes=args.send_when_idle)
