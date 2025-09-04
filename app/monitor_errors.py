import os, sys, csv, sqlite3, datetime as dt, smtplib
from email.message import EmailMessage
from pathlib import Path
import pyodbc
from dotenv import load_dotenv

HERE = Path(__file__).parent
load_dotenv(HERE / ".env")

STATE = HERE / ".monitor_state.sqlite"
AUDIT = HERE / "monitor_audit.csv"
CONN_STR = os.getenv("SQL_CONNECTION_STRING")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_STARTTLS = os.getenv("SMTP_STARTTLS", "true").lower() == "true"
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_TO = [e.strip() for e in os.getenv("SMTP_TO", "").split(";") if e.strip()]

def get_conn(): return pyodbc.connect(CONN_STR, autocommit=True, timeout=30)

def get_last_id():
    con = sqlite3.connect(STATE)
    con.execute("CREATE TABLE IF NOT EXISTS s(k TEXT PRIMARY KEY, v INTEGER)")
    row = con.execute("SELECT v FROM s WHERE k='last_error_id'").fetchone()
    con.close()
    return row[0] if row else 0

def set_last_id(v):
    con = sqlite3.connect(STATE)
    con.execute("INSERT INTO s(k,v) VALUES('last_error_id', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",(v,))
    con.commit(); con.close()

def fetch_new_errors(since_id):
    with get_conn() as c:
        cur = c.cursor()
        cur.execute("""SELECT ErrorID, ProcName, ErrorNumber, ErrorMessage, OccurredAt
                       FROM dbo.ErrorLog WHERE ErrorID > ? ORDER BY ErrorID""", since_id)
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
            if not cur.nextset(): break
    return issues

def append_csv(tag, rows):
    if not rows: return
    exists = AUDIT.exists()
    with open(AUDIT, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists: w.writerow(["Tag","Col1","Col2","Col3","Col4","Col5"])
        for r in rows: w.writerow([tag, *map(str, r)])

def send_email(subject, text):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and SMTP_TO):
        print("Email not configured; set SMTP_* in .env"); return
    msg = EmailMessage()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(SMTP_TO)
    msg["Subject"] = subject
    msg.set_content(text)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        if SMTP_STARTTLS: s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

def fmt_errors(rows):
    if not rows: return "No new ErrorLog rows."
    return "\n".join(f"{r.ErrorID}: [{r.ProcName}] #{r.ErrorNumber} @ {r.OccurredAt} -> {r.ErrorMessage}" for r in rows)

def fmt_health(issues):
    if not issues: return "HealthCheck: OK"
    return "\n".join(str(it) for it in issues)

if __name__ == "__main__":
    now = dt.datetime.utcnow()
    print(f"\n=== FinTx Monitor @ {now}Z ===")

    last = get_last_id()
    rows = fetch_new_errors(last)
    if rows:
        print("\n*** New ErrorLog entries ***")
        print(fmt_errors(rows))
        set_last_id(rows[-1][0])
    else:
        print("No new ErrorLog rows.")

    issues = run_health_check()
    if issues:
        print("\n*** HealthCheck issues ***")
        print(fmt_health(issues))
    else:
        print("HealthCheck: OK")

    append_csv("ErrorLog", rows)
    append_csv("Health", issues)

    subject = f"[FinTx] {len(rows)} new errors, {'issues found' if issues else 'health OK'}"
    body = f"Time: {now}Z\n\n{fmt_errors(rows)}\n\n{fmt_health(issues)}"
    if rows or issues:
        send_email(subject, body)

    print("Done.")
