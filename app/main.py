import os
import time
import functools
from pathlib import Path
from uuid import uuid4

import pyodbc
from dotenv import load_dotenv
from monitor_errors import send_email  # reuse email helper

# --- Load .env next to this file (works even if you run from another folder)
load_dotenv(Path(__file__).with_name(".env"))

# -------------------------
# Connection helpers
# -------------------------
def pick_driver() -> str | None:
    """Return a best-guess installed SQL Server ODBC driver name."""
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",  # legacy
    ]
    drivers = [d.strip() for d in pyodbc.drivers()]
    for p in preferred:
        if p in drivers:
            return p
    return drivers[-1] if drivers else None


def get_conn_str() -> str:
    """Use .env connection string or fallback to Windows Auth localhost."""
    env_str = os.getenv("SQL_CONNECTION_STRING")
    if env_str and env_str.strip():
        return env_str

    driver = pick_driver()
    if not driver:
        raise RuntimeError("No ODBC SQL Server driver found. Install 'ODBC Driver 18 for SQL Server'.")

    return f"DRIVER={{{driver}}};SERVER=localhost;DATABASE=FinDB;Trusted_Connection=Yes;Encrypt=no;TrustServerCertificate=yes"


def get_conn():
    """Open a DB connection with autocommit disabled so we can commit/rollback explicitly."""
    conn_str = get_conn_str()
    conn = pyodbc.connect(conn_str, autocommit=False)
    conn.timeout = 30
    return conn

# -------------------------
# Retry helper
# -------------------------
TRANSIENT_ERRORS = {1205, 1222, 4060, 40197, 40501, 49918, 49919, 49920}
BUSINESS_ERRORS = {50001, 50002, 50003, 2601}  # expected business errors

def _extract_errnum(ex: pyodbc.Error) -> int | None:
    """Extract SQL Server error number from pyodbc.Error."""
    try:
        if len(ex.args) > 1:
            text = str(ex.args[1])
        else:
            text = str(ex)
        for tok in text.replace('(', ' ').replace(')', ' ').replace('#', ' ').replace('[', ' ').replace(']', ' ').split():
            if tok.isdigit():
                return int(tok)
    except Exception:
        pass
    return None

def is_business_error(ex: pyodbc.Error) -> bool:
    return _extract_errnum(ex) in BUSINESS_ERRORS

def with_retry(max_attempts: int = 3, base_delay: float = 0.8):
    """Decorator retries a function on transient SQL errors with exponential backoff."""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 1
            while True:
                try:
                    return fn(*args, **kwargs)
                except pyodbc.Error as ex:
                    errnum = _extract_errnum(ex)
                    if errnum in TRANSIENT_ERRORS and attempt < max_attempts:
                        delay = base_delay * attempt
                        print(f"[retry] transient SQL error {errnum}; retrying in {delay:.1f}s (attempt {attempt+1}/{max_attempts})")
                        time.sleep(delay)
                        attempt += 1
                        continue
                    raise
        return wrapper
    return deco

# -------------------------
# Utilities
# -------------------------
def gen_ref(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"

# -------------------------
# Business operations
# -------------------------
@with_retry()
def deposit(account_id: int, amount: float, ref: str | None = None):
    if ref is None:
        ref = gen_ref("dep")
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute("EXEC dbo.usp_Deposit ?, ?, ?", (account_id, amount, ref))
            conn.commit()
            print(f"Deposit OK: acct={account_id} +{amount} (ref={ref})")
        except pyodbc.Error as ex:
            conn.rollback()
            if is_business_error(ex):
                print("Deposit FAILED (business error):", ex)
                return False
            print("Deposit FAILED:", ex)
            raise

@with_retry()
def withdraw(account_id: int, amount: float, ref: str | None = None):
    if ref is None:
        ref = gen_ref("wd")
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute("EXEC dbo.usp_Withdraw ?, ?, ?", (account_id, amount, ref))
            conn.commit()
            print(f"Withdraw OK: acct={account_id} -{amount} (ref={ref})")
        except pyodbc.Error as ex:
            conn.rollback()
            if is_business_error(ex):
                print("Withdraw FAILED (business error):", ex)
                return False
            print("Withdraw FAILED:", ex)
            raise

@with_retry()
def transfer(from_id: int, to_id: int, amount: float, ref: str | None = None):
    if ref is None:
        ref = gen_ref("tx")
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute("EXEC dbo.usp_TransferFunds ?, ?, ?, ?", (from_id, to_id, amount, ref))
            conn.commit()
            print(f"Transfer OK: {from_id} -> {to_id} amount={amount} (ref={ref})")
        except pyodbc.Error as ex:
            conn.rollback()
            if is_business_error(ex):
                print("Transfer FAILED (business error):", ex)
                return False
            print("Transfer FAILED:", ex)
            raise

# -------------------------
# Utilities for quick checks
# -------------------------
def show_accounts():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT AccountID, CustomerName, Balance FROM dbo.Accounts ORDER BY AccountID")
        rows = cur.fetchall()
        print("\nAccounts:")
        for r in rows:
            print(f"  #{r.AccountID:>3} | {r.CustomerName:<12} | Balance = {r.Balance:,.2f}")

def show_recent_errors(limit: int = 10):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT TOP ({limit})
                ErrorID, ProcName, ErrorNumber, ErrorMessage, OccurredAt
            FROM dbo.ErrorLog
            ORDER BY ErrorID DESC
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("\nErrorLog: (empty)")
            return
        print("\nRecent ErrorLog:")
        for r in rows:
            print(f"  {r.ErrorID}: [{r.ProcName}] #{r.ErrorNumber} @ {r.OccurredAt} -> {r.ErrorMessage}")

def health_check():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("EXEC dbo.usp_HealthCheck")
        any_rows = False
        while True:
            if cur.description:
                rows = cur.fetchall()
                if rows:
                    if not any_rows:
                        print("\nHealthCheck results:")
                    any_rows = True
                    for r in rows:
                        print(tuple(r))
            if not cur.nextset():
                break
        if not any_rows:
            print("\nHealthCheck results: OK (no issues found)")

# -------------------------
# CLI
# -------------------------
if __name__ == "__main__":
    import argparse

    print("Installed ODBC drivers:", pyodbc.drivers())
    print("Using .env connection string:", bool(os.getenv("SQL_CONNECTION_STRING")))

    p = argparse.ArgumentParser(prog="finance-tx", description="Bank ops with robust T-SQL error handling")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_dep = sub.add_parser("deposit", help="Deposit into an account")
    p_dep.add_argument("--account", type=int, required=True)
    p_dep.add_argument("--amount", type=float, required=True)
    p_dep.add_argument("--ref", type=str, default=None)

    p_wd = sub.add_parser("withdraw", help="Withdraw from an account")
    p_wd.add_argument("--account", type=int, required=True)
    p_wd.add_argument("--amount", type=float, required=True)
    p_wd.add_argument("--ref", type=str, default=None)

    p_tx = sub.add_parser("transfer", help="Transfer between accounts")
    p_tx.add_argument("--from-id", type=int, required=True)
    p_tx.add_argument("--to-id", type=int, required=True)
    p_tx.add_argument("--amount", type=float, required=True)
    p_tx.add_argument("--ref", type=str, default=None)

    sub.add_parser("show-accounts", help="List accounts & balances")
    sub.add_parser("show-errors", help="Show recent ErrorLog entries")
    sub.add_parser("health-check", help="Run DB integrity checks")
    sub.add_parser("mail-test", help="Send a test email using current SMTP settings")

    args = p.parse_args()

    if args.cmd == "deposit":
        deposit(args.account, args.amount, args.ref)
    elif args.cmd == "withdraw":
        withdraw(args.account, args.amount, args.ref)
    elif args.cmd == "transfer":
        transfer(args.from_id, args.to_id, args.amount, args.ref)
    elif args.cmd == "show-accounts":
        show_accounts()
    elif args.cmd == "show-errors":
        show_recent_errors()
    elif args.cmd == "health-check":
        health_check()
    elif args.cmd == "mail-test":
        send_email("FinTx Test", "Hello from FinTx — this is a test email.")
        print("Test email sent.")
