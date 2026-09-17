import argparse
import os
import random
import sys
import time
from decimal import Decimal, ROUND_DOWN

import psycopg2  # pyright: ignore[reportMissingImports]
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = Decimal("1000.00")
CURRENCY = "USD"
INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")
DEFAULT_LOOP = True
SLEEP_SECONDS = 2

parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

fake = Faker()


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    value = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
conn.autocommit = False
cur = conn.cursor()


def run_iteration():
    customers = []
    for _ in range(NUM_CUSTOMERS):
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (fake.first_name(), fake.last_name(), fake.unique.email()),
        )
        customers.append(cur.fetchone()[0])

    accounts = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (customer_id, account_type, random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX), CURRENCY),
            )
            accounts.append(cur.fetchone()[0])

    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(txn_types)
        amount = Decimal(str(random.uniform(1, float(MAX_TXN_AMOUNT)))).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )
        related_account = None
        if txn_type == "TRANSFER":
            related_account = random.choice([account for account in accounts if account != account_id])

        cur.execute("SELECT balance FROM accounts WHERE id = %s FOR UPDATE", (account_id,))
        source_balance = cur.fetchone()[0]
        status = "COMPLETED"

        if txn_type == "DEPOSIT":
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, account_id))
        elif txn_type == "WITHDRAWAL" and source_balance >= amount:
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, account_id))
        elif txn_type == "TRANSFER" and source_balance >= amount:
            cur.execute("SELECT id FROM accounts WHERE id = %s FOR UPDATE", (related_account,))
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, account_id))
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, related_account))
        else:
            status = "FAILED"

        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) "
            "VALUES (%s, %s, %s, %s, %s)",
            (account_id, txn_type, amount, related_account, status),
        )

    conn.commit()
    print(
        f"Generated {len(customers)} customers, {len(accounts)} accounts, "
        f"{NUM_TRANSACTIONS} transactions.",
        flush=True,
    )


try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---", flush=True)
        run_iteration()
        print(f"--- Iteration {iteration} finished ---", flush=True)
        if not LOOP:
            break
        time.sleep(SLEEP_SECONDS)
except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...", flush=True)
except Exception:
    conn.rollback()
    print("\nERROR while generating data:", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    cur.close()
    conn.close()
