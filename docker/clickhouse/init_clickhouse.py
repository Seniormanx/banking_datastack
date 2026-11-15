
import os
from pathlib import Path

import clickhouse_connect

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    database="default"
)

# Locate SQL file
sql_file = (
    Path(__file__).resolve().parents[1]
    / "clickhouse"
    / "create_database.sql"
)

# Read SQL
sql_content = sql_file.read_text(encoding="utf-8")

# Execute statements individually
statements = [
    statement.strip()
    for statement in sql_content.split(";")
    if statement.strip()
]

for statement in statements:
    client.command(statement)
    print(f"Executed: {statement[:80]}...")

print("ClickHouse initialization completed successfully.")

client.close()
