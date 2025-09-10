# Script to ingest raw JSON into raw SQLite tables
#python src/ingest.py to test

import os
import json
import sqlite3

# Path to the SQLite DB
DB_PATH = "db/fraud.db"

# Path where your raw JSON files are stored
EVENTS_PATH = "data/raw_events"

# Map of filenames to their matching raw table name
file_table_map = {
    "transactions.json": "raw_transaction_created",
    "company_updates.json": "raw_company_profile_updated",
    "user_updates.json": "raw_user_settings_updated",
    "beneficiaries.json": "raw_beneficiary_added"
}

def ingest_all_events():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for filename, table in file_table_map.items():
        filepath = os.path.join(EVENTS_PATH, filename)
        
        if not os.path.exists(filepath):
            print(f"⚠️ File not found: {filename}")
            continue

        with open(filepath, "r") as f:
            records = json.load(f)

        if records:
            # Extract column names and placeholders
            keys = records[0].keys()
            placeholders = ", ".join(["?"] * len(keys))
            insert_sql = f"""
                INSERT INTO {table} ({', '.join(keys)})
                VALUES ({placeholders});
            """

            # Convert each record to a tuple
            data = [tuple(record.get(k) for k in keys) for record in records]

            # Insert into DB
            cursor.executemany(insert_sql, data)
            print(f"✅ Inserted {len(data)} records into {table}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    ingest_all_events()
