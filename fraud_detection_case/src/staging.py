# Script to create staging tables from raw


import sqlite3

DB_PATH = "db/fraud.db"  # Adjusted path based on project root

def create_stg_transactions():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Drop if exists for clean re-runs
    cursor.execute("DROP TABLE IF EXISTS stg_transactions;")

    # Create staging table from raw
    cursor.execute("""
        CREATE TABLE stg_transactions AS
        SELECT
            transaction_id,
            company_id,
            bank_account_id,
            amount / 100.0 AS amount_eur,  -- Convert from cents
            currency,
            type,
            COALESCE(debtor_iban, creditor_iban) AS counterparty_iban,
            mcc_code,
            title,
            datetime(created_at) AS created_at, -- Convert to datetime
            strftime('%Y-%m-%d', created_at) AS created_date
        FROM raw_transaction_created
        WHERE currency = 'EUR'
          AND amount IS NOT NULL
          AND created_at IS NOT NULL;
    """)

    conn.commit()
    conn.close()
    print("✅ stg_transactions table created.")



def create_stg_company_updates():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS stg_company_updates;")

    cursor.execute("""
        CREATE TABLE stg_company_updates AS
        SELECT
            company_id,
            update_type,
            new_value,
            datetime(updated_at) AS updated_at
        FROM raw_company_profile_updated
        WHERE company_id IS NOT NULL
          AND updated_at IS NOT NULL;
    """)

    conn.commit()
    conn.close()
    print("✅ stg_company_updates created.")



def create_stg_user_settings():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS stg_user_settings;")

    cursor.execute("""
        CREATE TABLE stg_user_settings AS
        SELECT
            user_id,
            update_type,
            datetime(updated_at) AS updated_at
        FROM raw_user_settings_updated
        WHERE user_id IS NOT NULL
          AND updated_at IS NOT NULL;
    """)

    conn.commit()
    conn.close()
    print("✅ stg_user_settings created.")



def create_stg_beneficiaries():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS stg_beneficiaries;")

    cursor.execute("""
        CREATE TABLE stg_beneficiaries AS
        SELECT
            beneficiary_id,
            company_id,
            datetime(created_at) AS created_at
        FROM raw_beneficiary_added
        WHERE beneficiary_id IS NOT NULL
          AND created_at IS NOT NULL;
    """)

    conn.commit()
    conn.close()
    print("✅ stg_beneficiaries created.")


def create_all_staging():
    create_stg_transactions()
    create_stg_company_updates()
    create_stg_user_settings()
    create_stg_beneficiaries()

if __name__ == "__main__":
    create_all_staging()
