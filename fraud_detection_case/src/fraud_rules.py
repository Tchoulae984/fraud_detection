# Fraud detection rule logic functions
import sqlite3

DB_PATH = "db/fraud.db"

def detect_account_takeover():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS wrk_account_takeover_alerts;")

    cursor.execute("""
        CREATE TABLE wrk_account_takeover_alerts AS
        SELECT distinct
            t.transaction_id,
            t.company_id,
            t.amount_eur,
            t.created_at AS transaction_time,
            cu.updated_at AS company_update_time,
            CAST((strftime('%s', t.created_at) - strftime('%s', cu.updated_at)) AS INTEGER) AS seconds_diff,
            'Company profile updated shortly before transaction' AS match_reason
        FROM stg_transactions t
        LEFT JOIN stg_company_updates cu
            ON t.company_id = cu.company_id
        WHERE t.amount_eur > 1000
          AND cu.updated_at IS NOT NULL
          AND cu.updated_at <= t.created_at
          AND cu.updated_at >= datetime(t.created_at, '-15 minutes')
    ;
    """)


    conn.commit()
    conn.close()
    print("🚨 Rule 1 (Account Takeover) alerts generated.")



def detect_suspicious_mcc_usage():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Drop old table if exists
    cursor.execute("DROP TABLE IF EXISTS wrk_mcc_fraud_alerts;")

    # Create the alert table using group + filter
    cursor.execute("""
        CREATE TABLE wrk_mcc_fraud_alerts AS
        SELECT
            company_id,
            mcc_code,
            strftime('%Y-%m', created_at) AS month_window,
            '1_month' AS window_type,
            COUNT(*) AS suspicious_tx_count,
            SUM(amount_eur) AS total_suspicious_amount,
            'Multiple high-value gambling transactions in 1 month' AS match_reason
        FROM stg_transactions
        WHERE mcc_code = '7995'
        AND amount_eur > 500
        GROUP BY company_id, mcc_code, month_window

        UNION

        SELECT
            company_id,
            mcc_code,
            null as month_window,
            '3_month' AS window_type,
            COUNT(*) AS suspicious_tx_count,
            SUM(amount_eur) AS total_suspicious_amount,
            'High-value gambling across 3+ months' AS match_reason
        FROM stg_transactions
        WHERE mcc_code = '7995'
        AND amount_eur > 3000
        GROUP BY company_id, mcc_code
        HAVING COUNT(DISTINCT strftime('%Y-%m', created_at)) >= 3
    ;
    """)

    conn.commit()
    conn.close()
    print("🚨 Rule 2 (Suspicious MCC usage) alerts generated.")



def detect_shared_beneficiaries():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS wrk_shared_beneficiary_alerts;")

    cursor.execute("""
        CREATE TABLE wrk_shared_beneficiary_alerts AS
        SELECT
            beneficiary_id,
            COUNT(DISTINCT company_id) AS num_companies,
            GROUP_CONCAT(DISTINCT company_id) AS involved_companies,
            MIN(created_at) AS first_added,
            MAX(created_at) AS last_added,
            'Beneficiary shared across multiple companies' AS match_reason
        FROM stg_beneficiaries
        GROUP BY beneficiary_id
        HAVING num_companies >= 2;
    """)

    conn.commit()
    conn.close()
    print("🚨 Rule 3 (Shared Beneficiaries) alerts generated.")


if __name__ == "__main__":
    detect_account_takeover()
    detect_suspicious_mcc_usage()
    detect_shared_beneficiaries()
