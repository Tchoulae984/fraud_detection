# Entry point: orchestrates full fraud detection flow
# main.py
from src.ingest import ingest_all_events
from src.staging import create_all_staging
from src.fraud_rules import (
    detect_account_takeover,
    detect_suspicious_mcc_usage,
    detect_shared_beneficiaries
)

def main():
    print("🚀 Starting fraud detection pipeline...")

    # Step 1: Ingest raw JSON into DB
    print("📥 Ingesting events...")
    ingest_all_events()

    # Step 2: Create staging tables
    print("🧹 Building staging tables...")
    create_all_staging()

    # Step 3: Run fraud detection rules
    print("🔍 Running fraud rules...")
    detect_account_takeover()
    detect_suspicious_mcc_usage()
    detect_shared_beneficiaries()

    print("✅ Fraud detection pipeline completed.")

if __name__ == "__main__":
    main()
