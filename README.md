# Fraud Detection Case - Shine

This project analyzes banking event data to detect fraud patterns using Python and SQLite.

## Project Overview
- Processes raw banking transaction data from JSON files
- Stores data in a local SQLite database (`fraud.db`)
- Includes scripts for data ingestion, management, and fraud detection

## Project Structure
- `src/` : Python source code for data processing and fraud detection
  - `ingest.py` : Ingests data from JSON files into the database
  - `main.py` : Runs the full pipeline from ingestion to fraud detection
- `db/` : Contains the SQLite database file (`fraud.db`)
- `README.md` : Project documentation

## Database Setup
- The database file is located at `db/fraud.db`
- Data is ingested automatically from JSON files using `ingest.py`
- Restitution tables prefixed with `wrk_` are created directly and can be viewed using SQLite Viewer or SQLite Explorer

## How to Use
1. Clone the repository
2. Install Python 3.x and required packages
3. Place your JSON data files in the appropriate directory
4. Run `main.py` to execute the full pipeline (ingestion and fraud detection)
5. View results in the `wrk_` tables using SQLite tools or SQL viewer

## Main Scripts
- `main.py`: Runs the entire pipeline from ingestion to fraud detection
- Other scripts in `src/` for data cleaning and fraud detection

