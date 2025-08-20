# Logcomex Importer - Minimalistic Version

Two simple, self-contained Python scripts for importing and summarizing Logcomex data.

## Files

- **`import_records.py`** - Fetches data from Logcomex API and stores in SQLite
- **`import_summarize.py`** - Generates business intelligence summaries from imported data
- **`importer.db`** - SQLite database file
- **`.gitignore`** - Git ignore file
- **`venv/`** - Python virtual environment

## Usage

1. **Activate virtual environment:**
   ```bash
   venv\Scripts\activate
   ```

2. **Import data from API:**
   ```bash
   python import_records.py
   ```

3. **Generate summaries:**
   ```bash
   python import_summarize.py
   ```

## Configuration

Set environment variable `DEFAULT_MONTHS_BACK` to control the time range (default: 12 months).

## Features

- **Self-contained**: All dependencies included in each script
- **No external imports**: Works independently
- **Same business logic**: Preserves all KPI calculations and business intelligence
- **SQLite database**: Simple file-based storage
- **API integration**: Logcomex API with rate limiting and pagination 