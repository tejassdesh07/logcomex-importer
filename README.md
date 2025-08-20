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

### Configure import_records.py
Edit the configuration section in `import_records.py` to customize your settings:

```python
# Configuration section in import_records.py (lines 16-20)
DATABASE_FILE = "importer.db"
API_KEY = "your-api-key-here"
API_URL = "https://bi-api.logcomex.io/api/v1/details"
MONTHS_BACK = 6
IMPORTER_NAME = "your-importer-name-here"
```

**Important**: 
- Update the `API_KEY` with your actual Logcomex API key
- Change `MONTHS_BACK` to control time range (default: 6 months)
- Modify `IMPORTER_NAME` to filter specific importers

## Features

- **Self-contained**: All dependencies included in each script
- **No external imports**: Works independently
- **Same business logic**: Preserves all KPI calculations and business intelligence
- **SQLite database**: Simple file-based storage
- **API integration**: Logcomex API with rate limiting and pagination 