# Logcomex Importer - Enhanced Interactive Version

Two comprehensive, self-contained Python scripts for importing and summarizing Mexican import data from Logcomex API with interactive features and business intelligence capabilities.

## Files

- **`import_records.py`** - Interactive data importer with date/importer selection and CSV export
- **`import_summarize.py`** - Business intelligence summarizer with KPI calculations and webhook integration
- **`importer.db`** - SQLite database with comprehensive analytics schema
- **`import_records_*.csv`** - Timestamped CSV exports of import records
- **`import_summaries_*.csv`** - Timestamped CSV exports of business summaries
- **`.gitignore`** - Git ignore file
- **`venv/`** - Python virtual environment (if using virtual environment)

## Quick Start

1. **Install Python dependencies** (no external packages required - uses built-in libraries only)

2. **Import data from Logcomex API:**
   ```bash
   python import_records.py
   ```
   - Interactive date range selection (default: 6 months starting 3 months ago)
   - Choose specific importer or import all
   - Optional database clearing
   - CSV export functionality

3. **Generate business intelligence summaries:**
   ```bash
   python import_summarize.py
   ```
   - Webhook trigger options
   - Interactive date range selection
   - Optional summary clearing
   - CSV export of KPI analytics
   - Automatic business opportunity scoring

## Interactive Features

### Data Import (`import_records.py`)
- **📅 Date Range Selection**: Choose default (6 months starting 3 months ago) or custom dates
- **🏢 Importer Selection**: Import all companies or filter by specific importer name
- **🗄️ Database Management**: Option to keep or clear existing records
- **📊 CSV Export**: Export import records with timestamps
- **⚡ Real-time Progress**: Live progress tracking during API calls

### Business Summarization (`import_summarize.py`)
- **🚀 Webhook Control**: Choose to trigger or skip API webhooks
- **📈 KPI Calculations**: 50+ business intelligence metrics per importer
- **🎯 Opportunity Scoring**: Automated business opportunity assessment (1-10 scale)
- **📊 Analytics Export**: CSV export of all calculated summaries
- **🌐 Multi-endpoint Webhooks**: Test and production webhook integration

## Configuration

### API Configuration
Update the configuration constants in both scripts:

**In `import_records.py` (lines ~15-20):**
```python
DATABASE_FILE = "importer.db"
API_KEY = "your-api-key-here"  # Replace with your Logcomex API key
API_URL = "https://bi-api.logcomex.io/api/v1/details"
SIGNATURE_API_URL = "https://bi-api.logcomex.io/api/v1/details/signature"
MONTHS_BACK = 6  # Default time range for interactive selection
```

**In `import_summarize.py` (lines ~10-15):**
```python
DATABASE_FILE = "importer.db"
MONTHS_BACK = 6  # Default time range for interactive selection
```

### Webhook Configuration
Webhook URLs are configured in `import_summarize.py`:
```python
# Webhook endpoints (lines ~650-660)
test_url = "http://68.183.85.45:5678/webhook-test/1538d58f-70e9-4879-84ee-4ac85b7a755c"
prod_url = "http://68.183.85.45:5678/webhook/1538d58f-70e9-4879-84ee-4ac85b7a755c"
```

**⚠️ Important**: Update the `API_KEY` in `import_records.py` with your actual Logcomex API credentials.

## Key Features

### 📊 Comprehensive Analytics
- **50+ KPI Metrics**: Transport modes, customs regimes, port usage, HS codes, origin countries
- **Business Intelligence**: Automated scoring for cross-border potential, ocean freight opportunities
- **Supply Chain Analysis**: Custom broker usage patterns, incoterm preferences
- **Risk Assessment**: Business opportunity scoring (1-10 scale) based on multiple factors

### 🔄 Data Management
- **SQLite Database**: Robust schema with 20+ columns for import records and 50+ for summaries
- **Incremental Updates**: Option to preserve existing data or start fresh
- **Data Export**: Timestamped CSV files for external analysis
- **Data Validation**: Built-in validation for dates, API responses, and calculations

### 🌐 API Integration
- **Logcomex API**: Full integration with Mexican import data API
- **Rate Limiting**: Built-in request throttling to respect API limits
- **Pagination Handling**: Automatic handling of large datasets
- **Error Recovery**: Robust error handling with retry mechanisms
- **Webhook Integration**: Automatic data transmission to external systems

### 💡 User Experience
- **Interactive Menus**: Intuitive command-line interface with numbered options
- **Progress Tracking**: Real-time progress indicators during long operations  
- **Input Validation**: Comprehensive validation for all user inputs
- **Clear Feedback**: Detailed success/error messages and operation summaries

## Database Schema

### Import Records Table (`import_records`)
- **Basic Info**: ID, dispatch date, pedimento, importer details
- **Logistics**: Transport modes, customs offices, weights, values
- **Trade Data**: HS codes, origin countries, incoterms, broker information
- **Analytics**: Calculated fields for business intelligence

### Import Summaries Table (`import_summaries`)
- **Company Profile**: Importer name, RFC, total shipments, freight values
- **Operational Metrics**: Transport/port/regime percentages, customs usage
- **Product Analysis**: HS code distributions, weight analytics
- **Business Intelligence**: Opportunity scores, cross-border potential, supply chain ratings
- **Geographic Data**: Origin country breakdowns, USA sourcing indicators

## Generated Files

- **`import_records_YYYYMMDD_HHMMSS.csv`**: Raw import data export
- **`import_summaries_YYYYMMDD_HHMMSS.csv`**: Business intelligence summaries
- **`importer.db`**: SQLite database with all processed data

## LLM Integration Support

The scripts generate comprehensive datasets suitable for Large Language Model analysis:

**For Commercial Summary Generation:**
```
Use the data from import_summaries.csv to generate executive business summaries for Mexican importers, focusing on:
- Business opportunity assessment scores
- Cross-border trade potential 
- Supply chain optimization opportunities
- Market positioning and competitive analysis
- Risk factors and regulatory compliance patterns
```

## Technical Details

- **Dependencies**: Uses only Python built-in libraries (sqlite3, requests, json, csv, datetime)
- **Platform**: Cross-platform compatibility (Windows, macOS, Linux)
- **Database**: SQLite for zero-configuration data persistence
- **API Rate Limits**: Built-in throttling and retry logic
- **Memory Efficient**: Processes large datasets in chunks
- **Error Handling**: Comprehensive exception handling and user feedback

## Troubleshooting

**Common Issues:**
- **API Key Errors**: Ensure your Logcomex API key is valid and has sufficient quota
- **Date Range Issues**: Mexican import data has ~7-day reporting delays
- **Large Dataset Timeouts**: Use smaller date ranges for initial testing
- **CSV Export Failures**: Check file permissions in working directory

**Performance Tips:**
- Start with 1-month date ranges for testing
- Use importer filtering for focused analysis
- Export to CSV before processing large summaries
- Monitor API rate limits during bulk operations 