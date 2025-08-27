#!/usr/bin/env python3
"""
Minimalistic Import Records Script
Self-contained - all dependencies included
"""

import os
import sqlite3
import requests
import json
import time
import csv
from datetime import datetime, timedelta
from decimal import Decimal
from collections import Counter
import sys

# Configuration
DATABASE_FILE = "importer.db"
API_KEY = "n8HPUtVG16Ea5mYi5jvlOYqFyObzTd1ZvXMTjU8s"
API_URL = "https://bi-api.logcomex.io/api/v1/details"
MONTHS_BACK = int(os.getenv("DEFAULT_MONTHS_BACK", "6"))
# IMPORTER_NAME = os.getenv("IMPORTER_NAME", "CISCO SYSTEMS DE MEXICO S DE RL DE CV")
IMPORTER_NAME = os.getenv("IMPORTER_NAME", "DANFOSS INDUSTRIES SA DE CV")


def get_date_input(date_type):
    """Get date input from user with month, day, year"""
    print(f"\n📅 Enter {date_type} date:")
    
    while True:
        try:
            year = int(input(f"  Year (e.g., 2024): ").strip())
            if year < 2020 or year > 2025:
                print("  ❌ Please enter a valid year between 2020 and 2025")
                continue
                
            month = int(input(f"  Month (1-12): ").strip())
            if month < 1 or month > 12:
                print("  ❌ Please enter a valid month (1-12)")
                continue
                
            day = int(input(f"  Day (1-31): ").strip())
            if day < 1 or day > 31:
                print("  ❌ Please enter a valid day (1-31)")
                continue
                
            # Create and validate the date
            date = datetime(year, month, day).date()
            return date
            
        except ValueError as e:
            print(f"  ❌ Invalid date: {e}. Please try again.")
        except Exception as e:
            print(f"  ❌ Error: {e}. Please try again.")


def get_date_range():
    """Get date range from user input"""
    print("\n📅 DATE RANGE SELECTION")
    print("=" * 30)
    print("1. Use default (6 months starting 3 months ago)")
    print("2. Enter custom date range")
    
    while True:
        choice = input("\nSelect option (1-2): ").strip()
        
        if choice == "1":
            # Default option: 6 months starting 3 months ago
            end_date = datetime.now().date() - timedelta(days=90)  # 3 months ago
            start_date = end_date - timedelta(days=MONTHS_BACK * 30)  # 6 months before that
            break
            
        elif choice == "2":
            # Custom date range
            print("\n🗓️  Custom Date Range Entry")
            start_date = get_date_input("START")
            end_date = get_date_input("END")
            
            if start_date >= end_date:
                print("❌ Start date must be before end date. Please try again.")
                continue
                
            # Check if dates are too recent (API restrictions)
            today = datetime.now().date()
            if end_date > today - timedelta(days=7):
                print("⚠️  Warning: End date is less than 7 days ago. API may have restrictions.")
                confirm = input("Continue anyway? (y/n): ").strip().lower()
                if confirm != 'y':
                    continue
            
            break
            
        else:
            print("❌ Invalid choice. Please select 1 or 2.")
    
    # Show selected range
    duration = (end_date - start_date).days
    print(f"\n✅ Selected date range:")
    print(f"   From: {start_date.strftime('%B %d, %Y')} ({start_date})")
    print(f"   To: {end_date.strftime('%B %d, %Y')} ({end_date})")
    print(f"   Duration: {duration} days ({duration/30.44:.1f} months)")
    
    # Confirm
    confirm = input(f"\nProceed with this date range? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Operation cancelled.")
        return None, None
    
    return start_date, end_date


def get_importer_name():
    """Get importer name from user input"""
    print("\n🏢 IMPORTER NAME SELECTION")
    print("=" * 30)
    print("1. Use default (DANFOSS INDUSTRIES SA DE CV)")
    print("2. Use Cisco (CISCO SYSTEMS DE MEXICO S DE RL DE CV)")
    print("3. Enter custom importer name")
    
    default_options = {
        "1": "DANFOSS INDUSTRIES SA DE CV",
        "2": "CISCO SYSTEMS DE MEXICO S DE RL DE CV"
    }
    
    while True:
        choice = input("\nSelect option (1-3): ").strip()
        
        if choice in ["1", "2"]:
            importer_name = default_options[choice]
            break
            
        elif choice == "3":
            # Custom importer name
            print("\n📝 Enter Custom Importer Name")
            while True:
                importer_name = input("Importer name (exact match): ").strip()
                if importer_name:
                    break
                else:
                    print("❌ Please enter a valid importer name.")
            break
            
        else:
            print("❌ Invalid choice. Please select 1, 2, or 3.")
    
    print(f"\n✅ Selected importer: {importer_name}")
    
    # Confirm
    confirm = input(f"\nProceed with this importer? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Operation cancelled.")
        return None
    
    return importer_name


def export_to_csv():
    """Export import_records table to CSV"""
    print("\n📊 CSV EXPORT OPTIONS")
    print("=" * 20)
    print("1. Export all records")
    print("2. Skip CSV export")
    
    while True:
        choice = input("\nSelect option (1-2): ").strip()
        if choice == "1":
            break
        elif choice == "2":
            return
        else:
            print("❌ Invalid choice. Please select 1 or 2.")
    
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Get all records
        cursor.execute("SELECT * FROM import_records")
        records = cursor.fetchall()
        
        if not records:
            print("No records to export")
            conn.close()
            return
        
        # Get column names
        cursor.execute("PRAGMA table_info(import_records)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"import_records_{timestamp}.csv"
        
        # Export to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(columns)
            
            # Write data
            writer.writerows(records)
        
        conn.close()
        print(f"✅ Exported {len(records)} records to: {filename}")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")


def create_database():
    """Create SQLite database and tables"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Create import_records table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS import_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dispatch_date TEXT,
        importer_name TEXT,
        importer_address TEXT,
        supplier_name TEXT,
        supplier_address TEXT,
        origin_destination_country TEXT,
        buyer_seller_country TEXT,
        entry_exit_transport TEXT,
        departure_hscodes TEXT,
        departure_gross_weight REAL,
        departure_goods_usd_value REAL,
        dispatch_customs TEXT,
        entry_customs TEXT,
        custom_broker_id TEXT,
        customs_regime TEXT,
        customs_regime_id TEXT,
        declaration_type TEXT,
        dispatch_customs_state TEXT,
        importer_id TEXT,
        incoterm TEXT,
        container_type TEXT,
        teus_qty REAL,
        departure_insurance_usd_value REAL,
        departure_freight_usd_value REAL,
        custom_brokers_used TEXT,
        top_custom_broker_id TEXT,
        pct_top_custom_broker_id REAL,
        num_custom_brokers_used INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    conn.commit()
    conn.close()

def clear_existing_data():
    """Clear existing import records"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM import_records")
    conn.commit()
    conn.close()

def fetch_data_from_api(start_date, end_date, importer_name):
    """Fetch data from Logcomex API"""
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY,
        'product-signature': 'mexico-import-logistic'
    }
    
    payload = {
        "filters": [
            {
                "field": "dispatch_date",
                "value": [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")]
            },
            {
                "field": "importer_name",
                "value": importer_name
            }
        ],
        "page": 1,
        "size": 100
    }
    
    all_records = []
    page = 1
    
    while True:
        payload["page"] = page
        print(f"Fetching page {page}...")
        
        try:
            response = requests.post(API_URL, headers=headers, json=payload)
            print(f"  Response status: {response.status_code}")
            if response.status_code != 200:
                print(f"  Response text: {response.text}")
            response.raise_for_status()
            data = response.json()
            
            # FIX: Handle the fact that data['data'] is a dict, not a list
            data_section = data.get("data", {})
            if isinstance(data_section, dict):
                records = list(data_section.values())  # Convert dict values to list
            else:
                records = data_section if data_section else []
            
            if not records:
                break
                
            all_records.extend(records)
            print(f"  Got {len(records)} records")
            
            # Rate limiting
            time.sleep(1)
            page += 1
            
            # Check if more pages
            if len(records) < 100:
                break
                
        except Exception as e:
            print(f"Error fetching data: {e}")
            break
    
    return all_records

def process_broker_data(records):
    """Process broker data to extract top 5 brokers and statistics"""
    
    # Extract broker IDs from records
    brokers = [r.get("custom_broker_id") for r in records if r.get("custom_broker_id")]
    broker_counts = Counter(brokers)
    
    # Get total number of unique brokers
    num_brokers = len(broker_counts)
    
    # Get top 5 brokers by frequency
    top_brokers = broker_counts.most_common(5)
    
    # Format top brokers as comma-separated string (e.g., "1973, 1893, 1983, 9831, 1995")
    custom_brokers_used = ", ".join([str(broker_id) for broker_id, _ in top_brokers])
    
    # Get top broker and its percentage
    top_broker_id = top_brokers[0][0] if top_brokers else ""
    pct_top_broker = round((top_brokers[0][1] / len(records) * 100), 2) if top_brokers else 0.0
    
    return {
        'custom_brokers_used': custom_brokers_used,
        'top_custom_broker_id': top_broker_id,
        'pct_top_custom_broker_id': pct_top_broker,
        'num_custom_brokers_used': num_brokers
    }

def insert_records(records, broker_stats):
    """Insert records into database"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    inserted = 0
    for record in records:
        try:
            # Skip if record is not a dictionary
            if not isinstance(record, dict):
                print(f"Skipping non-dict record: {type(record)} - {record}")
                continue
                
            cursor.execute("""
            INSERT INTO import_records (
                dispatch_date, importer_name, importer_address, supplier_name, supplier_address,
                origin_destination_country, buyer_seller_country, entry_exit_transport,
                departure_hscodes, departure_gross_weight, departure_goods_usd_value,
                dispatch_customs, entry_customs, custom_broker_id, customs_regime,
                customs_regime_id, declaration_type, dispatch_customs_state, importer_id,
                incoterm, container_type, teus_qty, departure_insurance_usd_value,
                departure_freight_usd_value, custom_brokers_used, top_custom_broker_id,
                pct_top_custom_broker_id, num_custom_brokers_used
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get("dispatch_date"),
                record.get("importer_name"),
                record.get("importer_address"),
                record.get("supplier_name"),
                record.get("supplier_address"),
                record.get("origin_destination_country"),
                record.get("buyer_seller_country"),
                record.get("entry_exit_transport"),
                record.get("departure_hscodes"),
                float(record.get("departure_gross_weight", 0) or 0),
                float(record.get("departure_goods_usd_value", 0) or 0),
                record.get("dispatch_customs"),
                record.get("entry_customs"),
                record.get("custom_broker_id"),
                record.get("customs_regime"),
                record.get("customs_regime_id"),
                record.get("declaration_type"),
                record.get("dispatch_customs_state"),
                record.get("importer_id"),
                record.get("incoterm"),
                record.get("container_type"),
                float(record.get("teus_qty", 0) or 0),
                float(record.get("departure_insurance_usd_value", 0) or 0),
                float(record.get("departure_freight_usd_value", 0) or 0),
                broker_stats['custom_brokers_used'],
                broker_stats['top_custom_broker_id'],
                broker_stats['pct_top_custom_broker_id'],
                broker_stats['num_custom_brokers_used']
            ))
            inserted += 1
        except Exception as e:
            print(f"Error inserting record: {e}")
    
    conn.commit()
    conn.close()
    return inserted

def main():
    print("LOGCOMEX IMPORT RECORDS")
    print("=" * 40)
    
    # Create database
    print("Setting up database...")
    create_database()
    
    # Check existing data
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM import_records")
    existing_count = cursor.fetchone()[0]
    conn.close()
    
    if existing_count > 0:
        print(f"Found {existing_count} existing records in database")
        print("\n🗂️  DATABASE OPTIONS")
        print("=" * 20)
        print("1. Keep existing data and add new records")
        print("2. Clear database and start fresh")
        
        while True:
            choice = input("\nSelect option (1-2): ").strip()
            if choice == "1":
                clear_db = False
                break
            elif choice == "2":
                clear_db = True
                break
            else:
                print("❌ Invalid choice. Please select 1 or 2.")
    else:
        print("No existing records found")
        clear_db = False
    
    # Get date range from user
    start_date, end_date = get_date_range()
    if not start_date or not end_date:
        return
    
    # Get importer name from user
    importer_name = get_importer_name()
    if not importer_name:
        return
    
    print(f"\n🔍 FETCH CONFIGURATION")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Importer: {importer_name}")
    print(f"Database action: {'Clear and refresh' if clear_db else 'Append new records'}")
    
    # Clear existing data if requested
    if clear_db:
        print("\nClearing existing data...")
        clear_existing_data()
    else:
        print(f"\nKeeping existing {existing_count} records...")
    
    # Fetch data from API
    print("Fetching data from Logcomex API...")
    records = fetch_data_from_api(start_date, end_date, importer_name)
    
    if not records:
        print("No records fetched from API")
        return
    
    print(f"Fetched {len(records)} records from API")
    
    # Process broker data
    broker_stats = process_broker_data(records)
    print(f"Processed broker data:")
    print(f"  Total unique brokers: {broker_stats['num_custom_brokers_used']}")
    print(f"  Top broker ID: {broker_stats['top_custom_broker_id']}")
    print(f"  Percentage of records with top broker: {broker_stats['pct_top_custom_broker_id']}%")
    
    # Insert into database
    print("Inserting records into database...")
    inserted = insert_records(records, broker_stats)
    
    # Get final count
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM import_records")
    final_count = cursor.fetchone()[0]
    
    print(f"\nRESULTS:")
    print(f"  Records fetched: {len(records)}")
    print(f"  Records inserted: {inserted}")
    print(f"  Total records in database: {final_count}")
    print(f"  Database: {DATABASE_FILE}")
    
    # Show broker information
    print(f"\n📊 BROKER ANALYSIS:")
    print(f"  Total unique brokers: {broker_stats['num_custom_brokers_used']}")
    print(f"  Top 5 brokers: {broker_stats['custom_brokers_used']}")
    print(f"  Top broker ID: {broker_stats['top_custom_broker_id']}")
    print(f"  Percentage with top broker: {broker_stats['pct_top_custom_broker_id']}%")
    
    # Show sample data
    cursor.execute("SELECT importer_name, departure_goods_usd_value, custom_brokers_used, num_custom_brokers_used FROM import_records LIMIT 3")
    samples = cursor.fetchall()
    
    if samples:
        print(f"\nSample records:")
        for i, (name, value, brokers, num_brokers) in enumerate(samples, 1):
            print(f"  {i}. {name}: ${value} | Brokers: {brokers} ({num_brokers} total)")
    
    conn.close()
    print(f"\nImport completed successfully!")
    
    # Offer CSV export
    export_to_csv()


if __name__ == "__main__":
    # Test broker processing logic
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("🧪 Testing broker processing logic...")
        
        # Test data
        test_records = [
            {"custom_broker_id": "1973"},
            {"custom_broker_id": "1893"},
            {"custom_broker_id": "1973"},
            {"custom_broker_id": "1983"},
            {"custom_broker_id": "9831"},
            {"custom_broker_id": "1995"},
            {"custom_broker_id": "1973"},
            {"custom_broker_id": "1893"},
            {"custom_broker_id": "2000"},
            {"custom_broker_id": "1973"}
        ]
        
        broker_stats = process_broker_data(test_records)
        print(f"Test results:")
        print(f"  Total unique brokers: {broker_stats['num_custom_brokers_used']}")
        print(f"  Top 5 brokers: {broker_stats['custom_brokers_used']}")
        print(f"  Top broker ID: {broker_stats['top_custom_broker_id']}")
        print(f"  Percentage with top broker: {broker_stats['pct_top_custom_broker_id']}%")
        print("✅ Test completed!")
        sys.exit(0)
    
    main() 