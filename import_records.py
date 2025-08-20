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
from datetime import datetime, timedelta
from decimal import Decimal

# Configuration
DATABASE_FILE = "importer.db"
API_KEY = ""
API_URL = "https://bi-api.logcomex.io/api/v1/details"
MONTHS_BACK = int(os.getenv("DEFAULT_MONTHS_BACK", "6"))
IMPORTER_NAME = os.getenv("IMPORTER_NAME", "CISCO SYSTEMS DE MEXICO S DE RL DE CV")

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

def fetch_data_from_api(start_date, end_date):
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
                "value": IMPORTER_NAME
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

def insert_records(records):
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
                departure_freight_usd_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                float(record.get("departure_freight_usd_value", 0) or 0)
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
    
    # Calculate date range (use past dates to avoid plan restrictions)
    end_date = datetime.now().date() - timedelta(days=7)  # 1 week ago
    start_date = end_date - timedelta(days=MONTHS_BACK * 30)  # N months ago
    
    print(f"Fetching data from {start_date} to {end_date}")
    print(f"Time range: Last {MONTHS_BACK} months")
    print(f"Importer filter: {IMPORTER_NAME}")
    
    # Clear existing data
    print("Clearing existing data...")
    clear_existing_data()
    
    # Fetch data from API
    print("Fetching data from Logcomex API...")
    records = fetch_data_from_api(start_date, end_date)
    
    if not records:
        print("No records fetched from API")
        return
    
    print(f"Fetched {len(records)} records from API")
    
    # Insert into database
    print("Inserting records into database...")
    inserted = insert_records(records)
    
    print(f"\nRESULTS:")
    print(f"  Records fetched: {len(records)}")
    print(f"  Records inserted: {inserted}")
    print(f"  Database: {DATABASE_FILE}")
    
    # Show sample data
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT importer_name, departure_goods_usd_value FROM import_records LIMIT 3")
    samples = cursor.fetchall()
    
    if samples:
        print(f"\nSample records:")
        for i, (name, value) in enumerate(samples, 1):
            print(f"  {i}. {name}: ${value}")
    
    conn.close()
    print(f"\nImport completed successfully!")

if __name__ == "__main__":
    main() 