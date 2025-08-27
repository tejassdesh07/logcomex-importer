#!/usr/bin/env python3
"""
Minimalistic Import Summarize Script
Self-contained - all business logic included
"""

import os
import sqlite3
import csv
from datetime import datetime, timedelta
from collections import Counter

# Configuration
MONTHS_BACK = 6

# Configuration
DATABASE_FILE = "importer.db"

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
                print("⚠️  Warning: End date is less than 7 days ago. Data may be incomplete.")
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


def create_summary_table():
    """Create summary table if not exists"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS import_summaries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        importer_name TEXT UNIQUE,
        rfc TEXT,
        total_pedimentos_last_6_months INTEGER,
        total_freight_usd_value REAL,
        avg_freight_usd_per_shipment REAL,
        customs_offices_used TEXT,
        pct_shipments_key_locations REAL,
        pct_regime_A1 REAL,
        pct_regime_F4 REAL,
        pct_regime_IN REAL,
        pct_regime_A3 REAL,
        pct_regime_AF REAL,
        pct_transport_carretero REAL,
        pct_transport_aereo REAL,
        pct_transport_maritimo REAL,
        pct_transport_not_declared REAL,
        pct_port_NUEVO_LAREDO REAL,
        pct_port_COLOMBIA_NL REAL,
        pct_port_MONTERREY_AIRPORT REAL,
        pct_port_MANZANILLO REAL,
        pct_port_PUEBLA REAL,
        pct_port_OTHERS REAL,
        pct_hs_84 REAL,
        pct_hs_85 REAL,
        pct_hs_90 REAL,
        pct_hs_73 REAL,
        pct_hs_74 REAL,
        pct_hs_OTROS REAL,
        is_origin_usa INTEGER,
        is_candidate_for_crossborder INTEGER,
        pct_incoterm_DAP REAL,
        pct_incoterm_EXW REAL,
        pct_incoterm_FCA REAL,
        pct_incoterm_OTROS REAL,
        custom_brokers_used TEXT,
        top_custom_broker_id TEXT,
        pct_top_custom_broker_id REAL,
        num_custom_brokers_used INTEGER,
        pct_origin_TAIWAN REAL,
        pct_origin_VIETNAM REAL,
        pct_origin_CHINA REAL,
        pct_origin_USA REAL,
        pct_origin_GERMANY REAL,
        pct_origin_DENMARK REAL,
        pct_origin_FRANCE REAL,
        pct_origin_OTROS REAL,
        last_import_date TEXT,
        first_import_date TEXT,
        total_weight_kg REAL,
        avg_weight_per_shipment REAL,
        business_opportunity_score INTEGER,
        crossborder_potential INTEGER,
        ocean_freight_potential INTEGER,
        supply_chain_potential INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    conn.commit()
    conn.close()

def clear_summaries():
    """Clear existing summaries"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM import_summaries")
    conn.commit()
    conn.close()

def get_importers():
    """Get list of unique importers"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT importer_name FROM import_records WHERE importer_name IS NOT NULL")
    importers = [row[0] for row in cursor.fetchall()]
    conn.close()
    return importers

def get_importer_records(importer_name, start_date, end_date):
    """Get all records for a specific importer within date range"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    SELECT * FROM import_records 
    WHERE importer_name = ? AND dispatch_date >= ? AND dispatch_date <= ?
    """, (importer_name, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
    
    records = cursor.fetchall()
    conn.close()
    return records

def calculate_summary(importer_name, records):
    """Calculate all KPIs for an importer"""
    if not records:
        return None
    
    total_records = len(records)
    
    # Basic metrics
    total_freight = round(sum(float(r[11] or 0) for r in records), 2)  # departure_goods_usd_value
    avg_freight = round(total_freight / total_records if total_records > 0 else 0, 2)
    total_weight = round(sum(float(r[10] or 0) for r in records), 2)  # departure_gross_weight
    avg_weight = round(total_weight / total_records if total_records > 0 else 0, 2)
    
    # Extract RFC from importer_id
    rfc = records[0][19] if records[0][19] else ""  # importer_id
    
    # Date ranges
    dates = [r[1] for r in records if r[1]]  # dispatch_date
    first_date = min(dates) if dates else ""
    last_date = max(dates) if dates else ""
    
    # Regime percentages
    regimes = [r[16] or "" for r in records]  # customs_regime_id
    regime_counts = Counter(regimes)
    pct_regime_A1 = round((regime_counts.get("A1", 0) / total_records) * 100, 2)
    pct_regime_F4 = round((regime_counts.get("F4", 0) / total_records) * 100, 2)
    pct_regime_IN = round((regime_counts.get("IN", 0) / total_records) * 100, 2)
    pct_regime_A3 = round((regime_counts.get("A3", 0) / total_records) * 100, 2)
    pct_regime_AF = round((regime_counts.get("AF", 0) / total_records) * 100, 2)
    
    # Transport percentages
    transports = [r[8] or "" for r in records]  # entry_exit_transport
    transport_counts = {"CARRETERO": 0, "AEREO": 0, "MARITIMO": 0, "NOT_DECLARED": 0}
    
    for transport in transports:
        transport_upper = transport.upper()
        if "CARRETERO" in transport_upper:
            transport_counts["CARRETERO"] += 1
        elif "AEREO" in transport_upper or "AÉREO" in transport_upper:
            transport_counts["AEREO"] += 1
        elif "MARITIMO" in transport_upper or "MARÍTIMO" in transport_upper:
            transport_counts["MARITIMO"] += 1
        else:
            transport_counts["NOT_DECLARED"] += 1
    
    pct_transport_carretero = round((transport_counts["CARRETERO"] / total_records) * 100, 2)
    pct_transport_aereo = round((transport_counts["AEREO"] / total_records) * 100, 2)
    pct_transport_maritimo = round((transport_counts["MARITIMO"] / total_records) * 100, 2)
    pct_transport_not_declared = round((transport_counts["NOT_DECLARED"] / total_records) * 100, 2)
    
    # Port percentages
    port_counts = {"NUEVO_LAREDO": 0, "COLOMBIA_NL": 0, "MONTERREY_AIRPORT": 0, "MANZANILLO": 0, "PUEBLA": 0, "OTHERS": 0}
    
    for record in records:
        entry_customs = (record[13] or "").upper()  # entry_customs
        dispatch_customs = (record[12] or "").upper()  # dispatch_customs
        customs_combined = entry_customs + " " + dispatch_customs
        
        if "NUEVO LAREDO, NUEVO LAREDO, TAMAULIPAS" in customs_combined:
            port_counts["NUEVO_LAREDO"] += 1
        elif "MONTERREY, GENERAL MARIANO ESCOBEDO, NUEVO LEON" in customs_combined:
            port_counts["COLOMBIA_NL"] += 1
        elif "AEROPUERTO INTERNACIONAL GENERAL MARIANO ESCOBEDO, APODACA, NUEVO LEON" in customs_combined:
            port_counts["MONTERREY_AIRPORT"] += 1
        elif "MANZANILLO, MANZANILLO, COLIMA" in customs_combined:
            port_counts["MANZANILLO"] += 1
        elif "PUEBLA, HEROICA PUEBLA DE ZARAGOZA, PUEBLA" in customs_combined:
            port_counts["PUEBLA"] += 1
        else:
            port_counts["OTHERS"] += 1
    
    pct_port_NUEVO_LAREDO = round((port_counts["NUEVO_LAREDO"] / total_records) * 100, 2)
    pct_port_COLOMBIA_NL = round((port_counts["COLOMBIA_NL"] / total_records) * 100, 2)
    pct_port_MONTERREY_AIRPORT = round((port_counts["MONTERREY_AIRPORT"] / total_records) * 100, 2)
    pct_port_MANZANILLO = round((port_counts["MANZANILLO"] / total_records) * 100, 2)
    pct_port_PUEBLA = round((port_counts["PUEBLA"] / total_records) * 100, 2)
    pct_port_OTHERS = round((port_counts["OTHERS"] / total_records) * 100, 2)
    
    # HS Code percentages
    hs_counts = {"84": 0, "85": 0, "90": 0, "73": 0, "74": 0, "OTROS": 0}
    
    for record in records:
        hs_code = record[9] or ""  # departure_hscodes
        if hs_code and len(hs_code) >= 2:
            hs_prefix = hs_code[:2]
            if hs_prefix in ["84", "85", "90", "73", "74"]:
                hs_counts[hs_prefix] += 1
            else:
                hs_counts["OTROS"] += 1
        else:
            hs_counts["OTROS"] += 1
    
    pct_hs_84 = round((hs_counts["84"] / total_records) * 100, 2)
    pct_hs_85 = round((hs_counts["85"] / total_records) * 100, 2)
    pct_hs_90 = round((hs_counts["90"] / total_records) * 100, 2)
    pct_hs_73 = round((hs_counts["73"] / total_records) * 100, 2)
    pct_hs_74 = round((hs_counts["74"] / total_records) * 100, 2)
    pct_hs_OTROS = round((hs_counts["OTROS"] / total_records) * 100, 2)
    
    # Origin country percentages
    countries = [r[6] or "" for r in records]  # origin_destination_country
    country_counts = {"TAIWAN": 0, "VIETNAM": 0, "CHINA": 0, "USA": 0, "GERMANY": 0, "DENMARK": 0, "FRANCE": 0, "OTROS": 0}
    
    for country in countries:
        country_upper = country.upper()
        if "TAIWAN" in country_upper:
            country_counts["TAIWAN"] += 1
        elif "VIETNAM" in country_upper:
            country_counts["VIETNAM"] += 1
        elif "CHINA" in country_upper:
            country_counts["CHINA"] += 1
        elif "ESTADOS UNIDOS" in country_upper or "USA" in country_upper:
            country_counts["USA"] += 1
        elif "ALEMANIA" in country_upper or "GERMANY" in country_upper:
            country_counts["GERMANY"] += 1
        elif "DINAMARCA" in country_upper or "DENMARK" in country_upper:
            country_counts["DENMARK"] += 1
        elif "FRANCIA" in country_upper or "FRANCE" in country_upper:
            country_counts["FRANCE"] += 1
        else:
            country_counts["OTROS"] += 1
    
    pct_origin_TAIWAN = round((country_counts["TAIWAN"] / total_records) * 100, 2)
    pct_origin_VIETNAM = round((country_counts["VIETNAM"] / total_records) * 100, 2)
    pct_origin_CHINA = round((country_counts["CHINA"] / total_records) * 100, 2)
    pct_origin_USA = round((country_counts["USA"] / total_records) * 100, 2)
    pct_origin_GERMANY = round((country_counts["GERMANY"] / total_records) * 100, 2)
    pct_origin_DENMARK = round((country_counts["DENMARK"] / total_records) * 100, 2)
    pct_origin_FRANCE = round((country_counts["FRANCE"] / total_records) * 100, 2)
    pct_origin_OTROS = round((country_counts["OTROS"] / total_records) * 100, 2)
    
    # Incoterm percentages
    incoterms = [r[20] or "" for r in records]  # incoterm
    incoterm_counts = Counter(incoterms)
    pct_incoterm_DAP = round((incoterm_counts.get("DAP", 0) / total_records) * 100, 2)
    pct_incoterm_EXW = round((incoterm_counts.get("EXW", 0) / total_records) * 100, 2)
    pct_incoterm_FCA = round((incoterm_counts.get("FCA", 0) / total_records) * 100, 2)
    pct_incoterm_OTROS = round(((total_records - incoterm_counts.get("DAP", 0) - incoterm_counts.get("EXW", 0) - incoterm_counts.get("FCA", 0)) / total_records) * 100, 2)
    
    # Custom brokers
    brokers = [r[14] or "" for r in records if r[14]]  # custom_broker_id
    broker_counts = Counter(brokers)
    num_brokers = len(broker_counts)
    
    # Get top 5 brokers by frequency
    top_brokers = broker_counts.most_common(5)
    
    # Format top brokers as comma-separated string (e.g., "1973, 1893, 1983, 9831, 1995")
    custom_brokers_used = ", ".join([str(broker_id) for broker_id, _ in top_brokers])
    
    top_broker = broker_counts.most_common(1)[0] if broker_counts else ("", 0)
    pct_top_broker = round((top_broker[1] / total_records) * 100, 2) if top_broker[1] > 0 else 0
    
    # Business intelligence flags
    is_origin_usa = 1 if pct_origin_USA > 50 else 0
    is_candidate_for_crossborder = 1 if is_origin_usa else 0
    
    # Business opportunity score (1-10)
    score = 1
    if total_records > 10: score += 1
    if total_freight > 50000: score += 1
    if num_brokers > 3: score += 1
    if pct_regime_A1 > 50: score += 1
    if pct_transport_carretero > 50: score += 1
    if is_origin_usa: score += 2
    if total_records > 50: score += 1
    if total_freight > 200000: score += 1
    
    business_opportunity_score = min(score, 10)
    
    # Potential scores
    crossborder_potential = 1 if is_candidate_for_crossborder else 0
    ocean_freight_potential = 1 if pct_transport_maritimo > 30 else 0
    supply_chain_potential = 1 if (total_records > 20 and num_brokers > 5) else 0
    
    return {
        'importer_name': importer_name,
        'rfc': rfc,
        'total_pedimentos_last_6_months': total_records,
        'total_freight_usd_value': total_freight,
        'avg_freight_usd_per_shipment': avg_freight,
        'customs_offices_used': str(num_brokers),
        'pct_shipments_key_locations': 0.0,
        'pct_regime_A1': pct_regime_A1,
        'pct_regime_F4': pct_regime_F4,
        'pct_regime_IN': pct_regime_IN,
        'pct_regime_A3': pct_regime_A3,
        'pct_regime_AF': pct_regime_AF,
        'pct_transport_carretero': pct_transport_carretero,
        'pct_transport_aereo': pct_transport_aereo,
        'pct_transport_maritimo': pct_transport_maritimo,
        'pct_transport_not_declared': pct_transport_not_declared,
        'pct_port_NUEVO_LAREDO': pct_port_NUEVO_LAREDO,
        'pct_port_COLOMBIA_NL': pct_port_COLOMBIA_NL,
        'pct_port_MONTERREY_AIRPORT': pct_port_MONTERREY_AIRPORT,
        'pct_port_MANZANILLO': pct_port_MANZANILLO,
        'pct_port_PUEBLA': pct_port_PUEBLA,
        'pct_port_OTHERS': pct_port_OTHERS,
        'pct_hs_84': pct_hs_84,
        'pct_hs_85': pct_hs_85,
        'pct_hs_90': pct_hs_90,
        'pct_hs_73': pct_hs_73,
        'pct_hs_74': pct_hs_74,
        'pct_hs_OTROS': pct_hs_OTROS,
        'is_origin_usa': is_origin_usa,
        'is_candidate_for_crossborder': is_candidate_for_crossborder,
        'pct_incoterm_DAP': pct_incoterm_DAP,
        'pct_incoterm_EXW': pct_incoterm_EXW,
        'pct_incoterm_FCA': pct_incoterm_FCA,
        'pct_incoterm_OTROS': pct_incoterm_OTROS,
        'custom_brokers_used': custom_brokers_used,
        'top_custom_broker_id': top_broker[0],
        'pct_top_custom_broker_id': pct_top_broker,
        'num_custom_brokers_used': num_brokers,
        'pct_origin_TAIWAN': pct_origin_TAIWAN,
        'pct_origin_VIETNAM': pct_origin_VIETNAM,
        'pct_origin_CHINA': pct_origin_CHINA,
        'pct_origin_USA': pct_origin_USA,
        'pct_origin_GERMANY': pct_origin_GERMANY,
        'pct_origin_DENMARK': pct_origin_DENMARK,
        'pct_origin_FRANCE': pct_origin_FRANCE,
        'pct_origin_OTROS': pct_origin_OTROS,
        'last_import_date': last_date,
        'first_import_date': first_date,
        'total_weight_kg': total_weight,
        'avg_weight_per_shipment': avg_weight,
        'business_opportunity_score': business_opportunity_score,
        'crossborder_potential': crossborder_potential,
        'ocean_freight_potential': ocean_freight_potential,
        'supply_chain_potential': supply_chain_potential
    }

def insert_summary(summary):
    """Insert summary into database"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        INSERT INTO import_summaries (
            importer_name, rfc, total_pedimentos_last_6_months, total_freight_usd_value,
            avg_freight_usd_per_shipment, customs_offices_used, pct_shipments_key_locations,
            pct_regime_A1, pct_regime_F4, pct_regime_IN, pct_regime_A3, pct_regime_AF,
            pct_transport_carretero, pct_transport_aereo, pct_transport_maritimo, pct_transport_not_declared,
            pct_port_NUEVO_LAREDO, pct_port_COLOMBIA_NL, pct_port_MONTERREY_AIRPORT, pct_port_MANZANILLO,
            pct_port_PUEBLA, pct_port_OTHERS, pct_hs_84, pct_hs_85, pct_hs_90, pct_hs_73, pct_hs_74,
            pct_hs_OTROS, is_origin_usa, is_candidate_for_crossborder, pct_incoterm_DAP, pct_incoterm_EXW,
            pct_incoterm_FCA, pct_incoterm_OTROS, custom_brokers_used, top_custom_broker_id,
            pct_top_custom_broker_id, num_custom_brokers_used, pct_origin_TAIWAN, pct_origin_VIETNAM, pct_origin_CHINA, pct_origin_USA,
            pct_origin_GERMANY, pct_origin_DENMARK, pct_origin_FRANCE, pct_origin_OTROS,
            last_import_date, first_import_date, total_weight_kg, avg_weight_per_shipment,
            business_opportunity_score, crossborder_potential, ocean_freight_potential, supply_chain_potential
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, tuple(summary.values()))
        
        conn.commit()
        return True
    except Exception as e:
        print(f"Error inserting summary for {summary['importer_name']}: {e}")
        return False
    finally:
        conn.close()

def export_to_csv():
    """Export import_summaries table to CSV"""
    print("\n📊 CSV EXPORT OPTIONS")
    print("=" * 20)
    print("1. Export all summaries")
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
        
        # Get all summaries
        cursor.execute("SELECT * FROM import_summaries")
        summaries = cursor.fetchall()
        
        if not summaries:
            print("No summaries to export")
            conn.close()
            return
        
        # Get column names
        cursor.execute("PRAGMA table_info(import_summaries)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"import_summaries_{timestamp}.csv"
        
        # Export to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(columns)
            
            # Write data
            writer.writerows(summaries)
        
        conn.close()
        print(f"✅ Exported {len(summaries)} summaries to: {filename}")
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def main():
    print("LOGCOMEX IMPORT SUMMARIZER")
    print("=" * 40)
    
    # Ask about webhook triggering first
    print("\n🚀 WEBHOOK TRIGGER OPTIONS")
    print("=" * 25)
    print("1. Trigger webhook after summarization")
    print("2. Skip webhook trigger")
    
    while True:
        webhook_choice = input("\nSelect option (1-2): ").strip()
        if webhook_choice == "1":
            trigger_webhook = True
            break
        elif webhook_choice == "2":
            trigger_webhook = False
            break
        else:
            print("❌ Invalid choice. Please select 1 or 2.")
    
    # Check if data exists
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM import_records")
    record_count = cursor.fetchone()[0]
    
    if record_count == 0:
        print("No import records found. Run import_records.py first.")
        conn.close()
        return
    
    print(f"Found {record_count} import records")
    
    # Create summary table
    print("Setting up summary table...")
    create_summary_table()
    
    # Check existing summaries
    cursor.execute("SELECT COUNT(*) FROM import_summaries")
    existing_summaries = cursor.fetchone()[0]
    conn.close()
    
    if existing_summaries > 0:
        print(f"Found {existing_summaries} existing summaries in database")
        print("\n📊 SUMMARY DATABASE OPTIONS")
        print("=" * 30)
        print("1. Keep existing summaries and add new ones")
        print("2. Clear summaries and regenerate all")
        
        while True:
            choice = input("\nSelect option (1-2): ").strip()
            if choice == "1":
                clear_summaries_flag = False
                break
            elif choice == "2":
                clear_summaries_flag = True
                break
            else:
                print("❌ Invalid choice. Please select 1 or 2.")
    else:
        print("No existing summaries found")
        clear_summaries_flag = False
    
    # Get date range from user
    start_date, end_date = get_date_range()
    if not start_date or not end_date:
        return
    
    print(f"\n🔍 SUMMARIZATION CONFIGURATION")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Summary action: {'Clear and regenerate' if clear_summaries_flag else 'Keep existing and add new'}")
    
    # Clear existing summaries if requested
    if clear_summaries_flag:
        print("\nClearing existing summaries...")
        clear_summaries()
    else:
        print(f"\nKeeping existing {existing_summaries} summaries...")
    
    # Get importers
    importers = get_importers()
    print(f"Found {len(importers)} unique importers")
    
    # Generate summaries
    summaries_created = 0
    
    for i, importer in enumerate(importers, 1):
        if i % 10 == 0:
            print(f"Processed {i}/{len(importers)} importers")
        
        records = get_importer_records(importer, start_date, end_date)
        if records:
            summary = calculate_summary(importer, records)
            if summary and insert_summary(summary):
                summaries_created += 1
    
    # Get final count
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM import_summaries")
    final_summaries = cursor.fetchone()[0]
    
    print(f"\nRESULTS:")
    print(f"  Importers processed: {len(importers)}")
    print(f"  Summaries created: {summaries_created}")
    print(f"  Total summaries in database: {final_summaries}")
    
    # Show statistics
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    cursor.execute("SELECT AVG(business_opportunity_score) FROM import_summaries")
    avg_score = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM import_summaries WHERE is_origin_usa = 1")
    usa_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM import_summaries WHERE is_candidate_for_crossborder = 1")
    crossborder_count = cursor.fetchone()[0]
    
    print(f"\nSTATISTICS:")
    print(f"  Average business score: {avg_score:.1f}/10")
    print(f"  USA origin companies: {usa_count}")
    print(f"  Cross-border candidates: {crossborder_count}")
    
    # Show sample summaries
    cursor.execute("SELECT importer_name, business_opportunity_score FROM import_summaries LIMIT 3")
    samples = cursor.fetchall()
    
    if samples:
        print(f"\nSample summaries:")
        for i, (name, score) in enumerate(samples, 1):
            print(f"  {i}. {name}: Score {score}")
    
    conn.close()
    print(f"\nSummarization completed successfully!")
    
    # Offer CSV export
    export_to_csv()
    
    # Trigger webhook if requested
    if trigger_webhook:
        # Auto-trigger webhook after summary creation
        print(f"\n🚀 AUTO-TRIGGERING WEBHOOK...")
        try:
            # Import webhook sender directly
            import requests
            import json
            from datetime import datetime
            
            # Webhook URLs
            test_url = "http://68.183.85.45:5678/webhook-test/1538d58f-70e9-4879-84ee-4ac85b7a755c"
            prod_url = "http://68.183.85.45:5678/webhook/1538d58f-70e9-4879-84ee-4ac85b7a755c"
           
            # Get the summary data we just created
            conn = sqlite3.connect(DATABASE_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM import_summaries")
            summaries = cursor.fetchall()
            
            # Get column names
            cursor.execute("PRAGMA table_info(import_summaries)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Convert to list of dictionaries
            summary_data = []
            for summary in summaries:
                summary_dict = {}
                for i, col in enumerate(columns):
                    summary_dict[col] = summary[i]
                summary_data.append(summary_dict)
            
            conn.close()
            
            # Prepare payload
            payload = {
                "timestamp": datetime.now().isoformat(),
                "source": "logcomex_importer",
                "trigger": "summary_created",
                "summary_count": len(summary_data),
                "data": summary_data
            }
            
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Logcomex-Importer/1.0'
            }
            
            # Send to test webhook
            print(f"📤 Sending to TEST webhook...")
            try:
                response = requests.get(test_url, json=payload, headers=headers, timeout=30)
                if response.status_code == 200:
                    print(f"  ✅ TEST webhook triggered successfully!")
                else:
                    print(f"  ❌ TEST webhook failed: {response.status_code}")
            except Exception as e:
                print(f"  ❌ TEST webhook error: {e}")
            
            # Send to production webhook
            print(f"📤 Sending to PRODUCTION webhook...")
            try:
                response = requests.post(prod_url, json=payload, headers=headers, timeout=30)
                if response.status_code == 200:
                    print(f"  ✅ PRODUCTION webhook triggered successfully!")
                else:
                    print(f"  ❌ PRODUCTION webhook failed: {response.status_code}")
            except Exception as e:
                print(f"  ❌ PRODUCTION webhook error: {e}")
            
            print(f"🎉 Webhook auto-trigger completed!")
            
        except Exception as e:
            print(f"⚠️  Webhook auto-trigger failed: {e}")
            print(f"💡 Summary was created successfully, but webhook trigger failed")
    else:
        print(f"\n⏭️  Webhook trigger skipped as requested.")


if __name__ == "__main__":
    main() 