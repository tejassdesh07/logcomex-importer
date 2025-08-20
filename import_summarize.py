#!/usr/bin/env python3
"""
Minimalistic Import Summarize Script
Self-contained - all business logic included
"""

import os
import sqlite3
from datetime import datetime, timedelta
from collections import Counter

# Configuration
MONTHS_BACK = 6

# Configuration
DATABASE_FILE = "importer.db"

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

def get_importer_records(importer_name):
    """Get all records for a specific importer within date range"""
    end_date = datetime.now().date() - timedelta(days=7)  # 1 week ago
    start_date = end_date - timedelta(days=MONTHS_BACK * 30)  #
    
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
    total_freight = sum(float(r[24] or 0) for r in records)  # departure_freight_usd_value
    avg_freight = total_freight / total_records if total_records > 0 else 0
    total_weight = sum(float(r[10] or 0) for r in records)  # departure_gross_weight
    avg_weight = total_weight / total_records if total_records > 0 else 0
    
    # Extract RFC from importer_id
    rfc = records[0][19] if records[0][19] else ""  # importer_id
    
    # Date ranges
    dates = [r[1] for r in records if r[1]]  # dispatch_date
    first_date = min(dates) if dates else ""
    last_date = max(dates) if dates else ""
    
    # Regime percentages
    regimes = [r[16] or "" for r in records]  # customs_regime_id
    regime_counts = Counter(regimes)
    pct_regime_A1 = (regime_counts.get("A1", 0) / total_records) * 100
    pct_regime_F4 = (regime_counts.get("F4", 0) / total_records) * 100
    pct_regime_IN = (regime_counts.get("IN", 0) / total_records) * 100
    pct_regime_A3 = (regime_counts.get("A3", 0) / total_records) * 100
    pct_regime_AF = (regime_counts.get("AF", 0) / total_records) * 100
    
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
    
    pct_transport_carretero = (transport_counts["CARRETERO"] / total_records) * 100
    pct_transport_aereo = (transport_counts["AEREO"] / total_records) * 100
    pct_transport_maritimo = (transport_counts["MARITIMO"] / total_records) * 100
    pct_transport_not_declared = (transport_counts["NOT_DECLARED"] / total_records) * 100
    
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
    
    pct_port_NUEVO_LAREDO = (port_counts["NUEVO_LAREDO"] / total_records) * 100
    pct_port_COLOMBIA_NL = (port_counts["COLOMBIA_NL"] / total_records) * 100
    pct_port_MONTERREY_AIRPORT = (port_counts["MONTERREY_AIRPORT"] / total_records) * 100
    pct_port_MANZANILLO = (port_counts["MANZANILLO"] / total_records) * 100
    pct_port_PUEBLA = (port_counts["PUEBLA"] / total_records) * 100
    pct_port_OTHERS = (port_counts["OTHERS"] / total_records) * 100
    
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
    
    pct_hs_84 = (hs_counts["84"] / total_records) * 100
    pct_hs_85 = (hs_counts["85"] / total_records) * 100
    pct_hs_90 = (hs_counts["90"] / total_records) * 100
    pct_hs_73 = (hs_counts["73"] / total_records) * 100
    pct_hs_74 = (hs_counts["74"] / total_records) * 100
    pct_hs_OTROS = (hs_counts["OTROS"] / total_records) * 100
    
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
    
    pct_origin_TAIWAN = (country_counts["TAIWAN"] / total_records) * 100
    pct_origin_VIETNAM = (country_counts["VIETNAM"] / total_records) * 100
    pct_origin_CHINA = (country_counts["CHINA"] / total_records) * 100
    pct_origin_USA = (country_counts["USA"] / total_records) * 100
    pct_origin_GERMANY = (country_counts["GERMANY"] / total_records) * 100
    pct_origin_DENMARK = (country_counts["DENMARK"] / total_records) * 100
    pct_origin_FRANCE = (country_counts["FRANCE"] / total_records) * 100
    pct_origin_OTROS = (country_counts["OTROS"] / total_records) * 100
    
    # Incoterm percentages
    incoterms = [r[20] or "" for r in records]  # incoterm
    incoterm_counts = Counter(incoterms)
    pct_incoterm_DAP = (incoterm_counts.get("DAP", 0) / total_records) * 100
    pct_incoterm_EXW = (incoterm_counts.get("EXW", 0) / total_records) * 100
    pct_incoterm_FCA = (incoterm_counts.get("FCA", 0) / total_records) * 100
    pct_incoterm_OTROS = ((total_records - incoterm_counts.get("DAP", 0) - incoterm_counts.get("EXW", 0) - incoterm_counts.get("FCA", 0)) / total_records) * 100
    
    # Custom brokers
    brokers = [r[14] or "" for r in records if r[14]]  # custom_broker_id
    broker_counts = Counter(brokers)
    num_brokers = len(broker_counts)
    top_broker = broker_counts.most_common(1)[0] if broker_counts else ("", 0)
    pct_top_broker = (top_broker[1] / total_records) * 100 if top_broker[1] > 0 else 0
    
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
        'custom_brokers_used': str(num_brokers),
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

def main():
    print("LOGCOMEX IMPORT SUMMARIZER")
    print("=" * 40)
    
    # Check if data exists
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM import_records")
    record_count = cursor.fetchone()[0]
    conn.close()
    
    if record_count == 0:
        print("No import records found. Run import_records.py first.")
        return
    
    print(f"Found {record_count} import records")
    
    # Create summary table
    print("Setting up summary table...")
    create_summary_table()
    
    # Clear existing summaries
    print("Clearing existing summaries...")
    clear_summaries()
    
    # Get importers
    importers = get_importers()
    print(f"Found {len(importers)} unique importers")
    
    # Generate summaries
    summaries_created = 0
    
    for i, importer in enumerate(importers, 1):
        if i % 10 == 0:
            print(f"Processed {i}/{len(importers)} importers")
        
        records = get_importer_records(importer)
        if records:
            summary = calculate_summary(importer, records)
            if summary and insert_summary(summary):
                summaries_created += 1
    
    print(f"\nRESULTS:")
    print(f"  Importers processed: {len(importers)}")
    print(f"  Summaries created: {summaries_created}")
    
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
            response = requests.post(test_url, json=payload, headers=headers, timeout=30)
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

if __name__ == "__main__":
    main() 