#!/usr/bin/env python3
"""
FastAPI Backend for Logcomex Importer
Integrates import_records and import_summarize functionality
"""

import os
import mysql.connector
import aiomysql
import aiohttp
import asyncio
import requests
import json
import time
import csv
from datetime import datetime, timedelta
from decimal import Decimal
from collections import Counter
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import uvicorn
from concurrent.futures import ThreadPoolExecutor
import logging

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_HOST = os.getenv("DB_HOST", "ritstest.cnfwdrtgyxew.us-west-2.rds.amazonaws.com")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "logcomex")
DB_USER = os.getenv("DB_USER", "sarvesh")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Saved6-Hydrogen-Smirk-Paltry-Trimmer")
API_KEY = os.getenv("API_KEY", "n8HPUtVG16Ea5mYi5jvlOYqFyObzTd1ZvXMTjU8s")
API_URL = "https://bi-api.logcomex.io/api/v1/details"
DEFAULT_MONTHS_BACK = int(os.getenv("DEFAULT_MONTHS_BACK", "6"))
DEFAULT_IMPORTER_NAME = os.getenv("DEFAULT_IMPORTER_NAME", "DANFOSS INDUSTRIES SA DE CV")

# Global database connection pool
db_pool = None
async_db_pool = None
thread_pool = ThreadPoolExecutor(max_workers=4)

# Initialize FastAPI app
app = FastAPI(
    title="Logcomex Importer API",
    description="API for importing and summarizing Logcomex import records",
    version="1.0.0"
)

# Pydantic models for request/response
class ImportRequest(BaseModel):
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format")
    importer_name: str = Field(..., description="Importer name to search for")
    clear_existing: bool = Field(False, description="Whether to clear existing records before import")
    run_summarize: bool = Field(True, description="Whether to run summarization after import")

class ImportResponse(BaseModel):
    success: bool
    message: str
    records_fetched: int
    records_inserted: int
    total_records: int
    summaries_created: Optional[int] = None
    total_summaries: Optional[int] = None
    execution_time: float

class SummaryRequest(BaseModel):
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format")
    clear_existing: bool = Field(False, description="Whether to clear existing summaries before generation")

class SummaryResponse(BaseModel):
    success: bool
    message: str
    importers_processed: int
    summaries_created: int
    total_summaries: int
    execution_time: float

class StatusResponse(BaseModel):
    database_exists: bool
    records_count: int
    summaries_count: int
    last_updated: Optional[str] = None

# Database connection management
async def get_async_db_pool():
    """Get async database connection pool"""
    global async_db_pool
    if async_db_pool is None:
        async_db_pool = await aiomysql.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            charset='utf8mb4',
            autocommit=True,
            minsize=5,
            maxsize=20
        )
    return async_db_pool

def get_db_connection():
    """Get a database connection from the pool"""
    global db_pool
    if db_pool is None:
        # Initialize connection pool with more connections
        db_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="logcomex_pool",
            pool_size=10,  # Increased pool size
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=True,
            charset='utf8mb4',
            use_unicode=True,
            buffered=True
        )
    return db_pool.get_connection()

def close_db_connection(conn):
    """Return connection to the pool"""
    if conn:
        conn.close()

async def execute_query_async(query: str, params: tuple = None):
    """Execute a query asynchronously"""
    pool = await get_async_db_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, params or ())
            if query.strip().upper().startswith('SELECT'):
                return await cursor.fetchall()
            return cursor.rowcount

# Database functions
async def create_database():
    """Create MySQL database and tables with indexes for performance"""
    pool = await get_async_db_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Create import_records table
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS import_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                dispatch_date DATE,
                importer_name VARCHAR(255),
                importer_address TEXT,
                supplier_name VARCHAR(255),
                supplier_address TEXT,
                origin_destination_country VARCHAR(255),
                buyer_seller_country VARCHAR(255),
                entry_exit_transport VARCHAR(255),
                departure_hscodes VARCHAR(50),
                departure_gross_weight DECIMAL(15,2),
                departure_goods_usd_value DECIMAL(15,2),
                dispatch_customs VARCHAR(255),
                entry_customs VARCHAR(255),
                custom_broker_id VARCHAR(255),
                customs_regime VARCHAR(50),
                customs_regime_id VARCHAR(50),
                declaration_type VARCHAR(255),
                dispatch_customs_state VARCHAR(255),
                importer_id VARCHAR(255),
                incoterm VARCHAR(50),
                container_type VARCHAR(255),
                teus_qty DECIMAL(15,2),
                departure_insurance_usd_value DECIMAL(15,2),
                departure_freight_usd_value DECIMAL(15,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_importer_name (importer_name),
                INDEX idx_dispatch_date (dispatch_date),
                INDEX idx_importer_date (importer_name, dispatch_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Create import_summaries table
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS import_summaries (
                id INT AUTO_INCREMENT PRIMARY KEY,
                importer_name VARCHAR(255) UNIQUE,
                rfc VARCHAR(255),
                total_pedimentos_last_6_months INT,
                total_freight_usd_value DECIMAL(15,2),
                avg_freight_usd_per_shipment DECIMAL(15,2),
                customs_offices_used TEXT,
                pct_shipments_key_locations DECIMAL(5,2),
                pct_regime_A1 DECIMAL(5,2),
                pct_regime_F4 DECIMAL(5,2),
                pct_regime_IN DECIMAL(5,2),
                pct_regime_A3 DECIMAL(5,2),
                pct_regime_AF DECIMAL(5,2),
                pct_transport_carretero DECIMAL(5,2),
                pct_transport_aereo DECIMAL(5,2),
                pct_transport_maritimo DECIMAL(5,2),
                pct_transport_not_declared DECIMAL(5,2),
                pct_port_NUEVO_LAREDO DECIMAL(5,2),
                pct_port_COLOMBIA_NL DECIMAL(5,2),
                pct_port_MONTERREY_AIRPORT DECIMAL(5,2),
                pct_port_MANZANILLO DECIMAL(5,2),
                pct_port_PUEBLA DECIMAL(5,2),
                pct_port_OTHERS DECIMAL(5,2),
                pct_hs_84 DECIMAL(5,2),
                pct_hs_85 DECIMAL(5,2),
                pct_hs_90 DECIMAL(5,2),
                pct_hs_73 DECIMAL(5,2),
                pct_hs_74 DECIMAL(5,2),
                pct_hs_OTROS DECIMAL(5,2),
                is_origin_usa TINYINT(1),
                is_candidate_for_crossborder TINYINT(1),
                pct_incoterm_DAP DECIMAL(5,2),
                pct_incoterm_EXW DECIMAL(5,2),
                pct_incoterm_FCA DECIMAL(5,2),
                pct_incoterm_OTROS DECIMAL(5,2),
                custom_brokers_used TEXT,
                top_custom_broker_id VARCHAR(255),
                pct_top_custom_broker_id DECIMAL(5,2),
                num_custom_brokers_used INT,
                pct_origin_TAIWAN DECIMAL(5,2),
                pct_origin_VIETNAM DECIMAL(5,2),
                pct_origin_CHINA DECIMAL(5,2),
                pct_origin_USA DECIMAL(5,2),
                pct_origin_GERMANY DECIMAL(5,2),
                pct_origin_DENMARK DECIMAL(5,2),
                pct_origin_FRANCE DECIMAL(5,2),
                pct_origin_OTROS DECIMAL(5,2),
                last_import_date DATE,
                first_import_date DATE,
                total_weight_kg DECIMAL(15,2),
                avg_weight_per_shipment DECIMAL(15,2),
                business_opportunity_score INT,
                crossborder_potential INT,
                ocean_freight_potential INT,
                supply_chain_potential INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_business_score (business_opportunity_score),
                INDEX idx_usa_origin (is_origin_usa),
                INDEX idx_crossborder (is_candidate_for_crossborder)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)

async def clear_existing_data_async():
    """Clear existing import records asynchronously"""
    await execute_query_async("DELETE FROM import_records")

def clear_existing_data():
    """Clear existing import records (sync wrapper)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(clear_existing_data_async())
    finally:
        loop.close()

async def clear_summaries_async():
    """Clear existing summaries asynchronously"""
    await execute_query_async("DELETE FROM import_summaries")

def clear_summaries():
    """Clear existing summaries (sync wrapper)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(clear_summaries_async())
    finally:
        loop.close()

async def fetch_data_from_api_async(start_date: str, end_date: str, importer_name: str) -> List[Dict]:
    """Fetch data from Logcomex API asynchronously"""
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY,
        'product-signature': 'mexico-import-logistic'
    }
    
    payload = {
        "filters": [
            {
                "field": "dispatch_date",
                "value": [start_date, end_date]
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
    
    async with aiohttp.ClientSession() as session:
        while True:
            payload["page"] = page
            
            try:
                async with session.post(API_URL, headers=headers, json=payload, timeout=30) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Handle the fact that data['data'] is a dict, not a list
                    data_section = data.get("data", {})
                    if isinstance(data_section, dict):
                        records = list(data_section.values())  # Convert dict values to list
                    else:
                        records = data_section if data_section else []
                    
                    if not records:
                        break
                        
                    all_records.extend(records)
                    
                    # Rate limiting
                    await asyncio.sleep(0.5)  # Reduced sleep time
                    page += 1
                    
                    # Check if more pages
                    if len(records) < 100:
                        break
                        
            except Exception as e:
                logger.error(f"Error fetching data: {e}")
                break
    
    return all_records

def fetch_data_from_api(start_date: str, end_date: str, importer_name: str) -> List[Dict]:
    """Synchronous wrapper for API fetch (for backward compatibility)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(fetch_data_from_api_async(start_date, end_date, importer_name))
    finally:
        loop.close()

async def insert_records_bulk_async(records: List[Dict]) -> int:
    """Insert records into database using bulk operations for better performance"""
    if not records:
        return 0
        
    # Prepare all data for bulk insert
    values = []
    for record in records:
        if not isinstance(record, dict):
            continue
        values.append((
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
    
    if not values:
        return 0
    
    pool = await get_async_db_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # Bulk insert with executemany for better performance
            query = """
            INSERT INTO import_records (
                dispatch_date, importer_name, importer_address, supplier_name, supplier_address,
                origin_destination_country, buyer_seller_country, entry_exit_transport,
                departure_hscodes, departure_gross_weight, departure_goods_usd_value,
                dispatch_customs, entry_customs, custom_broker_id, customs_regime,
                customs_regime_id, declaration_type, dispatch_customs_state, importer_id,
                incoterm, container_type, teus_qty, departure_insurance_usd_value,
                departure_freight_usd_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            await cursor.executemany(query, values)
            return len(values)

def insert_records(records: List[Dict]) -> int:
    """Synchronous wrapper for bulk insert (for backward compatibility)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(insert_records_bulk_async(records))
    finally:
        loop.close()

async def get_importers_async() -> List[str]:
    """Get list of unique importers asynchronously"""
    results = await execute_query_async("SELECT DISTINCT importer_name FROM import_records WHERE importer_name IS NOT NULL")
    return [row[0] for row in results] if results else []

def get_importers() -> List[str]:
    """Get list of unique importers (sync wrapper)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(get_importers_async())
    finally:
        loop.close()

async def get_importer_records_async(importer_name: str, start_date: str, end_date: str) -> List[tuple]:
    """Get all records for a specific importer within date range asynchronously"""
    results = await execute_query_async(
        "SELECT * FROM import_records WHERE importer_name = %s AND dispatch_date >= %s AND dispatch_date <= %s",
        (importer_name, start_date, end_date)
    )
    return results if results else []

def get_importer_records(importer_name: str, start_date: str, end_date: str) -> List[tuple]:
    """Get all records for a specific importer within date range (sync wrapper)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(get_importer_records_async(importer_name, start_date, end_date))
    finally:
        loop.close()

def calculate_summary(importer_name: str, records: List[tuple]) -> Optional[Dict]:
    """Calculate all KPIs for an importer"""
    if not records:
        return None
    
    total_records = len(records)
    
    # Basic metrics
    total_freight = round(sum(float(r[11] or 0) for r in records), 2)
    avg_freight = round(total_freight / total_records if total_records > 0 else 0, 2)
    total_weight = round(sum(float(r[10] or 0) for r in records), 2)
    avg_weight = round(total_weight / total_records if total_records > 0 else 0, 2)
    
    # Extract RFC from importer_id
    rfc = records[0][19] if records[0][19] else ""
    
    # Date ranges
    dates = [r[1] for r in records if r[1]]
    first_date = min(dates) if dates else ""
    last_date = max(dates) if dates else ""
    
    # Regime percentages
    regimes = [r[16] or "" for r in records]
    regime_counts = Counter(regimes)
    pct_regime_A1 = round((regime_counts.get("A1", 0) / total_records) * 100, 2)
    pct_regime_F4 = round((regime_counts.get("F4", 0) / total_records) * 100, 2)
    pct_regime_IN = round((regime_counts.get("IN", 0) / total_records) * 100, 2)
    pct_regime_A3 = round((regime_counts.get("A3", 0) / total_records) * 100, 2)
    pct_regime_AF = round((regime_counts.get("AF", 0) / total_records) * 100, 2)
    
    # Transport percentages
    transports = [r[8] or "" for r in records]
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
        entry_customs = (record[13] or "").upper()
        dispatch_customs = (record[12] or "").upper()
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
        hs_code = record[9] or ""
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
    countries = [r[6] or "" for r in records]
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
    incoterms = [r[20] or "" for r in records]
    incoterm_counts = Counter(incoterms)
    pct_incoterm_DAP = round((incoterm_counts.get("DAP", 0) / total_records) * 100, 2)
    pct_incoterm_EXW = round((incoterm_counts.get("EXW", 0) / total_records) * 100, 2)
    pct_incoterm_FCA = round((incoterm_counts.get("FCA", 0) / total_records) * 100, 2)
    pct_incoterm_OTROS = round(((total_records - incoterm_counts.get("DAP", 0) - incoterm_counts.get("EXW", 0) - incoterm_counts.get("FCA", 0)) / total_records) * 100, 2)
    
    # Custom brokers
    brokers = [r[14] or "" for r in records if r[14]]
    broker_counts = Counter(brokers)
    num_brokers = len(broker_counts)
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
        'pct_regime_IN': pct_regime_A1,
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

async def insert_summary_async(summary: Dict) -> bool:
    """Insert summary into database asynchronously"""
    try:
        await execute_query_async("""
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
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            rfc = VALUES(rfc),
            total_pedimentos_last_6_months = VALUES(total_pedimentos_last_6_months),
            total_freight_usd_value = VALUES(total_freight_usd_value),
            avg_freight_usd_per_shipment = VALUES(avg_freight_usd_per_shipment),
            customs_offices_used = VALUES(customs_offices_used),
            pct_shipments_key_locations = VALUES(pct_shipments_key_locations),
            pct_regime_A1 = VALUES(pct_regime_A1),
            pct_regime_F4 = VALUES(pct_regime_F4),
            pct_regime_IN = VALUES(pct_regime_IN),
            pct_regime_A3 = VALUES(pct_regime_A3),
            pct_regime_AF = VALUES(pct_regime_AF),
            pct_transport_carretero = VALUES(pct_transport_carretero),
            pct_transport_aereo = VALUES(pct_transport_aereo),
            pct_transport_maritimo = VALUES(pct_transport_maritimo),
            pct_transport_not_declared = VALUES(pct_transport_not_declared),
            pct_port_NUEVO_LAREDO = VALUES(pct_port_NUEVO_LAREDO),
            pct_port_COLOMBIA_NL = VALUES(pct_port_COLOMBIA_NL),
            pct_port_MONTERREY_AIRPORT = VALUES(pct_port_MONTERREY_AIRPORT),
            pct_port_MANZANILLO = VALUES(pct_port_MANZANILLO),
            pct_port_PUEBLA = VALUES(pct_port_PUEBLA),
            pct_port_OTHERS = VALUES(pct_port_OTHERS),
            pct_hs_84 = VALUES(pct_hs_84),
            pct_hs_85 = VALUES(pct_hs_85),
            pct_hs_90 = VALUES(pct_hs_90),
            pct_hs_73 = VALUES(pct_hs_73),
            pct_hs_74 = VALUES(pct_hs_74),
            pct_hs_OTROS = VALUES(pct_hs_OTROS),
            is_origin_usa = VALUES(is_origin_usa),
            is_candidate_for_crossborder = VALUES(is_candidate_for_crossborder),
            pct_incoterm_DAP = VALUES(pct_incoterm_DAP),
            pct_incoterm_EXW = VALUES(pct_incoterm_EXW),
            pct_incoterm_FCA = VALUES(pct_incoterm_FCA),
            pct_incoterm_OTROS = VALUES(pct_incoterm_OTROS),
            custom_brokers_used = VALUES(custom_brokers_used),
            top_custom_broker_id = VALUES(top_custom_broker_id),
            pct_top_custom_broker_id = VALUES(pct_top_custom_broker_id),
            num_custom_brokers_used = VALUES(num_custom_brokers_used),
            pct_origin_TAIWAN = VALUES(pct_origin_TAIWAN),
            pct_origin_VIETNAM = VALUES(pct_origin_VIETNAM),
            pct_origin_CHINA = VALUES(pct_origin_CHINA),
            pct_origin_USA = VALUES(pct_origin_USA),
            pct_origin_GERMANY = VALUES(pct_origin_GERMANY),
            pct_origin_DENMARK = VALUES(pct_origin_DENMARK),
            pct_origin_FRANCE = VALUES(pct_origin_FRANCE),
            pct_origin_OTROS = VALUES(pct_origin_OTROS),
            last_import_date = VALUES(last_import_date),
            first_import_date = VALUES(first_import_date),
            total_weight_kg = VALUES(total_weight_kg),
            avg_weight_per_shipment = VALUES(avg_weight_per_shipment),
            business_opportunity_score = VALUES(business_opportunity_score),
            crossborder_potential = VALUES(crossborder_potential),
            ocean_freight_potential = VALUES(ocean_freight_potential),
            supply_chain_potential = VALUES(supply_chain_potential),
            updated_at = CURRENT_TIMESTAMP
        """, tuple(summary.values()))
        return True
    except Exception as e:
        logger.error(f"Error inserting summary for {summary['importer_name']}: {e}")
        return False

def insert_summary(summary: Dict) -> bool:
    """Insert summary into database (sync wrapper)"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(insert_summary_async(summary))
    finally:
        loop.close()

# Background task functions
async def run_summarization_background(start_date: str, end_date: str, clear_existing: bool):
    """Run summarization in background"""
    try:
        await run_summarization_internal_async(SummaryRequest(
            start_date=start_date,
            end_date=end_date,
            clear_existing=clear_existing
        ))
        logger.info("Background summarization completed successfully")
    except Exception as e:
        logger.error(f"Background summarization failed: {e}")

async def process_importer_summary(importer: str, start_date: str, end_date: str) -> bool:
    """Process summary for a single importer"""
    try:
        records = await get_importer_records_async(importer, start_date, end_date)
        if records:
            summary = calculate_summary(importer, records)
            if summary:
                return await insert_summary_async(summary)
        return False
    except Exception as e:
        logger.error(f"Error processing summary for {importer}: {e}")
        return False

# API Endpoints
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Logcomex Importer API",
        "version": "1.0.0",
        "endpoints": {
            "import": "/import",
            "summarize": "/summarize",
            "status": "/status",
            "export_csv": "/export/csv",
            "docs": "/docs"
        }
    }

@app.get("/status")
async def get_status():
    """Get current database status with optimized async queries"""
    try:
        # Check if tables exist
        tables_result = await execute_query_async("""
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ('import_records', 'import_summaries')
        """, (DB_NAME,))
        tables = [row[0] for row in tables_result] if tables_result else []
        
        records_count = 0
        summaries_count = 0
        last_updated = None
        
        if 'import_records' in tables:
            count_result = await execute_query_async("SELECT COUNT(*) FROM import_records")
            records_count = count_result[0][0] if count_result else 0
            
            date_result = await execute_query_async("SELECT MAX(created_at) FROM import_records")
            if date_result and date_result[0][0]:
                last_updated = date_result[0][0].isoformat()
        
        if 'import_summaries' in tables:
            summary_result = await execute_query_async("SELECT COUNT(*) FROM import_summaries")
            summaries_count = summary_result[0][0] if summary_result else 0
        
        return StatusResponse(
            database_exists=len(tables) > 0,
            records_count=records_count,
            summaries_count=summaries_count,
            last_updated=last_updated
        )
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")

@app.post("/import")
async def import_records(request: ImportRequest, background_tasks: BackgroundTasks):
    """Import records from Logcomex API with optimized async operations"""
    start_time = time.time()
    
    try:
        # Validate dates
        try:
            datetime.strptime(request.start_date, "%Y-%m-%d")
            datetime.strptime(request.end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        # Create database if not exists
        await create_database()
        
        # Clear existing data if requested (run in background)
        if request.clear_existing:
            await clear_existing_data_async()
        
        # Fetch data from API asynchronously
        records = await fetch_data_from_api_async(request.start_date, request.end_date, request.importer_name)
        
        if not records:
            return ImportResponse(
                success=True,
                message="No records found for the specified criteria",
                records_fetched=0,
                records_inserted=0,
                total_records=0,
                execution_time=time.time() - start_time
            )
        
        # Insert records using bulk operations
        inserted = await insert_records_bulk_async(records)
        
        # Get final count asynchronously
        total_records = await execute_query_async("SELECT COUNT(*) FROM import_records")
        total_records = total_records[0][0] if total_records else 0
        
        execution_time = time.time() - start_time
        
        # Run summarization if requested (in background to not block response)
        summaries_created = None
        total_summaries = None
        
        if request.run_summarize:
            # Run summarization in background for faster response
            background_tasks.add_task(
                run_summarization_background,
                request.start_date,
                request.end_date,
                False
            )
        
        return ImportResponse(
            success=True,
            message=f"Successfully imported {inserted} records",
            records_fetched=len(records),
            records_inserted=inserted,
            total_records=total_records,
            summaries_created=summaries_created,
            total_summaries=total_summaries,
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"Import failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

@app.post("/summarize")
async def run_summarization(request: SummaryRequest):
    """Run summarization on imported records with optimized async processing"""
    return await run_summarization_internal_async(request)

async def run_summarization_internal_async(request: SummaryRequest) -> SummaryResponse:
    """Internal summarization function with async operations"""
    start_time = time.time()
    
    try:
        # Validate dates
        try:
            datetime.strptime(request.start_date, "%Y-%m-%d")
            datetime.strptime(request.end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        # Check if data exists
        record_count_result = await execute_query_async("SELECT COUNT(*) FROM import_records")
        record_count = record_count_result[0][0] if record_count_result else 0
        
        if record_count == 0:
            raise HTTPException(status_code=400, detail="No import records found. Run import first.")
        
        # Create summary table if not exists
        await create_database()
        
        # Clear existing summaries if requested
        if request.clear_existing:
            await clear_summaries_async()
        
        # Get importers
        importers = await get_importers_async()
        
        # Generate summaries with concurrent processing
        summaries_created = 0
        batch_size = 10  # Process in batches to avoid overwhelming the database
        
        for i in range(0, len(importers), batch_size):
            batch = importers[i:i + batch_size]
            batch_tasks = []
            
            for importer in batch:
                batch_tasks.append(process_importer_summary(importer, request.start_date, request.end_date))
            
            # Process batch concurrently
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            summaries_created += sum(1 for result in batch_results if result is True)
        
        # Get final count
        total_summaries_result = await execute_query_async("SELECT COUNT(*) FROM import_summaries")
        total_summaries = total_summaries_result[0][0] if total_summaries_result else 0
        
        execution_time = time.time() - start_time
        
        return SummaryResponse(
            success=True,
            message=f"Successfully created {summaries_created} summaries",
            importers_processed=len(importers),
            summaries_created=summaries_created,
            total_summaries=total_summaries,
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"Summarization failed: {e}")
        raise HTTPException(status_code=500, detail=f"Summarization failed: {str(e)}")

async def run_summarization_internal(request: SummaryRequest) -> SummaryResponse:
    """Internal summarization function (async wrapper for compatibility)"""
    return await run_summarization_internal_async(request)

@app.get("/export/csv")
async def export_csv(
    table: str = Query(..., description="Table to export: 'records' or 'summaries'"),
    filename: Optional[str] = Query(None, description="Custom filename (optional)")
):
    """Export data to CSV"""
    try:
        if table not in ['records', 'summaries']:
            raise HTTPException(status_code=400, detail="Table must be 'records' or 'summaries'")
        
        try:
            if table == 'records':
                data = await execute_query_async("SELECT * FROM import_records")
                columns_result = await execute_query_async("SHOW COLUMNS FROM import_records")
                columns = [row[0] for row in columns_result] if columns_result else []
            else:
                data = await execute_query_async("SELECT * FROM import_summaries")
                columns_result = await execute_query_async("SHOW COLUMNS FROM import_summaries")
                columns = [row[0] for row in columns_result] if columns_result else []
            
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            raise HTTPException(status_code=500, detail=f"Error exporting data: {str(e)}")
        
        if not data:
            raise HTTPException(status_code=404, detail=f"No data found in {table} table")
        
        # Generate filename
        if filename:
            if not filename.endswith('.csv'):
                filename += '.csv'
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{table}_{timestamp}.csv"
        
        # Create CSV file
        csv_path = f"exports/{filename}"
        os.makedirs("exports", exist_ok=True)
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(data)
        
        return FileResponse(
            path=csv_path,
            filename=filename,
            media_type='text/csv'
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/ping")
async def ping():
    """Simple ping endpoint without database access for performance testing"""
    return {"message": "pong", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    async def startup():
        """Initialize database on startup"""
        await create_database()
        logger.info("Database initialized successfully")
    
    # Add startup event
    @app.on_event("startup")
    async def startup_event():
        await startup()
    
    @app.on_event("shutdown")
    async def shutdown_event():
        """Cleanup on shutdown"""
        global async_db_pool, thread_pool
        if async_db_pool:
            async_db_pool.close()
            await async_db_pool.wait_closed()
        if thread_pool:
            thread_pool.shutdown(wait=True)
        logger.info("Server shutdown completed")
    
    # Run the FastAPI application
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        workers=1  # Single worker for development
    )
