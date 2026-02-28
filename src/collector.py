"""
FluxCharge - EV Charging Station Data Collector
Collects and stores EV charging station data for predictive analytics

Data Sources:
- Trafikverket NVDB (Swedish Transport Administration) - locations
- Simulated status for demo (real-time APIs require network partnerships)
"""

import sqlite3
import requests
import time
import os
import json
from datetime import datetime
from typing import List, Dict, Optional

# Configuration
DATABASE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'fluxcharge.db')
COLLECTION_INTERVAL_MINUTES = 15

# Trafikverket API (free, no key needed for some endpoints)
TRAFIKVERKET_API = "https://api.trafikinfo.trafikverket.se/v2/data.json"


def init_database():
    """Initialize SQLite database with tables"""
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Stations table - static data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            name TEXT,
            latitude REAL,
            longitude REAL,
            municipality TEXT,
            operator TEXT,
            power_kw REAL,
            connectors TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Status history table - dynamic data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id INTEGER,
            status TEXT,
            available_connectors INTEGER,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (station_id) REFERENCES stations(id)
        )
    ''')
    
    # Create index for faster queries
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_status_station_time 
        ON status_history(station_id, collected_at)
    ''')
    
    conn.commit()
    conn.close()
    print(f"✅ Database initialized at {DATABASE_PATH}")


def fetch_stations_from_trafikverket() -> List[Dict]:
    """
    Fetch EV charging stations from Trafikverket NVDB via their API
    Note: This is a simplified example - full implementation needs API key registration
    """
    # For now, we'll use a curated list of known stations in Sweden
    # In production, you'd register for API key at data.trafikverket.se
    
    # Known charging station networks in Sweden
    # This is demo data - replace with actual API calls when registered
    demo_stations = [
        {"name": "Borås Centrum", "lat": 57.7211, "lon": 12.9405, "municipality": "Borås", "operator": "Recharge"},
        {"name": "Borås Arena", "lat": 57.7357, "lon": 12.9348, "municipality": "Borås", "operator": "Circle K"},
        {"name": "Korsängsgatan Borås", "lat": 57.7174, "lon": 12.9396, "municipality": "Borås", "operator": "OKQ8"},
        {"name": "Göteborg Central", "lat": 57.7089, "lon": 11.9746, "municipality": "Göteborg", "operator": "Västra Götaland"},
        {"name": "Göteborg Kungsbacka", "lat": 57.4872, "lon": 12.0765, "municipality": "Kungsbacka", "operator": "Recharge"},
        {"name": "Malmö Central", "lat": 55.6059, "lon": 13.0013, "municipality": "Malmö", "operator": "E.ON"},
        {"name": "Stockholm City", "lat": 59.3293, "lon": 18.0686, "municipality": "Stockholm", "operator": "Stockholm Exergi"},
        {"name": "Jönköping", "lat": 57.7810, "lon": 14.1566, "municipality": "Jönköping", "operator": "Circle K"},
        {"name": "Halmstad", "lat": 56.6744, "lon": 12.8568, "municipality": "Halmstad", "operator": "OKQ8"},
        {"name": "Växjö", "lat": 56.8777, "lon": 14.8093, "municipality": "Växjö", "operator": "Recharge"},
    ]
    
    stations = []
    for i, s in enumerate(demo_stations, 1):
        stations.append({
            'external_id': f"SE-BORAS-{i:04d}" if s['municipality'] == 'Borås' else f"SE-{s['municipality'][:3].upper()}-{i:04d}",
            'name': s['name'],
            'latitude': s['lat'],
            'longitude': s['lon'],
            'municipality': s['municipality'],
            'operator': s['operator'],
            'power_kw': 22.0,  # Typical for L2 charging
            'connectors': '[{"type":"CCS2","count":2,"power":22},{"type":"CHAdeMO","count":1,"power":50}]'
        })
    
    print(f"📡 Loaded {len(stations)} stations (demo data - register for Trafikverket API for full coverage)")
    return stations


def add_stations_to_db(stations: List[Dict]) -> int:
    """Add stations to database, return count of new stations"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    count = 0
    for station in stations:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO stations 
                (external_id, name, latitude, longitude, municipality, operator, power_kw, connectors)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                station['external_id'],
                station['name'],
                station['latitude'],
                station['longitude'],
                station.get('municipality', 'Unknown'),
                station['operator'],
                station.get('power_kw', 22.0),
                station.get('connectors', '')
            ))
            if cursor.rowcount > 0:
                count += 1
        except Exception as e:
            print(f"❌ Error adding station: {e}")
    
    conn.commit()
    conn.close()
    
    if count > 0:
        print(f"✅ Added {count} new stations to database")
    return count


def record_status(station_id: int, status: str = "unknown", available: int = 0):
    """Record current status of a station"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO status_history (station_id, status, available_connectors)
        VALUES (?, ?, ?)
    ''', (station_id, status, available))
    
    conn.commit()
    conn.close()


def get_all_stations() -> List[Dict]:
    """Get all stations from database"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT id, external_id, name, latitude, longitude, operator FROM stations')
    rows = cursor.fetchall()
    
    conn.close()
    
    return [
        {
            'id': row[0],
            'external_id': row[1],
            'name': row[2],
            'latitude': row[3],
            'longitude': row[4],
            'operator': row[5]
        }
        for row in rows
    ]


def get_station_stats() -> Dict:
    """Get statistics about collected data"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM stations')
    station_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM status_history')
    status_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT municipality, COUNT(*) FROM stations GROUP BY municipality')
    by_municipality = dict(cursor.fetchall())
    
    conn.close()
    
    return {
        'stations': station_count,
        'status_records': status_count,
        'by_municipality': by_municipality
    }


def simulate_status_collection():
    """
    Simulate status collection for demo purposes
    In production, you'd poll real APIs from charging networks
    """
    import random
    
    stations = get_all_stations()
    
    for station in stations:
        # Simulate realistic patterns based on time of day
        hour = datetime.now().hour
        
        # More occupied during day (8-18), less at night
        if 8 <= hour <= 18:
            # Day: 50% available, 40% occupied, 10% unknown
            rand = random.random()
            if rand < 0.5:
                status = "available"
                available = random.randint(1, 4)
            elif rand < 0.9:
                status = "occupied"
                available = 0
            else:
                status = "unknown"
                available = 0
        else:
            # Night: 80% available
            rand = random.random()
            if rand < 0.8:
                status = "available"
                available = random.randint(1, 4)
            elif rand < 0.95:
                status = "occupied"
                available = 0
            else:
                status = "unknown"
                available = 0
        
        record_status(station['id'], status, available)
    
    print(f"📊 Recorded status for {len(stations)} stations")


def collect_data():
    """Main collection function"""
    print(f"\n{'='*50}")
    print(f"🔌 FluxCharge Collector - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    
    # Fetch and store stations
    stations = fetch_stations_from_trafikverket()
    if stations:
        add_stations_to_db(stations)
    
    # Record status for all known stations
    simulate_status_collection()
    
    # Print stats
    stats = get_station_stats()
    print(f"📈 Total stations: {stats['stations']}")
    print(f"📈 Total status records: {stats['status_records']}")
    print(f"✅ Collection complete!")


def run_collector():
    """Run collector on a schedule"""
    import schedule
    
    print("🚀 Starting FluxCharge Collector...")
    print(f"📍 Collecting every {COLLECTION_INTERVAL_MINUTES} minutes")
    print("Press Ctrl+C to stop\n")
    
    # Initial collection
    collect_data()
    
    # Schedule recurring collection
    schedule.every(COLLECTION_INTERVAL_MINUTES).minutes.do(collect_data)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n🛑 Collector stopped")


if __name__ == "__main__":
    # Initialize database first
    init_database()
    
    # Run once for testing
    collect_data()
    
    # To run continuously, uncomment:
    # run_collector()
