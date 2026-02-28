"""
FluxCharge - EV Charging Station Data Collector
Collects and stores EV charging station data for predictive analytics
"""

import sqlite3
import requests
import time
import os
from datetime import datetime
from typing import List, Dict, Optional

# Configuration
DATABASE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'fluxcharge.db')
OPENCHARGE_API_KEY = os.environ.get('OPENCHARGE_API_KEY', '')  # Optional: get from https://openchargemap.org/site/admin
COLLECTION_INTERVAL_MINUTES = 15


def init_database():
    """Initialize SQLite database with tables"""
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Stations table - static data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY,
            external_id TEXT UNIQUE,
            name TEXT,
            latitude REAL,
            longitude REAL,
            operator TEXT,
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


def fetch_stations_from_openchargemap(country: str = "SE", limit: int = 100) -> List[Dict]:
    """Fetch EV charging stations from Open Charge Map API"""
    # Using their public API (no key required for basic usage)
    url = "https://api.openchargemap.io/v3/poi/"
    
    params = {
        'output': 'json',
        'countrycode': country,
        'maxresults': limit,
        'compact': 'true',
        'verbose': 'false'
    }
    
    if OPENCHARGE_API_KEY:
        params['key'] = OPENCHARGE_API_KEY
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        stations = []
        for item in data:
            station = {
                'external_id': str(item.get('ID', '')),
                'name': item.get('AddressInfo', {}).get('Title', 'Unknown'),
                'latitude': item.get('AddressInfo', {}).get('Latitude'),
                'longitude': item.get('AddressInfo', {}).get('Longitude'),
                'operator': item.get('OperatorInfo', {}).get('Title', 'Unknown'),
                'connectors': str(item.get('Connections', []))
            }
            stations.append(station)
        
        print(f"📡 Fetched {len(stations)} stations from Open Charge Map")
        return stations
        
    except Exception as e:
        print(f"❌ Error fetching stations: {e}")
        return []


def add_stations_to_db(stations: List[Dict]) -> int:
    """Add stations to database, return count of new stations"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    count = 0
    for station in stations:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO stations 
                (external_id, name, latitude, longitude, operator, connectors)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                station['external_id'],
                station['name'],
                station['latitude'],
                station['longitude'],
                station['operator'],
                station['connectors']
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


def simulate_status_collection():
    """Simulate status collection for demo purposes"""
    # Since live status APIs are limited, we simulate for now
    # In production, you'd poll each network's API
    
    stations = get_all_stations()
    
    import random
    for station in stations:
        # Simulate: 70% available, 20% occupied, 10% unknown
        rand = random.random()
        if rand < 0.7:
            status = "available"
            available = random.randint(1, 4)
        elif rand < 0.9:
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
    stations = fetch_stations_from_openchargemap(country="SE", limit=50)
    if stations:
        add_stations_to_db(stations)
    
    # Record status for all known stations
    if stations:
        simulate_status_collection()
    
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
    
    # Run once for testing, or uncomment to run continuously
    # run_collector()
    
    # Single run for now
    collect_data()
