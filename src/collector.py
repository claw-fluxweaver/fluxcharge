"""
FluxCharge - EV Charging Station Data Collector
Collects and stores EV charging station data for predictive analytics

Data Sources:
- Trafikverket NVDB (Swedish Transport Administration) - locations
- Trafikverket Traffic API - traffic flow
- SMHI - weather data
- Swedish calendar - holidays/events
- Simulated status for demo (real-time APIs require network partnerships)
"""

import sqlite3
import requests
import time
import os
import json
from datetime import datetime, date
from typing import List, Dict, Optional

# Configuration
DATABASE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'fluxcharge.db')
COLLECTION_INTERVAL_MINUTES = 15

# Swedish holidays for 2025-2026 (can be extended with API)
SWEDISH_HOLIDAYS = {
    2025: [
        "2025-01-01",  # Nyårsdagen
        "2025-01-06",  # Trettondagen
        "2025-04-18",  # Långfredagen
        "2025-04-20",  # Påskdagen
        "2025-04-21",  # Annandag påsk
        "2025-05-01",  # Första maj
        "2025-05-29",  # Kristi himmelsfärdsdag
        "2025-06-06",  # Nationaldagen
        "2025-06-20",  # Midsommarafton
        "2025-06-21",  # Midsommardagen
        "2025-12-24",  # Julafton
        "2025-12-25",  # Juldagen
        "2025-12-26",  # Annandag jul
        "2025-12-31",  # Nyårsafton
    ],
    2026: [
        "2026-01-01",  # Nyårsdagen
        "2026-01-06",  # Trettondagen
        "2026-04-03",  # Långfredagen
        "2026-04-05",  # Påskdagen
        "2026-04-06",  # Annandag påsk
        "2026-05-01",  # Första maj
        "2026-05-14",  # Kristi himmelsfärdsdag
        "2026-06-06",  # Nationaldagen
        "2026-06-19",  # Midsommarafton
        "2026-06-20",  # Midsommardagen
        "2026-12-24",  # Julafton
        "2026-12-25",  # Juldagen
        "2026-12-26",  # Annandag jul
        "2026-12-31",  # Nyårsafton
    ]
}


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
    
    # Weather history table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id INTEGER,
            temperature REAL,
            weather_condition TEXT,
            wind_speed REAL,
            precipitation REAL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (station_id) REFERENCES stations(id)
        )
    ''')
    
    # Traffic/flow data table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS traffic_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id INTEGER,
            traffic_volume REAL,
            avg_speed REAL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (station_id) REFERENCES stations(id)
        )
    ''')
    
    # Calendar context table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS calendar_context (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE UNIQUE,
            is_holiday BOOLEAN,
            holiday_name TEXT,
            day_of_week INTEGER,
            week_number INTEGER,
            is_weekend BOOLEAN,
            is_school_break BOOLEAN
        )
    ''')
    
    # Create index for faster queries
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_status_station_time 
        ON status_history(station_id, collected_at)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_weather_time 
        ON weather_history(station_id, collected_at)
    ''')
    
    conn.commit()
    conn.close()
    print(f"✅ Database initialized at {DATABASE_PATH}")


def is_holiday(check_date: date) -> tuple:
    """Check if date is a Swedish holiday"""
    year = check_date.year
    date_str = check_date.strftime("%Y-%m-%d")
    
    if date_str in SWEDISH_HOLIDAYS.get(year, []):
        return True, "Swedish Holiday"
    
    # Check for weekends
    if check_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return True, "Weekend"
    
    return False, None


def get_calendar_context(check_date: date) -> Dict:
    """Get calendar context for a date"""
    is_hol, holiday_name = is_holiday(check_date)
    
    # Swedish school breaks (approximate)
    week = check_date.isocalendar()[1]
    is_school_break = week in [7, 8, 9, 27, 28, 29, 30, 31, 32]  # Feb, Easter, Summer
    
    return {
        'date': check_date,
        'is_holiday': is_hol,
        'holiday_name': holiday_name,
        'day_of_week': check_date.weekday(),
        'week_number': week,
        'is_weekend': check_date.weekday() >= 5,
        'is_school_break': is_school_break
    }


def save_calendar_context(calendar_data: Dict):
    """Save calendar context to database"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR IGNORE INTO calendar_context 
        (date, is_holiday, holiday_name, day_of_week, week_number, is_weekend, is_school_break)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        calendar_data['date'],
        calendar_data['is_holiday'],
        calendar_data['holiday_name'],
        calendar_data['day_of_week'],
        calendar_data['week_number'],
        calendar_data['is_weekend'],
        calendar_data['is_school_break']
    ))
    
    conn.commit()
    conn.close()


def fetch_stations_from_trafikverket() -> List[Dict]:
    """
    Fetch EV charging stations from Trafikverket NVDB via their API
    Note: This is demo data - replace with actual API calls when registered
    """
    # Known charging station networks in Sweden - focused on Borås + region
    demo_stations = [
        # Borås area
        {"name": "Borås Centrum", "lat": 57.7211, "lon": 12.9405, "municipality": "Borås", "operator": "Recharge"},
        {"name": "Borås Arena", "lat": 57.7357, "lon": 12.9348, "municipality": "Borås", "operator": "Circle K"},
        {"name": "Korsängsgatan Borås", "lat": 57.7174, "lon": 12.9396, "municipality": "Borås", "operator": "OKQ8"},
        {"name": "Borås Restad", "lat": 57.7429, "lon": 12.9318, "municipality": "Borås", "operator": "Recharge"},
        {"name": "Borgsten", "lat": 57.7577, "lon": 12.9188, "municipality": "Borås", "operator": "Circle K"},
        
        # Surrounding municipalities
        {"name": "Ulricehamn", "lat": 57.7910, "lon": 13.4164, "municipality": "Ulricehamn", "operator": "Recharge"},
        {"name": "Herrljunga", "lat": 57.8595, "lon": 13.0396, "municipality": "Herrljunga", "operator": "OKQ8"},
        {"name": "Svenljunga", "lat": 57.8284, "lon": 13.1055, "municipality": "Svenljunga", "operator": "Circle K"},
        {"name": "Vårgårda", "lat": 57.9801, "lon": 12.8004, "municipality": "Vårgårda", "operator": "Recharge"},
        {"name": "Bollebygd", "lat": 57.6756, "lon": 12.5721, "municipality": "Bollebygd", "operator": "OKQ8"},
        
        # Larger cities
        {"name": "Göteborg Central", "lat": 57.7089, "lon": 11.9746, "municipality": "Göteborg", "operator": "Västra Götaland"},
        {"name": "Göteborg Kungsbacka", "lat": 57.4872, "lon": 12.0765, "municipality": "Kungsbacka", "operator": "Recharge"},
        {"name": "Malmö Central", "lat": 55.6059, "lon": 13.0013, "municipality": "Malmö", "operator": "E.ON"},
        {"name": "Stockholm City", "lat": 59.3293, "lon": 18.0686, "municipality": "Stockholm", "operator": "Stockholm Exergi"},
        {"name": "Jönköping", "lat": 57.7810, "lon": 14.1566, "municipality": "Jönköping", "operator": "Circle K"},
    ]
    
    stations = []
    for i, s in enumerate(demo_stations, 1):
        stations.append({
            'external_id': f"SE-{s['municipality'][:3].upper()}-{i:04d}",
            'name': s['name'],
            'latitude': s['lat'],
            'longitude': s['lon'],
            'municipality': s['municipality'],
            'operator': s['operator'],
            'power_kw': 22.0,
            'connectors': '[{"type":"CCS2","count":2,"power":22},{"type":"CHAdeMO","count":1,"power":50}]'
        })
    
    print(f"📡 Loaded {len(stations)} stations (demo data)")
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


def fetch_weather_for_station(latitude: float, longitude: float) -> Optional[Dict]:
    """Fetch weather data from SMHI for a location"""
    try:
        # Use SMHI's location-based forecast API
        # For simplicity, we'll use wttr.in which is already working
        url = f"https://wttr.in/{latitude},{longitude}?format=j1"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            current = data.get('current_condition', [{}])[0]
            
            return {
                'temperature': float(current.get('temp_C', 5)),
                'weather_condition': current.get('weatherCode', '0'),
                'wind_speed': float(current.get('windspeedKmph', 0)),
                'precipitation': float(current.get('precipMM', 0))
            }
    except Exception as e:
        print(f"⚠️ Weather fetch error: {e}")
    
    return None


def fetch_traffic_for_location(latitude: float, longitude: float) -> Optional[Dict]:
    """
    Fetch traffic data from Trafikverket
    Note: Requires API key - this is simulated for demo
    """
    # TODO: Implement real Trafikverket API when key is obtained
    # For now, simulate based on time of day
    
    import random
    hour = datetime.now().hour
    
    # Simulate traffic patterns
    if 7 <= hour <= 9:  # Morning rush
        base_volume = 0.8
    elif 16 <= hour <= 18:  # Evening rush
        base_volume = 0.9
    elif 10 <= hour <= 15:  # Midday
        base_volume = 0.5
    elif 19 <= hour <= 21:  # Evening
        base_volume = 0.4
    else:  # Night
        base_volume = 0.1
    
    # Add some randomness
    volume = base_volume + random.uniform(-0.1, 0.1)
    
    return {
        'traffic_volume': volume,  # 0-1 scale
        'avg_speed': 50 + random.uniform(-20, 20)  # km/h
    }


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


def record_weather(station_id: int, weather: Dict):
    """Record weather data for a station"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO weather_history 
        (station_id, temperature, weather_condition, wind_speed, precipitation)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        station_id,
        weather.get('temperature'),
        weather.get('weather_condition'),
        weather.get('wind_speed'),
        weather.get('precipitation')
    ))
    
    conn.commit()
    conn.close()


def record_traffic(station_id: int, traffic: Dict):
    """Record traffic data for a station"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO traffic_history 
        (station_id, traffic_volume, avg_speed)
        VALUES (?, ?, ?)
    ''', (
        station_id,
        traffic.get('traffic_volume'),
        traffic.get('avg_speed')
    ))
    
    conn.commit()
    conn.close()


def get_all_stations() -> List[Dict]:
    """Get all stations from database"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT id, external_id, name, latitude, longitude, operator, municipality FROM stations')
    rows = cursor.fetchall()
    
    conn.close()
    
    return [
        {
            'id': row[0],
            'external_id': row[1],
            'name': row[2],
            'latitude': row[3],
            'longitude': row[4],
            'operator': row[5],
            'municipality': row[6]
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
    
    cursor.execute('SELECT COUNT(*) FROM weather_history')
    weather_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM traffic_history')
    traffic_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT date) FROM calendar_context')
    calendar_days = cursor.fetchone()[0]
    
    cursor.execute('SELECT municipality, COUNT(*) FROM stations GROUP BY municipality')
    by_municipality = dict(cursor.fetchall())
    
    conn.close()
    
    return {
        'stations': station_count,
        'status_records': status_count,
        'weather_records': weather_count,
        'traffic_records': traffic_count,
        'calendar_days': calendar_days,
        'by_municipality': by_municipality
    }


def simulate_status_with_traffic(station: Dict) -> tuple:
    """
    Simulate station status based on traffic and time patterns
    This is where the ML model would eventually go
    """
    import random
    from datetime import datetime
    
    hour = datetime.now().hour
    weekday = datetime.now().weekday()
    
    # Get traffic (simulated for now)
    traffic = fetch_traffic_for_location(station['latitude'], station['longitude'])
    traffic_volume = traffic.get('traffic_volume', 0.5) if traffic else 0.5
    
    # Higher traffic = more likely occupied
    base_occupied_prob = traffic_volume * 0.7
    
    # Adjust for time of day
    if weekday < 5:  # Weekday
        if 7 <= hour <= 9 or 16 <= hour <= 18:  # Rush hours
            base_occupied_prob += 0.15
        elif 22 <= hour or hour <= 5:  # Night
            base_occupied_prob -= 0.3
    
    # Check if it's a holiday
    today = date.today()
    is_hol, _ = is_holiday(today)
    if is_hol:
        base_occupied_prob += 0.2  # More people traveling
    
    # Add randomness
    rand = random.random()
    if rand < (1 - base_occupied_prob):
        status = "available"
        available = random.randint(1, 4)
    elif rand < 0.95:
        status = "occupied"
        available = 0
    else:
        status = "unknown"
        available = 0
    
    return status, available


def collect_data():
    """Main collection function"""
    print(f"\n{'='*60}")
    print(f"🔌 FluxCharge Collector - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    # Save calendar context for today
    today = date.today()
    calendar_data = get_calendar_context(today)
    save_calendar_context(calendar_data)
    print(f"📅 Calendar: {calendar_data['holiday_name'] or calendar_data['date'].strftime('%A')}")
    
    # Fetch and store stations
    stations = fetch_stations_from_trafikverket()
    if stations:
        add_stations_to_db(stations)
    
    # Collect data for each station
    stations_list = get_all_stations()
    
    for station in stations_list:
        # Get traffic data
        traffic = fetch_traffic_for_location(station['latitude'], station['longitude'])
        if traffic:
            record_traffic(station['id'], traffic)
        
        # Get weather (sample a few, not all to avoid rate limits)
        if stations_list.index(station) < 3:  # Just first 3 for demo
            weather = fetch_weather_for_station(station['latitude'], station['longitude'])
            if weather:
                record_weather(station['id'], weather)
                print(f"  🌤️ {station['name']}: {weather['temperature']}°C")
        
        # Simulate status with traffic correlation
        status, available = simulate_status_with_traffic(station)
        record_status(station['id'], status, available)
    
    # Print stats
    stats = get_station_stats()
    print(f"\n📊 Collection Summary:")
    print(f"   Stations: {stats['stations']}")
    print(f"   Status records: {stats['status_records']}")
    print(f"   Weather records: {stats['weather_records']}")
    print(f"   Traffic records: {stats['traffic_records']}")
    print(f"   Calendar days: {stats['calendar_days']}")
    print(f"✅ Collection complete!")


def run_collector():
    """Run collector on a schedule"""
    print("🚀 Starting FluxCharge Collector...")
    print(f"📍 Collecting every {COLLECTION_INTERVAL_MINUTES} minutes")
    print("Press Ctrl+C to stop\n")
    
    # Initial collection
    collect_data()
    
    # Simple scheduling - check every 60 seconds
    try:
        import time
        while True:
            time.sleep(COLLECTION_INTERVAL_MINUTES * 60)
            collect_data()
    except KeyboardInterrupt:
        print("\n🛑 Collector stopped")


if __name__ == "__main__":
    # Initialize database first
    init_database()
    
    # Run once for testing
    collect_data()
    
    # To run continuously, uncomment:
    # run_collector()
