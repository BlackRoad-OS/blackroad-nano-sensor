"""
Nano-Sensor Network Management System
BlackRoad OS - Distributed sensor fleet orchestration
"""

import sqlite3
import json
import statistics
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import argparse
import sys


@dataclass
class Sensor:
    """Sensor device descriptor"""
    id: str
    name: str
    type: str  # temperature, humidity, pressure, co2, motion, light, vibration, chemical, radiation, bio
    location: str
    unit: str
    value: Optional[float] = None
    battery_pct: float = 100.0
    last_seen: Optional[str] = None
    firmware_version: str = "1.0.0"
    tags: List[str] = field(default_factory=list)


@dataclass
class SensorReading:
    """Sensor measurement record"""
    sensor_id: str
    value: float
    timestamp: str
    quality: float = 1.0


class NanoSensorNetwork:
    """Distributed nano-sensor network controller"""
    
    VALID_SENSOR_TYPES = {
        "temperature", "humidity", "pressure", "co2", "motion",
        "light", "vibration", "chemical", "radiation", "bio"
    }
    
    def __init__(self, db_path: str = "~/.blackroad/sensors.db"):
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database schema"""
        self.conn = sqlite3.connect(str(self.db_path))
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensors (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                location TEXT NOT NULL,
                unit TEXT NOT NULL,
                value REAL,
                battery_pct REAL DEFAULT 100.0,
                last_seen TEXT,
                firmware_version TEXT DEFAULT '1.0.0',
                tags TEXT DEFAULT '[]',
                created_at TEXT,
                UNIQUE(name, location)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                value REAL NOT NULL,
                timestamp TEXT NOT NULL,
                quality REAL DEFAULT 1.0,
                FOREIGN KEY(sensor_id) REFERENCES sensors(id),
                INDEX idx_sensor_time (sensor_id, timestamp DESC)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS calibration (
                sensor_id TEXT PRIMARY KEY,
                offset REAL DEFAULT 0.0,
                scale REAL DEFAULT 1.0,
                calibrated_at TEXT,
                FOREIGN KEY(sensor_id) REFERENCES sensors(id)
            )
        """)
        
        self.conn.commit()
    
    def register_sensor(self, name: str, type: str, location: str, unit: str, 
                       tags: List[str] = None) -> str:
        """Register a new sensor in the network"""
        if type not in self.VALID_SENSOR_TYPES:
            raise ValueError(f"Invalid sensor type: {type}")
        
        tags = tags or []
        sensor_id = f"{type}_{location.replace(' ', '_').lower()}_{int(datetime.now().timestamp() * 1000) % 10000}"
        now = datetime.now().isoformat()
        
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO sensors 
            (id, name, type, location, unit, tags, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (sensor_id, name, type, location, unit, json.dumps(tags), now))
        self.conn.commit()
        
        return sensor_id
    
    def ingest_reading(self, sensor_id: str, value: float, quality: float = 1.0) -> bool:
        """Record a sensor reading"""
        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
        
        # Validate sensor exists
        cursor.execute("SELECT id FROM sensors WHERE id = ?", (sensor_id,))
        if not cursor.fetchone():
            return False
        
        # Store reading
        cursor.execute("""
            INSERT INTO readings (sensor_id, value, timestamp, quality)
            VALUES (?, ?, ?, ?)
        """, (sensor_id, value, now, quality))
        
        # Update sensor's current value and last_seen
        cursor.execute("""
            UPDATE sensors SET value = ?, last_seen = ?
            WHERE id = ?
        """, (value, now, sensor_id))
        
        self.conn.commit()
        return True
    
    def get_latest(self, sensor_id: str) -> Optional[SensorReading]:
        """Get most recent reading for a sensor"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT sensor_id, value, timestamp, quality
            FROM readings
            WHERE sensor_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (sensor_id,))
        
        row = cursor.fetchone()
        if row:
            return SensorReading(*row)
        return None
    
    def get_history(self, sensor_id: str, hours: int = 24) -> List[SensorReading]:
        """Get readings from the last N hours"""
        cursor = self.conn.cursor()
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        
        cursor.execute("""
            SELECT sensor_id, value, timestamp, quality
            FROM readings
            WHERE sensor_id = ? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (sensor_id, cutoff))
        
        return [SensorReading(*row) for row in cursor.fetchall()]
    
    def get_stats(self, sensor_id: str, hours: int = 24) -> Dict:
        """Compute statistics over N hours"""
        readings = self.get_history(sensor_id, hours)
        if not readings:
            return {"count": 0}
        
        values = [r.value for r in readings]
        
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": statistics.mean(values),
            "std_dev": statistics.stdev(values) if len(values) > 1 else 0.0,
            "latest": values[0] if values else None
        }
    
    def get_anomalies(self, sensor_id: str, threshold_std: float = 2.0) -> List[SensorReading]:
        """Find readings that deviate > N standard deviations from mean"""
        readings = self.get_history(sensor_id, hours=24)
        if not readings:
            return []
        
        values = [r.value for r in readings]
        if len(values) < 2:
            return []
        
        mean = statistics.mean(values)
        std_dev = statistics.stdev(values)
        
        return [
            r for r in readings
            if abs((r.value - mean) / std_dev) > threshold_std
        ]
    
    def fleet_status(self) -> List[Dict]:
        """Get status of all sensors"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, name, type, location, value, battery_pct, last_seen
            FROM sensors
            ORDER BY last_seen DESC NULLS LAST
        """)
        
        return [
            {
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "location": row[3],
                "current_value": row[4],
                "battery_pct": row[5],
                "last_seen": row[6]
            }
            for row in cursor.fetchall()
        ]
    
    def alert_check(self) -> Dict:
        """Check for sensors needing attention"""
        cursor = self.conn.cursor()
        now = datetime.now()
        one_hour_ago = (now - timedelta(hours=1)).isoformat()
        
        # Low battery
        cursor.execute("""
            SELECT id, name, battery_pct FROM sensors WHERE battery_pct < 10
        """)
        low_battery = [
            {"id": row[0], "name": row[1], "battery_pct": row[2]}
            for row in cursor.fetchall()
        ]
        
        # Stale readings
        cursor.execute("""
            SELECT id, name, last_seen FROM sensors 
            WHERE last_seen IS NULL OR last_seen < ?
        """, (one_hour_ago,))
        stale_sensors = [
            {"id": row[0], "name": row[1], "last_seen": row[2]}
            for row in cursor.fetchall()
        ]
        
        return {
            "low_battery": low_battery,
            "stale_sensors": stale_sensors
        }
    
    def calibrate(self, sensor_id: str, offset: float, scale: float = 1.0) -> bool:
        """Store calibration parameters for a sensor"""
        cursor = self.conn.cursor()
        
        # Verify sensor exists
        cursor.execute("SELECT id FROM sensors WHERE id = ?", (sensor_id,))
        if not cursor.fetchone():
            return False
        
        now = datetime.now().isoformat()
        cursor.execute("""
            INSERT OR REPLACE INTO calibration 
            (sensor_id, offset, scale, calibrated_at)
            VALUES (?, ?, ?, ?)
        """, (sensor_id, offset, scale, now))
        
        self.conn.commit()
        return True
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    """CLI interface"""
    parser = argparse.ArgumentParser(description="Nano-Sensor Network Controller")
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Fleet command
    subparsers.add_parser("fleet", help="Show fleet status")
    
    # Ingest command
    ingest = subparsers.add_parser("ingest", help="Ingest sensor reading")
    ingest.add_argument("sensor_id", help="Sensor ID")
    ingest.add_argument("value", type=float, help="Reading value")
    ingest.add_argument("--quality", type=float, default=1.0, help="Reading quality (0-1)")
    
    # Anomalies command
    anomalies = subparsers.add_parser("anomalies", help="Find anomalous readings")
    anomalies.add_argument("sensor_id", help="Sensor ID")
    anomalies.add_argument("--threshold", type=float, default=2.0, help="Std deviation threshold")
    
    args = parser.parse_args()
    
    network = NanoSensorNetwork()
    
    try:
        if args.command == "fleet":
            status = network.fleet_status()
            print(json.dumps(status, indent=2))
        
        elif args.command == "ingest":
            success = network.ingest_reading(args.sensor_id, args.value, args.quality)
            if success:
                print(f"✓ Ingested {args.value} for {args.sensor_id}")
            else:
                print(f"✗ Sensor {args.sensor_id} not found")
        
        elif args.command == "anomalies":
            anom = network.get_anomalies(args.sensor_id, args.threshold)
            if anom:
                print(f"Found {len(anom)} anomalies:")
                for reading in anom:
                    print(f"  {reading.timestamp}: {reading.value} (quality: {reading.quality})")
            else:
                print(f"No anomalies found for {args.sensor_id}")
        
        else:
            parser.print_help()
    
    finally:
        network.close()


if __name__ == "__main__":
    main()
