#!/usr/bin/env python3
"""
BlackRoad Nano-Sensor / Climate Tracker
Distributed environmental sensor network management with anomaly detection,
trend analysis, and time-series export.

Usage:
    python nano_sensor.py register --station S001 --location "Point Reyes, CA" --lat 38.0 --lon -122.8
    python nano_sensor.py log --station S001 --temp 18.5 --humidity 72 --pressure 1013 --precip 0.0 --wind 3.2
    python nano_sensor.py trend --station S001 --metric temp_c --days 30
    python nano_sensor.py extreme --station S001 --pct 95
    python nano_sensor.py anomaly --station S001 --baseline 30
    python nano_sensor.py export --station S001 --metric temp_c --format csv
    python nano_sensor.py fleet --status
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import statistics
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DB_PATH = Path.home() / ".blackroad" / "nano_sensor.db"

METRICS = ["temp_c", "humidity_pct", "pressure_hpa", "precipitation_mm", "wind_ms",
           "co2_ppm", "pm25_ugm3", "uv_index", "noise_db", "lux"]

METRIC_LABELS: Dict[str, str] = {
    "temp_c":          "Temperature (°C)",
    "humidity_pct":    "Relative Humidity (%)",
    "pressure_hpa":    "Atmospheric Pressure (hPa)",
    "precipitation_mm":"Precipitation (mm)",
    "wind_ms":         "Wind Speed (m/s)",
    "co2_ppm":         "CO₂ (ppm)",
    "pm25_ugm3":       "PM2.5 (µg/m³)",
    "uv_index":        "UV Index",
    "noise_db":        "Noise Level (dB)",
    "lux":             "Illuminance (lux)",
}

# WHO / NOAA threshold references for extreme detection
SAFE_RANGES: Dict[str, Tuple[float, float]] = {
    "temp_c":           (-50,   55),
    "humidity_pct":     (0,    100),
    "pressure_hpa":     (870,  1084),
    "precipitation_mm": (0,    500),
    "wind_ms":          (0,     80),
    "co2_ppm":          (300,  2000),
    "pm25_ugm3":        (0,    250),
    "uv_index":         (0,     15),
    "noise_db":         (0,    140),
    "lux":              (0,  200000),
}


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class SensorStation:
    id:          int
    station_id:  str
    location:    str
    latitude:    float
    longitude:   float
    altitude_m:  float
    sensor_type: str
    active:      bool
    installed_at: str
    notes:       str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ClimateRecord:
    id:               int
    station_id:       str
    location:         str
    recorded_at:      str
    temp_c:           float
    humidity_pct:     float
    pressure_hpa:     float
    precipitation_mm: float
    wind_ms:          float
    co2_ppm:          float = 415.0
    pm25_ugm3:        float = 0.0
    uv_index:         float = 0.0
    noise_db:         float = 0.0
    lux:              float = 0.0
    quality_flag:     int   = 1    # 1=good, 0=suspect, -1=bad
    notes:            str   = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ── Database ───────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _init_db(conn)
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS stations (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id   TEXT NOT NULL UNIQUE,
            location     TEXT NOT NULL DEFAULT '',
            latitude     REAL NOT NULL DEFAULT 0,
            longitude    REAL NOT NULL DEFAULT 0,
            altitude_m   REAL NOT NULL DEFAULT 0,
            sensor_type  TEXT NOT NULL DEFAULT 'multi',
            active       INTEGER NOT NULL DEFAULT 1,
            installed_at TEXT NOT NULL DEFAULT (datetime('now')),
            notes        TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS climate_records (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id       TEXT NOT NULL,
            location         TEXT NOT NULL DEFAULT '',
            recorded_at      TEXT NOT NULL DEFAULT (datetime('now')),
            temp_c           REAL NOT NULL DEFAULT 0,
            humidity_pct     REAL NOT NULL DEFAULT 0,
            pressure_hpa     REAL NOT NULL DEFAULT 1013.25,
            precipitation_mm REAL NOT NULL DEFAULT 0,
            wind_ms          REAL NOT NULL DEFAULT 0,
            co2_ppm          REAL NOT NULL DEFAULT 415,
            pm25_ugm3        REAL NOT NULL DEFAULT 0,
            uv_index         REAL NOT NULL DEFAULT 0,
            noise_db         REAL NOT NULL DEFAULT 0,
            lux              REAL NOT NULL DEFAULT 0,
            quality_flag     INTEGER NOT NULL DEFAULT 1,
            notes            TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_cr_station ON climate_records(station_id);
        CREATE INDEX IF NOT EXISTS idx_cr_ts      ON climate_records(recorded_at);
    """)
    conn.commit()


# ── Station management ─────────────────────────────────────────────────────────

def register_station(
    station_id: str, location: str = "",
    latitude: float = 0.0, longitude: float = 0.0,
    altitude_m: float = 0.0, sensor_type: str = "multi", notes: str = "",
) -> SensorStation:
    now = datetime.now().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT OR REPLACE INTO stations"
            "(station_id,location,latitude,longitude,altitude_m,sensor_type,active,installed_at,notes)"
            " VALUES(?,?,?,?,?,?,1,?,?)",
            (station_id, location, latitude, longitude, altitude_m, sensor_type, now, notes),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM stations WHERE station_id=?", (station_id,)).fetchone()
    return SensorStation(**dict(row))


def get_station(station_id: str) -> Optional[SensorStation]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM stations WHERE station_id=?", (station_id,)
        ).fetchone()
    return SensorStation(**dict(row)) if row else None


def fleet_status() -> List[dict]:
    with get_conn() as conn:
        stations = conn.execute("SELECT * FROM stations ORDER BY station_id").fetchall()
        result   = []
        for st in stations:
            last = conn.execute(
                "SELECT recorded_at, temp_c, humidity_pct FROM climate_records"
                " WHERE station_id=? ORDER BY recorded_at DESC LIMIT 1",
                (st["station_id"],),
            ).fetchone()
            d = dict(st)
            d["last_reading"] = dict(last) if last else None
            result.append(d)
    return result


# ── Climate data ───────────────────────────────────────────────────────────────

def log_record(
    station_id: str,
    temp_c: float,
    humidity_pct: float,
    pressure_hpa: float = 1013.25,
    precipitation_mm: float = 0.0,
    wind_ms: float = 0.0,
    co2_ppm: float = 415.0,
    pm25_ugm3: float = 0.0,
    uv_index: float = 0.0,
    noise_db: float = 0.0,
    lux: float = 0.0,
    quality_flag: int = 1,
    notes: str = "",
    recorded_at: Optional[str] = None,
) -> ClimateRecord:
    st  = get_station(station_id)
    loc = st.location if st else ""
    ts  = recorded_at or datetime.now().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO climate_records"
            "(station_id,location,recorded_at,temp_c,humidity_pct,pressure_hpa,"
            "precipitation_mm,wind_ms,co2_ppm,pm25_ugm3,uv_index,noise_db,lux,"
            "quality_flag,notes)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (station_id, loc, ts, temp_c, humidity_pct, pressure_hpa,
             precipitation_mm, wind_ms, co2_ppm, pm25_ugm3, uv_index,
             noise_db, lux, quality_flag, notes),
        )
        conn.commit()
        row_id = cur.lastrowid
    return ClimateRecord(row_id, station_id, loc, ts, temp_c, humidity_pct,
                         pressure_hpa, precipitation_mm, wind_ms, co2_ppm,
                         pm25_ugm3, uv_index, noise_db, lux, quality_flag, notes)


def _fetch_records(station_id: str, days: int) -> List[ClimateRecord]:
    since = (datetime.now() - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM climate_records WHERE station_id=? AND recorded_at>=?"
            " ORDER BY recorded_at",
            (station_id, since),
        ).fetchall()
    return [ClimateRecord(**dict(r)) for r in rows]


def get_trend(station_id: str, metric: str, days: int = 30) -> dict:
    """Linear regression trend for a metric over N days."""
    records = _fetch_records(station_id, days)
    if not records:
        return {"station_id": station_id, "metric": metric, "error": "no_data"}

    values = [getattr(r, metric, 0.0) for r in records]
    n      = len(values)
    mean_v = statistics.mean(values)
    x      = list(range(n))
    mean_x = statistics.mean(x)

    cov_xy = sum((xi - mean_x) * (yi - mean_v) for xi, yi in zip(x, values))
    var_x  = sum((xi - mean_x) ** 2 for xi in x) or 1
    slope  = cov_xy / var_x
    interc = mean_v - slope * mean_x

    # Trend magnitude normalized per day
    records_per_day = n / days if days else 1
    slope_per_day   = slope * records_per_day

    direction = "rising" if slope_per_day > 0.01 else "falling" if slope_per_day < -0.01 else "stable"
    return {
        "station_id":     station_id,
        "metric":         metric,
        "label":          METRIC_LABELS.get(metric, metric),
        "days":           days,
        "n_readings":     n,
        "mean":           round(mean_v, 4),
        "min":            round(min(values), 4),
        "max":            round(max(values), 4),
        "stddev":         round(statistics.stdev(values), 4) if n > 1 else 0.0,
        "slope_per_day":  round(slope_per_day, 6),
        "trend":          direction,
        "first":          round(values[0], 4),
        "last":           round(values[-1], 4),
        "change":         round(values[-1] - values[0], 4),
    }


def detect_extreme(station_id: str, threshold_pct: float = 95.0, days: int = 365) -> dict:
    """Flag readings above the Nth percentile for each metric."""
    records  = _fetch_records(station_id, days)
    if not records:
        return {"station_id": station_id, "extremes": {}, "error": "no_data"}

    extremes: Dict[str, dict] = {}
    for metric in METRICS:
        vals = [getattr(r, metric, 0.0) for r in records]
        if not any(v != 0 for v in vals):
            continue
        sorted_v = sorted(vals)
        pct_idx  = max(0, int(len(sorted_v) * threshold_pct / 100) - 1)
        threshold = sorted_v[pct_idx]
        extreme_recs = [
            {"id": r.id, "recorded_at": r.recorded_at, "value": getattr(r, metric)}
            for r in records if getattr(r, metric, 0.0) >= threshold
        ]
        # Also flag safe-range violations
        safe = SAFE_RANGES.get(metric)
        violations = []
        if safe:
            lo, hi = safe
            violations = [
                {"id": r.id, "recorded_at": r.recorded_at,
                 "value": getattr(r, metric), "reason": "below_safe" if getattr(r,metric)<lo else "above_safe"}
                for r in records
                if not (lo <= getattr(r, metric, lo) <= hi)
            ]
        extremes[metric] = {
            "threshold_pct":   threshold_pct,
            "threshold_value": round(threshold, 4),
            "n_extreme":       len(extreme_recs),
            "extreme_events":  extreme_recs[:10],  # cap output
            "n_violations":    len(violations),
            "violations":      violations[:5],
        }
    return {"station_id": station_id, "days": days, "extremes": extremes}


def calculate_anomaly(station_id: str, baseline_days: int = 30, compare_days: int = 7) -> dict:
    """
    Compare recent N days vs baseline to detect anomalous departures.
    Returns Z-score style deviation per metric.
    """
    baseline_recs = _fetch_records(station_id, baseline_days)
    recent_recs   = _fetch_records(station_id, compare_days)

    if not baseline_recs:
        return {"station_id": station_id, "error": "insufficient_baseline"}

    anomalies: Dict[str, dict] = {}
    for metric in METRICS:
        base_vals   = [getattr(r, metric, 0.0) for r in baseline_recs]
        recent_vals = [getattr(r, metric, 0.0) for r in recent_recs]

        if not any(v != 0 for v in base_vals):
            continue

        base_mean = statistics.mean(base_vals)
        base_std  = statistics.stdev(base_vals) if len(base_vals) > 1 else 1.0
        if not recent_vals:
            continue
        recent_mean = statistics.mean(recent_vals)
        z_score     = (recent_mean - base_mean) / (base_std or 1.0)

        anomalies[metric] = {
            "baseline_mean":  round(base_mean, 4),
            "baseline_std":   round(base_std, 4),
            "recent_mean":    round(recent_mean, 4),
            "departure":      round(recent_mean - base_mean, 4),
            "z_score":        round(z_score, 3),
            "anomalous":      abs(z_score) > 2.0,
            "label":          METRIC_LABELS.get(metric, metric),
        }

    flagged = [m for m, v in anomalies.items() if v["anomalous"]]
    return {
        "station_id":      station_id,
        "baseline_days":   baseline_days,
        "compare_days":    compare_days,
        "n_flagged":       len(flagged),
        "flagged_metrics": flagged,
        "metrics":         anomalies,
    }


def export_timeseries(station_id: str, metric: str, fmt: str = "csv", days: int = 30) -> str:
    """Export a single metric time series as CSV or JSON."""
    records = _fetch_records(station_id, days)
    data    = [
        {"recorded_at": r.recorded_at, "value": getattr(r, metric, 0.0),
         "quality_flag": r.quality_flag}
        for r in records
    ]
    if fmt == "json":
        return json.dumps({
            "station_id": station_id, "metric": metric,
            "label": METRIC_LABELS.get(metric, metric),
            "days": days, "n": len(data), "series": data,
        }, indent=2)

    out    = StringIO()
    writer = csv.writer(out)
    writer.writerow(["recorded_at", metric, "quality_flag"])
    for row in data:
        writer.writerow([row["recorded_at"], row["value"], row["quality_flag"]])
    return out.getvalue()


# ── CLI ────────────────────────────────────────────────────────────────────────

def _print(obj):
    print(json.dumps(obj, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(description="BlackRoad Nano-Sensor / Climate Tracker")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("register", help="Register a sensor station")
    p.add_argument("--station",  required=True, dest="station_id")
    p.add_argument("--location", default="")
    p.add_argument("--lat",      type=float, default=0, dest="latitude")
    p.add_argument("--lon",      type=float, default=0, dest="longitude")
    p.add_argument("--alt",      type=float, default=0, dest="altitude_m")
    p.add_argument("--type",     default="multi", dest="sensor_type")
    p.add_argument("--notes",    default="")

    p = sub.add_parser("log", help="Log a climate reading")
    p.add_argument("--station",   required=True, dest="station_id")
    p.add_argument("--temp",      type=float, required=True, dest="temp_c")
    p.add_argument("--humidity",  type=float, required=True, dest="humidity_pct")
    p.add_argument("--pressure",  type=float, default=1013.25, dest="pressure_hpa")
    p.add_argument("--precip",    type=float, default=0, dest="precipitation_mm")
    p.add_argument("--wind",      type=float, default=0, dest="wind_ms")
    p.add_argument("--co2",       type=float, default=415, dest="co2_ppm")
    p.add_argument("--pm25",      type=float, default=0, dest="pm25_ugm3")
    p.add_argument("--uv",        type=float, default=0, dest="uv_index")
    p.add_argument("--noise",     type=float, default=0, dest="noise_db")
    p.add_argument("--lux",       type=float, default=0)
    p.add_argument("--quality",   type=int,   default=1, dest="quality_flag")
    p.add_argument("--notes",     default="")

    p = sub.add_parser("trend", help="Metric trend over N days")
    p.add_argument("--station", required=True, dest="station_id")
    p.add_argument("--metric",  required=True, choices=METRICS)
    p.add_argument("--days",    type=int, default=30)

    p = sub.add_parser("extreme", help="Detect extreme readings")
    p.add_argument("--station", required=True, dest="station_id")
    p.add_argument("--pct",     type=float, default=95, dest="threshold_pct")
    p.add_argument("--days",    type=int,   default=365)

    p = sub.add_parser("anomaly", help="Anomaly detection vs baseline")
    p.add_argument("--station",  required=True, dest="station_id")
    p.add_argument("--baseline", type=int, default=30, dest="baseline_days")
    p.add_argument("--compare",  type=int, default=7,  dest="compare_days")

    p = sub.add_parser("export", help="Export time series")
    p.add_argument("--station", required=True, dest="station_id")
    p.add_argument("--metric",  required=True, choices=METRICS)
    p.add_argument("--format",  choices=["csv","json"], default="csv", dest="fmt")
    p.add_argument("--days",    type=int, default=30)

    p = sub.add_parser("fleet", help="Fleet status overview")

    args = parser.parse_args()

    if args.cmd == "register":
        st = register_station(args.station_id, args.location, args.latitude,
                              args.longitude, args.altitude_m, args.sensor_type, args.notes)
        _print({"status": "registered", "station": st.to_dict()})

    elif args.cmd == "log":
        r = log_record(
            args.station_id, args.temp_c, args.humidity_pct, args.pressure_hpa,
            args.precipitation_mm, args.wind_ms, args.co2_ppm, args.pm25_ugm3,
            args.uv_index, args.noise_db, args.lux, args.quality_flag, args.notes,
        )
        _print({"status": "logged", "id": r.id, "station": r.station_id,
                "temp_c": r.temp_c, "recorded_at": r.recorded_at})

    elif args.cmd == "trend":
        _print(get_trend(args.station_id, args.metric, args.days))

    elif args.cmd == "extreme":
        _print(detect_extreme(args.station_id, args.threshold_pct, args.days))

    elif args.cmd == "anomaly":
        _print(calculate_anomaly(args.station_id, args.baseline_days, args.compare_days))

    elif args.cmd == "export":
        print(export_timeseries(args.station_id, args.metric, args.fmt, args.days))

    elif args.cmd == "fleet":
        _print(fleet_status())


if __name__ == "__main__":
    main()
