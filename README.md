# BlackRoad Nano Sensor

[![CI](https://github.com/BlackRoad-OS/blackroad-nano-sensor/actions/workflows/ci.yml/badge.svg)](https://github.com/BlackRoad-OS/blackroad-nano-sensor/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-proprietary-red.svg)](LICENSE)
[![BlackRoad OS](https://img.shields.io/badge/BlackRoad-OS-black.svg)](https://blackroad.io)

> Climate & environmental sensor network: trends, anomaly detection, time-series export

Part of the **BlackRoad OS** health & science platform — production-grade implementations with SQLite persistence, pytest coverage, and CI/CD.

## Features

- **ClimateRecord** dataclass: temperature, humidity, pressure, precipitation, wind speed, CO₂, PM2.5, UV index, noise, lux
- **SensorStation** fleet management
- `register_station(station_id, location, lat, lon)` — register a sensor node
- `log_record(station_id, temp, humidity, ...)` — persist a climate reading to SQLite
- `get_trend(station_id, metric, days)` — linear regression slope and direction
- `detect_extreme(station_id, threshold_pct=95)` — percentile + WHO/NOAA safe-range violations
- `calculate_anomaly(station_id, baseline_days=30)` — Z-score departure from baseline
- `export_timeseries(station_id, metric, format)` — CSV or JSON export
- `fleet_status()` — overview of all stations and last readings

## Quick Start

```bash
python src/nano_sensor.py register --station S001 --location "Point Reyes, CA" --lat 38.0 --lon -122.8
python src/nano_sensor.py log --station S001 --temp 18.5 --humidity 72 --pressure 1013 --co2 415
python src/nano_sensor.py trend --station S001 --metric temp_c --days 30
python src/nano_sensor.py extreme --station S001 --pct 95
python src/nano_sensor.py anomaly --station S001 --baseline 30 --compare 7
python src/nano_sensor.py export --station S001 --metric temp_c --format csv
python src/nano_sensor.py fleet
```

## Supported Metrics

`temp_c` · `humidity_pct` · `pressure_hpa` · `precipitation_mm` · `wind_ms` · `co2_ppm` · `pm25_ugm3` · `uv_index` · `noise_db` · `lux`

## Installation

```bash
# No dependencies required — pure Python stdlib + sqlite3
python src/nano_sensor.py --help
```

## Testing

```bash
pip install pytest pytest-cov
pytest tests/ -v --cov=src
```

## Data Storage

All data is stored locally in `~/.blackroad/nano-sensor.db` (SQLite). Zero external dependencies.

## License

Proprietary — © BlackRoad OS, Inc. All rights reserved.
