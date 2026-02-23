"""Tests for BlackRoad Nano-Sensor / Climate Tracker."""
import os, tempfile, pytest
os.environ["HOME"] = tempfile.mkdtemp()

from nano_sensor import (
    register_station, get_station, log_record, get_trend,
    detect_extreme, calculate_anomaly, export_timeseries, fleet_status,
)

@pytest.fixture
def station():
    return register_station("TEST01", "Test Location", 37.0, -122.0, 10, "multi")

def test_register_station(station):
    assert station.station_id == "TEST01"
    assert station.location == "Test Location"

def test_get_station(station):
    s = get_station("TEST01")
    assert s is not None
    assert s.latitude == 37.0

def test_log_record(station):
    r = log_record("TEST01", temp_c=20.5, humidity_pct=65, pressure_hpa=1013)
    assert r.id > 0
    assert r.temp_c == 20.5

def test_get_trend_no_data():
    register_station("EMPTY01", "Empty")
    t = get_trend("EMPTY01", "temp_c", days=7)
    assert "error" in t

def test_get_trend_with_data(station):
    for t in [15, 17, 19, 21, 23]:
        log_record("TEST01", temp_c=t, humidity_pct=60)
    trend = get_trend("TEST01", "temp_c", days=1)
    assert trend["n_readings"] >= 5
    assert trend["mean"] > 0

def test_detect_extreme(station):
    for t in [10, 11, 12, 13, 14, 15, 100]:  # 100 is extreme
        log_record("TEST01", temp_c=t, humidity_pct=50)
    r = detect_extreme("TEST01", threshold_pct=90, days=1)
    assert "extremes" in r

def test_calculate_anomaly(station):
    for _ in range(10):
        log_record("TEST01", temp_c=20, humidity_pct=60)
    r = calculate_anomaly("TEST01", baseline_days=1, compare_days=1)
    assert "metrics" in r

def test_export_csv(station):
    log_record("TEST01", temp_c=22, humidity_pct=55)
    csv_out = export_timeseries("TEST01", "temp_c", "csv", days=1)
    assert "temp_c" in csv_out

def test_export_json(station):
    log_record("TEST01", temp_c=22, humidity_pct=55)
    import json
    json_out = export_timeseries("TEST01", "temp_c", "json", days=1)
    data = json.loads(json_out)
    assert data["station_id"] == "TEST01"

def test_fleet_status(station):
    fleet = fleet_status()
    ids = [s["station_id"] for s in fleet]
    assert "TEST01" in ids
