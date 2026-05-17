import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pendulum import DateTime

from dags.traffic_intensity import (
    fetch_tomtom_traffic_intensity,
    create_traffic_intensity_table_if_not_exists,
    insert_traffic_data_to_snowflake,
    process_traffic_intensity,
    save_traffic_data_to_disk,
    load_traffic_data_from_disk,
    purge_traffic_data_from_disk,
)


def test_fetch_tomtom_traffic_intensity_success(monkeypatch):
    expected_json = {
        'flowSegmentData': {
            'currentSpeed': 23.5,
            'freeFlowSpeed': 45.0,
            'currentTravelTime': 70.0,
            'freeFlowTravelTime': 40.0,
            'confidence': 0.98,
        }
    }

    response_mock = Mock()
    response_mock.raise_for_status = Mock()
    response_mock.json.return_value = expected_json

    get_mock = Mock(return_value=response_mock)
    monkeypatch.setattr('dags.traffic_intensity.requests.get', get_mock)

    result = fetch_tomtom_traffic_intensity(lat=50.0, lon=20.0, api_key='key')

    assert result == {
        'current_speed': 23.5,
        'free_flow_speed': 45.0,
        'current_travel_time': 70.0,
        'free_flow_travel_time': 40.0,
        'confidence': 0.98,
    }
    get_mock.assert_called_once()


def test_insert_traffic_data_to_snowflake_calls_create_table_and_insert(monkeypatch):
    hook = Mock()
    traffic_data = [
        {
            'STOP_NAME': "Test Stop",
            'stop_lat': 50.0,
            'stop_lon': 20.0,
            'current_speed': 10.0,
            'free_flow_speed': 20.0,
            'current_travel_time': 50.0,
            'free_flow_travel_time': 25.0,
            'confidence': 0.9,
            'load_timestamp': '2026-05-17',
        }
    ]

    insert_traffic_data_to_snowflake(traffic_data, hook)

    assert hook.run.call_count == 2
    assert 'CREATE TABLE IF NOT EXISTS' in hook.run.call_args_list[0][0][0]
    assert 'INSERT INTO GTFS_TEST.SCHEDULE.TRAFFIC_INTENSITY' in hook.run.call_args_list[1][0][0]
    assert "Test Stop" in hook.run.call_args_list[1][0][0]


def test_insert_traffic_data_to_snowflake_empty_only_creates_table():
    hook = Mock()
    insert_traffic_data_to_snowflake([], hook)

    assert hook.run.call_count == 1
    assert 'CREATE TABLE IF NOT EXISTS' in hook.run.call_args[0][0]


def test_process_traffic_intensity_uses_stubbed_fetch(monkeypatch):
    fake_stops = {
        'Stop A': (50.0, 20.0),
        'Stop B': (50.1, 20.1),
    }

    monkeypatch.setattr('dags.traffic_intensity.STOPS_INCLUDED', fake_stops)

    fetched_values = [
        {
            'current_speed': 10.0,
            'free_flow_speed': 20.0,
            'current_travel_time': 30.0,
            'free_flow_travel_time': 40.0,
            'confidence': 0.5,
        },
        {
            'current_speed': 12.0,
            'free_flow_speed': 22.0,
            'current_travel_time': 32.0,
            'free_flow_travel_time': 42.0,
            'confidence': 0.8,
        },
    ]

    def fake_fetch(lat, lon, api_key):
        return fetched_values.pop(0)

    monkeypatch.setattr('dags.traffic_intensity.fetch_tomtom_traffic_intensity', fake_fetch)

    result = process_traffic_intensity(logical_date=DateTime(2026, 5, 17), api_keys=['key1', 'key2'])

    assert len(result) == 2
    assert result[0]['STOP_NAME'] == 'Stop A'
    assert result[1]['STOP_NAME'] == 'Stop B'
    assert result[0]['current_speed'] == 10.0
    assert result[1]['current_speed'] == 12.0


def test_save_load_and_purge_traffic_data_roundtrip(tmp_path):
    traffic_data = [
        {
            'STOP_NAME': 'Test Stop',
            'stop_lat': 50.0,
            'stop_lon': 20.0,
            'current_speed': 10.0,
            'free_flow_speed': 20.0,
            'current_travel_time': 50.0,
            'free_flow_travel_time': 25.0,
            'confidence': 0.9,
            'load_timestamp': '2026-05-17',
        }
    ]

    file_path = save_traffic_data_to_disk(traffic_data, DateTime(2026, 5, 17, 7, 15), str(tmp_path))
    assert Path(file_path).exists()

    loaded_data = load_traffic_data_from_disk(str(tmp_path))
    assert loaded_data == traffic_data

    purge_traffic_data_from_disk(str(tmp_path))
    assert not any(Path(str(tmp_path)).glob('traffic_*.json'))


def test_load_traffic_data_from_disk_skips_invalid_json(tmp_path, capsys):
    valid = tmp_path / 'traffic_20260517_0715.json'
    invalid = tmp_path / 'traffic_20260517_0730.json'
    valid.write_text(json.dumps([{'STOP_NAME': 'Valid Stop'}]))
    invalid.write_text('{ invalid json }')

    result = load_traffic_data_from_disk(str(tmp_path))

    assert result == [{'STOP_NAME': 'Valid Stop'}]
    captured = capsys.readouterr()
    assert 'Error loading' in captured.out
