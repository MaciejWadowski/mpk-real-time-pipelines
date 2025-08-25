import pytest
from unittest.mock import Mock, patch
from dags.traffic_intensity import (
    fetch_tomtom_traffic_intensity,
    get_gtfs_stops_from_snowflake,
    create_traffic_intensity_table_if_not_exists,
    insert_traffic_data_to_snowflake,
    process_traffic_intensity,
    BATCH_SIZE,
)
from pendulum import DateTime

SAMPLE_PAYLOAD = {
    "flowSegmentData": {
        "frc": "FRC4",
        "currentSpeed": 39,
        "freeFlowSpeed": 39,
        "currentTravelTime": 124,
        "freeFlowTravelTime": 124,
        "confidence": 1,
        "roadClosure": False,
        "coordinates": {
            "coordinate": [
                {"latitude": 50.074295556798191, "longitude": 19.945879839348095}
                # ... truncated for brevity ...
            ]
        },
        "@version": "4"
    }
}

@patch("dags.traffic_intensity.requests.get")
def test_fetch_tomtom_traffic_intensity_success(mock_get) -> None:
    mock_response = Mock()
    mock_response.raise_for_status = Mock()
    mock_response.json.return_value = SAMPLE_PAYLOAD
    mock_get.return_value = mock_response

    result = fetch_tomtom_traffic_intensity(50.0, 19.9, "dummy_api_key")
    assert result == {
        "current_speed": 39,
        "free_flow_speed": 39,
        "current_travel_time": 124,
        "free_flow_travel_time": 124,
        "confidence": 1
    }
    mock_get.assert_called_once()

@patch("dags.traffic_intensity.requests.get")
@patch("dags.traffic_intensity.time.sleep", return_value=None)
def test_fetch_tomtom_traffic_intensity_unsuccessfull(_, mock_get) -> None:
    mock_response = Mock()
    mock_response.raise_for_status = Mock()
    mock_response.json.return_value = SAMPLE_PAYLOAD
    mock_get.side_effect = [
        Exception("Network error"),
        Exception("Network error"),
        mock_response
    ]

    result = fetch_tomtom_traffic_intensity(50.0, 19.9, "dummy_api_key")
    assert result == {
        "current_speed": 39,
        "free_flow_speed": 39,
        "current_travel_time": 124,
        "free_flow_travel_time": 124,
        "confidence": 1
    }
    assert mock_get.call_count == 3

def test_get_gtfs_stops_from_snowflake():
    mock_hook = Mock()
    mock_hook.get_records.return_value = [
        ("StopA", 50.1, 19.9),
        ("StopB", 50.2, 20.0),
    ]
    logical_date = DateTime(2025, 7, 29)
    result = get_gtfs_stops_from_snowflake(logical_date, mock_hook)
    assert result == [
        {"STOP_NAME": "StopA", "stop_lat": 50.1, "stop_lon": 19.9},
        {"STOP_NAME": "StopB", "stop_lat": 50.2, "stop_lon": 20.0},
    ]
    assert mock_hook.get_records.called

def test_create_traffic_intensity_table_if_not_exists():
    mock_hook = Mock()
    create_traffic_intensity_table_if_not_exists(mock_hook)
    assert mock_hook.run.called
    sql = mock_hook.run.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "GTFS_TEST.SCHEDULE.TRAFFIC_INTENSITY" in sql

def test_insert_traffic_data_to_snowflake():
    mock_hook = Mock()
    traffic_data = [
        {
            "STOP_NAME": "StopA",
            "stop_lat": 50.1,
            "stop_lon": 19.9,
            "current_speed": 39,
            "free_flow_speed": 39,
            "current_travel_time": 124,
            "free_flow_travel_time": 124,
            "confidence": 1,
            "load_timestamp": "2025-07-29"
        }
    ]
    insert_traffic_data_to_snowflake(traffic_data, mock_hook)
    assert mock_hook.run.called
    sql = mock_hook.run.call_args[0][0]
    assert "INSERT INTO GTFS_TEST.SCHEDULE.TRAFFIC_INTENSITY" in sql
    assert "'StopA'" in sql

def test_insert_traffic_data_to_snowflake_empty():
    mock_hook = Mock()
    insert_traffic_data_to_snowflake([], mock_hook)
    mock_hook.run.assert_not_called()

@patch("dags.traffic_intensity.fetch_tomtom_traffic_intensity")
@patch("dags.traffic_intensity.insert_traffic_data_to_snowflake")
@patch("dags.traffic_intensity.create_traffic_intensity_table_if_not_exists")
def test_process_traffic_intensity_batches(mock_create_table, mock_insert, mock_fetch):
    # Prepare stops more than BATCH_SIZE to test batching
    stops = [
        {"STOP_NAME": f"Stop{i}", "stop_lat": 50.0 + i, "stop_lon": 19.0 + i}
        for i in range(BATCH_SIZE + 5)
    ]
    logical_date = DateTime(2025, 7, 29)
    api_key = "dummy"
    mock_hook = Mock()
    mock_fetch.return_value = {
        "current_speed": 39,
        "free_flow_speed": 39,
        "current_travel_time": 124,
        "free_flow_travel_time": 124,
        "confidence": 1
    }

    process_traffic_intensity(stops, logical_date, api_key, mock_hook)

    # Should create table once
    mock_create_table.assert_called_once_with(hook=mock_hook)
    # Should call insert twice (for 105 stops and BATCH_SIZE=100)
    assert mock_insert.call_count == 2
    # Should call fetch for each stop
    assert mock_fetch.call_count