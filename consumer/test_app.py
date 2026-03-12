import pytest
from datetime import datetime, timezone
from pydantic import ValidationError

# Import the model to test
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from app import SensorData

def test_valid_sensor_data():
    """Test that a valid payload is accepted without raising exceptions."""
    data = {
        "device_id": "sensor-123",
        "timestamp_utc": "2026-03-12T16:57:11.439077Z",
        "temperature_celsius": 25.5,
        "humidity_percent": 60.0
    }
    model = SensorData(**data)
    assert model.device_id == "sensor-123"
    assert model.temperature_celsius == 25.5
    assert model.humidity_percent == 60.0
    assert isinstance(model.timestamp_utc, datetime)

def test_invalid_temperature_too_high():
    """Test that validation fails when temperature exceeds upper bound."""
    data = {
        "device_id": "sensor-123",
        "timestamp_utc": "2026-03-12T16:57:11.439077Z",
        "temperature_celsius": 150.0, # Too high (max 100)
        "humidity_percent": 60.0
    }
    with pytest.raises(ValidationError) as exc_info:
        SensorData(**data)
    
    assert "temperature_celsius" in str(exc_info.value)
    assert "Input should be less than or equal to 100" in str(exc_info.value)

def test_invalid_temperature_too_low():
    """Test that validation fails when temperature exceeds lower bound."""
    data = {
        "device_id": "sensor-123",
        "timestamp_utc": "2026-03-12T16:57:11.439077Z",
        "temperature_celsius": -60.0, # Too low (min -50)
        "humidity_percent": 60.0
    }
    with pytest.raises(ValidationError) as exc_info:
        SensorData(**data)
        
    assert "temperature_celsius" in str(exc_info.value)
    assert "Input should be greater than or equal to -50" in str(exc_info.value)

def test_invalid_humidity_negative():
    """Test that validation fails when humidity is negative."""
    data = {
        "device_id": "sensor-123",
        "timestamp_utc": "2026-03-12T16:57:11.439077Z",
        "temperature_celsius": 25.5,
        "humidity_percent": -5.0 # Invalid (min 0)
    }
    with pytest.raises(ValidationError) as exc_info:
        SensorData(**data)
        
    assert "humidity_percent" in str(exc_info.value)

def test_invalid_humidity_too_high():
    """Test that validation fails when humidity exceeds 100%."""
    data = {
        "device_id": "sensor-123",
        "timestamp_utc": "2026-03-12T16:57:11.439077Z",
        "temperature_celsius": 25.5,
        "humidity_percent": 105.0 # Invalid (max 100)
    }
    with pytest.raises(ValidationError) as exc_info:
        SensorData(**data)
        
    assert "humidity_percent" in str(exc_info.value)

def test_missing_required_field():
    """Test that validation fails when a required field is missing."""
    data = {
        "timestamp_utc": "2026-03-12T16:57:11.439077Z",
        "temperature_celsius": 25.5,
        "humidity_percent": 60.0
        # Missing 'device_id'
    }
    with pytest.raises(ValidationError) as exc_info:
        SensorData(**data)
    
    assert "device_id" in str(exc_info.value)
    assert "Field required" in str(exc_info.value)

def test_invalid_timestamp_format():
    """Test that validation fails for non-ISO 8601 timestamps."""
    data = {
        "device_id": "sensor-123",
        "timestamp_utc": "invalid-time",
        "temperature_celsius": 25.5,
        "humidity_percent": 60.0
    }
    with pytest.raises(ValidationError) as exc_info:
        SensorData(**data)
        
    assert "timestamp_utc" in str(exc_info.value)
