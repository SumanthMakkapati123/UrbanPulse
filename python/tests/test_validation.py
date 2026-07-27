import unittest

from urbanpulse.validation import validate_air_quality, validate_bus_gps


def valid_aq() -> dict[str, object]:
    return {
        "sensor_id": "AQ-0001",
        "zone": "CENTRAL",
        "pm25": 80.0,
        "pm10": 120.0,
        "no2": 20.0,
        "aqi": 175,
        "timestamp": "2026-07-18T10:00:00Z",
    }


def valid_bus() -> dict[str, object]:
    return {
        "bus_id": "BUS-00001",
        "route_id": "R101",
        "lat": 12.9716,
        "lon": 77.5946,
        "speed_kmh": 35,
        "occupancy_pct": 70,
        "timestamp": "2026-07-18T10:00:00Z",
    }


class ValidationTests(unittest.TestCase):
    def test_valid_air_quality_event(self) -> None:
        self.assertEqual(validate_air_quality(valid_aq()), [])

    def test_null_aqi_is_rejected(self) -> None:
        event = valid_aq()
        event["aqi"] = None
        self.assertEqual(
            [error.error_type for error in validate_air_quality(event)], ["NULL_AQI"]
        )

    def test_out_of_range_aqi_is_rejected(self) -> None:
        event = valid_aq()
        event["aqi"] = 501
        self.assertIn(
            "AQI_OUT_OF_RANGE", [error.error_type for error in validate_air_quality(event)]
        )

    def test_impossible_gps_is_rejected(self) -> None:
        event = valid_bus()
        event["lat"] = 91
        self.assertIn("IMPOSSIBLE_GPS", [e.error_type for e in validate_bus_gps(event)])

    def test_missing_field_is_rejected(self) -> None:
        event = valid_bus()
        del event["route_id"]
        errors = validate_bus_gps(event)
        self.assertEqual(errors[0].error_type, "MISSING_FIELD")
        self.assertIn("route_id", errors[0].reason)


if __name__ == "__main__":
    unittest.main()
