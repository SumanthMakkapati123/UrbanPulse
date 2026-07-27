import unittest

from urbanpulse.enrichment import enrich_bus_event


class EnrichmentTests(unittest.TestCase):
    def test_enrichment_adds_all_required_schedule_fields(self) -> None:
        bus = {"bus_id": "BUS-1", "route_id": "R101", "lat": 12.9, "lon": 77.5}
        schedule = {
            "route_id": "R101",
            "scheduled_arrival_time": "08:15:00",
            "route_name": "Central - Tech Park",
            "terminal": "Tech Park",
        }
        enriched = enrich_bus_event(bus, schedule)
        self.assertEqual(enriched["scheduled_arrival_time"], "08:15:00")
        self.assertEqual(enriched["route_name"], "Central - Tech Park")
        self.assertEqual(enriched["terminal"], "Tech Park")
        self.assertTrue(enriched["schedule_match"])

    def test_enrichment_preserves_unmatched_bus(self) -> None:
        enriched = enrich_bus_event({"bus_id": "BUS-2", "route_id": "R999"}, None)
        self.assertEqual(enriched["bus_id"], "BUS-2")
        self.assertFalse(enriched["schedule_match"])


if __name__ == "__main__":
    unittest.main()
