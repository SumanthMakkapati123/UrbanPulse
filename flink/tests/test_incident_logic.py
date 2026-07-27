import unittest

from incident_logic import haversine_metres, pair_key


class IncidentLogicTests(unittest.TestCase):
    def test_haversine_is_about_111_metres_for_point001_degree(self) -> None:
        self.assertTrue(110 < haversine_metres(0, 0, 0.001, 0) < 112)

    def test_pair_key_is_order_independent(self) -> None:
        self.assertEqual(pair_key("BUS-2", "BUS-1"), "BUS-1|BUS-2")
        self.assertEqual(pair_key("BUS-1", "BUS-2"), "BUS-1|BUS-2")


if __name__ == "__main__":
    unittest.main()
