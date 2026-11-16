import unittest
from collections import defaultdict
from datetime import datetime, timedelta

from confirmation_window import ConfirmedObservations
from web_app import event_duration, public_rows


class DurationTests(unittest.TestCase):
    def test_closed_outage_is_measured_in_hours(self):
        start=datetime(2026,10,27,8)
        duration=event_duration(start,start+timedelta(hours=2,minutes=30))
        self.assertEqual(duration['duration_seconds'],9000)
        self.assertEqual(duration['duration_hours'],2.5)
        self.assertFalse(duration['ongoing'])

    def test_ongoing_outage_uses_current_time_and_unknown_start_is_not_invented(self):
        duration=event_duration(1000,now_ms=3_602_000)
        self.assertEqual(duration['duration_seconds'],3601)
        self.assertTrue(duration['ongoing'])
        self.assertIsNone(event_duration(None)['duration_seconds'])

    def test_outage_start_and_end_use_receipt_boundaries(self):
        policy=ConfirmedObservations(defaultdict(set))
        for stamp in range(10_000,320_000,10_000):
            policy.observe('point',{'停止发送'},stamp,starts={'停止发送':1000})
        policy.observe('point',set(),320_000,ends={'停止发送':315_000})
        self.assertEqual(policy.events,[dict(eid='point',reason='停止发送',opened_ms=1000,closed_ms=315_000)])

    def test_public_event_rows_include_duration(self):
        row=public_rows([dict(opened_at=datetime(2026,10,27,8),closed_at=datetime(2026,10,27,10))])[0]
        self.assertEqual(row['duration_hours'],2)
        self.assertEqual(row['opened_at'],'2026-10-27 08:00:00')


if __name__ == '__main__':
    unittest.main()
