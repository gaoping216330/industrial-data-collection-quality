import unittest

from confirmation_window import ConfirmationWindow, ConfirmedObservations
from collections import defaultdict


class ConfirmationTests(unittest.TestCase):
    def test_full_five_minute_observation_required(self):
        window = ConfirmationWindow()
        for now in range(0, 300_000, 10_000):
            self.assertEqual(window.observe('point', {'BAD'}, now), set())
        self.assertEqual(window.observe('point', {'BAD'}, 300_000), {'BAD'})

    def test_recovery_and_reason_change_reset_confirmation(self):
        window = ConfirmationWindow()
        for now in range(0, 300_000, 10_000):
            window.observe('point', {'BAD'}, now)
        window.observe('point', set(), 300_000)
        self.assertEqual(window.observe('point', {'BAD'}, 310_000), set())
        self.assertEqual(window.observe('point', {'stopped'}, 320_000), set())

    def test_monitoring_gap_and_clock_reversal_are_not_continuous(self):
        window = ConfirmationWindow()
        window.observe('point', {'BAD'}, 0)
        self.assertEqual(window.observe('point', {'BAD'}, 300_000), set())
        self.assertEqual(window.observe('point', {'BAD'}, 200_000), set())

    def test_two_five_minute_checks_mode(self):
        window = ConfirmationWindow(max_gap_ms=360_000)
        self.assertEqual(window.observe('point', {'BAD'}, 0), set())
        self.assertEqual(window.observe('point', {'BAD'}, 300_000), {'BAD'})


class EventBufferTests(unittest.TestCase):
    def test_confirmed_episode_that_recovers_between_publications_is_retained(self):
        policy=ConfirmedObservations(defaultdict(set))
        for now in range(0,310_000,10_000):
            policy.observe('eid',{'BAD'},now)
        policy.observe('eid',set(),310_000)
        self.assertEqual(policy.events,[{'eid':'eid','reason':'BAD','opened_ms':0,'closed_ms':310_000}])
        self.assertEqual(policy.active['eid'],set())
        policy.committed()
        self.assertFalse(policy.events)

    def test_recovery_of_committed_event_is_closed_once(self):
        active=defaultdict(set,{'eid':{'BAD'}})
        policy=ConfirmedObservations(active)
        self.assertEqual(policy.observe('eid',{'BAD'},0),{'BAD'})
        policy.observe('eid',set(),10_000)
        policy.observe('eid',set(),20_000)
        self.assertEqual(policy.closes,[('eid','BAD',10_000)])


if __name__ == '__main__':
    unittest.main()
