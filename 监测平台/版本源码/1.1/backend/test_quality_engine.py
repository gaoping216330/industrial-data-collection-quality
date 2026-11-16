import unittest

from quality_engine import clock_state, flat_state, quality_bad, effective_rule


class QualityRulesTests(unittest.TestCase):
    def test_processing_delay_does_not_become_clock_error(self):
        receipt = {'source_time': 100_000, 'received_time': 100_500}
        self.assertIsNone(clock_state(receipt, 180_000))
        self.assertEqual(clock_state({'source_time': 90_000, 'received_time': 101_000}, 180_000),
                         '发送时间滞后')
        self.assertEqual(clock_state({'source_time': 112_000, 'received_time': 101_000}, 180_000),
                         '发送时间超前')

    def test_flatline_needs_full_receipt_window_and_no_gap(self):
        now = 1_000_000
        row = {'tag_type': 'Float'}
        receipt = {'received_time': now - 1000}
        latest = {'last_change_time': now - 400_000}
        state = {'continuous_since': now - 400_000}
        rule = {'flatline_enabled': True, 'flatline_seconds': 300, 'excluded': False}
        self.assertEqual(flat_state(row, receipt, latest, state, rule, now, 0), 'flat')
        self.assertEqual(flat_state(row, receipt, latest, state, rule, now, now - 10_000), 'unknown')
        state['continuous_since'] = now - 200_000
        self.assertEqual(flat_state(row, receipt, latest, state, rule, now, 0), 'unknown')
        state['continuous_since'] = now - 400_000
        self.assertEqual(flat_state({'tag_type':'Boolean'}, receipt, latest, state, rule, now, 0),
                         'excluded')

    def test_oa_flatline_enabled_and_explicit_rules_still_apply(self):
        row={'source':'oa','device_key':'oa:device','eid':'x'}
        self.assertTrue(effective_rule(row,{})['flatline_enabled'])
        self.assertFalse(effective_rule(row,{('point','oa','x'):{'flatline_enabled':0,'flatline_seconds':600,'tolerance':0,'excluded':0}})['flatline_enabled'])

    def test_only_explicit_bad_quality_is_bad(self):
        self.assertTrue(quality_bad('False'))
        self.assertTrue(quality_bad('BAD'))
        self.assertFalse(quality_bad(''))
        self.assertFalse(quality_bad('True'))


if __name__ == '__main__':
    unittest.main()
