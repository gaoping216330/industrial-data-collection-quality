import unittest
from display_group import display_group_token

class DisplayGroupTests(unittest.TestCase):
    def test_same_business_device_merges_distinct_collectors(self):
        base=dict(source='kepserver',department='炼铁厂',process='一号高炉',equipment='出铁场除尘',group='')
        self.assertEqual(display_group_token(dict(base,device_key='plc1')),display_group_token(dict(base,device_key='plc2')))
        self.assertNotEqual(display_group_token(base),display_group_token(dict(base,process='二号高炉')))
    def test_unknown_devices_stay_separate_but_known_device_merges_sources(self):
        self.assertNotEqual(display_group_token(dict(source='kepserver',device_key='plc1')),display_group_token(dict(source='kepserver',device_key='plc2')))
        base=dict(department='dept',equipment='equipment')
        self.assertEqual(display_group_token(dict(base,source='oa')),display_group_token(dict(base,source='kepserver')))
