import unittest
from pymysql.cursors import RE_INSERT_VALUES
from sync_autosensor import difference, upsert_sql


class SyncTests(unittest.TestCase):
    def test_diff_updates_nulls_inserts_and_retains_target_only_rows(self):
        source=[dict(eid='same',channel='oa'),dict(eid='change',channel=None),dict(eid='new',channel='kepserver')]
        target=[dict(eid='same',channel='oa'),dict(eid='change',channel='oa'),dict(eid='target-only',channel='oa')]
        additions,updates,extra=difference(source,target,['eid','channel'])
        self.assertEqual([r['eid'] for r in additions],['new'])
        self.assertEqual([r['eid'] for r in updates],['change'])
        self.assertEqual(extra,1)

    def test_upsert_escapes_identifiers_and_supports_real_batching(self):
        sql=upsert_sql(['eid','group','table','channel'])
        self.assertIsNotNone(RE_INSERT_VALUES.match(sql))
        self.assertIn('`group`',sql)
        self.assertNotIn('`eid`=VALUES',sql)


if __name__=='__main__':
    unittest.main()
