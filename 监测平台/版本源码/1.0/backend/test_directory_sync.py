import unittest
from datetime import datetime
from unittest.mock import MagicMock

from directory_sync import reconcile
from pymysql.cursors import RE_INSERT_VALUES


class DirectoryTests(unittest.TestCase):
    def test_identity_switch_and_removal_reset_old_monitor_state(self):
        cur=MagicMock()
        cur.fetchall.return_value=[dict(eid='switch',source='oa',device_key='old'),dict(eid='removed',source='kepserver',device_key='gone')]
        roster={'switch':dict(eid='switch',source='kepserver',device_key='new',createdBy='yyh')}
        reset=reconcile(cur,roster,datetime.now())
        self.assertEqual(reset,{'switch','removed'})
        statements=[call.args[0] for call in cur.execute.call_args_list]
        self.assertTrue(any('resolution_note' in sql for sql in statements))
        self.assertTrue(any('DELETE p FROM dq_point_quality_current' in sql for sql in statements))
        self.assertFalse(any('DELETE m FROM dq_model_quality_current' in sql for sql in statements))
        self.assertTrue(any('DIRECTORY_CHANGED' in sql for sql in statements))
        writes=[call for call in cur.executemany.call_args_list if 'dq_point_quality_current' in call.args[0]]
        sql,rows=writes[0].args
        self.assertIsNotNone(RE_INSERT_VALUES.match(sql))
        self.assertEqual(sql.count('%s'),len(rows[0]))
        self.assertLess(sql.index('status=IF'),sql.index('`source`=VALUES'))

    def test_empty_directory_does_not_clear_monitoring_state(self):
        cur=MagicMock()
        with self.assertRaises(ValueError):
            reconcile(cur,{},datetime.now())
        cur.execute.assert_not_called()


if __name__=='__main__':
    unittest.main()
