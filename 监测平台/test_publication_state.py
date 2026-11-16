import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import publication_state
import web_app


class PublicationTests(unittest.TestCase):
    def test_atomic_marker_round_trip_without_redis(self):
        with tempfile.TemporaryDirectory() as folder,patch.object(publication_state,'PATH',Path(folder)/'publication.json'):
            self.assertIsNone(publication_state.read_publication())
            first=publication_state.write_publication({'published_at':1000})
            self.assertEqual(publication_state.read_publication(),first)
            second=publication_state.write_publication({'published_at':1000},kind='directory')
            self.assertNotEqual(first['version'],second['version'])

    def test_overview_uses_committed_summary_before_redis_poll_catches_up(self):
        publication={'version':'new','kind':'judgment','summary':{'published_at':2000,'expected':{'oa':5}}}
        with patch.object(web_app,'read_publication',return_value=publication),patch.object(web_app,'start_overview_poll'),patch.dict(web_app._overview_snapshot,{'summary':{'published_at':1000}}):
            result=web_app.app.test_client().get('/api/overview').get_json()
            self.assertEqual(result['summary']['published_at'],2000)
            self.assertEqual(result['summary']['expected']['oa'],5)
        with patch.object(web_app,'read_publication',return_value=publication),patch.object(web_app,'redis_connect') as connect:
            result=web_app.app.test_client().get('/api/publication').get_json()
            self.assertEqual(result['version'],'new')
            connect.assert_not_called()


if __name__=='__main__':
    unittest.main()
