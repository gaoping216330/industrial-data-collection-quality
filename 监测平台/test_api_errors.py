import unittest
from unittest.mock import patch
import pymysql
import web_app as w

class ApiErrorsTests(unittest.TestCase):
    def test_database_errors_return_readable_json(self):
        with patch.object(w,'query',side_effect=pymysql.OperationalError(2003,'unavailable')),patch.object(w.app.logger,'error'):
            response=w.app.test_client().get('/api/events?status=open')
        self.assertEqual(response.status_code,503)
        self.assertEqual(response.json['code'],'database_unavailable')
        self.assertIn('MySQL',response.json['error'])
        self.assertNotIn('<!doctype',response.get_data(as_text=True).lower())
