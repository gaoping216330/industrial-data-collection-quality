import unittest
from unittest.mock import Mock
from flask import Flask
from statistics_api import aggregate,install_statistics

class StatisticsTests(unittest.TestCase):
    def test_normalize_totals(self):
        rows=[dict(department=' 炼钢厂 ',process=None,owner_code='a',owner_name='袁银和',point_count=10,stopped_count=3,abnormal_count=4),dict(department='炼钢厂',process='',owner_code='a',owner_name='袁银和',point_count=5,stopped_count=2,abnormal_count=2)]
        result=aggregate(rows)
        self.assertEqual(len(result['items']),1)
        self.assertEqual(result['point_count'],15)
        self.assertEqual(result['stopped_count'],5)
        self.assertEqual(result['abnormal_count'],6)
    def test_drill_exact_filters(self):
        app=Flask(__name__);query=Mock(side_effect=[[{'n':2}],[]]);install_statistics(app,query,lambda rows:rows,lambda key,load:load())
        response=app.test_client().get('/api/stopped-points?department=炼钢厂&process=&owner_code=a&page=2')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['total'],2)
        sql,params=query.call_args.args
        self.assertIn("data_status IN ('从未收到','停止发送')",sql)
        self.assertEqual(params,['炼钢厂','','a',50])
        self.assertIn('COALESCE(quality_check,0)=0',sql)

    def test_affected_drill_uses_union_not_sum(self):
        app=Flask(__name__);query=Mock(side_effect=[[{'n':1}],[]]);install_statistics(app,query,lambda rows:rows,lambda key,load:load())
        response=app.test_client().get('/api/stopped-points?metric=affected')
        self.assertEqual(response.status_code,200)
        sql=query.call_args.args[0]
        self.assertIn("status='异常' OR data_status IN",sql)
        self.assertIn('COALESCE(quality_check,0)=0',sql)
