import json,unittest
from unittest.mock import MagicMock
import redis
from observation_reader import read_observation
class ObservationReaderTests(unittest.TestCase):
 def test_each_batch_is_executed_independently_and_mapping_is_preserved(self):
  cache=MagicMock();pipes=[]
  for ids,latest in [(['a','b'],['a']),(['c'],[])]:
   pipe=MagicMock();pipe.__enter__.return_value=pipe
   pipe.execute.return_value=[[json.dumps({'eid':x}) for x in ids]]+([[json.dumps({'value':1}) for _ in latest]] if latest else [])
   pipes.append(pipe)
  cache.pipeline.side_effect=pipes
  receipts,latest=read_observation(cache,[{'eid':x} for x in ['a','b','c']],lambda r:r['eid']=='a','p:',batch_size=2)
  self.assertEqual(set(receipts),{'a','b','c'});self.assertEqual(latest['a']['value'],1);self.assertIsNone(latest['c'])
  self.assertEqual(cache.pipeline.call_count,2)
  self.assertEqual(pipes[0].hmget.call_args.args,('p:receipt',['a','b']))
  self.assertEqual(pipes[1].mget.call_count,0)
 def test_retry_is_bounded_and_partial_data_never_returned(self):
  cache=MagicMock();pipe=cache.pipeline.return_value.__enter__.return_value
  pipe.execute.side_effect=redis.TimeoutError('timeout')
  with self.assertRaises(redis.TimeoutError):read_observation(cache,[{'eid':'a'}],lambda _:True,'p:')
  self.assertEqual(pipe.execute.call_count,2)
 def test_retry_recovers(self):
  cache=MagicMock();pipe=cache.pipeline.return_value.__enter__.return_value
  pipe.execute.side_effect=[redis.TimeoutError('timeout'),[[None]]]
  receipts,latest=read_observation(cache,[{'eid':'a'}],lambda _:False,'p:')
  self.assertEqual(receipts,{'a':None})
 def test_incomplete_reply_is_not_offline(self):
  cache=MagicMock();cache.pipeline.return_value.__enter__.return_value.execute.return_value=[[]]
  with self.assertRaises(ValueError):read_observation(cache,[{'eid':'a'}],lambda _:False,'p:')
