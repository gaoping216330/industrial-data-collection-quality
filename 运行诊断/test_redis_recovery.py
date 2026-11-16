import unittest
from unittest.mock import patch,MagicMock
from redis_recovery import Recovery
class RedisRecoveryTests(unittest.TestCase):
 def recovery(self):
  r=Recovery(MagicMock());r.started-=200;return r
 def test_healthy_does_not_spawn_wsl(self):
  r=self.recovery()
  with patch('redis_recovery.run') as run:
   for _ in range(10):r.check('正常')
   run.assert_not_called()
 def test_internal_failure_does_not_shutdown(self):
  r=self.recovery()
  with patch('redis_recovery.run',return_value=(1,'')) as run:
   for _ in range(5):r.check('连接失败:TimeoutError')
   self.assertEqual(run.call_count,1)
 def test_only_confirmed_relay_fault_can_repair_and_cooldown(self):
  r=self.recovery();listing=MagicMock(returncode=0,stdout='DataQualityRedis\n'.encode('utf-16-le'))
  with patch('redis_recovery.run',side_effect=[(0,'PONG'),(0,''),(0,''),(0,'')]) as run,patch('redis_recovery.subprocess.run',return_value=listing):
   for _ in range(7):r.check('连接失败:TimeoutError')
   self.assertEqual(run.call_count,4)
   self.assertTrue(any('--shutdown' in call.args[0] for call in run.call_args_list))
 def test_other_distro_prevents_shutdown(self):
  r=self.recovery();listing=MagicMock(returncode=0,stdout=b'DataQualityRedis\nUbuntu\n')
  with patch('redis_recovery.run',return_value=(0,'PONG')) as run,patch('redis_recovery.subprocess.run',return_value=listing):
   for _ in range(3):r.check('连接失败:TimeoutError')
   self.assertEqual(run.call_count,1)
