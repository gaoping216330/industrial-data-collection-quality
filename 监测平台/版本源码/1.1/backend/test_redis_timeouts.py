import socket,threading,time,unittest
from unittest.mock import patch
import redis
import quality_engine

class RedisTimeoutTests(unittest.TestCase):
 def test_silent_peer_does_not_hold_worker_indefinitely(self):
  listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);release=threading.Event()
  def stall():
   connection,_=listener.accept()
   try:release.wait(6)
   finally:connection.close()
  worker=threading.Thread(target=stall,daemon=True);worker.start()
  try:
   with patch('quality_engine.config',return_value={'redis':{'host':'127.0.0.1','port':listener.getsockname()[1]}}):
    client=quality_engine.redis_connect();started=time.monotonic()
    with self.assertRaises(redis.RedisError):client.ping()
    self.assertLess(time.monotonic()-started,5)
    client.close()
  finally:release.set();worker.join(2);listener.close()
