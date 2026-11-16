import tempfile,time,unittest
from pathlib import Path
from durable_inbox import DiskInbox
from mqtt_collector import Packet

class DurableInboxTests(unittest.TestCase):
 def test_restart_retry_and_ack(self):
  with tempfile.TemporaryDirectory() as root:
   path=str(Path(root)/'jobs.db');q=DiskInbox(path,100)
   q.push(Packet('a',b'first',1,time.monotonic()));q.push(Packet('a',b'next',2,time.monotonic()))
   p=q.pop(timeout=0);self.assertEqual(p.payload,b'first');self.assertIsNone(q.pop(timeout=0))
   q.release(p);self.assertEqual(q.snapshot()['pending_packets'],2)
   q.db.close();q=DiskInbox(path,100);p=q.pop(timeout=0);self.assertEqual(p.payload,b'first')
   q.acknowledge(p);q.release(p);self.assertEqual(q.pop(timeout=0).payload,b'next');q.db.close()
 def test_limit_keeps_old_data(self):
  with tempfile.TemporaryDirectory() as root:
   q=DiskInbox(str(Path(root)/'jobs.db'),5)
   self.assertEqual(q.push(Packet('a',b'12345',1,time.monotonic())),[])
   self.assertEqual(len(q.push(Packet('b',b'x',2,time.monotonic()))),1)
   self.assertEqual(q.pop(timeout=0).payload,b'12345');self.assertEqual(q.clear(),[]);q.db.close()
 def test_rejected_data_retains_budget(self):
  with tempfile.TemporaryDirectory() as root:
   path=str(Path(root)/'jobs.db');q=DiskInbox(path,5);q.push(Packet('a',b'bad',1,time.monotonic()))
   p=q.pop(timeout=0);q.reject(p,'invalid json');q.release(p)
   self.assertEqual(q.snapshot()['pending_packets'],0);self.assertEqual(q.bytes,3);q.db.close()
   q=DiskInbox(path,5);self.assertEqual(q.bytes,3);q.db.close()
class ExpiryIsolationTests(unittest.TestCase):
 def test_old_packets_isolated_and_live_budget_reclaimed(self):
  with tempfile.TemporaryDirectory() as root:
   path=str(Path(root)/'jobs.db');q=DiskInbox(path,10,max_age_seconds=60);now=int(time.time()*1000)
   q.push(Packet('a',b'old',now-120000,time.monotonic()-120));q.push(Packet('a',b'fresh',now,time.monotonic()))
   self.assertEqual(q.quarantine_expired(),1)
   self.assertEqual(q.snapshot()['quarantined_packets'],1);self.assertEqual(q.bytes,5)
   packet=q.pop(timeout=0);self.assertEqual(packet.payload,b'fresh');q.release(packet);q.db.close()
   q=DiskInbox(path,10,max_age_seconds=60)
   self.assertEqual(q.snapshot()['pending_packets'],1)
   self.assertEqual(q.db.execute('SELECT payload FROM inbox WHERE quarantined_at<>0').fetchone()[0],b'old');q.db.close()
 def test_inflight_packets_not_removed(self):
  with tempfile.TemporaryDirectory() as root:
   q=DiskInbox(str(Path(root)/'jobs.db'),100,max_age_seconds=60);now=int(time.time()*1000)
   q.push(Packet('a',b'old',now-120000,time.monotonic()-120));packet=q.pop(timeout=0)
   self.assertEqual(q.quarantine_expired(),0)
   q.release(packet);self.assertEqual(q.quarantine_expired(),1);q.db.close()
