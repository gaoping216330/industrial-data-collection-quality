import unittest
from packet_header import packet_timestamp

class HeaderTests(unittest.TestCase):
 def test_reads_outer_header_without_parsing_point_array(self):
  self.assertEqual(packet_timestamp(b'{"timestamp":15620,"values":[THIS_IS_NOT_PARSED]}'),15620)
 def test_skips_nested_timestamps_and_quoted_brackets(self):
  raw=b'{"values":[{"timestamp":16620,"id":"a}b\\\"c","t":1}],"timestamp":15620}'
  self.assertEqual(packet_timestamp(raw),15620)
 def test_timestamp_string_and_spacing(self):
  self.assertEqual(packet_timestamp(b' { "timestamp" : "15620" , "values":[]}'),15620)
 def test_invalid_header_is_rejected(self):
  for raw in (b'[]',b'{"values":[]}',b'{"timestamp":true}',b'{"timestamp":null}',b'{"timestamp":-1}'):
   with self.subTest(raw=raw):
    with self.assertRaises(ValueError):packet_timestamp(raw)
