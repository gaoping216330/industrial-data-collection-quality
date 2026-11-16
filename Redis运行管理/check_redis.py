"""Read status; --write-test only writes a unique expiring diagnostic key."""
import argparse
import json
from pathlib import Path
from uuid import uuid4

import redis

parser = argparse.ArgumentParser()
parser.add_argument('--write-test', action='store_true')
args = parser.parse_args()
config = json.loads(Path(__file__).with_name('redis_connection.json').read_text(encoding='utf-8'))
client = redis.Redis(**config)
try:
    assert client.ping()
    server = client.info('server')
    memory = client.info('memory')
    persistence = client.info('persistence')
    result = {
        'connected': True,
        'address': f"{config['host']}:{config['port']}",
        'redis_version': server['redis_version'],
        'used_memory': memory['used_memory_human'],
        'maxmemory': memory['maxmemory_human'],
        'eviction_policy': memory['maxmemory_policy'],
        'aof_enabled': persistence['aof_enabled'],
        'aof_write_status': persistence.get('aof_last_write_status'),
        'last_snapshot_status': persistence.get('rdb_last_bgsave_status'),
        'keys_in_selected_db': client.dbsize(),
    }
    if args.write_test:
        key = f'dq:diagnostic:{uuid4().hex}'
        try:
            client.set(key, 'connection-ok', ex=60)
            assert client.get(key) == 'connection-ok'
            result['write_read_test'] = 'passed'
        finally:
            client.delete(key)
    print(json.dumps(result, ensure_ascii=False, indent=2))
finally:
    client.close()
