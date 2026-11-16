"""Deployment check: gracefully stop Redis, restart WSL, verify saved data."""
import json
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import redis

config = json.loads(Path(__file__).with_name('redis_connection.json').read_text(encoding='utf-8'))
key = f'dq:diagnostic:persistence:{uuid4().hex}'
value = uuid4().hex
client = redis.Redis(**config)
client.set(key, value, ex=600)
subprocess.run(['wsl.exe', '-d', 'DataQualityRedis', '-u', 'root', '--',
                'service', 'redis-server', 'stop'], check=True)
subprocess.run(['wsl.exe', '--terminate', 'DataQualityRedis'], check=True)
time.sleep(3)
subprocess.run(['wsl.exe', '-d', 'DataQualityRedis', '-u', 'root', '--',
                'service', 'redis-server', 'start'], check=True)
client.close()
client = redis.Redis(**config)
for attempt in range(20):
    try:
        client.ping()
        break
    except redis.ConnectionError:
        if attempt == 19:
            raise
        time.sleep(1)
try:
    assert client.get(key) == value, 'Data was not recovered after WSL restart'
    print('PASS: diagnostic value recovered after Redis and WSL restart')
    print(json.dumps(client.config_get('dir', 'appendonly', 'appendfsync'), indent=2))
finally:
    client.delete(key)
    client.close()
