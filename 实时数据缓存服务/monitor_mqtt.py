"""Read-only samples from Redis and this client's RabbitMQ queue."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import time

import redis

from cache_service import Cache, decode_mqtt
from mqtt_collector import MqttCollector

parser = argparse.ArgumentParser()
parser.add_argument('--samples', type=int, default=1)
parser.add_argument('--interval', type=float, default=10)
args = parser.parse_args()
root = Path(__file__).resolve().parent
config = json.loads((root / 'cache_config.json').read_text(encoding='utf-8-sig'))
cache = Cache(redis.Redis(**config['redis']), config['history_minutes'])
probe = MqttCollector(config, cache, decode_mqtt)  # No MQTT connection is opened.
for index in range(args.samples):
    runtime = {key: json.loads(value) for key, value in
               cache.client.hgetall(cache.prefix + 'mqtt_runtime').items()}
    stats = cache.client.hgetall(cache.prefix + 'stats:kepserver')
    oa = cache.client.hgetall(cache.prefix + 'stats:oa')
    result = {'time': datetime.now().isoformat(timespec='seconds'), 'mqtt': runtime,
              'received_total': int(stats.get('received', 0)), 'cached_total': int(stats.get('cached', 0)),
              'oa_cached_total': int(oa.get('cached', 0)),
              'redis_memory': cache.client.info('memory')['used_memory_human']}
    try:
        result['rabbitmq'] = probe.broker_snapshot()
    except Exception as exc:
        result['rabbitmq_error'] = str(exc)
    print(json.dumps(result, ensure_ascii=False), flush=True)
    if index + 1 < args.samples:
        time.sleep(args.interval)
