#!/bin/sh
set -eu
[ "$(redis-cli --raw CONFIG GET appendonly | tail -n 1)" = "yes" ]
cp -p /etc/redis/redis.conf /etc/redis/redis.conf.before-live-tuning-20261028
redis-cli CONFIG SET save ""
redis-cli CONFIG SET auto-aof-rewrite-percentage 300
redis-cli CONFIG SET auto-aof-rewrite-min-size 1073741824
redis-cli CONFIG REWRITE
redis-cli --raw CONFIG GET appendonly appendfsync save auto-aof-rewrite-percentage auto-aof-rewrite-min-size
