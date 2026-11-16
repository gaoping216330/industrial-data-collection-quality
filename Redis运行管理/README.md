# 本机 Redis 运行说明

部署日期：2026-10-22。

## 已完成

- Redis 8.10.2，来自 Redis 官方签名 APT 软件源。
- Ubuntu 22.04.5 / WSL2，发行版名称 `DataQualityRedis`。
- Windows Python：`D:\python313\python.exe`；客户端 redis-py 8.1.0。
- 连接地址 `127.0.0.1:6379`，默认数据库 0；仅监听本机，无外部防火墙开放。
- Redis 数据内存上限已按实际全厂负载调整为 6.5 GiB，`noeviction` 策略。到达上限会拒绝写入，不会任意淘汰模型历史；页面“运行状态”可查看使用量。
- AOF 持久化，每秒 fsync；同时配置 RDB 快照。突然断电仍可能丢失最近约一秒或故障期间尚未保存的数据，不能视为零丢失保证。
- 当前用户登录后计划任务 `DataQualityRedis-StartAtLogon` 启动 Redis。登录前不会由此任务启动。
- 已通过 Windows Python 写入读取测试，以及 Redis/WSL 重启后的持久化恢复测试。

## 保存位置

- Linux 环境、Redis 软件、配置、日志、持久化数据都位于 D 盘虚拟磁盘：`D:\WSL\DataQualityRedis\ext4.vhdx`。
- Linux 内配置 `/etc/redis/redis.conf`，数据 `/var/lib/redis`，日志 `/var/log/redis/redis-server.log`。
- Windows 可在 Redis 所在 WSL 运行时访问 `\\wsl$\DataQualityRedis\var\lib\redis`；不要手工修改运行中的数据文件。
- 安装包：`D:\WSL\installers`。
- WSL 交换文件：`D:\WSL\wsl-swap.vhdx`，上限 2GB；`.wslconfig` 内存上限已设为 10GB，下次重启 WSL 后生效，当前实例仍沿用原 8GB 上限。
- Windows 必需的 WSL 系统组件、用户目录 `.wslconfig` 和系统计划任务由 Windows 管理，这是用户允许的 C 盘例外。

## 操作

- `start-redis.cmd`：启动 Redis 并检查连接。
- `status-redis.cmd`：查看版本、内存、持久化状态。
- `stop-redis.cmd`：正常停止 Redis，停止期间不会接收新缓存写入。
- `check_redis.py --write-test`：使用唯一临时键验证写入和读取，结束时清理测试键。
- `verify_persistence.py`：部署维护测试，会停止 Redis 并重启专用 WSL；接入正式采集后不要随意运行。

Python 连接示例：

```python
import redis

cache = redis.Redis(
    host="127.0.0.1", port=6379, db=0,
    decode_responses=True,
    socket_connect_timeout=5, socket_timeout=5,
)
print(cache.ping())
```

`redis_connection.json` 保存同样的连接参数。配置参考副本 `redis-platform.conf` 不会自动同步到 Linux，实际生效配置以 `/etc/redis/redis.conf` 为准。

## 当前接入

OA HTTP 接收、Kepserver MQTT 订阅、60 分钟滚动历史和独立质量监测平台已接入 Redis。采集目录和来源优先级由 MySQL `autosensor` 控制。页面运行于本机 8789，也可由 10.6 内网访问 `http://192.0.2.10:8789/`。模型服务读取 Redis 历史，历史缺口后需先积累 35 分钟连续数据再推理。容量应继续按页面监测，不建议在生产采集时运行会停止 WSL 的持久化验证脚本。


2026-10-27：全量 RDB 快照条件调整为 `save 900 1`，运行时已 CONFIG SET 并 CONFIG REWRITE，Windows 配置参考副本同步；AOF 仍为 everysec。为减轻正在进行的快照对写入的影响，本次快照子进程降低为 idle I/O 优先级（仅本次进程有效）。不清空或批量删除原有历史。
