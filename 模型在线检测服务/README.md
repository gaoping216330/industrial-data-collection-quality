# MQTT传感器模型在线检测服务

本程序从现有MQTT服务器读取数据，调用 `models_production` 中部署状态为 `ready` 的390个模型，并将结果更新到 `gpwinccoa.autosensor`。

## 检测范围

- 只输出390个 `ready` 模型的结果。
- 为完成工况识别和关联传感器比较，内部会缓存591个目标/相关/BOOL工况点位。
- 其他点位不执行模型检测。
- 程序启动时把390个ready点位的 `havemodel` 同步为 `Y`；当 `synchronize_havemodel=true` 时，其他点位原有的 `Y` 会先清空。

数据库实际存在的消息列名是 `modelmseeage`。程序会自动在 `modelmessage`、`modelmseeage`、`modelmeseage` 中选择真实存在的列，因此不需要改表。

## 默认时序

- MQTT原始数据：约每秒一笔。
- 模型聚合粒度：10秒，与训练时一致。
- 内存缓存：45分钟。
- 首次判断：收到第一笔有效数据35分钟后。
- 后续判断：每5分钟一次。

温度模型的“不变化/卡死”判断最长需要约30分钟，因此不建议把 `warmup_minutes` 配置得低于30。检测周期可在 `config.json` 的 `detection.interval_seconds` 修改，建议范围为180～600秒。

## 数据库写入

每轮对390个点位更新：

- `havemodel`：`Y`
- `modeltestresult`：`正常`、`预警`、`异常` 或 `无法判断`
- `modeltesttime`：本轮模型执行时间
- `modelmseeage`：置信等级、模型类型、健康分、实测值、预测值、正常范围、工况、关联数据覆盖率和原因

示例消息：

```text
A/contextual_ridge;健康=97.5;实测=36.2;预测=36;范围=[34.8,37.1];工况=2;关联=92%;模型范围内
```

状态含义：

- `正常`：当前值在模型范围内。
- `预警`：当前10秒点异常，但尚未满足模型持续异常条件。
- `异常`：已经满足模型持续异常、卡死或其他确认条件。
- `无法判断`：目标数据超时、质量位无效、关联数据覆盖不足或模型执行失败。

## 使用方法

1. 首次使用运行 `安装依赖.bat`。
2. 运行 `检查环境.bat`，应显示390个ready模型、11个模型组、591个缓存点位。
3. 双击 `启动在线检测.bat`。
4. 日志在 `logs/online_model_detector.log`。

需要长期后台运行时，可使用：

- `启动后台服务.bat`
- `查看服务状态.bat`
- `停止后台服务.bat`

命令行检查：

```powershell
python online_model_detector.py --config config.json --check
```

离线回放（不连接MQTT、不写数据库）：

```powershell
python offline_replay_test.py --config config.json --output offline_test_output
```

全390模型推理链路冒烟测试（构造完整缓存，不写数据库）：

```powershell
python model_smoke_test.py --config config.json
```

## 配置说明

主要修改 `config.json`：

- `detection.interval_seconds`：检测周期，默认300秒。
- `detection.cache_minutes`：内存缓存长度，默认45分钟。
- `detection.warmup_minutes`：首次检测前等待时间，默认35分钟。
- `detection.max_sample_age_seconds`：目标数据最大允许延迟，默认45秒。
- `detection.min_peer_coverage`：相关点位最小覆盖率，默认50%。
- `mqtt.topics`：订阅主题。
- `database.synchronize_havemodel`：是否让数据库的 `havemodel=Y` 与当前ready清单严格同步。

数据库和MQTT密码既可保留在配置文件，也可使用环境变量 `MODEL_DB_PASSWORD`、`MODEL_MQTT_PASSWORD` 覆盖。

## 注意

- 本程序只写 `autosensor` 新增的四个模型字段，不修改原离线判断字段。
- 正式运行时不要同时启动两个实例，否则会重复消费和重复写入。
- 模型结果适合预警和人工确认，不应直接作为设备联锁停机信号。
