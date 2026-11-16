# 点位目录单向同步

已部署：`192.0.2.10/gpwinccoa.autosensor` → `192.0.2.10/gpwinccoa.autosensor`。

- 启动时立即同步，此后按启动时间每 600 秒执行一次。
- 后台进程由 `start-background.ps1` 启动；当前用户登录任务 `DataQualityCatalogSync-StartAtLogon` 自动启动。
- 源库复用 `../模型在线检测服务/config.json` 的 database 连接；目标库复用 `../实时数据缓存服务/cache_config.json`，不重复保存密码。
- 以主键 EID 比对全部列，仅新增及更新有差异的行。保留目标库独有记录，不执行全表清空或同步删除。
- 写入前将目标全表的恢复 SQL 压缩到本目录 `backups/`。备份包含撤销本轮新增 EID 的 SQL，**仅用于人工审核后的恢复，不会自动执行**。
- 首次发现目标 dpname 为 varchar(45)、源库为 varchar(100)，备份后扩宽为 varchar(100)，防止截断源库最长 58 字符的数据。其他未知结构差异拒绝同步。
- 数据写入使用单个事务；提交后与本次源库读取快照全列比对。源库始终只读。MySQL 命名锁防止重叠同步。
- 每次成功后发布目录版本通知；Redis 不可用时，下轮重试通知，采集和判定仍每 10 分钟从目标库重新读取。
- `status.json` 为最近一次结果，`sync.log` 为轮转日志。网页“运行状态 → 点位目录同步”显示状态、时间和新增/更新数量。
- 备份会保留在 D 盘，尚未设置自动删除备份的策略。

手动只读比对：

```powershell
D:\python313\python.exe .\sync_autosensor.py
```

手动执行一次（不要在已有后台循环运行时重复启动循环）：

```powershell
D:\python313\python.exe .\sync_autosensor.py --apply
```

首次执行结果（2026-10-27 08:32）：新增 1,034、更新 42,747，目标共 54,622 行，耗时约 24 秒。同步后实际监测目录为 Kepserver 32,115、OA 5,595（重叠 EID 优先 Kepserver）。已核对所有 37,710 个监测 EID：来源不匹配 0、缺失 0。

验证：同步差异/批量 SQL 2 项、采集及来源切换 24 项、监测/网页/模型目录 24 项，共 50 项测试通过。首次压缩备份已完整解压校验 CRC。
