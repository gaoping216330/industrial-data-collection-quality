# Redis存储与判定读取

## 实际数据结构

|用途|键|类型|内容|
|---|---|---|---|
|点位来源目录|dq:cache:catalog|Hash|EID对应oa/kepserver|
|最近收包状态|dq:cache:receipt|Hash|字段为EID，值为JSON；received_time、source_time、last_change_time、quality、source|
|点位最新值|dq:cache:latest:<EID>|String|JSON包含eid/value/quality/source/source_time/last_change_time/received_time/time_schema|
|点位趋势|dq:cache:history:<EID>|Stream|每条字段t为发送时间5秒网格的毫秒时间戳，v为数值；Stream条目ID是写入时生成的ID，并非t|

趋势按发送时间整5秒网格保存，保留约180分钟；正常连续样本理论约2160条。后台裁剪与TTL共同控制保留，不能据缓存窗口配置推断数据绝对完整。KEP异常质量不会作为有效数值趋势样本写入；收包和质量信息仍保留。Redis保留原始数值精度，网页按两位小数显示。

接收端用Lua脚本批量写入、更新最新值和趋势，时间戳和去重校验防止旧值覆盖新值。

## 读取
判定程序每10秒观察：每批500个点位用HMGET取收包信息；仅对需要恒值检测的点位用MGET取最新值。每批最多两条命令，pipeline(transaction=False)一次发送；本批完成后再执行下一批。不是把全部点位的76批命令一次堆到同一流水线。

接收端连接2秒/读写3秒超时不变；判定专用客户端读取6秒、连接2秒。读取失败批次最多重试一次，仍失败则本轮不发布部分结果，避免误判为离线。读取后重新取得当前时间计算数据时效。模型读取继续采用原有客户端，不随判定端放宽。

趋势网页使用XRANGE按时间段读取Stream，模型使用增量XRANGE读取新样本；实时点位列表仅取当前页最新状态。

## 验证与上线
2026-11-12，115项平台测试通过；实际37825个点位、76批读取实测1.125秒（一次观测，非时延承诺）。只重启判定服务，接收、Redis和模型服务未重启。
