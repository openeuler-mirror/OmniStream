# 配置指南

本文介绍OmniStream各功能模块的配置项，包括配置项名称、含义、默认值、是否必配和可配置范围。除非特别说明，配置项均写入Flink的`flink-conf.yaml`，修改集群级配置后需要重启相关Flink进程才能生效。

## 通用配置

| 配置项名称 | 含义 | 默认值 | 是否必配 | 可配置范围 |
| --- | --- | --- | --- | --- |
| `state.backend` | 指定Flink任务使用的状态后端。 | `hashmap`，状态存储在Java堆内存中 | 否；使用BSS时必须配置为BSS工厂类 | `hashmap`：Heap状态后端；`rocksdb`：RocksDB状态后端；`com.huawei.ock.bss.OckDBStateBackendFactory`：BSS状态后端 |

## BSS状态后端配置

本节介绍OmniStream使用BSS（OmniStateStore）状态后端时支持的配置项。BSS配置由OmniAdaptor解析，并随任务信息传递给OmniStream Native侧。

### 启用BSS状态后端

在`flink-conf.yaml`中配置以下内容：

```yaml
state.backend: com.huawei.ock.bss.OckDBStateBackendFactory
state.backend.ockdb.localdir: /data/ockdb
state.backend.ockdb.jni.logfile: /usr/local/flink/log/kv.log
```

使用BSS状态后端前，需要将OmniStateStore插件JAR放入Flink的`lib`目录，并确保OmniStream使用`WITH_OMNISTATESTORE`宏完成编译。路径配置所指向的目录需要提前创建，并授予Flink运行用户读写权限。

### 配置项

下表中的默认值以当前OmniAdaptor和OmniStream实现为准。数值范围参考OmniStateStore约束，并同时考虑Java配置类型的取值上限。

| 配置项名称 | 含义 | 默认值 | 是否必配 | 可配置范围 |
| --- | --- | --- | --- | --- |
| `state.backend.ockdb.localdir` | BSS本地数据目录。支持配置多个目录，多个目录间使用逗号或系统路径分隔符分隔；Native侧在多个目录间轮转创建算子DB。 | 空字符串，使用任务临时工作目录 | 否 | Flink运行用户可读写的本地绝对路径；支持`file://`前缀，不支持其他URI协议 |
| `state.backend.ockdb.checkpoint.backup` | 开启本地恢复时保存Checkpoint本地备份文件的目录。 | 空字符串，使用当前DB目录下的`snapshot-backup` | 否 | Flink运行用户可读写的本地目录 |
| `state.backend.ockdb.checkpoint.transfer.thread.num` | 每个有状态算子上传和下载Checkpoint文件的线程数。 | `4` | 否 | 整数`[1, 20]` |
| `state.backend.ockdb.timer-service.factory` | Flink计时器的存储位置。 | `HEAP` | 否 | `HEAP`：JVM堆；`OCKDB`：BSS状态后端 |
| `state.backend.ockdb.jni.logfile` | BSS Native日志文件路径。 | `/usr/local/flink/log/kv.log` | 否 | Flink运行用户可写的文件路径，父目录必须存在 |
| `state.backend.ockdb.jni.logsize` | 单个BSS Native日志文件的最大大小。支持Flink内存单位，例如`20mb`。 | `20mb` | 否 | `10mb`～`50mb` |
| `state.backend.ockdb.jni.lognum` | 最多保留的BSS Native日志文件数。 | `20` | 否 | 整数`[10, 50]` |
| `state.backend.ockdb.jni.loglevel` | BSS Native日志级别。 | `2` | 否 | `1`：DEBUG；`2`：INFO；`3`：WARN；`4`：ERROR |
| `state.backend.ockdb.jni.slice.watermark.ratio` | 缓存层触发冷数据淘汰到LSM层的水位比例。 | `0.8` | 否 | 浮点数`(0, 1)` |
| `state.backend.ockdb.file.memory.fraction` | 读写LSM层的文件缓存占单个DB内存上限的比例。 | `0.2` | 否 | 浮点数`[0.1, 0.5]` |
| `state.backend.ockdb.jni.lsmstore.compaction.switch` | LSM文件整理合并开关。 | `1` | 否 | `0`：关闭；`1`：开启 |
| `state.backend.ockdb.lsmstore.compression.policy` | LSM层的默认压缩算法。 | `lz4` | 否 | `none`或`lz4` |
| `state.backend.ockdb.lsmstore.compression.level.policy` | 按Level指定LSM压缩算法，值按Level 0到Level 5依次排列。 | `none,none,lz4` | 否 | 由`none`、`lz4`组成的逗号分隔列表，最多6项 |
| `state.backend.ockdb.snapshot.compression.algo` | 快照压缩算法保留项。当前Native适配仅保存该值，尚未下传给OmniStateStore。 | `none` | 否 | 当前仅建议配置`none` |
| `state.backend.ockdb.ttl.filter.switch` | 后台清理已过期TTL状态的开关。 | `false` | 否 | `true`或`false` |
| `state.backend.ockdb.cache.filter.and.index.switch` | LSM层Filter和Index Block使用缓存的开关。 | `true` | 否 | `true`或`false` |
| `state.backend.ockdb.cache.filter.and.index.ratio` | Filter和Index Block独占缓存占总缓存的比例。 | `0.0` | 否 | `0`表示不独占；启用独占缓存时为浮点数`(0, 1)` |
| `state.backend.ockdb.bloom.filter.switch` | 状态Key的布隆过滤器开关。当前Native适配会接收该值，但尚未显式设置到BSS Config。 | `true` | 否 | `true`或`false` |
| `state.backend.bloom.filter.expected.key.count` | 单个状态中布隆过滤器的预期Key数量。当前Native适配会接收该值，但尚未显式设置到BSS Config。 | `8000000` | 否 | 整数`[1000000, 10000000]` |
| `state.backend.ockdb.peak.filter.elem.num` | Peak Filter的元素数量。`0`表示使用BSS默认行为。 | `0` | 否 | 整数`[0, 2147483647]` |
| `state.backend.ockdb.kv-separate.switch` | 大Value采用KV分离存储的开关。 | `false` | 否 | `true`或`false` |
| `state.backend.ockdb.kv-separate.threshold` | 启用KV分离的Value大小阈值，超过阈值的Value单独存储。 | `200` | 否 | 整数`[9, 2147483647]` |
| `state.backend.ockdb.lazy.download.switch` | 从Checkpoint恢复时进行懒加载的开关。当前Native适配会接收该值，但尚未显式设置到BSS Config。 | `false` | 否 | `true`或`false` |

### 配置示例

```yaml
state.backend: com.huawei.ock.bss.OckDBStateBackendFactory

# 本地存储与Checkpoint
state.backend.ockdb.localdir: /data1/ockdb,/data2/ockdb
state.backend.ockdb.checkpoint.backup: /data/ockdb-backup
state.backend.ockdb.checkpoint.transfer.thread.num: 4
state.backend.ockdb.timer-service.factory: HEAP

# Native日志
state.backend.ockdb.jni.logfile: /usr/local/flink/log/kv.log
state.backend.ockdb.jni.logsize: 20mb
state.backend.ockdb.jni.lognum: 20
state.backend.ockdb.jni.loglevel: 2

# BSS存储参数
state.backend.ockdb.jni.slice.watermark.ratio: 0.8
state.backend.ockdb.file.memory.fraction: 0.2
state.backend.ockdb.jni.lsmstore.compaction.switch: 1
state.backend.ockdb.lsmstore.compression.policy: lz4
state.backend.ockdb.lsmstore.compression.level.policy: none,none,lz4
state.backend.ockdb.ttl.filter.switch: false
state.backend.ockdb.cache.filter.and.index.switch: true
state.backend.ockdb.cache.filter.and.index.ratio: 0.0
state.backend.ockdb.kv-separate.switch: false
state.backend.ockdb.kv-separate.threshold: 200
```

修改集群级`flink-conf.yaml`后，需要重启相关Flink进程才能确保配置生效。
