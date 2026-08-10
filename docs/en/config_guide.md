# Configuration Guide

This document describes configuration options for OmniStream modules, including each option's name, meaning, default value, whether it is required, and valid values. Unless otherwise specified, configure these options in Flink's `flink-conf.yaml`. Restart the related Flink processes after changing cluster-level options.

## General Configuration

| Configuration Option | Description | Default Value | Required | Valid Values |
| --- | --- | --- | --- | --- |
| `state.backend` | Specifies the state backend used by a Flink job. | `hashmap`; state is stored in Java heap memory | No; it must be set to the BSS factory class when BSS is used | `hashmap`: Heap state backend; `rocksdb`: RocksDB state backend; `com.huawei.ock.bss.OckDBStateBackendFactory`: BSS state backend |

## BSS State Backend Configuration

This section describes the options supported when OmniStream uses the BSS (OmniStateStore) state backend. OmniAdaptor parses the BSS options and passes them to the OmniStream native side as part of the task information.

### Enabling the BSS State Backend

Add the following options to `flink-conf.yaml`:

```yaml
state.backend: com.huawei.ock.bss.OckDBStateBackendFactory
state.backend.ockdb.localdir: /data/ockdb
state.backend.ockdb.jni.logfile: /usr/local/flink/log/kv.log
```

Before using the BSS state backend, place the OmniStateStore plugin JAR in Flink's `lib` directory and ensure that OmniStream is built with `WITH_OMNISTATESTORE`. Create the configured directories in advance and grant read and write permissions to the user running Flink.

### Configuration Options

The default values in the following table match the current OmniAdaptor and OmniStream implementations. The valid ranges are based on OmniStateStore constraints and also account for Java configuration type limits.

| Configuration Option | Description | Default Value | Required | Valid Values |
| --- | --- | --- | --- | --- |
| `state.backend.ockdb.localdir` | Local BSS data directories. Multiple directories can be separated by commas or the system path separator. The native side creates operator databases across these directories in rotation. | Empty string; the task temporary working directory is used | No | Local absolute paths readable and writable by the Flink user. The `file://` prefix is supported; other URI schemes are not supported. |
| `state.backend.ockdb.checkpoint.backup` | Directory for local checkpoint backup files when local recovery is enabled. | Empty string; the `snapshot-backup` directory under the current database directory is used | No | A local directory readable and writable by the Flink user |
| `state.backend.ockdb.checkpoint.transfer.thread.num` | Number of threads used by each stateful operator to upload and download checkpoint files. | `4` | No | Integer in `[1, 20]` |
| `state.backend.ockdb.timer-service.factory` | Storage location for Flink timers. | `HEAP` | No | `HEAP`: JVM heap; `OCKDB`: BSS state backend |
| `state.backend.ockdb.jni.logfile` | BSS native log file path. | `/usr/local/flink/log/kv.log` | No | A file path writable by the Flink user. The parent directory must exist. |
| `state.backend.ockdb.jni.logsize` | Maximum size of a single BSS native log file. Flink memory units, such as `20mb`, are supported. | `20mb` | No | `10mb` to `50mb` |
| `state.backend.ockdb.jni.lognum` | Maximum number of BSS native log files to retain. | `20` | No | Integer in `[10, 50]` |
| `state.backend.ockdb.jni.loglevel` | BSS native log level. | `2` | No | `1`: DEBUG; `2`: INFO; `3`: WARN; `4`: ERROR |
| `state.backend.ockdb.jni.slice.watermark.ratio` | Watermark ratio at which the cache layer starts evicting cold data to the LSM layer. | `0.8` | No | Floating-point number in `(0, 1)` |
| `state.backend.ockdb.file.memory.fraction` | Fraction of the per-database memory limit used as the file cache for reading and writing LSM data. | `0.2` | No | Floating-point number in `[0.1, 0.5]` |
| `state.backend.ockdb.jni.lsmstore.compaction.switch` | Enables or disables LSM file compaction. | `1` | No | `0`: disabled; `1`: enabled |
| `state.backend.ockdb.lsmstore.compression.policy` | Default compression algorithm for the LSM levels. | `lz4` | No | `none` or `lz4` |
| `state.backend.ockdb.lsmstore.compression.level.policy` | Specifies the LSM compression algorithm for each level, in order from Level 0 through Level 5. | `none,none,lz4` | No | A comma-separated list containing `none` and `lz4`, with at most six entries |
| `state.backend.ockdb.snapshot.compression.algo` | Reserved snapshot compression option. The current native integration stores this value but does not pass it to OmniStateStore. | `none` | No | Currently, only `none` is recommended. |
| `state.backend.ockdb.ttl.filter.switch` | Enables or disables background cleanup of expired TTL state. | `false` | No | `true` or `false` |
| `state.backend.ockdb.cache.filter.and.index.switch` | Enables or disables caching for Filter and Index Blocks in the LSM layer. | `true` | No | `true` or `false` |
| `state.backend.ockdb.cache.filter.and.index.ratio` | Fraction of the total cache reserved exclusively for Filter and Index Blocks. | `0.0` | No | `0` means no exclusive cache; when enabled, a floating-point number in `(0, 1)` |
| `state.backend.ockdb.bloom.filter.switch` | Enables or disables the Bloom filter for state keys. The current native integration receives this value but does not explicitly set it in the BSS Config. | `true` | No | `true` or `false` |
| `state.backend.bloom.filter.expected.key.count` | Expected number of keys for the Bloom filter in a single state. The current native integration receives this value but does not explicitly set it in the BSS Config. | `8000000` | No | Integer in `[1000000, 10000000]` |
| `state.backend.ockdb.peak.filter.elem.num` | Number of Peak Filter elements. `0` uses the default BSS behavior. | `0` | No | Integer in `[0, 2147483647]` |
| `state.backend.ockdb.kv-separate.switch` | Enables or disables separate storage for large values. | `false` | No | `true` or `false` |
| `state.backend.ockdb.kv-separate.threshold` | Value size threshold for KV separation. Values larger than this threshold are stored separately. | `200` | No | Integer in `[9, 2147483647]` |
| `state.backend.ockdb.lazy.download.switch` | Enables or disables lazy loading during checkpoint restore. The current native integration receives this value but does not explicitly set it in the BSS Config. | `false` | No | `true` or `false` |

### Configuration Example

```yaml
state.backend: com.huawei.ock.bss.OckDBStateBackendFactory

# Local storage and checkpoints
state.backend.ockdb.localdir: /data1/ockdb,/data2/ockdb
state.backend.ockdb.checkpoint.backup: /data/ockdb-backup
state.backend.ockdb.checkpoint.transfer.thread.num: 4
state.backend.ockdb.timer-service.factory: HEAP

# Native logging
state.backend.ockdb.jni.logfile: /usr/local/flink/log/kv.log
state.backend.ockdb.jni.logsize: 20mb
state.backend.ockdb.jni.lognum: 20
state.backend.ockdb.jni.loglevel: 2

# BSS storage options
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

After changing cluster-level options in `flink-conf.yaml`, restart the related Flink processes to ensure that the changes take effect.
