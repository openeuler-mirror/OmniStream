# Heap 状态后端 Checkpoint & Restore 设计文档

## 一、现状分析

### 当前代码架构

```
CheckpointableKeyedStateBackend<K>::snapshot()  [纯虚]
  └── HeapKeyedStateBackend<K>::snapshot()       → return nullptr ❌
  └── RocksdbKeyedStateBackend<K>::snapshot()     → SnapshotStrategyRunner ✅

CheckpointableKeyedStateBackend<K>::savepoint()  [纯虚]
  └── HeapKeyedStateBackend<K>::savepoint()       → NOT_IMPL_EXCEPTION ❌
  └── RocksdbKeyedStateBackend<K>::savepoint()    → RocksDBFullSnapshotResources ✅
```

### RocksDB Checkpoint 写入链路（与 Heap 方案无关，仅做对比参考）

> **重要澄清**：RocksDB 的 checkpoint 和 savepoint 使用**完全不同的写入机制**。

#### RocksDB Checkpoint（SST 文件拷贝，非 KV 迭代）

1. `RocksdbKeyedStateBackend::snapshot()` 调用已初始化的 `SnapshotStrategyRunner`
2. 策略为 `RocksNativeFullSnapshotStrategy`（非增量）或 `RocksIncrementalSnapshotStrategy`（增量）
3. `syncPrepareResources()`：
   - 调用 `rocksdb::Checkpoint::CreateCheckpoint()` 创建 RocksDB 原生 checkpoint（SST、MANIFEST 等文件）
   - 通过 `CallMaterializeMetaData()` JNI 反调将元数据序列化到 Java 侧
4. `asyncSnapshot()`：
   - **不使用 `FullSnapshotAsyncWriter`**
   - 通过 `RocksDBStateUploader::callUploadFilesToCheckpointFs()` 直接上传 SST 文件
   - 返回 `IncrementalRemoteKeyedStateHandle`

关键文件：
- `cpp/runtime/snapshot/RocksNativeFullSnapshotStrategy.h/.cpp`
- `cpp/runtime/snapshot/RocksIncrementalSnapshotStrategy.h/.cpp`
- `cpp/runtime/snapshot/RocksDBSnapshotStrategyBase.h/.cpp`

#### RocksDB Savepoint（KV 迭代序列化）

1. `RocksdbKeyedStateBackend::savepoint()` 创建 `RocksDBFullSnapshotResources`
   - 取 RocksDB snapshot：`db->GetSnapshot()`
   - 创建列族迭代器
2. 返回 `SavepointResources` 包装 `RocksDBFullSnapshotResources`
3. 通过 `SavepointSnapshotStrategy` → `FullSnapshotAsyncWriter`
4. `FullSnapshotAsyncWriter::get()` 通过 `KeyValueStateIterator`（`RocksStatesPerKeyGroupMergeIterator`）迭代 KV 数据
5. 写入 `CheckpointStateOutputStreamProxy`（通过 JNI 写到 Java checkpoint 流）

关键文件：
- `cpp/runtime/state/RocksDBFullSnapshotResources.h/.cpp`
- `cpp/runtime/state/SavepointSnapshotStrategy.h`
- `cpp/runtime/state/FullSnapshotAsyncWriter.h/.cpp`

#### Checkpoint vs Savepoint 对比

| 方面 | RocksDB Checkpoint | RocksDB Savepoint |
|---|---|---|
| **策略类** | `RocksNativeFullSnapshotStrategy` / `RocksIncrementalSnapshotStrategy` | `SavepointSnapshotStrategy` |
| **写入器** | 直接文件上传（无 `FullSnapshotAsyncWriter`） | `FullSnapshotAsyncWriter` |
| **是否遍历 KV** | **否** — 使用 RocksDB 原生 checkpoint（SST 文件拷贝） | **是** — 通过 `KeyValueStateIterator` 逐条迭代 |
| **元数据** | 通过 JNI `CallMaterializeMetaData()` | 在 `FullSnapshotAsyncWriter::get()` 中序列化 |
| **状态句柄** | `IncrementalRemoteKeyedStateHandle` | `KeyGroupsStateHandle`（通过 `SnapshotResult`） |

### Heap 的关键差异

Heap 状态后端**没有 SST 文件**，数据全部在内存中。因此：
- **Heap Checkpoint 和 Savepoint 都必须走 KV 迭代序列化路径**（与 RocksDB Savepoint 类似）
- 这与 Flink 1.16.3 Java 的 `HeapSnapshotStrategy` 设计一致
- 复用 `FullSnapshotAsyncWriter` + `KeyValueStateIterator` 是正确的方案

### Restore 链路（已有，RocksDB 参考）

1. `FullSnapshotRestoreOperation::restore()` → `SavepointRestoreResultIterator`
2. 迭代 `SavepointRestoreResult`，获取 meta info + `KeyGroupIterator`
3. `KeyGroupIterator` 通过 JNI 从 Java 读回序列化的 KV 数据
4. `RocksDBFullRestoreOperation::restoreKVStateData()` 把 KV 写入 RocksDB

### 关键发现

- **`FullSnapshotAsyncWriter`** 和 **`FullSnapshotRestoreOperation`** 是后端无关的通用组件
- RocksDB checkpoint 使用原生文件拷贝（非 KV 迭代），而 Heap 必须使用 KV 迭代
- 需要实现的核心：
  1. `HeapFullSnapshotResources`：从 Heap 状态表中读取 KV 数据，提供 `KeyValueStateIterator`
  2. `HeapSnapshotStrategy`：Heap 的 checkpoint 快照策略（复用 `FullSnapshotAsyncWriter`）
  3. `HeapFullRestoreOperation`：将恢复的 KV 数据写回 Heap 状态表
  4. 在 `HeapKeyedStateBackend` 中接入上述组件

## 二、Heap 数据结构组织

### 2.1 整体层次结构

```
HeapKeyedStateBackend<K>
│
├── registeredKvStates: HashMap<string, tuple<uintptr_t, StateDescriptor*>>
│   │   ↑ key = 状态名（如 "TempName"）
│   │   ↑ value = (类型擦除的 StateTable 指针, 状态描述符)
│   │
│   ├── ["stateName1"] → (CopyOnWriteStateTable<K, VoidNamespace, int64_t>*, desc)
│   ├── ["stateName2"] → (CopyOnWriteStateTable<K, VoidNamespace, RowData*>*, desc)
│   └── ["mapState1"]  → (CopyOnWriteStateTable<K, VoidNamespace, HashMap<int,int>*>*, desc)
│
├── createdKvState: HashMap<string, uintptr_t>
│   │   ↑ 存储已创建的 State 对象（HeapValueState / HeapMapState / HeapListState）
│   │
│   ├── ["stateName1"] → HeapValueState<K, VoidNamespace, int64_t>*
│   └── ["mapState1"]  → HeapMapState<K, VoidNamespace, int, int>*
│
└── keyContext: InternalKeyContext<K>
        ├── currentKey: K
        ├── currentKeyGroupIndex: int
        ├── numberOfKeyGroups: int
        └── keyGroupRange: KeyGroupRange {startKeyGroup, endKeyGroup}
```

### 2.2 StateTable 层

```
CopyOnWriteStateTable<K, N, S> extends StateTable<K, N, S>
│
├── keyGroupedStateMaps: vector<StateMap<K, N, S>*>
│   │   ↑ 每个 key group 一个 StateMap，共 keyGroupRange.size() 个
│   │
│   ├── [0] → CopyOnWriteStateMap<K, N, S>*   (keyGroup = startKeyGroup + 0)
│   ├── [1] → CopyOnWriteStateMap<K, N, S>*   (keyGroup = startKeyGroup + 1)
│   └── [n] → CopyOnWriteStateMap<K, N, S>*   (keyGroup = startKeyGroup + n)
│
├── keyContext: InternalKeyContext<K>*
├── keyGroupRange: KeyGroupRange*
├── metaInfo: RegisteredKeyValueStateBackendMetaInfo*
│   ├── stateType: StateDescriptor::Type (VALUE/LIST/MAP)
│   ├── namespaceSerializer: TypeSerializer*
│   └── stateSerializer: TypeSerializer*
│
├── keySerializer: TypeSerializer*
└── namespaceSerializer: TypeSerializer* (来自 metaInfo)
```

### 2.3 CopyOnWriteStateMap 层（单个 key group 的哈希表）

```
CopyOnWriteStateMap<KeyT, N, ValueT> extends StateMap<KeyT, N, ValueT>
│
├── _pairs[bucket] → CopyOnWriteMapEntry<KeyT, N, ValueT>
│   │
│   │   每个 Entry 的字段布局：
│   ├── first:        KeyT        (key，如 RowData*)
│   ├── second:       ValueT      (state value，如 int64_t / HashMap<UK,UV>*)
│   ├── third:        N           (namespace，如 VoidNamespace)
│   ├── bucket:       size_type   (开放寻址的桶索引)
│   ├── entryVersion: int         (条目版本，用于 COW)
│   └── stateVersion: int         (状态版本，用于 COW)
│
├── 哈希函数: CombineHash — XOR(hash(key), hash(namespace))
├── 相等判断: CombineEqual — key AND namespace 都必须匹配
├── 负载因子: 0.80
└── 冲突解决: 开放寻址 + 二次探测
```

### 2.4 状态类型与模板参数映射

| StateDescriptor::Type | BackendDataType | 模板参数 S | State 对象 |
|---|---|---|---|
| VALUE | INT_BK | `int` | `HeapValueState<K, VoidNamespace, int>` |
| VALUE | BIGINT_BK | `int64_t` | `HeapValueState<K, VoidNamespace, int64_t>` |
| VALUE | ROW_BK | `RowData*` | `HeapValueState<K, VoidNamespace, RowData*>` |
| VALUE | OBJECT_BK / POJO_BK | `Object*` | `HeapValueState<K, VoidNamespace, Object*>` |
| LIST | BIGINT_BK | `vector<int64_t>*` | `HeapListState<K, VoidNamespace, int64_t>` |
| MAP | INT_BK × INT_BK | `HashMap<int,int>*` | `HeapMapState<K, VoidNamespace, int, int>` |
| MAP | VARCHAR_BK × INT_BK | `HashMap<string,int>*` | `HeapMapState<K, VoidNamespace, string, int>` |
| MAP | OBJECT_BK × OBJECT_BK | `HashMap<Object*,Object*>*` | `HeapMapState<K, VoidNamespace, Object*, Object*>` |

### 2.5 类型擦除与恢复模式

```cpp
// 注册时：全模板类型 → uintptr_t 类型擦除
auto *table = new CopyOnWriteStateTable<K, VoidNamespace, int64_t>(...);
registeredKvStates["name"] = {reinterpret_cast<uintptr_t>(table), descriptor};

// 使用时：根据 StateDescriptor 分发恢复具体类型
StateDescriptor *desc = std::get<1>(pair.second);
uintptr_t ptr = std::get<0>(pair.second);
if (desc->getType() == VALUE && desc->getBackendId() == BIGINT_BK) {
    auto *table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t>*>(ptr);
    // 使用具体类型操作
}
```

### 2.6 Key Group 分配机制

```
setCurrentKey(key)
    → keyGroup = KeyGroupRangeAssignment::assignToKeyGroup(key, numberOfKeyGroups)
    → setCurrentKeyGroupIndex(keyGroup)

// 状态访问时：
stateMap = keyGroupedStateMaps[keyGroup - keyGroupRange.startKeyGroup]
stateMap->get(key, namespace)  // 在对应 key group 的 map 中查找
```

### 2.7 对象生命周期管理

- **指针类型（Object\*、RowData\*）**：插入时 clone/copy，析构时逐个释放
- **值类型（int、int64_t）**：直接存储，无特殊管理
- **容器指针（HashMap\*、vector\*）**：作为 S 模板参数存储，析构时需删除

## 三、对应 Flink 1.16.3 原生调用链

| Flink 1.16.3 (Java) | OmniStream (C++) | 状态 |
|---|---|---|
| `HeapKeyedStateBackend.snapshot()` | `HeapKeyedStateBackend::snapshot()` | 需实现 |
| `HeapKeyedStateBackend.savepoint()` | `HeapKeyedStateBackend::savepoint()` | 需实现 |
| `HeapSnapshotResources` | **新建** `HeapFullSnapshotResources` | 需新建 |
| `HeapSnapshotStrategy` | **新建** `HeapSnapshotStrategy` | 需新建 |
| `StateTable.stateSnapshot()` | 无需（直接迭代 StateMap） | N/A |
| `FullSnapshotAsyncWriter` | `FullSnapshotAsyncWriter` | 已有 ✅ |
| `HeapRestoreOperation` | **新建** `HeapFullRestoreOperation` | 需新建 |
| `HeapKeyedStateBackendBuilder` (含 restore) | 增强 `HeapKeyedStateBackendBuilder` | 需改造 |

> **注意**：Flink Java 中 `HeapSnapshotStrategy` 对 checkpoint 和 savepoint 使用相同的 KV 迭代序列化路径。
> 这与 RocksDB 不同——RocksDB checkpoint 使用原生 SST 文件拷贝，而 savepoint 才走 KV 迭代。

## 四、核心类继承关系

```
CheckpointableKeyedStateBackend<K> (纯虚)
    ↑
AbstractKeyedStateBackend<K>
    ↑
    ├─→ HeapKeyedStateBackend<K>       [snapshot() → nullptr → 需实现]
    ├─→ RocksdbKeyedStateBackend<K>    [snapshot() → SnapshotStrategyRunner]
    └─→ BssKeyedStateBackend<K>        [条件编译]
```

### 快照策略执行链

```
RocksDB Checkpoint 路径（SST 文件拷贝）：
RocksdbKeyedStateBackend.snapshot()
    ↓
SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources>
    ↓
RocksNativeFullSnapshotStrategy.syncPrepareResources()
    ↓ rocksdb::Checkpoint::CreateCheckpoint()
RocksNativeFullSnapshotStrategy.asyncSnapshot()
    ↓ RocksDBStateUploader::callUploadFilesToCheckpointFs()  ← 文件上传，非 KV 迭代
SnapshotResult<KeyedStateHandle> (返回 IncrementalRemoteKeyedStateHandle)

RocksDB / Heap Savepoint 路径（KV 迭代序列化）：
keyedStateBackend.savepoint()
    ↓
SavepointSnapshotStrategy + SnapshotStrategyRunner
    ↓
FullSnapshotAsyncWriter::get()
    ↓ KeyValueStateIterator 逐条迭代
CheckpointStateOutputStreamProxy (JNI → Java)
    ↓
SnapshotResult<KeyedStateHandle> (返回 KeyGroupsStateHandle)

Heap Checkpoint 路径（与 Savepoint 相同，KV 迭代序列化）：
HeapKeyedStateBackend.snapshot()
    ↓
HeapSnapshotStrategy + SnapshotStrategyRunner
    ↓
FullSnapshotAsyncWriter::get()
    ↓ KeyValueStateIterator 逐条迭代（通过 HeapSingleStateIterator）
CheckpointStateOutputStreamProxy (JNI → Java)
    ↓
SnapshotResult<KeyedStateHandle>
```

### 快照资源接口

```cpp
// FullSnapshotResources 接口 (cpp/runtime/state/FullSnapshotResources.h)
class FullSnapshotResources : public SnapshotResources {
public:
    virtual const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() = 0;
    virtual KeyGroupRange *getKeyGroupRange() = 0;
    virtual TypeSerializer *getKeySerializer() = 0;
    virtual std::shared_ptr<KeyValueStateIterator> createKVStateIterator() = 0;
    virtual void cleanup() = 0;
};
```

### KV 迭代器接口

```cpp
// KeyValueStateIterator 接口 (cpp/runtime/state/KeyValueStateIterator.h)
class KeyValueStateIterator {
public:
    virtual void next() = 0;
    virtual int keyGroup() const = 0;
    virtual std::vector<int8_t> key() const = 0;
    virtual std::vector<int8_t> value() const = 0;
    virtual int kvStateId() const = 0;
    virtual bool isNewKeyValueState() const = 0;
    virtual bool isNewKeyGroup() const = 0;
    virtual bool isValid() const = 0;
    virtual void close() = 0;
};

// SingleStateIterator 接口 (cpp/runtime/state/rocksdb/iterator/SingleStateIterator.h)
class SingleStateIterator {
public:
    virtual void next() = 0;
    virtual bool isValid() const = 0;
    virtual std::vector<int8_t> key() const = 0;
    virtual std::vector<int8_t> value() const = 0;
    virtual int getKvStateId() const = 0;
    virtual void close() = 0;
};
```

## 五、关键设计决策

### 5.1 序列化格式兼容性

**问题**：Heap 状态的 key/value 是内存中的类型化对象（如 `int64_t`、`RowData*`），而 checkpoint 流要求序列化后的字节数组 `vector<int8_t>`。

**决策**：使用与 RocksDB 完全相同的 key 序列化格式：
```
key bytes = keyGroupPrefix(1-2 bytes) + TypeSerializer(key).serialize() + TypeSerializer(namespace).serialize()
value bytes = TypeSerializer(value).serialize()
```

这样保证：
- Java 侧无需修改（已能正确处理此格式）
- 理论上 Heap checkpoint 和 RocksDB savepoint restore 可以互操作

**参考代码**：`RocksdbStateTable::GetKeyNameSpaceSlice()` 中的序列化逻辑：
```cpp
outputSerializer.writeByte(static_cast<uint32_t>(keyContext->getCurrentKeyGroupIndex()));
LongSerializer::INSTANCE->serialize(&currentKey, outputSerializer);
getNamespaceSerializer()->serialize(&nameSpace, outputSerializer);
```

### 5.2 类型擦除问题

**问题**：`HeapKeyedStateBackend` 通过 `emhash7::HashMap<string, tuple<uintptr_t, StateDescriptor*>>` 存储状态表，类型信息被擦除为 `uintptr_t`。C++ 模板不支持运行时多态的类型恢复。

**决策**：沿用析构函数中已有的类型分发模式，在 `createFullSnapshotResources()` 中根据 `StateDescriptor::Type` 和 `BackendDataType` 进行 `reinterpret_cast` 恢复具体类型。

**理由**：
- 代码库中已有此模式（析构函数、`createOrUpdateInternalState()`）
- 所有支持的类型组合是有限且已知的
- 新增类型只需在分发 switch 中增加一个分支

### 5.3 急切序列化 vs 惰性序列化

**问题**：`HeapSingleStateIterator` 是在构造时一次性序列化所有 KV 条目（急切），还是在 `next()` 调用时逐条序列化（惰性）？

**决策**：采用急切序列化。

**理由**：
- Heap 状态本身就在内存中，多占一份序列化副本的内存开销可接受
- 急切序列化在 `syncPrepareResources()` 阶段完成，保证数据一致性快照
- 实现更简单，避免迭代器生命周期管理复杂性
- `CopyOnWriteStateMap` 的 COW 机制理论上可以保护惰性迭代，但增加实现复杂度

### 5.4 复用 RocksStatesPerKeyGroupMergeIterator

**问题**：Heap 的 KV 迭代器需要按 key group 有序输出，是自己实现排序还是复用已有的 MergeIterator？

**决策**：复用 `RocksStatesPerKeyGroupMergeIterator`，将 Heap 的 `HeapSingleStateIterator` 作为 `heapPriorityQueueIterators` 传入。

**理由**：
- `RocksStatesPerKeyGroupMergeIterator` 已实现多路归并排序和 key group 检测逻辑
- 其 `SingleStateIterator` 接口是后端无关的
- 减少代码重复和潜在的排序 bug

### 5.5 OmniTaskBridge 传递

**问题**：`HeapKeyedStateBackend::snapshot()` 需要 `OmniTaskBridge` 来通过 JNI 写入 checkpoint 流，但 `snapshot()` 接口签名不包含 bridge 参数。RocksDB 通过构造函数获取 bridge。

**决策**：在 `StreamOperatorStateHandler::snapshotState()` 中，通过 `dynamic_cast` 检测 Heap 后端，调用 `setOmniTaskBridge(bridge)` 设置。

**理由**：
- 不修改 `snapshot()` 虚函数签名（避免影响所有子类）
- 不修改 Heap 构造函数签名（保持向后兼容）
- bridge 在整个 task 生命周期内不变，设置一次后续 checkpoint 都可用

### 5.6 Heap Checkpoint vs Savepoint 统一实现

**问题**：Heap 的 checkpoint 和 savepoint 是否需要不同的策略？

**决策**：统一使用 `FullSnapshotAsyncWriter` + `KeyValueStateIterator` 路径。

**理由**：
- Heap 状态全在内存中，没有 RocksDB 那样的原生 checkpoint（SST 文件拷贝）机制
- Flink 1.16.3 Java 的 `HeapSnapshotStrategy` 也是对 checkpoint 和 savepoint 使用相同的序列化路径
- `snapshot()` 和 `savepoint()` 的差别仅在于触发方式和 `CheckpointOptions`，底层数据序列化逻辑完全一致

## 六、数据流图

### Checkpoint 写入流

```
HeapKeyedStateBackend::snapshot(checkpointId, ...)
    │
    ├─ createFullSnapshotResources()
    │   ├─ 遍历 registeredKvStates
    │   ├─ 对每个状态表：
    │   │   ├─ 类型分发 (VALUE/MAP/LIST × BackendDataType)
    │   │   ├─ reinterpret_cast 到具体 CopyOnWriteStateTable<K,N,S>
    │   │   ├─ 收集 metaInfo->snapshot()
    │   │   └─ 创建 HeapSingleStateIterator<K,N,S>
    │   │       └─ 构造时急切序列化所有 KV 条目
    │   │           ├─ 遍历每个 key group 的 CopyOnWriteStateMap
    │   │           ├─ 对每个 CopyOnWriteMapEntry：
    │   │           │   ├─ key bytes = keyGroupPrefix + serialize(entry.first) + serialize(entry.third)
    │   │           │   └─ value bytes = serialize(entry.second)
    │   │           └─ 按 keyGroupPrefix 排序
    │   └─ 返回 HeapFullSnapshotResources
    │
    ├─ HeapSnapshotStrategy
    │   ├─ syncPrepareResources() → 返回 HeapFullSnapshotResources
    │   └─ asyncSnapshot() → 创建 FullSnapshotAsyncWriter
    │
    └─ SnapshotStrategyRunner::snapshot()
        └─ 返回 packaged_task (异步执行)
            └─ FullSnapshotAsyncWriter::get(bridge)
                ├─ HeapFullSnapshotResources::createKVStateIterator()
                │   └─ RocksStatesPerKeyGroupMergeIterator (复用)
                │       └─ 按 key group 有序归并迭代
                ├─ CheckpointStateOutputStreamProxy (通过 JNI 写入 Java)
                │   ├─ writeMetadata(metaInfoSnapshots)
                │   └─ 逐条写入 key/value 字节
                └─ 返回 SnapshotResult<KeyedStateHandle>
```

### Restore 读取流（待完整实现）

```
HeapKeyedStateBackendBuilder::build()
    │
    ├─ FullSnapshotRestoreOperation<K>::restore()
    │   └─ SavepointRestoreResultIterator
    │       └─ 通过 JNI 从 Java 读取 checkpoint 数据
    │
    └─ 遍历 SavepointRestoreResult
        ├─ getStateMetaInfoSnapshots() → 状态元信息
        └─ getKeyGroupIterator() → KeyGroupIterator
            └─ 逐 key group 读取 KeyGroupEntry (kvStateId, key bytes, value bytes)
                └─ TODO: 反序列化并写入 CopyOnWriteStateTable
```

## 七、文件清单

### 新建文件

| 文件 | 用途 |
|---|---|
| `cpp/runtime/state/heap/HeapSingleStateIterator.h` | 单个状态表的 SingleStateIterator 适配，负责序列化 |
| `cpp/runtime/state/heap/HeapFullSnapshotResources.h` | FullSnapshotResources 实现，管理迭代器集合 |
| `cpp/runtime/state/heap/HeapSnapshotStrategy.h` | Checkpoint 策略，复用 FullSnapshotAsyncWriter |

### 修改文件

| 文件 | 改动 |
|---|---|
| `cpp/runtime/state/HeapKeyedStateBackend.h` | 实现 snapshot()/savepoint()，增加 createFullSnapshotResources() 和 omniTaskBridge_ |
| `cpp/runtime/state/heap/HeapKeyedStateBackendBuilder.h` | 增加 restore 参数支持 |
| `cpp/streaming/api/operators/StreamOperatorStateHandler.h` | 设置 Heap 后端的 bridge |

## 八、CopyOnWriteStateMap 数据结构

```cpp
// CopyOnWriteMapEntry<K, N, S> 的字段布局：
// - first:  K (key)
// - second: S (value/state)  
// - third:  N (namespace)
// - bucket: size_type (桶索引)
// - entryVersion: int (条目版本)
// - stateVersion: int (状态版本)

// 迭代方式：
for (auto it = cowMap->begin(); it != cowMap->end(); ++it) {
    K key       = it->first;
    S value     = it->second;
    N namespace = it->third;
}

// 哈希规则：
// - 哈希函数：XOR(hash(key), hash(namespace))
// - 相等条件：key AND namespace 同时匹配
// - 负载因子：0.80
// - 冲突解决：开放寻址 + 二次探测
```

## 九、`snapshot()` 返回 nullptr 的影响分析

当前（未修改前）`HeapKeyedStateBackend::snapshot()` 返回 `nullptr`，checkpoint 仍能成功的原因：

```
snapshot() → nullptr
    ↓
OperatorSnapshotFutures::setKeyedStateManagedFuture(nullptr)
    ↓
OperatorSnapshotFinalizer 构造：
    FutureUtils::runIfNotDoneAndGet(nullptr)
        → if (!task) return T();  // 直接返回默认值
    ↓
    keyedStateManaged == nullptr → false
        → jobManagerOwnedManaged = SingletonOrEmpty(nullptr)  // 空集合
    ↓
TaskStateSnapshot 中 keyed state 为空集合
    → Java CheckpointCoordinator 认为"没有 keyed state"
    → checkpoint 成功
```

**后果**：checkpoint 成功但 keyed state **未被保存**。从该 checkpoint 恢复时，keyed state 为空。

## 十、风险分析

| 风险 | 影响 | 缓解措施 |
|---|---|---|
| 急切序列化的内存开销 | 状态很大时内存翻倍 | Heap 后端适用于小中规模状态；大状态应使用 RocksDB |
| TypeSerializer 不支持某类型的序列化 | 运行时崩溃 | RocksDB 路径已验证这些 serializer；类型分发时 skip 不支持的类型 |
| MAP 类型的嵌套序列化 | emhash7::HashMap* 的 serialize 可能不支持 | 需验证 stateSerializer 是否覆盖 map 类型 |
| Restore 路径不完整 | 从 checkpoint 恢复后状态为空 | 第二期补齐 |
| 并发修改 | 序列化期间状态被修改 | checkpoint barrier 保证一致性 |

## 十一、第二期规划

1. **补齐 Heap Restore**：在 `HeapKeyedStateBackendBuilder::build()` 中将 KV 字节反序列化写入 `CopyOnWriteStateTable`
2. **Savepoint Restore**：原理相同，只是触发方式不同
3. **端到端验证**：Flink DataStream co_key 算子的 checkpoint → kill → restore 流程
