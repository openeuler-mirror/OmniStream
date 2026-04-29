# RocksdbKeyedStateBackend HEAP PRIORITY_QUEUE 快照实现分析（OmniStream）

> 分析对象：`OmniStream.zip` 中当前 C++ 实现。  
> 分析范围：RocksdbKeyedStateBackend 下 `PRIORITY_QUEUE` 的 HEAP 类型实现，按 **CP、CP 恢复、SP、SP 恢复** 四条链路梳理。  
> 说明：本报告基于源码静态分析，未在当前环境完成编译与端到端运行验证。

---

## 1. 总体结论

当前实现的总体方向是合理的，基本贴近 Flink 1.16.3 中 RocksDB backend + HEAP timer/PQ 的分工：

- **Checkpoint（CP）**：RocksDB 的 managed keyed state 继续走已有 RocksDB snapshot 策略；HEAP PQ/timer 不进入 RocksDB managed state，而是通过 **legacy raw keyed timer snapshot** 写入 raw keyed state。
- **Checkpoint 恢复（CP 恢复）**：RocksDB managed keyed state 恢复 KEY_VALUE；raw keyed state 再由 `InternalTimeServiceManager` 读取并恢复 timer/PQ。
- **Savepoint（SP）**：canonical savepoint 不走 raw keyed timer snapshot，而是通过 `RocksdbKeyedStateBackend::savepoint()` 把 HEAP PQ 作为 `PRIORITY_QUEUE` 类型的 managed keyed state 写入 savepoint。
- **Savepoint 恢复（SP 恢复）**：`RocksDBHeapTimersFullRestoreOperation` 将 KEY_VALUE 写回 RocksDB，将 `PRIORITY_QUEUE` 先恢复成 pending wrapper；等 timer service 真正创建时，再将 pending serialized PQ entries drain 到真实 HEAP PQ。

因此，这版实现已经覆盖了 HEAP PQ 的四条核心链路。不过目前仍存在一些比较重要的问题，尤其是：

1. **CP 与 SP 的 PQ 格式不同**：CP 走 raw keyed timer stream，SP 走 managed keyed full snapshot，必须分别验证。
2. **序列化兼容性不足**：当前 timer snapshot 中大量使用 JSON/启发式恢复 serializer，并不是 Flink Java 1.16.3 的 `TypeSerializerSnapshot` 二进制格式。
3. **serializer compatibility 检查基本缺失**：存在 TODO 或只做 backendId 字符串比较的情况。
4. **KEY_VALUE 与 PRIORITY_QUEUE 的 metadata 顺序、state id 分配、restore 分类必须严格稳定**，否则 savepoint 可能出现错误路由。
5. **一些边界语义仍需验证**：event-time timer 的 `<= watermark` 触发语义、processing timer head 重注册、多 key group、多 timer service、rescale、空状态等。

---

## 2. 关键类和职责

| 类 / 文件 | 当前职责 |
|---|---|
| `cpp/runtime/state/RocksdbKeyedStateBackend.h` | RocksDB keyed backend 主入口；`snapshot()` 处理 CP；`savepoint()` 处理 canonical SP；`create()` 创建 HEAP/ROCKSDB PQ。 |
| `cpp/runtime/state/RocksDBKeyedStateBackendBuilder.h` | backend 构建与 restore operation 选择；HEAP PQ 时选择 `RocksDBHeapTimersFullRestoreOperation`。 |
| `cpp/runtime/state/HeapPriorityQueuesManager.h` | 管理 HEAP PQ 的注册、创建、pending restored wrapper drain。 |
| `cpp/runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapper.h` | 真实 HEAP PQ 的 snapshot/restore wrapper；负责将 serialized PQ element 反序列化并加入 queue。 |
| `cpp/runtime/state/heap/RestoredHeapPriorityQueueSnapshotRestoreWrapper.h` | SP 恢复阶段的 pending wrapper；在 timer service 还没创建时暂存 serialized PQ entries。 |
| `cpp/runtime/state/RocksDBFullSnapshotResources.cpp` | canonical savepoint 的 full snapshot resources；把 RocksDB KV iterator 与 HEAP PQ iterator 合并。 |
| `cpp/runtime/state/FullSnapshotAsyncWriter.cpp` | 写 full snapshot/savepoint 数据流；输出 metadata、key group、state id、key/value entries。 |
| `cpp/table/runtime/operators/InternalTimeServiceManager.h` | timer service 管理；CP raw keyed timer snapshot 写入与恢复入口。 |
| `cpp/table/runtime/operators/InternalTimerServiceSerializationProxy.h` | raw keyed timer snapshot 的 versioned proxy。 |
| `cpp/table/runtime/operators/InternalTimersSnapshotReaderWriters.h` | raw timer snapshot 的 reader/writer。 |
| `cpp/runtime/state/RocksDBHeapTimersFullRestoreOperation.h` | HEAP PQ savepoint 恢复入口；KEY_VALUE 入 RocksDB，PRIORITY_QUEUE 入 pending wrapper。 |

---

## 3. CP 调用链分析

### 3.1 顶层入口

CP 从 stream operator 的 snapshot 入口进入：

```text
AbstractStreamOperator::SnapshotState
  -> StreamOperatorStateHandler::SnapshotState
      -> StreamOperatorStateHandler::snapshotState
```

在 `StreamOperatorStateHandler::snapshotState(...)` 中，会先判断 keyed backend 是否需要 legacy synchronous timer snapshot：

```cpp
abstractBackend->requiresLegacySynchronousTimerSnapshots(checkpointOptions->GetCheckpointType())
```

`RocksdbKeyedStateBackend` 的实现逻辑是：

```cpp
return heapPriorityQueuesManager_ != nullptr
    && checkpointType != nullptr
    && !checkpointType->IsSavepoint();
```

也就是说：

- 当前 backend 使用 HEAP PQ；
- 当前 snapshot 类型不是 savepoint；
- 才会触发 CP raw keyed timer snapshot。

这点符合 Flink RocksDB + HEAP timer 的设计：**HEAP timer/PQ 不进入 RocksDB native/incremental CP，而是作为 raw keyed state 单独写出。**

### 3.2 HEAP PQ / Timer 的 raw keyed snapshot

当上面的判断为 true 时，调用：

```text
InternalTimeServiceManager::snapshotToRawKeyedState(...)
```

整体链路如下：

```text
StreamOperatorStateHandler::snapshotState
  -> timeServiceManager->snapshotToRawKeyedState(rawKeyedOutput, operatorName)
      -> KeyedStateCheckpointOutputStream::startNewKeyGroup(keyGroup)
      -> InternalTimerServiceSerializationProxy::write(output, keyGroup)
          -> InternalTimeServiceManager::writeTimersForKeyGroup(output, keyGroup)
              -> writeTimerServiceMap(...)
                  -> InternalTimerServiceImpl::snapshotTimersForKeyGroup(keyGroup)
                  -> InternalTimersSnapshotWriterV2::writeTimersSnapshot(...)
```

当前 raw timer snapshot 的大致格式是：

```text
version = 2
serviceCount
for each timer service:
  serviceName
  keySerializerJson
  namespaceSerializerJson
  eventTimerCount
    eventTimer(key, namespace, timestamp) ...
  processingTimerCount
    processingTimer(key, namespace, timestamp) ...
```

每个 key group 会被写到 `KeyedStateCheckpointOutputStream`，最后通过：

```text
StateSnapshotContextSynchronousImpl::getKeyedStateStreamFuture
  -> KeyedStateCheckpointOutputStream::closeAndGetHandle
  -> KeyGroupsStateHandle
```

形成 raw keyed state handle，并最终进入 `OperatorSubtaskState.rawKeyedState`。

### 3.3 RocksDB KEY_VALUE 的 managed CP

HEAP PQ 写 raw keyed state 之后，KEY_VALUE 仍走 RocksDB backend 原有 CP 逻辑：

```text
RocksdbKeyedStateBackend::snapshot(...)
  -> SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources>
      -> RocksNativeFullSnapshotStrategy / RocksIncrementalSnapshotStrategy
          -> RocksDBSnapshotStrategyBase::syncPrepareResources
          -> snapshotMetaData(kvStateInformation)
          -> takeDBNativeCheckpoint / incremental materialization
```

需要注意：当前 RocksDB CP managed path 中的 metadata 只来自 `kvStateInformation_`，并没有纳入 `registeredPQStates`。这不是 bug，而是当前实现的设计选择：

- **CP 的 HEAP PQ 只存在 raw keyed state 中**；
- **managed keyed CP 只负责 RocksDB KEY_VALUE。**

这个前提非常关键：如果某次 CP 没有走 `snapshotToRawKeyedState`，HEAP PQ/timer 就会丢失。

---

## 4. CP 恢复调用链分析

### 4.1 managed keyed KEY_VALUE 恢复

任务恢复时，`StreamTaskStateInitializerImpl` 负责构建 keyed backend：

```text
StreamTaskStateInitializerImpl::keyedStatedBackend
  -> RocksDBKeyedStateBackendBuilder::build
      -> getRocksDBRestoreOperation(...)
      -> restoreOperation->restore()
      -> new RocksdbKeyedStateBackend(...)
```

对于 RocksDB managed keyed state：

- 如果是 incremental checkpoint handle，走 `RocksDBIncrementalRestoreOperation`；
- 如果是 full/native/checkpoint handle，走 `RocksDBFullRestoreOperation` 或 HEAP PQ 特化 restore operation；
- KEY_VALUE 会恢复到 RocksDB column family。

### 4.2 raw keyed timer/PQ 恢复

CP 的 HEAP PQ 不在 managed keyed state 里，因此恢复还需要 raw keyed state：

```text
StreamTaskStateInitializerImpl::collectRawKeyedStateHandles
  -> InternalTimeServiceManager(rawKeyedStateHandles, ...)
      -> restoreRawKeyedState(rawKeyedStateHandles)
          -> restoreRawKeyGroupState(...)
              -> restoreStateForKeyGroup(...)
                  -> InternalTimerServiceSerializationProxy::read(...)
                      -> InternalTimeServiceManager::readTimersForKeyGroup(...)
                          -> readTimerServiceForNamespace(...)
                              -> registerOrGetTimerServiceForRestore(...)
                              -> InternalTimerServiceImpl::restoreTimersForKeyGroup(...)
```

恢复过程中会读取每个 key group 的 raw keyed timer stream，然后按 service name、key serializer、namespace serializer 重建 timer service，并将 event-time / processing-time timers 加入对应 HEAP PQ。

### 4.3 timer service 与 HEAP PQ 的创建关系

在 RocksDB backend 使用 HEAP PQ 时，timer service 创建队列时会走：

```text
InternalTimeServiceManager::registerOrGetTimerService
  -> priorityQueueSetFactory->create(...)
      -> RocksdbKeyedStateBackend::create(...)
          -> HeapPriorityQueuesManager::createOrUpdate(...)
              -> HeapPriorityQueueSetFactory::create(...)
              -> HeapPriorityQueueSnapshotRestoreWrapper
```

因此 CP 恢复链路中，raw keyed timer snapshot 读出来的 timer 会最终进入 HEAP PQ。

需要特别关注 processing-time timer：`InternalTimerServiceImpl::startTimerService(...)` 中会尝试重注册 processing-time head timer，这决定了恢复后 processing-time timer 是否还能继续触发。

---

## 5. SP 调用链分析

### 5.1 canonical savepoint 入口

savepoint 与 checkpoint 分流发生在 `StreamOperatorStateHandler::snapshotState(...)`：

```text
isCanonicalSavepoint(checkpointOptions->GetCheckpointType()) == true
  -> prepareCanonicalSavepoint(keyedStateBackend)
      -> keyedStateBackend->savepoint()
      -> SavepointSnapshotStrategy
```

`RocksdbKeyedStateBackend::requiresLegacySynchronousTimerSnapshots(...)` 对 savepoint 返回 false，所以 savepoint 不会写 raw keyed timer state。

也就是说：

- **CP：HEAP PQ/timer 写 raw keyed state；**
- **SP：HEAP PQ/timer 写 managed keyed full snapshot。**

### 5.2 RocksdbKeyedStateBackend::savepoint

`RocksdbKeyedStateBackend::savepoint()` 的核心逻辑是：

```text
RocksdbKeyedStateBackend::savepoint
  -> flush cache / write batch
  -> registeredPQStates = heapPriorityQueuesManager_->getRegisteredPQStates()
  -> RocksDBFullSnapshotResources::create(
         kvStateInformation,
         registeredPQStates,
         db,
         keyGroupRange,
         keyGroupPrefixBytes,
         ...)
  -> SavepointResources
```

这里开始将 HEAP PQ 正式纳入 savepoint full snapshot resources。

### 5.3 RocksDBFullSnapshotResources::create

`RocksDBFullSnapshotResources::create(...)` 会做两类事情：

1. 遍历 `kvStateInformation`，收集 KEY_VALUE 的 metadata；
2. 遍历 `registeredPQStates`，收集 PRIORITY_QUEUE 的 metadata 和 iterator。

当前实现中，PQ state id 从 KV state 数量之后开始分配：

```text
kvStateId: 0 ... kvStateCount - 1
pqStateId: kvStateCount ... kvStateCount + pqStateCount - 1
```

PQ 的 iterator 由 wrapper 创建：

```text
HeapPriorityQueueSnapshotRestoreWrapper::createSnapshotIterator
  -> HeapPriorityQueueSingleStateIterator
```

每条 PQ entry 的 key 大致为：

```text
[key-group-prefix] + [serialized timer / priority queue element]
```

value 当前为空。

### 5.4 FullSnapshotAsyncWriter 写出

savepoint writer 通过 merge iterator 将 RocksDB KV entries 和 HEAP PQ entries 合并写出：

```text
SavepointSnapshotStrategy
  -> FullSnapshotAsyncWriter::get(...)
      -> snapshotResources->createKVStateIterator()
          -> RocksStatesPerKeyGroupMergeIterator
              -> RocksDB KV iterators
              -> HEAP PQ iterators
      -> writeMetadata(...)
      -> write key-group/state-id/key/value entries
      -> KeyGroupsSavepointStateHandle
```

因此，SP 中的 HEAP PQ 是作为 `BackendStateType::PRIORITY_QUEUE` 的 managed keyed state 写入 savepoint，而不是 raw keyed state。

---

## 6. SP 恢复调用链分析

### 6.1 restore operation 选择

`RocksDBKeyedStateBackendBuilder::getRocksDBRestoreOperation(...)` 根据 state handle 和 PQ 类型选择恢复策略：

```text
if no state handles:
  RocksDBNoneRestoreOperation
else if first handle is incremental:
  RocksDBIncrementalRestoreOperation
else if priorityQueueStateType == HEAP:
  RocksDBHeapTimersFullRestoreOperation
else:
  RocksDBFullRestoreOperation
```

当前默认 `priorityQueueStateType` 是 HEAP，也可以通过环境变量 `PriorityQueueStateType` 设置为 `ROCKSDB`。

### 6.2 RocksDBHeapTimersFullRestoreOperation

HEAP PQ savepoint 恢复的核心链路：

```text
RocksDBHeapTimersFullRestoreOperation::restore
  -> FullSnapshotRestoreOperation::restore
  -> applyRestoreResult(...)
      -> 遍历 restored meta info
          -> KEY_VALUE: register column family
          -> PRIORITY_QUEUE: create RestoredHeapPriorityQueueSnapshotRestoreWrapper
      -> restoreKVStateData(...)
          -> stateId 属于 KEY_VALUE: write batch put 到 RocksDB
          -> stateId 属于 PRIORITY_QUEUE: restoreSerializedElement 到 pending wrapper
```

这里的 pending wrapper 是：

```text
RestoredHeapPriorityQueueSnapshotRestoreWrapper
```

它不会马上反序列化 timer，因为此时还不一定知道具体 timer service 的 key/namespace/element 类型。

### 6.3 pending wrapper drain

backend 构建完成后，`registeredPQStates` 会传给 `RocksdbKeyedStateBackend`。当 operator 后续创建 timer service 时，会调用：

```text
RocksdbKeyedStateBackend::create(...)
  -> HeapPriorityQueuesManager::createOrUpdate(...)
```

如果发现对应 state name 已经是 pending wrapper：

```text
RestoredHeapPriorityQueueSnapshotRestoreWrapper
```

则会创建真实的：

```text
HeapPriorityQueueSnapshotRestoreWrapper
```

然后执行：

```text
pendingRestoredState->drainTo(realWrapper, keyGroupPrefixBytes)
```

最终 serialized PQ entries 被反序列化并加入真实 HEAP PQ。

这个设计是合理的，因为 savepoint restore 时，PQ state metadata 先被恢复，但真正的 timer serializer 和 comparator 通常要等 timer service 创建时才完整可用。

---

## 7. 当前实现的主要问题和风险

### 7.1 P0/P1：raw timer snapshot 格式不是 Flink Java 1.16.3 原生格式

当前 CP raw timer snapshot 中使用：

```text
keySerializerJson
namespaceSerializerJson
```

再通过 JSON 或字符串启发式推断 serializer。这与 Flink Java 1.16.3 中 `TypeSerializerSnapshot` 的二进制兼容机制并不等价。

影响：

- C++ 自己写、自己读，简单场景可能可行；
- 如果目标是和 Flink Java 1.16.3 checkpoint/savepoint 格式互通，目前兼容性不足；
- serializer migration / compatibility resolution 基本无法正确表达。

建议：明确当前目标是 **C++ 内部闭环兼容** 还是 **Flink Java 格式兼容**。如果要兼容 Flink，需要实现更接近 Flink 的 serializer snapshot wire format，而不是 JSON 近似。

---

### 7.2 P0：CP 与 SP 的 HEAP PQ 持久化路径完全不同，必须分别验证

当前实现中：

| 类型 | HEAP PQ 存储位置 |
|---|---|
| CP | raw keyed state |
| SP | managed keyed full snapshot |

这符合 RocksDB + HEAP timer 的思路，但也意味着测试不能只覆盖一条路径。

必须覆盖：

- CP -> CP 恢复；
- SP -> SP 恢复；
- CP 恢复后再做 SP；
- SP 恢复后再做 CP；
- CP/SP 混合链路下 timer 不重复、不丢失。

---

### 7.3 P0：serializer / namespace 推断比较脆弱

当前 `InternalTimersSnapshotReaderWriters` 中 serializer 恢复大体依赖：

- JSON 中的 type；
- 字符串 fallback；
- namespace kind 推断；
- key serializer fallback。

风险点：

1. `VoidNamespace`、`TimeWindow`、空 JSON 的区分不够稳；
2. `BinaryRowData`、`RowData`、复杂 Object key 可能无法被可靠恢复；
3. restore 时如果 fallback key serializer 与 snapshot 写入时不一致，可能静默错读；
4. namespace serializer 一旦识别错误，timer 会恢复到错误 namespace 类型。

建议：

- timer snapshot 中显式记录 namespace kind；
- 不要用空 JSON 推断 TimeWindow；
- 为 key serializer / namespace serializer 增加明确 type id 与版本；
- 恢复时做强校验，失败要 fail fast。

---

### 7.4 P1：managed CP metadata 不包含 HEAP PQ，依赖 raw keyed timer snapshot 一定执行

`RocksDBSnapshotStrategyBase`、`RocksNativeFullSnapshotStrategy`、`RocksIncrementalSnapshotStrategy` 的 metadata 都只来自 `kvStateInformation_`。

这意味着 CP 中 HEAP PQ 的正确性完全依赖：

```text
requiresLegacySynchronousTimerSnapshots(...) == true
&& timeServiceManager != nullptr
&& raw keyed state 被最终写入 OperatorSubtaskState
```

如果其中任何一个条件被破坏，CP 中 timer/PQ 会丢失，而 RocksDB managed CP 自身不会发现。

建议：

- 在 RocksDB + HEAP PQ 且 CP 场景下增加明确日志；
- 如果 backend 已注册 HEAP PQ 但 `timeServiceManager == nullptr`，考虑 fail fast 或至少 warning；
- 在 CP 完成前记录 raw timer service 数、timer 数、key group 数。

---

### 7.5 P1：savepoint metadata 顺序不完全稳定

`RocksDBFullSnapshotResources::create(...)` 中：

- PQ state name 已经排序；
- KV state 仍来自 `unordered_map` 遍历。

虽然单个 snapshot 内 metadata 与 data 的 state id 是一致的，但 unordered 顺序会导致 savepoint 字节不稳定，也可能给跨版本/测试带来不必要的不确定性。

建议：KV state metadata 也按 state name 排序后再分配 state id，与 PQ 保持一致。

---

### 7.6 P1：HeapPriorityQueuesManager 的 serializer compatibility 仍是 TODO

`HeapPriorityQueuesManager::createOrUpdate(...)` 中存在类似 TODO：

```text
// todo: serializer compatibility checks
```

当前 pending restored PQ drain 时，基本信任当前 serializer 可以读取历史 serialized element。

风险：

- timer key serializer 变更后可能读错；
- namespace serializer 变更后可能恢复错误；
- element serializer 变更后可能出现内存错误或 silent data corruption。

建议：引入类似 Flink 的 compatibility check：compatible-as-is、compatible-after-migration、incompatible，并在不兼容时 fail fast。

---

### 7.7 P1：`InternalTimerServiceImpl::restoreTimersForKeyGroup` 只保留最后一个 restored snapshot 用于校验

当前恢复每个 key group 时会设置：

```text
restoredTimersSnapshot = timersSnapshot
hasRestoredTimersSnapshot = true
```

如果多个 key group 都恢复了 timers，最后只保留最后一个 snapshot 对象。实际 timers 已经加进 queue，所以数据本身可能不丢；但 `startTimerService(...)` 中的 serializer compatibility 检查只会看到最后一个 key group 的 snapshot。

建议：

- serializer compatibility 应该按 timer service 维度保存，而不是覆盖成最后一个 key group；
- 或者在每个 key group restore 时立刻检查 serializer，一旦不一致直接失败。

---

### 7.8 P1：event-time timer 的 watermark 触发条件疑似不符合 Flink 语义

`InternalTimerServiceImpl::advanceWatermark(...)` 中循环条件类似：

```cpp
while (timer != nullptr && timer->getTimestamp() < time) {
    ...
}
```

Flink event-time timer 通常应在：

```text
timer.timestamp <= currentWatermark
```

时触发。

如果当前实现使用 `< time`，则 timestamp 正好等于 watermark 的 timer 可能不会在当前 watermark 触发，而是要等下一个更大的 watermark。

这不是纯 snapshot 问题，但会直接影响 CP/SP 恢复后 timer 行为验证。

建议确认语义，必要时改为 `<=`。

---

### 7.9 P2：raw keyed restore 目前只接受 `KeyGroupsStateHandle`

`InternalTimeServiceManager::restoreRawKeyedState(...)` 当前对 raw keyed handle 做了具体类型假设：

```text
KeyGroupsStateHandle
```

如果将来 raw keyed state 以其他 handle 类型出现，例如 savepoint handle 或包装 handle，可能无法恢复。

当前 CP raw keyed state 大概率就是 `KeyGroupsStateHandle`，所以短期可接受；但最好抽象为统一的 key-group-offset handle 接口。

---

### 7.10 P2：RocksStatesPerKeyGroupMergeIterator 使用 `const_cast` 移动 `priority_queue::top()` 元素，代码风险较高

merge iterator 内部为了从 `std::priority_queue` 的 `top()` 中移动 `unique_ptr`，使用了较激进的 `const_cast` 方式。

这类写法容易引入未定义行为或后续维护风险。

建议改为：

- 使用 `std::vector` + `std::make_heap` / `std::pop_heap`；
- 或 priority queue 中存 raw pointer / shared pointer；
- 或自定义可安全 pop/move 的 heap 容器。

---

### 7.11 P2：HEAP PQ iterator 只按 key-group prefix 排序，组内顺序不稳定

`HeapPriorityQueueSingleStateIterator` 当前主要保证按 key group 分组，组内元素顺序没有稳定 tie-break。

恢复正确性一般不依赖组内顺序，但会影响：

- savepoint 字节级稳定性；
- 单测断言；
- 后续 debug 可读性。

建议排序 key 增加：

```text
keyGroup -> stateId -> serializedElement bytes
```

---

### 7.12 P2：key-group prefix bytes 逻辑建议统一

`HeapPriorityQueuesManager` 中通过：

```cpp
numberOfKeyGroups_ > 128 ? 2 : 1
```

计算 key group prefix bytes。

这与 Flink 常见逻辑一致，但项目中 RocksDB 其他地方可能已有类似工具方法。建议统一到一个工具函数，避免后续边界条件分叉。

---

### 7.13 P2：PQ restore 的内存所有权需要审计

`HeapPriorityQueueSnapshotRestoreWrapper::restoreSerializedElement(...)` 中大致流程是：

```text
serializer->deserialize(input) -> raw pointer
T restoredElement(static_cast<ElementType*>(rawElement))
priorityQueue->add(restoredElement)
```

这要求：

- serializer 返回的对象必须由调用方接管；
- `T` 必须是合适的智能指针类型；
- raw pointer 类型必须和 `ElementType` 完全匹配。

对于 timer 场景可能成立，但建议加静态约束或封装，避免后续泛型 PQ 类型引入悬空指针、重复释放或类型错误。

---

### 7.14 P2：SP 恢复强依赖 metadata 中 `BackendStateType::PRIORITY_QUEUE`

`RocksDBHeapTimersFullRestoreOperation` 根据 restored meta info 的 backend state type 分类：

```text
KEY_VALUE -> RocksDB column family
PRIORITY_QUEUE -> pending restored PQ wrapper
```

如果 metadata 写入/读取桥接层没有正确保留 `PRIORITY_QUEUE` 类型，PQ entry 会被错误当成 KV 写入 RocksDB，timer 就无法恢复。

建议针对 metadata bridge 增加单测：

- 写入一个 KEY_VALUE + 一个 PRIORITY_QUEUE；
- 读回后确认 state name、state id、backend type 完全一致。

---

## 8. 建议补充的验证用例

### 8.1 CP -> CP 恢复

1. 单 timer service，event-time timer 恢复后能触发；
2. 单 timer service，processing-time timer 恢复后能重注册并触发；
3. 多 timer service，不同 service name 不串数据；
4. 多 key group，恢复后每个 key group 的 timer 数正确；
5. 空 timer service / 空 key group 不报错；
6. timestamp 等于 watermark 的 event-time timer 是否按预期触发；
7. CP 恢复后再次 CP，timer 不重复、不丢失。

### 8.2 CP rescale

1. 原并行度 N，恢复到并行度 M；
2. raw keyed handle 与 local key group range 有交集时只恢复交集；
3. 无交集时不恢复；
4. 所有 key group 合计 timer 数保持一致。

### 8.3 SP -> SP 恢复

1. KEY_VALUE + PRIORITY_QUEUE 同时存在；
2. savepoint metadata 中同时包含 `KEY_VALUE` 和 `PRIORITY_QUEUE`；
3. `RocksDBHeapTimersFullRestoreOperation` 能正确创建 pending wrapper；
4. operator 创建 timer service 后 pending entries 被 drain 到真实 HEAP PQ；
5. drain 后再次 savepoint，不重复写 pending entries；
6. savepoint 恢复后 event-time / processing-time timer 正常触发。

### 8.4 混合链路

1. CP 恢复后立即做 SP，再 SP 恢复；
2. SP 恢复后立即做 CP，再 CP 恢复；
3. 多次恢复后 timer 数不重复；
4. 恢复过程中 operator 延迟创建 timer service，pending wrapper 仍能正确保留数据。

### 8.5 serializer 兼容性

1. key serializer 一致：恢复成功；
2. key serializer 不一致：应 fail fast；
3. namespace serializer 一致：恢复成功；
4. namespace serializer 不一致：应 fail fast；
5. TimeWindow / VoidNamespace / Long namespace 分别覆盖；
6. BinaryRowData / Object key 场景覆盖。

---

## 9. 修改建议优先级

### 优先级 1：先保证 C++ 内部闭环正确

- 为 CP raw timer snapshot 增加完整单测；
- 为 SP managed PQ snapshot 增加完整单测；
- 验证 CP/SP 混合链路；
- 修复 event-time timer `<= watermark` 语义；
- 增加 timer 数、service 数、key group 数的恢复前后断言。

### 优先级 2：补齐 serializer compatibility

- 不再仅依赖 JSON 字符串或 backendId；
- 为 timer key serializer、namespace serializer、element serializer 引入明确版本与类型；
- restore 时进行强校验；
- 不兼容时直接失败，而不是静默恢复。

### 优先级 3：增强 savepoint 确定性和可维护性

- KV state metadata 也按 state name 排序；
- PQ iterator 组内增加稳定排序；
- 统一 key-group prefix bytes 工具函数；
- 清理 `RocksStatesPerKeyGroupMergeIterator` 中危险的 `const_cast` move 写法。

### 优先级 4：如果目标是 Flink Java 1.16.3 格式兼容

当前实现还不够，需要进一步对齐：

- Flink 的 timer serializer snapshot 格式；
- Flink 的 `TypeSerializerSnapshot` 读写与 compatibility resolution；
- savepoint metadata bridge 对 `BackendStateType::PRIORITY_QUEUE` 的完整兼容；
- RocksDB + HEAP timers 在 Java Flink 中的 raw keyed / managed savepoint 格式细节。

---

## 10. 最终判断

当前这版实现已经形成了比较完整的 HEAP PRIORITY_QUEUE 快照恢复框架：

```text
CP:
  HEAP PQ/timer -> raw keyed state
  KEY_VALUE     -> RocksDB managed keyed state

CP restore:
  KEY_VALUE     -> RocksDB restore operation
  HEAP PQ/timer -> InternalTimeServiceManager raw keyed restore

SP:
  KEY_VALUE + HEAP PQ -> RocksDBFullSnapshotResources -> FullSnapshotAsyncWriter

SP restore:
  KEY_VALUE -> RocksDB column family
  HEAP PQ   -> pending wrapper -> timer service 创建时 drain 到真实 HEAP PQ
```

从架构上看，这条路线是可行的；但从工程可靠性看，目前还不能认为已经完全稳定。最需要优先解决的是：

1. raw timer snapshot serializer 格式与兼容性；
2. CP/SP 两套路径的端到端测试；
3. savepoint metadata state id 顺序稳定性；
4. pending wrapper drain 后的重复/丢失验证；
5. event-time / processing-time timer 恢复后的真实触发语义。

如果当前目标只是 **OmniStream C++ 内部闭环**，这版可以作为第一版继续补测试与修边界问题。  
如果目标是 **与 Flink Java 1.16.3 checkpoint/savepoint 格式完全互通**，当前 serializer snapshot 与 metadata 兼容层还需要明显加强。
