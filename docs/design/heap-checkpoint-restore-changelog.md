# Heap 状态后端 Checkpoint & Restore 实现记录

## 概述

本文档记录 Flink DataStream Heap 状态后端 Checkpoint & Restore 功能的实现变更。在此之前，Snapshot（写）已部分实现但存在类型覆盖缺口，Restore（读）仅有骨架未实际写入数据。本次工作补齐了这两部分。

---

## 任务 1：修复 `StringToType()` 支持数字编码

**文件**: `cpp/core/api/common/state/StateDescriptor.h`

**问题**: `RegisteredKeyValueStateBackendMetaInfo::computeSnapshot()` 将 `KEYED_STATE_TYPE` 存储为数字字符串（如 MAP → `"6"`），但 `StateDescriptor::StringToType()` 只识别字符串名（如 `"MAP"`），导致 restore 时无法正确解析状态类型。

**修复**: 在 `StringToType()` 的映射表中增加数字编码：

```cpp
// 新增数字编码映射（computeSnapshot 存储格式）
{"0", Type::UNKNOWN}, {"1", Type::VALUE}, {"2", Type::LIST},
{"3", Type::REDUCING}, {"4", Type::FOLDING},
{"5", Type::AGGREGATING}, {"6", Type::MAP}
```

---

## 任务 2：补全 `createFullSnapshotResources()` 类型覆盖

**文件**: `cpp/runtime/state/HeapKeyedStateBackend.h`

### 2.1 扩展 `registeredKvStates` 存储结构

将 `registeredKvStates` 从 `tuple<uintptr_t, StateDescriptor*>` 扩展为 `tuple<uintptr_t, StateDescriptor*, BackendDataType>`，第三个元素存储 namespace 的 BackendDataType，用于在 snapshot 时区分 VoidNamespace 和非 VoidNamespace 的 VALUE/LIST 类型。

### 2.2 新增 MAP 类型覆盖（8 种）

原有 3 种 MAP 组合（INT×INT, VARCHAR×INT, OBJECT×OBJECT），新增 8 种：

| MAP 类型 | emhash7 模板 |
|----------|-------------|
| BIGINT×BIGINT | `emhash7::HashMap<int64_t, int64_t>*` |
| ROW×INT | `emhash7::HashMap<RowData*, int32_t>*` |
| ROW×ROW | `emhash7::HashMap<RowData*, RowData*>*` |
| XXHASH128×TUPLE_INT32_INT64 | `emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>>*` |
| XXHASH128×TUPLE_INT32_INT32_INT64 | `emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>*` |
| TIME_WINDOW×TIME_WINDOW | `emhash7::HashMap<TimeWindow, TimeWindow>*` |
| ROW×ROW_LIST | `emhash7::HashMap<RowData*, std::vector<RowData*>*>*` |
| OBJECT×OBJECT | （已有，确认覆盖） |

### 2.3 新增非 VoidNamespace 的 VALUE/LIST 类型

| 类型 | Namespace | Value |
|------|-----------|-------|
| VALUE | int64_t | RowData* |
| VALUE | TimeWindow | RowData* |
| LIST | int64_t | std::vector\<int64_t\>* |

### 2.4 添加公共访问方法

- `getStateTablePtr(const std::string &name)` — 通过状态名获取 state table 指针
- `getRegisteredState(const std::string &name)` — 获取注册状态的完整元组

---

## 任务 3：新建 `RestoreStateDescriptor`

**文件**: `cpp/core/api/common/state/RestoreStateDescriptor.h`（新建）

一个非模板的 `StateDescriptor` 子类，用于 restore 时从 `StateMetaInfoSnapshot` metadata 重建状态描述符。存储：

- `type_` — 状态类型（VALUE/LIST/MAP）
- `backendId_` — 值的 BackendDataType（VALUE/LIST 使用）
- `keyDataId_` — MAP key 的 BackendDataType
- `valueDataId_` — MAP value 的 BackendDataType

```cpp
class RestoreStateDescriptor : public StateDescriptor {
public:
    RestoreStateDescriptor(const std::string &name, Type type,
                           TypeSerializer *stateSerializer, BackendDataType backendId,
                           BackendDataType keyDataId = BackendDataType::VOID_NAMESPACE_BK,
                           BackendDataType valueDataId = BackendDataType::VOID_NAMESPACE_BK);
    Type getType() override { return type_; }
    BackendDataType getBackendId() override { return backendId_; }
    BackendDataType getKeyDataId() override { return keyDataId_; }
    BackendDataType getValueDataId() override { return valueDataId_; }
};
```

---

## 任务 4：添加序列化器 accessor 方法

### 4.1 MapSerializer

**文件**: `cpp/core/typeutils/MapSerializer.h`

```cpp
TypeSerializer* getKeySerializer() const { return keySerializer; }
TypeSerializer* getValueSerializer() const { return valueSerializer; }
```

### 4.2 ListSerializer

**文件**: `cpp/core/typeutils/ListSerializer.h`

```cpp
TypeSerializer* getElementSerializer() const { return elementSerializer; }
```

---

## 任务 5：实现 Heap Restore 核心逻辑

**文件**: `cpp/runtime/state/heap/HeapKeyedStateBackendBuilder.h`

### 5.1 整体架构

```
HeapKeyedStateBackendBuilder::build()
  ├─ 创建 backend
  ├─ FullSnapshotRestoreOperation::restore() → SavepointRestoreResultIterator
  └─ while (restoreIterator->hasNext())
      ├─ Phase 1: 从 StateMetaInfoSnapshot 创建 state table
      │   ├─ 获取 KEYED_STATE_TYPE → StateDescriptor::Type
      │   ├─ 获取 NAMESPACE_SERIALIZER、VALUE_SERIALIZER
      │   ├─ createRestoreDescriptor() → RestoreStateDescriptor
      │   └─ backend->createOrUpdateInternalState() 创建 state table
      └─ Phase 2: 遍历 KV entries 反序列化并写入 state table
          ├─ keyGroupIterator → keyGroup → entryIterator → entry
          ├─ keySerializer->deserialize() → key
          ├─ namespaceSerializer->deserialize() → namespace
          ├─ 类型分发 → deserialize value → stateTable->put()
          └─ restoreEntryToHeap() 处理全部类型分支
```

### 5.2 关键辅助结构

```cpp
struct RestoreStateInfo {
    StateDescriptor *stateDesc;
    TypeSerializer *namespaceSerializer;
    TypeSerializer *valueSerializer;
};
```

### 5.3 类型分发 — restoreEntryToHeap()

与 `createFullSnapshotResources()` 镜像对称的类型分发，覆盖：

- **VALUE**: OBJECT/POJO (Object* path), INT, BIGINT, ROW (VoidNamespace); int64_t ns + ROW, TimeWindow ns + ROW
- **LIST**: BIGINT (VoidNamespace 和 int64_t namespace)
- **MAP**: 全部 11 种组合

---

## 发现并修复的序列化器问题

在实现过程中发现多个序列化器的 `void*` 路径未实现（`NOT_IMPL_EXCEPTION`），导致 Heap 状态后端的 snapshot 和 restore 无法工作。

### 问题 1: MapSerializer::serialize/deserialize(void*) = NOT_IMPL

**影响**: 所有 MAP 类型的 snapshot 和 restore 均不工作。

**原因**: `MapSerializer` 设计时仅实现了 `Object*` 路径（使用 `java_util_HashMap`），但 Heap 状态后端存储的是 `emhash7::HashMap<UK,UV>*`（原生 C++ 类型），无法通过 `Object*` 路径处理。

**修复**: 在 `HeapSingleStateIterator` 中添加 `serializeEmhashMap()` 静态模板方法，直接使用 MapSerializer 的子序列化器逐条序列化 emhash7 entries。在 `HeapKeyedStateBackendBuilder` 中添加对称的 `deserializeEmhashMap()` 方法。

**序列化格式**: `[int size] [for each entry: serialize(key) + bool isNull + serialize(value)]`

**文件**: `cpp/runtime/state/heap/HeapSingleStateIterator.h`, `cpp/runtime/state/heap/HeapKeyedStateBackendBuilder.h`

### 问题 2: ListSerializer::serialize/deserialize(void*) = NOT_IMPL

**影响**: 所有 LIST 类型的 snapshot 和 restore 均不工作。

**原因**: 与 MapSerializer 相同，`ListSerializer` 仅实现了 `Object*` 路径（使用 `java_util_ArrayList`），但 Heap 状态后端存储 `std::vector<V>*`。

**修复**: 在 `HeapSingleStateIterator` 中添加 `serializeVector()` 方法，在 `HeapKeyedStateBackendBuilder` 中添加 `deserializeVector()` 方法。

**序列化格式**: `[int size] [elem_1] [elem_2] ...`

### 问题 3: IntSerializer::serialize/deserialize(void*) = NOT_IMPL

**影响**: VALUE\<int\> 状态的 snapshot/restore，以及 MAP 中含 int 类型 key/value 的序列化。

**修复** (`cpp/core/typeutils/LongSerializer.cpp`):

```cpp
void *IntSerializer::deserialize(DataInputView &source)
{
    return reinterpret_cast<void *>(new int(source.readInt()));
}

void IntSerializer::serialize(void *record, DataOutputSerializer &target)
{
    target.writeInt(*(int *)record);
}
```

### 问题 4: PojoSerializer::deserialize(void*) = NOT_IMPL

**影响**: VALUE\<Object*\> 和 MAP\<Object*, Object*\> 的 restore。

**修复**: 在 restore 代码中，对 OBJECT/POJO 类型改用 `GetBuffer()` + `deserialize(Object*, DataInputView&)` 路径，在 serialize 时利用 C++ 重载解析自动选择 `serialize(Object*, ...)` 而非 `serialize(void*, ...)`。

### 问题 5: ListSerializer::getBackendId() 返回 OBJECT_BK

**影响**: LIST restore 时类型分发失败——`createRestoreDescriptor` 使用 `valSerializer->getBackendId()` 得到 `OBJECT_BK`，但 `restoreEntryToHeap` 的 LIST 分支检查 `BIGINT_BK`，永远不匹配。

**修复**: `createRestoreDescriptor` 中对 LIST 类型特殊处理，从 `ListSerializer::getElementSerializer()->getBackendId()` 获取正确的元素类型 BackendDataType。

---

## 类型 trait 设计

为了在编译期区分 `emhash7::HashMap*` 和 `std::vector*` 类型，避免调用未实现的 `void*` 序列化路径，引入两个类型 trait：

```cpp
// 定义于 HeapSingleStateIterator.h
template<typename T> struct IsEmhashMapPtr : std::false_type {};
template<typename UK, typename UV> struct IsEmhashMapPtr<emhash7::HashMap<UK, UV>*> : std::true_type {};

template<typename T> struct IsVectorPtr : std::false_type {};
template<typename V> struct IsVectorPtr<std::vector<V>*> : std::true_type {};
```

在 `serializeValue()` 中使用 `if constexpr` 分支：

```
IsEmhashMapPtr<S>  → serializeEmhashMap()   // MAP 类型
IsVectorPtr<S>     → serializeVector()       // LIST 类型
is_pointer_v<S>    → serialize(void*, ...)   // 其他指针类型（RowData*, Object* 等）
else               → serialize(&val, ...)    // 值类型（int, int64_t 等）
```

---

## 变更文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `cpp/core/api/common/state/StateDescriptor.h` | 修改 | `StringToType()` 增加数字编码映射 |
| `cpp/core/api/common/state/RestoreStateDescriptor.h` | **新建** | 非模板 StateDescriptor 子类 |
| `cpp/core/typeutils/MapSerializer.h` | 修改 | 添加 `getKeySerializer()`/`getValueSerializer()` |
| `cpp/core/typeutils/ListSerializer.h` | 修改 | 添加 `getElementSerializer()` |
| `cpp/core/typeutils/LongSerializer.cpp` | 修改 | 实现 `IntSerializer` 的 void* serialize/deserialize |
| `cpp/runtime/state/HeapKeyedStateBackend.h` | 修改 | 补全 snapshot 类型覆盖；扩展 registeredKvStates；添加 public getter |
| `cpp/runtime/state/heap/HeapSingleStateIterator.h` | 修改 | 添加类型 trait、`serializeEmhashMap()`、`serializeVector()`；修改 `serializeValue()` |
| `cpp/runtime/state/heap/HeapKeyedStateBackendBuilder.h` | **重写** | 完整 restore 实现（Phase 1 + Phase 2 + 类型分发 + 反序列化 helpers） |

---

## 已知限制

1. **MAP\<VARCHAR, INT\>**: emhash7 map 使用 `std::string` 作为 key，但 `StringSerializer` 的 void* 路径使用 `std::u32string`，类型不匹配，snapshot/restore 可能不工作。
2. **Operator State**: `OperatorStateBackend::snapshot()` 返回 `nullptr`，未实现。影响 Kafka Source/Sink 的 offset 持久化。

---

## 验证方式

1. **单元测试**: 创建 HeapKeyedStateBackend → 写入各类型状态 → snapshot → 新建 backend → restore → 验证状态数据一致
2. **端到端验证**: Flink DataStream 用例（如 WordCount）设置 Heap 后端 + Checkpoint → kill → 从 checkpoint 恢复 → 验证结果正确
