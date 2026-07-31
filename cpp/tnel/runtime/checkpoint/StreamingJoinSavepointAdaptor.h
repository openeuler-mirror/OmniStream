/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <xxhash.h>

#include "OperatorSavepointAdaptor.h"
#include "core/utils/ByteView.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "table/types/logical/LogicalType.h"

namespace omnistream {

// restore 方向带 VectorBatch side table 的 KV 状态 writer 声明。
class RestoreKVStateVB;

// StreamingJoin NoUniqueKey 兼容 savepoint 适配器。
//
// Omni 运行时将左右两侧状态拆成两类存储：
//   主 MapState：key 后缀保存 XXH128(row)，value 保存 count/(numAssociations)/comboId。
//   VB 侧表：通过 comboId 引用完整 RowData。
// Flink 标准 StreamingJoin 则期望每侧只有一个逻辑 MapState：map key 内包含完整 RowData 字节，
// map value 只保存 count 相关字段。该适配器负责在算子边界完成两种格式互转，并复用
// VectorBatchSaveFlow 读取 VB 侧表。
class StreamingJoinSavepointAdaptor : public OperatorSavepointAdaptor, public VectorBatchSaveHooks {
public:
    // 使用工厂判定出的 adaptorType 创建 StreamingJoin 格式互通适配器，算子描述在 prepare 阶段解析。
    explicit StreamingJoinSavepointAdaptor(FlinkSavepointAdaptorType adaptorType);

    // 适配器不单独持有 serializer、restore backend 等外部资源，使用默认析构逻辑。
    ~StreamingJoinSavepointAdaptor() override = default;

    /*========== OperatorSavepointAdaptor ==========*/

    // 解析 Omni -> Flink save 所需的左右输入类型和 Join 状态布局。
    void prepareForSave(const nlohmann::json& operatorDescription) override;

    // 解析 Flink -> Omni restore 所需的左右输入类型和 Join 状态布局。
    void prepareForRestore(const nlohmann::json& operatorDescription) override;

    // 校验 Omni 源状态包含左右主状态和 VB 侧表，并验证 Omni serializer 布局。
    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    // 校验 Flink 源状态只包含左右 logical MapState，并验证 Flink serializer 布局。
    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    // 按 Flink StreamingJoin logical MapState 格式写出兼容 savepoint。
    void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) override;

    // 将 Flink StreamingJoin logical MapState 恢复为 Omni 主状态和 VB 侧表。
    void restore(SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend) override;

    /*========== OperatorSavepointAdaptor ==========*/
    /*========== VectorBatchSaveHooks ==========*/

    // 根据保存计划创建各主状态对应的 VB accessor 和格式转换上下文。
    std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) override;

    // 从 Omni Join tuple value 中读取 comboId，作为 VB 侧表完整行数据的引用。
    omnistream::ComboId parseVectorBatchReference(
        ByteView value, const VectorBatchSaveStateContext& context, const VectorBatchSavePlan& plan) override;

    // 将 Omni keyed key 前缀和 VB RowData 编码为 Flink logical MapState key。
    std::vector<int8_t> encodeFlinkLogicalKey(
        const KeyValueStateIterator::CurrentEntry& entry,
        RowData& row,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan) override;

    // 将 Omni Join tuple 中的计数字段编码为 Flink logical MapState value。
    std::vector<int8_t> encodeFlinkLogicalValue(
        const KeyValueStateIterator::CurrentEntry& entry,
        RowData& row,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan) override;

    // Join 主状态的一个 source entry 可能引用多个 VB row；这里把每个 comboId
    // 解引用为一个 Flink logical MapState entry，供 VectorBatchSaveFlow 写出。
    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& entry,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan,
        std::function<void(ConvertedEntry)> output) override;

    /*========== VectorBatchSaveHooks ==========*/
    /*========== Restore ==========*/

    // 返回指定 Flink 状态的 restore 处理类型。
    RestoreStateType getStateType(const StateMetaInfoSnapshot& metaInfo) const;

    // 根据 kvStateId 对应的 Flink 状态元数据构造 Omni 主状态元数据。
    StateMetaInfoSnapshot buildOmniMainMetaInfo(int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo);

    std::vector<omniruntime::type::DataTypeId> columnTypes(int kvStateId) const;

    int batchSize(int kvStateId) const;

    // 解析 Flink logical key/value 中的 RowData 和计数信息，并通过 VB writer 写入 Omni VB 与 map entry。
    void retrieveKVRowData(
        const std::vector<int8_t>& keyBytes,
        const std::vector<int8_t>& valueBytes,
        int kvStateId,
        RestoreKVStateVB* writer);

    /*========== Restore ==========*/

private:
    // 单侧 Join 状态的格式转换参数。
    struct SidePlan {
        // 当前侧在 Flink 和 Omni 状态元数据中共同使用的 logical state name。
        std::string stateName;
        // 算子描述中的 Flink 输入逻辑类型名称，用于构造 RowType serializer。
        std::vector<std::string> inputTypeNames;
        // 只接管工厂动态创建的 LogicalType，静态单例仍由类型系统持有。
        std::vector<std::unique_ptr<LogicalType>> ownedInputTypes;
        // 转换后的 LogicalType 对象，用于构造 RowData serializer；在进入公共 VectorBatch 流程前再转换为 DataTypeId。
        std::vector<LogicalType*> inputTypes;
        // 当前侧 value 是否包含 left outer join 的 numAssociations 字段。
        bool outerJoinState = false;
    };

    // 根据算子描述初始化左右两侧的输入类型、状态名称和 outer join 标记。
    void prepareSidePlans(const nlohmann::json& operatorDescription);

    // 从算子描述的数组字段中解析输入字段类型名称，并拒绝缺失、非数组或非字符串元素。
    void parseInputTypes(SidePlan& sidePlan, const nlohmann::json& description, const std::string& fieldName);

    // 将 Heap 聚合或普通 Omni MapState entry 统一展开并直接回调输出。
    void parseSourceMapEntries(
        const KeyValueStateIterator::CurrentEntry& entry,
        const SidePlan& sidePlan,
        const std::function<void(ByteView keyBytes, ByteView valueBytes, omnistream::ComboId comboId)>& emit) const;

    // 根据源状态元数据构造 VectorBatchSaveFlow 所需的保存计划。
    VectorBatchSavePlan buildSavePlan(FullSnapshotResources& snapshotResources);

    // 根据状态名称返回左右两侧对应的 Join 状态计划。
    const SidePlan& sidePlanForState(const std::string& stateName) const;

    // 返回 restore writer 创建阶段为 kvStateId 绑定的 Join 单侧计划。
    const SidePlan& restoreSidePlan(int kvStateId) const;

    // restore 方向根据 Flink RowData bytes 计算运行态 StreamingJoin 主 MapState 使用的 XXH128(row)。
    XXH128_hash_t calculateRestoreRowHash(
        const std::vector<int8_t>& rowBytes, const std::vector<omniruntime::type::DataTypeId>& columnTypes) const;

    // VB 反序列化行缓存上限，避免 save 转换过程中重复解码同一批数据。
    static constexpr std::size_t VB_SAVE_CACHE_BYTES = 64UL * 1024 * 1024;

    // 创建 adaptor 时确定的 StreamingJoin 兼容类型，用于区分 inner 和 left outer 状态布局。
    FlinkSavepointAdaptorType adaptorType_;

    // 左侧 Join 状态的名称、输入类型和状态布局参数。
    SidePlan leftPlan_;

    // 右侧 Join 状态的名称、输入类型和状态布局参数。
    SidePlan rightPlan_;

    // 当前 restore handle 中左侧状态的 kvStateId。
    int leftRestoreKvStateId_ = -1;

    // 当前 restore handle 中右侧状态的 kvStateId。
    int rightRestoreKvStateId_ = -1;
};

} // namespace omnistream
