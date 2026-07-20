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

#include <cstdint>
#include <string>
#include <vector>

#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreKVStateVB.h"

// 前置声明：避免头文件中直接依赖 SVE（table/）类型，防止 kacc 组件编译失败
// 注意：VectorBatch 定义在 namespace omnistream 内，BinaryRowData 定义在全局命名空间
class BinaryRowData;

namespace omnistream {

class VectorBatch;

// 序列化结果：替代 table/utils/VectorBatchSerializationUtils.h 中的 SerializedBatchInfo
struct SerializedVbBatchInfo {
    uint8_t* buffer = nullptr;
    int32_t size = 0;
};

// 恢复方向无状态工具集合：RowData 反序列化/VectorBatch 填充/Omni 主表 metadata 构造。
// 所有 SVE 依赖已移至 .cpp 实现文件。
class VectorBatchRestoreUtil {
public:
    static void populateVectorBatchFromRow(
        VectorBatch* batch,
        const std::vector<omniruntime::type::DataTypeId>& columnTypes,
        ::BinaryRowData* row,
        int rowPos);

    static int64_t appendRowToVectorBatch(
        VbBatchState& vbState,
        const std::vector<int8_t>& valueBytes,
        const std::vector<omniruntime::type::DataTypeId>& columnTypes,
        int batchSize);

    static StateMetaInfoSnapshot buildOmniMainMetaInfo(
        const StateMetaInfoSnapshot& flinkMetaInfo, TypeSerializer* valueSerializer);

    // SVE 隔离桥接：避免头文件中直接依赖 table/data/util/VectorBatchUtil
    static VectorBatch* sliceVectorBatch(VectorBatch* batch, int startRow, int endRow);

    // SVE 隔离桥接：避免头文件中直接依赖 table/utils/VectorBatchSerializationUtils
    static int32_t calculateVbSerializableSize(VectorBatch* batch);
    static SerializedVbBatchInfo serializeVbBatch(VectorBatch* batch, int32_t bufferSize, uint8_t* buffer);

private:
    VectorBatchRestoreUtil() = delete;
};

} // namespace omnistream
