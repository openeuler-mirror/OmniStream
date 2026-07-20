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
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "common.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/utils/ByteView.h"
#include "table/data/RowData.h"
#include "table/data/vectorbatch/VectorBatchStorageInfo.h"

namespace omnistream {

// VectorBatch 保存流程使用的无状态工具函数集合。
//
// 本类仅提供通用字节处理与序列化辅助能力，不依赖具体算子语义，
// 不访问 Adaptor 内部状态，也不根据 state name 推断算子类型。
class VectorBatchSaveTools {
public:
    // 判定状态名是否符合 VectorBatch 侧表命名约定：以 "vb" 结尾的状态名视为侧表。
    static bool isVbStateName(const std::string& stateName)
    {
        return stateName.size() >= 2 && stateName.substr(stateName.size() - 2) == "vb";
    }

    // 从 ByteView 中解析 big-endian uint64_t comboId，调用方需保证 value 至少包含 8 字节。
    // 适用于主状态 value 直接保存 comboId 的 parseVectorBatchReference() 实现。
    static omnistream::ComboId parseComboId(ByteView value)
    {
        if (value.data() == nullptr || value.size() < sizeof(int64_t)) {
            INFO_RELEASE("Error:VectorBatchSaveTools::parseComboId invalid comboId bytes, size=" << value.size());
            throw std::runtime_error(
                "VectorBatchSaveTools: comboId value must contain at least 8 bytes, actual size=" +
                std::to_string(value.size()));
        }
        DataInputDeserializer input(value.data(), static_cast<int>(value.size()), 0);
        return input.readLong();
    }

    // 使用指定 serializer 将 RowData 同步序列化为 std::vector<int8_t>。
    // 适用于 encodeFlinkLogicalValue() 中直接使用单一 RowData serializer 的场景。
    // serializer 需非空，并且能够处理 RowData* 类型输入。
    static std::vector<int8_t> serializeRowData(RowData* row, TypeSerializer* serializer)
    {
        if (row == nullptr || serializer == nullptr) {
            return {};
        }
        // 预估容量：每列约 8 字节 header，加上实际字段内容。
        DataOutputSerializer target(256);
        serializer->serialize(static_cast<void*>(row), target);

        std::vector<int8_t> result;
        result.resize(target.getPosition());
        if (target.getPosition() > 0) {
            std::memcpy(result.data(), target.getData(), target.getPosition());
        }
        return result;
    }

    // 跳过 VectorBatch 侧表 value 的首字节 StreamElementTag。
    // 返回的 ByteView 指向原始 value 中 tag 之后的 payload 区域。
    static ByteView skipVbValueTag(ByteView vbValue)
    {
        if (vbValue.size() <= sizeof(int8_t)) {
            return ByteView();
        }
        return ByteView(vbValue.data() + sizeof(int8_t), vbValue.size() - sizeof(int8_t));
    }
};

} // namespace omnistream
