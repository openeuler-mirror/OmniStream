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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <xxhash.h>
#include <nlohmann/json.hpp>

#include "common.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeinfo/BasicTypeInfo.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/SerializerJsonInfo.h"
#include "core/typeutils/TupleSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "core/utils/ByteView.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "table/data/TimestampData.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/ComboIdUtil.h"
#include "table/data/RowData.h"
#include "table/data/RowKind.h"
#include "table/types/logical/LogicalType.h"
#include "table/types/logical/RowType.h"
#include "table/types/logical/TimestampWithLocalTimeZoneType.h"
#include "table/types/logical/TimestampWithoutTimeZoneType.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/RowDataSerializer.h"

namespace omnistream {

// StreamingJoin SP 格式互通工具类保持 header-only，便于 adaptor/factory
// 在不同编译单元直接复用同一套判定与序列化逻辑。
// 所有成员函数均为 inline，避免头文件实现被多个 .cpp 引入时产生重复定义。
class StreamingJoinSavepointUtil {
public:
    // Flink/Omni Join tuple 中已解析出的计数、关联数和 VB 行引用信息。
    struct ParsedJoinValue {
        // 当前 Join tuple 对应的匹配计数。
        int32_t count = 0;
        // left outer join 保留侧使用的关联记录数，inner join 时保持默认值。
        int32_t numAssociations = 0;
        // Omni VB 侧表中完整 RowData 的 comboId 引用。
        omnistream::ComboId comboId = 0;
        // 标记该 value 是否采用 left outer join 的三字段布局。
        bool outerJoinState = false;
    };

    // Heap StateMap 快照会把同一个 keyed key 下的多个 MapState entry 聚合到一个 value 中。
    // 该结构保留聚合 value 中的 XXH128 map key，并与对应的 Join tuple 一起交给 adaptor 展开。
    struct ParsedOmniMapEntry {
        // 聚合 MapState entry 的 XXH128 map key。
        XXH128_hash_t mapKey{};
        // 该 map key 对应的 Join tuple value。
        ParsedJoinValue value;
    };

    // Flink logical MapState key 拆分后的 keyed key 前缀和 RowData 序列化字节。
    struct FlinkMapKeyParts {
        // 不包含 RowData 后缀的 keyed key 和 namespace 序列化前缀。
        ByteView keyPrefix;
        // 从 Flink key 尾部识别出的完整 RowData 序列化字节。
        std::vector<int8_t> rowDataBytes;
    };

    // StreamingJoin 左侧 Omni 主状态的逻辑名称。
    static constexpr const char* LEFT_STATE_NAME = "left-records";
    // StreamingJoin 右侧 Omni 主状态的逻辑名称。
    static constexpr const char* RIGHT_STATE_NAME = "right-records";
    // 算子描述中 inner join 的类型名称。
    static constexpr const char* INNER_JOIN_TYPE = "InnerJoin";
    // 算子描述中无唯一键输入规格的名称。
    static constexpr const char* NO_UNIQUE_KEY = "NoUniqueKey";
    // 算子描述中 left outer join 的类型名称。
    static constexpr const char* LEFT_OUTER_JOIN_TYPE = "LeftOuterJoin";

    // Join 类型在算子描述中的字段名。
    static constexpr const char* JOIN_TYPE_FIELD = "joinType";
    // 左侧输入规格在算子描述中的字段名。
    static constexpr const char* LEFT_INPUT_SPEC_FIELD = "leftInputSpec";
    // 右侧输入规格在算子描述中的字段名。
    static constexpr const char* RIGHT_INPUT_SPEC_FIELD = "rightInputSpec";
    // 左侧唯一键配置在算子描述中的字段名。
    static constexpr const char* LEFT_UNIQUE_KEYS_FIELD = "leftUniqueKeys";
    // 右侧唯一键配置在算子描述中的字段名。
    static constexpr const char* RIGHT_UNIQUE_KEYS_FIELD = "rightUniqueKeys";
    // 左侧状态名称在算子描述中的字段名。
    static constexpr const char* LEFT_STATE_NAME_FIELD = "leftStateName";
    // 右侧状态名称在算子描述中的字段名。
    static constexpr const char* RIGHT_STATE_NAME_FIELD = "rightStateName";
    // 非等值 Join 条件在算子描述中的字段名。
    static constexpr const char* NON_EQUI_CONDITION_FIELD = "nonEquiCondition";
    // 左侧输入字段类型数组在算子描述中的字段名。
    static constexpr const char* LEFT_INPUT_TYPES_FIELD = "leftInputTypes";
    // 右侧输入字段类型数组在算子描述中的字段名。
    static constexpr const char* RIGHT_INPUT_TYPES_FIELD = "rightInputTypes";
    // 左侧 Join key 索引数组在算子描述中的字段名。
    static constexpr const char* LEFT_JOIN_KEY_FIELD = "leftJoinKey";
    // 右侧 Join key 索引数组在算子描述中的字段名。
    static constexpr const char* RIGHT_JOIN_KEY_FIELD = "rightJoinKey";
    // null 过滤配置在算子描述中的字段名。
    static constexpr const char* FILTER_NULLS_FIELD = "filterNulls";
    // XXH128 hash 序列化结果占用的字节数。
    static constexpr int XXH128_SERIALIZED_BYTES = 16;

    // 工具类仅提供静态方法，不允许创建实例。
    StreamingJoinSavepointUtil() = delete;

    // 根据完整算子描述解析 StreamingJoin 兼容格式适配器类型。
    // 返回 FlinkSavepointAdaptorType::None 表示当前算子描述超出已支持范围。
    static FlinkSavepointAdaptorType getAdaptorType(const nlohmann::json& description);

    // 按固定校验顺序构造当前 StreamingJoin 不支持兼容格式互通的具体原因。
    static std::string buildUnsupportedReason(const nlohmann::json& description);

    // 将 LogicalType 对象转换为公共 VectorBatch 恢复流程使用的数据类型标识。
    static std::vector<omniruntime::type::DataTypeId> convertToDataTypes(const std::vector<LogicalType*>& logicalTypes);

    // 根据 key-group prefix 和 keyed RowData 长度精确定位 Flink logical MapState key，并拆分 keyed key 前缀和 RowData
    // 字节。
    static FlinkMapKeyParts splitFlinkMapKey(
        const std::vector<int8_t>& flinkKey, const std::vector<LogicalType*>& inputTypes, int keyGroupPrefixBytes);

    // 解析 Omni StreamingJoin 主状态 value，提取 count、numAssociations 和 comboId。
    static ParsedJoinValue parseOmniJoinValue(ByteView rawValue);

    // 展开 Heap StateMap 聚合保存的多个 Omni MapState entry，并保留各 entry 的 XXH128 map key。
    static std::vector<ParsedOmniMapEntry> parseOmniMapStateEntries(ByteView rawValue, bool outerJoinState);

    // 将 keyed key 前缀与 VectorBatch 行计算得到的 XXH128 hash 组合为 Omni MapState key。
    static std::vector<int8_t> serializeOmniMapKey(ByteView keyPrefix, XXH128_hash_t rowHash);

    // 将 Flink MapState value 与恢复阶段分配的 comboId 组合为 Omni Join tuple。
    static std::vector<int8_t> serializeOmniJoinValue(const ParsedJoinValue& joinValue, omnistream::ComboId comboId);

    // 解析 Flink StreamingJoin MapState value，按 inner/left outer 状态布局读取计数字段。
    static ParsedJoinValue parseFlinkJoinValue(ByteView rawValue, bool outerJoinState);

    // 将 Omni keyed key 前缀与完整 RowData 组合为 Flink StreamingJoin logical MapState key。
    static std::vector<int8_t> serializeFlinkMapKey(ByteView omniKey, RowData& row);

    // 将 Omni Join tuple 转换为 Flink StreamingJoin MapState value。
    static std::vector<int8_t> serializeFlinkMapValue(const ParsedJoinValue& joinValue, bool outerJoinState);

    // 基于 Omni 源状态元数据创建 Flink StreamingJoin logical MapState 元数据。
    static std::shared_ptr<StateMetaInfoSnapshot> createFlinkMapStateSnapshot(
        const std::string& flinkStateName,
        const StateMetaInfoSnapshot& sourceMetaInfo,
        const std::vector<std::string>& inputTypeNames,
        bool outerJoinState);

    // 将类型名称列表格式化为日志使用的紧凑字符串。
    static std::string joinStrings(const std::vector<std::string>& values);

private:
    // 将 DataOutputSerializer 中的有效字节复制到独立缓冲区。
    // 返回的 vector 在临时 serializer 销毁后仍可安全使用。
    static std::vector<int8_t> copySerializerBuffer(DataOutputSerializer& serializer);

    // 校验序列化变长字段是否完整位于 RowData payload 范围内。
    // 使用减法比较可避免不可信状态字节中的 offset 与 len 相加产生溢出。
    static bool isValidStringFieldRange(int offset, int len, int rowBytesLen);

    // 防御性读取算子描述中的字符串字段。
    // 字段缺失或 JSON 类型不是字符串时统一返回空字符串，由上层生成明确的不支持原因。
    static std::string getStringField(const nlohmann::json& description, const std::string& fieldName);

    // 判断指定 inputSpec 字段是否明确声明为 NoUniqueKey。
    static bool isNoUniqueKeySpec(const nlohmann::json& description, const std::string& fieldName);

    // 判断指定 uniqueKeys 字段中是否包含任意有效唯一键定义。
    static bool hasUniqueKeys(const nlohmann::json& description, const std::string& fieldName);

    // 校验左右主状态名称是否缺省/为空，或符合 Flink StreamingJoin 默认约定名称。
    static bool areStateNamesEmptyOrDefault(const nlohmann::json& description);

    // 校验 Join key 索引合法，并确认对应输入字段均为 BIGINT 类型。
    static bool hasSupportedJoinKeys(
        const nlohmann::json& description, const std::string& keyField, const std::string& inputTypeField);

    // 判断单个 Flink 输入逻辑类型是否同时受 RowData restore、VectorBatch 构造和行级 XXH128 hash 支持。
    // 只有三条路径能力的交集才能进入 StreamingJoin adaptor，避免在 payload 转换阶段才因类型不支持而失败。
    static bool isSupportedInputType(const std::string& typeName);

    // 校验指定输入类型数组中的所有字段都位于 StreamingJoin 当前端到端支持范围内。
    static bool isAllOfSupportedInputTypes(const nlohmann::json& description, const std::string& inputTypeField);

    // 校验 filterNulls 字段存在，并且为布尔值或非空布尔数组。
    static bool hasFilterNullsContract(const nlohmann::json& description);

    // 校验 inner 和 left outer StreamingJoin 共同使用的 NoUniqueKey 元数据约束。
    // Join 类型映射保留在该方法之外，使调用方能够直接复用已经选定的 adaptorType。
    static bool hasSupportedNoUniqueKeyJoinContract(const nlohmann::json& description);

    // 按大端序读取四字节整数，用于解析 Flink serializer 写出的长度和偏移字段。
    static int32_t readBigEndianInt(const int8_t* data);

    // 按小端序读取四字节整数，避免依赖宿主机字节序。
    static int32_t readLittleEndianInt(const int8_t* data);

    // 判断 OmniRuntime 数据类型是否使用 Flink BinaryRowData 的字符串变长字段布局。
    static bool isStringType(const LogicalType* logicalType);

    // 对候选 Flink RowData payload 做轻量结构校验，避免误把 keyed key 前缀识别为 RowData。
    static bool isValidFlinkSerializedRowPayload(
        const int8_t* rowBytes, int rowBytesLen, const std::vector<LogicalType*>& inputTypes);
};

inline std::vector<int8_t> StreamingJoinSavepointUtil::copySerializerBuffer(DataOutputSerializer& serializer)
{
    std::vector<int8_t> result(serializer.getPosition());
    if (!result.empty()) {
        std::memcpy(result.data(), serializer.getData(), result.size());
    }
    return result;
}

inline bool StreamingJoinSavepointUtil::isValidStringFieldRange(int offset, int len, int rowBytesLen)
{
    return offset >= 0 && len >= 0 && offset <= rowBytesLen && len <= rowBytesLen - offset;
}

inline std::string StreamingJoinSavepointUtil::getStringField(
    const nlohmann::json& description, const std::string& fieldName)
{
    if (!description.contains(fieldName) || !description[fieldName].is_string()) {
        return "";
    }
    return description[fieldName].get<std::string>();
}

inline FlinkSavepointAdaptorType StreamingJoinSavepointUtil::getAdaptorType(const nlohmann::json& description)
{
    const std::string joinType = getStringField(description, JOIN_TYPE_FIELD);
    if (!hasSupportedNoUniqueKeyJoinContract(description)) {
        return FlinkSavepointAdaptorType::None;
    }
    if (joinType == INNER_JOIN_TYPE) {
        return FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor;
    }
    if (joinType == LEFT_OUTER_JOIN_TYPE) {
        return FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor;
    }
    return FlinkSavepointAdaptorType::None;
}

inline std::string StreamingJoinSavepointUtil::buildUnsupportedReason(const nlohmann::json& description)
{
    const std::string joinType = getStringField(description, JOIN_TYPE_FIELD);
    if (joinType != INNER_JOIN_TYPE && joinType != LEFT_OUTER_JOIN_TYPE) {
        return "StreamingJoin compatible savepoint only supports InnerJoin and LeftOuterJoin";
    }
    if (!isNoUniqueKeySpec(description, LEFT_INPUT_SPEC_FIELD) ||
        !isNoUniqueKeySpec(description, RIGHT_INPUT_SPEC_FIELD)) {
        return "StreamingJoin compatible savepoint only supports NoUniqueKey on both sides";
    }
    if (hasUniqueKeys(description, LEFT_UNIQUE_KEYS_FIELD) || hasUniqueKeys(description, RIGHT_UNIQUE_KEYS_FIELD)) {
        return "StreamingJoin compatible savepoint does not support unique-key join state";
    }
    if (!areStateNamesEmptyOrDefault(description)) {
        return "StreamingJoin compatible savepoint state names do not match left-records/right-records";
    }
    if (!isAllOfSupportedInputTypes(description, LEFT_INPUT_TYPES_FIELD) ||
        !isAllOfSupportedInputTypes(description, RIGHT_INPUT_TYPES_FIELD)) {
        return "StreamingJoin compatible savepoint only supports BIGINT, VARCHAR/STRING and TIMESTAMP input fields "
               "with precision <= 3";
    }
    if (!hasSupportedJoinKeys(description, LEFT_JOIN_KEY_FIELD, LEFT_INPUT_TYPES_FIELD) ||
        !hasSupportedJoinKeys(description, RIGHT_JOIN_KEY_FIELD, RIGHT_INPUT_TYPES_FIELD)) {
        return "StreamingJoin compatible savepoint requires BIGINT join keys";
    }
    if (!hasFilterNullsContract(description)) {
        return "StreamingJoin compatible savepoint requires filterNulls metadata";
    }
    return "StreamingJoin compatible savepoint adaptor is outside the current supported boundary";
}

inline std::vector<omniruntime::type::DataTypeId> StreamingJoinSavepointUtil::convertToDataTypes(
    const std::vector<LogicalType*>& logicalTypes)
{
    std::vector<omniruntime::type::DataTypeId> result;
    result.reserve(logicalTypes.size());
    for (const auto* logicalType : logicalTypes) {
        result.push_back(
            logicalType == nullptr ? omniruntime::type::DataTypeId::OMNI_INVALID
                                   : static_cast<omniruntime::type::DataTypeId>(logicalType->getTypeId()));
    }
    return result;
}

inline StreamingJoinSavepointUtil::FlinkMapKeyParts StreamingJoinSavepointUtil::splitFlinkMapKey(
    const std::vector<int8_t>& flinkKey, const std::vector<LogicalType*>& inputTypes, int keyGroupPrefixBytes)
{
    /*
     * Flink StreamingJoin 的 logical MapState key 布局是：
     *   [key-group][current key][namespace][BinaryRowDataSerializer(row)]
     * 当前算子的 keyed key 由 BinaryRowDataSerializer 写出，开头四字节明确记录 current key 的 RowData 长度；
     * source state 已校验为 VoidNamespace，其 serializer 会在 current key 后写入一字节 0 marker。这里从后端
     * 取得 key-group prefix 长度后按 serializer 边界顺序解析，避免 VARCHAR 内容中的任意四字节被误识别为
     * logical MapState RowData 长度。
     * 精确定位后只切分 bytes，不在这里重建 RowData 对象，VB restore writer 会继续使用统一反序列化路径。
     */
    constexpr int lengthFieldBytes = sizeof(int32_t);
    constexpr int namespaceBytes = sizeof(uint8_t);
    constexpr uint8_t voidNamespaceMarker = 0;
    const int rowArity = static_cast<int>(inputTypes.size());
    if (rowArity <= 0 || keyGroupPrefixBytes <= 0 ||
        flinkKey.size() < static_cast<size_t>(keyGroupPrefixBytes + lengthFieldBytes * 2 + namespaceBytes)) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::splitFlinkMapKey ->"
            << " keySize=" << flinkKey.size() << ", rowArity=" << rowArity
            << ", keyGroupPrefixBytes=" << keyGroupPrefixBytes << ", namespaceBytes=" << namespaceBytes
            << ", lengthFieldBytes=" << lengthFieldBytes);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::splitFlinkMapKey invalid Flink map key, keySize=" +
            std::to_string(flinkKey.size()) + ", rowArity=" + std::to_string(rowArity) +
            ", keyGroupPrefixBytes=" + std::to_string(keyGroupPrefixBytes));
    }

    const size_t currentKeyLengthOffset = static_cast<size_t>(keyGroupPrefixBytes);
    const size_t currentKeyBytesOffset = currentKeyLengthOffset + lengthFieldBytes;
    const int32_t currentKeyBytesLen = readBigEndianInt(flinkKey.data() + currentKeyLengthOffset);
    if (currentKeyBytesLen <= 0 || static_cast<size_t>(currentKeyBytesLen) >
                                       flinkKey.size() - currentKeyBytesOffset - namespaceBytes - lengthFieldBytes) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::splitFlinkMapKey ->"
            << " keySize=" << flinkKey.size() << ", rowArity=" << rowArity
            << ", keyGroupPrefixBytes=" << keyGroupPrefixBytes << ", currentKeyBytesLen=" << currentKeyBytesLen);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::splitFlinkMapKey invalid serialized current key length=" +
            std::to_string(currentKeyBytesLen));
    }

    const size_t namespaceOffset = currentKeyBytesOffset + static_cast<size_t>(currentKeyBytesLen);
    const uint8_t namespaceMarker = static_cast<uint8_t>(flinkKey[namespaceOffset]);
    if (namespaceMarker != voidNamespaceMarker) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::splitFlinkMapKey ->"
            << " keySize=" << flinkKey.size() << ", rowArity=" << rowArity
            << ", keyGroupPrefixBytes=" << keyGroupPrefixBytes << ", currentKeyBytesLen=" << currentKeyBytesLen
            << ", namespaceOffset=" << namespaceOffset << ", namespaceMarker=" << static_cast<int>(namespaceMarker));
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::splitFlinkMapKey invalid VoidNamespace marker=" +
            std::to_string(namespaceMarker));
    }

    const size_t rowLengthOffset = namespaceOffset + namespaceBytes;
    const size_t rowBytesOffset = rowLengthOffset + lengthFieldBytes;
    const int32_t rowBytesLen = readBigEndianInt(flinkKey.data() + rowLengthOffset);
    const int minRowBytesLen = BinaryRowData::calculateFixPartSizeInBytes(rowArity);
    if (rowBytesLen < minRowBytesLen || static_cast<size_t>(rowBytesLen) != flinkKey.size() - rowBytesOffset ||
        !isValidFlinkSerializedRowPayload(flinkKey.data() + rowBytesOffset, rowBytesLen, inputTypes)) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::splitFlinkMapKey ->"
            << " keySize=" << flinkKey.size() << ", rowArity=" << rowArity
            << ", keyGroupPrefixBytes=" << keyGroupPrefixBytes << ", currentKeyBytesLen=" << currentKeyBytesLen
            << ", rowLengthOffset=" << rowLengthOffset << ", rowBytesLen=" << rowBytesLen
            << ", minimumRowBytesLen=" << minRowBytesLen);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::splitFlinkMapKey invalid Flink RowData suffix, keySize=" +
            std::to_string(flinkKey.size()) + ", rowArity=" + std::to_string(rowArity));
    }

    FlinkMapKeyParts parts;
    parts.keyPrefix = ByteView::fromBuffer(flinkKey.data(), rowLengthOffset);
    parts.rowDataBytes.assign(flinkKey.begin() + rowLengthOffset, flinkKey.end());
    return parts;
}

inline StreamingJoinSavepointUtil::ParsedJoinValue StreamingJoinSavepointUtil::parseOmniJoinValue(ByteView rawValue)
{
    if (rawValue.size() < 1 + sizeof(int32_t) + sizeof(int64_t)) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::parseOmniJoinValue ->"
            << " valueSize=" << rawValue.size() << ", minimumValueSize=" << (1 + sizeof(int32_t) + sizeof(int64_t)));
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseOmniJoinValue invalid Omni join value length=" +
            std::to_string(rawValue.size()));
    }

    DataInputDeserializer input(rawValue.data(), static_cast<int>(rawValue.size()), 0);
    bool isNull = input.readBoolean();
    if (isNull) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::parseOmniJoinValue -> valueSize=" << rawValue.size()
                                                                                  << ", isNull=" << isNull);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseOmniJoinValue null Omni join value is not supported");
    }

    ParsedJoinValue value;
    int remaining = input.Available();
    if (remaining == static_cast<int>(sizeof(int32_t) + sizeof(int64_t))) {
        value.count = input.readInt();
        value.comboId = ComboIdUtil::readComboId(input);
        value.outerJoinState = false;
        return value;
    }
    if (remaining == static_cast<int>(sizeof(int32_t) + sizeof(int32_t) + sizeof(int64_t))) {
        value.count = input.readInt();
        value.numAssociations = input.readInt();
        value.comboId = ComboIdUtil::readComboId(input);
        value.outerJoinState = true;
        return value;
    }
    INFO_RELEASE(
        "Error: StreamingJoinSavepointUtil::parseOmniJoinValue -> valueSize=" << rawValue.size()
                                                                              << ", remainingBytes=" << remaining);
    throw std::runtime_error(
        "StreamingJoinSavepointUtil::parseOmniJoinValue unsupported Omni join value payload length=" +
        std::to_string(remaining));
}

inline std::vector<StreamingJoinSavepointUtil::ParsedOmniMapEntry> StreamingJoinSavepointUtil::parseOmniMapStateEntries(
    ByteView rawValue, bool outerJoinState)
{
    if (rawValue.size() < sizeof(int32_t)) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::parseOmniMapStateEntries -> valueSize="
            << rawValue.size() << ", minimumValueSize=" << sizeof(int32_t) << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseOmniMapStateEntries invalid aggregated Omni map value length=" +
            std::to_string(rawValue.size()));
    }

    DataInputDeserializer input(rawValue.data(), static_cast<int>(rawValue.size()), 0);
    int32_t entryCount = input.readInt();
    if (entryCount < 0) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::parseOmniMapStateEntries -> entryCount="
            << entryCount << ", valueSize=" << rawValue.size() << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseOmniMapStateEntries invalid aggregated Omni map entry count=" +
            std::to_string(entryCount));
    }
    const int32_t minimumEntryBytes =
        XXH128_SERIALIZED_BYTES + 1 + sizeof(int32_t) + sizeof(int64_t) + (outerJoinState ? sizeof(int32_t) : 0);
    if (entryCount > 0 && entryCount > input.Available() / minimumEntryBytes) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::parseOmniMapStateEntries ->"
            << " entryCount=" << entryCount << ", remainingBytes=" << input.Available()
            << ", minimumEntryBytes=" << minimumEntryBytes << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseOmniMapStateEntries aggregated Omni map entry count exceeds payload, "
            "entryCount=" +
            std::to_string(entryCount) + ", remainingBytes=" + std::to_string(input.Available()));
    }

    std::vector<ParsedOmniMapEntry> entries;
    entries.reserve(static_cast<size_t>(entryCount));
    for (int32_t index = 0; index < entryCount; ++index) {
        std::unique_ptr<XXH128_hash_t> mapKey(
            static_cast<XXH128_hash_t*>(XxH128_hashSerializer::INSTANCE->deserialize(input)));
        if (mapKey == nullptr) {
            INFO_RELEASE(
                "Error: StreamingJoinSavepointUtil::parseOmniMapStateEntries ->"
                << " entryIndex=" << index << ", entryCount=" << entryCount << ", mapKey=null");
            throw std::runtime_error(
                "StreamingJoinSavepointUtil::parseOmniMapStateEntries failed to deserialize aggregated Omni map key");
        }

        if (input.readBoolean()) {
            INFO_RELEASE(
                "Error: StreamingJoinSavepointUtil::parseOmniMapStateEntries ->"
                << " entryIndex=" << index << ", entryCount=" << entryCount << ", joinValueNull=true");
            throw std::runtime_error(
                "StreamingJoinSavepointUtil::parseOmniMapStateEntries null aggregated Omni join value is not "
                "supported");
        }

        ParsedOmniMapEntry entry;
        entry.mapKey = *mapKey;
        entry.value.outerJoinState = outerJoinState;
        entry.value.count = input.readInt();
        if (outerJoinState) {
            entry.value.numAssociations = input.readInt();
        }
        entry.value.comboId = ComboIdUtil::readComboId(input);
        entries.push_back(entry);
    }

    if (input.Available() != 0) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::parseOmniMapStateEntries -> entryCount="
            << entryCount << ", trailingBytes=" << input.Available() << ", valueSize=" << rawValue.size());
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseOmniMapStateEntries trailing bytes in aggregated Omni map value=" +
            std::to_string(input.Available()));
    }

    return entries;
}

inline std::vector<int8_t> StreamingJoinSavepointUtil::serializeOmniMapKey(ByteView keyPrefix, XXH128_hash_t rowHash)
{
    /*
     * Omni StreamingJoin 主表 key 与 Flink logical key 共用同一个 keyed 前缀，
     * 但 map-key 部分不是完整 RowData，而是运行态从 VB 行计算出的 XXH128(row)。
     */
    DataOutputSerializer hashOutput;
    OutputBufferStatus hashOutputStatus;
    hashOutput.setBackendBuffer(&hashOutputStatus);
    XxH128_hashSerializer::INSTANCE->serialize(&rowHash, hashOutput);
    std::vector<int8_t> hashBytes = copySerializerBuffer(hashOutput);
    std::vector<int8_t> result;
    result.reserve(keyPrefix.size() + hashBytes.size());
    result.insert(
        result.end(),
        reinterpret_cast<const int8_t*>(keyPrefix.data()),
        reinterpret_cast<const int8_t*>(keyPrefix.data()) + keyPrefix.size());
    result.insert(result.end(), hashBytes.begin(), hashBytes.end());
    return result;
}

inline std::vector<int8_t> StreamingJoinSavepointUtil::serializeOmniJoinValue(
    const ParsedJoinValue& joinValue, omnistream::ComboId comboId)
{
    /*
     * Flink logical map value 只保存匹配计数；Omni 运行态还需要 comboId 指向 VB 侧表中的完整 RowData。
     * left outer join 的保留侧多一个 numAssociations 字段，字段顺序必须和 JoinTupleSerializer2 保持一致。
     */
    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    output.writeBoolean(false);
    output.writeInt(joinValue.count);
    if (joinValue.outerJoinState) {
        output.writeInt(joinValue.numAssociations);
    }
    ComboIdUtil::writeComboId(output, comboId);
    return copySerializerBuffer(output);
}

inline StreamingJoinSavepointUtil::ParsedJoinValue StreamingJoinSavepointUtil::parseFlinkJoinValue(
    ByteView rawValue, bool outerJoinState)
{
    if (rawValue.size() < 1 + sizeof(int32_t)) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::parseFlinkJoinValue -> valueSize="
            << rawValue.size() << ", minimumValueSize=" << (1 + sizeof(int32_t))
            << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseFlinkJoinValue invalid Flink join value length=" +
            std::to_string(rawValue.size()));
    }

    DataInputDeserializer input(rawValue.data(), static_cast<int>(rawValue.size()), 0);
    bool isNull = input.readBoolean();
    if (isNull) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::parseFlinkJoinValue ->"
            << " valueSize=" << rawValue.size() << ", isNull=" << isNull << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseFlinkJoinValue null Flink join value is not supported");
    }

    ParsedJoinValue value;
    value.outerJoinState = outerJoinState;
    int remaining = input.Available();
    if (!outerJoinState && remaining == static_cast<int>(sizeof(int32_t))) {
        value.count = input.readInt();
        return value;
    }
    if (outerJoinState && remaining == static_cast<int>(sizeof(int32_t) + sizeof(int32_t))) {
        value.count = input.readInt();
        value.numAssociations = input.readInt();
        return value;
    }
    INFO_RELEASE(
        "Error: StreamingJoinSavepointUtil::parseFlinkJoinValue -> valueSize="
        << rawValue.size() << ", remainingBytes=" << remaining << ", outerJoinState=" << outerJoinState);
    throw std::runtime_error(
        "StreamingJoinSavepointUtil::parseFlinkJoinValue unsupported Flink join value payload length=" +
        std::to_string(remaining) + ", outerJoinState=" + std::to_string(outerJoinState));
}

inline std::vector<int8_t> StreamingJoinSavepointUtil::serializeFlinkMapKey(ByteView omniKey, RowData& row)
{
    if (omniKey.size() <= XXH128_SERIALIZED_BYTES) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::serializeFlinkMapKey -> omniKeySize="
            << omniKey.size() << ", hashSize=" << XXH128_SERIALIZED_BYTES << ", rowArity=" << row.getArity());
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::serializeFlinkMapKey invalid Omni join key length=" +
            std::to_string(omniKey.size()));
    }

    BinaryRowDataSerializer rowSerializer(row.getArity());
    DataOutputSerializer rowOutput;
    OutputBufferStatus rowOutputStatus;
    rowOutput.setBackendBuffer(&rowOutputStatus);
    rowSerializer.serialize(static_cast<void*>(&row), rowOutput);
    std::vector<int8_t> rowBytes = copySerializerBuffer(rowOutput);

    const size_t keyPrefixLength = omniKey.size() - XXH128_SERIALIZED_BYTES;
    std::vector<int8_t> result;
    result.reserve(keyPrefixLength + rowBytes.size());
    result.insert(result.end(), omniKey.begin(), omniKey.begin() + keyPrefixLength);
    result.insert(result.end(), rowBytes.begin(), rowBytes.end());
    return result;
}

inline std::vector<int8_t> StreamingJoinSavepointUtil::serializeFlinkMapValue(
    const ParsedJoinValue& joinValue, bool outerJoinState)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    output.writeBoolean(false);
    output.writeInt(joinValue.count);
    if (outerJoinState) {
        output.writeInt(joinValue.numAssociations);
    }
    return copySerializerBuffer(output);
}

inline std::shared_ptr<StateMetaInfoSnapshot> StreamingJoinSavepointUtil::createFlinkMapStateSnapshot(
    const std::string& flinkStateName,
    const StateMetaInfoSnapshot& sourceMetaInfo,
    const std::vector<std::string>& inputTypeNames,
    bool outerJoinState)
{
    TypeSerializer* namespaceSerializer = sourceMetaInfo.getTypeSerializer(
        {StateMetaInfoSnapshot::COMMON_NAMESPACE_SERIALIZER_KEY, SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY});
    if (namespaceSerializer == nullptr) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::createFlinkMapStateSnapshot ->"
            << " stateName=" << flinkStateName << ", namespaceSerializer=null"
            << ", inputTypeCount=" << inputTypeNames.size() << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::createFlinkMapStateSnapshot missing namespace serializer for state=" +
            flinkStateName);
    }
    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        INFO_RELEASE(
            "Error: StreamingJoinSavepointUtil::createFlinkMapStateSnapshot ->"
            << " stateName=" << flinkStateName << ", namespaceBackendId=" << namespaceSerializer->getBackendId()
            << ", inputTypeCount=" << inputTypeNames.size());
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::createFlinkMapStateSnapshot StreamingJoin requires VoidNamespace for state=" +
            flinkStateName);
    }

    BasicTypeInfo firstIntType(TYPE_NAME_INT_SERIALIZER);
    BasicTypeInfo secondIntType(TYPE_NAME_INT_SERIALIZER);
    std::vector<TypeInformation*> tupleFieldTypes{&firstIntType, &secondIntType};
    TypeSerializer* valueSerializer = outerJoinState
                                          ? static_cast<TypeSerializer*>(new Tuple2Serializer(tupleFieldTypes))
                                          : static_cast<TypeSerializer*>(new IntSerializer());
    auto* stateSerializer =
        new MapSerializer(new RowDataSerializer(new RowType(true, inputTypeNames)), valueSerializer);
    RegisteredKeyValueStateBackendMetaInfo convertedMetaInfo(
        StateDescriptor::Type::MAP, flinkStateName, namespaceSerializer, stateSerializer);

    return convertedMetaInfo.snapshot();
}

inline std::string StreamingJoinSavepointUtil::joinStrings(const std::vector<std::string>& values)
{
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            oss << ",";
        }
        oss << values[i];
    }
    oss << "]";
    return oss.str();
}

inline bool StreamingJoinSavepointUtil::isNoUniqueKeySpec(
    const nlohmann::json& description, const std::string& fieldName)
{
    const std::string value = getStringField(description, fieldName);
    return value == NO_UNIQUE_KEY;
}

inline bool StreamingJoinSavepointUtil::hasUniqueKeys(const nlohmann::json& description, const std::string& fieldName)
{
    if (!description.contains(fieldName) || !description[fieldName].is_array()) {
        return false;
    }
    for (const auto& key : description[fieldName]) {
        if ((key.is_array() && !key.empty()) || key.is_number_integer()) {
            return true;
        }
    }
    return false;
}

inline bool StreamingJoinSavepointUtil::areStateNamesEmptyOrDefault(const nlohmann::json& description)
{
    const std::string leftStateName = getStringField(description, LEFT_STATE_NAME_FIELD);
    const std::string rightStateName = getStringField(description, RIGHT_STATE_NAME_FIELD);
    return (leftStateName.empty() || leftStateName == LEFT_STATE_NAME) &&
           (rightStateName.empty() || rightStateName == RIGHT_STATE_NAME);
}

inline bool StreamingJoinSavepointUtil::hasSupportedJoinKeys(
    const nlohmann::json& description, const std::string& keyField, const std::string& inputTypeField)
{
    if (!description.contains(keyField) || !description[keyField].is_array() || description[keyField].empty() ||
        !description.contains(inputTypeField) || !description[inputTypeField].is_array() ||
        description[inputTypeField].empty()) {
        return false;
    }
    const auto& inputTypes = description[inputTypeField];
    if (!std::all_of(inputTypes.begin(), inputTypes.end(), [](const nlohmann::json& type) {
            return type.is_string() && !type.get<std::string>().empty();
        })) {
        return false;
    }
    for (const auto& key : description[keyField]) {
        if (!key.is_number_integer()) {
            return false;
        }
        const auto index = key.get<int>();
        if (index < 0 || index >= static_cast<int>(inputTypes.size()) || !inputTypes[index].is_string()) {
            return false;
        }
        const std::string typeName = inputTypes[index].get<std::string>();
        if (typeName.rfind("BIGINT", 0) != 0) {
            return false;
        }
    }
    return true;
}

inline bool StreamingJoinSavepointUtil::isSupportedInputType(const std::string& typeName)
{
    try {
        auto logicalTypeDeleter = [](LogicalType* logicalType) {
            if (!LogicalType::isSharedLogicalType(logicalType)) {
                delete logicalType;
            }
        };
        std::unique_ptr<LogicalType, decltype(logicalTypeDeleter)> logicalType(
            LogicalType::flinkTypeToOmniType(typeName), logicalTypeDeleter);
        const auto typeId = static_cast<omniruntime::type::DataTypeId>(logicalType->getTypeId());
        switch (typeId) {
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: return true;
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                auto* timestampType = dynamic_cast<TimestampWithoutTimeZoneType*>(logicalType.get());
                return timestampType != nullptr && TimestampData::isCompact(timestampType->getPrecision());
            }
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                auto* timestampType = dynamic_cast<TimestampWithLocalTimeZoneType*>(logicalType.get());
                return timestampType != nullptr && TimestampData::isCompact(timestampType->getPrecision());
            }
            default: return false;
        }
    } catch (const std::exception&) {
        // adaptor 匹配阶段不能让格式异常的类型字符串中断算子创建，统一按不支持处理并由 reason 返回诊断。
        return false;
    }
}

inline bool StreamingJoinSavepointUtil::isAllOfSupportedInputTypes(
    const nlohmann::json& description, const std::string& inputTypeField)
{
    if (!description.contains(inputTypeField) || !description[inputTypeField].is_array() ||
        description[inputTypeField].empty()) {
        return false;
    }
    const auto& inputTypes = description[inputTypeField];
    return std::all_of(inputTypes.begin(), inputTypes.end(), [](const nlohmann::json& type) {
        return type.is_string() && StreamingJoinSavepointUtil::isSupportedInputType(type.get<std::string>());
    });
}

inline bool StreamingJoinSavepointUtil::hasFilterNullsContract(const nlohmann::json& description)
{
    if (!description.contains(FILTER_NULLS_FIELD)) {
        return false;
    }
    const auto& filterNulls = description[FILTER_NULLS_FIELD];
    if (filterNulls.is_boolean()) {
        return true;
    }
    if (!filterNulls.is_array() || filterNulls.empty()) {
        return false;
    }
    return std::all_of(
        filterNulls.begin(), filterNulls.end(), [](const nlohmann::json& value) { return value.is_boolean(); });
}

inline bool StreamingJoinSavepointUtil::hasSupportedNoUniqueKeyJoinContract(const nlohmann::json& description)
{
    return isNoUniqueKeySpec(description, LEFT_INPUT_SPEC_FIELD) &&
           isNoUniqueKeySpec(description, RIGHT_INPUT_SPEC_FIELD) &&
           !hasUniqueKeys(description, LEFT_UNIQUE_KEYS_FIELD) &&
           !hasUniqueKeys(description, RIGHT_UNIQUE_KEYS_FIELD) && areStateNamesEmptyOrDefault(description) &&
           isAllOfSupportedInputTypes(description, LEFT_INPUT_TYPES_FIELD) &&
           isAllOfSupportedInputTypes(description, RIGHT_INPUT_TYPES_FIELD) &&
           hasSupportedJoinKeys(description, LEFT_JOIN_KEY_FIELD, LEFT_INPUT_TYPES_FIELD) &&
           hasSupportedJoinKeys(description, RIGHT_JOIN_KEY_FIELD, RIGHT_INPUT_TYPES_FIELD) &&
           hasFilterNullsContract(description);
}

inline int32_t StreamingJoinSavepointUtil::readBigEndianInt(const int8_t* data)
{
    const uint32_t result = (static_cast<uint32_t>(static_cast<uint8_t>(data[0])) << 24) |
                            (static_cast<uint32_t>(static_cast<uint8_t>(data[1])) << 16) |
                            (static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 8) |
                            static_cast<uint32_t>(static_cast<uint8_t>(data[3]));
    return static_cast<int32_t>(result);
}

inline int32_t StreamingJoinSavepointUtil::readLittleEndianInt(const int8_t* data)
{
    const uint32_t result = (static_cast<uint32_t>(static_cast<uint8_t>(data[3])) << 24) |
                            (static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 16) |
                            (static_cast<uint32_t>(static_cast<uint8_t>(data[1])) << 8) |
                            static_cast<uint32_t>(static_cast<uint8_t>(data[0]));
    return static_cast<int32_t>(result);
}

inline bool StreamingJoinSavepointUtil::isStringType(const LogicalType* logicalType)
{
    if (logicalType == nullptr) {
        return false;
    }
    const auto typeId = static_cast<omniruntime::type::DataTypeId>(logicalType->getTypeId());
    return typeId == omniruntime::type::DataTypeId::OMNI_CHAR || typeId == omniruntime::type::DataTypeId::OMNI_VARCHAR;
}

inline bool StreamingJoinSavepointUtil::isValidFlinkSerializedRowPayload(
    const int8_t* rowBytes, int rowBytesLen, const std::vector<LogicalType*>& inputTypes)
{
    /*
     * 这里做轻量校验，目标是避免把 key 前缀中的任意 int 误判为 RowData length。
     * 详细字段反序列化继续交给 BinaryRowDataSerializer；本校验只确认 row kind、固定区长度和
     * varchar offset/length 没有明显越界。
     */
    const int rowArity = static_cast<int>(inputTypes.size());
    if (rowArity <= 0 || rowBytesLen < BinaryRowData::calculateFixPartSizeInBytes(rowArity)) {
        return false;
    }
    uint8_t rowKind = static_cast<uint8_t>(rowBytes[0]);
    if (rowKind > static_cast<uint8_t>(RowKind::DELETE)) {
        return false;
    }

    const int bitSetWidth = BinaryRowData::calculateBitSetWidthInBytes(rowArity);
    for (int colIndex = 0; colIndex < rowArity; ++colIndex) {
        int fieldOffset = bitSetWidth + (colIndex << 3);
        if (fieldOffset < 0 || fieldOffset + static_cast<int>(sizeof(int64_t)) > rowBytesLen) {
            return false;
        }
        if (!isStringType(inputTypes[colIndex])) {
            continue;
        }
        int nullBitIndex = colIndex + BinaryRowData::HEADER_SIZE_IN_BITS;
        int nullByteIndex = nullBitIndex >> 3;
        if (nullByteIndex >= 0 && nullByteIndex < rowBytesLen &&
            (static_cast<uint8_t>(rowBytes[nullByteIndex]) & (1 << (nullBitIndex & 0x07))) != 0) {
            continue;
        }
        const auto* fieldBytes = reinterpret_cast<const uint8_t*>(rowBytes + fieldOffset);
        if ((fieldBytes[0] & 0x80U) != 0) {
            int len = fieldBytes[0] & 0x7F;
            if (len <= 7) {
                continue;
            }
        }
        if ((fieldBytes[7] & 0x80U) != 0) {
            int len = fieldBytes[7] & 0x7F;
            if (len <= 7) {
                continue;
            }
        }
        int bigEndianOffset = readBigEndianInt(rowBytes + fieldOffset);
        int bigEndianLen = readBigEndianInt(rowBytes + fieldOffset + static_cast<int>(sizeof(int32_t)));
        int littleEndianLen = readLittleEndianInt(rowBytes + fieldOffset);
        int littleEndianOffset = readLittleEndianInt(rowBytes + fieldOffset + static_cast<int>(sizeof(int32_t)));
        if (!isValidStringFieldRange(bigEndianOffset, bigEndianLen, rowBytesLen) &&
            !isValidStringFieldRange(littleEndianOffset, littleEndianLen, rowBytesLen)) {
            return false;
        }
    }
    return true;
}

} // namespace omnistream
