/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "DynamicKafkaRecordSerializationSchema.h"
#include <algorithm>

DynamicKafkaRecordSerializationSchema::
DynamicKafkaRecordSerializationSchema(std::vector<std::string>& inputFields,
                                      std::vector<std::string>& inputTypes): inputFields_(inputFields),
    inputTypes_(inputTypes) {}

void DynamicKafkaRecordSerializationSchema::RowToJson(RowData* row)
{
    for (size_t i = 0; i < inputTypes_.size(); ++i) {
        if (inputTypes_[i] == "BIGINT") {
            j[inputFields_[i]] = *row->getLong(i);
        } else if (inputTypes_[i].find("TIMESTAMP") != std::string::npos) {
            oss.str("");
            oss.clear();
            timeBuffer[0] = '\0';
            auto millis = *row->getLong(i);
            int milliseconds = millis % 1000;
            time_t seconds = millis / 1000;
            // 转换为 tm 结构体（UTC时间）
            struct tm timeinfo;
            gmtime_r(&seconds, &timeinfo);
            strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &timeinfo);
            oss << timeBuffer
                << "."
                << std::setw(3) << std::setfill('0')  // 强制3位宽度，不足补零
                << milliseconds;
            j[inputFields_[i]] = oss.str();
        } else if (inputTypes_[i] == "INTEGER") {
            j[inputFields_[i]] = *row->getInt(i);
        } else if (inputTypes_[i] == "DOUBLE") {
            j[inputFields_[i]] = *row->getLong(i);
        } else if (inputTypes_[i] == "BOOLEAN") {
            j[inputFields_[i]] = *row->getInt(i);
        } else if (inputTypes_[i] == "STRING"
                   || inputTypes_[i] == "VARCHAR"
                   || inputTypes_[i] == "VARCHAR(2147483647)") {
            BinaryStringData *stringRowData = row->getString(i);
            std::string strValue = std::string(stringRowData->toString()->begin(),
                                               stringRowData->toString()->end());
            j[inputFields_[i]] = strValue;
        } else {
            LOG("Data type not supported: " << inputTypes_[i])
            throw std::runtime_error("Data type not supported");
        };
    }
}

KeyValueByteContainer DynamicKafkaRecordSerializationSchema::Serialize(RowData *consumedRow)
{
    j.clear();
    RowToJson(consumedRow);
    std::string jsonStr = j.dump();
    return {nullptr, jsonStr.data()};
}

KeyValueByteContainer DynamicKafkaRecordSerializationSchema::Serialize(String *element)
{
    return {nullptr, element->data()};
}

GenericRowData DynamicKafkaRecordSerializationSchema::createProjectedRow(
    RowData &sourceRow,
    RowKind kind,
    std::vector<FieldGetter> &getters)
{
    GenericRowData projectedRow(getters.size());
    for (size_t i = 0; i < getters.size(); ++i) {
        void *fieldPtr = getters[i].getFieldOrNull(&sourceRow);
        if (fieldPtr != nullptr) {
            projectedRow.setField(i, *static_cast<long *>(fieldPtr));
        } else {
            // 处理 nullptr 的情况，例如设置默认值
            projectedRow.setField(i, 0L); // 假设设置默认值为 0
        }
    }
    return projectedRow;
}

void to_json(nlohmann::json& j, const RowKind& kind)
{
    switch (kind) {
        case RowKind::INSERT: j = "INSERT"; break;
        case RowKind::UPDATE_BEFORE: j = "UPDATE_BEFORE"; break;
        case RowKind::UPDATE_AFTER: j = "UPDATE_AFTER"; break;
        case RowKind::DELETE: j = "DELETE"; break;
        default: j = "UNKNOWN";
    }
}

void to_json(nlohmann::json& j, const Row& row)
{
    j["kind"] = row.getKind();
    j["arity"] = row.getFieldByPosition().size();
}

KeyValueByteContainer DynamicKafkaRecordSerializationSchema::Serialize(Row *row)
{
    nlohmann::json j = *row;
    std::string jsonStr = j.dump();
    return {nullptr, jsonStr.data()};
}
