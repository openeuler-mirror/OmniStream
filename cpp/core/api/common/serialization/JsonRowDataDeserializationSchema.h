/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_JSONROWDATADESERIALIZATIONSCHEMA_H
#define OMNISTREAM_JSONROWDATADESERIALIZATIONSCHEMA_H

#include <tuple>
#include "core/api/common/serialization/DeserializationSchema.h"

class JsonRowDataDeserializationSchema : public DeserializationSchema {
public:
    explicit JsonRowDataDeserializationSchema(nlohmann::json& opDescriptionJSON)
        : fieldNames(opDescriptionJSON["outputNames"].get<std::vector<std::string>>())
    {
        auto outputTypes = opDescriptionJSON["outputTypes"].get<std::vector<std::string>>();
        for (std::string type : outputTypes) {
            fieldTypes.push_back(LogicalType::flinkTypeToOmniTypeId(type));
        }
    }

    void Open() override
    {
    }

    void deserialize(const uint8_t* message, size_t length, Collector* out) override
    {
        auto* vectorBatch = deserializeSingleRecord(message, length);
        if (vectorBatch != nullptr) {
            out->collect(vectorBatch);
        }
    }

    void* deserialize(std::vector<const uint8_t*>& messageVec, std::vector<size_t>& lengthVec) override
    {
        int rowSize = static_cast<int>(messageVec.size());
        int colSize = static_cast<int>(fieldNames.size());
        auto *vectorBatch = createBatch(rowSize, fieldTypes);
        nlohmann::json node;
        for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            node = nlohmann::json::parse(std::string_view(
                reinterpret_cast<const char *>(messageVec[rowIndex]), lengthVec[rowIndex]));
            for (int colIndex = 0; colIndex < colSize; colIndex++) {
                setColValue(rowIndex, colIndex, vectorBatch, node);
            }
        }
        return vectorBatch;
    }

    void setColValue(int rowIndex, int colIndex, omnistream::VectorBatch* vectorBatch, nlohmann::json& node)
    {
        auto& type = fieldTypes[colIndex];
        auto& name = fieldNames[colIndex];
        auto fieldIt = node.find(name);
        if (fieldIt == node.end() || fieldIt->is_null()) {
            vectorBatch->Get(colIndex)->SetNull(rowIndex);
            return;
        }

        switch (type) {
            case omniruntime::type::DataTypeId::OMNI_INT:{
                vectorBatch->SetValueAt(colIndex, rowIndex, fieldIt->get<int32_t>());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_LONG:{
                vectorBatch->SetValueAt(colIndex, rowIndex, fieldIt->get<int64_t>());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:{
                vectorBatch->SetValueAt(colIndex, rowIndex,
                                        TimestampData::fromString(fieldIt->get<std::string>())->getMillisecond());
                break;
            }
            case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) : {
                vectorBatch->SetValueAt(colIndex, rowIndex,
                                        TimestampData::fromLocalTimeString(fieldIt->get<std::string>())->getMillisecond());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                auto stringVec = reinterpret_cast<omniruntime::vec::Vector<
                        omniruntime::vec::LargeStringContainer<std::string_view>> *>(vectorBatch->Get(colIndex));
                auto value = fieldIt->get<std::string>();
                std::string_view strView(value.data(), value.size());
                stringVec->SetValue(rowIndex, strView);
                break;
            }
            default:
                std::runtime_error("DataType not supported yet!");
        }
    }

    bool isEndOfStream(const void* nextElement) override
    {
        return false;
    }

private:
    omnistream::VectorBatch* deserializeSingleRecord(const uint8_t* message, size_t length)
    {
        std::vector<const uint8_t*> messageVec{message};
        std::vector<size_t> lengthVec{length};
        return reinterpret_cast<omnistream::VectorBatch*>(deserialize(messageVec, lengthVec));
    }

    std::vector<std::string> fieldNames;
    std::vector<omniruntime::type::DataTypeId> fieldTypes;
};


#endif // OMNISTREAM_JSONROWDATADESERIALIZATIONSCHEMA_H
