/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

    void* deserialize(std::vector<const uint8_t*>& messageVec, std::vector<size_t>& lengthVec) override
    {
        int rowSize = messageVec.size();
        int colSize = fieldNames.size();
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
        switch (type) {
            case omniruntime::type::DataTypeId::OMNI_LONG:{
                vectorBatch->SetValueAt(colIndex, rowIndex, node[name].get<int64_t>());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:{
                vectorBatch->SetValueAt(colIndex, rowIndex,
                                        TimestampData::fromString(node[name].get<std::string>())->getMillisecond());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                auto stringVec = reinterpret_cast<omniruntime::vec::Vector<
                        omniruntime::vec::LargeStringContainer<std::string_view>> *>(vectorBatch->Get(colIndex));
                auto value = node[name].get<std::string>();
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
    std::vector<std::string> fieldNames;
    std::vector<omniruntime::type::DataTypeId> fieldTypes;
};


#endif // OMNISTREAM_JSONROWDATADESERIALIZATIONSCHEMA_H
