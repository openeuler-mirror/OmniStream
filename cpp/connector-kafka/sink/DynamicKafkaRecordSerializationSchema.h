/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_DYNAMICKAFKARECORDSERIALIZATIONSCHEMA_H
#define FLINK_BENCHMARK_DYNAMICKAFKARECORDSERIALIZATIONSCHEMA_H

#include <vector>
#include <memory>
#include <stdexcept>
#include <cstdint>
#include <nlohmann/json.hpp>
#include "table/data/RowData.h"
#include "KafkaRecordSerializationSchema.h"
#include "table/data/GenericRowData.h"
#include "table/Row.h"
#include "KeyValueByteContainer.h"
#include "basictypes/String.h"

class DynamicKafkaRecordSerializationSchema : public KafkaRecordSerializationSchema<RowData> {
public:
    using MetadataPositions = std::vector<int>;

    DynamicKafkaRecordSerializationSchema() {};

    DynamicKafkaRecordSerializationSchema(std::vector<std::string>& inputFields,
                                          std::vector<std::string>& inputTypes);
    void RowToJson(RowData* row);
    KeyValueByteContainer Serialize(RowData* consumedRow);
    KeyValueByteContainer Serialize(String* element);
    KeyValueByteContainer Serialize(Row* row);

private:
    std::vector<FieldGetter> keyFieldGetters_;
    std::vector<FieldGetter> valueFieldGetters_;
    bool hasMetadata_;
    MetadataPositions metadataPositions_;
    bool upsertMode_;
    std::vector<std::string> inputFields_;
    std::vector<std::string> inputTypes_;
    nlohmann::ordered_json j;
    std::ostringstream oss;
    char timeBuffer[80];
    static GenericRowData createProjectedRow(
            RowData &sourceRow,
            RowKind kind,
            std::vector<FieldGetter>& getters
    );
};

#endif // FLINK_BENCHMARK_DYNAMICKAFKARECORDSERIALIZATIONSCHEMA_H
