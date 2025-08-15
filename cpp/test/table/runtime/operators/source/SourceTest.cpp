/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include <gtest/gtest.h>
#include "types/logical/LogicalType.h"
#include "runtime/operators/source/csv/CsvSchema.h"
#include "streamrecord/StreamRecord.h"
#include "runtime/operators/source/csv/CsvInputFormat.h"
#include "core/operators/StreamSource.h"
#include "table/runtime/operators/source/InputSplit.h"
#include "typeutils/BinaryRowDataSerializer.h"
#include "io/RecordWriterOutput.h"
#include "test/table/runtime/operators/DummyStreamPartitioner.h"

TEST(SourceTestTest, DISABLED_initoper)
{
    std::string description = R"DELIM({ "format" : "csv",
                        "delimiter" : null,
                        "filePath" : "input/genbid.csv",
                        "fields" : [{"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "auction"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "bidder"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "price"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "channel"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "url"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "TIMESTAMP(3)", "timestampKind" : 0, "fieldName" : "dateTime"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "extra"  }
                                    ],
                        "selectFields" : [ 0, 1, 2, 3, 4 ],
                        "csvSelectFieldToProjectFieldMapping" : [ 0, 1, 2, 3, 4 ],
                        "csvSelectFieldToCsvFieldMapping" : [ 3, 5, 2, 1, 0 ]})DELIM";
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    std::vector<DataTypeId> fields;
    for (auto &field: opDescriptionJSON["fields"]) {
        fields.push_back(LogicalType::flinkTypeToOmniTypeId(field["type"]));
    }
    omnistream::csv::CsvSchema schema(fields);
    std::vector<int> csvSelectFieldToProjectFieldMapping = opDescriptionJSON["csvSelectFieldToProjectFieldMapping"];
    std::vector<int> csvSelectFieldToCsvFieldMapping = opDescriptionJSON["csvSelectFieldToCsvFieldMapping"];
    std::vector<int> oneMap;
    oneMap.resize(csvSelectFieldToProjectFieldMapping.size());
    for (int i = 0; i < csvSelectFieldToProjectFieldMapping.size(); i++) {
        oneMap[csvSelectFieldToProjectFieldMapping[i]] = csvSelectFieldToCsvFieldMapping[i];
    }
    auto csvInputFormat = new omnistream::csv::CsvInputFormat<omnistream::VectorBatch>(schema, 1000,
                                                                            oneMap);
    omnistream::InputSplit *inputSplit = new omnistream::InputSplit(opDescriptionJSON["filePath"], 0,
                                                                    100000);
    auto func = new omnistream::InputFormatSourceFunction<omnistream::VectorBatch>(csvInputFormat, inputSplit);
    BinaryRowDataSerializer *binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t *address = new uint8_t[BUFFER_CAPACITY];
    auto *partitioner = new DummyStreamPartitioner();
    auto *recordWriter = new omnistream::datastream::RecordWriter(address, BUFFER_CAPACITY, partitioner);
    auto *recordWriteOutput = new omnistream::datastream::RecordWriterOutput(binaryRowDataSerializer, recordWriter);
    auto *source = new omnistream::StreamSource(func, recordWriteOutput);
    source->setup();
    source->run();
}