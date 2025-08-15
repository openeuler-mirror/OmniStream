/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "CsvConverter.h"

using namespace omniruntime::type;

namespace omnistream {
namespace csv {

BinaryRowData* CsvConverter::convert(const CsvRow& csvRow)
{
    // Original implementation uses `GenericRowData`
    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(csvRow.getNodes().size());
    for (size_t i = 0; i < csvRow.getNodes().size(); i++) {
        CsvNode* node = csvRow.getNodes()[i].get();
        omniruntime::type::DataTypeId type = node->getType();
        std::string value = node->getValue();
        if (type == omniruntime::type::DataTypeId::OMNI_INT) {
            rowData->setInt(i, std::stoi(value));
        } else if (type == omniruntime::type::DataTypeId::OMNI_LONG) {
            try {
                rowData->setLong(i, std::stol(value));
            } catch (const std::invalid_argument& e) {
                // Not a valid number
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                // Number is out of range for long
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_VARCHAR) {
            std::string_view sv = value;
            rowData->setStringView(i, sv);
        } else if (type == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE) {
            static int milliSec = 3;
            rowData->setTimestamp(i, *TimestampData::fromString(value), milliSec);
        } else {
            throw std::runtime_error("Unsupported type: " + type);
        }
    }
    return rowData;
}

/**
 * @brief Convert csv rows to vector batch using the first row's schema as the target schema
 * @param csvRows
 * @return VectorBatch
 */
omnistream::VectorBatch* CsvConverter::convert(std::vector<CsvRow> &csvRows)
{
    // Create a dummy oneMap that directly uses the csv field index as the project field index
    std::vector<int> oneMap;
    oneMap.resize(csvRows[0].getSchema().getArity());
    for (int i = 0; i < csvRows[0].getSchema().getArity(); i++) {
        oneMap[i] = i;
    }
    return convert(csvRows, oneMap);
}

/**
 * @brief Convert csv rows to vector batch using the given `oneMap`
 * @param csvRows
 * @param oneMap mapping from project field index to csv field index
 * @return VectorBatch
 */
omnistream::VectorBatch* CsvConverter::convert(std::vector<CsvRow> &csvRows, std::vector<int>& oneMap)
{
    // create new vectorbatch
    std::vector<DataTypeId> targetTypes = csvRows[0].getSchema().getTypes();
    std::vector<DataTypeId> newVecBatchTypes;
    for (size_t i = 0; i < oneMap.size(); i++) {
        newVecBatchTypes.push_back(targetTypes[oneMap[i]]);
    }
    auto vectorBatch = omnistream::VectorBatch::CreateVectorBatch(csvRows.size(), newVecBatchTypes);
    // Put data
    for (size_t rowIndex = 0; rowIndex < csvRows.size(); rowIndex++) {
        CsvRow csvRow = csvRows[rowIndex];
        for (size_t colIndex = 0; colIndex < oneMap.size(); colIndex++) {
            int csvFieldIndex = oneMap[colIndex];
            CsvNode *node = csvRow.getNodes()[csvFieldIndex].get();
            omniruntime::type::DataTypeId nodeType = node->getType();
            std::string nodeValue = node->getValue();
            if (nodeType != targetTypes[csvFieldIndex]) {
                throw std::runtime_error("CsvNode mismatch.");
            }

            if (nodeValue == "null") {
                vectorBatch->Get(colIndex)->SetNull(rowIndex);
                continue;
            }

            switch (nodeType) {
                case (omniruntime::type::DataTypeId::OMNI_INT): {
                    vectorBatch->SetValueAt(colIndex, rowIndex, std::stoi(nodeValue));
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG:{
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, std::stol(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        // Not a valid number
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        // Number is out of range for long
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:{
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, TimestampData::fromString(nodeValue)->getMillisecond());
                    } catch (...) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                    auto stringVec = reinterpret_cast<omniruntime::vec::Vector
                        <omniruntime::vec::LargeStringContainer<std::string_view>> *>(vectorBatch->Get(colIndex));
                    std::string_view strView(nodeValue.data(), nodeValue.size());
                    stringVec->SetValue(rowIndex, strView);
                    break;
                }
                default:
                    std::runtime_error("DataType not supported yet!");
            }
        }
    }
    return vectorBatch;
}

}  // namespace csv
}  // namespace omnistream

