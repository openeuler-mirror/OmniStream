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
#include "CsvConverter.h"
#include <string>

using namespace omniruntime::type;

namespace omnistream {
namespace csv {

namespace {
bool isDefaultNullLiteral(const std::string& value)
{
    return value == "null";
}

bool isCsvNullValue(const std::string& value, const CsvSchema& schema)
{
    if (schema.hasNullValue()) {
        return value == schema.getNullValue();
    }

    return isDefaultNullLiteral(value);
}

} // namespace

BinaryRowData* CsvConverter::convert(const CsvRow& csvRow)
{
    // Original implementation uses `GenericRowData`
    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(csvRow.getNodes().size());
    const CsvSchema schema = csvRow.getSchema();
    for (size_t i = 0; i < csvRow.getNodes().size(); i++) {
        CsvNode* node = csvRow.getNodes()[i].get();
        omniruntime::type::DataTypeId type = node->getType();
        std::string value = node->getValue();
        if (isCsvNullValue(value, schema)) {
            LOG("CsvConverter: Detected null value for column " << i << ", setting it as null in BinaryRowData.");
            rowData->setNullAt(i);
            continue;
        }

        if (type == omniruntime::type::DataTypeId::OMNI_INT) {
            LOG("CsvConverter: Converting value '" << value << "' to int for column " << i);
            try {
                rowData->setInt(i, std::stoi(value));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid integer value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Integer value '" << value << "' out of range for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_LONG) {
            LOG("CsvConverter: Converting value '" << value << "' to long for column " << i);
            try {
                rowData->setLong(i, std::stol(value));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid long value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Long value '" << value << "' out of range for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_VARCHAR) {
            LOG("CsvConverter: Converting value '" << value << "' to string for column " << i);
            std::string_view sv = value;
            rowData->setStringView(i, sv);
        } else if (type == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
            type == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            LOG("CsvConverter: Converting value '" << value << "' to timestamp for column " << i);
            try {
                static int milliSec = 3;
                rowData->setTimestamp(i, *TimestampData::fromString(value), milliSec);
            } catch (...) {
                rowData->setNullAt(i);
            }
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
        const CsvSchema schema = csvRow.getSchema();
        for (size_t colIndex = 0; colIndex < oneMap.size(); colIndex++) {
            int csvFieldIndex = oneMap[colIndex];
            CsvNode *node = csvRow.getNodes()[csvFieldIndex].get();
            omniruntime::type::DataTypeId nodeType = node->getType();
            std::string nodeValue = node->getValue();
            if (nodeType != targetTypes[csvFieldIndex]) {
                throw std::runtime_error("CsvNode mismatch.");
            }

            if (isCsvNullValue(nodeValue, schema)) {
                LOG("CsvConverter: Detected null value for column " << colIndex << " in row " << rowIndex
                    << ", setting it as null in VectorBatch.");
                vectorBatch->Get(colIndex)->SetNull(rowIndex);
                continue;
            }

            switch (nodeType) {
                case (omniruntime::type::DataTypeId::OMNI_INT): {
                    try {
                        LOG("CsvConverter: Converting value '" << nodeValue << "' to integer for column " << colIndex << " in row " << rowIndex);
                        vectorBatch->SetValueAt(colIndex, rowIndex, std::stoi(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        LOG("CsvConverter: Invalid integer value '" << nodeValue << "' for column " << colIndex << " in row " << rowIndex << ", setting it as null.");
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        LOG("CsvConverter: Integer value '" << nodeValue << "' out of range for column " << colIndex << " in row " << rowIndex << ", setting it as null."); 
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
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
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:{
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, TimestampData::fromLocalTimeString(nodeValue)->getMillisecond());
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

} // namespace csv
} // namespace omnistream

