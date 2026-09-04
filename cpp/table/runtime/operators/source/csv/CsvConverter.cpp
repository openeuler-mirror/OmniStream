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
#include <stdexcept>
#include "OmniOperatorJIT/core/src/type/TimestampConversion.h"

using namespace omniruntime::type;

namespace omnistream {
namespace csv {

namespace {
bool isDefaultNullLiteral(const std::string& value)
{
    return value == "null";
}

int32_t parseDateToEpochDays(const std::string& value)
{
    auto result = omniruntime::type::util::fromDateString(
        std::string_view(value.data(), value.size()));
    if (result.hasError()) {
        throw std::invalid_argument("invalid date literal: " + value);
    }
    return result.value();
}

CsvNode* getProjectedNodeOrNull(const CsvRow& csvRow, int csvFieldIndex)
{
    const auto& nodes = csvRow.getNodes();
    if (csvFieldIndex < 0 || static_cast<size_t>(csvFieldIndex) >= nodes.size()) {
        return nullptr;
    }

    return nodes[csvFieldIndex].get();
}

bool isCsvNullValue(const std::string& value, const CsvSchema& schema)
{
    if (schema.hasNullValue()) {
        return value == schema.getNullValue();
    }

    return isDefaultNullLiteral(value);
}

long parseDecimal64ToUnscaledLong(const std::string& value, int32_t scale)
{
    std::string valueStr = value;
    bool negative = false;
    if (!valueStr.empty() && valueStr[0] == '-') {
        negative = true;
        valueStr = valueStr.substr(1);
    }
    std::string intPart, fracPart;
    size_t dotPos = valueStr.find('.');
    if (dotPos != std::string::npos) {
        intPart = valueStr.substr(0, dotPos);
        fracPart = valueStr.substr(dotPos + 1);
    } else {
        intPart = valueStr;
        fracPart = "";
    }
    if (static_cast<int32_t>(fracPart.length()) < scale) {
        fracPart += std::string(scale - fracPart.length(), '0');
    } else if (static_cast<int32_t>(fracPart.length()) > scale) {
        fracPart = fracPart.substr(0, scale);
    }
    long unscaledValue = std::stol(intPart + fracPart);
    if (negative) {
        unscaledValue = -unscaledValue;
    }
    return unscaledValue;
}

Decimal128 parseDecimal128ToValue(const std::string& value, int32_t scale)
{
    std::string valueStr = value;
    bool negative = false;
    if (!valueStr.empty() && valueStr[0] == '-') {
        negative = true;
        valueStr = valueStr.substr(1);
    }
    std::string intPart, fracPart;
    size_t dotPos = valueStr.find('.');
    if (dotPos != std::string::npos) {
        intPart = valueStr.substr(0, dotPos);
        fracPart = valueStr.substr(dotPos + 1);
    } else {
        intPart = valueStr;
        fracPart = "";
    }
    if (static_cast<int32_t>(fracPart.length()) < scale) {
        fracPart += std::string(scale - fracPart.length(), '0');
    } else if (static_cast<int32_t>(fracPart.length()) > scale) {
        fracPart = fracPart.substr(0, scale);
    }
    std::string unscaledStr = intPart + fracPart;
    if (negative) {
        unscaledStr = "-" + unscaledStr;
    }
    return Decimal128(std::string_view(unscaledStr));
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
                LOG("CsvConverter: Invalid integer value '" << value << "' for column " << i
                                                            << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Integer value '" << value << "' out of range for column " << i
                                                    << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_BOOLEAN) {
            LOG("CsvConverter: Converting value '" << value << "' to boolean for column " << i);
            try {
                std::string trimmed = value;
                trimmed.erase(trimmed.begin(), std::find_if(trimmed.begin(), trimmed.end(),
                    [](unsigned char ch) { return !::isspace(ch); }));
                trimmed.erase(std::find_if(trimmed.rbegin(), trimmed.rend(),
                    [](unsigned char ch) { return !::isspace(ch); }).base(), trimmed.end());
                std::transform(trimmed.begin(), trimmed.end(), trimmed.begin(),
                    [](unsigned char ch) { return static_cast<char>(::tolower(ch)); });
                if (trimmed == "true") {
                    rowData->setBool(i, true);
                } else {
                    rowData->setBool(i, false); //不能识别的也默认false，不报错
                }
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid boolean value '" << value << "' for column " << i << ", setting it as null.");
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
                LOG("CsvConverter: Long value '" << value << "' out of range for column " << i
                                                 << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_DECIMAL64) {
            LOG("CsvConverter: Converting value '" << value << "' to decimal64 for column " << i);
            try {
                int32_t scale = schema.getScaleAtIdx(i);
                rowData->setLong(i, parseDecimal64ToUnscaledLong(value, scale));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid decimal64 value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Decimal64 value '" << value << "' out of range for column " << i
                                                      << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_DECIMAL128) {
            LOG("CsvConverter: Converting value '" << value << "' to decimal128 for column " << i);
            try {
                int32_t scale = schema.getScaleAtIdx(i);
                Decimal128 decimalValue = parseDecimal128ToValue(value, scale);
                rowData->setDecimal128(i, decimalValue.LowBits(), decimalValue.HighBits());
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid decimal128 value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Decimal128 value '" << value << "' out of range for column " << i
                                                      << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_DOUBLE) {
            LOG("CsvConverter: Converting value '" << value << "' to double for column " << i);
            try {
                rowData->setDouble(i, std::stod(value));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid double value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Double value '" << value << "' out of range for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_DATE32) {
            LOG("CsvConverter: Converting value '" << value << "' to date for column " << i);
            try {
                rowData->setInt(i, parseDateToEpochDays(value));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid date value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE) {
            LOG("CsvConverter: Converting value '" << value << "' to time for column " << i);
            try {
                static int milliSec = 0;
                rowData->setTimestamp(i, TimestampData::fromTimeString(value), milliSec);
            } catch (...) {
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_VARCHAR) {
            LOG("CsvConverter: Converting value '" << value << "' to string for column " << i);
            std::string_view sv = value;
            rowData->setStringView(i, sv);
        } else if (type == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE) {
            LOG("CsvConverter: Converting value '" << value << "' to timestamp for column " << i);
            try {
                static int milliSec = 3;
                rowData->setTimestamp(i, TimestampData::fromString(value), milliSec); //并没有读取毫秒精度以上的那部分小数点后的数据
            } catch (...) {
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            LOG("CsvConverter: Converting value '" << value << "' to timestamp with local time zone for column " << i);
            try {
                static int milliSec = 3;
                rowData->setTimestamp(i, TimestampData::fromLocalTimeString(value), milliSec);
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
omnistream::VectorBatch* CsvConverter::convert(std::vector<CsvRow>& csvRows)
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
omnistream::VectorBatch* CsvConverter::convert(std::vector<CsvRow>& csvRows, std::vector<int>& oneMap)
{
    // create new vectorbatch
    std::vector<DataTypeId> targetTypes = csvRows[0].getSchema().getTypes();
    std::vector<DataTypeId> newVecBatchTypes;
    for (size_t i = 0; i < oneMap.size(); i++) {
        newVecBatchTypes.push_back(targetTypes[oneMap[i]]);
    }
    auto vectorBatch = omnistream::VectorBatch::CreateVectorBatch(csvRows.size(), newVecBatchTypes);
    for (size_t i = 0; i < oneMap.size(); i++) {
        if (newVecBatchTypes[i] == DataTypeId::OMNI_DECIMAL64) {
            int32_t scale = csvRows[0].getSchema().getScaleAtIdx(oneMap[i]);
            vectorBatch->Get(i)->SetDataType(std::make_shared<Decimal64DataType>(18, scale));
        } else if (newVecBatchTypes[i] == DataTypeId::OMNI_DECIMAL128) {
            int32_t scale = csvRows[0].getSchema().getScaleAtIdx(oneMap[i]);
            vectorBatch->Get(i)->SetDataType(std::make_shared<Decimal128DataType>(38, scale));
        }
    }
    // Put data
    for (size_t rowIndex = 0; rowIndex < csvRows.size(); rowIndex++) {
        CsvRow csvRow = csvRows[rowIndex];
        const CsvSchema schema = csvRow.getSchema();
        for (size_t colIndex = 0; colIndex < oneMap.size(); colIndex++) {
            int csvFieldIndex = oneMap[colIndex];
            CsvNode* node = getProjectedNodeOrNull(csvRow, csvFieldIndex);
            if (node == nullptr) {
                LOG("CsvConverter: Missing projected CSV field " << csvFieldIndex << " for column " << colIndex
                                                                 << " in row " << rowIndex
                                                                 << ", setting it as null in VectorBatch.");
                vectorBatch->Get(colIndex)->SetNull(rowIndex);
                continue;
            }

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
                        LOG("CsvConverter: Converting value '" << nodeValue << "' to integer for column " << colIndex
                                                               << " in row " << rowIndex);
                        vectorBatch->SetValueAt(colIndex, rowIndex, std::stoi(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        LOG("CsvConverter: Invalid integer value '" << nodeValue << "' for column " << colIndex
                                                                    << " in row " << rowIndex
                                                                    << ", setting it as null.");
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        LOG("CsvConverter: Integer value '" << nodeValue << "' out of range for column " << colIndex
                                                            << " in row " << rowIndex << ", setting it as null.");
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case (omniruntime::type::DataTypeId::OMNI_BOOLEAN): {
                    try {
                        LOG("CsvConverter: Converting value '" << nodeValue << "' to boolean for column " << colIndex
                                                            << " in row " << rowIndex);
                        bool boolVal = false;
                        std::string trimmed = nodeValue;
                        trimmed.erase(trimmed.begin(), std::find_if(trimmed.begin(), trimmed.end(),
                            [](unsigned char ch) { return !::isspace(ch); }));
                        trimmed.erase(std::find_if(trimmed.rbegin(), trimmed.rend(),
                            [](unsigned char ch) { return !::isspace(ch); }).base(), trimmed.end());
                        std::transform(trimmed.begin(), trimmed.end(), trimmed.begin(),
                            [](unsigned char ch) { return static_cast<char>(::tolower(ch)); });
                        if (trimmed == "true") {
                            boolVal = true;
                        }
                        vectorBatch->SetValueAt(colIndex, rowIndex, boolVal);
                    } catch (const std::invalid_argument& e) {
                        LOG("CsvConverter: Invalid boolean value '" << nodeValue << "' for column " << colIndex
                                                                    << " in row " << rowIndex
                                                                    << ", setting it as null.");
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG: {
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
                case omniruntime::type::DataTypeId::OMNI_DECIMAL64: {
                    try {
                        int32_t scale = schema.getScaleAtIdx(csvFieldIndex);
                        vectorBatch->SetValueAt(colIndex, rowIndex, parseDecimal64ToUnscaledLong(nodeValue, scale));
                    } catch (const std::invalid_argument& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
                    try {
                        int32_t scale = schema.getScaleAtIdx(csvFieldIndex);
                        Decimal128 decimalValue = parseDecimal128ToValue(nodeValue, scale);
                        vectorBatch->SetValueAt(colIndex, rowIndex, decimalValue);
                    } catch (const std::invalid_argument& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP: {
                    try {
                        vectorBatch->SetValueAt(
                            colIndex, rowIndex, TimestampData::fromString(nodeValue).getMillisecond());
                    } catch (...) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                    try {
                        vectorBatch->SetValueAt(
                            colIndex, rowIndex, TimestampData::fromLocalTimeString(nodeValue).getMillisecond());
                    } catch (...) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:{
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, std::stod(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DATE32:{
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, parseDateToEpochDays(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:{
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, TimestampData::fromTimeString(nodeValue).getMillisecond());
                    } catch (...) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                    auto stringVec = reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        vectorBatch->Get(colIndex));
                    std::string_view strView(nodeValue.data(), nodeValue.size());
                    stringVec->SetValue(rowIndex, strView);
                    break;
                }
                default: std::runtime_error("DataType not supported yet!");
            }
        }
    }
    return vectorBatch;
}

} // namespace csv
} // namespace omnistream
