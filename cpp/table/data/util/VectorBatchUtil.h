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
#ifndef FLINK_TNEL_VECTOR_BATCH_UTIL_H
#define FLINK_TNEL_VECTOR_BATCH_UTIL_H

#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"

class VectorBatchUtil {
public:
    static inline int32_t getBatchId(int64_t id)
    {
        uint64_t uid = static_cast<uint64_t>(id);
        return (int32_t)(uid >> 32);
    }

    static inline int32_t getRowId(int64_t id)
    {
        return (int32_t)id;
    }

    static inline int64_t getComboId(int batchId, int rowId)
    {
        uint64_t ubatchId = static_cast<uint64_t>(batchId);
        uint32_t urowId = static_cast<uint32_t>(rowId);
        ubatchId =  (ubatchId << 32) | urowId;
        return static_cast<int64_t>(ubatchId);
    }

    // To print VectorBatch, use VectorHelper::PrintVecBatch from OmniOperatorJIT/core/src/vector/vector_helper.h

    template <typename fromType, typename returnType>
    static omniruntime::vec::BaseVector *castVectorTo(omniruntime::vec::BaseVector *vec)
    {
        auto data = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(reinterpret_cast<omniruntime::vec::Vector<fromType> *>(vec));
        auto numRows = vec->GetSize();
        returnType *dataCopy = new returnType[numRows];
        for (int i = 0; i < numRows; i++) {
            if constexpr (std::is_same<returnType, Decimal128>::value) {
                dataCopy[i] = Decimal128(static_cast<int128_t>(data[i]));
            } else {
                dataCopy[i] = static_cast<returnType>(data[i]);
            }
        }
        omniruntime::vec::Vector<returnType> *newVector = new omniruntime::vec::Vector<returnType>(numRows);
        for (int32_t i = 0; i < numRows; i++) {
            newVector->SetValue(i, dataCopy[i]);
        }
        return newVector;
    }

    static std::string getValueAtAsStr(omnistream::VectorBatch *vecBatch, int32_t col, int32_t row, std::vector<std::string> inputTypes = {})
    {
        std::string value;
        if (vecBatch->Get(col)->IsNull(row)) {
            value = "NULL";
        } else {
            int dataId = vecBatch->Get(col)->GetTypeId();
            switch (dataId) {
                case omniruntime::type::DataTypeId::OMNI_LONG:
                    if (static_cast<int32_t>(inputTypes.size()) > col && inputTypes[col].substr(0, 9) == "TIMESTAMP") {
                        auto millis = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(col))->GetValue(row);
                        return formatTimestamp(millis);
                    } else {
                        value = std::to_string(reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(vecBatch->Get(col))->GetValue(row));
                    }
                    break;
                case omniruntime::type::DataTypeId::OMNI_CHAR: {
                    if (vecBatch->Get(col)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                        auto casted = reinterpret_cast<omniruntime::vec::Vector
                                <omniruntime::vec::LargeStringContainer<std::string_view>> *>(vecBatch->Get(col));
                        value = casted->GetValue(row);
                    } else {
                        auto casted = reinterpret_cast<omniruntime::vec::Vector
                            <omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>> *>(vecBatch->Get(col));
                        value = casted->GetValue(row);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                    value = std::to_string(reinterpret_cast<omniruntime::vec::Vector<double> *>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_INT:
                    value = std::to_string(reinterpret_cast<omniruntime::vec::Vector<int32_t> *>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                    value = reinterpret_cast<omniruntime::vec::Vector<bool> *>(vecBatch->Get(col))->GetValue(row) ? "true" : "false";
                    break;
                case omniruntime::type::DataTypeId::OMNI_DECIMAL64:
                    value = std::to_string(reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                    value = reinterpret_cast<omniruntime::vec::Vector<Decimal128> *>(vecBatch->Get(col))->GetValue(row).ToString();
                    break;
                default:
                    value = "Unsupported Data Type";
                    break;
            }
        }
        return value;
    }

    static std::string formatTimestamp(int64_t millis)
    {
        int milliseconds = millis % 1000;
        time_t seconds = millis / 1000;
        struct tm* timeinfo = gmtime(&seconds);
        char buffer[80];
        size_t len = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", timeinfo);
        if (len == 0) {
            return "Invalid Time";
        }
        std::ostringstream oss;
        constexpr int millisWidth = 3;
        oss << buffer << "." << std::setw(millisWidth) << std::setfill('0') << milliseconds;
        return oss.str();
    }
};

#endif // FLINK_TNEL_VECTOR_BATCH_UTIL_H
