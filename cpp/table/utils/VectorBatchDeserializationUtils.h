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

#pragma once

#include <cstdint>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/vector/vector.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "OmniOperatorJIT/core/src/vector/nulls_buffer.h"
#include "table/data/vectorbatch/VectorBatch.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::vec::unsafe;

namespace omnistream {
// we assume all the char ana varchar data are stored
// vector<LargeStringContainer>, other primitive data types are stored in
// vector<xxx>
class VectorBatchDeserializationUtils {
public:
    static VectorBatch* deserializeVectorBatch(uint8_t *&buffer)
    {
        LOG("----deserializeVectorBatch start:: " << buffer)

        int32_t batchSize;
        memcpy_s(&batchSize, sizeof(batchSize), buffer, sizeof(batchSize));
        buffer += sizeof(batchSize);

        int32_t vectorCount;
        memcpy_s(&vectorCount, sizeof(vectorCount), buffer,
                 sizeof(vectorCount));
        buffer += sizeof(vectorCount);

        int32_t rowCnt = 0;
        memcpy_s(&rowCnt, sizeof(rowCnt), buffer, sizeof(rowCnt));
        buffer += sizeof(rowCnt);

        VectorBatch* batch = new VectorBatch(rowCnt);

        memcpy_s(batch->getTimestamps(), sizeof(int64_t) * rowCnt, buffer,
                 sizeof(int64_t) * rowCnt);
        buffer += sizeof(int64_t) * rowCnt;

        memcpy_s(batch->getRowKinds(), sizeof(RowKind) * rowCnt, buffer,
                 sizeof(RowKind) * rowCnt);
        buffer += sizeof(RowKind) * rowCnt;

        for (int idx = 0; idx < vectorCount; idx++) {
            int32_t vectorSize;
            memcpy_s(&vectorSize, sizeof(int32_t), buffer, sizeof(int32_t));
            buffer += sizeof(int32_t);

            int8_t encodingNum;
            memcpy_s(&encodingNum, sizeof(int8_t), buffer, sizeof(int8_t));

            buffer += sizeof(int8_t);

            int8_t dataTypeNum;
            memcpy_s(&dataTypeNum, sizeof(int8_t), buffer, sizeof(int8_t));
            buffer += sizeof(int8_t);
            DataTypeId dataType = static_cast<DataTypeId>(dataTypeNum);

            Encoding encoding = static_cast<Encoding>(encodingNum);
            if (encoding == OMNI_FLAT) {
                if (dataType == OMNI_CHAR || dataType == OMNI_VARCHAR) {
                    batch->Append(deserializeCharVector(rowCnt, buffer));
                } else {
                    batch->Append(
                        deserializePrimitiveVector(rowCnt, buffer, dataType));
                }
            } else if (encoding == OMNI_DICTIONARY) {
                if (dataType == OMNI_CHAR || dataType == OMNI_VARCHAR) {
                    batch->Append(deserializeStringDictionaryContainerVector(
                        rowCnt, buffer));
                } else {
                    throw std::runtime_error("Unsupported data type");
                }
            }
        }
        LOG("----deserializeVectorBatch END:: " << reinterpret_cast<long>(buffer))

        return batch;
    }

    static BaseVector *deserializePrimitiveVector(int32_t vectorSize, uint8_t *&buffer, DataTypeId dataType)
    {
        BaseVector *baseVector = nullptr;

        switch (dataType) {
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64: {
                Vector<int64_t> *vector64 = new Vector<int64_t>(vectorSize);
                baseVector = vector64;
                deserializeInt64(vector64, buffer);
                break;
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                Vector<int32_t> *vector32 = new Vector<int32_t>(vectorSize);
                baseVector = vector32;
                deserializeInt32(vector32, buffer);
                break;
            }

            case OMNI_SHORT: {
                Vector<int16_t> *vector16 = new Vector<int16_t>(vectorSize);
                baseVector = vector16;
                deserializeInt16(vector16, buffer);
                break;
            }

            case OMNI_DOUBLE: {
                Vector<double> *vectorDouble = new Vector<double>(vectorSize);
                baseVector = vectorDouble;
                deserializeDouble(vectorDouble, buffer);
                break;
            }
            case OMNI_BOOLEAN: {
                Vector<bool> *vectorBool = new Vector<bool>(vectorSize);
                baseVector = vectorBool;
                deserializeBool(vectorBool, buffer);
                break;
            }
            case OMNI_DECIMAL128: {
                Vector<Decimal128> *vectorDecimal128 =
                    new Vector<Decimal128>(vectorSize);
                baseVector = vectorDecimal128;
                deserializeDecimal128(vectorDecimal128, buffer);
                break;
            }
            default:
                throw std::runtime_error("Unsupported data type");
        }
        return baseVector;
    }

    static Vector<LargeStringContainer<std::string_view>> *
    deserializeCharVector(int32_t size, uint8_t *&buffer)
    {
        int32_t stringBodySize;
        memcpy_s(&stringBodySize, sizeof(int32_t), buffer, sizeof(int32_t));
        buffer += sizeof(int32_t);

        Vector<LargeStringContainer<std::string_view>> *charVector =
            new Vector<LargeStringContainer<std::string_view>>(size,
                                                               stringBodySize);
        deserializeNulls(charVector, buffer, size);

        // deserialize offset data
        int32_t *offsetArr = UnsafeStringVector::GetOffsets(charVector);
        memcpy_s(offsetArr, sizeof(int32_t) * (size + 1), buffer,
                 sizeof(int32_t) * (size + 1));
        buffer += sizeof(int32_t) * (size + 1);

        size_t copySize = offsetArr[size] * sizeof(char);
        memcpy_s(UnsafeStringVector::GetValues(charVector), copySize, buffer,
                 copySize);
        buffer += offsetArr[size] * sizeof(char);

        return charVector;
    }

    static void deserializeNulls(BaseVector *baseVector, uint8_t *&buffer,
                                 int32_t size)
    {
        auto nullData = UnsafeBaseVector::GetNulls(baseVector);
        auto nullByteSize = omniruntime::vec::NullsBuffer::CalculateNbytes(size);
        memcpy_s(nullData, nullByteSize, buffer, nullByteSize);
        buffer += nullByteSize;
    }

    static void deserializeInt64(Vector<int64_t> *vector64, uint8_t *&buffer)
    {
        int32_t size = vector64->GetSize();

        deserializeNulls(vector64, buffer, size);

        int64_t *data = UnsafeVector::GetRawValues(vector64);
        memcpy_s(data, sizeof(int64_t) * size, buffer, sizeof(int64_t) * size);
        buffer += sizeof(int64_t) * size;
    }

    static void deserializeInt32(Vector<int32_t> *vector32, uint8_t *&buffer)
    {
        int32_t size = vector32->GetSize();

        deserializeNulls(vector32, buffer, size);
        int32_t *data = UnsafeVector::GetRawValues(vector32);

        memcpy_s(data, sizeof(int32_t) * size, buffer, sizeof(int32_t) * size);
        buffer += sizeof(int32_t) * size;
    }

    static void deserializeInt16(Vector<int16_t> *vector16, uint8_t *&buffer)
    {
        int32_t size = vector16->GetSize();
        deserializeNulls(vector16, buffer, size);
        int16_t *data = UnsafeVector::GetRawValues(vector16);
        memcpy_s(data, sizeof(int16_t) * size, buffer, sizeof(int16_t) * size);
        buffer += sizeof(int16_t) * size;
    }

    static void deserializeDouble(Vector<double> *vectorDouble,
                                  uint8_t *&buffer)
    {
        int32_t size = vectorDouble->GetSize();
        deserializeNulls(vectorDouble, buffer, size);
        double *data = UnsafeVector::GetRawValues(vectorDouble);
        memcpy_s(data, sizeof(double) * size, buffer, sizeof(double) * size);
        buffer += sizeof(double) * size;
    }

    static void deserializeBool(Vector<bool> *vectorBool, uint8_t *&buffer)
    {
        int32_t size = vectorBool->GetSize();
        deserializeNulls(vectorBool, buffer, size);
        bool *data = UnsafeVector::GetRawValues(vectorBool);
        memcpy_s(data, sizeof(bool) * size, buffer, sizeof(bool) * size);
        buffer += sizeof(bool) * size;
    }

    static void deserializeDecimal128(Vector<Decimal128> *vectorDecimal128,
                                      uint8_t *&buffer)
    {
        int32_t size = vectorDecimal128->GetSize();
        deserializeNulls(vectorDecimal128, buffer, size);
        Decimal128 *data = UnsafeVector::GetRawValues(vectorDecimal128);
        memcpy_s(data, sizeof(Decimal128) * size, buffer,
                 sizeof(Decimal128) * size);
        buffer += sizeof(Decimal128) * size;
    }

    static Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *
    deserializeStringDictionaryContainerVector(int32_t rowCnt,
                                               uint8_t *&buffer)
    {
        int32_t *values = new int32_t[rowCnt];
        memcpy_s(values, sizeof(int32_t) * rowCnt, buffer,
                 sizeof(int32_t) * rowCnt);
        buffer += sizeof(int32_t) * rowCnt;

        int32_t dictOffset = 0;
        memcpy_s(&dictOffset, sizeof(int32_t), buffer, sizeof(int32_t));

        buffer += sizeof(int32_t);

        int32_t dictSize = 0;
        memcpy_s(&dictSize, sizeof(int32_t), buffer, sizeof(int32_t));
        buffer += sizeof(int32_t);

        int32_t stringBodySize = 0;
        memcpy_s(&stringBodySize, sizeof(int32_t), buffer, sizeof(int32_t));

        buffer += sizeof(int32_t);
        auto nullByteSize = omniruntime::vec::NullsBuffer::CalculateNbytes(rowCnt);
        std::shared_ptr<AlignedBuffer<uint8_t>> nullsBuffer =
            std::make_shared<AlignedBuffer<uint8_t>>(nullByteSize);
        memcpy(nullsBuffer->GetBuffer(), buffer,
               nullByteSize);
        buffer += nullByteSize;

        std::shared_ptr<LargeStringContainer<std::string_view>>
            stringContainer =
                std::make_shared<LargeStringContainer<std::string_view>>(
                    dictSize, stringBodySize);

        // string offset
        int32_t *offset =
            UnsafeStringContainer::GetOffsets(stringContainer.get());

        auto ret = memcpy_s(offset, sizeof(int32_t) * (dictSize + 1), buffer,
            sizeof(int32_t) * (dictSize + 1));
        if (ret != EOK) {
            LOG("memcpy_s failed for string offset, ret = " << ret)
        }
        buffer += sizeof(int32_t) * (dictSize + 1);

        // string body
        ret = memcpy_s(UnsafeStringContainer::GetValues(stringContainer.get()),
                       offset[dictSize] * sizeof(char), buffer,
                       offset[dictSize] * sizeof(char));
        if (ret != EOK) {
            LOG("memcpy_s failed for string offset, ret = " << ret)
        }
        buffer += offset[dictSize] * sizeof(char);

        auto dictionary =
            std::make_shared<DictionaryContainer<std::string_view>>(
                values, rowCnt, stringContainer, dictSize, dictOffset);
        auto newNullsBuffer = new NullsBuffer(rowCnt, nullsBuffer);
        auto stringDictionaryVector =
            new Vector<DictionaryContainer<std::string_view>>(
                rowCnt, dictionary, newNullsBuffer, false, OMNI_CHAR);

        return stringDictionaryVector;
    }

    static long derializeWatermark(uint8_t*& buffer)
    {
        long timestamp;
        memcpy_s(&timestamp, sizeof(long), buffer, sizeof(long));
        buffer += sizeof(long);
        return timestamp;
    }
};
}  // namespace omnistream
