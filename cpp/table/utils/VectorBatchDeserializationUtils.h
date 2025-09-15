
#pragma once

#include <cstdint>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/vector/vector.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "OmniOperatorJIT/core/src/vector/nulls_buffer.h"
#include "table/vectorbatch/VectorBatch.h"
#include <arm_sve.h>

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::vec::unsafe;

namespace omnistream {
// we assume all the char ana varchar data are stored
// vector<LargeStringContainer>, other primitive data types are stored in
// vector<xxx>
class VectorBatchDeserializationUtils {
public:
    static void getResData(void* dst, void* src, size_t cur, int res)
    {
        svbool_t pg = svwhilelt_b8(0, res);
        svuint8_t s = svld1(pg, reinterpret_cast<uint8_t*>(src) + cur);
        svst1(pg, reinterpret_cast<uint8_t*>(dst) + cur, s);
    }

    static VectorBatch* deserializeVectorBatch(uint8_t *&buffer) {
        LOG("----deserializeVectorBatch start:: " << buffer)

        int32_t batchSize;
        size_t len = sizeof(batchSize);
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(&batchSize) + cur, s);
            cur += skip_num;
        }
        getResData(&batchSize, buffer, cur, len - cur);

        buffer += sizeof(batchSize);

        int32_t vectorCount;
        memcpy(&vectorCount, buffer,
                 sizeof(vectorCount));
        buffer += sizeof(vectorCount);

        int32_t rowCnt = 0;
        memcpy(&rowCnt, buffer, sizeof(rowCnt));
        buffer += sizeof(rowCnt);

        VectorBatch* batch = new VectorBatch(rowCnt);

        memcpy(batch->getTimestamps(), buffer,
                 sizeof(int64_t) * rowCnt);
        buffer += sizeof(int64_t) * rowCnt;

        memcpy(batch->getRowKinds(), buffer,
                 sizeof(RowKind) * rowCnt);
        buffer += sizeof(RowKind) * rowCnt;

        for (int idx = 0; idx < vectorCount; idx++) {
            int32_t vectorSize;
            memcpy(&vectorSize, buffer, sizeof(int32_t));
            buffer += sizeof(int32_t);

            int8_t encodingNum;
            memcpy(&encodingNum, buffer, sizeof(int8_t));

            buffer += sizeof(int8_t);

            int8_t dataTypeNum;
            memcpy(&dataTypeNum, buffer, sizeof(int8_t));
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

    static BaseVector *deserializePrimitiveVector(int32_t vectorSize,
                                                  uint8_t *&buffer,
                                                  DataTypeId dataType) {
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
                break;
        }

        return baseVector;
    }

    static Vector<LargeStringContainer<std::string_view>> *
    deserializeCharVector(int32_t size, uint8_t *&buffer) {
        int32_t stringBodySize;
        memcpy(&stringBodySize, buffer, sizeof(int32_t));
        buffer += sizeof(int32_t);

        Vector<LargeStringContainer<std::string_view>> *charVector =
            new Vector<LargeStringContainer<std::string_view>>(size,
                                                               stringBodySize);
        deserializeNulls(charVector, buffer, size);

        // deserialize offset data
        int32_t *offsetArr = UnsafeStringVector::GetOffsets(charVector);
        memcpy(offsetArr, buffer,
                 sizeof(int32_t) * (size + 1));
        buffer += sizeof(int32_t) * (size + 1);

        size_t copySize = offsetArr[size] * sizeof(char);

        size_t len = copySize;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(UnsafeStringVector::GetValues(charVector)) + cur, s);
            cur += skip_num;
        }
        getResData(UnsafeStringVector::GetValues(charVector), buffer, cur, len - cur);
        buffer += offsetArr[size] * sizeof(char);

        return charVector;
    }

    static void deserializeNulls(BaseVector *baseVector, uint8_t *&buffer,
                                 int32_t size) {
        auto nullData = UnsafeBaseVector::GetNulls(baseVector);
        auto nullByteSize = omniruntime::vec::NullsBuffer::CalculateNbytes(size);

        size_t len = nullByteSize;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(nullData) + cur, s);
            cur += skip_num;
        }
        getResData(nullData, buffer, cur, len - cur);

        buffer += nullByteSize;
    }

    static void deserializeInt64(Vector<int64_t> *vector64, uint8_t *&buffer) {
        int32_t size = vector64->GetSize();

        deserializeNulls(vector64, buffer, size);

        int64_t *data = UnsafeVector::GetRawValues(vector64);

        size_t len = sizeof(int64_t) * size;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(data) + cur, s);
            cur += skip_num;
        }
        getResData(data, buffer, cur, len - cur);
        buffer += sizeof(int64_t) * size;
    }

    static void deserializeInt32(Vector<int32_t> *vector32, uint8_t *&buffer) {
        int32_t size = vector32->GetSize();

        deserializeNulls(vector32, buffer, size);
        int32_t *data = UnsafeVector::GetRawValues(vector32);

        size_t len = sizeof(int32_t) * size;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(data) + cur, s);
            cur += skip_num;
        }
        getResData(data, buffer, cur, len - cur);

        buffer += sizeof(int32_t) * size;
    }

    static void deserializeInt16(Vector<int16_t> *vector16, uint8_t *&buffer) {
        int32_t size = vector16->GetSize();
        deserializeNulls(vector16, buffer, size);
        int16_t *data = UnsafeVector::GetRawValues(vector16);

        size_t len = sizeof(int16_t) * size;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(data) + cur, s);
            cur += skip_num;
        }
        getResData(data, buffer, cur, len - cur);

        buffer += sizeof(int16_t) * size;
    }

    static void deserializeDouble(Vector<double> *vectorDouble,
                                  uint8_t *&buffer)
    {
        int32_t size = vectorDouble->GetSize();
        deserializeNulls(vectorDouble, buffer, size);
        double *data = UnsafeVector::GetRawValues(vectorDouble);

        size_t len = sizeof(double) * size;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(data) + cur, s);
            cur += skip_num;
        }
        getResData(data, buffer, cur, len - cur);

        buffer += sizeof(double) * size;
    }

    static void deserializeBool(Vector<bool> *vectorBool, uint8_t *&buffer)
    {
        int32_t size = vectorBool->GetSize();
        deserializeNulls(vectorBool, buffer, size);
        bool *data = UnsafeVector::GetRawValues(vectorBool);

        size_t len = sizeof(bool) * size;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(data) + cur, s);
            cur += skip_num;
        }
        getResData(data, buffer, cur, len - cur);

        buffer += sizeof(bool) * size;
    }

    static void deserializeDecimal128(Vector<Decimal128> *vectorDecimal128,
                                      uint8_t *&buffer)
    {
        int32_t size = vectorDecimal128->GetSize();
        deserializeNulls(vectorDecimal128, buffer, size);
        Decimal128 *data = UnsafeVector::GetRawValues(vectorDecimal128);

        size_t len = sizeof(Decimal128) * size;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(data) + cur, s);
            cur += skip_num;
        }
        getResData(data, buffer, cur, len - cur);

        buffer += sizeof(Decimal128) * size;
    }

    static Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *
    deserializeStringDictionaryContainerVector(int32_t rowCnt,
                                               uint8_t *&buffer)
    {
        int32_t *values = new int32_t[rowCnt];

        size_t len = sizeof(int32_t) * rowCnt;
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(values) + cur, s);
            cur += skip_num;
        }
        getResData(values, buffer, cur, len - cur);

        buffer += sizeof(int32_t) * rowCnt;

        int32_t dictOffset = 0;
        memcpy(&dictOffset, buffer, sizeof(int32_t));

        buffer += sizeof(int32_t);

        int32_t dictSize = 0;
        memcpy(&dictSize, buffer, sizeof(int32_t));
        buffer += sizeof(int32_t);

        int32_t stringBodySize = 0;
        memcpy(&stringBodySize, buffer, sizeof(int32_t));

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

        memcpy(offset, buffer,
                 sizeof(int32_t) * (dictSize + 1));
        buffer += sizeof(int32_t) * (dictSize + 1);

        // string body
        memcpy(UnsafeStringContainer::GetValues(stringContainer.get()),
                 buffer,
                 offset[dictSize] * sizeof(char));
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

        size_t len = sizeof(long);
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(&timestamp) + cur, s);
            cur += skip_num;
        }
        getResData(&timestamp, buffer, cur, len - cur);

        buffer += sizeof(long);
        return timestamp;
    }
};
}  // namespace omnistream
