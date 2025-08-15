#pragma once

#include <cstdint>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/vector/vector.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "table/vectorbatch/VectorBatch.h"
#include "include/functions/StreamElement.h"
#include <arm_sve.h>

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::vec::unsafe;
using namespace std;

namespace omnistream {

struct SerializedBatchInfo {
    uint8_t *buffer;
    int32_t size;
    int event = -1;
};

// we assume all the char ana varchar data are stored
// vector<LargeStringContainer>, other primitive data types are stored in
// vector<xxx>
class VectorBatchSerializationUtils {
public:
    static void getResData(void* dst, void* src, size_t cur, int res)
    {
        svbool_t pg = svwhilelt_b8(0, res);
        svuint8_t s = svld1(pg, reinterpret_cast<uint8_t*>(src) + cur);
        svst1(pg, reinterpret_cast<uint8_t*>(dst) + cur, s);
    }

    static SerializedBatchInfo serializeVectorBatch(VectorBatch* vectorBatch, int32_t batchSize, uint8_t*& buffer)
    {
        errno_t ret;
        int32_t vectorCount = vectorBatch->GetVectorCount();
        int32_t rowCnt = vectorBatch->GetRowCount();
        uint8_t *original = buffer;
        int8_t dataType = static_cast<int>(StreamElementTag::VECTOR_BATCH);
        // dataType--batchSize--vectorCount--rowCnt

        size_t len = sizeof(int8_t);
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&dataType) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, &dataType, cur, len - cur);
        buffer += sizeof(int8_t);

        memcpy(buffer, &batchSize, sizeof(batchSize));
        buffer += sizeof(batchSize);

        memcpy(buffer, &vectorCount, sizeof(vectorCount));
        buffer += sizeof(vectorCount);

        memcpy(buffer, &rowCnt, sizeof(rowCnt));
        buffer += sizeof(rowCnt);

        // serialize timestamp and rowKinds
        serializeTimestampAndRowKinds(vectorBatch, buffer, batchSize);

        // serialize vector data
        for (int idx = 0; idx < vectorCount; idx++) {
            BaseVector *colVector = vectorBatch->Get(idx);
            DataTypeId dataType = colVector->GetTypeId();
            Encoding encoding = colVector->GetEncoding();
            if (encoding == OMNI_FLAT) {
                if (dataType == OMNI_CHAR || dataType == OMNI_VARCHAR) {
                    serializeCharVector(colVector, buffer, batchSize);
                } else {
                    serializePrimitiveVector(colVector, buffer, batchSize);
                }
            } else if (encoding == OMNI_DICTIONARY) {
                if (dataType == OMNI_CHAR || dataType == OMNI_VARCHAR) {
                    serializeStringDictionaryContainerVector(colVector, buffer,
                                                             batchSize);
                } else {
                    throw std::runtime_error("Encoding not supported");
                }
            } else {
                throw std::runtime_error("Encoding not supported");
            }
        }
        return {original, batchSize};
    }

    static void serializeTimestampAndRowKinds(VectorBatch *vectorBatch,
                                              uint8_t *&buffer,
                                              int32_t bufferSize) {
        int32_t rowCnt = vectorBatch->GetRowCount();
        int64_t *timestamps = vectorBatch->getTimestamps();
        RowKind *rowKinds = vectorBatch->getRowKinds();

        size_t len = sizeof(int64_t) * rowCnt;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(timestamps) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, timestamps, cur, len - cur);

        buffer += sizeof(int64_t) * rowCnt;
        memcpy(buffer, rowKinds, sizeof(RowKind) * rowCnt);
        buffer += sizeof(RowKind) * rowCnt;
    }

    static int32_t calculateVectorBatchSerializableSize(
        VectorBatch *vectorBatch) {
        int32_t vectorCount = vectorBatch->GetVectorCount();
        int32_t rowCnt = vectorBatch->GetRowCount();
        // dataType--batchSize--vectorCount--rowCnt
        int32_t headerSize =
            sizeof(byte)+ sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t);
        int32_t timestampsSize = rowCnt * sizeof(int64_t);
        int32_t rowKindsSize = rowCnt * sizeof(RowKind);
        int32_t totalSize = headerSize + timestampsSize + rowKindsSize;

        for (int32_t i = 0; i < vectorCount; i++) {
            BaseVector *columnVector = vectorBatch->Get(i);
            totalSize += calculateVectorSerializableSize(columnVector);
        }
        return totalSize;
    }

    static int32_t calculateVectorSerializableSize(BaseVector *baseVector) {
        int32_t totalSize = 0;

        int32_t size = baseVector->GetSize();
        DataTypeId dataType = baseVector->GetTypeId();

        Encoding encoding = baseVector->GetEncoding();
        // vectorSize--encoding--dataType
        int32_t vectorHeaderSize =
            sizeof(int32_t) + sizeof(int8_t) + sizeof(int8_t);

        int32_t nullByteSize = size * sizeof(bool);
        totalSize += nullByteSize + vectorHeaderSize;
        int32_t dataSize = 0;

        if (encoding == OMNI_FLAT) {
            if (dataType == OMNI_VARCHAR || dataType == OMNI_CHAR) {
                dataSize = calculateCharDataSize(baseVector);
            } else {
                dataSize = calculatePrimitiveDataSize(baseVector);
            }
        } else if (encoding == OMNI_DICTIONARY) {
            // todo should only exist for string data type
            dataSize = calculateStringDictionarySerializableSize(baseVector);
        } else {
            // todo
            throw std::runtime_error("Encoding not supported");
        }

        totalSize += dataSize;

        return totalSize;
    }

    // dataPayload
    static int32_t calculatePrimitiveDataSize(BaseVector *baseVector) {
        DataTypeId dataType = baseVector->GetTypeId();
        int32_t size = baseVector->GetSize();

        switch (dataType) {
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                return sizeof(int64_t) * size;

            case OMNI_INT:
            case OMNI_DATE32:
                return sizeof(int32_t) * size;

            case OMNI_SHORT:
                return sizeof(int16_t) * size;

            case OMNI_DOUBLE:
                return sizeof(double) * size;
            case OMNI_BOOLEAN:
                return sizeof(bool) * size;
            case OMNI_DECIMAL128:
                return sizeof(Decimal128) * size;

            default:
                LOG("Data type not supported: " << std::to_string(dataType))
                throw std::runtime_error("Data type not supported");
        }
    }

    // stringDataSize--offsetArray--stringData
    static int32_t calculateCharDataSize(BaseVector *baseVector) {
        Vector<LargeStringContainer<std::string_view>> *charVector =
            dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(
                baseVector);
        if (charVector == nullptr) {
            throw std::runtime_error(
                "Failed to cast BaseVector to "
                "Vector<LargeStringContainer<std::string_view>>");
        }

        int32_t rowCount = baseVector->GetSize();
        int32_t stringBodySizeField = sizeof(int32_t);
        int32_t offsetSize = (rowCount + 1) * sizeof(int32_t);
        int32_t dataSize = 0;
        std::shared_ptr<LargeStringContainer<std::string_view>>
            stringContainer = UnsafeStringVector::GetContainer(charVector);
        int32_t *offsetArr =
            UnsafeStringContainer::GetOffsets(stringContainer.get());

        dataSize = offsetArr[rowCount];
        return stringBodySizeField + offsetSize + dataSize * sizeof(char);
    }

    static void serializePrimitiveVector(BaseVector *baseVector,
                                         uint8_t *&buffer, int32_t bufferSize) {
        DataTypeId dataType = baseVector->GetTypeId();
        int32_t rowCount = baseVector->GetSize();
        serializeVectorBatchHeader(baseVector, buffer, bufferSize);
        bool *nullData = UnsafeBaseVector::GetNulls(baseVector);

        size_t len = sizeof(bool) * rowCount;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(nullData) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, nullData, cur, len - cur);

        buffer += sizeof(bool) * rowCount;

        switch (dataType) {
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_TIME64:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                serializeInt64(baseVector, buffer, bufferSize);
                break;

            case OMNI_INT:
            case OMNI_DATE32:
                serializeInt32(baseVector, buffer, bufferSize);
                break;

            case OMNI_SHORT:
                serializeInt16(baseVector, buffer, bufferSize);
                break;

            case OMNI_DOUBLE:
                serializeDouble(baseVector, buffer, bufferSize);
                break;
            case OMNI_BOOLEAN:
                serializeBool(baseVector, buffer, bufferSize);
                break;
            case OMNI_DECIMAL128:
                serializeDecimal128(baseVector, buffer, bufferSize);
                break;
            default:
                LOG("Data type not supported: " << std::to_string(dataType))
                throw std::runtime_error("Data type not supported");
        }
    }

    static void serializeCharVector(BaseVector *baseVector, uint8_t *&buffer,
                                    int32_t bufferSize) {
        serializeVectorBatchHeader(baseVector, buffer, bufferSize);

        Vector<LargeStringContainer<std::string_view>> *charVector =
            dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(
                baseVector);
        std::shared_ptr<LargeStringContainer<std::string_view>>
            stringContainer = UnsafeStringVector::GetContainer(charVector);
        int32_t *offsetArr =
            UnsafeStringContainer::GetOffsets(stringContainer.get());

        int32_t rowCount = baseVector->GetSize();

        // real data size
        int32_t stringBodySize = offsetArr[charVector->GetSize()];

        size_t len = sizeof(int32_t);
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&stringBodySize) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, &stringBodySize, cur, len - cur);

        buffer += sizeof(int32_t);

        // nullData
        bool *nullData = UnsafeBaseVector::GetNulls(baseVector);
        memcpy(buffer, nullData, sizeof(bool) * rowCount);
        buffer += sizeof(bool) * rowCount;

        // offset array
        memcpy(buffer, offsetArr,
                       sizeof(int32_t) * (rowCount + 1));
        buffer += sizeof(int32_t) * (rowCount + 1);

        // real data
        std::string dataStr(
            UnsafeStringContainer::GetStringBufferAddr(stringContainer.get()),
            stringBodySize);
        memcpy(buffer, dataStr.data(),
                       stringBodySize * sizeof(char));
        buffer += stringBodySize * sizeof(char);
    }

    static void serializeInt64(BaseVector *baseVector, uint8_t *&buffer,
                               int32_t bufferSize) {
        Vector<int64_t> *vector64 = static_cast<Vector<int64_t> *>(baseVector);
        int64_t *data = UnsafeVector::GetRawValues(vector64);
        int32_t size = sizeof(int64_t) * baseVector->GetSize();

        size_t len = size;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(data) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, data, cur, len - cur);

        buffer += size;
    }

    static void serializeInt32(BaseVector *baseVector, uint8_t *&buffer,
                               int32_t bufferSize) {
        Vector<int32_t> *vector32 = static_cast<Vector<int32_t> *>(baseVector);
        int32_t *data = UnsafeVector::GetRawValues(vector32);
        int32_t size = sizeof(int32_t) * baseVector->GetSize();

        size_t len = size;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(data) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, data, cur, len - cur);

        buffer += size;
    }

    static void serializeInt16(BaseVector *baseVector, uint8_t *&buffer,
                               int32_t bufferSize) {
        Vector<int16_t> *vector16 = static_cast<Vector<int16_t> *>(baseVector);
        int16_t *data = UnsafeVector::GetRawValues(vector16);
        int32_t size = sizeof(int16_t) * baseVector->GetSize();

        size_t len = size;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(data) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, data, cur, len - cur);

        buffer += size;
    }
    static void serializeDouble(BaseVector *baseVector, uint8_t *&buffer,
                                int32_t bufferSize) {
        Vector<double> *vectorDouble =
            static_cast<Vector<double> *>(baseVector);
        double *data = UnsafeVector::GetRawValues(vectorDouble);
        int32_t size = sizeof(double) * baseVector->GetSize();

        size_t len = size;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(data) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, data, cur, len - cur);

        buffer += size;
    }

    static void serializeBool(BaseVector *baseVector, uint8_t *&buffer,
                              int32_t bufferSize) {
        Vector<bool> *vectorBool = static_cast<Vector<bool> *>(baseVector);
        bool *data = UnsafeVector::GetRawValues(vectorBool);
        int32_t size = sizeof(bool) * baseVector->GetSize();

        size_t len = size;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(data) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, data, cur, len - cur);
        buffer += size;
    }

    static void serializeDecimal128(BaseVector *baseVector, uint8_t *&buffer,
                                    int32_t bufferSize) {
        Vector<Decimal128> *vectorDecimal128 =
            static_cast<Vector<Decimal128> *>(baseVector);
        Decimal128 *data = UnsafeVector::GetRawValues(vectorDecimal128);
        int32_t size = sizeof(Decimal128) * baseVector->GetSize();

        size_t len = size;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(data) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, data, cur, len - cur);
        buffer += size;
    }

    static int32_t calculateStringDictionarySerializableSize(
        BaseVector *baseVector) {
        int32_t stringDictionarySize = 0;
        auto *vec_string_dict_container = reinterpret_cast<Vector<
            DictionaryContainer<std::string_view, LargeStringContainer>> *>(
            baseVector);
        if (!vec_string_dict_container) {
            throw std::runtime_error("Encoding not supported");
        }
        int32_t size = baseVector->GetSize();

        int32_t valueIndexSize = sizeof(int32_t) * size;
        stringDictionarySize += valueIndexSize;

        // dictOffSetSize --- dictSize
        stringDictionarySize += sizeof(int32_t) + sizeof(int32_t);

        std::shared_ptr<DictionaryContainer<std::string_view>>
            string_dictionary = static_cast<
                std::shared_ptr<DictionaryContainer<std::string_view>>>(
                unsafe::UnsafeDictionaryVector::GetDictionaryOriginal(
                    vec_string_dict_container));
        std::shared_ptr<LargeStringContainer<std::string_view>>
            stringContainer =
                unsafe::UnsafeDictionaryContainer::GetStringDictionaryOriginal(
                    string_dictionary.get());
        int32_t dictSize = unsafe::UnsafeDictionaryContainer::GetDictSize(
            string_dictionary.get());
        int bodySize =
            calculateLargeStringContainerDataSize(stringContainer, dictSize);
        stringDictionarySize += bodySize;

        return stringDictionarySize;
    }

    static int32_t calculateLargeStringContainerDataSize(
        std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer,
        int32_t rowCount) {
        int32_t stringBodySizeField = sizeof(int32_t);
        int32_t offsetSize = (rowCount + 1) * sizeof(int32_t);
        int32_t dataSize = 0;
        int32_t *offsetArr =
            UnsafeStringContainer::GetOffsets(stringContainer.get());
        dataSize = offsetArr[rowCount];
        return stringBodySizeField + offsetSize + dataSize * sizeof(char);
    }

    static void serializeStringDictionaryContainerVector(BaseVector *baseVector,
                                                         uint8_t *&buffer,
                                                         int32_t bufferSize) {
        serializeVectorBatchHeader(baseVector, buffer, bufferSize);

        serializeStringDictionaryBody(baseVector, buffer, bufferSize);
    }

    static void serializeVectorBatchHeader(BaseVector *baseVector,
                                           uint8_t *&buffer,
                                           int32_t bufferSize)
    {
        int32_t vectorSize = calculateVectorSerializableSize(baseVector);

        size_t len = sizeof(int32_t);
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&vectorSize) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, &vectorSize, cur, len - cur);
        buffer += sizeof(int32_t);

        Encoding encoding = baseVector->GetEncoding();
        int8_t codingNumeric = static_cast<int8_t>(encoding);
        memcpy(buffer, &codingNumeric, sizeof(int8_t));
        buffer += sizeof(int8_t);

        DataTypeId dataType = baseVector->GetTypeId();
        int8_t numericValue = static_cast<int8_t>(dataType);
        memcpy(buffer,&numericValue, sizeof(int8_t));
        buffer += sizeof(int8_t);
    }

    static void serializeStringDictionaryBody(BaseVector *baseVector,
                                              uint8_t *&buffer,
                                              int32_t bufferSize)
    {
        auto *vec_string_dict_container = reinterpret_cast<Vector<
            DictionaryContainer<std::string_view, LargeStringContainer>> *>(
            baseVector);
        std::shared_ptr<DictionaryContainer<std::string_view>>
            string_dictionary = static_cast<
                std::shared_ptr<DictionaryContainer<std::string_view>>>(
                unsafe::UnsafeDictionaryVector::GetDictionaryOriginal(
                    vec_string_dict_container));
        std::shared_ptr<LargeStringContainer<std::string_view>>
            stringContainer =
                unsafe::UnsafeDictionaryContainer::GetStringDictionaryOriginal(
                    string_dictionary.get());

        // get value size
        int32_t valueSize = baseVector->GetSize();
        // get values
        int32_t *valueIndex =
            unsafe::UnsafeDictionaryContainer::GetIds(string_dictionary.get());

        size_t len = sizeof(int32_t) * valueSize;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(valueIndex) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, valueIndex, cur, len - cur);
        buffer += sizeof(int32_t) * valueSize;

        // get dict offset
        int32_t dictOffset = unsafe::UnsafeDictionaryContainer::GetDictOffset(
            string_dictionary.get());
        memcpy(buffer, &dictOffset, sizeof(int32_t));
        buffer += sizeof(int32_t);

        // get dict size
        int32_t dictSize = unsafe::UnsafeDictionaryContainer::GetDictSize(
            string_dictionary.get());
        memcpy(buffer, &dictSize, sizeof(int32_t));
        buffer += sizeof(int32_t);

        int32_t *offsetArr =
            UnsafeStringContainer::GetOffsets(stringContainer.get());

        int32_t rowCount = unsafe::UnsafeDictionaryContainer::GetDictSize(
            string_dictionary.get());

        // real data size
        int32_t stringBodySize = offsetArr[rowCount];
        memcpy(buffer, &stringBodySize, sizeof(int32_t));
        buffer += sizeof(int32_t);
        serializeStringDictionaryTail(baseVector, buffer, bufferSize, valueSize,
                                      offsetArr, rowCount, stringBodySize,
                                      stringContainer);
    }

    static void serializeStringDictionaryTail(
        BaseVector *baseVector, uint8_t *&buffer, int32_t bufferSize,
        int32_t valueSize, int32_t *offsetArr, int32_t rowCount,
        int32_t stringBodySize,
        std::shared_ptr<LargeStringContainer<std::string_view>>
            stringContainer)
    {
        // nullData
        bool *nullData = UnsafeBaseVector::GetNulls(baseVector);

        size_t len = sizeof(bool) * valueSize;
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(nullData) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, nullData, cur, len - cur);
        buffer += sizeof(bool) * valueSize;

        // offset array
        memcpy(buffer, offsetArr,
                       sizeof(int32_t) * (rowCount + 1));
        buffer += sizeof(int32_t) * (rowCount + 1);

        // real data
        std::string dataStr(
            UnsafeStringContainer::GetStringBufferAddr(stringContainer.get()),
            stringBodySize);
        memcpy(buffer, dataStr.data(),
                       stringBodySize * sizeof(char));
        buffer += stringBodySize * sizeof(char);
    }

    static void SerializWatermark(long timestamp, int32_t dataSize, uint8_t*& buffer)
    {
        errno_t ret;
        int dataType = static_cast<int>(StreamElementTag::TAG_WATERMARK);
        // dataType--timestamp
        size_t len = sizeof(int8_t);
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&dataType) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, &dataType, cur, len - cur);

        buffer += 1;
        memcpy(buffer, &timestamp, sizeof(timestamp));
        buffer += sizeof(timestamp);
    }

    static void SerializElementNum(int32_t elementNumber, uint8_t* buffer)
    {
        size_t len = sizeof(int32_t);
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(&elementNumber) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur, s);
            cur += skip_num;
        }
        getResData(buffer, &elementNumber, cur, len - cur);
    }
};

}  // namespace omnistream
