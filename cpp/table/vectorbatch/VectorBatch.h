//
// Created by xichen on 1/21/25.
//

#ifndef FLINK_TNEL_VECTORBATCH_H
#define FLINK_TNEL_VECTORBATCH_H
#include <fstream>
#include <iostream>
#include <vector/vector_helper.h>
#include <arm_sve.h>

#include "OmniOperatorJIT/core/src/vector/vector_batch.h"
#include <xxhash.h>
#include "table/RowKind.h"
#include "core/include/common.h"
#include "data/RowData.h"
using namespace omniruntime::type;
namespace omnistream {
class VectorBatch : public omniruntime::vec::VectorBatch {
public:
    explicit VectorBatch(size_t rowCnt);

    static void getResData(void* dst, void* src, size_t cur, int res)
    {
        svbool_t pg = svwhilelt_b8(0, res);
        svuint8_t s = svld1(pg, reinterpret_cast<uint8_t*>(src) + cur);
        svst1(pg, reinterpret_cast<uint8_t*>(dst) + cur, s);
    }

    // construct a new vectorbatch with ort vectorbatch
    VectorBatch(omniruntime::vec::VectorBatch* baseVecBatch, int64_t* timestamps, RowKind* rowkinds);

    ~VectorBatch();

    void setTimestamp(size_t rowIndex, int64_t timestampValue) {
        timestamps[rowIndex] = timestampValue;
    }

    void setTimestamps(size_t start, int64_t* data, size_t len) {
        if (data == nullptr) {
            return;
        }
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(data) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(timestamps) + cur, s);
            cur += skip_num;
        }
        getResData(timestamps, data, cur, len - cur);
        // memcpy_s(timestamps, len, data, len);
    }

    int64_t getTimestamp(size_t rowIndex){
        return timestamps[rowIndex];
    }

    int64_t* getTimestamps() {
        return timestamps;
    }

    void setRowKind(size_t rowIndex, RowKind rowKind) {
        rowKinds[rowIndex] = rowKind;
    }
    void setRowKinds(size_t start, RowKind* data, size_t len) {
        if (data == nullptr) {
            return;
        }

        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(data) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(rowKinds) + cur, s);
            cur += skip_num;
        }
        getResData(rowKinds, data, cur, len - cur);

        // memcpy_s(rowKinds, len, data, len);
    }
    RowKind getRowKind(size_t rowIndex) {
        return rowKinds[rowIndex];
    }

    RowKind* getRowKinds() {
        return rowKinds;
    }

    RowData* extractRowData(int rowIndex);

    [[nodiscard]] std::vector<XXH128_hash_t> getXXH128s();
    bool isEmpty(int64_t currentTimestamp) const{
        return maxTimestamp != INT64_MIN && currentTimestamp > maxTimestamp;
    }

    void setMaxTimestamp(int col);

    // adjust the position of columns, achieving a simple column projection
    void RearrangeColumns(std::vector<int32_t>& inputIndices);

    template <typename Type>
    Type GetValueAt(int32_t column, int32_t row)
    {
        return reinterpret_cast<omniruntime::vec::Vector<Type> *>(this->Get(column))->GetValue(row);
    }

    template <typename Type>
    void SetValueAt(int32_t column, int32_t row, Type value)
    {
        reinterpret_cast<omniruntime::vec::Vector<Type> *>(this->Get(column))->SetValue(row, value);
    }
   // write to file. The default mode is to open a new one
    void writeToFile(const std::string& filename, std::ios_base::openmode mode = std::ios::out,
                     std::vector<std::pair<int32_t, int32_t>> decimalInfo = {},
                     std::vector<std::string> inputTypes = {}) const;

    std::string TransformTime(int vectorID, int rowID) const;
    std::string transformDecimal(
            int vectorID, int rowID, std::vector<std::pair<int32_t, int32_t>>& decimalInfo) const;
    void WriteToFileInternal(int vectorID, int rowID,
                             std::ofstream& file,
                             std::vector<std::pair<int32_t, int32_t>> decimalInfo,
                             std::vector<std::string> inputTypes) const;
    void WriteString(std::ofstream& file, int vectorID, int rowID) const;
    VectorBatch* copy() {
        auto value = this;
        int32_t rowCount = value->GetRowCount();
        auto copiedVectorBatch = new VectorBatch(rowCount);
        int32_t vectorCount = value->GetVectorCount();

        std::vector<int> offsets(rowCount);
        std::iota(offsets.begin(), offsets.end(), 0);

        for (int i = 0; i < vectorCount; i++) {
            copiedVectorBatch->Append(omniruntime::vec::VectorHelper::CopyPositionsVector(
                value->Get(i), offsets.data(), 0, offsets.size()));
        }
        return copiedVectorBatch;
    }
    static omniruntime::vec::BaseVector* CopyPositionsAndFlatten(omniruntime::vec::BaseVector* input,
         const int *positions, int offset, int length);

    static omnistream::VectorBatch* CreateVectorBatch(int rowCnt, const std::vector<DataTypeId>& dataTypes);
    private:
        int64_t* timestamps;
        RowKind* rowKinds;
        int64_t maxTimestamp;
    };
}

#endif //FLINK_TNEL_VECTORBATCH_H
