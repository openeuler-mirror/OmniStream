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

#include <arm_sve.h>
#include <cstdint>
#include <numeric>
#include <vector>
#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "core/utils/SVEUtil.h"
#include "table/data/vectorbatch/VectorBatchStorageInfo.h"

namespace omnistream {
class VectorBatchUtil {
public:
    static inline int32_t getKeyGroup(ComboId comboId)
    {
        return static_cast<int32_t>((comboId >> 48) & UINT16_MAX);
    }

    static inline uint32_t getSequenceNumber(ComboId comboId)
    {
        return static_cast<uint32_t>((comboId >> 16) & UINT32_MAX);
    }

    static inline int32_t getRowId(ComboId comboId)
    {
        return static_cast<int32_t>(comboId & UINT16_MAX);
    }

    static inline VectorBatchId getVectorBatchId(int32_t keyGroup, uint32_t sequenceNumber)
    {
        if (keyGroup < 0 || keyGroup > UINT16_MAX) {
            THROW_RUNTIME_ERROR("keyGroup out of range");
        }

        auto ukeyGroup = static_cast<uint64_t>(keyGroup);
        auto usequenceNumber = static_cast<uint64_t>(sequenceNumber);
        return (ukeyGroup << 48) | (usequenceNumber << 16);
    }

    static inline VectorBatchId getVectorBatchId(ComboId comboId)
    {
        return (comboId >> 16) << 16;
    }

    static inline ComboId getComboId(int32_t keyGroup, uint32_t sequenceNumber, int32_t rowId)
    {
        auto vectorBatchId = getVectorBatchId(keyGroup, sequenceNumber);

        return getComboId(vectorBatchId, rowId);
    }

    static inline ComboId getComboId(omnistream::VectorBatchId vectorBatchId, int32_t rowId)
    {
        if ((vectorBatchId & UINT16_MAX) != 0) {
            THROW_RUNTIME_ERROR("the rowId part of VectorBatchId should be 0");
        }
        if (rowId < 0 || rowId > UINT16_MAX) {
            THROW_RUNTIME_ERROR("rowId out of range");
        }

        return vectorBatchId | static_cast<uint64_t>(rowId);
    }

    static void decodeComboIds(
        const std::vector<ComboId>& comboIds,
        std::vector<int32_t>& keyGroups,
        std::vector<uint32_t>& sequenceNumbers,
        std::vector<int32_t>& rowIds)
    {
        const auto perCount = static_cast<int32_t>(svcntw());
        const auto halfPerCount = static_cast<int32_t>(svcntd());
        const auto num = static_cast<int32_t>(comboIds.size());

        for (int32_t i = 0; i < num; i += perCount) {
            svbool_t pgLow = svwhilelt_b64_s32(i, num);
            svbool_t pgHigh = svwhilelt_b64_s32(i + halfPerCount, num);
            svbool_t pgOut = svwhilelt_b32_s32(i, num);

            svuint64_t comboIdsLow = svld1_u64(pgLow, comboIds.data() + i);
            svuint64_t comboIdsHigh = svld1_u64(pgHigh, comboIds.data() + i + halfPerCount);

            svuint64_t rowIdsLow = svand_n_u64_x(pgLow, comboIdsLow, UINT16_MAX);
            svuint64_t rowIdsHigh = svand_n_u64_x(pgHigh, comboIdsHigh, UINT16_MAX);

            svuint64_t sequenceNumbersLow = svand_n_u64_x(pgLow, svlsr_n_u64_x(pgLow, comboIdsLow, 16), UINT32_MAX);
            svuint64_t sequenceNumbersHigh = svand_n_u64_x(pgHigh, svlsr_n_u64_x(pgHigh, comboIdsHigh, 16), UINT32_MAX);

            svuint64_t keyGroupsLow = svlsr_n_u64_x(pgLow, comboIdsLow, 48);
            svuint64_t keyGroupsHigh = svlsr_n_u64_x(pgHigh, comboIdsHigh, 48);

            svuint32_t rowIdsPacked = svuzp1_u32(svreinterpret_u32_u64(rowIdsLow), svreinterpret_u32_u64(rowIdsHigh));
            svuint32_t sequenceNumbersPacked =
                svuzp1_u32(svreinterpret_u32_u64(sequenceNumbersLow), svreinterpret_u32_u64(sequenceNumbersHigh));
            svuint32_t keyGroupsPacked =
                svuzp1_u32(svreinterpret_u32_u64(keyGroupsLow), svreinterpret_u32_u64(keyGroupsHigh));

            svst1_s32(pgOut, rowIds.data() + i, svreinterpret_s32_u32(rowIdsPacked));
            svst1_u32(pgOut, sequenceNumbers.data() + i, sequenceNumbersPacked);
            svst1_s32(pgOut, keyGroups.data() + i, svreinterpret_s32_u32(keyGroupsPacked));
        }
    }

    static void getComboId_sve(int32_t keyGroup, uint32_t sequenceNumber, int32_t rowCount, ComboId* result)
    {
        if (keyGroup < 0 || keyGroup > UINT16_MAX) {
            THROW_RUNTIME_ERROR("keyGroup out of range");
        }

        const int32_t processNum = static_cast<int32_t>(svcntd());
        svuint64_t batchData = svorr_x(
            svptrue_b64(),
            svlsl_n_u64_x(svptrue_b64(), svdup_n_u64(static_cast<uint64_t>(keyGroup)), 48),
            svlsl_n_u64_x(svptrue_b64(), svdup_n_u64(static_cast<uint64_t>(sequenceNumber)), 16));
        for (int32_t rowId = 0; rowId < rowCount; rowId += processNum) {
            svbool_t pg = svwhilelt_b64(rowId, rowCount);
            svuint64_t rowData = svreinterpret_u64_s64(svindex_s64(rowId, 1));
            svuint64_t comboIds = svorr_z(pg, batchData, rowData);
            svst1_u64(pg, result + rowId, comboIds);
        }
    }

    // The caller takes ownership of the returned pointer
    static omnistream::VectorBatch* sliceVectorBatch(omnistream::VectorBatch* batch, int32_t offset, int32_t newRowCnt)
    {
        if (batch == nullptr || newRowCnt <= 0) {
            LOG("Warning: split batch count is not valid.");
            return nullptr;
        }

        auto* slicedBatch = new omnistream::VectorBatch(newRowCnt);
        std::vector<int> offsets(batch->GetRowCount());
        std::iota(offsets.begin(), offsets.end(), 0);

        for (int k = 0; k < batch->GetVectorCount(); k++) {
            slicedBatch->Append(
                omniruntime::vec::VectorHelper::CopyPositionsVector(batch->Get(k), offsets.data(), offset, newRowCnt));
        }

        for (int j = 0; j < newRowCnt; j++) {
            slicedBatch->setTimestamp(j, batch->getTimestamp(j + offset));
            slicedBatch->setRowKind(j, batch->getRowKind(j + offset));
        }
        return slicedBatch;
    }

    // The caller takes ownership of the returned pointer
    static omnistream::VectorBatch* buildNewVectorBatchByRowIds(
        omnistream::VectorBatch* vectorBatch, std::vector<int32_t>& rowIds)
    {
        if (vectorBatch == nullptr) {
            THROW_RUNTIME_ERROR("vectorBatch cannot be nullptr");
        }

        auto result = new omnistream::VectorBatch(rowIds.size());
        for (int32_t i = 0; i < vectorBatch->GetVectorCount(); ++i) {
            result->Append(copyVectorByRowIds(vectorBatch, i, rowIds));
        }

        omnistream::SVEUtil::copyU8ArrayByIndices(
            reinterpret_cast<uint8_t*>(vectorBatch->getRowKinds()),
            reinterpret_cast<uint8_t*>(result->getRowKinds()),
            rowIds);

        omnistream::SVEUtil::copyI64ArrayByIndices(vectorBatch->getTimestamps(), result->getTimestamps(), rowIds);
        return result;
    }

    /**
     * Copy selected elements from one vector in a VectorBatch into a new vector by row ids.
     *
     * @param vectorBatch Source VectorBatch that owns the vector to copy from.
     * @param vectorIndex Index of the source vector inside vectorBatch.
     * @param rowIds Row ids to copy from the source vector.
     * @return A newly allocated vector containing the selected elements. The caller takes ownership of the returned
     * pointer.
     */
    static omniruntime::vec::BaseVector* copyVectorByRowIds(
        omnistream::VectorBatch* vectorBatch, int32_t vectorIndex, std::vector<int32_t>& rowIds)
    {
        omniruntime::vec::BaseVector* result = nullptr;
        if (vectorBatch->Get(vectorIndex)->GetEncoding() == omniruntime::vec::OMNI_FLAT ||
            (vectorBatch->Get(vectorIndex)->GetTypeId() != omniruntime::type::OMNI_CHAR &&
             vectorBatch->Get(vectorIndex)->GetTypeId() != omniruntime::type::OMNI_VARCHAR)) {
            // The original vector is not varchar or it is varchar but flat
            result = omniruntime::vec::VectorHelper::CopyPositionsVector(
                vectorBatch->Get(vectorIndex), rowIds.data(), 0, static_cast<int32_t>(rowIds.size()));
        } else {
            // It is a varchar dictionary, copy it out
            result = omnistream::VectorBatch::CopyPositionsAndFlatten(
                vectorBatch->Get(vectorIndex), rowIds.data(), 0, static_cast<int32_t>(rowIds.size()));
        }
        return result;
    }

    // To print VectorBatch, use VectorHelper::PrintVecBatch from OmniOperatorJIT/core/src/vector/vector_helper.h

    template <typename fromType, typename returnType>
    static omniruntime::vec::BaseVector* castVectorTo(omniruntime::vec::BaseVector* vec)
    {
        auto data = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(
            reinterpret_cast<omniruntime::vec::Vector<fromType>*>(vec));
        auto numRows = vec->GetSize();
        returnType* dataCopy = new returnType[numRows];
        for (int i = 0; i < numRows; i++) {
            if constexpr (std::is_same<returnType, Decimal128>::value) {
                dataCopy[i] = Decimal128(static_cast<int128_t>(data[i]));
            } else {
                dataCopy[i] = static_cast<returnType>(data[i]);
            }
        }
        omniruntime::vec::Vector<returnType>* newVector = new omniruntime::vec::Vector<returnType>(numRows);
        for (int32_t i = 0; i < numRows; i++) {
            newVector->SetValue(i, dataCopy[i]);
        }
        delete[] dataCopy;
        return newVector;
    }

    static std::string getValueAtAsStr(
        omnistream::VectorBatch* vecBatch, int32_t col, int32_t row, std::vector<std::string> inputTypes = {})
    {
        std::string value;
        if (vecBatch->Get(col)->IsNull(row)) {
            value = "NULL";
        } else {
            int dataId = vecBatch->Get(col)->GetTypeId();
            switch (dataId) {
                case omniruntime::type::DataTypeId::OMNI_LONG:
                    if (static_cast<int32_t>(inputTypes.size()) > col && inputTypes[col].substr(0, 9) == "TIMESTAMP") {
                        auto millis =
                            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(col))->GetValue(row);
                        return formatTimestamp(millis);
                    } else {
                        value = std::to_string(
                            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(col))->GetValue(row));
                    }
                    break;
                case omniruntime::type::DataTypeId::OMNI_CHAR: {
                    if (vecBatch->Get(col)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                        auto casted = reinterpret_cast<
                            omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                            vecBatch->Get(col));
                        value = casted->GetValue(row);
                    } else {
                        auto casted = reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<
                            std::string_view,
                            omniruntime::vec::LargeStringContainer>>*>(vecBatch->Get(col));
                        value = casted->GetValue(row);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                    value = std::to_string(
                        reinterpret_cast<omniruntime::vec::Vector<double>*>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_INT:
                    value = std::to_string(
                        reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                    value = reinterpret_cast<omniruntime::vec::Vector<bool>*>(vecBatch->Get(col))->GetValue(row)
                                ? "true"
                                : "false";
                    break;
                case omniruntime::type::DataTypeId::OMNI_DECIMAL64:
                    value = std::to_string(
                        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                    value = reinterpret_cast<omniruntime::vec::Vector<Decimal128>*>(vecBatch->Get(col))
                                ->GetValue(row)
                                .ToString();
                    break;
                default: value = "Unsupported Data Type"; break;
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
} // namespace omnistream
