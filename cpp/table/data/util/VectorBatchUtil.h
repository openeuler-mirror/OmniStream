#ifndef FLINK_TNEL_VECTOR_BATCH_UTIL_H
#define FLINK_TNEL_VECTOR_BATCH_UTIL_H

#include "table/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include <arm_sve.h>

class VectorBatchUtil
{
public:
    static inline int32_t getBatchId(int64_t id)
    {
        return (int32_t)(id >> 32);
    }

    static inline int32_t getRowId(int64_t id)
    {
        return (int32_t)id;
    }

    static inline int64_t getComboId(int batchId, int rowId)
    {
        return ((int64_t)batchId << 32) | rowId;
    }

    static void deComboIDSVE(uint64_t* src, uint32_t* batchIDdst, uint32_t* rowIDdst, int num)
    {
        int processNum = 8;
        int half = 4;
        for (int i = 0; i < num; i+=processNum) {
            svbool_t pg = svwhilelt_b64(i, num);
            svbool_t pg2 = svwhilelt_b64(i + half, num);
            svbool_t pg3 = svwhilelt_b32(i, num);
            svuint64_t comboID = svld1(pg, src + i);
            svuint64_t comboID2 = svld1(pg2, src + i + half);

            svuint32_t rowID = svuzp1(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));
            svuint32_t batchID = svuzp2(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));

            svst1_u32(pg3, rowIDdst + i, rowID);
            svst1_u32(pg3, batchIDdst + i, batchID);
        }
    }

    static void getComboId_sve(int batchId, int rowCount, int64_t* result) {
        int processNum = 4;
        svint64_t batchData = svlsl_n_s64_x(svptrue_b64(), svdup_n_s64(batchId), 32);
        for (int i = 0; i < rowCount; i+= processNum) {
            svbool_t pg = svwhilelt_b64(i, rowCount);
            svint64_t rowData = svindex_s64(i, 1);
            svint64_t comboIDs = svorr_z(pg, batchData, rowData);
            svst1_s64(pg, result + i, comboIDs);
        }
    }

    // To print VectorBatch, use VectorHelper::PrintVecBatch from OmniOperatorJIT/core/src/vector/vector_helper.h

    template <typename fromType, typename returnType>
    static omniruntime::vec::BaseVector *castVectorTo(omniruntime::vec::BaseVector *vec)
    {
        auto data = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(reinterpret_cast<omniruntime::vec::Vector<fromType> *>(vec));
        auto numRows = vec->GetSize();
        returnType *dataCopy = new returnType[numRows];
        for (int i = 0; i < numRows; i++)
        {
            if constexpr (std::is_same<returnType, Decimal128>::value)
            {
                dataCopy[i] = Decimal128(static_cast<int128_t>(data[i]));
            }
            else
            {
                dataCopy[i] = static_cast<returnType>(data[i]);
            }
        }
        omniruntime::vec::Vector<returnType> *newVector = new omniruntime::vec::Vector<returnType>(numRows);
        for (int32_t i = 0; i < numRows; i++)
        {
            newVector->SetValue(i, dataCopy[i]);
        }
        return newVector;
    }

    static std::string getValueAtAsStr(omnistream::VectorBatch *vecBatch, int32_t col, int32_t row, std::vector<std::string> inputTypes={})
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
