#include "VectorBatch.h"
#include <fstream>
#include "data/binary/BinaryRowData.h"
#include "table/rowdata_marshaller.h"
namespace omnistream {
    VectorBatch::VectorBatch(size_t rowCnt)
        : omniruntime::vec::VectorBatch(rowCnt), timestamps(nullptr), rowKinds(nullptr), maxTimestamp(INT64_MIN) {
        if(rowCnt > 0) {
            timestamps = new int64_t[rowCnt];
            memset_s(timestamps, sizeof(int64_t) * rowCnt, 0, sizeof(int64_t) * rowCnt);

            rowKinds = new RowKind[rowCnt];
            memset_s(rowKinds, sizeof(RowKind) * rowCnt, 0, sizeof(RowKind) * rowCnt);
        }
    }

    VectorBatch::~VectorBatch() {
        delete[] timestamps;
        delete[] rowKinds;
    }

    VectorBatch::VectorBatch(omniruntime::vec::VectorBatch *baseVecBatch, int64_t *timestamps, RowKind *rowkinds) :
    omniruntime::vec::VectorBatch(baseVecBatch->GetRowCount()) {
        auto baseVectors = baseVecBatch->GetVectors();
        this->vectors.insert(this->vectors.end(), baseVectors, baseVectors + baseVecBatch->GetVectorCount());
        this->rowKinds = rowkinds;
        this->timestamps = timestamps;
    }
    void VectorBatch::setMaxTimestamp(int colIdx)
    {
        omniruntime::vec::Vector<int64_t>* col = reinterpret_cast<omniruntime::vec::Vector<int64_t>* >(this->Get(colIdx));
        for (int i = 0; i < this->GetRowCount(); i++) {
            maxTimestamp = std::max(maxTimestamp, col->GetValue(i));
        }
    }

    void VectorBatch::RearrangeColumns(std::vector<int32_t> &inputIndices)
    {
        LOG("=====>");
        std::vector<bool> toKeep(this->vectors.size(), false);
        // Move column to its new position
        std::vector<omniruntime::vec::BaseVector*> newVectors(inputIndices.size());
        for (size_t i = 0; i < inputIndices.size(); i++) {
            newVectors[i] = this->vectors[inputIndices[i]];
            toKeep[inputIndices[i]] = true;
        }
        // remove vectors(cols) that are no longer needed
        for (size_t i = 0; i < toKeep.size(); i++) {
            if(!toKeep[i]) {
                delete vectors[i];
            }
        }
        this->vectors = newVectors;
    }

    RowData* VectorBatch::extractRowData(int rowIndex) {
        if (rowIndex > this->GetRowCount()) {
            return nullptr;
        }
        // Get the number of columns in the batch.
        int numColumns = this->GetVectorCount();
        // Create a new BinaryRowData with numColumns fields.
        // BinaryRowData is assumed to implement the RowData interface.
        BinaryRowData* outRow = BinaryRowData::createBinaryRowDataWithMem(numColumns);

        for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
            auto col = this->Get(colIndex);
            if (col->IsNull(colIndex)) {
                outRow->setNullAt(colIndex);
                continue;
            }
            auto typeId = col->GetTypeId();
            // It is a omniruntime type
            if (typeId < OMNI_INVALID && rowSerializerCenter[typeId] != nullptr) {
                rowSerializerCenter[typeId](col, rowIndex, outRow, colIndex);
            } else if (typeId == DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE) {
                rowSerializerCenter[OMNI_LONG](col, rowIndex, outRow, colIndex);
            } else {
                throw std::runtime_error("Data type not supported");
            }
        }
        // Set the row kind using the row kinds stored in the batch.
        outRow->setRowKind(this->getRowKind(rowIndex));
        return outRow;
    }

    std::string removeTrailingZeros(std::string num) {
        size_t dot_pos = num.find('.');
        if (dot_pos == std::string::npos) {
            return num;
        }

        std::string integer_part = num.substr(0, dot_pos);
        std::string decimal_part = num.substr(dot_pos + 1);

        // 去除小数部分末尾的零
        size_t last_non_zero = decimal_part.find_last_not_of('0');
        if (last_non_zero != std::string::npos) {
            decimal_part = decimal_part.substr(0, last_non_zero + 1);
        } else {
            decimal_part.clear();
        }

        // 处理整数部分为空的情况（如输入是 ".500" → 转为 "0.500"）
        if (integer_part.empty()) {
            integer_part = "0";
        }

        // 组合结果
        if (decimal_part.empty()) {
            return integer_part;
        } else {
            return integer_part + "." + decimal_part;
        }
    }

    std::string VectorBatch::TransformTime(int vectorID, int rowID) const
    {
        auto millis = reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(vectors[vectorID])->GetValue(rowID);
        int64_t adjusted_seconds = (millis >= 0) ? (millis / 1000) : ((millis - 999) / 1000);
        int milliseconds = millis % 1000;
        if (milliseconds < 0) {
            const int addTime = 1000;
            milliseconds += addTime; // 确保毫秒非负（如-1234ms → -2秒 + 766ms）
        }

        struct tm timeinfo;
        gmtime_r(&adjusted_seconds, &timeinfo);

        // 格式化为字符串
        char buffer[80];
        strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &timeinfo);

        std::ostringstream oss;
        oss << buffer
            << "."
            << std::setw(3) << std::setfill('0')  // 强制3位宽度，不足补零
            << milliseconds;

        std::string result = oss.str();
        return result;
    }

    std::string VectorBatch::transformDecimal(int vectorID,
                                              int rowID, std::vector<std::pair<int32_t, int32_t>>& decimalInfo) const
    {
        std::string valueStr = (reinterpret_cast<omniruntime::vec::Vector<Decimal128> *>(vectors[vectorID])->GetValue(
            rowID)).ToString();
        if (static_cast<int>(decimalInfo.size()) > vectorID && decimalInfo[vectorID].second > 0) {
            int32_t scale = decimalInfo[vectorID].second;
            int len = valueStr.length();
            // Case when scale is greater than or equal to the number length
            if (scale >= len) {
                valueStr = "0." + std::string(scale - len, '0') + valueStr;
            } else {
                // Insert the decimal point at the correct position
                valueStr = valueStr.substr(0, len - scale) + "." + valueStr.substr(len - scale);
            }
        }
        return valueStr;
    }

    void VectorBatch::WriteString(std::ofstream& file, int vectorID, int rowID) const
    {
        if (vectors[vectorID]->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
            auto casted = reinterpret_cast<omniruntime::vec::Vector
                    <omniruntime::vec::LargeStringContainer<std::string_view>> *>(vectors[vectorID]);
            file << casted->GetValue(rowID);
        } else { // DICTIONARY
            auto casted =
                    reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<
                            std::string_view, omniruntime::vec::LargeStringContainer>> *>(vectors[vectorID]);
            file << casted->GetValue(rowID);
        }
    }


    void VectorBatch::WriteToFileInternal(int vectorID, int rowID,
                                          std::ofstream& file,
                                          std::vector<std::pair<int32_t, int32_t>> decimalInfo,
                                          std::vector<std::string> inputTypes) const
    {
        int dataId = vectors[vectorID]->GetTypeId();
        switch (dataId) {
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_LONG:
                LOG("vb writefile inputType is " << inputTypes[vectorID])
                if (inputTypes[vectorID].substr(0, 9) == "TIMESTAMP") {
                    auto result = TransformTime(vectorID, rowID);
                    file << result;
                } else {
                    file << reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(vectors[vectorID])->GetValue(rowID);
                }
                break;
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            {
                WriteString(file, vectorID, rowID);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                file << reinterpret_cast<omniruntime::vec::Vector<double> *>(vectors[vectorID])->GetValue(rowID);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
                file << reinterpret_cast<omniruntime::vec::Vector<int32_t> *>(vectors[vectorID])->GetValue(rowID);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                file << reinterpret_cast<omniruntime::vec::Vector<bool> *>(vectors[vectorID])->GetValue(rowID);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL64: {
                int scale10 = decimalInfo.empty() ? 1 : std::pow(10, decimalInfo[vectorID].second);
                file << reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(
                    vectors[vectorID])->GetValue(rowID) * 1.0 / scale10;
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
                auto valueStr = transformDecimal(vectorID, rowID, decimalInfo);
                file << valueStr;
                break;
            }
            default:
                std::runtime_error("data type not supported");
        }
    }

    void VectorBatch::writeToFile(const std::string &filename, std::ios_base::openmode mode,
                                  std::vector<std::pair<int32_t, int32_t>> decimalInfo,
                                  std::vector<std::string> inputTypes) const {
        std::ofstream file;
        file.open(filename, mode);
        if (!file.is_open()) {
            std::cerr << "Error opening file\n";
            return;
        }
        // Write row-wise
        std::vector<std::string> rowKindStr = {"+I", "-U", "+U", "-D"};
        for (size_t i = 0; i < rowCnt; ++i) {
            file << rowKindStr[(int) rowKinds[i]];
            for (size_t j = 0; j < vectors.size(); ++j) {
                file << ",";
                // write null or value
                if (vectors[j]->IsNull(i)) {
                    file << "NULL";
                } else {
                    WriteToFileInternal(j, i, file, decimalInfo, inputTypes);
                }
            }
            file << "\n";
        }
        file.close();
        LOG("write file finish")
    }

    std::vector<XXH128_hash_t> VectorBatch::getXXH128s() {
        std::vector<XXH128_hash_t> hashes(rowCnt);
        for (size_t i = 0; i < rowCnt; ++i) {
            XXH3_state_t *state = XXH3_createState();
            XXH3_128bits_reset(state);
            for (auto vec : vectors) {
                auto dataTypeId = vec->GetTypeId();
                switch(dataTypeId) {
                    case OMNI_LONG:
                    case OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                    case OMNI_TIMESTAMP: {
                        auto casted = reinterpret_cast<omniruntime::vec::Vector <int64_t> *>(vec);
                        auto val = casted->GetValue(i);
                        XXH3_128bits_update(state, &val, sizeof(int64_t));
                        break;
                    }
                    case OMNI_VARCHAR:
                    case OMNI_CHAR: {
                        if (vec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                            auto casted = reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *>(vec);
                            auto val = casted->GetValue(i);
                            XXH3_128bits_update(state, val.data(), val.size());
                        } else {
                            auto casted =
                                    reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<
                                            std::string_view, omniruntime::vec::LargeStringContainer>> *>(vec);
                            auto val = casted->GetValue(i);
                            XXH3_128bits_update(state, val.data(), val.size());
                        }
                        break;
                    }
                    default:
                        throw std::runtime_error("Type not supported yet");
                }
            }
            // Compute the final hash
            hashes[i] = XXH3_128bits_digest(state);
            XXH3_freeState(state);
        }
        return hashes;
    }

    omniruntime::vec::BaseVector* VectorBatch::CopyPositionsAndFlatten(omniruntime::vec::BaseVector *input,
        const int *positions, int offset, int length)
    {
        if (input->GetTypeId() != omniruntime::type::OMNI_VARCHAR &&
            input->GetTypeId() != omniruntime::type::OMNI_CHAR) {
            throw std::runtime_error("Type is not Varchar or Char");
        }
        if (input->GetEncoding() != omniruntime::vec::OMNI_DICTIONARY) {
            throw std::runtime_error("not dictionary");
        }
        // if it is a dictionary varchar vector, we copy the value out one by one and obtain a flat vector
        using DictVarcharVecType = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer
                <std::string_view, omniruntime::vec::LargeStringContainer>>;
        auto casted = reinterpret_cast<DictVarcharVecType *>(input);
        // new vector will be a flat one
        auto vector = new omniruntime::vec::Vector
                <omniruntime::vec::LargeStringContainer<std::string_view>>(length);
        auto startPositions = positions + offset;
        for (int32_t i = 0; i < length; i++) {
            auto position = startPositions[i];
            if (input->IsNull(position)) {
                vector->SetNull(i);
            } else {
                auto value = casted->GetValue(position);
                vector->SetValue(i, value);
            }
        }
        return vector;
    }

    omnistream::VectorBatch *VectorBatch::CreateVectorBatch(int rowCount, const std::vector<DataTypeId> &dataTypes)
    {
        // Build vector batch
        auto *vectorBatch = new omnistream::VectorBatch(rowCount);
        for (size_t i = 0; i < dataTypes.size(); i++) {
            switch (dataTypes[i]) {
                case (omniruntime::type::DataTypeId::OMNI_INT): {
                    auto vec = new omniruntime::vec::Vector<int32_t>(rowCount);
                    vectorBatch->Append(vec);
                    break;
                }
                case (omniruntime::type::DataTypeId::OMNI_LONG):
                case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE):
                case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP): {
                    auto vec = new omniruntime::vec::Vector<int64_t>(rowCount);
                    vectorBatch->Append(vec);
                    break;
                }
                case (omniruntime::type::DataTypeId::OMNI_CHAR):
                case (omniruntime::type::DataTypeId::OMNI_VARCHAR) : {
                    auto vec = new omniruntime::vec::Vector
                        <omniruntime::vec::LargeStringContainer<std::string_view>>(rowCount);
                    vectorBatch->Append(vec);
                    break;
                }
                default:
                    throw std::runtime_error("Unsupported type: " + dataTypes[i]);
            }
        }
        return vectorBatch;
    }
}