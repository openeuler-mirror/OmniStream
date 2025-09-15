/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "StreamingJoinOperator.h"

template class StreamingJoinOperator<RowData*>;
template class StreamingJoinOperator<long>;

template <typename K>
void StreamingJoinOperator<K>::processBatch(omnistream::VectorBatch *input, JoinRecordStateView<K> *inputSideStateView,
                                            JoinRecordStateView<K> *otherSideStateView, bool inputIsLeft, bool isSuppress)
{
    try {
        LOG("===================Join processBatch Start=======================")
        // 1. Find matched rows in the otherside. Result will be stored in AbstractStreamingJoinOperator::matchedLists
        if (auto casted = dynamic_cast<InputSideHasNoUniqueKey<K> *>(otherSideStateView)) {
            AbstractStreamingJoinOperator<K>::template of<InputSideHasNoUniqueKey<K> >(input, inputIsLeft, casted);
        } else if (auto casted = dynamic_cast<OuterInputSideHasNoUniqueKey<K> *>(otherSideStateView)) {
            AbstractStreamingJoinOperator<K>::template of<OuterInputSideHasNoUniqueKey<K> >(input, inputIsLeft, casted);
        } else {
            NOT_IMPL_EXCEPTION
        }

        // 2. Update inputSide state
        bool inputIsOuter = (inputIsLeft && leftIsOuter) || (!inputIsLeft && rightIsOuter);
        bool otherIsOuter = (!inputIsLeft && leftIsOuter) || (inputIsLeft && rightIsOuter);
        auto keySelector = inputIsLeft ? this->keySelectorLeft : this->keySelectorRight;
        bool filterNulls = this->filterNullKeys[0];
        auto backend = this->getKeyedStateBackend();
        inputSideStateView->addOrRectractRecord(input, keySelector, otherIsOuter, backend, filterNulls,
                                                this->matchedCount);

        // 3. Build the output
        if (!leftIsOuter && !rightIsOuter) {
            auto outputVB = buildOutputInner(input, inputIsLeft, otherSideStateView);
            if (outputVB != nullptr) {
                this->collector->collect(outputVB);
            }
        } else {
            auto outputVB = buildOutput(input, inputIsLeft, inputIsOuter, otherIsOuter, otherSideStateView);
            if (outputVB != nullptr) {
                this->collector->collect(outputVB);
            }
        }
    } catch (std::runtime_error &e) {
        throw std::runtime_error("join process element failed");
    }
}

template <typename K>
void StreamingJoinOperator<K>::open()
{
    AbstractStreamingJoinOperator<K>::open();
    if (leftIsOuter) {
        std::string stateName = "left-records_" + this->leftInputSpec;
        leftRecordStateView = new OuterInputSideHasNoUniqueKey<K>(this->getRuntimeContext(), stateName, nullptr);
    } else {
        std::string stateName = "left-records_" + this->leftInputSpec;
        leftRecordStateView = JoinRecordStateViews::create(this->getRuntimeContext(), stateName, nullptr, nullptr,
                                                           this->leftUniqueKeyIndex);
    }
    if (rightIsOuter) {
        NOT_IMPL_EXCEPTION
    } else {
        std::string stateName = "right-records_" + this->rightInputSpec;
        rightRecordStateView = JoinRecordStateViews::create(this->getRuntimeContext(), stateName, nullptr, nullptr,
                                                            this->rightUniqueKeyIndex);
    }
    std::vector<int> leftKeyTypes;
    std::vector<int> rightKeyTypes;
    for (auto kIndex: this->leftKeyIndex) {
        leftKeyTypes.push_back(this->leftInputTypes[kIndex]);
    }
    for (auto kIndex : this->rightKeyIndex) {
        rightKeyTypes.push_back(this->rightInputTypes[kIndex]);
    }
    // make sure the key types are the same
    assert(leftKeyTypes == rightKeyTypes);
    this->keySelectorLeft = new KeySelector<K>(leftKeyTypes, this->leftKeyIndex);
    this->keySelectorRight = new KeySelector<K>(rightKeyTypes, this->rightKeyIndex);
}

template<typename K>
omnistream::VectorBatch *StreamingJoinOperator<K>::buildOutputInner(omnistream::VectorBatch *input,
                                                                    bool inputIsLeft,
                                                                    JoinRecordStateView<K> *otherSideStateView)
{
    this->matchedCountTot = std::accumulate(this->matchedCount.begin(), this->matchedCount.end(), 0);
    if (this->matchedCountTot == 0) {
        return nullptr;
    }

    // Since this only runs for inner join
    omnistream::VectorBatch* outputVB = new omnistream::VectorBatch(this->matchedCountTot);
    outputVB->ResizeVectorCount(this->leftInputTypes.size() + this->rightInputTypes.size());
    // Build the columns that comes from inputSide

    AssembleFisrtTime(input, outputVB, inputIsLeft);
    // Build the columns that comes from the otherSide
    AssembleSecondTime(input, outputVB, otherSideStateView, inputIsLeft);

    // set the RowKind and timestamp. When both sides uses inner join. It uses the input's RowKind and Timestamp
    int rowIndex = 0;
    for (size_t i = 0; i < this->matchedLists.size(); i++) {
        if (this->matchedLists[i] != nullptr) {
            for (size_t j = 0; j < (this->matchedLists)[i]->size(); j++) {
                outputVB->setRowKind(rowIndex, input->getRowKind(i));
                outputVB->setTimestamp(rowIndex++, input->getTimestamp(i));
            }
        }
    }
    return outputVB;
}

template<typename K>
void StreamingJoinOperator<K>::AssembleFisrtTime(omnistream::VectorBatch* input,
                                                 omnistream::VectorBatch* outputVB,
                                                 bool inputIsLeft)
{
    bool inputIsOuter = false;
    const auto& inputTypes = inputIsLeft ? this->leftInputTypes : this->rightInputTypes;
    for (size_t icol = 0; icol < inputTypes.size(); icol++) {
        int outCol = inputIsLeft ? icol : this->leftInputTypes.size() + icol;
        switch ((omniruntime::type::DataTypeId) inputTypes[icol]) {
            case DataTypeId::OMNI_LONG:
                outputVB->SetVector(outCol, buildInputSideColumn<int64_t, int64_t>(input, icol, inputIsOuter));
                break;
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                outputVB->SetVector(outCol, buildInputSideColumn<int64_t, int64_t>(input, icol, inputIsOuter));
                break;
            case DataTypeId::OMNI_VARCHAR:
                if (input->Get(icol)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                    outputVB->SetVector(outCol,
                                        buildInputSideColumn<omniruntime::vec::LargeStringContainer<std::string_view>,
                                        omniruntime::vec::LargeStringContainer<std::string_view>>(input, icol, inputIsOuter));
                } else {
                    outputVB->SetVector(outCol,
                                        buildInputSideColumn<omniruntime::vec::LargeStringContainer<std::string_view>,
                                        omniruntime::vec::DictionaryContainer<
                                        std::string_view, omniruntime::vec::LargeStringContainer>>(input, icol, inputIsOuter));
                }
                break;
            default:
                std::runtime_error("DataType not supported yet!");
        }
    }
}

template<typename K>
void StreamingJoinOperator<K>::AssembleSecondTime(omnistream::VectorBatch* input,
                                                  omnistream::VectorBatch* outputVB,
                                                  JoinRecordStateView<K> *otherSideStateView,
                                                  bool inputIsLeft)
{
    bool inputIsOuter = false;
    const auto& otherTypes = !inputIsLeft ?  this->leftInputTypes : this->rightInputTypes;
    for (size_t icol = 0; icol < otherTypes.size(); icol++) {
        int outCol = inputIsLeft ? (this->leftInputTypes.size() + icol) : icol;
        switch ((omniruntime::type::DataTypeId) otherTypes[icol]) {
            case DataTypeId::OMNI_LONG:
                outputVB->SetVector(outCol, buildOtherSideColumn<int64_t, int64_t>(input, otherSideStateView,
                                                                                   icol, inputIsOuter));
                break;
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                outputVB->SetVector(outCol, buildOtherSideColumn<int64_t, int64_t>(input, otherSideStateView,
                                                                                   icol, inputIsOuter));
                break;
            case DataTypeId::OMNI_VARCHAR:
                outputVB->SetVector(outCol, buildOtherSideColumnVarchar(input, otherSideStateView, icol, inputIsOuter));
                break;
            default:
                std::runtime_error("DataType not supported yet!");
        }
    }
}

template<typename K>
template<typename T, typename S>
omniruntime::vec::BaseVector *StreamingJoinOperator<K>::buildOtherSideColumn(omnistream::VectorBatch *input,
                                                                             JoinRecordStateView<K> *otherSideStateView,
                                                                             int32_t icol, bool inputIsOuter)
{
    auto outputCol = new omniruntime::vec::Vector<T>(this->matchedCountTot);
    int rowIndex = 0; // rowIndex used for writing
    int curbatchId = -1;

    omniruntime::vec::Vector<S>* inputCol;

    for (size_t i = 0; i < this->matchedLists.size(); i++) {
        if (this->matchedLists[i] != nullptr) {
            const std::vector<int64_t>& vec = *(this->matchedLists[i]);
            for (auto id : vec) {
                DealOneBatchInColumn(id, icol, rowIndex, curbatchId, otherSideStateView, inputCol, outputCol);
            }
        } else if (inputIsOuter) { // No match and input is Outer: add null-padded records
            outputCol->SetNull(rowIndex++);
        }
    }

    int num = this->deleteRecords.size();
    uint32_t* batchIDdst = new uint32_t[num];
    uint32_t* rowIDdst = new uint32_t[num];

    int processNum = svcntw();
    int half = svcntd();
    for (int i = 0; i < num; i+=processNum) {
        svbool_t pg = svwhilelt_b64(i, num);
        svbool_t pg2 = svwhilelt_b64(i + half, num);
        svbool_t pg3 = svwhilelt_b32(i, num);
        svuint64_t comboID = svld1(pg, reinterpret_cast<uint64_t*>(this->deleteRecords.data()) + i);
        svuint64_t comboID2 = svld1(pg2, reinterpret_cast<uint64_t*>(this->deleteRecords.data()) + i + half);

        svuint32_t rowID = svuzp1(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));
        svuint32_t batchID = svuzp2(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));

        svst1_u32(pg3, rowIDdst + i, rowID);
        svst1_u32(pg3, batchIDdst + i, batchID);
    }

    // Loop wont run for inner join as deletedRecords can have elements only if other is Outer
    for (int i = 0; i < num; i++) {
        auto batchId = batchIDdst[i];
        auto rowId = rowIDdst[i];
        if (curbatchId != batchId) {
            if (otherSideStateView->getVectorBatch(batchId) == nullptr) {
                throw std::runtime_error("get batch is nullptr in buildOtherSideColumn");
            }
            inputCol = reinterpret_cast<omniruntime::vec::Vector<S>*>(
                    otherSideStateView->getVectorBatch(batchId)->GetVectors()[icol]);
            curbatchId = batchId;
        }
        auto val = inputCol->GetValue(rowId);
        outputCol->SetValue(rowIndex++, val);
    }

    delete[] batchIDdst;
    delete[] rowIDdst;

    return outputCol;
}

template<typename K>
template<typename T, typename S>
void StreamingJoinOperator<K>::DealOneBatchInColumn(long id, int32_t icol, int& rowIndex, int& curbatchId,
                                                    JoinRecordStateView<K> *otherSideStateView, omniruntime::vec::Vector<S>*& inputCol,
                                                    omniruntime::vec::Vector<T>*& outputCol)
{
    auto batchId = VectorBatchUtil::getBatchId(id);
    auto rowId = VectorBatchUtil::getRowId(id);
    if (curbatchId != batchId) {
        inputCol = reinterpret_cast<omniruntime::vec::Vector<S>*>(
                otherSideStateView->getVectorBatch(batchId)->GetVectors()[icol]);
        curbatchId = batchId;
    }
    auto val = inputCol->GetValue(rowId);
    outputCol->SetValue(rowIndex++, val);
}

template<typename K>
omniruntime::vec::BaseVector *StreamingJoinOperator<K>::buildOtherSideColumnVarchar(omnistream::VectorBatch *input,
                                                                                    JoinRecordStateView<K> *otherSideStateView, int32_t icol, bool inputIsOuter)
{
    auto outputCol = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
            this->matchedCountTot);
    int rowIndex = 0; // rowIndex used for writing
    using FlatTypeS = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    using DictTypeS = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<std::string_view,
            omniruntime::vec::LargeStringContainer>>;
    for (size_t i = 0; i < this->matchedLists.size(); i++) {
        if (this->matchedLists[i] != nullptr) {
            const std::vector<int64_t>& vec = *(this->matchedLists[i]);
            for (auto id : vec) {
                DealOneBatchInColumnVarchar(id, icol, rowIndex, otherSideStateView, outputCol);
            }
        } else if (inputIsOuter) { // No match and input is Outer: add null-padded records
            outputCol->SetNull(rowIndex++);
        }
    }

    int num = this->deleteRecords.size();
    uint32_t* batchIDdst = new uint32_t[num];
    uint32_t* rowIDdst = new uint32_t[num];

    int processNum = svcntw();
    int half = svcntd();
    for (int i = 0; i < num; i+=processNum) {
        svbool_t pg = svwhilelt_b64(i, num);
        svbool_t pg2 = svwhilelt_b64(i + half, num);
        svbool_t pg3 = svwhilelt_b32(i, num);
        svuint64_t comboID = svld1(pg, reinterpret_cast<uint64_t*>(this->deleteRecords.data()) + i);
        svuint64_t comboID2 = svld1(pg2, reinterpret_cast<uint64_t*>(this->deleteRecords.data()) + i + half);

        svuint32_t rowID = svuzp1(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));
        svuint32_t batchID = svuzp2(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));

        svst1_u32(pg3, rowIDdst + i, rowID);
        svst1_u32(pg3, batchIDdst + i, batchID);
    }

    // Loop wont run for inner join as deletedRecords can have elements only if other is Outer
    for (int i = 0; i < num; i++) {
        auto batchId = batchIDdst[i];
        auto rowId = rowIDdst[i];
        auto inputCol = otherSideStateView->getVectorBatch(batchId)->Get(icol);
        if (otherSideStateView->getVectorBatch(batchId) == nullptr) {
            LOG("string from vectorBatch is nullptr")
            throw std::runtime_error("string from vectorBatch is nullptr");
        }
        if (inputCol->GetEncoding() == OMNI_FLAT) {
            auto castedCol = reinterpret_cast<FlatTypeS*>(inputCol);
            auto sv = castedCol->GetValue(rowId);
            outputCol->SetValue(rowIndex++, sv);
        } else {
            auto castedCol = reinterpret_cast<DictTypeS*>(inputCol);
            auto sv = castedCol->GetValue(rowId);
            outputCol->SetValue(rowIndex++, sv);
        }
    }
    delete[] batchIDdst;
    delete[] rowIDdst;

    return outputCol;
}

template<typename K>
void StreamingJoinOperator<K>::DealOneBatchInColumnVarchar(long id, int32_t icol, int& rowIndex,
                                                           JoinRecordStateView<K> *otherSideStateView,
                                                           omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*& outputCol)
{
    using FlatTypeS = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    using DictTypeS = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<std::string_view,
            omniruntime::vec::LargeStringContainer>>;
    auto batchId = VectorBatchUtil::getBatchId(id);
    auto rowId = VectorBatchUtil::getRowId(id);
    if (otherSideStateView->getVectorBatch(batchId) == nullptr) {
        LOG("string from vectorBatch is nullptr")
        return;
    }
    auto inputCol = otherSideStateView->getVectorBatch(batchId)->Get(icol);
    if (inputCol->GetEncoding() == OMNI_FLAT) {
        auto castedCol = reinterpret_cast<FlatTypeS*>(inputCol);
        auto sv = castedCol->GetValue(rowId);
        outputCol->SetValue(rowIndex++, sv);
    } else {
        auto castedCol = reinterpret_cast<DictTypeS*>(inputCol);
        auto sv = castedCol->GetValue(rowId);
        outputCol->SetValue(rowIndex++, sv);
    }
}

template<typename K>
template<typename T, typename S>
omniruntime::vec::BaseVector *StreamingJoinOperator<K>::buildInputSideColumn(omnistream::VectorBatch *input,
                                                                             int32_t icol, bool inputIsOuter)
{
    auto inputCol = static_cast<omniruntime::vec::Vector<S>*>(input->GetVectors()[icol]);
    int rowIndex = 0; // rowIndex used for writing
    auto outputCol = new omniruntime::vec::Vector<T>(this->matchedCountTot);
    for (size_t i = 0; i < this->matchedLists.size(); i++) {
        auto value = inputCol->GetValue(i);
        if (this->matchedLists[i] != nullptr) {
            for (size_t j = 0; j < (this->matchedLists)[i]->size(); j++) {
                outputCol->SetValue(rowIndex++, value);
            }
        } else if (inputIsOuter) { // No match and input is Outer: add null-padded records
            outputCol->SetValue(rowIndex++, value);
        }
    }

    // Loop wont run for inner join as deletedRecords can have elements only if other is Outer
    for (size_t i = 0; i < this->deleteRecords.size(); i++) {
        outputCol->SetNull(rowIndex++);
    }

    return outputCol;
}

template<typename K>
void StreamingJoinOperator<K>::setOutPutValueInput(omnistream::VectorBatch *input, bool inputIsLeft, bool inputIsOuter,
                                                   JoinRecordStateView<K> *otherSideStateView,
                                                   omnistream::VectorBatch *outputVB)
{
    const auto &inputTypes = inputIsLeft ? this->leftInputTypes : this->rightInputTypes;
    for (size_t icol = 0; icol < inputTypes.size(); icol++) {
        int outCol = inputIsLeft ? icol : this->leftInputTypes.size() + icol;
        switch ((omniruntime::type::DataTypeId) inputTypes[icol]) {
            case DataTypeId::OMNI_LONG:
                outputVB->SetVector(outCol, buildInputSideColumn<int64_t, int64_t>(input, icol, inputIsOuter));
                break;
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                outputVB->SetVector(outCol, buildInputSideColumn<int64_t, int64_t>(input, icol, inputIsOuter));
                break;
            case DataTypeId::OMNI_VARCHAR:
                if (input->Get(icol)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                    outputVB->SetVector(outCol,
                                        buildInputSideColumn<omniruntime::vec::LargeStringContainer<std::string_view>,
                                        omniruntime::vec::LargeStringContainer<std::string_view>>(input, icol,
                                                inputIsOuter));
                } else {
                    outputVB->SetVector(outCol,
                                        buildInputSideColumn<omniruntime::vec::LargeStringContainer<std::string_view>,
                                        omniruntime::vec::DictionaryContainer<std::string_view,
                                        omniruntime::vec::LargeStringContainer>>(input, icol, inputIsOuter));
                }
                break;
            default:
                std::runtime_error("DataType not supported yet!");
        }
    }
}

template<typename K>
void StreamingJoinOperator<K>::setOutPutValueOther(omnistream::VectorBatch *input, bool inputIsLeft, bool inputIsOuter,
                                                   JoinRecordStateView<K> *otherSideStateView,
                                                   omnistream::VectorBatch* outputVB)
{
    const auto &otherTypes = !inputIsLeft ? this->leftInputTypes : this->rightInputTypes;
    for (size_t icol = 0; icol < otherTypes.size(); icol++) {
        int outCol = inputIsLeft ? (this->leftInputTypes.size() + icol) : icol;
        switch ((omniruntime::type::DataTypeId) otherTypes[icol]) {
            case DataTypeId::OMNI_LONG:
                outputVB->SetVector(outCol, buildOtherSideColumn<int64_t, int64_t>(input, otherSideStateView, icol,
                                                                                   inputIsOuter));
                break;
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                outputVB->SetVector(outCol, buildOtherSideColumn<int64_t, int64_t>(input, otherSideStateView, icol,
                                                                                   inputIsOuter));
                break;
            case DataTypeId::OMNI_VARCHAR:
                if (otherSideStateView->getVectorBatchesSize() > 0 &&
                    (otherSideStateView->getVectorBatch(0) != nullptr) &&
                    otherSideStateView->getVectorBatch(0)->Get(icol)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                    outputVB->SetVector(outCol,
                                        buildOtherSideColumn<omniruntime::vec::LargeStringContainer<std::string_view>,
                                        omniruntime::vec::LargeStringContainer<std::string_view> >(
                                                input, otherSideStateView, icol, inputIsOuter));
                } else {
                    outputVB->SetVector(outCol,
                                        buildOtherSideColumn<omniruntime::vec::LargeStringContainer<std::string_view>,
                                        omniruntime::vec::DictionaryContainer<std::string_view,
                                        omniruntime::vec::LargeStringContainer>>(
                                                input, otherSideStateView, icol, inputIsOuter));
                }

                break;
            default:
                std::runtime_error("DataType not supported yet!");
        }
    }
}

template<typename K>
RowKind StreamingJoinOperator<K>::getOutputVBRowKind(omnistream::VectorBatch *input, bool inputIsOuter,
                                                     bool otherIsOuter, int index)
{
    if (!inputIsOuter && !otherIsOuter) { // inner join
        return input->getRowKind(index);
    } else if (RowDataUtil::isRetractMsg(input->getRowKind(index))) {
        if (inputIsOuter) {
            return RowKind::DELETE;
        } else {
            return input->getRowKind(index);
        }
    } else {
        return RowKind::INSERT;
    }
}

template<typename K>
void StreamingJoinOperator<K>::setRowKind_sve(int i, int size, uint8_t* dst, int8_t* condition) {
    auto pg = svwhilelt_b8(i, size);
    svint8_t conditionData = svld1_s8(pg, condition);
    svbool_t mask = svcmpeq_n_s8(pg, conditionData, 0);
    svuint8_t data = svsel_u8(mask, svdup_n_u8(3), svdup_n_u8(0));
    svst1_u8(pg, dst, data);
}

template<typename K>
void StreamingJoinOperator<K>::setTimestamp_raw(int start, int size, const int64_t* src, int64_t* dst, int rowIndex) {
    int processElement = svcntb();
    for (int i = 0; i < processElement && start + i < size; i++) {
        dst[i + rowIndex] = src[i + start];
    }
}

template<typename K>
void StreamingJoinOperator<K>::setOutPutMetaData(omnistream::VectorBatch *input, bool inputIsOuter, bool otherIsOuter,
                                                 omnistream::VectorBatch *outputVB)
{
    int rowIndex = 0;
    for (size_t i = 0; i < this->matchedLists.size(); i++) {
        if (this->matchedLists[i] != nullptr) { // Found matches for record
            for (size_t j = 0; j < (this->matchedLists)[i]->size(); j++) {
                outputVB->setRowKind(rowIndex, getOutputVBRowKind(input, inputIsOuter, otherIsOuter, i));
                outputVB->setTimestamp(rowIndex++, input->getTimestamp(i));
            }
        } else if (inputIsOuter) { // No matches for record
            if (RowDataUtil::isRetractMsg(input->getRowKind(i))) {
                outputVB->setRowKind(rowIndex, RowKind::DELETE);
            } else {
                outputVB->setRowKind(rowIndex, RowKind::INSERT);
            }
            outputVB->setTimestamp(rowIndex++, input->getTimestamp(i));
        }
    }

    int size = this->deleteRecords.size();
    int processElement = svcntb();
    for (size_t i = 0; i < this->deleteRecords.size(); i++) {
        setRowKind_sve(i, size, reinterpret_cast<uint8_t*>(outputVB->getRowKinds()) + rowIndex, this->deleteKinds.data() + i);
        setTimestamp_raw(i, size, input->getTimestamps(), outputVB->getTimestamps(), rowIndex);
        rowIndex += processElement;
    }
}