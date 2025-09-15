/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_WINDOWJOINOPERATOR_H
#define FLINK_TNEL_WINDOWJOINOPERATOR_H
#include <set>
#include <vector>
#include "table/runtime/operators/join/AbstractStreamingJoinOperator.h"
#include "table/data/utils/JoinedRowData.h"
#include "table/data/GenericRowData.h"
#include "table/data/util/RowDataUtil.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/Triggerable.h"
#include "table/runtime/operators/InternalTimerServiceImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "table/runtime/operators/TableStreamOperator.h"
#include "table/runtime/operators/window/state/WindowListState.h"
#include "table/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "OmniOperatorJIT/core/src/codegen/simple_filter_codegen.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"

#include <arm_sve.h>

using VectorBatchId = uint64_t;

using namespace omnistream;
using FilterFunc = bool (*)(int64_t *, bool *, int32_t *, bool *, int32_t *, int64_t);
template <typename KeyType>
class WindowJoinOperator : public TableStreamOperator<KeyType>, public Triggerable<KeyType, int64_t>, public AbstractTwoInputStreamOperator {
public:
    WindowJoinOperator(
        const nlohmann::json &config, Output *output, TypeSerializer *leftSerializer, TypeSerializer *rightSerializer);
    ~WindowJoinOperator() override;
    void open() override;
    void close() override;
    void processBatch1(StreamRecord *element) override;
    void processBatch2(StreamRecord *element) override;
    void processElement1(StreamRecord *element) override {};
    void processElement2(StreamRecord *element) override {};

    void ProcessWatermark1(Watermark* watermark) override
    {
        AbstractStreamOperator<KeyType>::ProcessWatermark1(watermark);
    }
    void ProcessWatermark2(Watermark* watermark) override
    {
        AbstractStreamOperator<KeyType>::ProcessWatermark2(watermark);
    }
    void onEventTime(TimerHeapInternalTimer<KeyType, int64_t> *timer) override;
    void onProcessingTime(TimerHeapInternalTimer<KeyType, int64_t> *timer) override;
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override;
    virtual void join(std::vector<VectorBatchId> *leftRecords, std::vector<VectorBatchId> *rightRecords) = 0;
    std::string getTypeName() override { return "WindowJoinOperator"; }
    InternalTimerServiceImpl<KeyType, int64_t> *getInternalTimerService()
    {
        return internalTimerService;
    };
    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        LOG("WindowJoinOperator GetMectrics")
        return this->metrics;
    }
protected:
    TimestampedCollector *collector;
    std::vector<omniruntime::type::DataTypeId> leftTypes;
    std::vector<omniruntime::type::DataTypeId> rightTypes;
    std::vector<omniruntime::type::DataTypeId> outputTypes;
    WindowListState<KeyType, int64_t, VectorBatchId> *leftWindowState;
    WindowListState<KeyType, int64_t, VectorBatchId> *rightWindowState;
    ::FilterFunc generatedFilter; // Use global scope resolution to avoid ambiguity
    bool hasKey;
    int leftKeyCol;
    int rightKeyCol;
    void buildInner(
        std::vector<VectorBatchId> *leftElements, std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch);
    void buildRightNull(std::vector<VectorBatchId> *leftElements, omnistream::VectorBatch *outputBatch);
    void buildLeftNull(std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch);
    void buildSemiAnti(
        std::vector<VectorBatchId> *elements, omnistream::VectorBatch *outputBatch, bool isSemi, std::set<int> *matchedSet);
    bool filter(VectorBatchId leftElement, VectorBatchId rightElement);
    InternalTimerServiceImpl<KeyType, int64_t> *internalTimerService;
    bool isNonEquiCondition;

private:
    TypeSerializer *leftSerializer;
    TypeSerializer *rightSerializer;
    int leftWindowEndIndex;
    int rightWindowEndIndex;
    nlohmann::json description;
    std::set<int> colRefsForNonEquiCondition;
    int totalNumOfCols;
    // Vector, rowId, colId, valAddressPtr, isNullPtr
    std::vector<void (*)(omniruntime::vec::BaseVector *, int32_t, int32_t, int64_t *, bool *)> filterFuncPtrs;
    std::vector<bool> filterNullKeys;

    template <typename TYPE>
    void insertLeft(int colIdx, std::vector<VectorBatchId> *leftElements, std::vector<VectorBatchId> *rightElements,
        omnistream::VectorBatch *outputBatch, bool isInner);
    template <typename TYPE>
    void insertRight(int colIdx, std::vector<VectorBatchId> *leftElements, std::vector<VectorBatchId> *rightElements,
        omnistream::VectorBatch *outputBatch, bool isInner);
    void insertLeftVarchar(int colIdx, std::vector<VectorBatchId> *leftElements,
        std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch, bool isInner);
    void insertRightVarchar(int colIdx, std::vector<VectorBatchId> *leftElements,
        std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch, bool isInner);
    
    void processBatch(omnistream::VectorBatch *batch, int windowEndIndex,
        WindowListState<KeyType, int64_t, VectorBatchId> *recordState, bool isLeftSide);
    ::FilterFunc generateJoinCondition();
    void getAllColRefs(nlohmann::json &config);
    void BuildInnerLeft(std::vector<VectorBatchId> *leftElements, std::vector<VectorBatchId> *rightElements,
        omnistream::VectorBatch *outputBatch);
    void BuildInnerRight(std::vector<VectorBatchId> *leftElements, std::vector<VectorBatchId> *rightElements,
        omnistream::VectorBatch *outputBatch);
};

template <typename KeyType>
WindowJoinOperator<KeyType>::WindowJoinOperator(
    const nlohmann::json &config, Output *output, TypeSerializer *leftSerializer, TypeSerializer *rightSerializer)
    : TableStreamOperator<KeyType>(new TimestampedCollector(output)),
      hasKey(config["leftJoinKey"].size() != 0),
      leftKeyCol(hasKey ? (config["leftJoinKey"].size() == 0 ? 0 : static_cast<int>(config["leftJoinKey"][0])) : (-1)),
      rightKeyCol(hasKey ? (config["rightJoinKey"].size() == 0 ? 0 : static_cast<int>(config["rightJoinKey"][0])) : (-1)),
      isNonEquiCondition(config.contains("nonEquiCondition") && !config["nonEquiCondition"].is_null()),
      leftSerializer(leftSerializer), rightSerializer(rightSerializer),
      leftWindowEndIndex(config["leftWindowEndIndex"]), rightWindowEndIndex(config["rightWindowEndIndex"]),
      description(config),
      colRefsForNonEquiCondition(), totalNumOfCols(), filterFuncPtrs()
{
    auto leftTypeStr = config["leftInputTypes"].get<std::vector<std::string>>();
    auto rightTypeStr = config["rightInputTypes"].get<std::vector<std::string>>();
    for (const auto& i : leftTypeStr) {
        leftTypes.push_back(LogicalType::flinkTypeToOmniTypeId(i));
    }

    for (const auto& i : rightTypeStr) {
        rightTypes.push_back(LogicalType::flinkTypeToOmniTypeId(i));
    }
    outputTypes.insert(outputTypes.end(), this->leftTypes.begin(), this->leftTypes.end());
    outputTypes.insert(outputTypes.end(), this->rightTypes.begin(), this->rightTypes.end());
}

template <typename KeyType>
WindowJoinOperator<KeyType>::~WindowJoinOperator()
{
    delete collector;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::open()
{
    totalNumOfCols = leftTypes.size() + rightTypes.size();

    collector = new TimestampedCollector(this->output);
    collector->eraseTimestamp();
    TypeSerializer *windowSerializer = LongSerializer::INSTANCE;
    internalTimerService =
        AbstractStreamOperator<KeyType>::getInternalTimerService("window-timers", windowSerializer, this);
    std::string leftName = "leftWindowState";
    std::string rightName = "rightWindowState";
    auto leftDescriptor = new ListStateDescriptor(leftName, LongSerializer::INSTANCE);
    auto rightDescriptor = new ListStateDescriptor(rightName, LongSerializer::INSTANCE);

    using S = InternalListState<KeyType, int64_t, VectorBatchId>;
    auto keyedStateBackend = this->stateHandler->getKeyedStateBackend();
    leftWindowState = new WindowListState<KeyType, int64_t, VectorBatchId>(
        keyedStateBackend->template getOrCreateKeyedState<int64_t, S, VectorBatchId>(
            new LongSerializer(), leftDescriptor));
    rightWindowState = new WindowListState<KeyType, int64_t, VectorBatchId>(
        keyedStateBackend->template getOrCreateKeyedState<int64_t, S, VectorBatchId>(
            new LongSerializer(), rightDescriptor));
    // Just in case there's no key. give it a default value
    if constexpr (std::is_same_v<KeyType, int64_t>) {
        this->setCurrentKey(0);
    } else if constexpr (std::is_pointer_v<KeyType>) {
        this->setCurrentKey(nullptr);
    }
    generatedFilter = generateJoinCondition();
    getAllColRefs(description["nonEquiCondition"]);

    for (int i = 0; i < totalNumOfCols; i++) {
        if (colRefsForNonEquiCondition.find(i) == colRefsForNonEquiCondition.end()) {
            filterFuncPtrs.push_back(nullptr);
        } else {
            bool leftSideState = static_cast<size_t>(i) < leftTypes.size();
            switch (leftSideState ? leftTypes[i] : rightTypes[i - leftTypes.size()]) {
                case omniruntime::type::DataTypeId::OMNI_INT:
                    filterFuncPtrs.push_back(getValueAddress<int32_t>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_LONG:
                    filterFuncPtrs.push_back(getValueAddress<int64_t>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                    filterFuncPtrs.push_back(getValueAddress<double>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                    filterFuncPtrs.push_back(getValueAddress<bool>);
                    break;
                default:
                    THROW_LOGIC_EXCEPTION("Type not recognized")
            }
        }
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::close()
{
    AbstractStreamOperator<KeyType>::close();
    collector = nullptr;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::processBatch1(StreamRecord* element)
{
    processBatch(reinterpret_cast<omnistream::VectorBatch *>(element->getValue()), leftWindowEndIndex, leftWindowState, true);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::processBatch2(StreamRecord* element)
{
    processBatch(reinterpret_cast<omnistream::VectorBatch *>(element->getValue()), rightWindowEndIndex, rightWindowState, false);
}

template <typename KeyType>
inline void WindowJoinOperator<KeyType>::initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer)
{
    AbstractStreamOperator<KeyType>::initializeState(initializer, keySerializer);
}


template <typename KeyType>
void WindowJoinOperator<KeyType>::onEventTime(TimerHeapInternalTimer<KeyType, int64_t> *timer)
{
    int64_t window = timer->getNamespace();
    if (hasKey) {
        this->setCurrentKey(timer->getKey());
    }
    std::vector<VectorBatchId> *leftRecords = leftWindowState->get(window);
    std::vector<VectorBatchId> *rightRecords = rightWindowState->get(window);
    if (leftRecords != nullptr) {
        LOG_PRINTF("onEventTime from left %d", leftRecords->size());
    } else {
        LOG_PRINTF("onEventTime from left 0");
    }
    if (rightRecords != nullptr) {
        LOG_PRINTF("onEventTime from right %d", rightRecords->size());
    } else {
        LOG_PRINTF("onEventTime from right 0");
    }
    join(leftRecords, rightRecords);
    if (leftRecords != nullptr) {
        leftWindowState->clear(window);
    }
    if (rightRecords != nullptr) {
        rightWindowState->clear(window);
    }

    leftWindowState->clearVectors(window);
    rightWindowState->clearVectors(window);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::onProcessingTime(TimerHeapInternalTimer<KeyType, int64_t> *timer)
{
    THROW_LOGIC_EXCEPTION("Window Join only support event-time now")
}


/*
BuildInner() Example:
key is first column

left:
1, 2
1, 3
1, 4

right:
1, 0
1, 5

result:
(1, 2)(1, 0)
(1, 2)(1, 5)
(1, 3)(1, 0)
(1, 3)(1, 5)
(1, 4)(1, 0)
(1, 4)(1, 5)
*/
template <typename KeyType> void WindowJoinOperator<KeyType>::BuildInnerLeft(std::vector<VectorBatchId> *leftElements,
    std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch)
{
    int colIdx = 0;
    // Left side
    for (auto dataType : leftTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertLeft<int16_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
                insertLeft<int32_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                insertLeft<int64_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertLeft<double>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertLeft<bool>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertLeft<omniruntime::type::Decimal128>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                insertLeftVarchar(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            default:
                THROW_LOGIC_EXCEPTION("Type not recognized")
                break;
        }
        colIdx++;
    }
}
template <typename KeyType> void WindowJoinOperator<KeyType>::BuildInnerRight(std::vector<VectorBatchId> *leftElements,
    std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch)
{
    // Right side
    int colIdx = leftTypes.size();
    for (auto dataType : rightTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertRight<int16_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
                insertRight<int32_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                insertRight<int64_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertRight<double>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertRight<bool>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertRight<omniruntime::type::Decimal128>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                insertRightVarchar(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            default:
                THROW_LOGIC_EXCEPTION("Type not recognized")
                break;
        }
        colIdx++;
    }
}
template <typename KeyType> void WindowJoinOperator<KeyType>::buildInner(std::vector<VectorBatchId> *leftElements,
    std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch)
{
    BuildInnerLeft(leftElements, rightElements, outputBatch);
    BuildInnerRight(leftElements, rightElements, outputBatch);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::buildRightNull(std::vector<VectorBatchId> *leftElements, omnistream::VectorBatch *outputBatch)
{
    int colIdx = 0;
    for (auto dataType : leftTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertLeft<int16_t>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
                insertLeft<int32_t>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
                insertLeft<int64_t>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertLeft<double>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertLeft<bool>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertLeft<omniruntime::type::Decimal128>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
                insertLeftVarchar(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            default:
                THROW_LOGIC_EXCEPTION("Type not recognized")
                break;
        }
        colIdx++;
    }

    for (int i = leftTypes.size(); i < rightTypes.size() + leftTypes.size(); i++) {
        for (int j = 0; j < outputBatch->Get(i)->GetSize(); j++) {
            outputBatch->Get(i)->SetNull(j);
        }
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::buildLeftNull(std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch)
{
    int colIdx = 0 + leftTypes.size();
    for (auto dataType : rightTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertRight<int16_t>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
                insertRight<int32_t>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
                insertRight<int64_t>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertRight<double>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertRight<bool>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertRight<omniruntime::type::Decimal128>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
                insertRightVarchar(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            default:
                THROW_LOGIC_EXCEPTION("Type not recognized")
                break;
        }
        colIdx++;
    }

    for (int i = 0; i < leftTypes.size(); i++) {
        for (int j = 0; j < outputBatch->Get(i)->GetSize(); j++) {
            outputBatch->Get(i)->SetNull(j);
        }
    }
}

template <typename KeyType>
inline void WindowJoinOperator<KeyType>::buildSemiAnti(
    std::vector<VectorBatchId> *elements, omnistream::VectorBatch *outputBatch, bool isSemi, std::set<int> *matchedSet)
{
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::processBatch(omnistream::VectorBatch *batch, int windowEndIndex,
    WindowListState<KeyType, int64_t, VectorBatchId> *recordState, bool isLeftSide)
{
    batch->setMaxTimestamp(isLeftSide ? leftWindowEndIndex : rightWindowEndIndex);
    int batchID = recordState->getCurrentBatchId();
    recordState->addVectorBatch(batch);
    omniruntime::vec::Vector<KeyType> *keyCol;
    if (hasKey) {
        keyCol =
            reinterpret_cast<omniruntime::vec::Vector<KeyType> *>(batch->Get(isLeftSide ? leftKeyCol : rightKeyCol));
    }

    for (int i = 0; i < batch->GetRowCount(); i++) {
        if (hasKey) {
            KeyType key = keyCol->GetValue(i);
            this->setCurrentKey(key);
        }
        int64_t windowEndTime =
            reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(batch->Get(windowEndIndex))->GetValue(i);
        recordState->add(windowEndTime, VectorBatchUtil::getComboId(batchID, i));
        internalTimerService->registerEventTimeTimer(windowEndTime, windowEndTime - 1);
    }
}

template <typename KeyType>
template <typename TYPE>
inline void WindowJoinOperator<KeyType>::insertLeft(int colIdx, std::vector<VectorBatchId> *leftElements,
    std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch, bool isInner)
{

    int num = (*leftElements).size();
    uint32_t* batchIDdst = new uint32_t[num];
    uint32_t* rowIDdst = new uint32_t[num];

    int processNum = svcntw();
    int half = svcntd();
    for (int i = 0; i < num; i+=processNum) {
        svbool_t pg = svwhilelt_b64(i, num);
        svbool_t pg2 = svwhilelt_b64(i + half, num);
        svbool_t pg3 = svwhilelt_b32(i, num);
        svuint64_t comboID = svld1(pg, (*leftElements).data() + i);
        svuint64_t comboID2 = svld1(pg2, (*leftElements).data() + i + half);

        svuint32_t rowID = svuzp1(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));
        svuint32_t batchID = svuzp2(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));

        svst1_u32(pg3, rowIDdst + i, rowID);
        svst1_u32(pg3, batchIDdst + i, batchID);
    }

    auto col = reinterpret_cast<omniruntime::vec::Vector<TYPE> *>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (int j = 0; j < num; j++) {
            int batchId = batchIDdst[j];
            int rowId = rowIDdst[j];
            col->SetValue(
                rowIdx, leftWindowState->getVectorBatch(batchId)->template GetValueAt<TYPE>(colIdx, rowId));
            rowIdx++;
        }
    } else {
        int rowIdx = 0;
        for (int j = 0; j < num; j++) {
            int batchId = batchIDdst[j];
            int rowId = rowIDdst[j];
            auto value = leftWindowState->getVectorBatch(batchId)->template GetValueAt<TYPE>(colIdx, rowId);
            for (size_t i = 0; i < rightElements->size(); i++) {
                col->SetValue(i + rowIdx, value);
            }
            rowIdx += rightElements->size();
        }
    }

    delete batchIDdst;
    delete rowIDdst;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::insertLeftVarchar(int colIdx, std::vector<VectorBatchId> *leftElements,
    std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch, bool isInner)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto col = reinterpret_cast<varcharVecType *>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (auto element : *leftElements) {
            int batchId = VectorBatchUtil::getBatchId(element);
            int rowId = VectorBatchUtil::getRowId(element);
            auto value = reinterpret_cast<varcharVecType *>(
                leftWindowState->getVectorBatch(batchId)->Get(colIdx))->GetValue(rowId);
            col->SetValue(rowIdx, value);
            rowIdx++;
        }
    } else {
        int rowIdx = 0;
        int num = (*leftElements).size();
        uint32_t* batchIDdst = new uint32_t[num];
        uint32_t* rowIDdst = new uint32_t[num];

        int processNum = svcntw();
        int half = svcntd();
        for (int i = 0; i < num; i+=processNum) {
            svbool_t pg = svwhilelt_b64(i, num);
            svbool_t pg2 = svwhilelt_b64(i + half, num);
            svbool_t pg3 = svwhilelt_b32(i, num);
            svuint64_t comboID = svld1(pg, (*leftElements).data() + i);
            svuint64_t comboID2 = svld1(pg2, (*leftElements).data() + i + half);

            svuint32_t rowID = svuzp1(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));
            svuint32_t batchID = svuzp2(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));

            svst1_u32(pg3, rowIDdst + i, rowID);
            svst1_u32(pg3, batchIDdst + i, batchID);
        }
        for (int j = 0; j < num; j++) {
            int batchId = batchIDdst[j];
            int rowId = rowIDdst[j];
            auto value = reinterpret_cast<varcharVecType *>(
                leftWindowState->getVectorBatch(batchId)->Get(colIdx))->GetValue(rowId);
            for (size_t i = 0; i < rightElements->size(); i++) {
                col->SetValue(i + rowIdx, value);
            }
            rowIdx += rightElements->size();
        }
        delete batchIDdst;
        delete rowIDdst;
    }
}

template <typename KeyType>
template <typename TYPE>
inline void WindowJoinOperator<KeyType>::insertRight(int colIdx, std::vector<VectorBatchId> *leftElements,
    std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch, bool isInner)
{
    int num = (*rightElements).size();
    uint32_t* batchIDdst = new uint32_t[num];
    uint32_t* rowIDdst = new uint32_t[num];

    int processNum = svcntw();
    int half = svcntd();
    for (int i = 0; i < num; i+=processNum) {
        svbool_t pg = svwhilelt_b64(i, num);
        svbool_t pg2 = svwhilelt_b64(i + half, num);
        svbool_t pg3 = svwhilelt_b32(i, num);
        svuint64_t comboID = svld1(pg, (*rightElements).data() + i);
        svuint64_t comboID2 = svld1(pg2, (*rightElements).data() + i + half);

        svuint32_t rowID = svuzp1(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));
        svuint32_t batchID = svuzp2(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));

        svst1_u32(pg3, rowIDdst + i, rowID);
        svst1_u32(pg3, batchIDdst + i, batchID);
    }

    auto col = reinterpret_cast<omniruntime::vec::Vector<TYPE> *>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (int i = 0; i < num; i++) {
            int batchId = batchIDdst[i];
            int rowId = rowIDdst[i];
            col->SetValue(rowIdx,
                rightWindowState->getVectorBatch(batchId)->template GetValueAt<TYPE>(
                    colIdx - leftTypes.size(), rowId));
            rowIdx++;
        }
    } else {
        for (size_t i = 0; i < rightElements->size(); i++) {
            auto element = rightElements->at(i);
            int batchId = batchIDdst[i];
            int rowId = rowIDdst[i];
            auto value = rightWindowState->getVectorBatch(batchId)->template GetValueAt<TYPE>(
                colIdx - leftTypes.size(), rowId);
            for (size_t j = 0; j < leftElements->size(); j++) {
                int valIdx = i + leftElements->size() * j;
                col->SetValue(valIdx, value);
            }
        }
    }
    delete[] batchIDdst;
    delete[] rowIDdst;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::insertRightVarchar(int colIdx, std::vector<VectorBatchId> *leftElements,
    std::vector<VectorBatchId> *rightElements, omnistream::VectorBatch *outputBatch, bool isInner)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto col = reinterpret_cast<varcharVecType *>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (auto element : *rightElements) {
            int batchId = VectorBatchUtil::getBatchId(element);
            int rowId = VectorBatchUtil::getRowId(element);
            auto value = reinterpret_cast<varcharVecType *>(
                rightWindowState->getVectorBatch(batchId)->Get(colIdx - leftTypes.size()))->GetValue(rowId);
            col->SetValue(rowIdx, value);
            rowIdx++;
        }
    } else {
        int num = (*rightElements).size();
        uint32_t* batchIDdst = new uint32_t[num];
        uint32_t* rowIDdst = new uint32_t[num];

        int processNum = svcntw();
        int half = svcntd();
        for (int i = 0; i < num; i+=processNum) {
            svbool_t pg = svwhilelt_b64(i, num);
            svbool_t pg2 = svwhilelt_b64(i + half, num);
            svbool_t pg3 = svwhilelt_b32(i, num);
            svuint64_t comboID = svld1(pg, (*rightElements).data() + i);
            svuint64_t comboID2 = svld1(pg2, (*rightElements).data() + i + half);

            svuint32_t rowID = svuzp1(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));
            svuint32_t batchID = svuzp2(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));

            svst1_u32(pg3, rowIDdst + i, rowID);
            svst1_u32(pg3, batchIDdst + i, batchID);
        }
        for (size_t i = 0; i < rightElements->size(); i++) {
            auto element = rightElements->at(i);
            int batchId = batchIDdst[i];
            int rowId = rowIDdst[i];
            auto value = reinterpret_cast<varcharVecType *>(
                rightWindowState->getVectorBatch(batchId)->Get(colIdx - leftTypes.size()))->GetValue(rowId);
            for (size_t j = 0; j < leftElements->size(); j++) {
                int valIdx = i + leftElements->size() * j;
                col->SetValue(valIdx, value);
            }
        }
        delete[] batchIDdst;
        delete[] rowIDdst;
    }

}
template <typename KeyType>
::FilterFunc WindowJoinOperator<KeyType>::generateJoinCondition()
{
    if (isNonEquiCondition) {
        auto filter = description["nonEquiCondition"];
        Expr *jExpr = JSONParser::ParseJSON(filter);
        SimpleFilterCodeGen *filterCodegen = new SimpleFilterCodeGen("nonEquiCondition", *jExpr, nullptr);
        int64_t fAddr = filterCodegen->GetFunction();
        void *refFunc = &fAddr;
        return *static_cast<::FilterFunc *>(refFunc);
    }
    return nullptr;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::getAllColRefs(nlohmann::json &config)
{
    if (config["exprType"] == "FIELD_REFERENCE") {
        colRefsForNonEquiCondition.emplace(config["colVal"]);
    }

    if (config.contains("right")) {
        getAllColRefs(config["right"]);
    }

    if (config.contains("left")) {
        getAllColRefs(config["left"]);
    }
}

template <typename KeyType>
bool WindowJoinOperator<KeyType>::filter(VectorBatchId leftElement, VectorBatchId rightElement)
{
    if (isNonEquiCondition) {
        auto leftRowId = VectorBatchUtil::getRowId(leftElement);
        auto leftBatchId = VectorBatchUtil::getBatchId(leftElement);

        auto rightRowId = VectorBatchUtil::getRowId(rightElement);
        auto rightBatchId = VectorBatchUtil::getBatchId(rightElement);

        int64_t *vals = new int64_t[totalNumOfCols];
        bool *nulls = new bool[totalNumOfCols];
        auto resultBool = new bool(false);

        for (auto col : colRefsForNonEquiCondition) {
            auto vector = col < static_cast<int>(leftTypes.size())
                              ? leftWindowState->getVectorBatch(leftBatchId)->Get(col)
                              : rightWindowState->getVectorBatch(rightBatchId)->Get(col - leftTypes.size());
            filterFuncPtrs[col](vector, static_cast<size_t>(col) < leftTypes.size() ? leftRowId : rightRowId, col, vals, nulls);
        }

        omniruntime::op::ExecutionContext context;

        auto result = generatedFilter(vals, nulls, nullptr, resultBool, nullptr, (int64_t)(&context));

        delete vals;
        delete nulls;
        delete resultBool;
        return result;
    } else {
        return true;
    }
}
#endif

