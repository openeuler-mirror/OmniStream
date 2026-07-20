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
#include <set>
#include <unordered_map>
#include <vector>
#include <arm_sve.h>

#include "table/runtime/operators/join/AbstractStreamingJoinOperator.h"
#include "table/data/JoinedRowData.h"
#include "table/data/GenericRowData.h"
#include "table/data/util/RowDataUtil.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/Triggerable.h"
#include "table/runtime/operators/InternalTimerServiceImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "table/runtime/operators/TableStreamOperator.h"
#include "table/runtime/operators/window/state/WindowListState.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "OmniOperatorJIT/core/src/codegen/simple_filter_codegen.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/utils/TimeWindowUtil.h"
using namespace omnistream;
using FilterFunc = bool (*)(int64_t*, bool*, int32_t*, bool*, int32_t*, int64_t);

template <typename KeyType>
class WindowJoinOperator : public TableStreamOperator<KeyType>,
                           public Triggerable<KeyType, int64_t>,
                           public TwoInputStreamOperator {
public:
    WindowJoinOperator(
        const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer);
    ~WindowJoinOperator() override;
    void open() override;
    void close() override;
    void processBatch1(StreamRecord* input) override;
    void processBatch2(StreamRecord* input) override;

    void processElement1(StreamRecord* element) override {};

    void processElement2(StreamRecord* element) override {};

    void ProcessWatermark1(Watermark* watermark) override
    {
        LOG(">>>>>>>>>>");
        if (this->combinedWatermark->UpdateWatermark(0, watermark->getTimestamp())) {
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(
                    new Watermark(this->combinedWatermark->GetCombinedWatermark()));
            }
            this->output->emitWatermark(new Watermark(this->combinedWatermark->GetCombinedWatermark()));
        }
    }

    void ProcessWatermark2(Watermark* watermark) override
    {
        LOG(">>>>>>>>>>");
        if (this->combinedWatermark->UpdateWatermark(1, watermark->getTimestamp())) {
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(
                    new Watermark(this->combinedWatermark->GetCombinedWatermark()));
            }
            this->output->emitWatermark(new Watermark(this->combinedWatermark->GetCombinedWatermark()));
        }
    }

    void onEventTime(TimerHeapInternalTimer<KeyType, int64_t>* timer) override;
    void onProcessingTime(TimerHeapInternalTimer<KeyType, int64_t>* timer) override;
    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override;
    virtual void join(std::vector<ComboId>* leftRecords, std::vector<ComboId>* rightRecords) = 0;
    std::string getTypeName() override
    {
        return "WindowJoinOperator";
    }

    InternalTimerServiceImpl<KeyType, int64_t>* getInternalTimerService()
    {
        return internalTimerService;
    };

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        LOG("WindowJoinOperator GetMectrics");
        return this->metrics;
    }

protected:
    TimestampedCollector* collector;
    std::vector<omniruntime::type::DataTypeId> leftTypes;
    std::vector<omniruntime::type::DataTypeId> rightTypes;
    std::vector<omniruntime::type::DataTypeId> outputTypes;
    WindowListState<KeyType, int64_t, ComboId>* leftWindowState;
    WindowListState<KeyType, int64_t, ComboId>* rightWindowState;
    ::FilterFunc generatedFilter; // Use global scope resolution to avoid ambiguity
    std::vector<int32_t> leftKeyIndex;
    std::vector<int32_t> rightKeyIndex;
    void buildInner(
        std::vector<ComboId>* leftElements, std::vector<ComboId>* rightElements, omnistream::VectorBatch* outputBatch);
    void buildRightNull(std::vector<ComboId>* leftElements, omnistream::VectorBatch* outputBatch);
    void buildLeftNull(std::vector<ComboId>* rightElements, omnistream::VectorBatch* outputBatch);
    void buildSemiAnti(
        std::vector<ComboId>* elements, omnistream::VectorBatch* outputBatch, bool isSemi, std::set<int>* matchedSet);
    bool filter(ComboId leftElement, ComboId rightElement);
    InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService;
    bool isNonEquiCondition;
    std::unique_ptr<KeySelector<KeyType>> keySelectorLeft{};
    std::unique_ptr<KeySelector<KeyType>> keySelectorRight{};

private:
    TypeSerializer* leftSerializer;
    TypeSerializer* rightSerializer;
    int leftWindowEndIndex;
    int rightWindowEndIndex;
    nlohmann::json description;
    std::set<int> colRefsForNonEquiCondition;
    int totalNumOfCols;
    struct BatchCleanupInfo {
        int32_t keyGroup;
        uint32_t sequenceNumber;
        int64_t maxTimestamp;
    };
    // Vector, rowId, colId, valAddressPtr, isNullPtr
    std::vector<void (*)(omniruntime::vec::BaseVector*, int32_t, int32_t, int64_t*, bool*)> filterFuncPtrs;
    std::vector<bool> filterNullKeys;
    std::vector<BatchCleanupInfo> leftBatchCleanupInfos;
    std::vector<BatchCleanupInfo> rightBatchCleanupInfos;
    std::string shiftTimeZone = "UTC";
    omnistream::StateType backendType_ = omnistream::StateType::HEAP;
    int32_t maxParallelism_ = 0;
    std::unordered_map<int32_t, omnistream::VectorBatch*> reuseVectorBatchByKeyGroup_{};
    std::unordered_map<int32_t, std::vector<int32_t>> reuseOldRowIdsByKeyGroup_{};
    std::vector<KeyType> reuseKeys_{};
    std::vector<ComboId> reuseComboIds_{};
    std::vector<int64_t> reuseWindowEndTimes_{};

    template <typename TYPE>
    void insertLeft(
        int colIdx,
        std::vector<ComboId>* leftElements,
        std::vector<ComboId>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner);
    template <typename TYPE>
    void insertRight(
        int colIdx,
        std::vector<ComboId>* leftElements,
        std::vector<ComboId>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner);
    void insertLeftVarchar(
        int colIdx,
        std::vector<ComboId>* leftElements,
        std::vector<ComboId>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner);
    void insertRightVarchar(
        int colIdx,
        std::vector<ComboId>* leftElements,
        std::vector<ComboId>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner);

    void processBatch(
        omnistream::VectorBatch* batch,
        int windowEndIndex,
        WindowListState<KeyType, int64_t, ComboId>* recordState,
        bool isLeftSide);
    ::FilterFunc generateJoinCondition();
    void getAllColRefs(nlohmann::json& config);
    void BuildInnerLeft(
        std::vector<ComboId>* leftElements, std::vector<ComboId>* rightElements, omnistream::VectorBatch* outputBatch);
    void BuildInnerRight(
        std::vector<ComboId>* leftElements, std::vector<ComboId>* rightElements, omnistream::VectorBatch* outputBatch);
};

template <typename KeyType>
WindowJoinOperator<KeyType>::WindowJoinOperator(
    const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer)
    : TableStreamOperator<KeyType>(new TimestampedCollector(output)),
      isNonEquiCondition(config.contains("nonEquiCondition") && !config["nonEquiCondition"].is_null()),
      leftSerializer(leftSerializer),
      rightSerializer(rightSerializer),
      leftWindowEndIndex(config["leftWindowEndIndex"]),
      rightWindowEndIndex(config["rightWindowEndIndex"]),
      description(config),
      colRefsForNonEquiCondition(),
      totalNumOfCols(),
      filterFuncPtrs()
{
    auto leftTypeStr = config["leftInputTypes"].get<std::vector<std::string>>();
    auto rightTypeStr = config["rightInputTypes"].get<std::vector<std::string>>();
    rightKeyIndex = description["rightJoinKey"].get<std::vector<int32_t>>();
    leftKeyIndex = description["leftJoinKey"].get<std::vector<int32_t>>();
    for (const auto& i : leftTypeStr) {
        leftTypes.push_back(LogicalType::flinkTypeToOmniTypeId(i));
    }

    for (const auto& i : rightTypeStr) {
        rightTypes.push_back(LogicalType::flinkTypeToOmniTypeId(i));
    }
    if (config.contains("shiftTimeZone")) {
        shiftTimeZone = config["shiftTimeZone"].get<std::string>();
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
    internalTimerService =
        AbstractStreamOperator<KeyType>::getInternalTimerService("window-timers", new LongSerializer(), this);
    std::string leftName = "left-records";
    std::string rightName = "right-records";
    auto leftDescriptor = new ListStateDescriptor<ComboId>(leftName, new LongSerializer());
    auto rightDescriptor = new ListStateDescriptor<ComboId>(rightName, new LongSerializer());

    using S = InternalListState<KeyType, int64_t, ComboId>;
    auto keyedStateBackend = this->stateHandler->getKeyedStateBackend();
    if (dynamic_cast<HeapKeyedStateBackend<KeyType>*>(keyedStateBackend)) {
        backendType_ = omnistream::StateType::HEAP;
    } else if (dynamic_cast<RocksdbKeyedStateBackend<KeyType>*>(keyedStateBackend)) {
        backendType_ = omnistream::StateType::ROCKSDB;
    } else {
        THROW_LOGIC_EXCEPTION("Unsupported keyed state backend");
    }
    leftWindowState = new WindowListState<KeyType, int64_t, ComboId>(
        keyedStateBackend->template getOrCreateKeyedState<int64_t, S, std::vector<ComboId>*>(
            new LongSerializer(), leftDescriptor));
    rightWindowState = new WindowListState<KeyType, int64_t, ComboId>(
        keyedStateBackend->template getOrCreateKeyedState<int64_t, S, std::vector<ComboId>*>(
            new LongSerializer(), rightDescriptor));

    std::vector<int> leftKeyTypes;
    std::vector<int> rightKeyTypes;
    for (auto kIndex : this->leftKeyIndex) {
        leftKeyTypes.push_back(this->leftTypes[kIndex]);
    }
    for (auto kIndex : this->rightKeyIndex) {
        rightKeyTypes.push_back(this->rightTypes[kIndex]);
    }
    // make sure the key types are the same
    if (leftKeyTypes != rightKeyTypes) {
        throw std::runtime_error("Left key types do not match right key types");
    }
    this->keySelectorLeft = std::make_unique<KeySelector<KeyType>>(leftKeyTypes, this->leftKeyIndex);
    this->keySelectorRight = std::make_unique<KeySelector<KeyType>>(rightKeyTypes, this->rightKeyIndex);
    maxParallelism_ =
        static_cast<StreamingRuntimeContext<KeyType>*>(this->getRuntimeContext())->getMaxNumberOfSubtasks();

    generatedFilter = generateJoinCondition();
    getAllColRefs(description["nonEquiCondition"]);

    for (int i = 0; i < totalNumOfCols; i++) {
        if (colRefsForNonEquiCondition.find(i) == colRefsForNonEquiCondition.end()) {
            filterFuncPtrs.push_back(nullptr);
        } else {
            bool leftSideState = static_cast<size_t>(i) < leftTypes.size();
            switch (leftSideState ? leftTypes[i] : rightTypes[i - leftTypes.size()]) {
                case omniruntime::type::DataTypeId::OMNI_INT: filterFuncPtrs.push_back(getValueAddress<int32_t>); break;
                case omniruntime::type::DataTypeId::OMNI_LONG:
                    filterFuncPtrs.push_back(getValueAddress<int64_t>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                    filterFuncPtrs.push_back(getValueAddress<double>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                    filterFuncPtrs.push_back(getValueAddress<bool>);
                    break;
                default: THROW_LOGIC_EXCEPTION("Type not recognized");
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
void WindowJoinOperator<KeyType>::processBatch1(StreamRecord* input)
{
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
    processBatch(batch, leftWindowEndIndex, leftWindowState, true);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::processBatch2(StreamRecord* input)
{
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
    processBatch(batch, rightWindowEndIndex, rightWindowState, false);
}

template <typename KeyType>
inline void WindowJoinOperator<KeyType>::initializeState(
    StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer)
{
    // First do the shared initialization step
    INFO_RELEASE(
        "WindowJoinOperator initializeState with initializer, operatorID: "
        << TwoInputStreamOperator::GetOperatorID().toString());
    AbstractStreamOperator<KeyType>::SetOperatorID(TwoInputStreamOperator::GetOperatorID().toString());
    AbstractStreamOperator<KeyType>::initializeState(initializer, keySerializer);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::onEventTime(TimerHeapInternalTimer<KeyType, int64_t>* timer)
{
    int64_t window = timer->getNamespace();

    this->setCurrentKey(timer->getKey());

    // 内存状态后端情况下，不需要手动做删除
    std::vector<ComboId>* leftRecords = leftWindowState->get(window);
    std::vector<ComboId>* rightRecords = rightWindowState->get(window);
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
        if (backendType_ != omnistream::StateType::HEAP) {
            delete leftRecords;
        }
    }
    if (rightRecords != nullptr) {
        rightWindowState->clear(window);
        if (backendType_ != omnistream::StateType::HEAP) {
            delete rightRecords;
        }
    }

    std::unordered_map<int32_t, std::vector<uint32_t>> leftSequenceNumbersToDelete;
    for (auto& info : leftBatchCleanupInfos) {
        if (window > info.maxTimestamp && info.maxTimestamp != INT64_MIN) {
            leftSequenceNumbersToDelete[info.keyGroup].push_back(info.sequenceNumber);
            info.maxTimestamp = INT64_MAX;
        }
    }
    for (auto& [keyGroup, sequenceNumbers] : leftSequenceNumbersToDelete) {
        leftWindowState->clearVectorBatches(keyGroup, sequenceNumbers);
    }
    std::unordered_map<int32_t, std::vector<uint32_t>> rightSequenceNumbersToDelete;
    for (auto& info : rightBatchCleanupInfos) {
        if (window > info.maxTimestamp && info.maxTimestamp != INT64_MIN) {
            rightSequenceNumbersToDelete[info.keyGroup].push_back(info.sequenceNumber);
            info.maxTimestamp = INT64_MAX;
        }
    }
    for (auto& [keyGroup, sequenceNumbers] : rightSequenceNumbersToDelete) {
        rightWindowState->clearVectorBatches(keyGroup, sequenceNumbers);
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::onProcessingTime(TimerHeapInternalTimer<KeyType, int64_t>* timer)
{
    THROW_LOGIC_EXCEPTION("Window Join only support event-time now");
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
template <typename KeyType>
void WindowJoinOperator<KeyType>::BuildInnerLeft(
    std::vector<ComboId>* leftElements, std::vector<ComboId>* rightElements, omnistream::VectorBatch* outputBatch)
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
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
        }
        colIdx++;
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::BuildInnerRight(
    std::vector<ComboId>* leftElements, std::vector<ComboId>* rightElements, omnistream::VectorBatch* outputBatch)
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
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
        }
        colIdx++;
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::buildInner(
    std::vector<ComboId>* leftElements, std::vector<ComboId>* rightElements, omnistream::VectorBatch* outputBatch)
{
    BuildInnerLeft(leftElements, rightElements, outputBatch);
    BuildInnerRight(leftElements, rightElements, outputBatch);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::buildRightNull(
    std::vector<ComboId>* leftElements, omnistream::VectorBatch* outputBatch)
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
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
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
void WindowJoinOperator<KeyType>::buildLeftNull(
    std::vector<ComboId>* rightElements, omnistream::VectorBatch* outputBatch)
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
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
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
    std::vector<ComboId>* elements, omnistream::VectorBatch* outputBatch, bool isSemi, std::set<int>* matchedSet)
{
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::processBatch(
    omnistream::VectorBatch* batch,
    int windowEndIndex,
    WindowListState<KeyType, int64_t, ComboId>* recordState,
    bool isLeftSide)
{
    auto batchGuard = std::unique_ptr<omnistream::VectorBatch>(batch);
    KeySelector<KeyType>* keySelector = nullptr;
    keySelector = isLeftSide ? this->keySelectorLeft.get() : this->keySelectorRight.get();

    auto rowCount = batchGuard->GetRowCount();
    reuseKeys_.resize(rowCount);
    reuseComboIds_.resize(rowCount, INVALID_COMBO_ID);
    reuseWindowEndTimes_.resize(rowCount);
    for (int i = 0; i < rowCount; i++) {
        int64_t windowEndTime =
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batchGuard->Get(windowEndIndex))->GetValue(i);
        if (TimeWindowUtil::isWindowFired(windowEndTime, internalTimerService->currentWatermark(), shiftTimeZone)) {
            continue;
        }
        reuseWindowEndTimes_[i] = windowEndTime;
        auto key = keySelector->getKey(batchGuard.get(), i);
        reuseKeys_[i] = key;

        auto keyGroup = KeyGroupRangeAssignment<KeyType>::assignToKeyGroup(key, maxParallelism_);
        auto& oldRowIds = reuseOldRowIdsByKeyGroup_[keyGroup];
        auto newRowId = static_cast<int32_t>(oldRowIds.size());
        auto comboId = VectorBatchUtil::getComboId(keyGroup, recordState->getNextSequenceNumber(keyGroup), newRowId);
        reuseComboIds_[i] = comboId;

        oldRowIds.push_back(i);
    }

    for (int i = 0; i < rowCount; i++) {
        auto comboId = reuseComboIds_[i];
        if (comboId == INVALID_COMBO_ID) {
            continue;
        }
        auto key = reuseKeys_[i];
        this->setCurrentKey(key);
        auto windowEndTime = reuseWindowEndTimes_[i];

        // TODO: use addAll
        recordState->add(windowEndTime, comboId);
        internalTimerService->registerEventTimeTimer(
            windowEndTime, TimeWindowUtil::toEpochMillsForTimer(windowEndTime - 1, shiftTimeZone));
    }

    bool shouldSplitVectorBatch =
        reuseOldRowIdsByKeyGroup_.size() > 1 ||
        (reuseOldRowIdsByKeyGroup_.size() == 1 && reuseOldRowIdsByKeyGroup_.begin()->second.size() != rowCount);

    for (auto& [keyGroup, oldRowIds] : reuseOldRowIdsByKeyGroup_) {
        auto sequenceNumber = recordState->getNextSequenceNumber(keyGroup);
        omnistream::VectorBatch* splitBatch;
        if (shouldSplitVectorBatch) {
            splitBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(batch, oldRowIds);
        } else {
            splitBatch = batchGuard.release();
        }
        auto maxTimestamp = splitBatch->setMaxTimestamp(windowEndIndex);
        reuseVectorBatchByKeyGroup_[keyGroup] = splitBatch;
        if (isLeftSide) {
            leftBatchCleanupInfos.push_back({keyGroup, sequenceNumber, maxTimestamp});
        } else {
            rightBatchCleanupInfos.push_back({keyGroup, sequenceNumber, maxTimestamp});
        }
    }

    recordState->addVectorBatches(reuseVectorBatchByKeyGroup_);

    // clean up
    if (backendType_ != omnistream::StateType::HEAP) {
        for (auto& [keyGroup, vectorBatch] : reuseVectorBatchByKeyGroup_) {
            delete vectorBatch;
        }
    }

    if constexpr (std::is_pointer_v<KeyType>) {
        for (auto& key : reuseKeys_) {
            delete key;
        }
    }
    reuseVectorBatchByKeyGroup_.clear();
    reuseOldRowIdsByKeyGroup_.clear();
    reuseKeys_.clear();
    reuseComboIds_.clear();
    reuseWindowEndTimes_.clear();
}

template <typename KeyType>
template <typename TYPE>
inline void WindowJoinOperator<KeyType>::insertLeft(
    int colIdx,
    std::vector<ComboId>* leftElements,
    std::vector<ComboId>* rightElements,
    omnistream::VectorBatch* outputBatch,
    bool isInner)
{
    auto num = leftElements->size();
    auto keyGroups = std::vector<int32_t>(num);
    auto sequenceNumbers = std::vector<uint32_t>(num);
    auto rowIds = std::vector<int32_t>(num);
    VectorBatchUtil::decodeComboIds(*leftElements, keyGroups, sequenceNumbers, rowIds);
    auto col = reinterpret_cast<omniruntime::vec::Vector<TYPE>*>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (int j = 0; j < num; j++) {
            auto keyGroup = keyGroups[j];
            auto sequenceNumber = sequenceNumbers[j];
            auto rowId = rowIds[j];
            auto batch = leftWindowState->getVectorBatch(keyGroup, sequenceNumber);
            auto value = batch->template GetValueAt<TYPE>(colIdx, rowId);
            col->SetValue(rowIdx, value);
            rowIdx++;
            if (backendType_ != omnistream::StateType::HEAP) {
                delete batch;
            }
        }
    } else {
        int rowIdx = 0;
        for (int j = 0; j < num; j++) {
            auto keyGroup = keyGroups[j];
            auto sequenceNumber = sequenceNumbers[j];
            auto rowId = rowIds[j];
            auto batch = leftWindowState->getVectorBatch(keyGroup, sequenceNumber);
            auto value = batch->template GetValueAt<TYPE>(colIdx, rowId);
            for (size_t i = 0; i < rightElements->size(); i++) {
                col->SetValue(i + rowIdx, value);
            }
            rowIdx += rightElements->size();
            if (backendType_ != omnistream::StateType::HEAP) {
                delete batch;
            }
        }
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::insertLeftVarchar(
    int colIdx,
    std::vector<ComboId>* leftElements,
    std::vector<ComboId>* rightElements,
    omnistream::VectorBatch* outputBatch,
    bool isInner)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto col = reinterpret_cast<varcharVecType*>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (auto element : *leftElements) {
            auto keyGroup = VectorBatchUtil::getKeyGroup(element);
            auto sequenceNumber = VectorBatchUtil::getSequenceNumber(element);
            auto rowId = VectorBatchUtil::getRowId(element);
            auto batch = leftWindowState->getVectorBatch(keyGroup, sequenceNumber);
            auto value = reinterpret_cast<varcharVecType*>(batch->Get(colIdx))->GetValue(rowId);
            col->SetValue(rowIdx, value);
            rowIdx++;
            if (backendType_ != omnistream::StateType::HEAP) {
                delete batch;
            }
        }
    } else {
        int rowIdx = 0;
        auto num = leftElements->size();
        auto keyGroups = std::vector<int32_t>(num);
        auto sequenceNumbers = std::vector<uint32_t>(num);
        auto rowIds = std::vector<int32_t>(num);
        VectorBatchUtil::decodeComboIds(*leftElements, keyGroups, sequenceNumbers, rowIds);
        for (int j = 0; j < num; j++) {
            auto keyGroup = keyGroups[j];
            auto sequenceNumber = sequenceNumbers[j];
            auto rowId = rowIds[j];
            auto batch = leftWindowState->getVectorBatch(keyGroup, sequenceNumber);
            auto value = reinterpret_cast<varcharVecType*>(batch->Get(colIdx))->GetValue(rowId);
            for (size_t i = 0; i < rightElements->size(); i++) {
                col->SetValue(i + rowIdx, value);
            }
            rowIdx += rightElements->size();
            if (backendType_ != omnistream::StateType::HEAP) {
                delete batch;
            }
        }
    }
}

template <typename KeyType>
template <typename TYPE>
inline void WindowJoinOperator<KeyType>::insertRight(
    int colIdx,
    std::vector<ComboId>* leftElements,
    std::vector<ComboId>* rightElements,
    omnistream::VectorBatch* outputBatch,
    bool isInner)
{
    auto num = rightElements->size();
    auto keyGroups = std::vector<int32_t>(num);
    auto sequenceNumbers = std::vector<uint32_t>(num);
    auto rowIds = std::vector<int32_t>(num);
    VectorBatchUtil::decodeComboIds(*rightElements, keyGroups, sequenceNumbers, rowIds);
    auto col = reinterpret_cast<omniruntime::vec::Vector<TYPE>*>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (int i = 0; i < num; i++) {
            auto keyGroup = keyGroups[i];
            auto sequenceNumber = sequenceNumbers[i];
            auto rowId = rowIds[i];
            auto batch = rightWindowState->getVectorBatch(keyGroup, sequenceNumber);
            auto value = batch->template GetValueAt<TYPE>(colIdx - leftTypes.size(), rowId);
            col->SetValue(rowIdx, value);
            rowIdx++;
            if (backendType_ != omnistream::StateType::HEAP) {
                delete batch;
            }
        }
    } else {
        for (size_t i = 0; i < rightElements->size(); i++) {
            auto keyGroup = keyGroups[i];
            auto sequenceNumber = sequenceNumbers[i];
            auto rowId = rowIds[i];
            auto batch = rightWindowState->getVectorBatch(keyGroup, sequenceNumber);
            auto value = batch->template GetValueAt<TYPE>(colIdx - leftTypes.size(), rowId);
            for (size_t j = 0; j < leftElements->size(); j++) {
                int valIdx = i + leftElements->size() * j;
                col->SetValue(valIdx, value);
            }
            if (backendType_ != omnistream::StateType::HEAP) {
                delete batch;
            }
        }
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::insertRightVarchar(
    int colIdx,
    std::vector<ComboId>* leftElements,
    std::vector<ComboId>* rightElements,
    omnistream::VectorBatch* outputBatch,
    bool isInner)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto col = reinterpret_cast<varcharVecType*>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (auto element : *rightElements) {
            auto keyGroup = VectorBatchUtil::getKeyGroup(element);
            auto sequenceNumber = VectorBatchUtil::getSequenceNumber(element);
            auto rowId = VectorBatchUtil::getRowId(element);
            auto batch = rightWindowState->getVectorBatch(keyGroup, sequenceNumber);
            auto value = reinterpret_cast<varcharVecType*>(batch->Get(colIdx - leftTypes.size()))->GetValue(rowId);
            col->SetValue(rowIdx, value);
            rowIdx++;
            if (backendType_ != omnistream::StateType::HEAP) {
                delete batch;
            }
        }
    } else {
        auto num = rightElements->size();
        auto keyGroups = std::vector<int32_t>(num);
        auto sequenceNumbers = std::vector<uint32_t>(num);
        auto rowIds = std::vector<int32_t>(num);
        VectorBatchUtil::decodeComboIds(*rightElements, keyGroups, sequenceNumbers, rowIds);
        for (size_t i = 0; i < rightElements->size(); i++) {
            auto keyGroup = keyGroups[i];
            auto sequenceNumber = sequenceNumbers[i];
            auto rowId = rowIds[i];
            auto batch = rightWindowState->getVectorBatch(keyGroup, sequenceNumber);
            auto value = reinterpret_cast<varcharVecType*>(batch->Get(colIdx - leftTypes.size()))->GetValue(rowId);
            for (size_t j = 0; j < leftElements->size(); j++) {
                int valIdx = i + leftElements->size() * j;
                col->SetValue(valIdx, value);
            }
            if (backendType_ != omnistream::StateType::HEAP) {
                delete batch;
            }
        }
    }
}

template <typename KeyType>
::FilterFunc WindowJoinOperator<KeyType>::generateJoinCondition()
{
    if (isNonEquiCondition) {
        auto filter = description["nonEquiCondition"];
        Expr* jExpr = JSONParser::ParseJSON(filter);
        SimpleFilterCodeGen* filterCodegen = new SimpleFilterCodeGen("nonEquiCondition", *jExpr, nullptr);
        int64_t fAddr = filterCodegen->GetFunction();
        void* refFunc = &fAddr;
        return *static_cast<::FilterFunc*>(refFunc);
    }
    return nullptr;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::getAllColRefs(nlohmann::json& config)
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
bool WindowJoinOperator<KeyType>::filter(ComboId leftElement, ComboId rightElement)
{
    if (isNonEquiCondition) {
        auto leftKeyGroup = VectorBatchUtil::getKeyGroup(leftElement);
        auto leftSequenceNumber = VectorBatchUtil::getSequenceNumber(leftElement);
        auto leftRowId = VectorBatchUtil::getRowId(leftElement);

        auto rightKeyGroup = VectorBatchUtil::getKeyGroup(rightElement);
        auto rightSequenceNumber = VectorBatchUtil::getSequenceNumber(rightElement);
        auto rightRowId = VectorBatchUtil::getRowId(rightElement);

        int64_t* vals = new int64_t[totalNumOfCols];
        bool* nulls = new bool[totalNumOfCols];
        auto resultBool = new bool(false);
        auto batches = std::vector<omnistream::VectorBatch*>();

        for (auto col : colRefsForNonEquiCondition) {
            omnistream::VectorBatch* batch;
            omniruntime::vec::BaseVector* vector;
            if (col < static_cast<int>(leftTypes.size())) {
                batch = leftWindowState->getVectorBatch(leftKeyGroup, leftSequenceNumber);
                vector = batch->Get(col);
            } else {
                batch = rightWindowState->getVectorBatch(rightKeyGroup, rightSequenceNumber);
                vector = batch->Get(col - leftTypes.size());
            }
            batches.push_back(batch);
            filterFuncPtrs[col](
                vector, static_cast<size_t>(col) < leftTypes.size() ? leftRowId : rightRowId, col, vals, nulls);
        }

        omniruntime::op::ExecutionContext context;

        auto result = generatedFilter(vals, nulls, nullptr, resultBool, nullptr, (int64_t)(&context));

        delete[] vals;
        delete[] nulls;
        delete resultBool;
        if (backendType_ != omnistream::StateType::HEAP) {
            // Notice: The logic of the filter seems to compare pointers directly, so the batch memory can only be
            // deleted at the end.
            for (auto batch_ : batches) {
                delete batch_;
                batch_ = nullptr;
            }
        }
        return result;
    } else {
        return true;
    }
}
