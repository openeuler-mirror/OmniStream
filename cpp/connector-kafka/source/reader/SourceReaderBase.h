
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SOURCEREADERBASE_H
#define FLINK_TNEL_SOURCEREADERBASE_H

#include "core/api/connector/source/SourceReader.h"
#include "connector-kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/connector/source/SourceReaderContext.h"
#include "connector-kafka/source/reader/fetcher/KafkaSourceFetcherManager.h"
#include "connector-kafka/source/reader/KafkaRecordEmitter.h"
#include "connector-kafka/source/reader/KafkaPartitionSplitReader.h"
#include "SplitContext.h"
#include "connector-kafka/source/split/KafkaPartitionSplitState.h"
#include "connector-kafka/bind_core_manager.h"

template <typename E, typename SplitT, typename SplitStateT>
class SourceReaderBase : public SourceReader<SplitT> {
public:
    SourceReaderBase(std::shared_ptr<FutureCompletingBlockingQueue<RdKafka::Message>> elementsQueue,
        std::shared_ptr<SingleThreadFetcherManager<E, SplitT>> splitFetcherManager,
        std::shared_ptr<RecordEmitter<E, SplitStateT>> recordEmitter,
        const std::shared_ptr<SourceReaderContext> context, bool isBatch)
        : isBatch(isBatch), elementsQueue(elementsQueue), splitStates(),recordEmitter(recordEmitter),
        splitFetcherManager(splitFetcherManager), context(context), noMoreSplitsAssignment(false)
    {
        if (isBatch) {
            reuseMessageVec.reserve(batchSize);
        }
    }

    virtual ~SourceReaderBase() = default;

    // 轮询下一个记录
    InputStatus pollNext(ReaderOutput* output) override
    {
        RecordsWithSplitIds<E>* recordsWithSplitId = currentFetch;
        if (!recordsWithSplitId) {
            recordsWithSplitId = getNextFetch(output);
            if (!recordsWithSplitId) {
                return trace(finishedOrAvailableLater());
            }
        }

        // emit all records and one batch per pollNext, which is better
        const std::vector<RdKafka::Message*> &records = recordsWithSplitId->getRecordsFromSplit();
        if (isBatch) {
            batchPoll(records, recordsWithSplitId);
        } else {
            normalPoll(records, recordsWithSplitId);
        }
        moveToNextSplit(recordsWithSplitId, output);
        return InputStatus::MORE_AVAILABLE;
    }

    void normalPoll(const std::vector<RdKafka::Message*> &records, RecordsWithSplitIds<E>* recordsWithSplitId)
    {
        std::vector<RdKafka::Message*>::const_iterator recordIterator;
        auto currentSplitStoppingOffset = recordsWithSplitId->getSplitStoppingOffset();
        for (recordIterator = records.begin(); recordIterator != records.end(); ++recordIterator) {
            if (static_cast<size_t>((*recordIterator)->offset()) >= currentSplitStoppingOffset) {
                break;
            }
            recordEmitter->emitRecord(*recordIterator, currentSplitOutput, currentSplitContext->state);
        }
    }

    void batchPoll(const std::vector<RdKafka::Message*> &records, RecordsWithSplitIds<E>* recordsWithSplitId)
    {
        auto currentSplitStoppingOffset = recordsWithSplitId->getSplitStoppingOffset();
        size_t totalRecords = records.size();
        for (size_t i = 0; i < totalRecords; i += batchSize) {
            reuseMessageVec.clear();
            size_t currentGroupSize = std::min(batchSize, totalRecords - i);
            // 将当前组的元素添加到 messageVec
            for (size_t j = 0; j < currentGroupSize; ++j) {
                if (static_cast<size_t>(records[i + j]->offset()) >= currentSplitStoppingOffset) {
                    break;
                }
                reuseMessageVec.push_back(records[i + j]);
            }
            if (reuseMessageVec.empty()) {
                break;
            }
            // 处理当前组
            recordEmitter->emitBatchRecord(reuseMessageVec, currentSplitOutput, currentSplitContext->state);
            if (reuseMessageVec.size() < batchSize) {
                break;
            }
        }
    }

    std::shared_ptr<omnistream::CompletableFuture> getAvailable() override
    {
        if (!currentFetch) {
            return FutureCompletingBlockingQueue<RdKafka::Message>::AVAILABLE;
        } else {
            return elementsQueue->getAvailabilityFuture();
        }
    }

    // 对拆分状态进行快照
    std::vector<KafkaPartitionSplit> snapshotState(long checkpointId)  override
    {
        return {};
    }

    // 添加拆分
    void addSplits(std::vector<SplitT*>& splits) override
    {
        LOG("Adding split(s) to reader");
        // Initialize the state for each split.
        for (const auto& s : splits) {
            const std::string &splitId = s->splitId();
            splitStates[splitId] = std::make_shared<SplitContext>(splitId, initializedState(s));
        }
        // Hand over the splits to the split fetcher to start fetch.
        splitFetcherManager->addSplits(splits);
    }

    // 通知没有更多拆分
    void notifyNoMoreSplits() override
    {
        LOG("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
        if (elementsQueue) {
            elementsQueue->notifyAvailable();
        }
    }
    // 处理源事件
    void handleSourceEvents(const SourceEvent& sourceEvent) override
    {
        LOG("Received unhandled source event.");
    }

    void start() override
    {
    }

    // 关闭读取器
    void close() override
    {
        LOG("Closing Source Reader");
        splitFetcherManager->close(30000);
    }

    // 获取当前分配的拆分数量
    int getNumberOfCurrentlyAssignedSplits() const
    {
        return static_cast<int>(splitStates.size());
    }

protected:
    // 初始化新拆分的状态
    virtual std::shared_ptr<KafkaPartitionSplitState> initializedState(KafkaPartitionSplit* split) = 0;

    virtual void onSplitFinished(std::unordered_map<std::string,
        std::shared_ptr<KafkaPartitionSplitState>>& finishedSplitIds) {};
private:
    bool isBatch;
    std::shared_ptr<FutureCompletingBlockingQueue<E>> elementsQueue;
    std::unordered_map<std::string, std::shared_ptr<SplitContext>> splitStates;
    std::shared_ptr<RecordEmitter<E, SplitStateT>> recordEmitter;
    std::shared_ptr<SplitFetcherManager<E, SplitT>> splitFetcherManager;
    const std::shared_ptr<SourceReaderContext> context;
    // 最新从拆分读取器提取的按拆分的记录批次
    RecordsWithSplitIds<E>* currentFetch = nullptr;
    // 当前拆分的上下文
    std::shared_ptr<SplitContext> currentSplitContext;
    // 当前拆分的输出
    SourceOutput* currentSplitOutput;
    // 指示 SourceReader 是否将被分配更多拆分
    bool noMoreSplitsAssignment;

    size_t batchSize = 1000;

    int32_t bindCore1 = -1;

    int32_t bindCore2 = -1;

    bool binded1 = false, binded2 = false;

    std::vector<RdKafka::Message*> reuseMessageVec;

    // 跟踪输入状态并记录日志
    InputStatus trace(InputStatus status)
    {
        return status;
    }
    // 获取下一个提取的记录批次
    RecordsWithSplitIds<E>* getNextFetch(ReaderOutput* output)
    {
        splitFetcherManager->checkErrors();
        RecordsWithSplitIds<E>* recordsWithSplitId = elementsQueue->poll();
        if (!recordsWithSplitId || !moveToNextSplit(recordsWithSplitId, output)) {
            return nullptr;
        }
        currentFetch = recordsWithSplitId;
        return recordsWithSplitId;
    }
    // 完成当前提取
    void finishCurrentFetch(RecordsWithSplitIds<E>* fetch,  ReaderOutput* output)
    {
        currentFetch = nullptr;
        currentSplitContext = nullptr;
        currentSplitOutput = nullptr;

        if (unlikely(not binded1)) {
            if (elementsQueue->getCoreId() >= 0) {
                omnistream::BindCoreManager::GetInstance()->BindDirectCore(elementsQueue->getCoreId());
            }
            binded1 = true;
        }

        const std::set<std::string> finishedSplits = fetch->finishedSplits();
        if (!finishedSplits.empty()) {
            std::unordered_map<std::string, std::shared_ptr<KafkaPartitionSplitState>> stateOfFinishedSplits;
            for (const auto& finishedSplitId : finishedSplits) {
                stateOfFinishedSplits[finishedSplitId] = splitStates[finishedSplitId]->state;
                splitStates.erase(finishedSplitId);
                output->ReleaseOutputForSplit(finishedSplitId);
            }
            onSplitFinished(stateOfFinishedSplits);
        }

        fetch->recycle();
        // todo delete
        delete fetch;
    }
    // 移动到下一个拆分
    bool moveToNextSplit(RecordsWithSplitIds<E>* recordsWithSplitIds, ReaderOutput* output)
    {
        const std::string &nextSplitId = recordsWithSplitIds->nextSplit();
        if (nextSplitId.empty()) {
            finishCurrentFetch(recordsWithSplitIds, output);
            return false;
        }
        auto it = splitStates.find(nextSplitId);
        if (it == splitStates.end()) {
            return false;
            // throw std::runtime_error("Have records for a split that was not registered");
        }
        currentSplitContext = it->second;
        currentSplitOutput = &(currentSplitContext->getOrCreateSplitOutput(output));
        return true;
    }
    // 判断读取是否结束或稍后可用
    InputStatus finishedOrAvailableLater()
    {
        bool allFetchersHaveShutdown = splitFetcherManager->maybeShutdownFinishedFetchers();
        if (!(noMoreSplitsAssignment && allFetchersHaveShutdown)) {
            return InputStatus::NOTHING_AVAILABLE;
        }
        if (elementsQueue->isEmpty()) {
            // We may reach here because of exceptional split fetcher, check it.
            splitFetcherManager->checkErrors();
            return InputStatus::END_OF_INPUT;
        } else {
            // We can reach this case if we just processed all data from the queue and finished a
            // split,
            // and concurrently the fetcher finished another split, whose data is then in the queue.
            return InputStatus::MORE_AVAILABLE;
        }
    }
};
#endif // FLINK_TNEL_SOURCEREADERBASE_H
