/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_KAFKAPARTITIONSPLITREADER_H
#define FLINK_TNEL_KAFKAPARTITIONSPLITREADER_H


#include <chrono>
#include <iostream>
#include <memory>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <functional>
#include <exception>
#include "RdKafkaConsumer.h"
#include "connector-kafka/source/split/KafkaPartitionSplit.h"
#include "SplitReader.h"
#include "connector-kafka/source/metrics/KafkaSourceReaderMetrics.h"
#include "core/api/connector/source/SourceReaderContext.h"

class KafkaPartitionSplitRecords : public RecordsWithSplitIds<RdKafka::Message> {
public:
    explicit KafkaPartitionSplitRecords(ConsumerRecords* consumerRecords) : consumerRecords(consumerRecords)
    {
        consumerRecords->partitions(partitions);
        splitIterator = partitions.begin();
    }

    ~KafkaPartitionSplitRecords()
    {
        if (consumerRecords != ConsumerRecords::EMPTY) {
            delete consumerRecords;
        }
    }

    void setPartitionStoppingOffset(RdKafka::TopicPartition* topicPartition, long stoppingOffset)
    {
        stoppingOffsets[topicPartition] = stoppingOffset;
    }

    void addFinishedSplit(const std::string& splitId)
    {
        finishedSplits_.insert(splitId);
    }

    std::string nextSplit() override
    {
        if (splitIterator != partitions.end()) {
            currentTopicPartition = *splitIterator;
            records = consumerRecords->records(currentTopicPartition);
            auto it = stoppingOffsets.find(currentTopicPartition);
            currentSplitStoppingOffset = (it != stoppingOffsets.end()) ? it->second : std::numeric_limits<long>::max();
            const std::string &result = currentTopicPartition->topic()
                                        + "-" + std::to_string(currentTopicPartition->partition());
            ++splitIterator;
            return result;
        } else {
            currentTopicPartition = nullptr;
            currentSplitStoppingOffset = std::numeric_limits<long>::max();
            return "";
        }
    }

    const RdKafka::Message* nextRecordFromSplit() override
    {
        if (!currentTopicPartition) {
            throw std::runtime_error(
                "Make sure nextSplit() did not return null before iterate over the records split.");
        }
        if (recordIterator != records.end()) {
            const RdKafka::Message* record = *recordIterator;
            if (record->offset() < currentSplitStoppingOffset) {
                ++recordIterator;
                return record;
            }
        }
        return nullptr;
    }

    const std::vector<RdKafka::Message*>& getRecordsFromSplit() override
    {
        if (!currentTopicPartition) {
            throw std::runtime_error(
                "Make sure nextSplit() did not return null before iterate over the records split.");
        }
        return records;
    }

    size_t getSplitStoppingOffset() override
    {
        return currentSplitStoppingOffset;
    }

    std::set<std::string>& finishedSplits() override
    {
        return finishedSplits_;
    }
private:
    std::set<std::string> finishedSplits_;
    std::unordered_map<RdKafka::TopicPartition*, long, TopicPartitionHash, TopicPartitionComparator> stoppingOffsets;
    ConsumerRecords* consumerRecords;
    std::vector<RdKafka::TopicPartition*> partitions;
    std::vector<RdKafka::TopicPartition*>::const_iterator splitIterator;
    std::vector<RdKafka::Message*> records;
    std::vector<RdKafka::Message*>::const_iterator recordIterator;
    RdKafka::TopicPartition* currentTopicPartition;
    long currentSplitStoppingOffset;
};

class KafkaPartitionSplitReader : public SplitReader<RdKafka::Message, KafkaPartitionSplit> {
public:
    KafkaPartitionSplitReader(const std::unordered_map<std::string, std::string>& props,
        std::shared_ptr<SourceReaderContext> context);
    ~KafkaPartitionSplitReader();

    RecordsWithSplitIds<RdKafka::Message>* fetch() override;
    void handleSplitsChanges(const std::vector<KafkaPartitionSplit*>& splitsChange);
    void wakeUp();
    void close();

private:
    inline static const long POLL_TIMEOUT = 10000L;
    std::unique_ptr<RdKafkaConsumer> consumer;
    std::unordered_map<RdKafka::TopicPartition*, long, TopicPartitionHash, TopicPartitionComparator> stoppingOffsets;
    const int subtaskId;
    std::set<std::string> emptySplits;

    std::string createConsumerClientId(const std::unordered_map<std::string, std::string>& props);
    void parseStartingOffsets(
            KafkaPartitionSplit* split,
            std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStartingFromEarliest,
            std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStartingFromLatest,
            std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>&
            partitionsStartingFromSpecifiedOffsets
    );
    void parseStoppingOffsets(
            KafkaPartitionSplit* split,
            std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtLatest,
            std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtCommitted
    );
    void seekToStartingOffsets(
            std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStartingFromEarliest,
            std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStartingFromLatest,
            std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>&
            partitionsStartingFromSpecifiedOffsets);
    void acquireAndSetStoppingOffsets(
            std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtLatest,
            std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtCommitted);
    void removeEmptySplits();
    void maybeLogSplitChangesHandlingResult(const std::vector<KafkaPartitionSplit*>& splitsChange);
    void unassignPartitions(const std::vector<RdKafka::TopicPartition*>& partitionsToUnassign);
    long getStoppingOffset(RdKafka::TopicPartition* tp);
    void markEmptySplitsAsFinished(KafkaPartitionSplitRecords* recordsBySplits);
};

#endif // FLINK_TNEL_KAFKAPARTITIONSPLITREADER_H
