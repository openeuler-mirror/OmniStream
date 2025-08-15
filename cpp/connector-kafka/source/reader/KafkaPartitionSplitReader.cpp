/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "KafkaPartitionSplitReader.h"
#include <thread>
#include <utility>

KafkaPartitionSplitReader::KafkaPartitionSplitReader(
    const std::unordered_map<std::string, std::string>& props, std::shared_ptr<SourceReaderContext> context)
    : subtaskId(context->getSubTaskIndex())
{
    std::unordered_map<std::string, std::string> consumerProps = props;
    consumerProps["client.id"] = createConsumerClientId(props);

    std::string errstr;
    consumer = std::make_unique<RdKafkaConsumer>(consumerProps);
    // std::this_thread::sleep_for(std::chrono::seconds(2));
    if (!consumer) {
        throw std::runtime_error("Failed to create Kafka consumer: " + errstr);
    }
}

KafkaPartitionSplitReader::~KafkaPartitionSplitReader()
{
    close();
}

RecordsWithSplitIds<RdKafka::Message>* KafkaPartitionSplitReader::fetch()
{
    ConsumerRecords* consumerRecords;
    try {
        consumerRecords = consumer->poll(POLL_TIMEOUT);
    } catch (const std::exception& e) {
        consumerRecords = ConsumerRecords::EMPTY;
        auto recordsBySplits = new KafkaPartitionSplitRecords(consumerRecords);
        markEmptySplitsAsFinished(recordsBySplits);
        return recordsBySplits;
    }

    auto recordsBySplits = new KafkaPartitionSplitRecords(consumerRecords);
    std::vector<RdKafka::TopicPartition*> finishedPartitions;
    std::vector<RdKafka::TopicPartition*> recordsPartitions;
    consumerRecords->partitions(recordsPartitions);
    for (auto& tp : recordsPartitions) {
        long stoppingOffset = getStoppingOffset(tp);
        auto recordsFromPartition = consumerRecords->records(tp);
        if (!recordsFromPartition.empty()) {
            auto lastRecord = recordsFromPartition.back();
            if (lastRecord->offset() >= stoppingOffset - 1) {
                recordsBySplits->setPartitionStoppingOffset(tp, stoppingOffset);
                finishedPartitions.push_back(tp);
                recordsBySplits->addFinishedSplit(KafkaPartitionSplit::toSplitId(tp));
            }
        }
    }

    markEmptySplitsAsFinished(recordsBySplits);

    if (!finishedPartitions.empty()) {
        unassignPartitions(finishedPartitions);
    }
    return recordsBySplits;
}

void KafkaPartitionSplitReader::handleSplitsChanges(const std::vector<KafkaPartitionSplit*>& splitsChange)
{
    std::vector<RdKafka::TopicPartition*> newPartitionAssignments;
    std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, int64_t> partitionsStartingFromSpecifiedOffsets;
    std::vector<std::shared_ptr<RdKafka::TopicPartition>> partitionsStartingFromEarliest;
    std::vector<std::shared_ptr<RdKafka::TopicPartition>> partitionsStartingFromLatest;
    std::vector<std::shared_ptr<RdKafka::TopicPartition>> partitionsStoppingAtLatest;
    std::vector<std::shared_ptr<RdKafka::TopicPartition>> partitionsStoppingAtCommitted;

    for (const auto& s : splitsChange) {
        newPartitionAssignments.push_back(s->getTopicPartition().get());
        parseStartingOffsets(s, partitionsStartingFromEarliest, partitionsStartingFromLatest,
            partitionsStartingFromSpecifiedOffsets);
        parseStoppingOffsets(s, partitionsStoppingAtLatest, partitionsStoppingAtCommitted);
    }
    std::vector<RdKafka::TopicPartition*> currentAssignment;
    consumer->assignment(currentAssignment);
    newPartitionAssignments.insert(newPartitionAssignments.end(), currentAssignment.begin(), currentAssignment.end());
    consumer->assign(newPartitionAssignments);
//    std::this_thread::sleep_for(std::chrono::seconds(5));
//    delete records;
//    seekToStartingOffsets(partitionsStartingFromEarliest, partitionsStartingFromLatest,
//        partitionsStartingFromSpecifiedOffsets);
    acquireAndSetStoppingOffsets(partitionsStoppingAtLatest, partitionsStoppingAtCommitted);
    removeEmptySplits();

    maybeLogSplitChangesHandlingResult(splitsChange);
}

void KafkaPartitionSplitReader::wakeUp()
{
}

void KafkaPartitionSplitReader::close()
{
    consumer->close();
}

void KafkaPartitionSplitReader::markEmptySplitsAsFinished(KafkaPartitionSplitRecords* recordsBySplits)
{
    if (!emptySplits.empty()) {
        for (const auto& split : emptySplits) {
            recordsBySplits->finishedSplits().insert(split);
        }
        emptySplits.clear();
    }
}

std::string KafkaPartitionSplitReader::createConsumerClientId(
    const std::unordered_map<std::string, std::string>& props)
{
    auto it = props.find("client.id.prefix");
    std::string prefix = (it != props.end()) ? it->second : "";
    return prefix + "-" + std::to_string(subtaskId);
}

void KafkaPartitionSplitReader::parseStartingOffsets(
    KafkaPartitionSplit* split,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStartingFromEarliest,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStartingFromLatest,
    std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& partitionsStartingFromSpecifiedOffsets)
{
    const std::shared_ptr<RdKafka::TopicPartition> tp = split->getTopicPartition();
    if (split->getStartingOffset() == KafkaPartitionSplit::EARLIEST_OFFSET) {
        partitionsStartingFromEarliest.push_back(tp);
    } else if (split->getStartingOffset() == KafkaPartitionSplit::LATEST_OFFSET) {
        partitionsStartingFromLatest.push_back(tp);
    } else if (split->getStartingOffset() != KafkaPartitionSplit::COMMITTED_OFFSET) {
        partitionsStartingFromSpecifiedOffsets[tp] = split->getStartingOffset();
    }
}

void KafkaPartitionSplitReader::parseStoppingOffsets(
    KafkaPartitionSplit* split,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtLatest,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtCommitted)
{
    const std::shared_ptr<RdKafka::TopicPartition> tp = split->getTopicPartition();
    auto stoppingOffset = split->getStoppingOffset();
    if (stoppingOffset >= 0) {
        stoppingOffsets[tp.get()] = stoppingOffset;
    } else if (stoppingOffset == KafkaPartitionSplit::LATEST_OFFSET) {
        partitionsStoppingAtLatest.push_back(tp);
    } else if (stoppingOffset == KafkaPartitionSplit::COMMITTED_OFFSET) {
        partitionsStoppingAtCommitted.push_back(tp);
    }
}

void KafkaPartitionSplitReader::seekToStartingOffsets(
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStartingFromEarliest,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStartingFromLatest,
    std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& partitionsStartingFromSpecifiedOffsets)
{
    if (!partitionsStartingFromEarliest.empty()) {
        consumer->seekToBeginning(partitionsStartingFromEarliest);
    }
    if (!partitionsStartingFromLatest.empty()) {
        consumer->seekToEnd(partitionsStartingFromLatest);
    }
    if (!partitionsStartingFromSpecifiedOffsets.empty()) {
        consumer->seek(partitionsStartingFromSpecifiedOffsets);
    }
}

void KafkaPartitionSplitReader::acquireAndSetStoppingOffsets(
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtLatest,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtCommitted)
{
    consumer->endOffsets(partitionsStoppingAtLatest);
    for (const auto& tp : partitionsStoppingAtLatest) {
        stoppingOffsets[tp.get()] = tp->offset();
    }
    if (!partitionsStoppingAtCommitted.empty()) {
        consumer->committed(partitionsStoppingAtCommitted);
        for (const auto& tp : partitionsStoppingAtCommitted) {
            stoppingOffsets[tp.get()] = tp->offset();
        }
    }
}

void KafkaPartitionSplitReader::removeEmptySplits()
{
    std::vector<RdKafka::TopicPartition*> emptyPartitions;
    std::vector<RdKafka::TopicPartition*> currentAssignment;
    consumer->assignment(currentAssignment);
    consumer->position(currentAssignment);
    for (auto* tp : currentAssignment) {
        if (tp->offset() >= getStoppingOffset(tp)) {
            emptyPartitions.push_back(tp);
        }
    }
    if (!emptyPartitions.empty()) {
        for (auto* tp : emptyPartitions) {
            emptySplits.insert(KafkaPartitionSplit::toSplitId(tp));
        }
        unassignPartitions(emptyPartitions);
    }
    for (auto* tp : currentAssignment) {
        delete tp;
    }
}

void KafkaPartitionSplitReader::maybeLogSplitChangesHandlingResult(
    const std::vector<KafkaPartitionSplit*>& splitsChange)
{
}

void KafkaPartitionSplitReader::unassignPartitions(const std::vector<RdKafka::TopicPartition*>& partitionsToUnassign)
{
    std::vector<RdKafka::TopicPartition*> currentAssignment;
    consumer->assignment(currentAssignment);

    std::set<RdKafka::TopicPartition*, TopicPartitionComparator> unassignSet(
        partitionsToUnassign.begin(), partitionsToUnassign.end());

    std::vector<RdKafka::TopicPartition*> newAssignment;
    for (auto tp : currentAssignment) {
        if (unassignSet.find(tp) == unassignSet.end()) {
            newAssignment.push_back(tp);
        }
    }
    consumer->assign(newAssignment);
}

long KafkaPartitionSplitReader::getStoppingOffset(RdKafka::TopicPartition* tp)
{
    auto it = stoppingOffsets.find(tp);
    return (it != stoppingOffsets.end()) ? it->second : std::numeric_limits<long>::max();
}
