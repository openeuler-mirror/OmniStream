/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_KAFKASOURCE_H
#define FLINK_TNEL_KAFKASOURCE_H

#include <memory>
#include <vector>
#include <string>
#include <functional>
#include <unordered_map>
#include <future>
#include "connector-kafka/source/split/KafkaPartitionSplitSerializer.h"
#include "connector-kafka/source/reader/KafkaSourceReader.h"
#include "ConsumerConfig.h"
#include "utils/ConfigLoader.h"

class KafkaSource {
public:
    explicit KafkaSource(nlohmann::json& opDescriptionJSON);
    ~KafkaSource() {}
    SourceReader<KafkaPartitionSplit>* createReader(
            const std::shared_ptr<SourceReaderContext>& readerContext) const;
    KafkaPartitionSplitSerializer* getSplitSerializer() const;

private:
    bool isBatch;
    std::unordered_map<std::string, std::string> props;
    KafkaRecordDeserializationSchema* deserializationSchema;
};


#endif // FLINK_TNEL_KAFKASOURCE_H
