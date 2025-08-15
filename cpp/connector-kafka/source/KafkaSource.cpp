/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "KafkaSource.h"
#include "core/api/common/serialization/DeserializationFactory.h"

KafkaSource::KafkaSource(nlohmann::json& opDescriptionJSON) : isBatch(opDescriptionJSON["batch"])
{
    nlohmann::json properties = opDescriptionJSON["properties"];
    // obtain the config
    for (auto &[key, value] : properties.items()) {
        const std::map<std::string, std::string>::iterator &iter = ConsumerConfig.find(key);
        if (iter != ConsumerConfig.end() && iter->second != "") {
            props.emplace(iter->second, value);
        }
    }
    // opt kafka consumer para from file
    std::unordered_map<std::string, std::string> optConsumerConfig = ConfigLoader::GetKafkaConsumerConfig();
    for (const auto &pair : optConsumerConfig) {
        props.emplace(pair.first, pair.second);
    }

    auto innerDeserializationSchema = DeserializationFactory::getDeserializationSchema(
        opDescriptionJSON);
    deserializationSchema = KafkaRecordDeserializationSchema::valueOnly(innerDeserializationSchema);
}

SourceReader<KafkaPartitionSplit>* KafkaSource::createReader(
    const std::shared_ptr<SourceReaderContext>& readerContext) const
{
    auto subTaskId = readerContext->getSubTaskIndex();
    auto elementsQueue = std::make_shared<FutureCompletingBlockingQueue<RdKafka::Message>>(subTaskId);
    deserializationSchema->open();
    std::function<std::shared_ptr<SplitReader<RdKafka::Message, KafkaPartitionSplit>>()> splitReaderSupplier =
        [this, readerContext]() {
            return std::make_shared<KafkaPartitionSplitReader>(props, readerContext);
        };
    std::shared_ptr<RecordEmitter<RdKafka::Message, KafkaPartitionSplitState>> recordEmitter =
        std::make_shared<KafkaRecordEmitter>(deserializationSchema);
    std::shared_ptr<SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>> kafkaSourceFetcherManager =
        std::make_shared<KafkaSourceFetcherManager>(elementsQueue, splitReaderSupplier);
    return new KafkaSourceReader(elementsQueue, kafkaSourceFetcherManager, recordEmitter, readerContext, isBatch);
}

KafkaPartitionSplitSerializer* KafkaSource::getSplitSerializer() const
{
    return new KafkaPartitionSplitSerializer();
}