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

#include "KafkaPartitionSplitSerializer.h"


int KafkaPartitionSplitSerializer::getVersion() const
{
    return CURRENT_VERSION;
}

std::vector<uint8_t> KafkaPartitionSplitSerializer::serialize(const KafkaPartitionSplit& split)
{
    std::vector<uint8_t> serialized;

    // 获取 KafkaPartitionSplit 的相关字段
    const auto& topicPartition = split.getTopicPartition();
    const std::string& topic = topicPartition->topic();
    uint32_t partition = static_cast<uint32_t>(topicPartition->partition());
    uint64_t startingOffset = static_cast<uint64_t>(split.getStartingOffset());
    uint64_t stoppingOffset = static_cast<uint64_t>(split.getStoppingOffset());

    // 序列化 topic 的长度（int16_t）
    uint16_t topicSize = static_cast<uint16_t>(topic.size());
    serialized.push_back(static_cast<uint8_t>((topicSize >> ONE_BYTE_LENGTH) & 0xFF)); // 高字节
    serialized.push_back(static_cast<uint8_t>(topicSize & 0xFF));       // 低字节

    // 序列化 topic 的内容
    for (char c : topic) {
        serialized.push_back(static_cast<uint8_t>(c));
    }

    // 序列化 partition（int32_t）
    serialized.push_back(static_cast<uint8_t>((partition >> THREE_BYTE_LENGTH) & 0xFF)); // 最高字节
    serialized.push_back(static_cast<uint8_t>((partition >> TWO_BYTE_LENGTH) & 0xFF)); // 次高字节
    serialized.push_back(static_cast<uint8_t>((partition >> ONE_BYTE_LENGTH) & 0xFF));  // 次低字节
    serialized.push_back(static_cast<uint8_t>(partition & 0xFF));         // 最低字节

    // 序列化 startingOffset（int64_t）
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((startingOffset >> (ONE_BYTE_LENGTH * i)) & 0xFF));
    }

    // 序列化 stoppingOffset（int64_t）
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((stoppingOffset >> (ONE_BYTE_LENGTH * i)) & 0xFF));
    }

    return serialized;
}

KafkaPartitionSplit* KafkaPartitionSplitSerializer::deserialize(int version, std::vector<uint8_t>& serialized)
{
    if (version != CURRENT_VERSION) {
        throw std::runtime_error("Unsupported version");
    }

    unsigned int start = 0;
    uint16_t topicSize = (serialized[start] << ONE_BYTE_LENGTH) | serialized[start + 1];
    start += sizeof(int16_t);
    std::string topic(serialized.begin() + start,
                      serialized.begin() + start + topicSize);

    start += topicSize;
    int32_t partition = (serialized[start] << THREE_BYTE_LENGTH) | (serialized[start + 1] << TWO_BYTE_LENGTH)
        | (serialized[start + 2] << ONE_BYTE_LENGTH) | serialized[start + 3];

    start += sizeof(int32_t);
    uint64_t startingOffset = 0;
    for (size_t i = 0; i < sizeof(int64_t); ++i) {
        startingOffset = (startingOffset << ONE_BYTE_LENGTH) | serialized[start + i];
    }

    start += sizeof(int64_t);
    uint64_t stoppingOffset = 0;
    for (size_t i = 0; i < sizeof(int64_t); ++i) {
        stoppingOffset = (stoppingOffset << ONE_BYTE_LENGTH) | serialized[start + i];
    }

    std::shared_ptr<RdKafka::TopicPartition> topicPartition;
    if (static_cast<int64_t>(startingOffset) == KafkaPartitionSplit::COMMITTED_OFFSET) {
        topicPartition = std::shared_ptr<RdKafka::TopicPartition>(
            RdKafka::TopicPartition::create(topic, partition));
    } else {
        topicPartition = std::shared_ptr<RdKafka::TopicPartition>(
            RdKafka::TopicPartition::create(topic, partition, static_cast<uint64_t>(startingOffset)));
    }
    return new KafkaPartitionSplit(topicPartition, startingOffset, static_cast<uint64_t>(stoppingOffset));
}