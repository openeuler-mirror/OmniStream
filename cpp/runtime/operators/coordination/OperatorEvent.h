/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_OPERATOREVENT_H
#define FLINK_TNEL_OPERATOREVENT_H

#include "io/SimpleVersionedSerializer.h"

class OperatorEvent {
public:
    virtual ~OperatorEvent() = default;
};

class WatermarkAlignmentEvent : public OperatorEvent {
public:
    WatermarkAlignmentEvent(long maxWatermark) : maxWatermark(maxWatermark) {}

    long getMaxWatermark()
    {
        return maxWatermark;
    }
private:
    long maxWatermark;
};


template <typename E>
class AddSplitEvent : public OperatorEvent {
public:
    AddSplitEvent(int serializerVersion, std::vector<std::vector<uint8_t>> splitsVec)
        : serializerVersion(serializerVersion), splitsVec(std::move(splitsVec)) {}

    AddSplitEvent(std::vector<E> splits, SimpleVersionedSerializer<E>* splitSerializer)
    {
        splitsVec.reserve(splits.size());
        serializerVersion = splitSerializer->getVersion();
        for (const auto& split : splits) {
            splitsVec.push_back(splitSerializer->serialize(split));
        }
    }

    std::vector<E*> splits(SimpleVersionedSerializer<E>* splitSerializer) const
    {
        std::vector<E*> result;
        result.reserve(splitsVec.size());
        // 遍历所有序列化后的分片数据
        for (auto serializedSplit : splitsVec) {
            // 调用反序列化方法并将结果添加到结果向量中
            result.push_back(splitSerializer->deserialize(serializerVersion, serializedSplit));
        }

        return result;
    }
private:
    int serializerVersion;
    std::vector<std::vector<uint8_t>> splitsVec;
};

class SourceEvent {
public:
    virtual ~SourceEvent() = default;
};


class NoMoreSplitsEvent : public OperatorEvent {
};

#endif // FLINK_TNEL_OPERATOREVENT_H
