/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SIMPLEVERSIONEDSERIALIZER_H
#define FLINK_TNEL_SIMPLEVERSIONEDSERIALIZER_H

#include <vector>
#include <stdexcept>
#include <memory>
#include "KafkaPartitionSplit.h"
#include "io/SimpleVersionedSerializer.h"


class KafkaPartitionSplitSerializer : public SimpleVersionedSerializer<KafkaPartitionSplit> {
public:
    int getVersion() const override ;
    std::vector<uint8_t> serialize(const KafkaPartitionSplit& split) override;
    KafkaPartitionSplit*  deserialize(int version, std::vector<uint8_t>& serialized) override;
private:
    inline static const int CURRENT_VERSION = 0;
    inline static const int ONE_BYTE_LENGTH = 8;
    inline static const int TWO_BYTE_LENGTH = 16;
    inline static const int THREE_BYTE_LENGTH = 24;
};


#endif // FLINK_TNEL_SIMPLEVERSIONEDSERIALIZER_H
