#ifndef FLINK_BENCHMARK_KAFKACOMMITTABLESERIALIZER_H
#define FLINK_BENCHMARK_KAFKACOMMITTABLESERIALIZER_H

#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include "KafkaCommittable.h"
#include "core/io/SimpleVersionedSerializer.h"

class KafkaCommittableSerializer : public SimpleVersionedSerializer<KafkaCommittable> {
public:
    int getVersion() const override;
    std::vector<uint8_t> serialize(const KafkaCommittable& obj) override;
    KafkaCommittable* deserialize(int version, std::vector<uint8_t>& serialized) override;
};

#endif // FLINK_BENCHMARK_KAFKACOMMITTABLESERIALIZER_H
