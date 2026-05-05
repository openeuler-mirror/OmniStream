#ifndef FLINK_BENCHMARK_KAFKAWRITERSTATESERIALIZER_H
#define FLINK_BENCHMARK_KAFKAWRITERSTATESERIALIZER_H

#include <string>
#include <vector>
#include <stdexcept>

#include "KafkaWriterState.h"
#include "core/io/SimpleVersionedSerializer.h"

class KafkaWriterStateSerializer : public SimpleVersionedSerializer<KafkaWriterState> {
public:
    int getVersion() const override;
    std::vector<uint8_t> serialize(const KafkaWriterState& obj) override;
    KafkaWriterState* deserialize(int version, std::vector<uint8_t>& serialized) override;
};

#endif // FLINK_BENCHMARK_KAFKAWRITERSTATESERIALIZER_H
