#include "KafkaCommittableSerializer.h"

int KafkaCommittableSerializer::getVersion() const
{
    return 1;
}

std::vector<uint8_t> KafkaCommittableSerializer::serialize(const KafkaCommittable&  obj ) { return {}; }

KafkaCommittable* KafkaCommittableSerializer::deserialize(int version, std::vector<uint8_t>&  serialized) { return nullptr; }
