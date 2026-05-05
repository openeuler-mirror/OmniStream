#include "KafkaWriterStateSerializer.h"
#include <sstream>

int KafkaWriterStateSerializer::getVersion() const
{
    return 1;
}

std::vector<uint8_t> KafkaWriterStateSerializer::serialize(const KafkaWriterState&  obj ) {
    return {};
}

KafkaWriterState* KafkaWriterStateSerializer::deserialize(int  version , std::vector<uint8_t>&  serialized ) {
    return new KafkaWriterState({});
}
