/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Spilling Adaptive Spanning Record Deserializer for DataStream
 */
#ifndef FLINK_TNEL_SPILLINGADAPTIVESPANNINGRECORDDESERIALIZER_H
#define FLINK_TNEL_SPILLINGADAPTIVESPANNINGRECORDDESERIALIZER_H


#include "SpanningWrapper.h"
#include "NonSpanningWrapper.h"
#include "RecordDeserializer.h"
namespace omnistream::datastream {
class SpillingAdaptiveSpanningRecordDeserializer : public RecordDeserializer {

public:
    SpillingAdaptiveSpanningRecordDeserializer();

    DeserializationResult &getNextRecord(IOReadableWritable& target) override ;

    void setNextBuffer(const uint8_t *buffer, int size) override;

    void clear() override ;

    void *deserialize(DataInputView &source) override;

    void serialize(void *record, DataOutputSerializer &target) override;

    const char *getName() const override;

    BackendDataType getBackendId() const override { return BackendDataType::INVALID_BK; };
private:

    DeserializationResult& readNextRecord(IOReadableWritable& target);
    DeserializationResult& readNonSpanningRecord(IOReadableWritable& target);

    NonSpanningWrapper *nonSpanningWrapper;
    SpanningWrapper *spanningWrapper;
};
}


#endif  //FLINK_TNEL_SPILLINGADAPTIVESPANNINGRECORDDESERIALIZER_H
