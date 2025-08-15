/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * @Description: AbstractStreamTaskNetworkInput implementations
 */
#ifndef FLINK_TNEL_ABSTRACTSTREAMTASKNETWORKINPUT_H
#define FLINK_TNEL_ABSTRACTSTREAMTASKNETWORKINPUT_H

#include <memory>
#include <unordered_map>
#include <memory>
#include <unordered_map>

#include "DataInputStatus.h"
#include "DataOutput.h"
#include "RecordDeserializer.h"
#include "DeserializationResult.h"
#include "SpillingAdaptiveSpanningRecordDeserializer.h"
#include "../typeutils/TypeSerializer.h"
#include "../plugable/DeserializationDelegate.h"
#include "functions/StreamElement.h"
#include "../include/common.h"

namespace omnistream::datastream {
    class AbstractStreamTaskNetworkInput {
    public:

        AbstractStreamTaskNetworkInput(TypeSerializer* type_serializer,
                                       std::unique_ptr<std::unordered_map<long, RecordDeserializer *>>&& recordDeserializers);

        ~AbstractStreamTaskNetworkInput();

        uint32_t emitNextProcessElement(DataOutput &output, int32_t &inputNumber);

        virtual void emitNextProcessBuffer(const uint8_t *buffer, size_t length, long channelInfo);

        [[nodiscard]] RecordDeserializer*  getActiveSerializer(long channelInfo) const;

        static __attribute__((always_inline))
        bool processElement(StreamElement* streamElement, DataOutput& output, int32_t &inputNumber)
        {
            if (streamElement->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP ||
                streamElement->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP) {
                output.emitRecord(static_cast<StreamRecord *>(streamElement));
                ++inputNumber;
                return false;
            } else if (streamElement->getTag() == StreamElementTag::TAG_RECORD_ATTRIBUTES) {
                /**
                 recordAttributesCombiner.inputRecordAttributes(
                         streamElement.asRecordAttributes(),
                         flattenedChannelIndices.get(lastChannel),
                         outputToOut);
                 **/
                return true;
            } else {
                LOG("Bypass the tag for now, tag  is " + std::to_string(static_cast<int> (streamElement->getTag())))
                // throw std::logic_error("Unknown type of StreamElement");
                return false;
            }
        }

    protected:
        DeserializationDelegate* deserializationDelegate_;

    private:
        RecordDeserializer* currentRecordDeserializer_;

        std::unique_ptr<std::unordered_map<long, RecordDeserializer *>> recordDeserializers_;
        std::unordered_map<long, RecordDeserializer *>* rawRecordDeserializers_;

        long lastChannel_ = -1;
    };
}


#endif  //FLINK_TNEL_ABSTRACTSTREAMTASKNETWORKINPUT_H

