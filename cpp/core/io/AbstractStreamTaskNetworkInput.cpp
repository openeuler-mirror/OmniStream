/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <stdexcept>
#include <memory>
#include <iostream>
#include "SpillingAdaptiveSpanningRecordDeserializer.h"
#include "DeserializationResult.h"
#include "../plugable/NonReusingDeserializationDelegate.h"
#include "../streamrecord/StreamElementSerializer.h"
#include "AbstractStreamTaskNetworkInput.h"

namespace omnistream::datastream {
    AbstractStreamTaskNetworkInput::AbstractStreamTaskNetworkInput(TypeSerializer *inputSerializer,
                                                                   std::unique_ptr<std::unordered_map<long, RecordDeserializer *>> &&recordDeserializers)
            :
            currentRecordDeserializer_(nullptr), recordDeserializers_(std::move(recordDeserializers)) {
        deserializationDelegate_ = new NonReusingDeserializationDelegate(
                std::make_unique<StreamElementSerializer>(inputSerializer));
        rawRecordDeserializers_ = recordDeserializers_.get();
    }

    AbstractStreamTaskNetworkInput::~AbstractStreamTaskNetworkInput() {
        delete deserializationDelegate_;
    }

//  As this is in native task, it has different function with corresponding java implementation.
//  It should return more information so that in adapter layer can have correct login with the system
//  1. it should return whether result is fullRecord
//  2. it should return breakBatchEmitting
//  3. ifBufferConsumed
// 3.1  in java, buffer is recycled during getNextRecord
//      and ifBufferConsumed notify the AbsStreamTaskInput to process accordingly
// 3.2  in omni-flink, buffer can not be recycle in native,
//      so isBufferConsumed is to notify  AbsStreamTaskInput to recycle buffer
//  The outer adapter layer based on above return status, should do
//  1.  buffer recycle and current channel status updates
//  2.  continue batching processing or not
//  3.  return DataInputStatus status to mailbox loop


    uint32_t AbstractStreamTaskNetworkInput::emitNextProcessElement(DataOutput &output, int32_t &inputNumber) {
        uint32_t status = OmniDataInputStatus::DataInputStatus_NOT_PROCESSED;
        bool atLeastOneFullRecord = false;

        // get the stream element_ from the deserializer
        while (currentRecordDeserializer_ != nullptr) {
            status = OmniDataInputStatus::DataInputStatus_NOT_PROCESSED;
            DeserializationResult &result = currentRecordDeserializer_->getNextRecord(*deserializationDelegate_);

            if (unlikely(result.isBufferConsumed())) {
                LOG("isBufferConsumed: do we really buffer consumed?!!!")
                currentRecordDeserializer_ = nullptr;
                status |= OmniDataInputStatus::BUFFER_CONSUMED_TRUE;
            }

            if (likely(result.isFullRecord())) {
                atLeastOneFullRecord = true;
                status |= OmniDataInputStatus::FULL_RECORD_TRUE;

                auto *element = static_cast<StreamElement *>(deserializationDelegate_->getInstance());

                LOG("Got element_  ")
                bool breakBatchEmitting = processElement(element, output, inputNumber);

                status = breakBatchEmitting ? status | OmniDataInputStatus::BREAK_BATCH_EMITTING_TRUE : status;
            }
        }
        return atLeastOneFullRecord ? status | OmniDataInputStatus::AT_LEAST_ONE_FULL_RECORD_CONSUMED : status;
    }


    RecordDeserializer *AbstractStreamTaskNetworkInput::getActiveSerializer(long channelInfo) const {
        return (*rawRecordDeserializers_)[channelInfo];
    }


    void AbstractStreamTaskNetworkInput::emitNextProcessBuffer(const uint8_t *buffer, size_t length, long channelInfo) {
        lastChannel_ = channelInfo;
        if (lastChannel_ < 0) {
            THROW_LOGIC_EXCEPTION("Invalid channel info");
        }
        currentRecordDeserializer_ = getActiveSerializer(channelInfo);
        if (currentRecordDeserializer_ == nullptr) {
            THROW_LOGIC_EXCEPTION("currentRecordDeserializer has already been released");
        }

        currentRecordDeserializer_->setNextBuffer(buffer, length);
    }

}