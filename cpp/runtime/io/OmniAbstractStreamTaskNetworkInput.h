/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/3/25.
//

#ifndef OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H
#define OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H

#include <utility>

#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "OmniStreamTaskInput.h"
#include "network/api/serialization/EventSerializer.h"
#include "partition/consumer/InputChannelInfo.h"
#include "partition/consumer/InputGate.h"
#include "typeutils/TypeSerializer.h"

namespace omnistream {
class OmniAbstractStreamTaskNetworkInput : public OmniStreamTaskInput {
public:
    OmniAbstractStreamTaskNetworkInput(int64_t inputIndex, std::shared_ptr<InputGate> inputGate)
        : inputIndex(inputIndex), inputGate(std::move(inputGate))
    {
    }

    std::shared_ptr<CompletableFuture> getAvailableFuture() override
    {
        // no inputGate no output
        return AVAILABLE;
    }

    DataInputStatus emitNext(OmniPushingAsyncDataInput::OmniDataOutput *output) override
    {
        // LOG("OmniSourceStreamTask::processInput")
        while (true) {
            auto bufferOrEventOpt = inputGate->pollNext();
            if (bufferOrEventOpt) {
                // troubleshooting
                isLastValueNull = false;
                NullValueCount  = 0;
                // troubleshooting end

                LOG(">>>>> bufferOrEventOpt has value")
                auto bufferOrEvent = bufferOrEventOpt.value();
                LOG(">>>>> bufferOrEventOpt bufferOrEvent" +
                    std::to_string(reinterpret_cast<int64_t>(bufferOrEvent.get())))
                if (bufferOrEvent->isBuffer()) {
                    auto buff = bufferOrEvent->getBuffer();

                    auto size       = buff->GetSize();
                    auto objSegment = buff->GetObjectSegment();
                    auto offset     = buff->GetOffset();
                    LOG(">>>>object segment is " << std::to_string(reinterpret_cast<long>(objSegment.get())))
                    LOG(">>>>>buffer size is " << size << " buffer offset is " << offset)

                    LOG("===================start output=======================")
                    for (int64_t index = offset; index < offset + size; index++) {
                        StreamElement *object = objSegment->getObject(index);
                        LOG("OmniAbstractStreamTaskNetworkInput tag: " << static_cast<int>(object->getTag()))
                        if (object->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP ||
                            object->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP) {

                            auto record = static_cast<StreamRecord*>(object);
                            auto vectorBatch = static_cast<VectorBatch*>(record->getValue());
                            size_t row_cnt = vectorBatch->GetRowCount();
                            numberOfRow += row_cnt;

                            output->emitRecord(reinterpret_cast<StreamRecord*>(object));
                        } else if (object->getTag() == StreamElementTag::TAG_WATERMARK) {
                            output->emitWatermark(reinterpret_cast<Watermark *>(object));
                        }
                    }
                    // more avaiable means there could be more data come in
                    buff->RecycleBuffer();
                    return DataInputStatus::MORE_AVAILABLE;
                } else {
                    // we got event
                    int event = bufferOrEvent->getEvent();
                    // so far, we only knows
                    DataInputStatus status = processEvent(event);
                    return status;
                }
            } else {
                if (isLastValueNull) {
                    NullValueCount++;
                }
                isLastValueNull = true;
                if (NullValueCount % 100 == 1) {
                    // LOG(">>>>> bufferOrEventOpt has NO value, found continousely " << NullValueCount)
                }
                return DataInputStatus::NOTHING_AVAILABLE;
            }
        }
    }

    int getInputIndex() override
    {
        return static_cast<int>(inputIndex);
    }

    void close() override
    {
        INFO_RELEASE("OmniAbstractStreamTaskNetworkInput received numberOfRow: " << numberOfRow)
    }

protected:
    int64_t inputIndex;
    std::shared_ptr<InputGate> inputGate;
    std::atomic<long> numberOfRow {0};

    void processElement(StreamElement *recordOrMark, OmniDataOutput *output)
    {
        output->emitRecord(static_cast<StreamRecord *>(recordOrMark));
    }

    DataInputStatus processEvent(int event)
    {
        if (EventSerializer::END_OF_USER_RECORDS_EVENT == event) { // END_OF_USER_RECORDS_EVENT is End_of_Data
            if (inputGate->hasReceivedEndOfData()) {
                return DataInputStatus::END_OF_DATA;
            }
        } else if (EventSerializer::END_OF_PARTITION_EVENT == event) {
            // it means one sub partition or channel end. we need to check if all end by checking input gate state
            if (inputGate->isFinished()) {
                return DataInputStatus::END_OF_INPUT;
            }
        }
        // by default,continue the data processing
        return DataInputStatus::MORE_AVAILABLE;
    }

private:
    // for troubleshooting
    int NullValueCount   = 0;
    bool isLastValueNull = false;
};
}  // namespace omnistream

#endif  // OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H
