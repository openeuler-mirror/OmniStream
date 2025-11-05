/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H
#define OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H

#include <utility>
#include <runtime/io/network/api/serialization/RecordDeserializer.h>
#include <runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.h>
#include <runtime/plugable/DeserializationDelegate.h>
#include <runtime/plugable/NonReusingDeserializationDelegate.h>
#include <streaming/runtime/streamrecord/StreamElementSerializer.h>

#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "OmniStreamTaskInput.h"
#include "buffer/NetworkBuffer.h"
#include "event/EndOfData.h"
#include "event/EndOfPartitionEvent.h"
#include "runtime/io/network/api/serialization/EventSerializer.h"
#include "partition/consumer/InputChannelInfo.h"
#include "partition/consumer/InputGate.h"
#include "streaming/runtime/io/OmniStreamTaskNetworkOutput.h"
#include "typeutils/TypeSerializer.h"
#include "runtime/io/checkpointing/CheckpointedInputGate.h"
namespace omnistream {
class OmniAbstractStreamTaskNetworkInput : public OmniStreamTaskInput {
public:
    OmniAbstractStreamTaskNetworkInput(int64_t inputIndex, std::shared_ptr<CheckpointedInputGate> inputGate, int taskType,
        TypeSerializer *inputSerializer, std::vector<long> & channelInfos)
        : inputIndex(inputIndex), inputGate(std::move(inputGate)), taskType(taskType), currentRecordDeserializer(nullptr)
    {
        deserializationDelegate_ = new NonReusingDeserializationDelegate(
                std::make_unique<datastream::StreamElementSerializer>(inputSerializer));
        recordDeserializers = getRecordDeserializers(channelInfos);
    }

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        // no inputGate no output
        return AVAILABLE;
    }

    std::unique_ptr<std::unordered_map<long, datastream::RecordDeserializer *>> getRecordDeserializers(
    std::vector<long> & channelInfos)
    {
        std::unique_ptr<std::unordered_map<long, datastream::RecordDeserializer *>> recordDeserializers
                = std::make_unique<std::unordered_map<long, datastream::RecordDeserializer *>>();
        for (size_t i = 0; i < channelInfos.size(); i++) {
            auto deserializer = new datastream::SpillingAdaptiveSpanningRecordDeserializer();
            (*recordDeserializers)[channelInfos.at(i)] = deserializer;
        }
        return recordDeserializers;
    }

    [[nodiscard]] datastream::RecordDeserializer *getActiveSerializer(long channelInfo) const
    {
        return (*recordDeserializers)[channelInfo];
    }

    DataInputStatus processBufferOrEventOptForSQL(OmniPushingAsyncDataInput::OmniDataOutput *output,
                                                  std::optional<std::shared_ptr<BufferOrEvent>>& bufferOrEventOpt)
    {
        isLastValueNull = false;
        NullValueCount = 0;

        LOG(">>>>> bufferOrEventOpt has value")
        auto bufferOrEvent = bufferOrEventOpt.value();
        LOG(">>>>> bufferOrEventOpt bufferOrEvent" +
            std::to_string(reinterpret_cast<int64_t>(bufferOrEvent.get())))
        if (bufferOrEvent->isBuffer()) {
            auto buff = std::reinterpret_pointer_cast<ObjectBuffer>(bufferOrEvent->getBuffer());

            auto size = buff->GetSize();
            auto objSegment = buff->GetObjectSegment();
            auto offset = buff->GetOffset();
            LOG(">>>>object segment is " << std::to_string(reinterpret_cast<long>(objSegment.get())))
            LOG(">>>>>buffer size is " << size << " buffer offset is " << offset)

            LOG("===================start output=======================")
            for (int64_t index = offset; index < offset + size; index++) {
                StreamElement *object = objSegment->getObject(index);
                LOG("OmniAbstractStreamTaskNetworkInput tag: " << static_cast<int>(object->getTag()))
                if (object->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP ||
                    object->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP) {
                    auto record = static_cast<StreamRecord *>(object);
                    auto vectorBatch = static_cast<VectorBatch *>(record->getValue());
                    size_t row_cnt = vectorBatch->GetRowCount();
                    numberOfRow += row_cnt;

                    output->emitRecord(reinterpret_cast<StreamRecord *>(object));
                } else if (object->getTag() == StreamElementTag::TAG_WATERMARK) {
                    output->emitWatermark(reinterpret_cast<Watermark *>(object));
                }
            }
            // more avaiable means there could be more data come in
            buff->RecycleBuffer();
            return DataInputStatus::MORE_AVAILABLE;
        } else {
            // we got event
            std::shared_ptr<AbstractEvent> event = bufferOrEvent->getEvent();
            // so far, we only knows
            DataInputStatus status = processEvent(event);
            return status;
        }
    }

    DataInputStatus processForSQL(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        while (true) {
            auto bufferOrEventOpt = inputGate->PollNext();
            if (bufferOrEventOpt) {
                return processBufferOrEventOptForSQL(output, bufferOrEventOpt);
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

    void processBufferForDataStream(std::shared_ptr<BufferOrEvent> bufferOrEvent)
    {
        auto buffer = std::static_pointer_cast<ReadOnlySlicedNetworkBuffer>(bufferOrEvent->getBuffer());
        auto inputChannelInfo = bufferOrEvent->getChannelInfo();
        currentRecordDeserializer = getActiveSerializer(inputChannelInfo.getInputChannelIdx());
        if (currentRecordDeserializer == nullptr) {
            THROW_LOGIC_EXCEPTION("currentRecordDeserializer has already been released");
        }
        currentRecordDeserializer->SetNextBuffer(buffer);
    }

    DataInputStatus processFullRecordForDataStream(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        auto *element = static_cast<StreamElement *>(deserializationDelegate_->getInstance());
        if (element->getTag() == StreamElementTag::TAG_WATERMARK) {
            output->emitWatermark(reinterpret_cast<Watermark *>(element));
        } else {
            processElement(element, output);
        }
        return DataInputStatus::MORE_AVAILABLE;
    }

    DataInputStatus processForDataStream(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        while (true) {
            if (currentRecordDeserializer != nullptr) {
                DeserializationResult &result = currentRecordDeserializer->getNextRecord(*deserializationDelegate_);

                if (unlikely(result.isBufferConsumed())) {
                    LOG("isBufferConsumed: do we really buffer consumed?!!!")
                    currentRecordDeserializer = nullptr;
                }

                if (likely(result.isFullRecord())) {
                    return processFullRecordForDataStream(output);
                }
            }

            auto bufferOrEventOpt = inputGate->PollNext();
            if (bufferOrEventOpt) {
                auto bufferOrEvent = bufferOrEventOpt.value();
                if (bufferOrEvent->isBuffer()) {
                    processBufferForDataStream(bufferOrEvent);
                } else  {
                    std::cout << "current is event" << std::endl;
                    std::shared_ptr<AbstractEvent> event = bufferOrEvent->getEvent();
                    DataInputStatus status = processEvent(event);
                    return status;
                }
            } else {
                return DataInputStatus::NOTHING_AVAILABLE;
            }
        }
    }

    DataInputStatus emitNext(OmniPushingAsyncDataInput::OmniDataOutput *output) override
    {
        // we might need reconstruct here
        if (auto curOutput = dynamic_cast<OmniStreamTaskNetworkOutput*>(output)) {
            curOutput->setTaskType(taskType);
        }

        if (taskType == 1) {
            return processForSQL(output);
        } else if (taskType == 2) {
            return processForDataStream(output);
        } else {
            throw std::runtime_error("Unknown taskType: " + taskType);
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
    std::shared_ptr<CheckpointedInputGate> inputGate;
    std::atomic<long> numberOfRow {0};

    void processElement(StreamElement *recordOrMark, OmniDataOutput *output)
    {
        output->emitRecord(static_cast<StreamRecord *>(recordOrMark));
    }

    DataInputStatus processEvent(std::shared_ptr<AbstractEvent> event)
    {
        if (dynamic_cast<EndOfData *>(event.get())) { // END_OF_USER_RECORDS_EVENT is End_of_Data
            if (inputGate->HasReceivedEndOfData()) {
                return DataInputStatus::END_OF_DATA;
            }
        } else if (dynamic_cast<EndOfPartitionEvent *>(event.get())) {
            // it means one sub partition or channel end. we need to check if all end by checking input gate state
            if (inputGate->IsFinished()) {
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
    int taskType;
    std::unique_ptr<std::unordered_map<long, datastream::RecordDeserializer *>> recordDeserializers;
    datastream::RecordDeserializer* currentRecordDeserializer;
    DeserializationDelegate* deserializationDelegate_;
};
}  // namespace omnistream

#endif  // OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H
