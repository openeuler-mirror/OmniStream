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
#ifndef OMNISTREAM_OMNIRESCALINGSTREAMTASKNETWORKINPUT_H
#define OMNISTREAM_OMNIRESCALINGSTREAMTASKNETWORKINPUT_H

#pragma once

#include <functional>
#include <future>
#include <memory>
#include <unordered_map>
#include <vector>

#include "api/common/TaskInfo.h"
#include "runtime/checkpoint/InflightDataRescalingDescriptor.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/io/network/api/serialization/RecordDeserializer.h"
#include "runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.h"
#include "runtime/plugable/DeserializationDelegate.h"
#include "streaming/runtime/io/OmniAbstractStreamTaskNetworkInput.h"
#include "streaming/runtime/io/DataInputStatus.h"
#include "streaming/runtime/io/StreamTaskNetworkInput.h"
#include "streaming/runtime/partitioner/StreamPartitioner.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/runtime/tasks/StreamTask.h"
#include "DemultiplexingRecordDeserializer.h"
#include "io/checkpointing/CheckpointedInputGate.h"
#include "checkpoint/InflightDataRescalingDescriptor.h"

namespace omnistream {
class OmniRescalingStreamTaskNetworkInput : public OmniAbstractStreamTaskNetworkInput {
public:
    OmniRescalingStreamTaskNetworkInput(
        int64_t inputIndex, std::shared_ptr<CheckpointedInputGate> inputGate, int taskType,
        TypeSerializer *inputSerializer, std::vector<long> &channelInfos,
        std::shared_ptr<InflightDataRescalingDescriptor> inflightDataRescalingDescriptor,
        std::function<StreamPartitioner<IOReadableWritable> *(int)> getPartitionerFunction,TaskInformationPOD *taskInfo)
        : OmniAbstractStreamTaskNetworkInput(inputIndex, inputGate, taskType, inputSerializer, channelInfos,
                                             getRecordDeserializers(inputGate, inputSerializer,
                                                                    *inflightDataRescalingDescriptor,
                                                                    getPartitionerFunction, taskInfo))
    {
        INFO_RELEASE("create OmniRescalingStreamTaskNetworkInput");
    }

    datastream::RecordDeserializer *getActiveSerializer(long channelInfo) override
    {
        auto ser = OmniAbstractStreamTaskNetworkInput::getActiveSerializer(channelInfo);
        if(ser == nullptr){
            INFO_RELEASE("error OmniRescalingStreamTaskNetworkInput::getActiveSerializer not find:"<< channelInfo);
            throw std::runtime_error("Channel " + std::to_string(channelInfo) +
                                     " should not receive data during recovery.");
        }
        auto deserializer = dynamic_cast<DemultiplexingRecordDeserializer *>(ser);
        if (!deserializer || !deserializer->hasMappings()) {
            INFO_RELEASE("error OmniRescalingStreamTaskNetworkInput::getActiveSerializer trans fail:"<< channelInfo);
            throw std::runtime_error("Channel " + std::to_string(channelInfo) +
                                     " should not receive data during recovery.");
        }
        return deserializer;
    }

    DataInputStatus processEvent(BufferOrEvent *bufferOrEvent) override
    {
        auto event = bufferOrEvent->getEvent();
        if (auto subtaskConnDesc = dynamic_cast<SubtaskConnectionDescriptor *>(event.get())) {
            auto channelId = bufferOrEvent->getChannelInfo().getComplexId();
            auto serializer = dynamic_cast<DemultiplexingRecordDeserializer *>(getActiveSerializer(channelId));
            serializer->select(subtaskConnDesc);
            INFO_RELEASE(
                "process SubtaskConnectionDescriptor event, channelInfo:" << bufferOrEvent->getChannelInfo().toString());
            return DataInputStatus::MORE_AVAILABLE;
        }
        return OmniAbstractStreamTaskNetworkInput::processEvent(bufferOrEvent);
    }

    OmniStreamTaskInput *finishRecover()
    {
        std::vector<long> channelIds;
        for (const auto &item : inputGate->GetChannelInfos()) {
            channelIds.emplace_back(item.getInputChannelIdx());
        }
        return new OmniAbstractStreamTaskNetworkInput(inputIndex, inputGate, taskType, inSerializer, channelIds);
    }

    class RecordFilterFactory {
    public:
        RecordFilterFactory(int subtaskIndex, TypeSerializer *inputSerializer, int numberOfChannels,
                            std::function<StreamPartitioner<IOReadableWritable> *(int)> gatePartitioners,
                            int maxParallelism)
            : gatePartitioners_(std::move(gatePartitioners)),
              inputSerializer_(std::move(inputSerializer)),
              numberOfChannels_(numberOfChannels),
              subtaskIndex_(subtaskIndex),
              maxParallelism_(maxParallelism)
        {
        }

        std::function<bool(StreamRecord &)> apply(const InputChannelInfo &channelInfo)
        {
            auto it = partitionerCache_.find(channelInfo.getGateIdx());
            if (it == partitionerCache_.end()) {
                auto partitioner = createPartitioner(channelInfo.getGateIdx());
                partitionerCache_[channelInfo.getGateIdx()] = partitioner;
                it = partitionerCache_.find(channelInfo.getGateIdx());
            }
            auto filter = std::make_shared<RecordFilter>(dynamic_cast<ChannelSelector<IOReadableWritable> *>(
                                                                 it->second),
                                                         inputSerializer_, subtaskIndex_);
            auto function = std::function<bool(StreamRecord &)>(
                [filter](StreamRecord &record) { return filter->apply(record); });
            return function;
        }

    private:
        std::unordered_map<int, StreamPartitioner<IOReadableWritable> *> partitionerCache_;
        std::function<StreamPartitioner<IOReadableWritable> *(int)> gatePartitioners_;
        TypeSerializer *inputSerializer_;
        int numberOfChannels_;
        int subtaskIndex_;
        int maxParallelism_;

        StreamPartitioner<IOReadableWritable> *createPartitioner(int index)
        {
            auto partitioner = gatePartitioners_(index);
            partitioner->setup(numberOfChannels_);
            return partitioner;
        }
    };

    class DeserializerFactory {
    public:
        explicit DeserializerFactory()
        {
        }

        static std::shared_ptr<RecordDeserializer> apply(int totalChannel)
        {
            return std::make_shared<SpillingAdaptiveSpanningRecordDeserializer>();
        }
    };

    static std::unique_ptr<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>> getRecordDeserializers(
        std::shared_ptr<CheckpointedInputGate> checkpointedInputGate, TypeSerializer *inputSerializer,
        const InflightDataRescalingDescriptor &rescalingDescriptor,
        std::function<StreamPartitioner<IOReadableWritable> *(int)> gatePartitioners, TaskInformationPOD *taskInfo)
    {
        auto recordFilterFactory = std::make_shared<RecordFilterFactory>(taskInfo->getIndexOfSubtask(), inputSerializer, taskInfo->getNumberOfSubtasks(), gatePartitioners,  taskInfo->getMaxNumberOfSubtasks());

        auto function = std::function<std::function<bool(StreamRecord &)>(const InputChannelInfo &)>(
            [recordFilterFactory](const InputChannelInfo &channelInfo) {
                return recordFilterFactory->apply(channelInfo);
            });
        auto deserializers = std::make_unique<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>>();
        deserializers->reserve(checkpointedInputGate->GetChannelInfos().size());

        for (auto &channelInfo : checkpointedInputGate->GetChannelInfos()) {
            auto channelId = channelInfo.getComplexId();
            deserializers->emplace(channelId, DemultiplexingRecordDeserializer::create(channelInfo, rescalingDescriptor,
                                                                                DeserializerFactory::apply, function));
        }
        return deserializers;
    }
};
}  // namespace omnistream

#endif  // OMNISTREAM_OMNIRESCALINGSTREAMTASKNETWORKINPUT_H
