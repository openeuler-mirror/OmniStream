/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "RecoveredChannelStateHandler.h"
#include "event/SubtaskConnectionDescriptor.h"
#include "io/network/api/serialization/EventSerializer.h"

namespace omnistream {

ResultSubpartitionRecoveredStateHandler::ResultSubpartitionRecoveredStateHandler(
    std::vector<std::shared_ptr<ResultPartitionWriter>> writers,
    bool notifyAndBlockOnCompletion, 
    std::shared_ptr<InflightDataRescalingDescriptor> channelMapping)
    : writers_(std::move(writers)),
      notifyAndBlockOnCompletion_(notifyAndBlockOnCompletion),
      channelMapping_(std::move(channelMapping)) {}

ResultSubpartitionRecoveredStateHandler::~ResultSubpartitionRecoveredStateHandler()
{
    this->close();
}

ResultSubpartitionRecoveredStateHandler::BufferWithContext ResultSubpartitionRecoveredStateHandler::getBuffer(const ResultSubpartitionInfoPOD& subpartitionInfo)
{
    
    auto channels = getMappedChannels(subpartitionInfo);
    
    if (channels.empty()) {
        throw std::runtime_error("No mapped channels found");
    }
    BufferBuilder *bufferBuilder = channels.at(0)->requestBufferBuilderBlocking();
    return BufferWithContext(ChannelStateByteBuffer::wrap(bufferBuilder), bufferBuilder);
}

void ResultSubpartitionRecoveredStateHandler::recover(const ResultSubpartitionInfoPOD& subpartitionInfo, int oldSubtaskIndex, const BufferWithContext& bufferWithContext)
{
    LOG("ResultSubpartitionRecoveredStateHandler recover111")
    BufferBuilder *bufferBuilder = bufferWithContext.context_;
    auto bufferConsumer = bufferBuilder->createBufferConsumerFromBeginning();
    LOG("ResultSubpartitionRecoveredStateHandler recover222")
    bufferBuilder->finish();
    LOG("ResultSubpartitionRecoveredStateHandler recover333")

    if (!bufferConsumer->isDataAvailable()) {
        return;
    }

    LOG("ResultSubpartitionRecoveredStateHandler recover444")
    auto channels = getMappedChannels(subpartitionInfo);
    if (channels.empty()) {
        throw std::runtime_error("No mapped channels found in recover()");
    }
    LOG("ResultSubpartitionRecoveredStateHandler recover555, channel size: " << channels.size())
    if (channels.size() == 1) {
        channels[0]->addRecovered(bufferConsumer);
    } else {
        // 现在这条分支先明确失败，避免再次踩到 MemoryBufferConsumer::copy() 未实现
        throw std::runtime_error(
                "ResultSubpartitionRecoveredStateHandler::recover does not support fan-out restore yet: "
                "multiple mapped channels require BufferConsumer::copy(), but MemoryBufferConsumer::copy() "
                "is not implemented.");
    }

    LOG("Recovered state for partition " << subpartitionInfo.getPartitionIdx()
                                         << ", subpartition " << subpartitionInfo.getSubPartitionIdx()
                                         << ", size " << bufferConsumer->getBufferSize()
                                         << ", mappedChannels=" << channels.size())
}

void ResultSubpartitionRecoveredStateHandler::close()
{
    for (auto& writer : writers_) {
        if (auto checkpointedWriter = std::dynamic_pointer_cast<CheckpointedResultPartition>(writer)) {
            checkpointedWriter->finishReadRecoveredState(notifyAndBlockOnCompletion_);
        }
    }
    LOG("Close ResultSubpartitionRecoveredStateHandler, finishReadRecoveredState writers size:" << writers_.size());
}

std::shared_ptr<CheckpointedResultSubpartition> ResultSubpartitionRecoveredStateHandler::getSubpartition(
        int partitionIndex,
        int subPartitionIdx)
{
    LOG("ResultSubpartitionRecoveredStateHandler getSubpartition111")
    auto writer = writers_.at(partitionIndex);

    auto checkpointedWriter = std::dynamic_pointer_cast<omnistream::CheckpointedResultPartition>(writer);
    if (!checkpointedWriter) {
        LOG("ResultSubpartitionRecoveredStateHandler getSubpartition222")
        throw std::runtime_error(
                "Cannot restore state to a non-checkpointable partition type, partitionIndex=" +
                std::to_string(partitionIndex));
    }
    LOG("ResultSubpartitionRecoveredStateHandler getSubpartition333")
    auto checkpointedSubpartition = checkpointedWriter->getCheckpointedSubpartition(subPartitionIdx);
    if (!checkpointedSubpartition) {
        LOG("ResultSubpartitionRecoveredStateHandler getSubpartition555")
        throw std::runtime_error(
                "Checkpointed subpartition is not a PipelinedSubpartition, partitionIndex=" +
                std::to_string(partitionIndex) + ", subPartitionIdx=" + std::to_string(subPartitionIdx));
    }
    LOG("ResultSubpartitionRecoveredStateHandler getSubpartition666")
    return checkpointedSubpartition;
}

std::vector<std::shared_ptr<CheckpointedResultSubpartition>> ResultSubpartitionRecoveredStateHandler::getMappedChannels(const ResultSubpartitionInfoPOD& subpartitionInfo)
{
    LOG("ResultSubpartitionRecoveredStateHandler getMappedChannels111")
    auto it = rescaledChannels_.find(subpartitionInfo);
    if (it != rescaledChannels_.end()) {
        return it->second;
    }
    LOG("getMappedChannels add InfoPOD: " << subpartitionInfo.toString());
    auto pipelinedSubpartitions = calculateMapping(subpartitionInfo);
    rescaledChannels_.emplace(subpartitionInfo, pipelinedSubpartitions);
    return pipelinedSubpartitions;
}

std::vector<std::shared_ptr<CheckpointedResultSubpartition>>
    ResultSubpartitionRecoveredStateHandler::calculateMapping(const ResultSubpartitionInfoPOD& info)
{
    int pIdx = info.getPartitionIdx();

    auto mapping = channelMapping_ ? channelMapping_->GetChannelMapping(pIdx)
                                   : IdentityRescaleMappings::SYMMETRIC_IDENTITY;

    // 纯恢复 / 未 rescale：直接按 identity 映射，不要去 invert SYMMETRIC_IDENTITY
    if (!mapping || mapping->isIdentity()) {
        return { getSubpartition(pIdx, info.getSubPartitionIdx()) };
    }

    if (oldToNewMappings_.find(pIdx) == oldToNewMappings_.end()) {
        oldToNewMappings_.emplace(pIdx, mapping->invert());
    }

    const auto& oldToNewMapping = oldToNewMappings_.at(pIdx);
//        std::vector<std::shared_ptr<PipelinedSubpartition>> subpartitions;
    std::vector<std::shared_ptr<CheckpointedResultSubpartition>> subpartitions;

    auto mappedIndexes = oldToNewMapping.getMappedIndexes(info.getSubPartitionIdx());
    for (int newIndex : mappedIndexes) {
        subpartitions.push_back(getSubpartition(pIdx, newIndex));
    }

    if (subpartitions.empty()) {
        LOG("ERROR: Recovered a buffer that has no mapping, partitionIdx=" << std::to_string(info.getPartitionIdx())
            << ", subPartitionIdx=" << std::to_string(info.getSubPartitionIdx()));
        throw std::runtime_error(
                "Recovered a buffer that has no mapping, partitionIdx=" +
                std::to_string(info.getPartitionIdx()) +
                ", subPartitionIdx=" + std::to_string(info.getSubPartitionIdx()));
    }
    return subpartitions;
}

InputChannelRecoveredStateHandler::~InputChannelRecoveredStateHandler()
{
    this->close();
}

RecoveredChannelStateHandler<InputChannelInfo, Buffer *>::BufferWithContext
    InputChannelRecoveredStateHandler::getBuffer(const InputChannelInfo &inputChannelInfo)
{

    auto channel = getMappedChannels(inputChannelInfo)[0];

    auto buffer = channel->requestBufferBlocking();
    // support nothing
    return BufferWithContext(ChannelStateByteBuffer::wrap(&*buffer), &*buffer);
}

void InputChannelRecoveredStateHandler::recover(const InputChannelInfo &inputChannelInfo,
    int oldSubtaskIndex,
    const BufferWithContext &bufferWithContext)
{
    auto buffer = bufferWithContext.context_;

    try {
        if (buffer->GetSize() > 0) {
            auto channels = getMappedChannels(inputChannelInfo);
            if (channels.empty()) {
                throw std::runtime_error("No mapped channels found in InputChannelRecoveredStateHandler::recover");
            }

            if (channels.size() == 1) {
                channels[0]->onRecoveredStateBuffer2(buffer);
            } else {
                throw std::runtime_error(
                                "InputChannelRecoveredStateHandler::recover does not support fan-out restore yet");
            }

            LOG("Recovered state for gate " << inputChannelInfo.getGateIdx()
                                            << ", channel " << inputChannelInfo.getInputChannelIdx()
                                            << ", size " << buffer->GetSize()
                                            << ", mappedChannels=" << channels.size());
        }
    } catch (const std::exception& e){
        buffer->RecycleBuffer();
        throw std::runtime_error("failed to InputChannelRecoveredStateHandler recover");
    }
}

void InputChannelRecoveredStateHandler::close()
{
    for (const auto& inputGate : inputGates) {
        inputGate->FinishReadRecoveredState();
    }
    LOG("Close InputChannelRecoveredStateHandler, finishReadRecoveredState inputGate size：" << inputGates.size());
}

std::shared_ptr<RecoveredInputChannel> InputChannelRecoveredStateHandler::getChannel(int gateIndex,
    int subPartitionIndex)
{
    auto inputChannel = inputGates.at(gateIndex)->getChannel(subPartitionIndex);
    auto inputChannel2 = std::dynamic_pointer_cast<RecoveredInputChannel>(inputChannel);
    if (!inputChannel2) {
        INFO_RELEASE("ERROR: Cannot restore state to a non-checkpointable partition type");
        throw std::runtime_error("Cannot restore state to a non-checkpointable partition type");
    }
    return inputChannel2;
}

std::vector<std::shared_ptr<RecoveredInputChannel>>
InputChannelRecoveredStateHandler::calculateMapping(InputChannelInfo info)
{
    LOG("InputChannelRecoveredStateHandler calculateMapping111")
    int pIdx = info.getGateIdx();

    auto mapping = channelMapping ? channelMapping->GetChannelMapping(pIdx)
                                  : IdentityRescaleMappings::SYMMETRIC_IDENTITY;

    if (!mapping || mapping->isIdentity()) {
        return { getChannel(pIdx, info.getInputChannelIdx()) };
    }

    if (oldToNewMappings.find(pIdx) == oldToNewMappings.end()) {
        oldToNewMappings.emplace(pIdx, mapping->invert());
    }

    const auto& oldToNewMapping = oldToNewMappings.at(pIdx);
    std::vector<std::shared_ptr<RecoveredInputChannel>> channels;

    auto mappedIndexes = oldToNewMapping.getMappedIndexes(info.getInputChannelIdx());
    for (int newIndex : mappedIndexes) {
        channels.push_back(getChannel(pIdx, newIndex));
    }

    if (channels.empty()) {
        throw std::runtime_error("Recovered a buffer that has no mapping");
    }
    LOG("InputChannelRecoveredStateHandler calculateMapping end")
    return channels;
}

std::vector<std::shared_ptr<RecoveredInputChannel>> InputChannelRecoveredStateHandler::getMappedChannels(
    InputChannelInfo channelInfo)
{
    auto it = rescaledChannels.find(channelInfo);
    if (it != rescaledChannels.end()) {
        return it->second;
    }
    LOG("getMappedChannels add ChannelInfo: " << channelInfo.toString());
    auto channels = calculateMapping(channelInfo);
    rescaledChannels.emplace(channelInfo, channels);
    return channels;
}
} // namespace omnistream