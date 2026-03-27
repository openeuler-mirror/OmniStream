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

#include "SequentialChannelStateReaderImpl.h"

#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <tuple>
#include <stdexcept>
#include <memory>
#include <sstream>
#include <functional>

void SequentialChannelStateReaderImpl::readInputData(const std::vector<std::shared_ptr<InputGate>> &inputGates)
{
    LOG("Read input data start.");
    auto stateHandler = std::make_shared<InputChannelRecoveredStateHandler>(
        inputGates, taskStateSnapshot->GetInputRescalingDescriptor());

    readByInputChannel(stateHandler, groupByDelegateByInputChannel(streamSubtaskStates()));
    LOG("Read input data end.");
}

void SequentialChannelStateReaderImpl::readOutputData(const std::vector<std::shared_ptr<ResultPartitionWriter>> &writers, bool notifyAndBlockOnCompletion)
{
    LOG("Read output data start.");
    auto stateHandler = std::make_shared<ResultSubpartitionRecoveredStateHandler>(
        writers, notifyAndBlockOnCompletion, taskStateSnapshot->GetOutputRescalingDescriptor());
    
    readByResultSubpartition(stateHandler, groupByDelegateByResultSubpartition(streamSubtaskStates()));
    LOG("Read output data end.");
}

void SequentialChannelStateReaderImpl::close()
{
}

void SequentialChannelStateReaderImpl::readByInputChannel(
    std::shared_ptr<InputChannelRecoveredStateHandler> stateHandler,
    const InputChannelHandleGroup &streamStateHandleListMap)
{
    LOG("readByInputChannel size: " << streamStateHandleListMap.size());
    for (auto& entry : streamStateHandleListMap) {
        readSequentiallyByInputChannel(entry.second.first, entry.second.second, stateHandler);
    }
}

void SequentialChannelStateReaderImpl::readByResultSubpartition(
    std::shared_ptr<ResultSubpartitionRecoveredStateHandler> stateHandler,
    const ResultSubpartitionHandleGroup &streamStateHandleListMap)
{
    LOG("readByResultSubpartition size: " << streamStateHandleListMap.size());
    for (auto& entry : streamStateHandleListMap) {
        readSequentiallyByResultSubpartition(entry.second.first, entry.second.second, stateHandler);
    }
}

std::string ExtractChkPath(const std::string& fullPath) {
    size_t pos = fullPath.find("chk-");
    if (pos == std::string::npos) {
        LOG("ERROR: ExtractChkPath path failed.");
        return "";
    }
    return fullPath.substr(pos);
}

void SequentialChannelStateReaderImpl::readSequentiallyByInputChannel(
   std::shared_ptr<StreamStateHandle> streamStateHandle,
   std::vector<InputChannelStateHandle> channelStateHandles,
   std::shared_ptr<InputChannelRecoveredStateHandler> stateHandle)
{
    LOG("readSequentiallyByInputChannel start.");
    if(auto byteStreamStateHandle = std::dynamic_pointer_cast<ByteStreamStateHandle>(streamStateHandle)) {
        auto inputStream = byteStreamStateHandle->OpenInputStream();
        std::shared_ptr<ByteStateHandleInputStream> byteInputStream;
        if (byteInputStream = std::dynamic_pointer_cast<ByteStateHandleInputStream>(inputStream)) {
            serializer->ReadHeader2(byteInputStream);
        } else {
            LOG("ERROR: Failed to cast input stream to ByteStateHandleInputStream");
            throw std::runtime_error("Failed to cast input stream to ByteStateHandleInputStream");
        }

        for (const auto& offsetAndChannelInfo:extractOffsetsSortedByInputChannel(channelStateHandles)){
            chunkReader->readChunkByByteStreamForInputChannel(
                byteInputStream,
                offsetAndChannelInfo.offset,
                stateHandle,
                offsetAndChannelInfo.channelInfo,
                offsetAndChannelInfo.oldSubtaskIndex);
        }
        LOG("readSequentiallyByInputChannel end");
    } else if(std::dynamic_pointer_cast<RelativeFileStateHandle>(streamStateHandle)) {
        auto filePath = streamStateHandle->GetStreamStateHandleID().getKeyString();
        LOG("readSequentiallyByInputChannel file path: " << filePath);
        auto tmpPath = "/tmp/" + ExtractChkPath(filePath);
        if (omniTaskBridge_->CallDownloadFileToLocal(*streamStateHandle, tmpPath)) {
            LOG("downLoad file success: " << tmpPath);
        }
        std::ifstream is(tmpPath, std::ios::binary);
        if(!is.is_open()) {
            LOG("ERROR: Failed to open stream state handle input stream. file path: " << filePath);
            throw std::ios_base::failure("Failed to open stream state handle input stream.");
        }

        serializer->ReadHeader(is);

        LOG("before extractOffsetsSortedByInputChannel");
        for (const auto& offsetAndChannelInfo:extractOffsetsSortedByInputChannel(channelStateHandles)){
            LOG("before readChunkByFsForInputChannel");
            chunkReader->readChunkByFsForInputChannel(
                is,
                offsetAndChannelInfo.offset,
                stateHandle,
                offsetAndChannelInfo.channelInfo,
                offsetAndChannelInfo.oldSubtaskIndex);
            LOG("after readChunkByFsForInputChannel");
        }
        LOG("after extractOffsetsSortedByInputChannel");
        try {
            if (is.is_open()) {
                is.close();
                std::filesystem::remove(tmpPath);
            }
        } catch(...) {
            LOG("ERROR: close file failed.");
        }
        LOG("readSequentiallyByInputChannel end");
    } else {
        LOG("ERROR: streamStateHandle type is not excepted.");
    }
}

void SequentialChannelStateReaderImpl::readSequentiallyByResultSubpartition(
   std::shared_ptr<StreamStateHandle> streamStateHandle,
   std::vector<ResultSubpartitionStateHandle> channelStateHandles,
   std::shared_ptr<ResultSubpartitionRecoveredStateHandler> stateHandle)
{
    LOG("readSequentiallyByResultSubpartition start.");
    if(auto byteStreamStateHandle = std::dynamic_pointer_cast<ByteStreamStateHandle>(streamStateHandle)) {
        auto inputStream = byteStreamStateHandle->OpenInputStream();
        std::shared_ptr<ByteStateHandleInputStream> byteInputStream;
        if (byteInputStream = std::dynamic_pointer_cast<ByteStateHandleInputStream>(inputStream)) {
            serializer->ReadHeader2(byteInputStream);
        } else {
            LOG("ERROR: Failed to cast input stream to ByteStateHandleInputStream");
            throw std::runtime_error("Failed to cast input stream to ByteStateHandleInputStream");
        }

        for (const auto& offsetAndChannelInfo:extractOffsetsSortedByResultSubpartition(channelStateHandles)){
            chunkReader->readChunkByByteStreamForResultSubpartition(
                byteInputStream,
                offsetAndChannelInfo.offset,
                stateHandle,
                offsetAndChannelInfo.channelInfo,
                offsetAndChannelInfo.oldSubtaskIndex);
        }
        LOG("readSequentiallyByResultSubpartition end");
    } else if(std::dynamic_pointer_cast<RelativeFileStateHandle>(streamStateHandle)) {
        auto filePath = streamStateHandle->GetStreamStateHandleID().getKeyString();
        LOG("readSequentiallyByResultSubpartition file path: " << filePath);
        auto tmpPath = "/tmp/" + ExtractChkPath(filePath);
        if (omniTaskBridge_->CallDownloadFileToLocal(*streamStateHandle, tmpPath)) {
            LOG("downLoad file success: " << tmpPath);
        }
        std::ifstream is(tmpPath, std::ios::binary);
        if(!is.is_open()) {
            LOG("ERROR: Failed to open stream state handle input stream. file path: " << filePath);
            throw std::ios_base::failure("Failed to open stream state handle input stream.");
        }

        serializer->ReadHeader(is);

        for (const auto& offsetAndChannelInfo:extractOffsetsSortedByResultSubpartition(channelStateHandles)){
            chunkReader->readChunkByFsForResultSubpartition(
                is,
                offsetAndChannelInfo.offset,
                stateHandle,
                offsetAndChannelInfo.channelInfo,
                offsetAndChannelInfo.oldSubtaskIndex);
        }
        try {
            if (is.is_open()) {
                is.close();
                std::filesystem::remove(tmpPath);
            }
        } catch(...) {
            LOG("ERROR: close file failed.");
        }
        LOG("readSequentiallyByResultSubpartition end");
    } else {
        LOG("ERROR: streamStateHandle type is not excepted.");
    }
}

std::vector<std::shared_ptr<OperatorSubtaskState>> SequentialChannelStateReaderImpl::streamSubtaskStates()
{
    std::vector<std::shared_ptr<OperatorSubtaskState>> subtaskStates;

    auto subtaskStateMappings = taskStateSnapshot->GetSubtaskStateMappings();

    for (const auto& entry : subtaskStateMappings){
        subtaskStates.push_back(entry.second);      //entry.second 应该是一个std::shared_ptr<OperatorSubtaskState>
    }

    return subtaskStates;
}

InputChannelHandleGroup SequentialChannelStateReaderImpl::groupByDelegateByInputChannel(
    std::vector<std::shared_ptr<OperatorSubtaskState>> states)
{
    InputChannelHandleGroup result;
    for (auto& state : states) {
        auto stateHandles = state->getInputChannelState();
        for (auto& handle : stateHandles) {
            auto delegate = handle->GetDelegate();
            auto key = delegate->GetStreamStateHandleID().getKeyString();

            auto& entry = result[key];
            if (!entry.first) {
                entry.first = delegate;
            }
            entry.second.push_back(*handle);
        }
    }
    return result;
}

ResultSubpartitionHandleGroup SequentialChannelStateReaderImpl::groupByDelegateByResultSubpartition(
    std::vector<std::shared_ptr<OperatorSubtaskState>> states)
{
    ResultSubpartitionHandleGroup result;
    for (auto& state : states) {
        auto stateHandles = state->getResultSubpartitionState();
        for (auto& handle : stateHandles) {
            auto delegate = handle->GetDelegate();
            auto key = delegate->GetStreamStateHandleID().getKeyString();

            auto& entry = result[key];
            if (!entry.first) {
                entry.first = delegate;
            }
            entry.second.push_back(*handle);
        }
    }
    return result;
}

std::vector<RescaledOffset<InputChannelInfo>> SequentialChannelStateReaderImpl::extractOffsetsSortedByInputChannel(
    const std::vector<InputChannelStateHandle>& channelStateHandles)
{
    std::vector<RescaledOffset<InputChannelInfo>> offsets;

    for (const auto& handle : channelStateHandles) {
        auto extractedOffsets = extractOffsetsByInputChannel(handle);
        offsets.insert(offsets.end(),extractedOffsets.begin(), extractedOffsets.end());
    }
    LOG("channelStateHandles vec: " << channelStateHandles.size() << ", offsets vec: " << offsets.size()
        << ", start off: " << offsets[0].offset << ", end off: " << offsets[offsets.size() - 1].offset
        << ", info: " << offsets[0].channelInfo.toString());

    std::sort(offsets.begin(), offsets.end(), [](const RescaledOffset<InputChannelInfo>& a, const RescaledOffset<InputChannelInfo>& b) {
        return a.offset < b.offset;
    });
    return offsets;
}

std::vector<RescaledOffset<ResultSubpartitionInfoPOD>> SequentialChannelStateReaderImpl::extractOffsetsSortedByResultSubpartition(
    const std::vector<ResultSubpartitionStateHandle>& channelStateHandles)
{
    std::vector<RescaledOffset<ResultSubpartitionInfoPOD>> offsets;

    for (const auto& handle : channelStateHandles) {
        auto extractedOffsets = extractOffsetsByResultSubpartition(handle);
        offsets.insert(offsets.end(),extractedOffsets.begin(), extractedOffsets.end());
    }
    LOG("channelStateHandles vec: " << channelStateHandles.size() << ", offsets vec: " << offsets.size()
        << ", start off: " << offsets[0].offset << ", end off: " << offsets[offsets.size() - 1].offset
        << ", info: " << offsets[0].channelInfo.toString());
    std::sort(offsets.begin(), offsets.end(), [](const RescaledOffset<ResultSubpartitionInfoPOD>& a, const RescaledOffset<ResultSubpartitionInfoPOD>& b) {
        return a.offset < b.offset;
    });
    return offsets;
}

std::vector<RescaledOffset<InputChannelInfo>> SequentialChannelStateReaderImpl::extractOffsetsByInputChannel(const InputChannelStateHandle& handle)
{
    std::vector<RescaledOffset<InputChannelInfo>>  offsets;

    auto handleOffsets = handle.GetOffsets();

    for (const auto& offset : handleOffsets){
        offsets.emplace_back(offset, handle.GetInfo(), handle.GetSubtaskIndex());
    }
    return offsets;
}

std::vector<RescaledOffset<ResultSubpartitionInfoPOD>> SequentialChannelStateReaderImpl::extractOffsetsByResultSubpartition(const ResultSubpartitionStateHandle& handle)
{
    std::vector<RescaledOffset<ResultSubpartitionInfoPOD>>  offsets;

    auto handleOffsets = handle.GetOffsets();

    for (const auto& offset : handleOffsets){
        offsets.emplace_back(offset, handle.GetInfo(), handle.GetSubtaskIndex());
    }
    return offsets;
}

