/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: StreamOneInputProcess implementations
 */
#include "../task/StreamTask.h"
#include "StreamOneInputProcessor.h"

namespace omnistream::datastream {
    StreamOneInputProcessor::StreamOneInputProcessor(std::unique_ptr<StreamTaskNetworkOutput> streamTaskOutput,
                                                     std::unique_ptr<StreamTaskNetworkInput> streamTaskInput) :
            streamTaskOutput(std::move(streamTaskOutput)), streamTaskInput(std::move(streamTaskInput))
    {
    }

    StreamOneInputProcessor::~StreamOneInputProcessor()
    {
    }

    int StreamOneInputProcessor::processInput(const uint8_t *input_buffer, size_t input_size,
                                              long channelInfo, int32_t &inputNumber)
    {
        this->streamTaskInput->emitNextProcessBuffer(input_buffer, input_size, channelInfo);
        int status = this->streamTaskInput->emitNextProcessElement(*streamTaskOutput, inputNumber);
        return status;
    }
}

