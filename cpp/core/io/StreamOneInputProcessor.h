/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: StreamOneInputProcess for DataStream
 */
#ifndef STREAMTONEINPUTPROCESSOR_H
#define STREAMTONEINPUTPROCESSOR_H

#include <nlohmann/json.hpp>
#include "../task/StreamTaskNetworkOutput.h"
#include "../task/StreamTaskNetworkInput.h"

using json = nlohmann::json;

namespace omnistream::datastream {
    class StreamOneInputProcessor {
    public:
        StreamOneInputProcessor(std::unique_ptr<StreamTaskNetworkOutput> streamTaskOutput, std::unique_ptr<StreamTaskNetworkInput> streamTaskInput);
        ~StreamOneInputProcessor();

        /* data */
        std::unique_ptr<StreamTaskNetworkOutput> streamTaskOutput;
        std::unique_ptr<StreamTaskNetworkInput> streamTaskInput;

        int processInput(const uint8_t *input_buffer, size_t input_size, long channelInfo, int32_t &inputNumber);
    };

}
#endif