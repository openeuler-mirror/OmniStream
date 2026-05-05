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

#include "OmniStreamOneInputProcessor.h"
#include "io/recover/OmniRescalingStreamTaskNetworkInput.h"
#include <thread>

namespace omnistream {
    OmniStreamOneInputProcessor::OmniStreamOneInputProcessor(OmniStreamTaskInput *input,
        OmniStreamTaskInput::OmniDataOutput *output, OperatorChainV2 *operatorChain)
        : input(input), output(output), endOfInputAware(operatorChain) {
    }

    OmniStreamOneInputProcessor::~OmniStreamOneInputProcessor() {
        std::thread::id tid = std::this_thread::get_id();
        INFO_RELEASE("DOUBLE_FREE_DEBUG: ~OmniStreamOneInputProcessor() START | this="
                << static_cast<void*>(this)
                << " | input=" << static_cast<void*>(input)
                << " | output=" << static_cast<void*>(output)
                << " | thread_id=" << tid);
        if (input != nullptr) {
            INFO_RELEASE("DOUBLE_FREE_DEBUG: ~OmniStreamOneInputProcessor() deleting input="
                    << static_cast<void*>(input)
                    << " | thread_id=" << tid);
            delete input;
            input = nullptr;
        }
        if (output != nullptr) {
            INFO_RELEASE("DOUBLE_FREE_DEBUG: ~OmniStreamOneInputProcessor() deleting output="
                    << static_cast<void*>(output)
                    << " | thread_id=" << tid);
            delete output;
            output = nullptr;
        }
        INFO_RELEASE("DOUBLE_FREE_DEBUG: ~OmniStreamOneInputProcessor() END | this="
                << static_cast<void*>(this)
                << " | thread_id=" << tid);
    }

    DataInputStatus OmniStreamOneInputProcessor::processInput()
    {
        // LOG(">>>process Input")
        DataInputStatus status = input->emitNext(output);
        if(status == DataInputStatus::END_OF_RECOVERY){
            auto recoverInput = dynamic_cast<OmniRescalingStreamTaskNetworkInput *>(input);
            if(recoverInput){
                input = recoverInput->finishRecover();
            }
            return status;
        }
        LOG("emitNext return status: "  << DataInputStatusHelper::mapToInt(status))
        return status;
    }

    std::shared_ptr<CompletableFuture> OmniStreamOneInputProcessor::GetAvailableFuture()
    {
        return input->GetAvailableFuture();
    }

    OmniStreamTaskInput* OmniStreamOneInputProcessor::GetInput()
    {
        return input;
    }

    std::shared_ptr<CompletableFutureV2<void>> OmniStreamOneInputProcessor::PrepareSnapshot(std::shared_ptr<ChannelStateWriter> writer,
            long checkpointID)
    {
        LOG("OneInput prepare snapshot, checkpointID: " << checkpointID);
        return input->PrepareSnapshot(writer, checkpointID);
    }

    void OmniStreamOneInputProcessor::close()
    {
        std::thread::id tid = std::this_thread::get_id();
        INFO_RELEASE("DOUBLE_FREE_DEBUG: OmniStreamOneInputProcessor::close() START | this="
                << static_cast<void*>(this)
                << " | input=" << static_cast<void*>(input)
                << " | output=" << static_cast<void*>(output)
                << " | thread_id=" << tid);
        if (input != nullptr) {
            INFO_RELEASE("DOUBLE_FREE_DEBUG: OmniStreamOneInputProcessor::close() calling input->close() | input="
                    << static_cast<void*>(input)
                    << " | thread_id=" << tid);
            input->close();
            INFO_RELEASE("DOUBLE_FREE_DEBUG: OmniStreamOneInputProcessor::close() input->close() DONE | input="
                    << static_cast<void*>(input)
                    << " | thread_id=" << tid);
        }
        if (output != nullptr) {
            INFO_RELEASE("DOUBLE_FREE_DEBUG: OmniStreamOneInputProcessor::close() calling output->close() | output="
                    << static_cast<void*>(output)
                    << " | thread_id=" << tid);
            output->close();
            INFO_RELEASE("DOUBLE_FREE_DEBUG: OmniStreamOneInputProcessor::close() output->close() DONE | output="
                    << static_cast<void*>(output)
                    << " | thread_id=" << tid);
        }
        INFO_RELEASE("DOUBLE_FREE_DEBUG: OmniStreamOneInputProcessor::close() END | this="
                << static_cast<void*>(this)
                << " | thread_id=" << tid);
    }
}