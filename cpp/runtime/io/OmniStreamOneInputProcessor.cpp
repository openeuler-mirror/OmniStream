/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OmniStreamOneInputProcessor.h"

namespace omnistream {
    OmniStreamOneInputProcessor::OmniStreamOneInputProcessor(OmniStreamTaskInput *input,
        OmniStreamTaskInput::OmniDataOutput *output, OperatorChainV2 *operatorChain)
        : input(input), output(output), endOfInputAware(operatorChain) {
    }

    DataInputStatus OmniStreamOneInputProcessor::processInput()
    {
        // LOG(">>>process Input")
        DataInputStatus status = input->emitNext(output);
        // LOG_TRACE(" Return status  "  << DataInputStatusHelper::mapToInt(status))
        return status;
    }

    std::shared_ptr<CompletableFuture> OmniStreamOneInputProcessor::getAvailableFuture()
    {
        return input->getAvailableFuture();
    }

    OmniStreamTaskInput* OmniStreamOneInputProcessor::GetInput()
    {
        return input;
    }
    void OmniStreamOneInputProcessor::close()
    {
        input->close();
        output->close();
    }

}