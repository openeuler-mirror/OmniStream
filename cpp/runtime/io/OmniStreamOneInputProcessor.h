/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OMNISTREAMONEINPUTPROCESSOR_H
#define OMNISTREAM_OMNISTREAMONEINPUTPROCESSOR_H

#include "OmniStreamInputProcessor.h"
#include "OmniStreamTaskInput.h"
#include "OmniPushingAsyncDataInput.h"
#include "tasks/OperatorChain.h"

namespace omnistream {
    class OmniStreamOneInputProcessor : public OmniStreamInputProcessor {
    public:
        OmniStreamOneInputProcessor(OmniStreamTaskInput *input, OmniStreamTaskInput::OmniDataOutput *output,
            OperatorChainV2 *operatorChain);

        DataInputStatus processInput() override;
        std::shared_ptr<CompletableFuture> getAvailableFuture() override;
        OmniStreamTaskInput* GetInput();
        void close() override;

    private:
        OmniStreamTaskInput *input;
        OmniStreamTaskInput::OmniDataOutput *output;
        OperatorChainV2 *endOfInputAware;
    };
}

#endif // OMNISTREAM_OMNISTREAMONEINPUTPROCESSOR_H
