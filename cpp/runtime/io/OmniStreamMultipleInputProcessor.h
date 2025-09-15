/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OMNISTREAMMULTIPLEINPUTPROCESSOR_H
#define OMNISTREAM_OMNISTREAMMULTIPLEINPUTPROCESSOR_H

#include "OmniStreamOneInputProcessor.h"
#include "MutipleInputSelectionHandler.h"

namespace omnistream {
    class OmniStreamMultipleInputProcessor : public OmniStreamInputProcessor {
    public:
        OmniStreamMultipleInputProcessor(std::vector<OmniStreamOneInputProcessor *> &&processors,
                                         std::shared_ptr<MutipleInputSelectionHandler> inputSelectionHandler)
                : processors(processors), inputSelectionHandler(inputSelectionHandler) {
        }

        DataInputStatus processInput() override;

    private:
        std::vector<OmniStreamOneInputProcessor *> processors;
        std::shared_ptr<MutipleInputSelectionHandler> inputSelectionHandler;
        bool isPrepared = false;
        int32_t lastReadInputIndex;
        int selectFirstReadingInputIndex();
        int selectNextReadingInputIndex();
        void FullCheckAndSetAvailable();
    };
}

#endif //OMNISTREAM_OMNISTREAMMULTIPLEINPUTPROCESSOR_H
