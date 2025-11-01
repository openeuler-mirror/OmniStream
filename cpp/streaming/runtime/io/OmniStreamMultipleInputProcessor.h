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
        std::shared_ptr<CompletableFuture> GetAvailableFuture() override
        {
            // TTODO
            return nullptr;
        };
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

#endif
