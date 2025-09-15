/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OmniStreamMultipleInputProcessor.h"
namespace omnistream {
    DataInputStatus OmniStreamMultipleInputProcessor::processInput()
    {
        int readingInputIndex;
        if (isPrepared) {
            readingInputIndex = selectNextReadingInputIndex();
        } else {
            // the preparations here are not placed in the constructor because all work in it
            // must be executed after all operators are opened.
            readingInputIndex = selectFirstReadingInputIndex();
        }
        if (readingInputIndex == -1) {
            return DataInputStatus::NOTHING_AVAILABLE;
        }
//        return DataInputStatus::NOTHING_AVAILABLE;
//    }
        lastReadInputIndex = readingInputIndex;
        LOG("OmniStreamMultipleInputProcessor processInput reading index = " << readingInputIndex)
        DataInputStatus dataInputStatus = processors[readingInputIndex]->processInput();
        DataInputStatus status = inputSelectionHandler->updateStatusAndSelection(dataInputStatus, readingInputIndex);
        return status;
    }

    int OmniStreamMultipleInputProcessor::selectFirstReadingInputIndex()
    {
        isPrepared = true;

        return selectNextReadingInputIndex();
    }

    void OmniStreamMultipleInputProcessor::FullCheckAndSetAvailable()
    {
        for (size_t i = 0; i < processors.size(); i++) {
            auto inputProcessor = processors[i];
            // the input is constantly available and another is not, we will be checking this
            // volatile
            // once per every record. This might be optimized to only check once per processed
            // NetworkBuffer
            if (inputProcessor->isApproximatelyAvailable() || inputProcessor->isAvailable()) {
                inputSelectionHandler->SetAvailableInput(i);
            }
        }
    }

    int OmniStreamMultipleInputProcessor::selectNextReadingInputIndex()
    {
        if (!inputSelectionHandler->isAnyInputAvailable()) {
            FullCheckAndSetAvailable();
        }

        int readingInputIndex = inputSelectionHandler->selectNextInputIndex(lastReadInputIndex);
        if (readingInputIndex == -1) {
            return -1;
        }

        if (inputSelectionHandler->ShouldSetAvailableForAnotherInput()) {
            FullCheckAndSetAvailable();
        }
        return readingInputIndex;
    }
}