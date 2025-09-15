/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "MutipleInputSelectionHandler.h"
#include "InputSelection.h"

namespace omnistream {
    int MutipleInputSelectionHandler::selectNextInputIndex(int lastReadInputIndex) {
        return InputSelection::fairSelectNextIndex(
                selectedInputsMask,
                availableInputsMask & notFinishedInputsMask,
                lastReadInputIndex);
    }
} // omnistream