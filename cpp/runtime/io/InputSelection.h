/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_INPUTSELECTION_H
#define OMNISTREAM_INPUTSELECTION_H

class InputSelection {
public:
    static int fairSelectNextIndex(
            long inputMask, long availableInputsMask, int lastReadInputIndex)
    {
        long combineMask = availableInputsMask & inputMask;

        if (combineMask == 0) {
            return -1;
        }

        int nextReadInputIndex = selectFirstBitRightFromNext(combineMask, lastReadInputIndex + 1);
        if (nextReadInputIndex >= 0) {
            return nextReadInputIndex;
        }
        return selectFirstBitRightFromNext(combineMask, 0);
    }

    static int selectFirstBitRightFromNext(long bits, int next)
    {
        if (next >= 64) {
            return -1;
        }
        for (bits >>= next; bits != 0 && (bits & 1) != 1; bits >>= 1, next++) {}
        return bits != 0 ? next : -1;
    }
};

#endif //OMNISTREAM_INPUTSELECTION_H
