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

#ifndef OMNISTREAM_INPUTSELECTION_H
#define OMNISTREAM_INPUTSELECTION_H

class InputSelection {
public:
    static int fairSelectNextIndex(
            long inputMask, long availableInputsMask, int lastReadInputIndex)
    {
        uint64_t uavailableInputsMask = static_cast<uint64_t>(availableInputsMask);
        uint64_t uinputMask = static_cast<uint64_t>(inputMask);
        long combineMask = static_cast<int64_t>(uavailableInputsMask & uinputMask);

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
        uint64_t ubits = static_cast<uint64_t>(bits);
        for (ubits >>= next; ubits != 0 && (ubits & 1) != 1; ubits >>= 1, next++) {}
        return ubits != 0 ? next : -1;
    }
};

#endif
