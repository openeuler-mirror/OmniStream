/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "CircularDoubleArray.h"
#include <algorithm> // for std::copy

namespace omnistream {
    CircularDoubleArray::CircularDoubleArray(int windowSize)
        : backingArray(windowSize, 0.0), nextPos(0), fullSize(false),
          elementsSeen(0)
    {
    }

    void CircularDoubleArray::AddValue(double value)
    {
        std::lock_guard<std::mutex> lock(mtx);
        backingArray[nextPos] = value;
        ++elementsSeen;
        ++nextPos;
        if (nextPos == static_cast<int>(backingArray.size())) {
            nextPos = 0;
            fullSize = true;
        }
    }

    std::vector<double> CircularDoubleArray::ToUnsortedArray()
    {
        std::lock_guard<std::mutex> lock(mtx);
        int size = GetSize();
        std::vector<double> result(size);
        std::copy(backingArray.begin(), backingArray.begin() + size, result.begin());
        return result;
    }

    int CircularDoubleArray::GetSize()
    {
        // Called within a lock so no additional locking is required.
        return fullSize ? static_cast<int>(backingArray.size()) : nextPos;
    }

    long CircularDoubleArray::GetElementsSeen()
    {
        // Called within a lock so no additional locking is required.
        return elementsSeen;
    }
} // namespace omnistream
