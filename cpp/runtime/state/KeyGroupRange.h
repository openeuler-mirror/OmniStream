/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_KEYGROUPRANGE_H
#define FLINK_TNEL_KEYGROUPRANGE_H

#include <stdexcept>
#include "core/utils/Iterator.h"
class KeyGroupRange
{
public:
    KeyGroupRange()
    {
        startKeyGroup = 0;
        endKeyGroup = -1;
    }

    static KeyGroupRange *EMPTY_KEY_GROUP_RANGE()
    {
        return new KeyGroupRange();
    }

    KeyGroupRange(int startKeyGroup, int endKeyGroup)
    {
        if (startKeyGroup < 0)
        {
            throw std::invalid_argument("IllegalArgumentException");
        }
        if (startKeyGroup > endKeyGroup)
        {
            throw std::invalid_argument("IllegalArgumentException");
        }
        this->startKeyGroup = startKeyGroup;
        this->endKeyGroup = endKeyGroup;
        if (getNumberOfKeyGroups() < 0)
        {
            throw std::invalid_argument("Potential Overflow Detected.");
        }
    }

    bool contains(int keyGroup)
    {
        return keyGroup >= startKeyGroup && keyGroup <= endKeyGroup;
    }

    int getNumberOfKeyGroups()
    {
        return 1 + endKeyGroup - startKeyGroup;
    }

    int getStartKeyGroup()
    {
        return startKeyGroup;
    }

    int getEndKeyGroup()
    {
        return endKeyGroup;
    }

    int getKeyGroupId(int idx)
    {
        return startKeyGroup + idx;
    }

    bool equals(KeyGroupRange other)
    {
        return startKeyGroup == other.startKeyGroup && endKeyGroup == other.endKeyGroup;
    }

    KeyGroupRange *of(int startKeyGroup, int endKeyGroup)
    {
        if (startKeyGroup <= endKeyGroup)
        {
            return new KeyGroupRange(startKeyGroup, endKeyGroup);
        }
        else
        {
            return EMPTY_KEY_GROUP_RANGE();
        }
    }

    // Iterator
    class KeyGroupIterator : public Iterator<int>
    {
    public:
        KeyGroupIterator(const KeyGroupRange &r) : range(r), iteratorPos(0) {}
        bool hasNext()
        {
            return iteratorPos < 1 + range.endKeyGroup + range.startKeyGroup;
        }
        int next()
        {
            return range.startKeyGroup + (iteratorPos++);
        }
    private:
        const KeyGroupRange &range;
        int iteratorPos;
    };
    KeyGroupIterator iterator()
    {
        return KeyGroupIterator(*this);
    }
private:
    int startKeyGroup;
    int endKeyGroup;

};

#endif // FLINK_TNEL_KEYGROUPRANGE_H
