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
#ifndef OMNISTREAM_ABSTRACT_CHANNEL_STATE_HANDLE_H
#define OMNISTREAM_ABSTRACT_CHANNEL_STATE_HANDLE_H

#include <vector>
#include <memory>
#include <cstdint>
#include <stdexcept>
#include "runtime/state/StreamStateHandle.h"

namespace omnistream {

template <typename Info>
class AbstractChannelStateHandle {
public:
    struct StateContentMetaInfo {
        std::vector<int64_t> offsets;
        int64_t size = 0;

        StateContentMetaInfo() = default;

        StateContentMetaInfo(const std::vector<int64_t>& offsets, int64_t size)
            : offsets(offsets), size(size) {}

        void WithDataAdded(int64_t offset, int64_t dataSize)
        {
            offsets.push_back(offset);
            size += dataSize;
        }

        const std::vector<int64_t>& GetOffsets() const
        {
            return offsets;
        }

        int64_t GetSize() const
        {
            return size;
        }
    };

    AbstractChannelStateHandle(int subtaskIndex,
                               const Info& info,
                               std::shared_ptr<StreamStateHandle> delegate,
                               const std::vector<int64_t>& offsets,
                               int64_t size)
        : subtaskIndex(subtaskIndex),
          info(info),
          delegate(std::move(delegate)),
          offsets(offsets),
          size(size) {}

    virtual ~AbstractChannelStateHandle() = default;

    int GetSubtaskIndex() const
    {
        return subtaskIndex;
    }

    const Info& GetInfo() const
    {
        return info;
    }

    const std::vector<int64_t>& GetOffsets() const
    {
        return offsets;
    }

    std::shared_ptr<StreamStateHandle> GetDelegate() const
    {
        return delegate;
    }

    virtual void DiscardState()
    {
        if (delegate) {
            delegate->DiscardState();
        }
    }

    virtual int64_t GetStateSize()
    {
        return size;
    }

protected:
    const int subtaskIndex;
    const Info info;
    const std::shared_ptr<StreamStateHandle> delegate;
    const std::vector<int64_t> offsets;
    const int64_t size;
};

} // namespace omnistream

#endif // OMNISTREAM_ABSTRACT_CHANNEL_STATE_HANDLE_H