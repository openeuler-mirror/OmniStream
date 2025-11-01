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
#ifndef OMNISTREAM_KEYGROUPENTRYITERATOR_H
#define OMNISTREAM_KEYGROUPENTRYITERATOR_H

#include <stdexcept>
#include "KeyGroupEntry.h"

class KeyGroupEntryIterator {
public:
    KeyGroupEntryIterator(const std::vector<KeyGroupEntry>& entries)
        : entries_(entries), currentIndex_(0) {
    }

    bool hasNext()
    {
        return currentIndex_ < entries_.size();
    }

    std::unique_ptr<KeyGroupEntry> next()
    {
        if (!hasNext()) {
            throw std::out_of_range("No more elements in KeyGroupEntryIterator");
        }

        return std::make_unique<KeyGroupEntry>(entries_[currentIndex_++]);
    }

private:
    std::vector<KeyGroupEntry> entries_;
    size_t currentIndex_;
};

#endif // OMNISTREAM_KEYGROUPENTRYITERATOR_H
