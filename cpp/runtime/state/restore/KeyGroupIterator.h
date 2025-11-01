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

#ifndef OMNISTREAM_KEYGROUPITERATOR_H
#define OMNISTREAM_KEYGROUPITERATOR_H
#include "KeyGroup.h"

class KeyGroupIterator {
public:
    KeyGroupIterator(const std::vector<KeyGroup>& keyGroups)
        : keyGroups_(keyGroups), currentIndex_(0) {
    }

    bool hasNext()
    {
        return currentIndex_ < keyGroups_.size();
    }

    std::unique_ptr<KeyGroup> next()
    {
        if (!hasNext()) {
            throw std::out_of_range("No more elements in KeyGroupIterator");
        }

        return std::make_unique<KeyGroup>(keyGroups_[currentIndex_++]);
    }

private:
    std::vector<KeyGroup> keyGroups_;
    size_t currentIndex_;
};

#endif // OMNISTREAM_KEYGROUPITERATOR_H
