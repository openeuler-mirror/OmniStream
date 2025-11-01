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
#ifndef OMNISTREAM_KEYGROUP_H
#define OMNISTREAM_KEYGROUP_H

#include "KeyGroupEntryIterator.h"
class KeyGroup {
public:
    explicit KeyGroup(int keyGroupId) : keyGroupId_(keyGroupId) {}

    KeyGroup(int keyGroupId, std::shared_ptr<KeyGroupEntryIterator> keyGroupEntries)
        : keyGroupId_(keyGroupId), keyGroupEntries_(keyGroupEntries) {}

    int getKeyGroupId() const { return keyGroupId_; }

    std::shared_ptr<KeyGroupEntryIterator> getKeyGroupEntries() { return keyGroupEntries_; }

private:
    int keyGroupId_;
    std::shared_ptr<KeyGroupEntryIterator> keyGroupEntries_;
};

#endif // OMNISTREAM_KEYGROUP_H
