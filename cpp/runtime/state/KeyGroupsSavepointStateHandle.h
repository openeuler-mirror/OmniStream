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
#ifndef OMNISTREAM_KEYGROUPSSAVEPOINTSTATEHANDLE
#define OMNISTREAM_KEYGROUPSSAVEPOINTSTATEHANDLE
#include "runtime/state/KeyGroupsStateHandle.h"
#include <memory>
#include <nlohmann/json.hpp>

class KeyGroupsSavepointStateHandle : public KeyGroupsStateHandle {
public:
    KeyGroupsSavepointStateHandle(
        const KeyGroupRangeOffsets& groupRangeOffsets,
        const std::shared_ptr<StreamStateHandle>& streamStateHandle);
    explicit KeyGroupsSavepointStateHandle(const nlohmann::json &description);
    std::shared_ptr<KeyedStateHandle> GetIntersection(
        const KeyGroupRange& keyGroupRange) const override;
    std::string ToString() const override;
};
#endif